from dataclasses import dataclass, field
from typing import Any

import rx_pb2
from structlog.testing import capture_logs

from session_manager.domain.ids import BlockId, ReceiverId, SessionId
from session_manager.domain.models import SessionSnapshot, SessionSpec, SessionState
from session_manager.services.aggregator import ProgressAggregator
from tests.fakes.fake_clock import FakeClock
from tests.fakes.fake_shm import FakeShm

R0 = ReceiverId(0)
R1 = ReceiverId(1)


def _spec(session_id: str = "s-1", *, total_blocks: int = 5, n: int = 255) -> SessionSpec:
    return SessionSpec(
        session_id=SessionId(session_id),
        relpath="sub/file.bin",
        file_size=1_000_000,
        file_hash=b"\x00" * 32,
        k=200,
        n=n,
        symbol_bytes=1400,
        total_blocks=total_blocks,
    )


def _stats(receiver_id: int, **fields: int) -> Any:
    return rx_pb2.ReceiverStats(receiver_id=receiver_id, **fields)


@dataclass
class _Rig:
    aggregator: ProgressAggregator
    clock: FakeClock
    shm: FakeShm
    completed: list[SessionSnapshot] = field(default_factory=list)


def _rig(*, shm_crosscheck: bool = True, stall_timeout_s: float = 8.0) -> _Rig:
    clock = FakeClock()
    shm = FakeShm()
    completed: list[SessionSnapshot] = []

    async def on_complete(snapshot: SessionSnapshot) -> None:
        completed.append(snapshot)

    aggregator = ProgressAggregator(
        clock=clock,
        shm_reader=shm,
        on_complete=on_complete,
        live_receivers=lambda: frozenset({R0, R1}),
        poll_interval_s=1.0,
        stall_timeout_s=stall_timeout_s,
        shm_crosscheck=shm_crosscheck,
    )
    return _Rig(aggregator, clock, shm, completed)


def _init_shm_session(shm: FakeShm, spec: SessionSpec) -> None:
    shm.create_or_adopt("seg", 4096)
    shm.init_session(spec, block_table_offset=64, bitmap_offset=256)


async def test_duplicate_block_decoded_is_idempotent_and_does_not_refresh_the_stall_timer() -> None:
    rig = _rig(shm_crosscheck=False, stall_timeout_s=8.0)
    spec = _spec(total_blocks=5)
    rig.aggregator.register_session(spec)

    rig.aggregator.handle_block_decoded(R0, spec.session_id, [0, 1])
    rig.clock.advance(5.0)
    rig.aggregator.handle_block_decoded(R0, spec.session_id, [0, 1])
    rig.aggregator.handle_block_decoded(R1, spec.session_id, [1])
    rig.clock.advance(4.0)

    await rig.aggregator.poll()

    snapshot = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot is not None
    assert snapshot.blocks_decoded == 2
    # last progress was at t=0; a refreshed timer at t=5 would leave 4s < 8s.
    assert snapshot.state is SessionState.INCOMPLETE


async def test_a_block_reported_by_two_receivers_counts_once() -> None:
    rig = _rig(shm_crosscheck=False)
    spec = _spec(total_blocks=5)
    rig.aggregator.register_session(spec)

    rig.aggregator.handle_block_decoded(R0, spec.session_id, [0, 1, 2])
    rig.aggregator.handle_block_decoded(R1, spec.session_id, [2, 3])
    await rig.aggregator.poll()

    snapshot = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot is not None
    assert snapshot.blocks_decoded == 4


async def test_out_of_range_block_ids_are_not_counted_as_progress() -> None:
    rig = _rig(shm_crosscheck=False)
    spec = _spec(total_blocks=3)
    rig.aggregator.register_session(spec)
    rig.clock.advance(10.0)

    rig.aggregator.handle_block_decoded(R0, spec.session_id, [5, 99])
    await rig.aggregator.poll()

    snapshot = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot is not None
    assert snapshot.blocks_decoded == 0
    assert snapshot.state is SessionState.INCOMPLETE


async def test_a_stalled_session_reports_incomplete_with_the_missing_blocks() -> None:
    rig = _rig(shm_crosscheck=False, stall_timeout_s=8.0)
    spec = _spec(total_blocks=5)
    rig.aggregator.register_session(spec)
    rig.aggregator.handle_block_decoded(R0, spec.session_id, [0, 2])
    rig.clock.advance(9.0)

    with capture_logs() as logs:
        await rig.aggregator.poll()

    snapshot = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot is not None
    assert snapshot.state is SessionState.INCOMPLETE
    assert snapshot.missing_blocks == (BlockId(1), BlockId(3), BlockId(4))
    assert snapshot.missing_block_count == 3
    assert rig.completed == []
    stalled = [entry for entry in logs if entry["event"] == "session_stalled"]
    assert stalled and stalled[0]["missing_blocks_preview"] == [1, 3, 4]


async def test_a_complete_session_is_handed_to_the_verifier_exactly_once() -> None:
    rig = _rig(shm_crosscheck=False)
    spec = _spec(total_blocks=3)
    rig.aggregator.register_session(spec)
    rig.aggregator.handle_block_decoded(R0, spec.session_id, [0, 1, 2])

    await rig.aggregator.poll()
    await rig.aggregator.poll()

    assert len(rig.completed) == 1
    assert rig.completed[0].state is SessionState.COMPLETE


async def test_a_diverging_bitmap_logs_a_warning_naming_both_counts() -> None:
    rig = _rig(shm_crosscheck=True)
    spec = _spec(total_blocks=8)
    _init_shm_session(rig.shm, spec)
    for block_id in (0, 1, 2):
        rig.shm.mark_block_decoded(spec.session_id, BlockId(block_id))
    rig.aggregator.register_session(spec)
    rig.aggregator.handle_block_decoded(R0, spec.session_id, [0, 1])

    with capture_logs() as logs:
        await rig.aggregator.poll()

    divergences = [entry for entry in logs if entry["event"] == "shm_bitmap_diverges_from_uds"]
    assert len(divergences) == 1
    assert divergences[0]["uds_count"] == 2
    assert divergences[0]["bitmap_count"] == 3


async def test_disabling_the_cross_check_changes_nothing_about_the_outcome() -> None:
    spec = _spec(total_blocks=8)
    block_ids = [0, 1, 3]

    outcomes: list[SessionSnapshot] = []
    for shm_crosscheck in (True, False):
        rig = _rig(shm_crosscheck=shm_crosscheck)
        rig.aggregator.register_session(spec)
        rig.aggregator.handle_block_decoded(R0, spec.session_id, block_ids)
        with capture_logs() as logs:
            await rig.aggregator.poll()
        assert not any(entry["event"].startswith("shm_bitmap_diverges") for entry in logs)
        snapshot = rig.aggregator.snapshot_for(spec.session_id)
        assert snapshot is not None
        outcomes.append(snapshot)

    with_check, without_check = outcomes
    assert with_check.state is without_check.state is SessionState.OPEN
    assert with_check.blocks_decoded == without_check.blocks_decoded == 3
    assert with_check.missing_block_count == without_check.missing_block_count == 5


async def test_block_decoded_for_an_unknown_session_is_ignored() -> None:
    rig = _rig(shm_crosscheck=False)

    with capture_logs() as logs:
        rig.aggregator.handle_block_decoded(R0, SessionId("ghost"), [1, 2])
    await rig.aggregator.poll()

    assert any(entry["event"] == "block_decoded_for_unknown_session" for entry in logs)
    assert rig.aggregator.snapshot_for(SessionId("ghost")) is None


async def test_receiver_stats_land_in_the_snapshot() -> None:
    rig = _rig(shm_crosscheck=False)
    spec = _spec(total_blocks=3)
    rig.aggregator.register_session(spec)

    rig.aggregator.handle_receiver_stats(
        _stats(1, pkts_ok=500, crc_fail=7, arena_high_water_pct=42)
    )
    await rig.aggregator.poll()

    snapshot = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot is not None
    counters = snapshot.counters_for(R1)
    assert counters is not None
    assert counters.pkts_ok == 500
    assert counters.crc_fail == 7
    assert counters.arena_high_water_pct == 42


async def test_recovered_blocks_seed_a_session_on_registration() -> None:
    rig = _rig(shm_crosscheck=False)
    spec = _spec(total_blocks=3)
    rig.aggregator.register_session(spec, decoded=[BlockId(0), BlockId(1)])

    rig.aggregator.handle_block_decoded(R0, spec.session_id, [2])
    await rig.aggregator.poll()

    assert len(rig.completed) == 1
