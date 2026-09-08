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


def _rig(*, shm_crosscheck: bool = True) -> _Rig:
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
        shm_crosscheck=shm_crosscheck,
    )
    return _Rig(aggregator, clock, shm, completed)


def _init_shm_session(shm: FakeShm, spec: SessionSpec) -> None:
    shm.create_or_adopt("seg", 4096)
    shm.init_session(spec, block_table_offset=64, bitmap_offset=256)


async def test_duplicate_block_decoded_is_idempotent_and_does_not_advance_the_stall_clock() -> None:
    rig = _rig(shm_crosscheck=False)
    spec = _spec(total_blocks=5)
    rig.aggregator.register_session(spec)  # last_progress_at = 0.0

    rig.aggregator.handle_block_decoded(R0, spec.session_id, [0, 1])  # real growth -> stamps
    rig.clock.advance(5.0)
    rig.aggregator.handle_block_decoded(R0, spec.session_id, [0, 1])  # duplicate -> no stamp
    rig.aggregator.handle_block_decoded(R1, spec.session_id, [1])  # overlap -> no stamp
    await rig.aggregator.poll()

    count, last_progress_at, missing = rig.aggregator.progress_of(spec.session_id) or (0, -1.0, ())
    assert count == 2  # deduped
    assert missing == (BlockId(2), BlockId(3), BlockId(4))
    assert last_progress_at == 0.0  # the real growth at t=0, not the t=5 duplicates
    snapshot = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot is not None
    assert snapshot.blocks_decoded == 2
    assert snapshot.state is SessionState.OPEN


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

    rig.aggregator.handle_block_decoded(R0, spec.session_id, [5, 99])
    await rig.aggregator.poll()

    snapshot = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot is not None
    assert snapshot.blocks_decoded == 0
    assert snapshot.state is SessionState.OPEN


async def test_progress_of_reports_count_last_progress_time_and_a_capped_preview() -> None:
    rig = _rig(shm_crosscheck=False)
    spec = _spec(total_blocks=5)
    rig.clock.advance(3.0)
    rig.aggregator.register_session(spec)  # last_progress_at = 3.0
    rig.clock.advance(4.0)
    rig.aggregator.handle_block_decoded(R0, spec.session_id, [0, 2])  # growth -> 7.0

    assert rig.aggregator.progress_of(spec.session_id) == (
        2,
        7.0,
        (BlockId(1), BlockId(3), BlockId(4)),
    )


async def test_a_recovered_session_last_progress_is_the_registration_instant_not_zero() -> None:
    # The adopt-safety mechanism: register_session runs during the authority's
    # _recover(), so the recovered session's stall clock starts fresh. A naive
    # last_progress_at (0.0, or restored from before the restart) would have
    # the authority's sweep purge every live transfer immediately.
    rig = _rig(shm_crosscheck=False)
    spec = _spec(total_blocks=5)
    rig.clock.advance(10_000.0)  # a long-running manager, or a monotonic clock

    rig.aggregator.register_session(spec, decoded=[BlockId(0), BlockId(1)])

    result = rig.aggregator.progress_of(spec.session_id)
    assert result is not None
    _count, last_progress_at, _missing = result
    assert last_progress_at == 10_000.0


async def test_progress_of_an_unregistered_session_is_none() -> None:
    rig = _rig(shm_crosscheck=False)

    assert rig.aggregator.progress_of(SessionId("ghost")) is None


async def test_mark_incomplete_freezes_the_snapshot_at_incomplete() -> None:
    rig = _rig(shm_crosscheck=False)
    spec = _spec(total_blocks=5)
    rig.aggregator.register_session(spec)
    rig.aggregator.handle_block_decoded(R0, spec.session_id, [0, 2])
    await rig.aggregator.poll()

    rig.aggregator.mark_incomplete(spec.session_id)

    snapshot = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot is not None
    assert snapshot.state is SessionState.INCOMPLETE

    rig.aggregator.handle_block_decoded(R0, spec.session_id, [1, 3, 4])
    await rig.aggregator.poll()  # a terminal session is not rebuilt

    snapshot_after = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot_after is not None
    assert snapshot_after.state is SessionState.INCOMPLETE
    assert rig.completed == []


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


async def test_a_persistent_divergence_warns_exactly_once_per_session() -> None:
    rig = _rig(shm_crosscheck=True)
    spec = _spec(total_blocks=8)
    _init_shm_session(rig.shm, spec)  # bitmap stays empty -- nothing marks it
    rig.aggregator.register_session(spec)

    with capture_logs() as logs:
        for block_id in range(5):
            # UDS count climbs every poll; the bitmap count never leaves 0.
            rig.aggregator.handle_block_decoded(R0, spec.session_id, [block_id])
            await rig.aggregator.poll()

    divergences = [entry for entry in logs if entry["event"] == "shm_bitmap_diverges_from_uds"]
    assert len(divergences) == 1


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
        R1, _stats(1, pkts_ok=500, crc_fail=7, arena_high_water_pct=42)
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


def test_spec_for_returns_the_registered_spec() -> None:
    rig = _rig(shm_crosscheck=False)
    spec = _spec(total_blocks=3)
    rig.aggregator.register_session(spec)

    assert rig.aggregator.spec_for(spec.session_id) is spec


def test_spec_for_an_unknown_session_is_none() -> None:
    rig = _rig(shm_crosscheck=False)

    assert rig.aggregator.spec_for(SessionId("ghost")) is None


async def test_snapshots_returns_every_session_after_a_poll() -> None:
    rig = _rig(shm_crosscheck=False)
    spec_a = _spec("s-1", total_blocks=3)
    spec_b = _spec("s-2", total_blocks=3)
    rig.aggregator.register_session(spec_a)
    rig.aggregator.register_session(spec_b)

    await rig.aggregator.poll()

    session_ids = {snapshot.spec.session_id for snapshot in rig.aggregator.snapshots()}
    assert session_ids == {spec_a.session_id, spec_b.session_id}


async def test_mark_verified_sets_a_terminal_state_further_polls_do_not_revert() -> None:
    rig = _rig(shm_crosscheck=False)
    spec = _spec(total_blocks=3)
    rig.aggregator.register_session(spec)
    rig.aggregator.handle_block_decoded(R0, spec.session_id, [0, 1, 2])
    await rig.aggregator.poll()
    assert len(rig.completed) == 1

    rig.aggregator.mark_verified(spec.session_id)

    snapshot = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot is not None
    assert snapshot.state is SessionState.VERIFIED

    await rig.aggregator.poll()  # must not rebuild the snapshot back to COMPLETE

    snapshot_after = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot_after is not None
    assert snapshot_after.state is SessionState.VERIFIED
    assert len(rig.completed) == 1


async def test_mark_hash_mismatch_sets_a_terminal_state() -> None:
    rig = _rig(shm_crosscheck=False)
    spec = _spec(total_blocks=3)
    rig.aggregator.register_session(spec)
    rig.aggregator.handle_block_decoded(R0, spec.session_id, [0, 1, 2])
    await rig.aggregator.poll()

    rig.aggregator.mark_hash_mismatch(spec.session_id)

    snapshot = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot is not None
    assert snapshot.state is SessionState.HASH_MISMATCH

    await rig.aggregator.poll()  # must not rebuild the snapshot back to COMPLETE
    snapshot_after = rig.aggregator.snapshot_for(spec.session_id)
    assert snapshot_after is not None
    assert snapshot_after.state is SessionState.HASH_MISMATCH


def test_marking_an_unknown_session_verified_is_a_noop() -> None:
    rig = _rig(shm_crosscheck=False)

    rig.aggregator.mark_verified(SessionId("ghost"))  # must not raise

    assert rig.aggregator.snapshot_for(SessionId("ghost")) is None
