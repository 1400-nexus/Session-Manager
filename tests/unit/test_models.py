import dataclasses

import pytest

from session_manager.domain.ids import BlockId, ReceiverId, SessionId
from session_manager.domain.models import (
    SESSION_STATE_BY_WIRE_NAME,
    ReceiverCounters,
    SessionSnapshot,
    SessionSpec,
    SessionState,
    sum_counters,
)


def _spec(**overrides: object) -> SessionSpec:
    base: dict[str, object] = {
        "session_id": SessionId("s-1"),
        "relpath": "sub/dir/file.bin",
        "file_size": 1_000_000,
        "file_hash": b"\x00" * 32,
        "k": 200,
        "n": 255,
        "symbol_bytes": 1400,
        "total_blocks": 4,
    }
    base.update(overrides)
    return SessionSpec(**base)  # type: ignore[arg-type]


def test_session_spec_block_bytes_is_k_times_symbol_bytes() -> None:
    assert _spec(k=200, symbol_bytes=1400).block_bytes == 280_000


def test_session_spec_is_frozen() -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        _spec().k = 5  # type: ignore[misc]


def test_receiver_counters_aggregate_field_wise_over_three_receivers() -> None:
    a = ReceiverCounters(pkts_ok=100, crc_fail=1, duplicates=2, kernel_drops=3)
    b = ReceiverCounters(pkts_ok=200, crc_fail=4, duplicates=0, kernel_drops=1)
    c = ReceiverCounters(pkts_ok=50, crc_fail=0, duplicates=5, kernel_drops=0)

    total = sum_counters([a, b, c])

    assert total.pkts_ok == 350
    assert total.crc_fail == 5
    assert total.duplicates == 7
    assert total.kernel_drops == 4


def test_receiver_counters_arena_high_water_is_a_max_not_a_sum() -> None:
    a = ReceiverCounters(arena_high_water_pct=40)
    b = ReceiverCounters(arena_high_water_pct=70)
    c = ReceiverCounters(arena_high_water_pct=55)

    assert sum_counters([a, b, c]).arena_high_water_pct == 70


def test_sum_counters_of_nothing_is_the_zero_value() -> None:
    assert sum_counters([]) == ReceiverCounters()


def test_state_name_table_covers_every_state() -> None:
    assert SESSION_STATE_BY_WIRE_NAME["HASH_MISMATCH"] is SessionState.HASH_MISMATCH
    assert set(SESSION_STATE_BY_WIRE_NAME) == {state.name for state in SessionState}


def _snapshot(**overrides: object) -> SessionSnapshot:
    base: dict[str, object] = {
        "spec": _spec(),
        "state": SessionState.OPEN,
        "blocks_decoded": 2,
        "total_blocks": 4,
        "observed_loss_pct": 1.5,
        "per_receiver": (
            (ReceiverId(0), ReceiverCounters(pkts_ok=10)),
            (ReceiverId(1), ReceiverCounters(pkts_ok=20)),
        ),
        "live_receivers": frozenset({ReceiverId(0), ReceiverId(1)}),
        "missing_blocks": (BlockId(2), BlockId(3)),
        "missing_block_count": 2,
        "seconds_since_progress": 0.0,
    }
    base.update(overrides)
    return SessionSnapshot(**base)  # type: ignore[arg-type]


def test_session_snapshot_is_hashable_now_that_it_holds_no_dict() -> None:
    assert hash(_snapshot()) == hash(_snapshot())


def test_session_snapshot_counters_for_looks_up_by_receiver_id() -> None:
    snapshot = _snapshot()
    assert snapshot.counters_for(ReceiverId(1)) == ReceiverCounters(pkts_ok=20)


def test_session_snapshot_counters_for_unknown_receiver_is_none() -> None:
    assert _snapshot().counters_for(ReceiverId(99)) is None


def test_session_snapshot_missing_block_count_can_exceed_the_previewed_ids() -> None:
    snapshot = _snapshot(missing_blocks=(BlockId(0),), missing_block_count=1278)
    assert len(snapshot.missing_blocks) == 1
    assert snapshot.missing_block_count == 1278
