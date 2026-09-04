import dataclasses

import pytest

from session_manager.domain.ids import SessionId
from session_manager.domain.models import (
    STATE_NAME_TO_STATE,
    ReceiverCounters,
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
    assert STATE_NAME_TO_STATE["HASH_MISMATCH"] is SessionState.HASH_MISMATCH
    assert set(STATE_NAME_TO_STATE) == {state.name for state in SessionState}
