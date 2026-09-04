from session_manager.domain.ids import BlockId
from session_manager.domain.progress import (
    completion_ratio,
    is_complete,
    is_stalled,
    missing_blocks,
    observed_loss_pct,
)


def _blocks(*indices: int) -> list[BlockId]:
    return [BlockId(index) for index in indices]


def test_is_complete_at_exactly_total_blocks() -> None:
    assert is_complete(_blocks(0, 1, 2), total_blocks=3)


def test_is_complete_one_block_short() -> None:
    assert not is_complete(_blocks(0, 1), total_blocks=3)


def test_is_complete_tolerates_duplicates_and_out_of_range_ids() -> None:
    assert is_complete(_blocks(0, 0, 1, 2, 2, 99), total_blocks=3)


def test_is_complete_zero_total_blocks_is_complete() -> None:
    assert is_complete([], total_blocks=0)


def test_missing_blocks_returns_sorted_ids_from_a_sparse_set() -> None:
    assert missing_blocks(_blocks(5, 1, 3), total_blocks=6) == (
        BlockId(0),
        BlockId(2),
        BlockId(4),
    )


def test_missing_blocks_none_missing() -> None:
    assert missing_blocks(_blocks(0, 1, 2), total_blocks=3) == ()


def test_missing_blocks_zero_total_blocks() -> None:
    assert missing_blocks([], total_blocks=0) == ()


def test_completion_ratio_partial() -> None:
    assert completion_ratio(1, 4) == 0.25


def test_completion_ratio_zero_total_blocks_does_not_divide() -> None:
    assert completion_ratio(0, 0) == 1.0


def test_completion_ratio_clamps_when_count_exceeds_total() -> None:
    assert completion_ratio(5, 4) == 1.0


def test_is_stalled_fires_exactly_at_the_boundary() -> None:
    assert is_stalled(last_progress_at=10.0, now=18.0, timeout_s=8.0)


def test_is_stalled_not_one_tick_early() -> None:
    assert not is_stalled(last_progress_at=10.0, now=17.999, timeout_s=8.0)


def test_observed_loss_pct_normal() -> None:
    assert observed_loss_pct(symbols_expected=1000, symbols_received=900) == 10.0


def test_observed_loss_pct_clamps_to_zero_when_more_arrive_than_expected() -> None:
    assert observed_loss_pct(symbols_expected=1000, symbols_received=1200) == 0.0


def test_observed_loss_pct_zero_expected_does_not_divide() -> None:
    assert observed_loss_pct(symbols_expected=0, symbols_received=0) == 0.0
