from collections.abc import Collection

from session_manager.domain.ids import BlockId

FULLY_COMPLETE_RATIO = 1.0
NO_LOSS_PCT = 0.0
PERCENT = 100.0


def _decoded_in_range(decoded: Collection[BlockId], total_blocks: int) -> set[BlockId]:
    return {block_id for block_id in decoded if 0 <= block_id < total_blocks}


def is_complete(decoded: Collection[BlockId], total_blocks: int) -> bool:
    return len(_decoded_in_range(decoded, total_blocks)) == total_blocks


def missing_blocks(decoded: Collection[BlockId], total_blocks: int) -> tuple[BlockId, ...]:
    present = _decoded_in_range(decoded, total_blocks)
    return tuple(BlockId(index) for index in range(total_blocks) if BlockId(index) not in present)


def completion_ratio(decoded_count: int, total_blocks: int) -> float:
    if total_blocks == 0:
        return FULLY_COMPLETE_RATIO
    return min(FULLY_COMPLETE_RATIO, decoded_count / total_blocks)


def is_stalled(last_progress_at: float, now: float, timeout_s: float) -> bool:
    return now - last_progress_at >= timeout_s


def observed_loss_pct(symbols_expected: int, symbols_received: int) -> float:
    if symbols_expected <= 0:
        return NO_LOSS_PCT
    lost = symbols_expected - symbols_received
    if lost <= 0:
        return NO_LOSS_PCT
    return PERCENT * lost / symbols_expected
