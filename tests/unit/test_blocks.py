from session_manager.domain.blocks import block_byte_range
from session_manager.domain.ids import BlockId, SessionId
from session_manager.domain.models import SessionSpec


def _spec(file_size: int, total_blocks: int) -> SessionSpec:
    return SessionSpec(
        session_id=SessionId("s-1"),
        relpath="f.bin",
        file_size=file_size,
        file_hash=b"\x00" * 32,
        k=2,
        n=3,
        symbol_bytes=100,
        total_blocks=total_blocks,
    )


def test_full_blocks_are_block_bytes_wide() -> None:
    spec = _spec(file_size=1000, total_blocks=5)  # block_bytes = 200
    assert block_byte_range(spec, BlockId(0)) == (0, 200)
    assert block_byte_range(spec, BlockId(2)) == (400, 200)


def test_the_final_partial_block_is_shorter() -> None:
    spec = _spec(file_size=450, total_blocks=3)  # block_bytes = 200: 200 + 200 + 50
    assert block_byte_range(spec, BlockId(2)) == (400, 50)


def test_a_block_id_past_the_file_end_has_zero_length_not_negative() -> None:
    spec = _spec(file_size=450, total_blocks=3)
    assert block_byte_range(spec, BlockId(5)) == (1000, 0)
