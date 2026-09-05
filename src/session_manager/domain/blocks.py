from session_manager.domain.ids import BlockId
from session_manager.domain.models import SessionSpec


def block_byte_range(spec: SessionSpec, block_id: BlockId) -> tuple[int, int]:
    """The (offset, length) of `block_id` within the destination file.

    Every block is `spec.block_bytes` wide except possibly the last, which is
    shorter when the file size isn't an exact multiple. This is pure
    arithmetic derived from the manifest -- `BlockDecoded` carries only
    `block_id`, not a byte range, so the journal (and anything else that
    needs one) recomputes it here rather than trusting a value that never
    arrives over the wire.
    """
    offset = block_id * spec.block_bytes
    length = max(min(spec.block_bytes, spec.file_size - offset), 0)
    return offset, length
