from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto

from session_manager.adapters.shm_layout import (
    HEADER_SIZE,
    AdoptDecision,
    build_header,
    header_is_valid,
)
from session_manager.domain.ids import BlockId, SessionId
from session_manager.domain.models import SessionSpec

DEFAULT_FAKE_ARENA_BYTES = 4096

# A non-zero fill for an existing segment's payload so a test can tell "adopt
# left the bytes alone" from "reinitialise zeroed them".
_SENTINEL_BYTE = 0xAB


class SegmentState(Enum):
    ABSENT = auto()
    LIVE = auto()
    STALE = auto()
    INCOMPATIBLE = auto()


@dataclass(frozen=True)
class _SessionRegion:
    block_table_offset: int
    bitmap_offset: int
    bitmap_len: int


def _valid_header() -> bytes:
    return build_header(b"\x00" * 16, 0, 0, 0)


class FakeShm:
    """In-memory ShmReader + ShmWriter over a bytearray.

    One object satisfies both ports, but the reader half still returns a
    read-only view from `bitmap_for` -- a test that gets a writable view has
    not tested the invariant the port split exists to enforce. The four
    starting states (`SegmentState`) plus `set_receiver_alive` are the whole
    difficulty of the adopt-vs-create decision in Step 6.

    Liveness: pass `probe_receiver_alive` to match `PosixShm`'s constructor
    (the shared contract test does); otherwise `create_or_adopt` reads the
    `set_receiver_alive` flag directly.
    """

    def __init__(self, probe_receiver_alive: Callable[[], bool] | None = None) -> None:
        self._buffer: bytearray | None = None
        self._receiver_alive: bool = False
        self._probe: Callable[[], bool] | None = probe_receiver_alive
        self._sessions: dict[SessionId, _SessionRegion] = {}
        self._attached_name: str | None = None
        self.closed: bool = False
        self.unlinked: bool = False
        self.last_decision: AdoptDecision | None = None
        self._pending_create_errors: list[Exception] = []
        self._pending_init_errors: list[Exception] = []

    # --- test-driven setup -------------------------------------------------

    def set_existing_segment(
        self, state: SegmentState, arena_bytes: int = DEFAULT_FAKE_ARENA_BYTES
    ) -> None:
        if state is SegmentState.ABSENT:
            self._buffer = None
            self._receiver_alive = False
            self._sessions.clear()
            return

        buffer = bytearray([_SENTINEL_BYTE]) * arena_bytes
        buffer[:HEADER_SIZE] = _valid_header()
        self._buffer = buffer
        self._sessions.clear()

        if state is SegmentState.INCOMPATIBLE:
            self.corrupt_header()
            self._receiver_alive = False
        else:
            self._receiver_alive = state is SegmentState.LIVE

    def set_receiver_alive(self, alive: bool) -> None:
        self._receiver_alive = alive

    def corrupt_header(self) -> None:
        buffer = self._buffer
        if buffer is None:
            raise RuntimeError("no segment to corrupt")
        buffer[0] ^= 0xFF

    def seed_payload(self, data: bytes) -> None:
        buffer = self._buffer
        if buffer is None:
            raise RuntimeError("no segment to seed")
        end = HEADER_SIZE + len(data)
        if end > len(buffer):
            raise ValueError("payload does not fit the segment")
        buffer[HEADER_SIZE:end] = data

    def fail_next_create_or_adopt(self, error: Exception) -> None:
        self._pending_create_errors.append(error)

    def fail_next_init_session(self, error: Exception) -> None:
        self._pending_init_errors.append(error)

    # --- introspection ---------------------------------------------------

    def current_segment_state(self) -> SegmentState:
        if self._buffer is None:
            return SegmentState.ABSENT
        if not self._header_valid():
            return SegmentState.INCOMPATIBLE
        return SegmentState.LIVE if self.probe_receiver_alive() else SegmentState.STALE

    def probe_receiver_alive(self) -> bool:
        return self._probe() if self._probe is not None else self._receiver_alive

    def payload_bytes(self) -> bytes:
        buffer = self._buffer
        return b"" if buffer is None else bytes(buffer[HEADER_SIZE:])

    def payload_is_zeroed(self) -> bool:
        buffer = self._buffer
        if buffer is None:
            return True
        return all(byte == 0 for byte in buffer[HEADER_SIZE:])

    def mark_block_decoded(self, session_id: SessionId, block_id: BlockId) -> None:
        self._set_bit(self._sessions[session_id].bitmap_offset, block_id)

    def mark_block_table_seen(self, session_id: SessionId, block_id: BlockId) -> None:
        self._set_bit(self._sessions[session_id].block_table_offset, block_id)

    # --- ShmWriter ------------------------------------------------------

    def create_or_adopt(self, name: str, arena_bytes: int) -> bool:
        if self._pending_create_errors:
            raise self._pending_create_errors.pop(0)
        self._attached_name = name

        if self._buffer is None:
            fresh = bytearray(arena_bytes)
            fresh[:HEADER_SIZE] = _valid_header()
            self._buffer = fresh
            self.last_decision = AdoptDecision.CREATED
            return False

        if not self._header_valid():
            self._reinitialise()
            return False

        if self.probe_receiver_alive():
            self.last_decision = AdoptDecision.ADOPTED
            return True

        self._reinitialise()
        return False

    def init_session(self, spec: SessionSpec, block_table_offset: int, bitmap_offset: int) -> None:
        if self._pending_init_errors:
            raise self._pending_init_errors.pop(0)
        buffer = self._buffer
        if buffer is None:
            raise RuntimeError("create_or_adopt must run before init_session")

        bitmap_len = (spec.total_blocks + 7) // 8
        if bitmap_offset + bitmap_len > len(buffer):
            raise ValueError(
                f"session {spec.session_id} bitmap [{bitmap_offset}, "
                f"{bitmap_offset + bitmap_len}) does not fit an arena of {len(buffer)}"
            )

        self._sessions[spec.session_id] = _SessionRegion(
            block_table_offset=block_table_offset,
            bitmap_offset=bitmap_offset,
            bitmap_len=bitmap_len,
        )
        buffer[bitmap_offset : bitmap_offset + bitmap_len] = bytes(bitmap_len)

    def purge_session(self, session_id: SessionId) -> None:
        self._sessions.pop(session_id, None)

    def close(self, unlink: bool) -> None:
        self.closed = True
        if unlink:
            self.unlinked = True
            self._buffer = None
            self._sessions.clear()

    # --- ShmReader ----------------------------------------------------

    def attach(self, name: str) -> None:
        self._attached_name = name

    def bitmap_for(self, session_id: SessionId) -> memoryview:
        buffer = self._buffer
        if buffer is None:
            raise RuntimeError("no segment attached")
        region = self._sessions[session_id]
        window = memoryview(buffer)[region.bitmap_offset : region.bitmap_offset + region.bitmap_len]
        return window.toreadonly()

    def block_table_seen(self, session_id: SessionId, block_id: BlockId) -> bool:
        buffer = self._buffer
        if buffer is None:
            raise RuntimeError("no segment attached")
        region = self._sessions[session_id]
        byte_index = region.block_table_offset + block_id // 8
        return bool(buffer[byte_index] & (1 << (block_id % 8)))

    # --- internals ------------------------------------------------

    def _header_valid(self) -> bool:
        buffer = self._buffer
        return buffer is not None and header_is_valid(bytes(buffer[:HEADER_SIZE]))

    def _reinitialise(self) -> None:
        buffer = self._buffer
        if buffer is None:
            raise RuntimeError("reinitialise with no segment")
        buffer[HEADER_SIZE:] = bytes(len(buffer) - HEADER_SIZE)
        buffer[:HEADER_SIZE] = _valid_header()
        self._sessions.clear()
        self.last_decision = AdoptDecision.REINITIALISED

    def _set_bit(self, base_offset: int, block_id: BlockId) -> None:
        buffer = self._buffer
        if buffer is None:
            raise RuntimeError("no segment attached")
        buffer[base_offset + block_id // 8] |= 1 << (block_id % 8)
