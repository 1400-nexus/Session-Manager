import os
from collections.abc import Callable
from dataclasses import dataclass
from multiprocessing import resource_tracker, shared_memory

from session_manager.adapters.shm_layout import (
    HEADER_SIZE,
    AdoptDecision,
    build_header,
    header_is_valid,
)
from session_manager.domain.ids import BlockId, SessionId
from session_manager.domain.models import SessionSpec

_BOOT_ID_BYTES = 16


def detach_resource_tracker(segment: shared_memory.SharedMemory) -> None:
    # multiprocessing.shared_memory registers every segment with the
    # resource_tracker, which unlinks it -- with a warning -- when THIS
    # process exits, even a clean exit. That is exactly the "manager restart
    # wiped the segment a receiver was holding" failure this service exists to
    # prevent, so unregister and let only an explicit close(unlink=True)
    # remove it. POSIX-only: on Windows SharedMemory is refcounted by the OS
    # and never registered, and unregister() there just warns about an unknown
    # resource. posix_ipc would avoid the private-attribute access below, but
    # it is a non-stdlib, POSIX-only dependency that would break the Windows
    # dev loop.
    if os.name != "posix":
        return
    # `_name` (not the public `.name`, which strips the leading slash the
    # tracker registered the segment under). typeshed does not expose it.
    tracked_name: str = getattr(segment, "_name")
    try:
        resource_tracker.unregister(tracked_name, "shared_memory")
    except (OSError, KeyError):
        pass


@dataclass(frozen=True)
class _SessionRegion:
    block_table_offset: int
    bitmap_offset: int
    bitmap_len: int


class PosixShm:
    """POSIX shared-memory implementation of ShmReader + ShmWriter.

    Liveness probing is not this adapter's job: it takes
    `probe_receiver_alive` and the composition root wires that to the UDS
    server, so the adapter never imports a socket and stays testable. The
    adopt-vs-create decision it makes is the one `FakeShm` already pins, and
    the shared contract test suite runs both against the same cases.
    """

    def __init__(self, slot_bytes: int, probe_receiver_alive: Callable[[], bool]) -> None:
        self._slot_bytes: int = slot_bytes
        self._probe_receiver_alive: Callable[[], bool] = probe_receiver_alive
        self._boot_id: bytes = os.urandom(_BOOT_ID_BYTES)
        self._segment: shared_memory.SharedMemory | None = None
        self._name: str | None = None
        self._sessions: dict[SessionId, _SessionRegion] = {}
        self._exported_views: list[memoryview] = []
        self.last_decision: AdoptDecision | None = None

    # --- ShmWriter -----------------------------------------------------

    def create_or_adopt(self, name: str, arena_bytes: int) -> bool:
        self._name = name
        existing = self._open(name)

        if existing is None:
            self._segment = self._create(name, arena_bytes)
            self.last_decision = AdoptDecision.CREATED
            return False

        self._segment = existing
        if not header_is_valid(self._buffer()):
            self._reinitialise()
            self.last_decision = AdoptDecision.REINITIALISED
            return False

        if self._probe_receiver_alive():
            self.last_decision = AdoptDecision.ADOPTED
            return True

        self._reinitialise()
        self.last_decision = AdoptDecision.REINITIALISED
        return False

    def init_session(self, spec: SessionSpec, block_table_offset: int, bitmap_offset: int) -> None:
        buffer = self._buffer()
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
        for view in self._exported_views:
            view.release()
        self._exported_views.clear()
        segment = self._segment
        if segment is None:
            return
        segment.close()
        if unlink:
            segment.unlink()
        self._segment = None

    # --- ShmReader ---------------------------------------------------

    def attach(self, name: str) -> None:
        if self._segment is not None:
            return
        self._name = name
        opened = self._open(name)
        if opened is None:
            raise FileNotFoundError(name)
        self._segment = opened

    def bitmap_for(self, session_id: SessionId) -> memoryview:
        buffer = self._buffer()
        region = self._sessions[session_id]
        readonly_view = buffer[
            region.bitmap_offset : region.bitmap_offset + region.bitmap_len
        ].toreadonly()
        self._exported_views.append(readonly_view)
        return readonly_view

    def block_table_seen(self, session_id: SessionId, block_id: BlockId) -> bool:
        buffer = self._buffer()
        region = self._sessions[session_id]
        byte_index = region.block_table_offset + block_id // 8
        return bool(buffer[byte_index] & (1 << (block_id % 8)))

    # --- internals ----------------------------------------------

    def _open(self, name: str) -> shared_memory.SharedMemory | None:
        try:
            existing = shared_memory.SharedMemory(name=name, create=False)
        except FileNotFoundError:
            return None
        detach_resource_tracker(existing)
        return existing

    def _create(self, name: str, arena_bytes: int) -> shared_memory.SharedMemory:
        segment = shared_memory.SharedMemory(name=name, create=True, size=arena_bytes)
        detach_resource_tracker(segment)
        self._write_header(self._buffer_of(segment))
        return segment

    def _reinitialise(self) -> None:
        buffer = self._buffer()
        buffer[HEADER_SIZE:] = bytes(len(buffer) - HEADER_SIZE)
        self._write_header(buffer)
        self._sessions.clear()

    def _write_header(self, buffer: memoryview) -> None:
        slot_count = len(buffer) // self._slot_bytes if self._slot_bytes else 0
        buffer[:HEADER_SIZE] = build_header(
            self._boot_id, os.getpid(), self._slot_bytes, slot_count
        )

    def _buffer(self) -> memoryview:
        if self._segment is None:
            raise RuntimeError("no segment: call create_or_adopt or attach first")
        return self._buffer_of(self._segment)

    @staticmethod
    def _buffer_of(segment: shared_memory.SharedMemory) -> memoryview:
        buffer = segment.buf
        if buffer is None:
            raise RuntimeError("segment buffer is closed")
        return buffer
