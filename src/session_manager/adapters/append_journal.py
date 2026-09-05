import os
import struct
import zlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import BinaryIO

import structlog

from session_manager.adapters.constants import (
    JOURNAL_CRC_FORMAT,
    JOURNAL_FIELDS_FORMAT,
    JOURNAL_FILENAME_SUFFIX,
    JOURNAL_SESSION_ID_BYTES,
    JOURNAL_SYNC_BATCH_SIZE,
)
from session_manager.domain.ids import BlockId, SessionId
from session_manager.domain.paths import is_unsafe_filename_component

logger = structlog.get_logger(__name__)

_FIELDS_SIZE = struct.calcsize(JOURNAL_FIELDS_FORMAT)
_CRC_SIZE = struct.calcsize(JOURNAL_CRC_FORMAT)
_RECORD_SIZE = _FIELDS_SIZE + _CRC_SIZE


def _pack_session_id(session_id: SessionId) -> bytes:
    # 16 bytes is an integrity-check field, not the record's identity -- the
    # file (named by the full session_id) is that. A session_id longer than
    # 16 UTF-8 bytes is truncated here; that only weakens this secondary
    # cross-check against a torn/misplaced record, it can't misfile one.
    return session_id.encode()[:JOURNAL_SESSION_ID_BYTES].ljust(JOURNAL_SESSION_ID_BYTES, b"\x00")


def _pack_record(session_id: SessionId, block_id: BlockId, offset: int, length: int) -> bytes:
    fields = struct.pack(
        JOURNAL_FIELDS_FORMAT, _pack_session_id(session_id), block_id, offset, length
    )
    return fields + struct.pack(JOURNAL_CRC_FORMAT, zlib.crc32(fields))


def _unpack_record(raw: bytes, expected_session_id: SessionId) -> BlockId | None:
    fields_raw = raw[:_FIELDS_SIZE]
    (stored_crc,) = struct.unpack(JOURNAL_CRC_FORMAT, raw[_FIELDS_SIZE:_RECORD_SIZE])
    if zlib.crc32(fields_raw) != stored_crc:
        return None
    session_id_bytes, block_id, _offset, _length = struct.unpack(JOURNAL_FIELDS_FORMAT, fields_raw)
    if session_id_bytes != _pack_session_id(expected_session_id):
        return None
    return BlockId(block_id)


def _durability_sync(handle: BinaryIO) -> None:
    handle.flush()
    file_descriptor = handle.fileno()
    if hasattr(os, "fdatasync"):
        # Linux: skips the metadata sync fsync() also does, which is not
        # needed here since the file size only grows by whole records.
        os.fdatasync(file_descriptor)
    else:
        # Windows (dev platform) and any other platform without fdatasync.
        os.fsync(file_descriptor)


class AppendJournal:
    """Append-only per-session journal of decoded blocks, for restart recovery.

    Destination writes are idempotent -- fixed offset, fixed content -- so
    replaying a record that was already applied is harmless and recovery
    never needs to know exactly where a crash landed. That property is what
    keeps this a small append log instead of a write-ahead protocol.

    Fixed-width binary records, not text: replay is a loop over a known
    stride, and a torn tail (a crash mid-write) is detectable by short length
    or a failed CRC rather than corrupting a parse. One file per session
    under `journal_dir`, named by session_id -- purging a completed session
    is a single unlink.
    """

    def __init__(self, journal_dir: Path, sync_batch_size: int = JOURNAL_SYNC_BATCH_SIZE) -> None:
        self._journal_dir: Path = journal_dir
        self._sync_batch_size: int = sync_batch_size
        self._handles: dict[SessionId, BinaryIO] = {}
        self._pending_since_sync: dict[SessionId, int] = {}

    def append(self, session_id: SessionId, block_id: BlockId, offset: int, length: int) -> None:
        handle = self._handle_for(session_id)
        handle.write(_pack_record(session_id, block_id, offset, length))
        pending = self._pending_since_sync.get(session_id, 0) + 1
        if pending >= self._sync_batch_size:
            self._sync_one(session_id, handle)
        else:
            self._pending_since_sync[session_id] = pending

    async def replay(self, session_id: SessionId) -> AsyncIterator[BlockId]:
        # Flush (not fsync) any of this instance's own buffered writes first,
        # so replay always sees everything appended so far in this process,
        # whether or not sync() was called -- durability and visibility are
        # different guarantees, and only the former needs the sync barrier.
        open_handle = self._handles.get(session_id)
        if open_handle is not None:
            open_handle.flush()

        path = self._path_for(session_id)
        if not path.is_file():
            return

        with open(path, "rb") as file_handle:
            while True:
                raw = file_handle.read(_RECORD_SIZE)
                if len(raw) < _RECORD_SIZE:
                    # A short read is a torn tail from a crash mid-write --
                    # the expected outcome here, not an error.
                    break
                block_id = _unpack_record(raw, session_id)
                if block_id is None:
                    logger.error("journal_record_corrupt", session_id=session_id, path=str(path))
                    break
                yield block_id

    def sync(self) -> None:
        for session_id, handle in self._handles.items():
            self._sync_one(session_id, handle)

    def _handle_for(self, session_id: SessionId) -> BinaryIO:
        handle = self._handles.get(session_id)
        if handle is None:
            path = self._path_for(session_id)
            path.parent.mkdir(parents=True, exist_ok=True)
            handle = open(path, "ab")
            self._handles[session_id] = handle
        return handle

    def _path_for(self, session_id: SessionId) -> Path:
        # session_id becomes a filename here -- the same corrupting-link
        # concern as a relpath, just for a different field (shared check:
        # domain/paths.is_unsafe_filename_component), so it gets its own
        # call rather than trusting that nothing upstream ever forwards an
        # unvalidated session_id.
        if is_unsafe_filename_component(session_id):
            raise ValueError(f"unsafe session_id for journal filename: {session_id!r}")
        return self._journal_dir / f"{session_id}{JOURNAL_FILENAME_SUFFIX}"

    def _sync_one(self, session_id: SessionId, handle: BinaryIO) -> None:
        _durability_sync(handle)
        self._pending_since_sync[session_id] = 0
