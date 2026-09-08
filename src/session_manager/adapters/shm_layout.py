"""Completion shared-memory segment layout.

Cross-language contract: the C++ receivers `struct`-parse the header from the
same bytes this module packs, so the format string, field order, magic, and
version are fixed. `SHM_HEADER_FORMAT` is explicit little-endian and standard
size (no alignment padding) precisely so that a native ordering which happens
to match on today's build machine is not mistaken for the contract.

Header fields, in order: magic (4 bytes), version (u32), boot_id (16 bytes),
owner_pid (u32), slot_bytes (u32), slot_count (u32), session_table_offset
(u64). Validity is magic + version only; boot_id / owner_pid are for spotting
a segment left by a previous boot.

`slot_bytes` and `slot_count` are a slot model the manager does not use --
not the receiver's slot geometry, do not read or derive from them; removed in
the next shm header revision. The fields a receiver may rely on are magic,
version, and session_table_offset.
"""

import struct
from dataclasses import dataclass
from enum import Enum, auto

from session_manager.adapters.constants import (
    SHM_HEADER_FORMAT,
    SHM_MAGIC,
    SHM_SESSION_ENTRY_FORMAT,
    SHM_SESSION_ID_BYTES,
    SHM_SESSION_TABLE_OFFSET,
    SHM_VERSION,
)
from session_manager.domain.ids import SessionId
from session_manager.domain.models import OpenSession

HEADER_SIZE = struct.calcsize(SHM_HEADER_FORMAT)

# Session table at SHM_SESSION_TABLE_OFFSET: a u32 count followed by that many
# fixed-size entries. Also a cross-language structure the C++ receivers read.
SESSION_COUNT_FORMAT = "<I"
SESSION_COUNT_SIZE = struct.calcsize(SESSION_COUNT_FORMAT)
SESSION_ENTRY_SIZE = struct.calcsize(SHM_SESSION_ENTRY_FORMAT)


class AdoptDecision(Enum):
    CREATED = auto()
    ADOPTED = auto()
    REINITIALISED = auto()


@dataclass(frozen=True)
class SegmentHeader:
    magic: bytes
    version: int
    boot_id: bytes
    owner_pid: int
    # slot_bytes / slot_count: a slot model the manager does not use -- not the
    # receiver's slot geometry, do not read or derive from them; removed in the
    # next shm header revision.
    slot_bytes: int
    slot_count: int
    session_table_offset: int

    @property
    def is_valid(self) -> bool:
        return self.magic == SHM_MAGIC and self.version == SHM_VERSION


def build_header(boot_id: bytes, owner_pid: int, slot_bytes: int, slot_count: int) -> bytes:
    return struct.pack(
        SHM_HEADER_FORMAT,
        SHM_MAGIC,
        SHM_VERSION,
        boot_id,
        owner_pid,
        slot_bytes,
        slot_count,
        SHM_SESSION_TABLE_OFFSET,
    )


def parse_header(raw: bytes | memoryview) -> SegmentHeader:
    if len(raw) < HEADER_SIZE:
        return SegmentHeader(b"", 0, b"", 0, 0, 0, 0)
    fields = struct.unpack(SHM_HEADER_FORMAT, bytes(raw[:HEADER_SIZE]))
    return SegmentHeader(*fields)


def header_is_valid(raw: bytes | memoryview) -> bool:
    return parse_header(raw).is_valid


def pack_session_table(sessions: tuple[OpenSession, ...]) -> bytes:
    packed = struct.pack(SESSION_COUNT_FORMAT, len(sessions))
    for session in sessions:
        packed += struct.pack(
            SHM_SESSION_ENTRY_FORMAT,
            session.session_id.encode()[:SHM_SESSION_ID_BYTES],
            session.total_blocks,
            session.block_table_offset,
            session.bitmap_offset,
        )
    return packed


def unpack_session_table(raw: bytes | memoryview) -> tuple[OpenSession, ...]:
    if len(raw) < SESSION_COUNT_SIZE:
        return ()
    (count,) = struct.unpack(SESSION_COUNT_FORMAT, bytes(raw[:SESSION_COUNT_SIZE]))
    sessions: list[OpenSession] = []
    cursor = SESSION_COUNT_SIZE
    for _ in range(count):
        entry = bytes(raw[cursor : cursor + SESSION_ENTRY_SIZE])
        if len(entry) < SESSION_ENTRY_SIZE:
            break
        raw_id, total_blocks, block_table_offset, bitmap_offset = struct.unpack(
            SHM_SESSION_ENTRY_FORMAT, entry
        )
        sessions.append(
            OpenSession(
                session_id=SessionId(raw_id.rstrip(b"\x00").decode()),
                total_blocks=total_blocks,
                block_table_offset=block_table_offset,
                bitmap_offset=bitmap_offset,
            )
        )
        cursor += SESSION_ENTRY_SIZE
    return tuple(sessions)
