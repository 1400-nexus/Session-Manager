"""Completion shared-memory segment layout.

Cross-language contract: the C++ receivers `struct`-parse the header from the
same bytes this module packs, so the format string, field order, magic, and
version are fixed. `SHM_HEADER_FORMAT` is explicit little-endian and standard
size (no alignment padding) precisely so that a native ordering which happens
to match on today's build machine is not mistaken for the contract.

Header fields, in order: magic (4 bytes), version (u32), boot_id (16 bytes),
owner_pid (u32), slot_bytes (u32), slot_count (u32), session_table_offset
(u64). Validity is magic + version only; the rest is informational for
operators and for detecting a segment left by a previous boot.
"""

import struct
from dataclasses import dataclass
from enum import Enum, auto

from session_manager.adapters.constants import (
    SHM_HEADER_FORMAT,
    SHM_MAGIC,
    SHM_SESSION_TABLE_OFFSET,
    SHM_VERSION,
)

HEADER_SIZE = struct.calcsize(SHM_HEADER_FORMAT)


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
