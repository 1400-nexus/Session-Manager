"""Domain models for session reassembly.

`SessionSnapshot.missing_blocks` is a capped preview; the full list is the
recovery instruction on a link with no back channel and stays exactly
recomputable from `progress.missing_blocks`, so it is not carried in a
snapshot rebuilt several times a second.
"""

from collections.abc import Collection
from dataclasses import dataclass, fields
from enum import Enum, auto

from session_manager.domain.ids import BlockId, ReceiverId, SessionId

MISSING_BLOCKS_PREVIEW_LIMIT = 32

_PEAK_GAUGE_FIELDS: frozenset[str] = frozenset({"arena_high_water_pct"})


class SessionState(Enum):
    OPEN = auto()
    COMPLETE = auto()
    VERIFIED = auto()
    HASH_MISMATCH = auto()
    INCOMPLETE = auto()
    FAILED = auto()


SESSION_STATE_BY_WIRE_NAME: dict[str, SessionState] = {state.name: state for state in SessionState}


@dataclass(frozen=True)
class SessionSpec:
    session_id: SessionId
    relpath: str
    file_size: int
    file_hash: bytes
    k: int
    n: int
    symbol_bytes: int
    total_blocks: int

    @property
    def block_bytes(self) -> int:
        return self.k * self.symbol_bytes


@dataclass(frozen=True)
class ReceiverCounters:
    pkts_ok: int = 0
    crc_fail: int = 0
    bad_magic: int = 0
    unparsable: int = 0
    duplicates: int = 0
    no_session: int = 0
    arena_exhausted: int = 0
    kernel_drops: int = 0
    arena_high_water_pct: int = 0

    def __add__(self, other: "ReceiverCounters") -> "ReceiverCounters":
        combined: dict[str, int] = {}
        for field in fields(self):
            left: int = getattr(self, field.name)
            right: int = getattr(other, field.name)
            if field.name in _PEAK_GAUGE_FIELDS:
                combined[field.name] = max(left, right)
            else:
                combined[field.name] = left + right
        return ReceiverCounters(**combined)


def sum_counters(per_receiver: Collection["ReceiverCounters"]) -> "ReceiverCounters":
    total = ReceiverCounters()
    for counters in per_receiver:
        total = total + counters
    return total


@dataclass(frozen=True)
class SessionSnapshot:
    spec: SessionSpec
    state: SessionState
    blocks_decoded: int
    total_blocks: int
    observed_loss_pct: float
    per_receiver: tuple[tuple[ReceiverId, ReceiverCounters], ...]
    live_receivers: frozenset[ReceiverId]
    missing_blocks: tuple[BlockId, ...]
    missing_block_count: int

    def counters_for(self, receiver_id: ReceiverId) -> ReceiverCounters | None:
        for candidate_id, counters in self.per_receiver:
            if candidate_id == receiver_id:
                return counters
        return None
