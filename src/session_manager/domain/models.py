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

# ReceiverStats.arena_high_water_pct is a gauge (a peak fill percentage), not
# a monotonic counter -- aggregating it across receivers is a max, not a sum.
_GAUGE_FIELD_NAME = "arena_high_water_pct"


class SessionState(Enum):
    OPEN = auto()
    COMPLETE = auto()
    VERIFIED = auto()
    HASH_MISMATCH = auto()
    INCOMPLETE = auto()
    FAILED = auto()


# rx_pb2 at the pinned contract has no SessionStatus.State enum, so the
# boundary layer translates by name against this table until the proto gains
# one. When it does, delete this and translate by SessionState[wire_name].
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
        # The wire field common.Manifest.block_bytes carries the SYMBOL size,
        # not the block size -- the boundary layer copies it into `symbol_bytes`.
        # The real per-block byte count is k symbols wide.
        return self.k * self.symbol_bytes


@dataclass(frozen=True)
class OpenSession:
    session_id: SessionId
    total_blocks: int
    block_table_offset: int
    bitmap_offset: int


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
            if field.name == _GAUGE_FIELD_NAME:
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
    # A sorted tuple, not a dict: `frozen=True` stops rebinding, not mutation,
    # and a shared mutable container is the reach into live state this snapshot
    # exists to prevent.
    per_receiver: tuple[tuple[ReceiverId, ReceiverCounters], ...]
    live_receivers: frozenset[ReceiverId]
    missing_blocks: tuple[BlockId, ...]
    missing_block_count: int
    # Seconds since the last BlockDecoded that grew this session's decoded
    # set (or since it was opened, if none has). The status display styles
    # its idle column off this; the authority's stall decision keys off the
    # underlying timestamp, not this derived value.
    seconds_since_progress: float

    def counters_for(self, receiver_id: ReceiverId) -> ReceiverCounters | None:
        for candidate_id, counters in self.per_receiver:
            if candidate_id == receiver_id:
                return counters
        return None
