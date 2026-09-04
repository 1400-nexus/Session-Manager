from collections.abc import Collection
from dataclasses import dataclass, fields
from enum import Enum, auto

from session_manager.domain.ids import BlockId, ReceiverId, SessionId


class SessionState(Enum):
    OPEN = auto()
    COMPLETE = auto()
    VERIFIED = auto()
    HASH_MISMATCH = auto()
    INCOMPLETE = auto()
    FAILED = auto()


# rx_pb2 at the pinned contract (cef65a6) has no SessionStatus.State enum, so
# the boundary layer (Step 7) must translate by name against this table until
# the proto gains one. When it does, delete this and translate by
# SessionState[wire_name].
STATE_NAME_TO_STATE: dict[str, SessionState] = {state.name: state for state in SessionState}


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
        # The wire field common.Manifest.block_bytes actually carries the
        # SYMBOL size, not the block size -- the boundary layer copies it into
        # `symbol_bytes`. The real per-block byte count is k symbols wide.
        return self.k * self.symbol_bytes


# ReceiverStats.arena_high_water_pct is a gauge (a peak fill percentage), not
# a monotonic counter -- aggregating it across receivers is a max, not a sum.
_GAUGE_FIELD_NAME = "arena_high_water_pct"


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


# On a link with no back channel the missing-block list IS the recovery
# instruction, so it must stay computable exactly -- but the full enumeration
# (over a thousand ids for a large file with a dead receiver) does not belong
# in a snapshot rebuilt several times a second. The snapshot carries only a
# capped preview plus the true count; the exhaustive list is recomputed on
# demand from progress.missing_blocks at final failure reporting.
MISSING_BLOCKS_PREVIEW_LIMIT = 32


@dataclass(frozen=True)
class SessionSnapshot:
    spec: SessionSpec
    state: SessionState
    blocks_decoded: int
    total_blocks: int
    observed_loss_pct: float
    # Sorted by receiver id, and a tuple rather than a dict: `frozen=True` only
    # stops the attribute being rebound, not the container being mutated, and a
    # shared mutable dict is exactly the reach into live state this snapshot
    # exists to prevent (it also leaves __hash__ broken). The aggregator that
    # builds the snapshot is responsible for the sort.
    per_receiver: tuple[tuple[ReceiverId, ReceiverCounters], ...]
    live_receivers: frozenset[ReceiverId]
    missing_blocks: tuple[BlockId, ...]
    missing_block_count: int

    def counters_for(self, receiver_id: ReceiverId) -> ReceiverCounters | None:
        for candidate_id, counters in self.per_receiver:
            if candidate_id == receiver_id:
                return counters
        return None
