"""Pure policy: when a session becomes terminal, and why.

Pure -- no I/O, no clock of its own, no knowledge of shm, sockets or
protobuf. The caller supplies ``now`` from ``time.monotonic()`` and the
``stall_timeout`` from config: there is deliberately no default, because a
silent fallback that only ever fires when the wiring is broken is worse than
a ``TypeError`` at startup.

This module exists to give ``PurgeSession`` a defined trigger condition.
Two of the three reasons were already implicit in the publish/quarantine
path; ``INCOMPLETE`` was the one that was never written down anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

__all__ = [
    "PurgeReason",
    "SessionProgress",
    "is_complete",
    "is_stalled",
    "terminal_reason",
]


class PurgeReason(Enum):
    """Why a session reached a terminal state.

    ``rx.proto``'s ``PurgeSession.reason`` is a free-text ``string``, not an
    enum. These members are an internal vocabulary; the boundary layer
    (``SessionAuthority._PURGE_REASON_WIRE``) maps them to the wire strings
    documented in ``session-manager/docs/RECEIVER_CONTRACT.md`` section 4 --
    ``PUBLISHED`` -> ``"verified"``, ``QUARANTINED`` -> ``"hash_mismatch"``,
    ``INCOMPLETE`` -> ``"incomplete"``. The member ``.value`` strings here
    are *not* the wire form; do not send them.
    """

    PUBLISHED = "published"
    QUARANTINED = "quarantined"
    INCOMPLETE = "incomplete"


@dataclass(frozen=True)
class SessionProgress:
    """Everything the policy needs to know about one session.

    ``last_block_at`` is the monotonic timestamp of the most recent
    ``BlockDecoded`` accepted for this session, or ``None`` if none has
    ever arrived — in which case the stall clock runs from ``opened_at``.
    That case is real: a Manifest can arrive and then nothing else ever
    does, and such a session must still terminate rather than sit open
    forever holding a staging file and a session-table slot.
    """

    total_blocks: int
    decoded_blocks: int
    opened_at: float
    last_block_at: float | None = None

    def __post_init__(self) -> None:
        if self.total_blocks <= 0:
            raise ValueError(f"total_blocks must be positive, got {self.total_blocks}")
        if self.decoded_blocks < 0:
            raise ValueError(f"decoded_blocks must be non-negative, got {self.decoded_blocks}")
        if self.decoded_blocks > self.total_blocks:
            raise ValueError(
                f"decoded_blocks {self.decoded_blocks} exceeds total_blocks {self.total_blocks}"
            )


def is_complete(progress: SessionProgress) -> bool:
    """Every block decoded. Says nothing about integrity."""
    return progress.decoded_blocks == progress.total_blocks


def is_stalled(progress: SessionProgress, now: float, stall_timeout: float) -> bool:
    """No forward progress for ``stall_timeout`` seconds.

    A complete session is never stalled — it has nowhere left to progress
    to, and calling it stalled would race the verifier.
    """
    if is_complete(progress):
        return False
    since = progress.last_block_at if progress.last_block_at is not None else progress.opened_at
    return (now - since) >= stall_timeout


def terminal_reason(
    progress: SessionProgress,
    now: float,
    *,
    stall_timeout: float,
    hash_ok: bool | None = None,
) -> PurgeReason | None:
    """The reason to purge, or ``None`` if the session is still live.

    ``hash_ok`` is the BLAKE3-vs-manifest verdict: ``None`` while
    verification has not run or has not finished yet. A complete session
    with ``hash_ok is None`` is deliberately *not* terminal — the caller
    must not purge out from under a running verifier.
    """
    if not is_complete(progress):
        return PurgeReason.INCOMPLETE if is_stalled(progress, now, stall_timeout) else None
    if hash_ok is None:
        return None
    return PurgeReason.PUBLISHED if hash_ok else PurgeReason.QUARANTINED
