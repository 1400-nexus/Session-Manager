from collections.abc import Awaitable, Callable, Collection, Iterable
from dataclasses import dataclass, replace
from typing import Any

import structlog

from session_manager.domain.ids import BlockId, ReceiverId, SessionId
from session_manager.domain.models import (
    MISSING_BLOCKS_PREVIEW_LIMIT,
    ReceiverCounters,
    SessionSnapshot,
    SessionSpec,
    SessionState,
)
from session_manager.domain.progress import (
    is_complete,
    missing_blocks,
    observed_loss_pct,
)
from session_manager.ports.protocols import Clock, ShmReader

logger = structlog.get_logger(__name__)

OnComplete = Callable[[SessionSnapshot], Awaitable[None]]
LiveReceivers = Callable[[], frozenset[ReceiverId]]

# Once a session reaches one of these, poll() must stop rebuilding its
# snapshot from is_complete() -- that logic only ever produces OPEN/COMPLETE,
# so recomputing after a terminal mark (verified / hash_mismatch / the
# authority's incomplete) would silently revert the outcome to COMPLETE (or
# OPEN) on the very next poll. INCOMPLETE is set only by mark_incomplete(),
# from the authority's sweep -- the aggregator no longer detects stalls
# itself (SessionAuthority.terminal_reason() is the single decision point).
_TERMINAL_STATES = frozenset(
    {SessionState.VERIFIED, SessionState.HASH_MISMATCH, SessionState.INCOMPLETE}
)


def _stats_to_counters(stats: Any) -> ReceiverCounters:
    return ReceiverCounters(
        pkts_ok=stats.pkts_ok,
        crc_fail=stats.crc_fail,
        bad_magic=stats.bad_magic,
        unparsable=stats.unparsable,
        duplicates=stats.duplicates,
        no_session=stats.no_session,
        arena_exhausted=stats.arena_exhausted,
        kernel_drops=stats.kernel_drops,
        arena_high_water_pct=stats.arena_high_water_pct,
    )


@dataclass
class _SessionProgress:
    spec: SessionSpec
    decoded: set[BlockId]
    # Monotonic time of the last BlockDecoded that grew `decoded` -- or of
    # registration, if none has. On an adopting restart, registration
    # happens at the adoption instant, so a recovered session's stall clock
    # starts fresh for free. The authority's sweep reads this via
    # `progress_of`; the status display shows how long a session has been
    # idle relative to it.
    last_progress_at: float
    state: SessionState = SessionState.OPEN
    # Set alongside a HASH_MISMATCH / INCOMPLETE mark: the path the partial
    # was quarantined to. Carried on the snapshot so the display can show it.
    quarantine_path: str | None = None


class ProgressAggregator:
    """Folds `BlockDecoded` into per-session completion state.

    This is the authoritative record -- the UDS stream, not the shm bitmap.
    The aggregator holds `ShmReader` only, never `ShmWriter`: it reads the
    bitmap purely as a cross-check and never writes session state. That
    asymmetry is why the two shm ports are split.

    It decides COMPLETE (hand to the verifier) and nothing else. Stall
    detection is `SessionAuthority`'s: the aggregator reports the decoded
    count and the last-progress time via `progress_of`, and the authority's
    sweep is the one place that says what "terminal" means.
    """

    def __init__(
        self,
        clock: Clock,
        shm_reader: ShmReader,
        on_complete: OnComplete,
        live_receivers: LiveReceivers,
        poll_interval_s: float,
        shm_crosscheck: bool,
    ) -> None:
        self._clock: Clock = clock
        self._shm_reader: ShmReader = shm_reader
        self._on_complete: OnComplete = on_complete
        self._live_receivers: LiveReceivers = live_receivers
        self._poll_interval_s: float = poll_interval_s
        self._shm_crosscheck: bool = shm_crosscheck
        self._sessions: dict[SessionId, _SessionProgress] = {}
        self._receiver_counters: dict[ReceiverId, ReceiverCounters] = {}
        self._snapshots: dict[SessionId, SessionSnapshot] = {}
        # Sessions whose shm cross-check has already produced a warning. The
        # bitmap either diverges or it doesn't; once it does, the condition
        # persists every poll, and 300 identical lines bury the one that
        # matters. Report it once per session, then stay quiet.
        self._crosscheck_warned: set[SessionId] = set()

    def register_session(self, spec: SessionSpec, decoded: Collection[BlockId] = ()) -> None:
        self._sessions[spec.session_id] = _SessionProgress(
            spec=spec,
            decoded={block_id for block_id in decoded if 0 <= block_id < spec.total_blocks},
            last_progress_at=self._clock.now(),
        )

    def handle_block_decoded(
        self, receiver_id: ReceiverId, session_id: SessionId, block_ids: Iterable[int]
    ) -> None:
        progress = self._sessions.get(session_id)
        if progress is None:
            logger.warning(
                "block_decoded_for_unknown_session",
                session_id=session_id,
                receiver_id=receiver_id,
            )
            return
        total_blocks = progress.spec.total_blocks
        before = len(progress.decoded)
        # Range-filtered and set-unioned: a receiver re-reporting the same
        # blocks, or two receivers reporting one, counts once.
        progress.decoded |= {
            BlockId(block_id) for block_id in block_ids if 0 <= block_id < total_blocks
        }
        # Only real growth advances the stall clock -- a receiver re-reporting
        # its shard (or two receivers overlapping) must not make a dead
        # session look alive.
        if len(progress.decoded) > before:
            progress.last_progress_at = self._clock.now()

    def handle_receiver_stats(self, receiver_id: ReceiverId, stats: Any) -> None:
        # receiver_id comes from the caller (the UDS connection the
        # handshake already verified), never from stats.receiver_id --
        # the same "trust the connection, not the payload" rule
        # handle_block_decoded already follows.
        self._receiver_counters[receiver_id] = _stats_to_counters(stats)

    async def poll(self) -> None:
        now = self._clock.now()
        for session_id, progress in list(self._sessions.items()):
            if progress.state in _TERMINAL_STATES:
                continue
            snapshot = self._build_snapshot(session_id, progress, now)
            self._snapshots[session_id] = snapshot
            await self._act_on(progress, snapshot)

    def progress_of(self, session_id: SessionId) -> tuple[int, float, tuple[BlockId, ...]] | None:
        """`(decoded_count, last_progress_at, missing-blocks preview)` for a
        tracked session, or `None` if it was never registered.
        `SessionAuthority`'s sweep reads this: the count and timestamp feed
        `terminal_reason()`, the preview goes into the `session_stalled` log."""
        progress = self._sessions.get(session_id)
        if progress is None:
            return None
        missing = missing_blocks(progress.decoded, progress.spec.total_blocks)
        return (
            len(progress.decoded),
            progress.last_progress_at,
            missing[:MISSING_BLOCKS_PREVIEW_LIMIT],
        )

    def snapshot_for(self, session_id: SessionId) -> SessionSnapshot | None:
        return self._snapshots.get(session_id)

    def snapshots(self) -> tuple[SessionSnapshot, ...]:
        return tuple(self._snapshots.values())

    def spec_for(self, session_id: SessionId) -> SessionSpec | None:
        progress = self._sessions.get(session_id)
        return progress.spec if progress is not None else None

    def mark_verified(self, session_id: SessionId) -> None:
        """Record that `on_complete`'s verify+publish succeeded for this session.

        Called by the composition root, never by `poll()` itself: only the
        code that actually ran the verifier knows the outcome.
        """
        self._set_terminal_state(session_id, SessionState.VERIFIED)

    def mark_hash_mismatch(self, session_id: SessionId, quarantine_path: str) -> None:
        """Record that `on_complete`'s verify failed and the file was quarantined.

        `quarantine_path` is what `Publisher.quarantine` returned -- shown in
        the status view so the quarantined file is findable from the screen.
        """
        self._set_terminal_state(session_id, SessionState.HASH_MISMATCH, quarantine_path)

    def mark_incomplete(self, session_id: SessionId, quarantine_path: str) -> None:
        """Record that `SessionAuthority`'s sweep declared this session stalled
        and quarantined its partial (at `quarantine_path`).

        Like `mark_hash_mismatch`, the aggregator does not decide the outcome
        -- it is told -- and then `poll()` stops rebuilding the snapshot so
        the status display keeps showing INCOMPLETE and where the partial is.
        """
        self._set_terminal_state(session_id, SessionState.INCOMPLETE, quarantine_path)

    def _set_terminal_state(
        self, session_id: SessionId, state: SessionState, quarantine_path: str | None = None
    ) -> None:
        progress = self._sessions.get(session_id)
        if progress is None:
            return
        progress.state = state
        progress.quarantine_path = quarantine_path
        snapshot = self._snapshots.get(session_id)
        if snapshot is not None:
            self._snapshots[session_id] = replace(
                snapshot, state=state, quarantine_path=quarantine_path
            )

    async def run(self) -> None:
        while True:
            await self._clock.sleep(self._poll_interval_s)
            await self.poll()

    async def _act_on(self, progress: _SessionProgress, snapshot: SessionSnapshot) -> None:
        if snapshot.state is progress.state:
            return
        if snapshot.state is SessionState.COMPLETE:
            progress.state = SessionState.COMPLETE
            await self._on_complete(snapshot)

    def _build_snapshot(
        self, session_id: SessionId, progress: _SessionProgress, now: float
    ) -> SessionSnapshot:
        spec = progress.spec
        decoded_count = len(progress.decoded)

        state = (
            SessionState.COMPLETE
            if is_complete(progress.decoded, spec.total_blocks)
            else SessionState.OPEN
        )

        missing_full = missing_blocks(progress.decoded, spec.total_blocks)

        if self._shm_crosscheck:
            self._cross_check(session_id, decoded_count)

        per_receiver = tuple(sorted(self._receiver_counters.items()))
        symbols_received = sum(counters.pkts_ok for _, counters in per_receiver)

        return SessionSnapshot(
            spec=spec,
            state=state,
            blocks_decoded=decoded_count,
            total_blocks=spec.total_blocks,
            observed_loss_pct=observed_loss_pct(spec.total_blocks * spec.n, symbols_received),
            per_receiver=per_receiver,
            live_receivers=self._live_receivers(),
            missing_blocks=missing_full[:MISSING_BLOCKS_PREVIEW_LIMIT],
            missing_block_count=len(missing_full),
            seconds_since_progress=max(0.0, now - progress.last_progress_at),
            quarantine_path=progress.quarantine_path,
        )

    def _cross_check(self, session_id: SessionId, uds_count: int) -> None:
        if session_id in self._crosscheck_warned:
            return
        bitmap_count = self._bitmap_popcount(session_id)
        if bitmap_count is None:
            # _bitmap_popcount already logged shm_bitmap_unavailable.
            self._crosscheck_warned.add(session_id)
            return
        if bitmap_count != uds_count:
            logger.warning(
                "shm_bitmap_diverges_from_uds",
                session_id=session_id,
                uds_count=uds_count,
                bitmap_count=bitmap_count,
            )
            self._crosscheck_warned.add(session_id)

    def _bitmap_popcount(self, session_id: SessionId) -> int | None:
        try:
            view = self._shm_reader.bitmap_for(session_id)
        except (KeyError, RuntimeError) as error:
            logger.warning("shm_bitmap_unavailable", session_id=session_id, error=str(error))
            return None
        try:
            return sum(byte.bit_count() for byte in view)
        finally:
            view.release()
