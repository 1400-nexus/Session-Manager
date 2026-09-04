from collections.abc import Awaitable, Callable, Collection, Iterable
from dataclasses import dataclass
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
    is_stalled,
    missing_blocks,
    observed_loss_pct,
)
from session_manager.ports.protocols import Clock, ShmReader

logger = structlog.get_logger(__name__)

OnComplete = Callable[[SessionSnapshot], Awaitable[None]]
LiveReceivers = Callable[[], frozenset[ReceiverId]]


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
    last_progress_at: float
    state: SessionState = SessionState.OPEN


class ProgressAggregator:
    """Folds `BlockDecoded` into per-session completion state.

    This is the authoritative record -- the UDS stream, not the shm bitmap.
    The aggregator holds `ShmReader` only, never `ShmWriter`: it reads the
    bitmap purely as a cross-check and never writes session state. That
    asymmetry is why the two shm ports are split.
    """

    def __init__(
        self,
        clock: Clock,
        shm_reader: ShmReader,
        on_complete: OnComplete,
        live_receivers: LiveReceivers,
        poll_interval_s: float,
        stall_timeout_s: float,
        shm_crosscheck: bool,
    ) -> None:
        self._clock: Clock = clock
        self._shm_reader: ShmReader = shm_reader
        self._on_complete: OnComplete = on_complete
        self._live_receivers: LiveReceivers = live_receivers
        self._poll_interval_s: float = poll_interval_s
        self._stall_timeout_s: float = stall_timeout_s
        self._shm_crosscheck: bool = shm_crosscheck
        self._sessions: dict[SessionId, _SessionProgress] = {}
        self._receiver_counters: dict[ReceiverId, ReceiverCounters] = {}
        self._snapshots: dict[SessionId, SessionSnapshot] = {}

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
        fresh = {BlockId(block_id) for block_id in block_ids if 0 <= block_id < total_blocks}
        before = len(progress.decoded)
        progress.decoded |= fresh
        # Only real growth is progress. A receiver re-reporting the same blocks
        # (or two receivers reporting one block) must not refresh the stall
        # timer, or duplicate spam keeps a dead session looking alive forever.
        if len(progress.decoded) > before:
            progress.last_progress_at = self._clock.now()

    def handle_receiver_stats(self, stats: Any) -> None:
        self._receiver_counters[ReceiverId(stats.receiver_id)] = _stats_to_counters(stats)

    async def poll(self) -> None:
        now = self._clock.now()
        for session_id, progress in list(self._sessions.items()):
            snapshot = self._build_snapshot(session_id, progress, now)
            self._snapshots[session_id] = snapshot
            await self._act_on(progress, snapshot)

    def snapshot_for(self, session_id: SessionId) -> SessionSnapshot | None:
        return self._snapshots.get(session_id)

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
        elif snapshot.state is SessionState.INCOMPLETE:
            progress.state = SessionState.INCOMPLETE
            logger.warning(
                "session_stalled",
                session_id=snapshot.spec.session_id,
                blocks_decoded=snapshot.blocks_decoded,
                total_blocks=snapshot.total_blocks,
                missing_block_count=snapshot.missing_block_count,
                missing_blocks_preview=[int(block_id) for block_id in snapshot.missing_blocks],
            )

    def _build_snapshot(
        self, session_id: SessionId, progress: _SessionProgress, now: float
    ) -> SessionSnapshot:
        spec = progress.spec
        decoded_count = len(progress.decoded)

        if is_complete(progress.decoded, spec.total_blocks):
            state = SessionState.COMPLETE
        elif is_stalled(progress.last_progress_at, now, self._stall_timeout_s):
            state = SessionState.INCOMPLETE
        else:
            state = SessionState.OPEN

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
        )

    def _cross_check(self, session_id: SessionId, uds_count: int) -> None:
        bitmap_count = self._bitmap_popcount(session_id)
        if bitmap_count is not None and bitmap_count != uds_count:
            logger.warning(
                "shm_bitmap_diverges_from_uds",
                session_id=session_id,
                uds_count=uds_count,
                bitmap_count=bitmap_count,
            )

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
