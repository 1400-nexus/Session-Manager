import asyncio
import math
from collections.abc import Awaitable, Callable, Collection
from pathlib import Path
from typing import Any

import rx_pb2
import structlog

from session_manager.domain.ids import BlockId, ReceiverId, SessionId
from session_manager.domain.models import IncompleteReport, OpenSession, SessionSpec
from session_manager.domain.paths import is_unsafe_relpath
from session_manager.domain.progress import missing_blocks
from session_manager.domain.purge_policy import (
    PurgeReason,
    SessionProgress,
    terminal_reason,
)
from session_manager.ipc import codec
from session_manager.ports.protocols import (
    Clock,
    FileLock,
    FileStore,
    Journal,
    SessionSpecStore,
    ShmWriter,
)
from session_manager.services.errors import ManifestRejected

logger = structlog.get_logger(__name__)

Broadcast = Callable[[bytes], Awaitable[None]]
SendToReceiver = Callable[[ReceiverId, bytes], Awaitable[None]]
# Called on the one path that opens a session -- fresh create and adopt
# recovery both -- with the decoded blocks known so far (empty for a fresh
# session). Wiring registration here rather than at each call site is what
# stops a new creation path from silently skipping progress tracking.
OnSessionOpened = Callable[[SessionSpec, Collection[BlockId]], None]
# `(decoded_count, last_progress_at, missing-blocks preview)` for a tracked
# session, or None if it was never registered with progress tracking (a lost
# spec sidecar). `last_progress_at` is the aggregator's monotonic stamp of
# the last decoded-set growth -- set at registration, which on an adopting
# restart is the adoption instant, so a recovered session gets fresh grace
# without the authority tracking anything itself.
DecodedProgress = Callable[[SessionId], tuple[int, float, tuple[BlockId, ...]] | None]
# Told the aggregator a session was swept INCOMPLETE and where its partial
# was quarantined, so the status display freezes the snapshot and shows the
# path -- `aggregator.mark_incomplete`.
MarkIncomplete = Callable[[SessionId, str], None]
# Move a stalled session's partial into quarantine/ with its missing-blocks
# report -- Publisher.quarantine_incomplete. Injected rather than reached for
# directly so the authority stays out of the output/quarantine directories,
# same as it never publishes.
QuarantineIncomplete = Callable[[SessionSpec, IncompleteReport], Path]

# PurgeReason -> the wire string PurgeSession.reason has always carried.
# rx.proto keeps `reason` a free-text string (see RECEIVER_CONTRACT.md s4),
# so the enum stays an internal vocabulary and this table is the one place
# the mapping is stated.
_PURGE_REASON_WIRE: dict[PurgeReason, str] = {
    PurgeReason.PUBLISHED: "verified",
    PurgeReason.QUARANTINED: "hash_mismatch",
    PurgeReason.INCOMPLETE: "incomplete",
}


def _bitmap_bytes(total_blocks: int) -> int:
    return (total_blocks + 7) // 8


def _manifest_to_spec(manifest: Any) -> SessionSpec:
    # The wire field Manifest.block_bytes carries the SYMBOL size, not the
    # block size (see domain.models.SessionSpec.block_bytes) -- this is the
    # boundary where that copy happens.
    return SessionSpec(
        session_id=SessionId(manifest.session_id),
        relpath=manifest.filepath,
        file_size=manifest.file_size,
        file_hash=manifest.file_hash,
        k=manifest.k,
        n=manifest.n,
        symbol_bytes=manifest.block_bytes,
        total_blocks=manifest.total_blocks,
    )


def _validate_spec(spec: SessionSpec) -> None:
    if spec.k <= 0 or spec.n <= 0 or spec.symbol_bytes <= 0:
        raise ManifestRejected(spec.session_id, "k, n and symbol_bytes must all be positive")
    if spec.k >= spec.n:
        raise ManifestRejected(spec.session_id, f"k ({spec.k}) must be < n ({spec.n})")
    if spec.file_size < 0:
        raise ManifestRejected(spec.session_id, f"file_size {spec.file_size} is negative")
    expected_blocks = math.ceil(spec.file_size / spec.block_bytes) if spec.file_size else 0
    if spec.total_blocks != expected_blocks:
        raise ManifestRejected(
            spec.session_id,
            f"total_blocks {spec.total_blocks} does not match "
            f"ceil(file_size / (k * symbol_bytes)) = {expected_blocks}",
        )
    if is_unsafe_relpath(spec.relpath):
        raise ManifestRejected(
            spec.session_id,
            f"relpath {spec.relpath!r} is absolute or escapes the staging directory",
        )


class SessionAuthority:
    """Sole writer of session state.

    Every session is created here, so the three receivers all reporting the
    same manifest is not a race: the first `ManifestSeen` creates the session
    and the rest are duplicates. There is deliberately no lock between
    receivers -- adding one would look correct and be pure overhead.
    """

    def __init__(
        self,
        file_lock: FileLock,
        shm: ShmWriter,
        file_store: FileStore,
        journal: Journal,
        spec_store: SessionSpecStore,
        broadcast: Broadcast,
        send_to_receiver: SendToReceiver,
        on_session_opened: OnSessionOpened,
        clock: Clock,
        progress_of: DecodedProgress,
        on_incomplete: MarkIncomplete,
        quarantine_incomplete: QuarantineIncomplete,
        shm_name: str,
        staging_dir: str,
        journal_dir: str,
        arena_bytes: int,
        session_region_base: int,
        sweep_interval_s: float,
        stall_timeout_s: float,
    ) -> None:
        self._file_lock: FileLock = file_lock
        self._shm: ShmWriter = shm
        self._file_store: FileStore = file_store
        self._journal: Journal = journal
        self._spec_store: SessionSpecStore = spec_store
        self._broadcast: Broadcast = broadcast
        self._send_to_receiver: SendToReceiver = send_to_receiver
        self._on_session_opened: OnSessionOpened = on_session_opened
        self._clock: Clock = clock
        self._progress_of: DecodedProgress = progress_of
        self._on_incomplete: MarkIncomplete = on_incomplete
        self._quarantine_incomplete: QuarantineIncomplete = quarantine_incomplete
        self._shm_name: str = shm_name
        self._staging_dir: str = staging_dir
        self._journal_dir: str = journal_dir
        self._arena_bytes: int = arena_bytes
        self._next_offset: int = session_region_base
        self._sweep_interval_s: float = sweep_interval_s
        self._stall_timeout_s: float = stall_timeout_s
        self._known: dict[SessionId, OpenSession] = {}
        self._specs: dict[SessionId, SessionSpec] = {}
        self._recovered_ids: set[SessionId] = set()
        self._purged: set[SessionId] = set()
        # session_id -> the first sweep `now` at which progress_of() came back
        # None for it. A session in _specs that the aggregator does not know
        # is a wiring bug (this service has shipped that class before): it can
        # never complete, never stall, and without this would sit in the
        # staging tree and hold a session-table slot forever. Instead it is
        # logged loudly and, once a stall_timeout has passed since it was
        # noticed, purged INCOMPLETE -- which quarantines its partial with a
        # missing-blocks report, the same as any other stall.
        self._untracked_since: dict[SessionId, float] = {}
        self._adopted: bool = False
        self._stop_event: asyncio.Event = asyncio.Event()
        self._sweep_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        # flock before ANYTHING touches shm: two managers on one segment is
        # the worst bug available here, so the second one must fail loudly.
        self._file_lock.acquire()
        self._adopted = self._shm.create_or_adopt(self._shm_name, self._arena_bytes)
        if self._adopted:
            await self._recover()
        # Only reached on the success path -- if acquire() raised, there is no
        # task and stop() is a no-op. sweep_interval_s <= 0 disables the loop:
        # unit tests drive _sweep_once() directly and never want a live task.
        if self._sweep_interval_s > 0:
            self._sweep_task = asyncio.create_task(self._run_sweep())

    async def stop(self) -> None:
        """Stop the sweep loop. Idempotent; safe if start() never ran."""
        self._stop_event.set()
        task = self._sweep_task
        if task is None:
            return
        self._sweep_task = None
        # cancel as well as signalling: a tick blocked inside _broadcast()
        # (a wedged receiver) will not observe _stop_event on its own. The
        # cancel is awaited from this non-cancelled context -- not the
        # task.cancel()-from-inside-a-cancelling-task shape that deadlocked
        # shutdown before.
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def handle_manifest_seen(self, receiver_id: ReceiverId, manifest_seen: Any) -> None:
        manifest = manifest_seen.manifest
        session_id = SessionId(manifest.session_id)
        if session_id in self._purged:
            # The session is over -- we broadcast PurgeSession and tore down
            # its shm entry, sidecar and journal. A resent ManifestSeen must
            # not resurrect it (that would re-init_session and re-broadcast a
            # SessionOpen for something B was told is finished).
            logger.debug("manifest_seen_for_purged_session", session_id=session_id)
            return
        if session_id in self._known:
            # A duplicate is still information this receiver needs: its
            # ManifestSeen may have arrived after another receiver's won the
            # create race, or it reconnected after the one broadcast already
            # went out. SessionOpen is idempotent (see rx.proto), so answer
            # this receiver directly rather than re-broadcasting.
            await self._send_session_open(receiver_id, session_id)
            return

        spec = _manifest_to_spec(manifest)
        _validate_spec(spec)

        block_table_offset = self._next_offset
        bitmap_offset = block_table_offset + _bitmap_bytes(spec.total_blocks)
        region_end = bitmap_offset + _bitmap_bytes(spec.total_blocks)
        if region_end > self._arena_bytes:
            raise ManifestRejected(
                spec.session_id, f"needs {region_end} bytes, arena is {self._arena_bytes}"
            )

        self._file_store.allocate(spec.relpath, spec.file_size)
        self._shm.init_session(spec, block_table_offset, bitmap_offset)
        # Durable before broadcasting: an adopting restart after this point
        # recovers the full spec, not just the decoded blocks.
        self._spec_store.save(spec)
        self._next_offset = region_end
        open_session = OpenSession(
            session_id=session_id,
            total_blocks=spec.total_blocks,
            block_table_offset=block_table_offset,
            bitmap_offset=bitmap_offset,
        )
        self._known[session_id] = open_session
        self._specs[session_id] = spec
        # Register progress tracking on the SAME path that creates the
        # session, so a future creation path physically cannot forget to.
        self._on_session_opened(spec, ())
        logger.info("session_opened", session_id=session_id, total_blocks=spec.total_blocks)
        await self._broadcast(self._session_open_message(spec, open_session))

    async def send_config_to(self, receiver_id: ReceiverId) -> None:
        # Delivered right after a successful ReceiverHello so a receiver never
        # has to be told the segment name / staging dir out of band and match
        # by convention -- the same implicit coupling that made a relative
        # dest_path fail. Safe before authority.start(): these are static
        # config, not session state.
        config = rx_pb2.Config(
            shm_name=self._shm_name,
            staging_dir=self._staging_dir,
            journal_dir=self._journal_dir,
        )
        await self._send_to_receiver(receiver_id, codec.encode(config))

    async def send_open_sessions_to(self, receiver_id: ReceiverId) -> None:
        # A receiver that connects (or reconnects) after a session opened
        # never saw its broadcast; this is how it learns the session exists
        # at all -- the path a receiver restart mid-transfer depends on.
        for session_id in list(self._specs):
            await self._send_session_open(receiver_id, session_id)

    def adopted(self) -> bool:
        return self._adopted

    def was_recovered(self, session_id: SessionId) -> bool:
        # True for a session rebuilt from the journal on an adopting restart.
        # A hash mismatch on one of these points at a durability gap (a block
        # journaled before its bytes hit disk -- see rx.proto BlockDecoded),
        # not FEC corruption, and the composition root logs it differently.
        return session_id in self._recovered_ids

    def is_purged(self, session_id: SessionId) -> bool:
        return session_id in self._purged

    def classify_completion(self, session_id: SessionId, *, hash_ok: bool) -> PurgeReason:
        """The publish-vs-quarantine decision for a session the aggregator
        just reported COMPLETE, routed through `terminal_reason()` so there is
        one module that says what terminal means.

        The session is complete by construction here, so the policy reduces
        to the hash verdict -- but a 0-block session (an empty file) cannot be
        expressed as a `SessionProgress` (that dataclass rejects
        total_blocks <= 0), so that degenerate case is decided directly.
        """
        spec = self._specs.get(session_id)
        if spec is None or spec.total_blocks <= 0:
            return PurgeReason.PUBLISHED if hash_ok else PurgeReason.QUARANTINED
        # A complete session: is_stalled() short-circuits False, so the
        # timestamps here are irrelevant and terminal_reason reduces to the
        # verdict. Going through it keeps the vocabulary in one module.
        progress = SessionProgress(
            total_blocks=spec.total_blocks,
            decoded_blocks=spec.total_blocks,
            opened_at=0.0,
            last_block_at=0.0,
        )
        reason = terminal_reason(
            progress,
            now=0.0,
            hash_ok=hash_ok,
            stall_timeout=self._stall_timeout_s,
        )
        if reason is None:
            # Unreachable today: is_complete(progress) holds (decoded == total),
            # so the policy returns PUBLISHED/QUARANTINED for any non-None
            # hash_ok. Kept so a future policy change cannot silently drop a
            # completed session on the floor.
            return PurgeReason.PUBLISHED if hash_ok else PurgeReason.QUARANTINED
        return reason

    async def purge(self, session_id: SessionId, reason: PurgeReason) -> None:
        """Announce a session's terminal state to every receiver, once.

        The single `PurgeSession` emission point -- the sweep and the
        completion path both funnel through here, and a session already
        purged is never re-announced. For an INCOMPLETE purge it also moves
        the partial into quarantine with its missing-blocks report, before
        tearing the durable footprint down.
        """
        if session_id in self._purged:
            return
        self._purged.add(session_id)
        wire_reason = _PURGE_REASON_WIRE[reason]
        await self._broadcast(
            codec.encode(rx_pb2.PurgeSession(session_id=str(session_id), reason=wire_reason))
        )
        if reason is PurgeReason.INCOMPLETE:
            # Preserve the partial as evidence BEFORE _tear_down unlinks the
            # journal its missing-blocks list is read from. Deliberately not
            # best-effort: if this raises, _tear_down does not run, so the
            # journal and shm entry survive for manual recovery rather than a
            # quarantined partial being stranded with no record of what it
            # holds.
            await self._preserve_incomplete_partial(session_id)
        self._tear_down(session_id)
        logger.info("session_purged", session_id=session_id, reason=wire_reason)

    async def _preserve_incomplete_partial(self, session_id: SessionId) -> None:
        # INCOMPLETE only: move the partial to quarantine/ and drop a
        # complete missing-blocks report beside it. A partial (up to the
        # whole file) is evidence of what the one-way link delivered, same
        # as a hash mismatch -- it belongs in quarantine/, not left orphaned
        # in the staging tree. The missing list comes from the journal, not
        # progress_of() (which caps its preview at 32), and can run a fold
        # ahead of the aggregator's count since append precedes the fold.
        spec = self._specs.get(session_id)
        if spec is None:
            logger.debug("incomplete_partial_no_spec", session_id=session_id)
            return
        decoded: set[BlockId] = set()
        async for block_id in self._journal.replay(session_id):
            decoded.add(block_id)
        missing = missing_blocks(decoded, spec.total_blocks)
        report = IncompleteReport(
            session_id=session_id,
            total_blocks=spec.total_blocks,
            decoded_blocks=spec.total_blocks - len(missing),
            missing_block_ids=missing,
        )
        quarantined_path = self._quarantine_incomplete(spec, report)
        # Freeze the aggregator's snapshot at INCOMPLETE and hand it the path,
        # now that the move has happened -- so the status view shows where the
        # partial went. Kept here, after the quarantine, rather than at the
        # sweep call site so the snapshot never carries a path the file isn't
        # at yet.
        self._on_incomplete(session_id, str(quarantined_path))

    def _tear_down(self, session_id: SessionId) -> None:
        # Remove the session's durable footprint so an adopting restart does
        # not re-recover a session B was told is finished and re-broadcast a
        # SessionOpen for it. `_purged` is in-memory only; these three are
        # what actually survive a crash. Best-effort: a failure here leaves a
        # stale artifact, not a wrong announcement (`_purged` still guards
        # this process; the artifact only matters on a later adopt).
        #
        # For an INCOMPLETE purge, _preserve_incomplete_partial() has already
        # run: it moved the partial into quarantine/ and wrote its
        # missing-blocks report, reading that list from the journal this
        # method is about to unlink. That step raises rather than returns on
        # failure, so if control reached here the report is durable and the
        # journal below is safe to drop.
        #
        # ORDER IS LOAD-BEARING. shm first: the on-segment session table is
        # what _recover() iterates, so once it is gone nothing downstream can
        # be re-adopted and the sidecar/journal order stops mattering. But if
        # shm.purge_session() fails (the except below), a mid-teardown crash
        # leaves the session in the table -- and then SIDECAR MUST GO BEFORE
        # JOURNAL:
        #  - sidecar deleted, then crash: _recover() replays the (still full)
        #    journal but spec_store.load() -> None -> session_spec_missing_on_
        #    recovery, `continue`. No SessionOpen re-broadcast. Recoverable.
        #  - journal purged, then crash: _recover() replays an empty journal,
        #    loads the (still present) spec, and re-registers the session with
        #    zero decoded blocks -- re-broadcasting a SessionOpen for a
        #    transfer B already tore down. Worse.
        # Do not reorder.
        try:
            self._shm.purge_session(session_id)
        except (KeyError, RuntimeError) as error:
            logger.error("shm_purge_session_failed", session_id=session_id, error=str(error))
        self._spec_store.delete(session_id)  # sidecar -- before the journal
        self._journal.purge(session_id)
        self._known.pop(session_id, None)
        self._specs.pop(session_id, None)
        self._untracked_since.pop(session_id, None)

    def shutdown(self, clean: bool = True) -> None:
        self._shm.close(unlink=clean)
        self._file_lock.release()

    async def _run_sweep(self) -> None:
        while not self._stop_event.is_set():
            await self._interruptible_sleep(self._sweep_interval_s)
            if self._stop_event.is_set():
                return
            await self._sweep_once()

    async def _interruptible_sleep(self, delay: float) -> None:
        # Mirrors ProcessSupervisor._wait_for_backoff_or_stop: race the sleep
        # against the stop signal so shutdown never waits out a full interval.
        sleep_task = asyncio.ensure_future(asyncio.sleep(delay))
        stop_task = asyncio.ensure_future(self._stop_event.wait())
        try:
            await asyncio.wait({sleep_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (sleep_task, stop_task):
                task.cancel()
            await asyncio.gather(sleep_task, stop_task, return_exceptions=True)

    async def _sweep_once(self) -> None:
        now = self._clock.now()
        for session_id in list(self._specs):
            if session_id in self._purged:
                continue
            try:
                await self._evaluate(session_id, now)
            except Exception as error:
                # One session's bad evaluation must not kill the sweep or
                # skip the sessions after it in this tick.
                logger.error(
                    "session_sweep_evaluation_failed", session_id=session_id, error=str(error)
                )

    async def _evaluate(self, session_id: SessionId, now: float) -> None:
        spec = self._specs.get(session_id)
        if spec is None or spec.total_blocks <= 0:
            return
        tracked = self._progress_of(session_id)
        if tracked is None:
            await self._handle_untracked(session_id, spec, now)
            return
        self._untracked_since.pop(session_id, None)  # it recovered
        decoded_count, last_progress_at, missing_preview = tracked

        # `last_progress_at` is the aggregator's stamp of the last decoded-set
        # growth -- or, for a recovered session, the adoption instant, because
        # register_session runs during _recover(). So a restart gives every
        # live transfer a full fresh stall_timeout without the authority
        # tracking anything itself.
        progress = SessionProgress(
            total_blocks=spec.total_blocks,
            decoded_blocks=decoded_count,
            opened_at=last_progress_at,
            last_block_at=last_progress_at,
        )
        reason = terminal_reason(
            progress, now=now, hash_ok=None, stall_timeout=self._stall_timeout_s
        )
        if reason is not PurgeReason.INCOMPLETE:
            # None -> still live. PUBLISHED/QUARANTINED never come back here
            # (hash_ok is None), and even if they did the completion path
            # owns publish/quarantine -- not the sweep.
            return

        logger.warning(
            "session_stalled",
            session_id=session_id,
            blocks_decoded=decoded_count,
            total_blocks=spec.total_blocks,
            missing_block_count=spec.total_blocks - decoded_count,
            missing_blocks_preview=[int(block_id) for block_id in missing_preview],
        )
        # purge() -> _preserve_incomplete_partial() calls _on_incomplete once
        # the partial is quarantined, so the frozen snapshot carries the real
        # path.
        await self.purge(session_id, PurgeReason.INCOMPLETE)

    async def _handle_untracked(self, session_id: SessionId, spec: SessionSpec, now: float) -> None:
        # The session is in _specs but progress_of() is None -- the aggregator
        # never registered it. That is a wiring bug, not a normal state, so it
        # is loud; and it is self-limiting: after a stall_timeout it is purged
        # INCOMPLETE, which quarantines the partial and frees the shm slot.
        # The bytes cannot be verified without the aggregator, but they are
        # still evidence of what the link delivered, so they go to quarantine/
        # like any other stall rather than being left behind or deleted.
        first_seen = self._untracked_since.get(session_id)
        if first_seen is None:
            self._untracked_since[session_id] = now
            logger.error(
                "session_untracked_by_aggregator",
                session_id=session_id,
                total_blocks=spec.total_blocks,
                hint="in the authority's _specs but progress_of() is None -- a "
                "wiring bug; will be purged INCOMPLETE after the stall timeout",
            )
            return
        if now - first_seen < self._stall_timeout_s:
            return
        logger.error("purging_untracked_session", session_id=session_id)
        await self.purge(session_id, PurgeReason.INCOMPLETE)

    async def _recover(self) -> None:
        # Each recovered session's stall clock starts now, at the adoption
        # instant: `_on_session_opened` -> `aggregator.register_session`
        # stamps `last_progress_at` with the current time, and the sweep
        # keys off that. The journal has no timestamp for when its blocks
        # actually arrived, and running from the original open time would
        # purge every live transfer on the first post-restart sweep.
        for open_session in self._shm.open_sessions():
            self._known[open_session.session_id] = open_session
            region_end = open_session.bitmap_offset + _bitmap_bytes(open_session.total_blocks)
            self._next_offset = max(self._next_offset, region_end)
            decoded: set[BlockId] = set()
            async for block_id in self._journal.replay(open_session.session_id):
                decoded.add(block_id)

            spec = self._spec_store.load(open_session.session_id)
            if spec is None:
                # The bytes are safe (decoded set above, bitmap in shm) but
                # without the spec this session can never be handed to the
                # aggregator, so it can never verify or publish. It stays in
                # _known so a resent ManifestSeen is (correctly) treated as
                # a duplicate rather than re-init_session-ing and zeroing a
                # bitmap receivers are still writing into.
                logger.error(
                    "session_spec_missing_on_recovery",
                    session_id=open_session.session_id,
                    decoded_blocks=len(decoded),
                )
                continue
            if not self._file_store.staged_file_exists(spec.relpath):
                # Sidecar and journal survived but the partial is gone -- the
                # signature of a previous run that purged this session
                # INCOMPLETE (moved the partial to quarantine/) and died
                # before _tear_down cleared the sidecar/journal/shm entry.
                # Adopting now would rebuild a session whose bytes no longer
                # exist and re-broadcast SessionOpen for one the receivers
                # were already sent PurgeSession for. Refuse. Stays in _known
                # only, so a resent ManifestSeen is a no-op rather than a
                # fresh init_session over the hole.
                logger.error(
                    "session_staged_file_missing_on_recovery",
                    session_id=open_session.session_id,
                    staged_path=str(self._file_store.staged_path(spec.relpath)),
                    decoded_blocks=len(decoded),
                    remedy=(
                        "restore the partial from quarantine/ to resume it, or delete the "
                        f"spec sidecar for {open_session.session_id} to abandon it"
                    ),
                )
                continue
            self._specs[open_session.session_id] = spec
            self._recovered_ids.add(open_session.session_id)
            self._on_session_opened(spec, frozenset(decoded))

            logger.info(
                "session_recovered",
                session_id=open_session.session_id,
                decoded_blocks=len(decoded),
            )

    async def _send_session_open(self, receiver_id: ReceiverId, session_id: SessionId) -> None:
        spec = self._specs.get(session_id)
        open_session = self._known.get(session_id)
        if spec is None or open_session is None:
            return
        await self._send_to_receiver(receiver_id, self._session_open_message(spec, open_session))

    def _session_open_message(self, spec: SessionSpec, open_session: OpenSession) -> bytes:
        # Absolute, not spec.relpath: a receiver must write to exactly this
        # path, not reconstruct one against its own idea of a staging
        # directory -- see rx.proto's SessionOpen.dest_path.
        dest_path = str(self._file_store.staged_path(spec.relpath).resolve())
        session_open = rx_pb2.SessionOpen(
            session_id=spec.session_id,
            dest_path=dest_path,
            total_blocks=spec.total_blocks,
            k=spec.k,
            n=spec.n,
            block_bytes=spec.symbol_bytes,
            block_table_offset=open_session.block_table_offset,
            bitmap_offset=open_session.bitmap_offset,
        )
        return codec.encode(session_open)
