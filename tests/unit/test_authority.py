import asyncio
import math
from collections.abc import Collection
from dataclasses import dataclass, field
from typing import Any, cast

import common_pb2
import pytest
import rx_pb2
from structlog.testing import capture_logs

from session_manager.adapters.errors import LockHeldError
from session_manager.adapters.shm_layout import AdoptDecision
from session_manager.domain.ids import BlockId, ReceiverId, SessionId
from session_manager.domain.models import IncompleteReport, SessionSpec
from session_manager.domain.purge_policy import PurgeReason
from session_manager.ipc import codec
from session_manager.services.authority import SessionAuthority
from session_manager.services.errors import ManifestRejected
from session_manager.services.publisher import Publisher
from tests.fakes.fake_clock import FakeClock
from tests.fakes.fake_file_lock import FakeFileLock
from tests.fakes.fake_file_store import FakeFileStore
from tests.fakes.fake_journal import FakeJournal
from tests.fakes.fake_session_spec_store import FakeSessionSpecStore
from tests.fakes.fake_shm import FakeShm

# Long, so a session created in a test is never accidentally past it.
STALL_TIMEOUT = 1_000.0

ARENA_BYTES = 1 << 20
REGION_BASE = 8192
SHM_NAME = "nexus-rx-test"

R1 = ReceiverId(1)
R2 = ReceiverId(2)
R3 = ReceiverId(3)

K = 200
SYMBOL_BYTES = 1400
FILE_SIZE = 800_000
TOTAL_BLOCKS = math.ceil(FILE_SIZE / (K * SYMBOL_BYTES))
# A file whose block count (12) comfortably exceeds the small decoded counts
# the sweep tests use, so `SessionProgress` never rejects `decoded > total`.
BIG_FILE_SIZE = 12 * K * SYMBOL_BYTES


def _manifest_seen(
    session_id: str = "s-1",
    *,
    filepath: str = "sub/dir/output.bin",
    file_size: int = FILE_SIZE,
    k: int = K,
    n: int = 255,
    symbol_bytes: int = SYMBOL_BYTES,
    total_blocks: int | None = None,
) -> Any:
    resolved_blocks = (
        total_blocks
        if total_blocks is not None
        else math.ceil(file_size / (k * symbol_bytes))
        if file_size
        else 0
    )
    return rx_pb2.ManifestSeen(
        receiver_id=1,
        manifest=common_pb2.Manifest(
            session_id=session_id,
            filepath=filepath,
            file_size=file_size,
            file_hash=b"\x00" * 32,
            k=k,
            n=n,
            block_bytes=symbol_bytes,
            total_blocks=resolved_blocks,
        ),
    )


@dataclass
class _Rig:
    authority: SessionAuthority
    lock: FakeFileLock
    shm: FakeShm
    store: FakeFileStore
    journal: FakeJournal
    spec_store: FakeSessionSpecStore
    clock: FakeClock
    # session_id -> (decoded_count, last_progress_at, missing preview) that
    # progress_of returns -- the shape the real aggregator gives. A session
    # absent here is "not registered with progress tracking" (a lost sidecar).
    tracked: dict[SessionId, tuple[int, float, tuple[BlockId, ...]]] = field(default_factory=dict)
    broadcasts: list[bytes] = field(default_factory=list)
    sends: list[tuple[ReceiverId, bytes]] = field(default_factory=list)
    opened: list[tuple[SessionSpec, frozenset[BlockId]]] = field(default_factory=list)
    # (session_id, quarantine_path) each time the authority marked a session
    # INCOMPLETE. `incomplete` keeps just the ids for the common assertion.
    incomplete_marks: list[tuple[SessionId, str]] = field(default_factory=list)

    @property
    def incomplete(self) -> list[SessionId]:
        return [session_id for session_id, _ in self.incomplete_marks]

    def set_progress(
        self,
        session_id: str,
        decoded: int,
        *,
        last_block_at: float | None = None,
        missing: tuple[int, ...] = (),
    ) -> None:
        # Default: "a block just landed" -- the real aggregator stamps
        # last_progress_at at the moment of growth.
        stamped = self.clock.now() if last_block_at is None else last_block_at
        self.tracked[SessionId(session_id)] = (
            decoded,
            stamped,
            tuple(BlockId(block_id) for block_id in missing),
        )


def _rig(
    *,
    alive: bool = False,
    lock: FakeFileLock | None = None,
    shm: FakeShm | None = None,
    journal: FakeJournal | None = None,
    spec_store: FakeSessionSpecStore | None = None,
    store: FakeFileStore | None = None,
    sweep_interval_s: float = 0.0,
    stall_timeout_s: float = STALL_TIMEOUT,
    broadcast_gate: asyncio.Event | None = None,
) -> _Rig:
    lock = lock or FakeFileLock()
    shm = shm or FakeShm(probe_receiver_alive=lambda: alive)
    store = store or FakeFileStore()
    journal = journal or FakeJournal()
    spec_store = spec_store or FakeSessionSpecStore()
    clock = FakeClock()
    tracked: dict[SessionId, tuple[int, float, tuple[BlockId, ...]]] = {}
    broadcasts: list[bytes] = []
    sends: list[tuple[ReceiverId, bytes]] = []
    opened: list[tuple[SessionSpec, frozenset[BlockId]]] = []
    incomplete_marks: list[tuple[SessionId, str]] = []

    async def broadcast(payload: bytes) -> None:
        # broadcast_gate set -> broadcasts pass; cleared -> broadcasts wedge.
        if broadcast_gate is not None:
            await broadcast_gate.wait()
        broadcasts.append(payload)

    async def send_to_receiver(receiver_id: ReceiverId, payload: bytes) -> None:
        sends.append((receiver_id, payload))

    def on_session_opened(spec: SessionSpec, decoded: Collection[BlockId]) -> None:
        opened.append((spec, frozenset(decoded)))

    def progress_of(session_id: SessionId) -> tuple[int, float, tuple[BlockId, ...]] | None:
        return tracked.get(session_id)

    authority = SessionAuthority(
        file_lock=lock,
        shm=shm,
        file_store=store,
        journal=journal,
        spec_store=spec_store,
        broadcast=broadcast,
        send_to_receiver=send_to_receiver,
        on_session_opened=on_session_opened,
        clock=clock,
        progress_of=progress_of,
        on_incomplete=lambda session_id, path: incomplete_marks.append((session_id, path)),
        quarantine_incomplete=Publisher(store).quarantine_incomplete,
        shm_name=SHM_NAME,
        staging_dir="/var/nexus/staging",
        journal_dir="/var/nexus/journal",
        arena_bytes=ARENA_BYTES,
        session_region_base=REGION_BASE,
        sweep_interval_s=sweep_interval_s,
        stall_timeout_s=stall_timeout_s,
    )
    return _Rig(
        authority,
        lock,
        shm,
        store,
        journal,
        spec_store,
        clock,
        tracked,
        broadcasts,
        sends,
        opened,
        incomplete_marks,
    )


async def test_three_simultaneous_manifest_seen_produce_one_session_open() -> None:
    rig = _rig()
    await rig.authority.start()

    manifest = _manifest_seen("s-1")
    await rig.authority.handle_manifest_seen(R1, manifest)
    await rig.authority.handle_manifest_seen(R2, manifest)
    await rig.authority.handle_manifest_seen(R3, manifest)

    # One create, one broadcast; the two duplicates are answered directly.
    assert len(rig.broadcasts) == 1
    field_name, decoded = codec.decode(rig.broadcasts[0])
    session_open = cast(Any, decoded)
    assert field_name == "session_open"
    assert session_open.session_id == "s-1"
    assert session_open.block_bytes == SYMBOL_BYTES
    assert [receiver_id for receiver_id, _ in rig.sends] == [R2, R3]
    for _, payload in rig.sends:
        name, duplicate_reply = codec.decode(payload)
        assert name == "session_open"
        assert cast(Any, duplicate_reply).session_id == "s-1"
    assert [session.session_id for session in rig.shm.open_sessions()] == [SessionId("s-1")]
    assert rig.store.allocated == [("sub/dir/output.bin", FILE_SIZE)]


async def test_handle_manifest_seen_persists_the_spec_and_registers_progress() -> None:
    rig = _rig()
    await rig.authority.start()

    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))

    saved = rig.spec_store.load(SessionId("s-1"))
    assert saved is not None
    assert saved.relpath == "sub/dir/output.bin"
    assert saved.total_blocks == TOTAL_BLOCKS
    assert [(spec.session_id, decoded) for spec, decoded in rig.opened] == [
        (SessionId("s-1"), frozenset())
    ]


async def test_a_second_authority_refuses_to_start_while_the_lock_is_held() -> None:
    rig = _rig(lock=FakeFileLock(held_by=4242))

    with pytest.raises(LockHeldError) as excinfo:
        await rig.authority.start()

    assert excinfo.value.holder_pid == 4242
    assert rig.shm.last_decision is None


async def test_start_with_no_prior_segment_creates_it_clean() -> None:
    rig = _rig()
    await rig.authority.start()

    assert rig.shm.last_decision is AdoptDecision.CREATED
    assert rig.authority.adopted() is False
    assert rig.opened == []


async def test_adopting_a_live_segment_leaves_its_bytes_intact_and_recovers_sessions() -> None:
    shared_shm = FakeShm(probe_receiver_alive=lambda: alive[0])
    shared_spec_store = FakeSessionSpecStore()
    alive = [False]

    first = _rig(shm=shared_shm, spec_store=shared_spec_store)
    await first.authority.start()
    await first.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    shared_shm.mark_block_decoded(SessionId("s-1"), BlockId(0))
    shared_shm.mark_block_decoded(SessionId("s-1"), BlockId(2))
    bytes_before_restart = shared_shm.payload_bytes()
    assert any(bytes_before_restart)

    alive[0] = True
    recovering_journal = FakeJournal()
    recovering_journal.preload(SessionId("s-1"), [BlockId(0), BlockId(2)])
    second = _rig(
        shm=shared_shm,
        journal=recovering_journal,
        spec_store=shared_spec_store,
        store=first.store,  # the staged file first allocated must still be visible
    )

    await second.authority.start()

    assert second.shm.last_decision is AdoptDecision.ADOPTED
    assert second.authority.adopted() is True
    assert shared_shm.payload_bytes() == bytes_before_restart
    # _recover() hands the session to progress tracking on the same
    # on_session_opened path a fresh session takes.
    recovered = {spec.session_id: (spec, decoded) for spec, decoded in second.opened}
    assert set(recovered) == {SessionId("s-1")}
    spec, decoded = recovered[SessionId("s-1")]
    assert decoded == frozenset({BlockId(0), BlockId(2)})
    assert spec.relpath == "sub/dir/output.bin"
    assert spec.total_blocks == TOTAL_BLOCKS
    assert second.authority.was_recovered(SessionId("s-1")) is True
    assert first.authority.was_recovered(SessionId("s-1")) is False  # created, not recovered

    await second.authority.handle_manifest_seen(R2, _manifest_seen("s-1"))
    assert second.broadcasts == []  # a duplicate is answered directly, not re-broadcast
    assert [receiver_id for receiver_id, _ in second.sends] == [R2]


async def test_send_config_to_carries_the_shm_name_and_dirs() -> None:
    rig = _rig()
    await rig.authority.start()

    await rig.authority.send_config_to(R1)

    assert len(rig.sends) == 1
    receiver_id, payload = rig.sends[0]
    assert receiver_id == R1
    field_name, message = codec.decode(payload)
    assert field_name == "config"
    assert cast(Any, message).shm_name == SHM_NAME
    assert cast(Any, message).staging_dir == "/var/nexus/staging"
    assert cast(Any, message).journal_dir == "/var/nexus/journal"


async def test_a_session_with_a_lost_spec_sidecar_is_known_but_untracked() -> None:
    shared_shm = FakeShm(probe_receiver_alive=lambda: alive[0])
    shared_spec_store = FakeSessionSpecStore()
    alive = [False]

    first = _rig(shm=shared_shm, spec_store=shared_spec_store)
    await first.authority.start()
    await first.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    shared_spec_store.drop(SessionId("s-1"))  # simulate a lost/corrupt sidecar

    alive[0] = True
    second = _rig(shm=shared_shm, spec_store=shared_spec_store)
    with capture_logs() as logs:
        await second.authority.start()

    assert any(entry["event"] == "session_spec_missing_on_recovery" for entry in logs)
    assert second.opened == []  # nothing to hand to progress tracking without a spec

    # Still treated as known -- a resent manifest must not re-init_session
    # and zero a bitmap a live receiver is writing into. With no spec there
    # is nothing to build a SessionOpen from, so the receiver gets no reply.
    await second.authority.handle_manifest_seen(R2, _manifest_seen("s-1"))
    assert second.broadcasts == []
    assert second.sends == []


async def test_recover_refuses_a_session_whose_staged_file_is_gone() -> None:
    # sidecar + journal + shm entry all present, but the staged partial is not
    # -- the signature of a prior run that purged this session INCOMPLETE
    # (moving the partial to quarantine/) then died before _tear_down.
    shared_shm = FakeShm(probe_receiver_alive=lambda: alive[0])
    shared_spec_store = FakeSessionSpecStore()
    shared_store = FakeFileStore()
    shared_journal = FakeJournal()
    alive = [False]

    first = _rig(
        shm=shared_shm, spec_store=shared_spec_store, store=shared_store, journal=shared_journal
    )
    await first.authority.start()
    await first.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    shared_journal.preload(SessionId("s-1"), [BlockId(0), BlockId(2)])
    shared_store.staged.discard("sub/dir/output.bin")  # the partial is gone

    alive[0] = True
    second = _rig(
        shm=shared_shm, spec_store=shared_spec_store, store=shared_store, journal=shared_journal
    )
    with capture_logs() as logs:
        await second.authority.start()

    missing = [e for e in logs if e["event"] == "session_staged_file_missing_on_recovery"]
    assert missing
    assert missing[0]["staged_path"] == str(shared_store.staged_path("sub/dir/output.bin"))
    assert "quarantine/" in missing[0]["remedy"] and "sidecar" in missing[0]["remedy"]

    # not adopted: invisible to progress tracking and to the sweep
    assert second.opened == []
    assert second.authority.was_recovered(SessionId("s-1")) is False
    assert second.broadcasts == []
    await second.authority.send_open_sessions_to(R2)
    assert second.sends == []  # no SessionOpen for a session with no bytes

    # sidecar, journal and shm entry left in place for manual recovery
    assert shared_spec_store.load(SessionId("s-1")) is not None
    assert [b async for b in shared_journal.replay(SessionId("s-1"))] == [BlockId(0), BlockId(2)]
    assert [s.session_id for s in shared_shm.open_sessions()] == [SessionId("s-1")]


async def test_a_fallocate_failure_leaves_no_session_in_shm_and_no_broadcast() -> None:
    rig = _rig()
    await rig.authority.start()
    rig.store.fail_next_allocate(OSError("ENOSPC"))

    with pytest.raises(OSError, match="ENOSPC"):
        await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))

    assert rig.shm.open_sessions() == ()
    assert rig.broadcasts == []
    assert rig.sends == []
    assert rig.opened == []


_REJECTED: list[tuple[str, dict[str, Any], str]] = [
    ("k_ge_n", {"k": 255, "n": 255}, r"k \(255\) must be < n"),
    ("total_blocks_mismatch", {"total_blocks": 99}, "total_blocks 99 does not match"),
    ("relpath_absolute", {"filepath": "/etc/passwd"}, "absolute or escapes"),
    ("relpath_traversal", {"filepath": "../../etc/passwd"}, "absolute or escapes"),
    ("relpath_backslash", {"filepath": "sub\\..\\x"}, "absolute or escapes"),
]


@pytest.mark.parametrize(
    ("label", "overrides", "match"), _REJECTED, ids=[case[0] for case in _REJECTED]
)
async def test_manifest_validation_rejects_bad_input(
    label: str, overrides: dict[str, Any], match: str
) -> None:
    rig = _rig()
    await rig.authority.start()

    with pytest.raises(ManifestRejected, match=match):
        await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1", **overrides))

    assert rig.shm.open_sessions() == ()
    assert rig.broadcasts == []
    assert rig.opened == []


async def test_two_distinct_sessions_get_non_overlapping_regions() -> None:
    rig = _rig()
    await rig.authority.start()

    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-2", filepath="other.bin"))

    sessions = {session.session_id: session for session in rig.shm.open_sessions()}
    a, b = sessions[SessionId("s-1")], sessions[SessionId("s-2")]
    assert b.block_table_offset >= a.bitmap_offset + math.ceil(a.total_blocks / 8)
    assert len(rig.broadcasts) == 2


async def test_send_open_sessions_to_replays_every_open_session_to_one_receiver() -> None:
    rig = _rig()
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-2", filepath="other.bin"))
    rig.sends.clear()

    await rig.authority.send_open_sessions_to(R2)

    assert {receiver_id for receiver_id, _ in rig.sends} == {R2}
    replayed = set()
    for _, payload in rig.sends:
        name, message = codec.decode(payload)
        assert name == "session_open"
        replayed.add(cast(Any, message).session_id)
    assert replayed == {"s-1", "s-2"}


async def test_send_open_sessions_to_is_a_noop_when_nothing_is_open() -> None:
    rig = _rig()
    await rig.authority.start()

    await rig.authority.send_open_sessions_to(R2)

    assert rig.sends == []


# --- the completion decision, routed through purge_policy -------------------


async def test_classify_completion_is_published_when_the_hash_matches() -> None:
    rig = _rig()
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))

    assert (
        rig.authority.classify_completion(SessionId("s-1"), hash_ok=True) is PurgeReason.PUBLISHED
    )


async def test_classify_completion_is_quarantined_when_the_hash_fails() -> None:
    rig = _rig()
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))

    assert (
        rig.authority.classify_completion(SessionId("s-1"), hash_ok=False)
        is PurgeReason.QUARANTINED
    )


async def test_purge_broadcasts_purge_session_once_then_dedupes() -> None:
    rig = _rig()
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    rig.broadcasts.clear()

    await rig.authority.purge(SessionId("s-1"), PurgeReason.PUBLISHED)
    await rig.authority.purge(SessionId("s-1"), PurgeReason.PUBLISHED)  # already purged

    assert len(rig.broadcasts) == 1
    field_name, message = codec.decode(rig.broadcasts[0])
    assert field_name == "purge_session"
    assert cast(Any, message).session_id == "s-1"
    assert cast(Any, message).reason == "verified"  # the wire string is unchanged
    assert rig.authority.is_purged(SessionId("s-1")) is True


async def test_purge_tears_down_the_durable_footprint_so_a_restart_cannot_re_adopt() -> None:
    rig = _rig()
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    assert [s.session_id for s in rig.shm.open_sessions()] == [SessionId("s-1")]
    assert rig.spec_store.load(SessionId("s-1")) is not None

    await rig.authority.purge(SessionId("s-1"), PurgeReason.PUBLISHED)

    assert rig.shm.open_sessions() == ()  # gone from the segment's session table
    assert rig.spec_store.load(SessionId("s-1")) is None  # sidecar deleted
    assert rig.journal.purged == [SessionId("s-1")]  # journal file removed


async def test_a_resent_manifest_for_a_purged_session_is_ignored_not_resurrected() -> None:
    rig = _rig()
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    await rig.authority.purge(SessionId("s-1"), PurgeReason.INCOMPLETE)
    rig.broadcasts.clear()
    rig.sends.clear()

    with capture_logs() as logs:
        await rig.authority.handle_manifest_seen(R2, _manifest_seen("s-1"))

    assert any(entry["event"] == "manifest_seen_for_purged_session" for entry in logs)
    assert rig.broadcasts == []  # no new SessionOpen
    assert rig.sends == []
    assert rig.shm.open_sessions() == ()  # not re-created


async def test_re_dropping_the_same_file_opens_a_fresh_session_after_the_first_purges() -> None:
    # file-monitor's generate_session_id() is secrets.token_hex(8) per dispatch,
    # so a re-drop of the same file arrives with a NEW session_id -- the
    # purged-guard keys on the old id and does not block it.
    rig = _rig()
    await rig.authority.start()

    same_file = "reports/quarterly.bin"
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("transfer-1", filepath=same_file))
    await rig.authority.purge(SessionId("transfer-1"), PurgeReason.PUBLISHED)
    rig.broadcasts.clear()

    await rig.authority.handle_manifest_seen(R1, _manifest_seen("transfer-2", filepath=same_file))

    assert len(rig.broadcasts) == 1  # a fresh SessionOpen went out
    field_name, message = codec.decode(rig.broadcasts[0])
    assert field_name == "session_open"
    assert cast(Any, message).session_id == "transfer-2"
    assert [s.session_id for s in rig.shm.open_sessions()] == [SessionId("transfer-2")]


# --- the periodic sweep ---------------------------------------------------


async def test_a_session_that_never_received_a_block_is_swept_to_incomplete() -> None:
    rig = _rig(stall_timeout_s=30.0)
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    rig.set_progress("s-1", decoded=0, last_block_at=0.0, missing=(0, 1, 2))
    rig.broadcasts.clear()

    rig.clock.advance(29.0)
    await rig.authority._sweep_once()
    assert rig.broadcasts == []  # still inside the grace window

    rig.clock.advance(2.0)  # now 31s of silence
    with capture_logs() as logs:
        await rig.authority._sweep_once()

    stalled = [entry for entry in logs if entry["event"] == "session_stalled"]
    assert stalled and stalled[0]["session_id"] == SessionId("s-1")
    assert stalled[0]["missing_block_count"] == 3
    field_name, message = codec.decode(rig.broadcasts[-1])
    assert field_name == "purge_session"
    assert cast(Any, message).reason == "incomplete"
    # marked INCOMPLETE with the path the partial was actually moved to
    [(marked_id, marked_path)] = rig.incomplete_marks
    assert marked_id == SessionId("s-1")
    assert "quarantine" in marked_path and marked_path.endswith("output.s-1.bin")


async def test_a_session_that_went_silent_mid_transfer_is_swept_once_across_ticks() -> None:
    rig = _rig(stall_timeout_s=30.0)
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1", file_size=BIG_FILE_SIZE))

    # got to 4 blocks, last one at t=10, then nothing more
    rig.set_progress("s-1", decoded=4, last_block_at=10.0, missing=(4, 5))
    rig.broadcasts.clear()
    rig.clock.advance(20.0)
    await rig.authority._sweep_once()  # t=20, 10s idle -- still live
    assert rig.broadcasts == []

    rig.clock.advance(25.0)  # t=45, 35s idle
    await rig.authority._sweep_once()
    await rig.authority._sweep_once()
    await rig.authority._sweep_once()

    purges = [codec.decode(payload) for payload in rig.broadcasts]
    assert [field_name for field_name, _ in purges] == ["purge_session"]  # exactly one
    assert rig.incomplete == [SessionId("s-1")]


async def test_a_still_progressing_session_is_left_alone_by_the_sweep() -> None:
    rig = _rig(stall_timeout_s=30.0)
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1", file_size=BIG_FILE_SIZE))
    rig.broadcasts.clear()

    for tick in range(1, 6):
        rig.set_progress("s-1", decoded=tick, missing=tuple(range(tick, 12)))
        rig.clock.advance(20.0)  # < stall_timeout each tick, but > it cumulatively
        await rig.authority._sweep_once()

    assert rig.broadcasts == []
    assert rig.incomplete == []


async def test_a_complete_but_unverified_session_is_not_swept() -> None:
    rig = _rig(stall_timeout_s=30.0)
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    rig.set_progress("s-1", decoded=TOTAL_BLOCKS, missing=())  # every block in
    rig.broadcasts.clear()

    rig.clock.advance(10_000.0)  # long past any stall timeout
    await rig.authority._sweep_once()

    # terminal_reason(complete, hash_ok=None) is None -- the verifier owns this
    assert rig.broadcasts == []
    assert rig.incomplete == []


async def test_an_untracked_session_is_logged_loudly_then_purged_after_the_timeout() -> None:
    # In _specs but progress_of() is None -- the aggregator never registered
    # it. A wiring bug; must not silently leak a staging file + shm slot.
    rig = _rig(stall_timeout_s=30.0)
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    # deliberately no rig.set_progress("s-1", ...) -> progress_of returns None
    rig.broadcasts.clear()

    with capture_logs() as first:
        await rig.authority._sweep_once()  # noticed now; grace starts
    assert any(e["event"] == "session_untracked_by_aggregator" for e in first)
    assert rig.broadcasts == []  # not purged yet

    rig.clock.advance(29.0)
    await rig.authority._sweep_once()
    assert rig.broadcasts == []  # still inside the grace window

    rig.clock.advance(2.0)  # 31s since first noticed
    with capture_logs() as later:
        await rig.authority._sweep_once()

    assert any(e["event"] == "purging_untracked_session" for e in later)
    assert [codec.decode(p)[0] for p in rig.broadcasts] == ["purge_session"]
    assert rig.authority.is_purged(SessionId("s-1")) is True
    assert rig.shm.open_sessions() == ()  # footprint torn down
    # Even here the partial is preserved: nothing was journaled (the
    # aggregator never registered it), so the report is "everything missing".
    assert rig.store.quarantined == ["sub/dir/output.s-1.bin"]
    report = rig.store.incomplete_reports["sub/dir/output.s-1.bin"]
    assert report.decoded_blocks == 0
    assert report.missing_block_ids == tuple(BlockId(i) for i in range(TOTAL_BLOCKS))


async def test_an_untracked_session_that_gets_registered_before_the_timeout_is_left_alone() -> None:
    rig = _rig(stall_timeout_s=30.0)
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    rig.broadcasts.clear()

    await rig.authority._sweep_once()  # untracked -- grace starts
    rig.clock.advance(20.0)
    rig.set_progress("s-1", decoded=1, missing=(1,))  # the aggregator catches up
    await rig.authority._sweep_once()
    rig.clock.advance(20.0)  # 40s since first noticed, but only 20s idle
    await rig.authority._sweep_once()

    assert rig.broadcasts == []
    assert rig.incomplete == []


async def test_one_bad_session_evaluation_does_not_stop_the_sweep() -> None:
    rig = _rig(stall_timeout_s=30.0)
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-2", filepath="other.bin"))
    # s-1's progress_of raises; s-2 is a clean silent session
    rig.tracked[SessionId("s-1")] = cast(Any, "not a tuple")  # unpacking blows up
    rig.set_progress("s-2", decoded=0, missing=(0, 1))
    rig.broadcasts.clear()

    rig.clock.advance(31.0)
    with capture_logs() as logs:
        await rig.authority._sweep_once()

    assert any(entry["event"] == "session_sweep_evaluation_failed" for entry in logs)
    assert rig.incomplete == [SessionId("s-2")]  # the sweep carried on past s-1


async def test_the_sweep_keys_its_stall_clock_off_progress_of_not_wall_time() -> None:
    # A recovered session is safe because the real aggregator's
    # register_session (called from _recover) stamps last_progress_at at the
    # adoption instant -- see test_aggregator's
    # test_a_recovered_session_last_progress_is_the_registration_instant_not_zero.
    # Here we prove the sweep honours that stamp: the same wall-clock instant
    # is live with a fresh stamp and terminal with a stale one.
    rig = _rig(stall_timeout_s=30.0)
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    rig.clock.advance(10_000.0)  # a long-running manager

    rig.set_progress("s-1", decoded=2, last_block_at=10_000.0, missing=(2,))  # fresh
    rig.broadcasts.clear()
    rig.clock.advance(29.0)
    await rig.authority._sweep_once()
    assert rig.broadcasts == []  # 29s idle < 30s

    rig.set_progress("s-1", decoded=2, last_block_at=9_000.0, missing=(2,))  # stale
    await rig.authority._sweep_once()
    assert [codec.decode(p)[0] for p in rig.broadcasts] == ["purge_session"]


async def test_a_swept_session_quarantines_its_partial_with_the_journal_missing_list() -> None:
    rig = _rig(stall_timeout_s=30.0)
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1", file_size=BIG_FILE_SIZE))
    # 5 of 12 blocks landed, with a non-contiguous gap -- this is the record
    # the sidecar must reproduce exactly, and it comes from the journal, not
    # progress_of()'s 32-capped preview.
    got = [BlockId(0), BlockId(2), BlockId(5), BlockId(6), BlockId(9)]
    rig.journal.preload(SessionId("s-1"), got)
    rig.set_progress("s-1", decoded=len(got), last_block_at=10.0, missing=(1, 3))
    rig.broadcasts.clear()

    rig.clock.advance(45.0)  # 35s past the last block
    await rig.authority._sweep_once()

    assert rig.store.quarantined == ["sub/dir/output.s-1.bin"]  # session id before the extension
    assert "sub/dir/output.bin" not in rig.store.staged
    report = rig.store.incomplete_reports["sub/dir/output.s-1.bin"]
    assert report == IncompleteReport(
        session_id=SessionId("s-1"),
        total_blocks=12,
        decoded_blocks=5,
        missing_block_ids=(
            BlockId(1),
            BlockId(3),
            BlockId(4),
            BlockId(7),
            BlockId(8),
            BlockId(10),
            BlockId(11),
        ),
    )
    assert rig.journal.purged == [SessionId("s-1")]  # journal unlinked only after the report


async def test_the_incomplete_report_is_written_before_tear_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # If the process dies (here: _tear_down raises) after the report is
    # written but before the journal is unlinked, the quarantined partial is
    # still interpretable and the journal survives for manual recovery.
    rig = _rig(stall_timeout_s=30.0)
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1", file_size=BIG_FILE_SIZE))
    rig.journal.preload(SessionId("s-1"), [BlockId(0), BlockId(1)])
    rig.broadcasts.clear()

    def _boom(_session_id: SessionId) -> None:
        raise RuntimeError("crashed mid-teardown")

    monkeypatch.setattr(rig.authority, "_tear_down", _boom)

    with pytest.raises(RuntimeError, match="crashed mid-teardown"):
        await rig.authority.purge(SessionId("s-1"), PurgeReason.INCOMPLETE)

    assert "sub/dir/output.s-1.bin" in rig.store.incomplete_reports  # step 2 completed
    assert rig.store.incomplete_reports["sub/dir/output.s-1.bin"].decoded_blocks == 2
    assert rig.journal.purged == []  # step 3 never ran -- journal still on disk


async def test_two_incomplete_transfers_of_one_file_get_separate_partials_and_reports() -> None:
    # A file dropped, purged INCOMPLETE, then re-dropped and purged again in one
    # manager run: each has its own session id, so neither partial nor report
    # clobbers the other's.
    rig = _rig(stall_timeout_s=30.0)
    await rig.authority.start()
    same_file = "reports/quarterly.bin"

    for session_id, decoded in (
        ("transfer-1", [BlockId(0)]),
        ("transfer-2", [BlockId(0), BlockId(1)]),
    ):
        await rig.authority.handle_manifest_seen(
            R1, _manifest_seen(session_id, filepath=same_file, file_size=BIG_FILE_SIZE)
        )
        rig.journal.preload(SessionId(session_id), decoded)
        rig.set_progress(session_id, decoded=len(decoded), last_block_at=0.0)
        rig.clock.advance(31.0)
        await rig.authority._sweep_once()

    assert sorted(rig.store.quarantined) == [
        "reports/quarterly.transfer-1.bin",
        "reports/quarterly.transfer-2.bin",
    ]
    assert rig.store.incomplete_reports["reports/quarterly.transfer-1.bin"].decoded_blocks == 1
    assert rig.store.incomplete_reports["reports/quarterly.transfer-2.bin"].decoded_blocks == 2


# --- stopping the sweep loop --------------------------------------------


async def test_stop_is_a_noop_when_the_sweep_was_never_started() -> None:
    rig = _rig()  # sweep_interval_s = 0 -> start() creates no task
    await rig.authority.start()

    await rig.authority.stop()
    await rig.authority.stop()  # idempotent


async def test_stop_returns_even_while_a_sweep_tick_is_blocked_in_broadcast() -> None:
    gate = asyncio.Event()
    gate.set()  # broadcasts pass while we set the session up
    rig = _rig(sweep_interval_s=0.01, stall_timeout_s=1.0, broadcast_gate=gate)
    await rig.authority.start()
    await rig.authority.handle_manifest_seen(R1, _manifest_seen("s-1"))
    rig.set_progress("s-1", decoded=0, missing=(0, 1, 2))
    rig.clock.advance(5.0)  # past the stall timeout -> the next tick calls purge()

    gate.clear()  # now broadcasts wedge -- the sweep's purge() will hang in broadcast()
    await asyncio.sleep(0.05)  # let a sweep tick start and wedge inside broadcast()

    await asyncio.wait_for(rig.authority.stop(), timeout=1.0)  # must not hang on the gate
