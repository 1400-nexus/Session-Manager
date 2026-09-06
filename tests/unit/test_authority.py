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
from session_manager.domain.models import SessionSpec
from session_manager.ipc import codec
from session_manager.services.authority import SessionAuthority
from session_manager.services.errors import ManifestRejected
from tests.fakes.fake_file_lock import FakeFileLock
from tests.fakes.fake_file_store import FakeFileStore
from tests.fakes.fake_journal import FakeJournal
from tests.fakes.fake_session_spec_store import FakeSessionSpecStore
from tests.fakes.fake_shm import FakeShm

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
    broadcasts: list[bytes] = field(default_factory=list)
    sends: list[tuple[ReceiverId, bytes]] = field(default_factory=list)
    opened: list[tuple[SessionSpec, frozenset[BlockId]]] = field(default_factory=list)


def _rig(
    *,
    alive: bool = False,
    lock: FakeFileLock | None = None,
    shm: FakeShm | None = None,
    journal: FakeJournal | None = None,
    spec_store: FakeSessionSpecStore | None = None,
) -> _Rig:
    lock = lock or FakeFileLock()
    shm = shm or FakeShm(probe_receiver_alive=lambda: alive)
    store = FakeFileStore()
    journal = journal or FakeJournal()
    spec_store = spec_store or FakeSessionSpecStore()
    broadcasts: list[bytes] = []
    sends: list[tuple[ReceiverId, bytes]] = []
    opened: list[tuple[SessionSpec, frozenset[BlockId]]] = []

    async def broadcast(payload: bytes) -> None:
        broadcasts.append(payload)

    async def send_to_receiver(receiver_id: ReceiverId, payload: bytes) -> None:
        sends.append((receiver_id, payload))

    def on_session_opened(spec: SessionSpec, decoded: Collection[BlockId]) -> None:
        opened.append((spec, frozenset(decoded)))

    authority = SessionAuthority(
        file_lock=lock,
        shm=shm,
        file_store=store,
        journal=journal,
        spec_store=spec_store,
        broadcast=broadcast,
        send_to_receiver=send_to_receiver,
        on_session_opened=on_session_opened,
        shm_name=SHM_NAME,
        staging_dir="/var/nexus/staging",
        journal_dir="/var/nexus/journal",
        arena_bytes=ARENA_BYTES,
        session_region_base=REGION_BASE,
    )
    return _Rig(authority, lock, shm, store, journal, spec_store, broadcasts, sends, opened)


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
    second = _rig(shm=shared_shm, journal=recovering_journal, spec_store=shared_spec_store)

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
