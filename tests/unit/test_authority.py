import math
from dataclasses import dataclass, field
from typing import Any, cast

import common_pb2
import pytest
import rx_pb2

from session_manager.adapters.errors import LockHeldError
from session_manager.adapters.shm_layout import AdoptDecision
from session_manager.domain.ids import BlockId, SessionId
from session_manager.ipc import codec
from session_manager.services.authority import SessionAuthority
from session_manager.services.errors import ManifestRejected
from tests.fakes.fake_file_lock import FakeFileLock
from tests.fakes.fake_file_store import FakeFileStore
from tests.fakes.fake_journal import FakeJournal
from tests.fakes.fake_shm import FakeShm

ARENA_BYTES = 1 << 20
REGION_BASE = 8192
SHM_NAME = "nexus-rx-test"

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
    broadcasts: list[bytes] = field(default_factory=list)


def _rig(
    *,
    alive: bool = False,
    lock: FakeFileLock | None = None,
    shm: FakeShm | None = None,
    journal: FakeJournal | None = None,
) -> _Rig:
    lock = lock or FakeFileLock()
    shm = shm or FakeShm(probe_receiver_alive=lambda: alive)
    store = FakeFileStore()
    journal = journal or FakeJournal()
    broadcasts: list[bytes] = []

    async def broadcast(payload: bytes) -> None:
        broadcasts.append(payload)

    authority = SessionAuthority(
        file_lock=lock,
        shm=shm,
        file_store=store,
        journal=journal,
        broadcast=broadcast,
        shm_name=SHM_NAME,
        arena_bytes=ARENA_BYTES,
        session_region_base=REGION_BASE,
    )
    return _Rig(authority, lock, shm, store, journal, broadcasts)


async def test_three_simultaneous_manifest_seen_produce_one_session_open() -> None:
    rig = _rig()
    await rig.authority.start()

    manifest = _manifest_seen("s-1")
    await rig.authority.handle_manifest_seen(manifest)
    await rig.authority.handle_manifest_seen(manifest)
    await rig.authority.handle_manifest_seen(manifest)

    assert len(rig.broadcasts) == 1
    field_name, decoded = codec.decode(rig.broadcasts[0])
    session_open = cast(Any, decoded)
    assert field_name == "session_open"
    assert session_open.session_id == "s-1"
    assert session_open.block_bytes == SYMBOL_BYTES
    assert [session.session_id for session in rig.shm.open_sessions()] == [SessionId("s-1")]
    assert rig.store.allocated == [("sub/dir/output.bin", FILE_SIZE)]


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
    assert rig.authority.recovered_blocks(SessionId("s-1")) == frozenset()


async def test_adopting_a_live_segment_leaves_its_bytes_intact_and_recovers_sessions() -> None:
    shared_shm = FakeShm(probe_receiver_alive=lambda: alive[0])
    alive = [False]

    first = _rig(shm=shared_shm)
    await first.authority.start()
    await first.authority.handle_manifest_seen(_manifest_seen("s-1"))
    shared_shm.mark_block_decoded(SessionId("s-1"), BlockId(0))
    shared_shm.mark_block_decoded(SessionId("s-1"), BlockId(2))
    bytes_before_restart = shared_shm.payload_bytes()
    assert any(bytes_before_restart)

    alive[0] = True
    recovering_journal = FakeJournal()
    recovering_journal.preload(SessionId("s-1"), [BlockId(0), BlockId(2)])
    second = _rig(shm=shared_shm, journal=recovering_journal)

    await second.authority.start()

    assert second.shm.last_decision is AdoptDecision.ADOPTED
    assert second.authority.adopted() is True
    assert shared_shm.payload_bytes() == bytes_before_restart
    assert second.authority.recovered_blocks(SessionId("s-1")) == frozenset(
        {BlockId(0), BlockId(2)}
    )

    await second.authority.handle_manifest_seen(_manifest_seen("s-1"))
    assert second.broadcasts == []


async def test_a_fallocate_failure_leaves_no_session_in_shm_and_no_broadcast() -> None:
    rig = _rig()
    await rig.authority.start()
    rig.store.fail_next_allocate(OSError("ENOSPC"))

    with pytest.raises(OSError, match="ENOSPC"):
        await rig.authority.handle_manifest_seen(_manifest_seen("s-1"))

    assert rig.shm.open_sessions() == ()
    assert rig.broadcasts == []


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
        await rig.authority.handle_manifest_seen(_manifest_seen("s-1", **overrides))

    assert rig.shm.open_sessions() == ()
    assert rig.broadcasts == []


async def test_two_distinct_sessions_get_non_overlapping_regions() -> None:
    rig = _rig()
    await rig.authority.start()

    await rig.authority.handle_manifest_seen(_manifest_seen("s-1"))
    await rig.authority.handle_manifest_seen(_manifest_seen("s-2", filepath="other.bin"))

    sessions = {session.session_id: session for session in rig.shm.open_sessions()}
    a, b = sessions[SessionId("s-1")], sessions[SessionId("s-2")]
    assert b.block_table_offset >= a.bitmap_offset + math.ceil(a.total_blocks / 8)
    assert len(rig.broadcasts) == 2
