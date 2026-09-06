import uuid
from collections.abc import Iterator
from multiprocessing import shared_memory
from typing import Protocol

import pytest

from session_manager.adapters.constants import SHM_SESSION_TABLE_OFFSET
from session_manager.adapters.posix_shm import PosixShm, detach_resource_tracker
from session_manager.adapters.shm_layout import HEADER_SIZE, AdoptDecision, build_header
from session_manager.domain.ids import SessionId
from session_manager.domain.models import SessionSpec
from session_manager.ports.protocols import ShmReader, ShmWriter
from tests.fakes.fake_shm import FakeShm, SegmentState

ARENA_BYTES = 4096
SLOT_BYTES = 256
SEEDED_PAYLOAD = b"receiver-owned-slot-state-do-not-wipe"
SESSION = SessionId("s-1")


def _spec() -> SessionSpec:
    return SessionSpec(
        session_id=SESSION,
        relpath="sub/file.bin",
        file_size=1_000_000,
        file_hash=b"\x00" * 32,
        k=200,
        n=255,
        symbol_bytes=1400,
        total_blocks=16,
    )


class _Shm(ShmReader, ShmWriter, Protocol):
    last_decision: AdoptDecision | None


class Harness(Protocol):
    name: str
    alive: list[bool]

    def make(self) -> _Shm: ...

    def ensure_absent(self) -> None: ...

    def prime(self, *, valid: bool, payload: bytes) -> None: ...

    def read_payload(self) -> bytes: ...

    def cleanup(self) -> None: ...


class FakeHarness:
    name = "fake-segment"

    def __init__(self) -> None:
        self.alive: list[bool] = [False]
        self._fake: FakeShm | None = None

    def make(self) -> _Shm:
        self._fake = FakeShm(probe_receiver_alive=lambda: self.alive[0])
        return self._fake

    def ensure_absent(self) -> None:
        self._require().set_existing_segment(SegmentState.ABSENT, arena_bytes=ARENA_BYTES)

    def prime(self, *, valid: bool, payload: bytes) -> None:
        fake = self._require()
        state = SegmentState.STALE if valid else SegmentState.INCOMPATIBLE
        fake.set_existing_segment(state, arena_bytes=ARENA_BYTES)
        fake.seed_payload(payload)

    def read_payload(self) -> bytes:
        return self._require().payload_bytes()

    def cleanup(self) -> None:
        pass

    def _require(self) -> FakeShm:
        if self._fake is None:
            raise RuntimeError("make() must be called first")
        return self._fake


class PosixHarness:
    def __init__(self) -> None:
        self.name = f"nx-{uuid.uuid4().hex[:20]}"
        self.alive: list[bool] = [False]
        self._prime_handle: shared_memory.SharedMemory | None = None
        self._shms: list[PosixShm] = []

    def make(self) -> _Shm:
        shm = PosixShm(slot_bytes=SLOT_BYTES, probe_receiver_alive=lambda: self.alive[0])
        self._shms.append(shm)
        return shm

    def ensure_absent(self) -> None:
        self._drop_prime_handle()

    def prime(self, *, valid: bool, payload: bytes) -> None:
        self._drop_prime_handle()
        handle = shared_memory.SharedMemory(name=self.name, create=True, size=ARENA_BYTES)
        detach_resource_tracker(handle)
        buffer = handle.buf
        assert buffer is not None
        if valid:
            buffer[:HEADER_SIZE] = build_header(
                b"\x00" * 16, 4242, SLOT_BYTES, ARENA_BYTES // SLOT_BYTES
            )
        else:
            buffer[:HEADER_SIZE] = b"XXXX" + bytes(HEADER_SIZE - 4)
        buffer[HEADER_SIZE : HEADER_SIZE + len(payload)] = payload
        self._prime_handle = handle

    def read_payload(self) -> bytes:
        reader = shared_memory.SharedMemory(name=self.name, create=False)
        detach_resource_tracker(reader)
        try:
            buffer = reader.buf
            assert buffer is not None
            return bytes(buffer[HEADER_SIZE:])
        finally:
            reader.close()

    def cleanup(self) -> None:
        for shm in self._shms:
            try:
                shm.close(unlink=False)
            except (RuntimeError, BufferError):
                pass
        self._drop_prime_handle()
        try:
            orphan = shared_memory.SharedMemory(name=self.name, create=False)
            detach_resource_tracker(orphan)
            orphan.close()
            orphan.unlink()
        except FileNotFoundError:
            pass

    def _drop_prime_handle(self) -> None:
        handle = self._prime_handle
        self._prime_handle = None
        if handle is not None:
            handle.close()


@pytest.fixture(params=["fake", "posix"])
def harness(request: pytest.FixtureRequest) -> Iterator[Harness]:
    made: Harness = FakeHarness() if request.param == "fake" else PosixHarness()
    try:
        yield made
    finally:
        made.cleanup()


def test_absent_segment_is_created_not_adopted(harness: Harness) -> None:
    shm = harness.make()
    harness.ensure_absent()

    assert shm.create_or_adopt(harness.name, ARENA_BYTES) is False
    assert shm.last_decision is AdoptDecision.CREATED
    assert not any(harness.read_payload())


def test_valid_segment_with_a_live_receiver_is_adopted_without_wiping(harness: Harness) -> None:
    shm = harness.make()
    harness.prime(valid=True, payload=SEEDED_PAYLOAD)
    harness.alive[0] = True
    before = harness.read_payload()
    assert any(before)

    assert shm.create_or_adopt(harness.name, ARENA_BYTES) is True
    assert shm.last_decision is AdoptDecision.ADOPTED
    assert harness.read_payload() == before


def test_valid_segment_without_a_receiver_is_reinitialised(harness: Harness) -> None:
    shm = harness.make()
    harness.prime(valid=True, payload=SEEDED_PAYLOAD)
    harness.alive[0] = False

    assert shm.create_or_adopt(harness.name, ARENA_BYTES) is False
    assert shm.last_decision is AdoptDecision.REINITIALISED
    assert not any(harness.read_payload())


def test_incompatible_header_is_reinitialised_even_with_a_live_receiver(harness: Harness) -> None:
    shm = harness.make()
    harness.prime(valid=False, payload=SEEDED_PAYLOAD)
    harness.alive[0] = True

    assert shm.create_or_adopt(harness.name, ARENA_BYTES) is False
    assert shm.last_decision is AdoptDecision.REINITIALISED
    assert not any(harness.read_payload())


def test_bitmap_for_returns_a_readonly_view_that_rejects_writes(harness: Harness) -> None:
    shm = harness.make()
    harness.ensure_absent()
    shm.create_or_adopt(harness.name, ARENA_BYTES)
    shm.init_session(
        _spec(),
        block_table_offset=SHM_SESSION_TABLE_OFFSET,
        bitmap_offset=SHM_SESSION_TABLE_OFFSET + 64,
    )

    view = shm.bitmap_for(SESSION)

    assert view.readonly is True
    with pytest.raises(TypeError):
        view[0] = 1


def test_a_created_segment_can_be_unlinked_then_created_again(harness: Harness) -> None:
    # close(unlink=True) must undo create_or_adopt cleanly -- detach then
    # SharedMemory.unlink() double-notifies the resource tracker, which used
    # to print a KeyError traceback on every clean shutdown. If the unlink
    # worked, the name is free and a second create succeeds.
    first = harness.make()
    harness.ensure_absent()
    assert first.create_or_adopt(harness.name, ARENA_BYTES) is False
    first.close(unlink=True)

    with pytest.raises(FileNotFoundError):
        shared_memory.SharedMemory(name=harness.name, create=False)

    second = harness.make()
    assert second.create_or_adopt(harness.name, ARENA_BYTES) is False
    assert second.last_decision is AdoptDecision.CREATED
    second.close(unlink=True)
