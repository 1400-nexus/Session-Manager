from pathlib import Path
from typing import Protocol

import pytest

from session_manager.adapters.json_session_spec_store import JsonSessionSpecStore
from session_manager.domain.ids import SessionId
from session_manager.domain.models import SessionSpec
from session_manager.ports.protocols import SessionSpecStore
from tests.fakes.fake_session_spec_store import FakeSessionSpecStore

SESSION = SessionId("s-1")


def _spec(session_id: SessionId = SESSION) -> SessionSpec:
    return SessionSpec(
        session_id=session_id,
        relpath="sub/dir/output.bin",
        file_size=1_000_000,
        file_hash=b"\xab" * 32,
        k=200,
        n=255,
        symbol_bytes=1400,
        total_blocks=3,
    )


class Harness(Protocol):
    def make(self) -> SessionSpecStore: ...


class FakeHarness:
    def make(self) -> SessionSpecStore:
        return FakeSessionSpecStore()


class JsonHarness:
    def __init__(self, tmp_path: Path) -> None:
        self._journal_dir = tmp_path / "journal"

    def make(self) -> SessionSpecStore:
        return JsonSessionSpecStore(self._journal_dir)


@pytest.fixture(params=["fake", "json"])
def harness(request: pytest.FixtureRequest, tmp_path: Path) -> Harness:
    if request.param == "fake":
        return FakeHarness()
    return JsonHarness(tmp_path)


def test_save_then_load_round_trips_every_field(harness: Harness) -> None:
    store = harness.make()
    spec = _spec()

    store.save(spec)
    loaded = store.load(SESSION)

    assert loaded == spec


def test_load_of_a_never_saved_session_is_none(harness: Harness) -> None:
    store = harness.make()

    assert store.load(SessionId("ghost")) is None


def test_delete_removes_a_saved_spec(harness: Harness) -> None:
    store = harness.make()
    store.save(_spec())

    store.delete(SESSION)

    assert store.load(SESSION) is None


def test_delete_of_an_unknown_session_is_a_noop(harness: Harness) -> None:
    store = harness.make()

    store.delete(SessionId("ghost"))  # must not raise

    assert store.load(SessionId("ghost")) is None


def test_two_sessions_do_not_collide(harness: Harness) -> None:
    store = harness.make()
    store.save(_spec(SessionId("s-1")))
    store.save(_spec(SessionId("s-2")))

    store.delete(SessionId("s-1"))

    assert store.load(SessionId("s-1")) is None
    assert store.load(SessionId("s-2")) is not None
