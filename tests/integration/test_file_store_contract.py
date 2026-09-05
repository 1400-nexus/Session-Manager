from pathlib import Path
from typing import Protocol

import pytest

from session_manager.adapters.local_file_store import LocalFileStore
from session_manager.ports.protocols import FileStore
from tests.fakes.fake_file_store import FakeFileStore

RELPATH = "sub/dir/output.bin"
ALLOCATE_SIZE = 128


class Harness(Protocol):
    def make(self) -> FileStore: ...

    def is_staged(self, relpath: str) -> bool: ...

    def is_published(self, relpath: str) -> bool: ...

    def is_quarantined(self, relpath: str) -> bool: ...


class FakeHarness:
    def __init__(self) -> None:
        self._store = FakeFileStore()

    def make(self) -> FileStore:
        return self._store

    def is_staged(self, relpath: str) -> bool:
        return relpath in self._store.staged

    def is_published(self, relpath: str) -> bool:
        return relpath in self._store.published

    def is_quarantined(self, relpath: str) -> bool:
        return relpath in self._store.quarantined


class LocalHarness:
    def __init__(self, tmp_path: Path) -> None:
        self._staging_dir = tmp_path / "staging"
        self._output_dir = tmp_path / "output"
        self._store = LocalFileStore(self._staging_dir, self._output_dir)

    def make(self) -> FileStore:
        return self._store

    def is_staged(self, relpath: str) -> bool:
        return (self._staging_dir / relpath).is_file()

    def is_published(self, relpath: str) -> bool:
        return (self._output_dir / relpath).is_file()

    def is_quarantined(self, relpath: str) -> bool:
        return (self._staging_dir / "quarantine" / relpath).is_file()


@pytest.fixture(params=["fake", "local"])
def harness(request: pytest.FixtureRequest, tmp_path: Path) -> Harness:
    if request.param == "fake":
        return FakeHarness()
    return LocalHarness(tmp_path)


def test_allocate_stages_the_relpath(harness: Harness) -> None:
    store = harness.make()

    store.allocate(RELPATH, ALLOCATE_SIZE)

    assert harness.is_staged(RELPATH)


def test_staged_path_alone_stages_nothing(harness: Harness) -> None:
    store = harness.make()

    store.staged_path(RELPATH)

    assert not harness.is_staged(RELPATH)


def test_publish_moves_staged_to_published(harness: Harness) -> None:
    store = harness.make()
    store.allocate(RELPATH, ALLOCATE_SIZE)

    store.publish(RELPATH)

    assert harness.is_published(RELPATH)
    assert not harness.is_staged(RELPATH)
    assert not harness.is_quarantined(RELPATH)


def test_quarantine_moves_staged_to_quarantined_never_to_published(harness: Harness) -> None:
    store = harness.make()
    store.allocate(RELPATH, ALLOCATE_SIZE)

    store.quarantine(RELPATH)

    assert harness.is_quarantined(RELPATH)
    assert not harness.is_staged(RELPATH)
    assert not harness.is_published(RELPATH)
