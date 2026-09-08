import json
from pathlib import Path
from typing import Protocol

import pytest

from session_manager.adapters.constants import INCOMPLETE_REPORT_FILENAME_SUFFIX
from session_manager.adapters.local_file_store import LocalFileStore
from session_manager.adapters.quarantine_paths import quarantine_name
from session_manager.domain.ids import BlockId, SessionId
from session_manager.domain.models import IncompleteReport
from session_manager.ports.protocols import FileStore
from tests.fakes.fake_file_store import FakeFileStore

RELPATH = "sub/dir/output.bin"
ALLOCATE_SIZE = 128
REPORT = IncompleteReport(
    session_id=SessionId("s-1"),
    total_blocks=5,
    decoded_blocks=3,
    missing_block_ids=(BlockId(1), BlockId(4)),
)
INCOMPLETE_NAME = quarantine_name(RELPATH, REPORT.session_id)
MISMATCH_SESSION = SessionId("m1sm4tch")
MISMATCH_NAME = quarantine_name(RELPATH, MISMATCH_SESSION)


class Harness(Protocol):
    def make(self) -> FileStore: ...

    def is_staged(self, relpath: str) -> bool: ...

    def is_published(self, relpath: str) -> bool: ...

    # `name` here is the on-disk quarantine name: the relpath for quarantine(),
    # the session-id-disambiguated name for quarantine_incomplete().
    def is_quarantined(self, name: str) -> bool: ...

    def incomplete_report(self, name: str) -> dict[str, object] | None: ...


class FakeHarness:
    def __init__(self) -> None:
        self._store = FakeFileStore()

    def make(self) -> FileStore:
        return self._store

    def is_staged(self, relpath: str) -> bool:
        return self._store.staged_file_exists(relpath)

    def is_published(self, relpath: str) -> bool:
        return relpath in self._store.published

    def is_quarantined(self, name: str) -> bool:
        return name in self._store.quarantined

    def incomplete_report(self, name: str) -> dict[str, object] | None:
        report = self._store.incomplete_reports.get(name)
        if report is None:
            return None
        return {
            "session_id": str(report.session_id),
            "total_blocks": report.total_blocks,
            "decoded_blocks": report.decoded_blocks,
            "missing_block_ids": [int(block_id) for block_id in report.missing_block_ids],
        }


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

    def is_quarantined(self, name: str) -> bool:
        return (self._staging_dir / "quarantine" / name).is_file()

    def incomplete_report(self, name: str) -> dict[str, object] | None:
        path = self._staging_dir / "quarantine" / f"{name}{INCOMPLETE_REPORT_FILENAME_SUFFIX}"
        if not path.is_file():
            return None
        parsed: dict[str, object] = json.loads(path.read_text())
        return parsed


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
    assert not harness.is_quarantined(MISMATCH_NAME)


def test_quarantine_moves_staged_to_quarantined_never_to_published(harness: Harness) -> None:
    store = harness.make()
    store.allocate(RELPATH, ALLOCATE_SIZE)

    store.quarantine(RELPATH, MISMATCH_SESSION)

    assert harness.is_quarantined(MISMATCH_NAME)  # session id before the extension
    assert not harness.is_staged(RELPATH)
    assert not harness.is_published(RELPATH)


def test_staged_file_exists_tracks_allocate_then_the_move_out(harness: Harness) -> None:
    store = harness.make()
    assert store.staged_file_exists(RELPATH) is False

    store.allocate(RELPATH, ALLOCATE_SIZE)
    assert store.staged_file_exists(RELPATH) is True

    store.quarantine(RELPATH, MISMATCH_SESSION)
    assert store.staged_file_exists(RELPATH) is False


def test_quarantine_incomplete_moves_the_partial_and_records_its_report(harness: Harness) -> None:
    store = harness.make()
    store.allocate(RELPATH, ALLOCATE_SIZE)

    store.quarantine_incomplete(RELPATH, REPORT)

    assert harness.is_quarantined(INCOMPLETE_NAME)  # session id before the extension
    assert not harness.is_staged(RELPATH)
    assert not harness.is_published(RELPATH)
    assert harness.incomplete_report(INCOMPLETE_NAME) == {
        "session_id": "s-1",
        "total_blocks": 5,
        "decoded_blocks": 3,
        "missing_block_ids": [1, 4],
    }


def test_two_incomplete_transfers_of_one_relpath_do_not_collide(harness: Harness) -> None:
    store = harness.make()
    report_a = IncompleteReport(SessionId("aaaa1111"), 5, 4, (BlockId(4),))
    report_b = IncompleteReport(SessionId("bbbb2222"), 5, 1, (BlockId(1), BlockId(2), BlockId(3)))

    store.allocate(RELPATH, ALLOCATE_SIZE)
    store.quarantine_incomplete(RELPATH, report_a)
    store.allocate(RELPATH, ALLOCATE_SIZE)
    store.quarantine_incomplete(RELPATH, report_b)

    name_a = quarantine_name(RELPATH, report_a.session_id)
    name_b = quarantine_name(RELPATH, report_b.session_id)
    assert name_a != name_b
    assert harness.is_quarantined(name_a)
    assert harness.is_quarantined(name_b)
    report_a_json = harness.incomplete_report(name_a)
    report_b_json = harness.incomplete_report(name_b)
    assert report_a_json is not None and report_a_json["missing_block_ids"] == [4]
    assert report_b_json is not None and report_b_json["missing_block_ids"] == [1, 2, 3]
