import json
import os
from pathlib import Path

import pytest

from session_manager.adapters.constants import INCOMPLETE_REPORT_FILENAME_SUFFIX
from session_manager.adapters.local_file_store import LocalFileStore
from session_manager.domain.ids import BlockId, SessionId
from session_manager.domain.models import IncompleteReport


def _store(tmp_path: Path) -> LocalFileStore:
    return LocalFileStore(tmp_path / "staging", tmp_path / "output")


def test_allocate_creates_intermediate_directories_and_reserves_the_size(tmp_path: Path) -> None:
    store = _store(tmp_path)

    path = store.allocate("a/b/c/output.bin", 1000)

    assert path.is_file()
    assert path.stat().st_size == 1000


def test_allocate_falls_back_to_truncate_when_fallocate_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delattr(os, "posix_fallocate", raising=False)
    store = _store(tmp_path)

    path = store.allocate("output.bin", 500)

    assert path.stat().st_size == 500


@pytest.mark.skipif(
    not hasattr(os, "posix_fallocate"), reason="posix_fallocate requires a POSIX host"
)
def test_allocate_falls_back_to_truncate_when_fallocate_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _unsupported(file_descriptor: int, offset: int, length: int) -> None:
        raise OSError("fallocate not supported on this filesystem")

    monkeypatch.setattr(os, "posix_fallocate", _unsupported)
    store = _store(tmp_path)

    path = store.allocate("output.bin", 500)

    assert path.stat().st_size == 500


@pytest.mark.skipif(
    not hasattr(os, "posix_fallocate"), reason="posix_fallocate requires a POSIX host"
)
def test_allocate_uses_posix_fallocate_when_available(tmp_path: Path) -> None:
    store = _store(tmp_path)

    path = store.allocate("output.bin", 5000)

    assert path.stat().st_size == 5000


def test_staged_path_is_pure_and_touches_no_filesystem(tmp_path: Path) -> None:
    store = _store(tmp_path)

    path = store.staged_path("sub/output.bin")

    assert path == tmp_path / "staging" / "sub" / "output.bin"
    assert not path.exists()
    assert not path.parent.exists()


def test_publish_preserves_content_and_removes_the_staged_copy(tmp_path: Path) -> None:
    store = _store(tmp_path)
    staged = store.allocate("sub/output.bin", 5)
    staged.write_bytes(b"hello")

    output = store.publish("sub/output.bin")

    assert output.read_bytes() == b"hello"
    assert not staged.exists()


def test_publish_refuses_to_overwrite_an_existing_output_file(tmp_path: Path) -> None:
    store = _store(tmp_path)
    output_path = tmp_path / "output" / "output.bin"
    output_path.parent.mkdir(parents=True)
    output_path.write_bytes(b"already published")
    staged = store.allocate("output.bin", 11)
    staged.write_bytes(b"new attempt")

    with pytest.raises(FileExistsError):
        store.publish("output.bin")

    assert staged.exists()
    assert output_path.read_bytes() == b"already published"


def test_quarantine_preserves_content_and_never_reaches_output(tmp_path: Path) -> None:
    store = _store(tmp_path)
    staged = store.allocate("output.bin", 5)
    staged.write_bytes(b"junk!")

    quarantined = store.quarantine("output.bin")

    assert quarantined.read_bytes() == b"junk!"
    assert not staged.exists()
    assert not (tmp_path / "output" / "output.bin").exists()


def test_quarantine_incomplete_keeps_the_partial_byte_identical_and_writes_the_report(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    staged = store.allocate("sub/part.bin", 9)
    partial_bytes = b"abc\x00\x00\x00ghi"  # a non-contiguous gap in the middle
    staged.write_bytes(partial_bytes)
    report = IncompleteReport(
        session_id=SessionId("abc123"),
        total_blocks=9,
        decoded_blocks=6,
        missing_block_ids=(BlockId(3), BlockId(4), BlockId(5)),
    )

    quarantined = store.quarantine_incomplete("sub/part.bin", report)

    assert quarantined == tmp_path / "staging" / "quarantine" / "sub" / "part.abc123.bin"
    assert quarantined.read_bytes() == partial_bytes
    assert not staged.exists()
    report_path = quarantined.with_name(f"{quarantined.name}{INCOMPLETE_REPORT_FILENAME_SUFFIX}")
    assert json.loads(report_path.read_text()) == {
        "session_id": "abc123",
        "total_blocks": 9,
        "decoded_blocks": 6,
        "missing_block_ids": [3, 4, 5],
    }


def test_quarantine_incomplete_leaves_no_temp_file_behind(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.allocate("part.bin", 4)
    report = IncompleteReport(
        session_id=SessionId("s"),
        total_blocks=4,
        decoded_blocks=0,
        missing_block_ids=(BlockId(0), BlockId(1), BlockId(2), BlockId(3)),
    )

    store.quarantine_incomplete("part.bin", report)

    quarantine_dir = tmp_path / "staging" / "quarantine"
    assert [p.name for p in sorted(quarantine_dir.iterdir())] == [
        "part.s.bin",
        f"part.s.bin{INCOMPLETE_REPORT_FILENAME_SUFFIX}",
    ]


def test_two_incomplete_transfers_of_one_relpath_keep_both_partials_and_reports(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)

    store.allocate("report.bin", 3).write_bytes(b"aaa")
    store.quarantine_incomplete(
        "report.bin",
        IncompleteReport(
            session_id=SessionId("aaaa1111"),
            total_blocks=3,
            decoded_blocks=1,
            missing_block_ids=(BlockId(1), BlockId(2)),
        ),
    )
    store.allocate("report.bin", 3).write_bytes(b"bbb")
    store.quarantine_incomplete(
        "report.bin",
        IncompleteReport(
            session_id=SessionId("bbbb2222"),
            total_blocks=3,
            decoded_blocks=2,
            missing_block_ids=(BlockId(2),),
        ),
    )

    quarantine_dir = tmp_path / "staging" / "quarantine"
    assert (quarantine_dir / "report.aaaa1111.bin").read_bytes() == b"aaa"
    assert (quarantine_dir / "report.bbbb2222.bin").read_bytes() == b"bbb"
    first = json.loads((quarantine_dir / "report.aaaa1111.bin.incomplete.json").read_text())
    second = json.loads((quarantine_dir / "report.bbbb2222.bin.incomplete.json").read_text())
    assert first["missing_block_ids"] == [1, 2]
    assert second["missing_block_ids"] == [2]
