import os
from pathlib import Path

import pytest

from session_manager.adapters.local_file_store import LocalFileStore


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
