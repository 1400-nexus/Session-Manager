import os
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from session_manager.adapters.json_session_spec_store import JsonSessionSpecStore
from session_manager.domain.ids import SessionId
from session_manager.domain.models import SessionSpec

SESSION = SessionId("s-1")


def _spec() -> SessionSpec:
    return SessionSpec(
        session_id=SESSION,
        relpath="sub/dir/output.bin",
        file_size=1_000_000,
        file_hash=b"\xab" * 32,
        k=200,
        n=255,
        symbol_bytes=1400,
        total_blocks=3,
    )


def test_save_writes_next_to_the_journal_directory(tmp_path: Path) -> None:
    journal_dir = tmp_path / "journal"
    store = JsonSessionSpecStore(journal_dir)

    store.save(_spec())

    assert (journal_dir / "s-1.spec.json").is_file()
    assert store.load(SESSION) == _spec()
    assert [p.name for p in journal_dir.iterdir()] == ["s-1.spec.json"]  # no .tmp residue


def test_a_save_that_fails_at_the_rename_leaves_the_previous_sidecar_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal_dir = tmp_path / "journal"
    store = JsonSessionSpecStore(journal_dir)
    store.save(_spec())
    original = _spec()

    newer = SessionSpec(
        session_id=SESSION,
        relpath="sub/dir/output.bin",
        file_size=2_000_000,
        file_hash=b"\xcd" * 32,
        k=200,
        n=255,
        symbol_bytes=1400,
        total_blocks=6,
    )
    monkeypatch.setattr(os, "replace", _raise_oserror)
    with pytest.raises(OSError):
        store.save(newer)

    assert store.load(SESSION) == original  # untouched, not truncated
    assert [p.name for p in journal_dir.iterdir()] == ["s-1.spec.json"]  # temp cleaned up


def _raise_oserror(*_args: object, **_kwargs: object) -> None:
    raise OSError("rename interrupted")


def test_a_corrupt_sidecar_loads_as_none_and_logs(tmp_path: Path) -> None:
    journal_dir = tmp_path / "journal"
    journal_dir.mkdir()
    (journal_dir / "s-1.spec.json").write_text("{not valid json")
    store = JsonSessionSpecStore(journal_dir)

    with capture_logs() as logs:
        loaded = store.load(SESSION)

    assert loaded is None
    corrupt = [entry for entry in logs if entry["event"] == "session_spec_corrupt"]
    assert len(corrupt) == 1


def test_a_sidecar_missing_a_field_loads_as_none_and_logs(tmp_path: Path) -> None:
    journal_dir = tmp_path / "journal"
    journal_dir.mkdir()
    (journal_dir / "s-1.spec.json").write_text('{"session_id": "s-1"}')
    store = JsonSessionSpecStore(journal_dir)

    with capture_logs() as logs:
        loaded = store.load(SESSION)

    assert loaded is None
    assert any(entry["event"] == "session_spec_corrupt" for entry in logs)


@pytest.mark.parametrize("bad_session_id", ["../escape", "a/b", "a\\b", ""])
def test_an_unsafe_session_id_is_rejected_before_touching_the_filesystem(
    tmp_path: Path, bad_session_id: str
) -> None:
    store = JsonSessionSpecStore(tmp_path / "journal")

    with pytest.raises(ValueError, match="unsafe session_id"):
        store.save(
            SessionSpec(
                session_id=SessionId(bad_session_id),
                relpath="f.bin",
                file_size=10,
                file_hash=b"\x00" * 32,
                k=2,
                n=3,
                symbol_bytes=10,
                total_blocks=1,
            )
        )
