import os
import struct
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from session_manager.adapters.append_journal import AppendJournal
from session_manager.adapters.constants import JOURNAL_CRC_FORMAT, JOURNAL_FIELDS_FORMAT
from session_manager.domain.ids import BlockId, SessionId

RECORD_SIZE = struct.calcsize(JOURNAL_FIELDS_FORMAT) + struct.calcsize(JOURNAL_CRC_FORMAT)
SESSION = SessionId("session-a")


def _journal_path(journal_dir: Path, session_id: SessionId) -> Path:
    return journal_dir / f"{session_id}.journal"


async def test_the_journal_file_is_named_by_session_id(tmp_path: Path) -> None:
    journal_dir = tmp_path / "journal"
    journal = AppendJournal(journal_dir)

    journal.append(SESSION, BlockId(0), 0, 100)

    assert _journal_path(journal_dir, SESSION).is_file()


async def test_a_truncated_tail_replays_every_complete_record_and_stops_silently(
    tmp_path: Path,
) -> None:
    journal_dir = tmp_path / "journal"
    journal = AppendJournal(journal_dir)
    for block_id in (0, 1, 2):
        journal.append(SESSION, BlockId(block_id), block_id * 100, 100)
    journal.sync()

    with open(_journal_path(journal_dir, SESSION), "ab") as file_handle:
        file_handle.write(b"\x99" * (RECORD_SIZE - 3))  # a crash mid-write: shorter than one record

    fresh = AppendJournal(journal_dir)
    blocks = [block_id async for block_id in fresh.replay(SESSION)]

    assert blocks == [BlockId(0), BlockId(1), BlockId(2)]


async def test_a_corrupted_record_in_the_middle_stops_replay_there_and_logs(
    tmp_path: Path,
) -> None:
    journal_dir = tmp_path / "journal"
    journal = AppendJournal(journal_dir)
    for block_id in (0, 1, 2):
        journal.append(SESSION, BlockId(block_id), block_id * 100, 100)
    journal.sync()

    path = _journal_path(journal_dir, SESSION)
    raw = bytearray(path.read_bytes())
    corrupt_offset = RECORD_SIZE + 4  # inside record 1's fields, past the session_id prefix
    raw[corrupt_offset] ^= 0xFF
    path.write_bytes(bytes(raw))

    fresh = AppendJournal(journal_dir)
    with capture_logs() as logs:
        blocks = [block_id async for block_id in fresh.replay(SESSION)]

    assert blocks == [BlockId(0)]
    corruption_logs = [entry for entry in logs if entry["event"] == "journal_record_corrupt"]
    assert len(corruption_logs) == 1


async def test_sync_makes_records_durable_for_a_freshly_opened_journal(tmp_path: Path) -> None:
    journal_dir = tmp_path / "journal"
    journal = AppendJournal(journal_dir)
    journal.append(SESSION, BlockId(7), 0, 100)
    journal.sync()

    reopened = AppendJournal(journal_dir)
    blocks = [block_id async for block_id in reopened.replay(SESSION)]

    assert blocks == [BlockId(7)]


async def test_replay_sees_unsynced_appends_from_the_same_instance(tmp_path: Path) -> None:
    journal = AppendJournal(tmp_path / "journal")

    journal.append(SESSION, BlockId(3), 0, 100)
    blocks = [block_id async for block_id in journal.replay(SESSION)]

    assert blocks == [BlockId(3)]


@pytest.mark.parametrize("bad_session_id", ["../escape", "a/b", "a\\b", ""])
def test_an_unsafe_session_id_is_rejected_before_touching_the_filesystem(
    tmp_path: Path, bad_session_id: str
) -> None:
    journal = AppendJournal(tmp_path / "journal")

    with pytest.raises(ValueError, match="unsafe session_id"):
        journal.append(SessionId(bad_session_id), BlockId(0), 0, 10)


async def test_sync_falls_back_to_fsync_when_fdatasync_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delattr(os, "fdatasync", raising=False)
    journal = AppendJournal(tmp_path / "journal")
    journal.append(SESSION, BlockId(1), 0, 100)

    journal.sync()

    reopened = AppendJournal(tmp_path / "journal")
    assert [block_id async for block_id in reopened.replay(SESSION)] == [BlockId(1)]
