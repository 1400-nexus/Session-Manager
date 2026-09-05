from pathlib import Path
from typing import Protocol

import pytest

from session_manager.adapters.append_journal import AppendJournal
from session_manager.domain.ids import BlockId, SessionId
from session_manager.ports.protocols import Journal
from tests.fakes.fake_journal import FakeJournal

SESSION_A = SessionId("session-a")
SESSION_B = SessionId("session-b")


class Harness(Protocol):
    def make(self) -> Journal: ...


class FakeHarness:
    def make(self) -> Journal:
        return FakeJournal()


class AppendJournalHarness:
    def __init__(self, tmp_path: Path) -> None:
        self._journal_dir = tmp_path / "journal"

    def make(self) -> Journal:
        return AppendJournal(self._journal_dir)


@pytest.fixture(params=["fake", "append"])
def harness(request: pytest.FixtureRequest, tmp_path: Path) -> Harness:
    if request.param == "fake":
        return FakeHarness()
    return AppendJournalHarness(tmp_path)


async def test_replay_yields_appended_blocks_in_append_order(harness: Harness) -> None:
    journal = harness.make()
    journal.append(SESSION_A, BlockId(0), 0, 100)
    journal.append(SESSION_A, BlockId(2), 200, 100)
    journal.append(SESSION_A, BlockId(1), 100, 100)

    blocks = [block_id async for block_id in journal.replay(SESSION_A)]

    assert blocks == [BlockId(0), BlockId(2), BlockId(1)]


async def test_replay_of_a_never_appended_session_yields_nothing(harness: Harness) -> None:
    journal = harness.make()

    blocks = [block_id async for block_id in journal.replay(SessionId("ghost"))]

    assert blocks == []


async def test_sessions_do_not_cross_contaminate(harness: Harness) -> None:
    journal = harness.make()
    journal.append(SESSION_A, BlockId(0), 0, 10)
    journal.append(SESSION_B, BlockId(9), 0, 10)

    assert [block_id async for block_id in journal.replay(SESSION_A)] == [BlockId(0)]
    assert [block_id async for block_id in journal.replay(SESSION_B)] == [BlockId(9)]


async def test_a_block_appended_twice_is_replayed_twice(harness: Harness) -> None:
    # No dedup here on purpose: recovery folds replayed ids into a set
    # (SessionAuthority._recover), so a duplicate is harmless downstream and
    # the journal doesn't need to know a block was already recorded.
    journal = harness.make()
    journal.append(SESSION_A, BlockId(0), 0, 10)
    journal.append(SESSION_A, BlockId(0), 0, 10)

    blocks = [block_id async for block_id in journal.replay(SESSION_A)]

    assert blocks == [BlockId(0), BlockId(0)]


def test_sync_does_not_raise_with_nothing_pending(harness: Harness) -> None:
    journal = harness.make()

    journal.sync()
