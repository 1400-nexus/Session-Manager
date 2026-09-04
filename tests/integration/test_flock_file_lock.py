import os
from pathlib import Path

import pytest

pytest.importorskip("fcntl", reason="fcntl.flock requires a POSIX host")

from session_manager.adapters.errors import LockHeldError  # noqa: E402
from session_manager.adapters.flock_file_lock import FlockFileLock  # noqa: E402


def test_acquire_then_release_round_trips(tmp_path: Path) -> None:
    lock = FlockFileLock(tmp_path / "sub" / "manager.lock")

    lock.acquire()
    assert lock.holder_pid() == os.getpid()
    lock.release()

    reacquired = FlockFileLock(tmp_path / "sub" / "manager.lock")
    reacquired.acquire()
    reacquired.release()


def test_a_second_holder_is_refused_and_the_first_pid_is_reported(tmp_path: Path) -> None:
    path = tmp_path / "manager.lock"
    first = FlockFileLock(path)
    first.acquire()
    try:
        second = FlockFileLock(path)
        with pytest.raises(LockHeldError) as excinfo:
            second.acquire()
        assert excinfo.value.holder_pid == os.getpid()
    finally:
        first.release()


def test_release_is_idempotent(tmp_path: Path) -> None:
    lock = FlockFileLock(tmp_path / "manager.lock")
    lock.acquire()
    lock.release()
    lock.release()
