import asyncio
import uuid
from collections.abc import Callable
from multiprocessing import shared_memory
from pathlib import Path

import pytest

pytest.importorskip("fcntl", reason="FlockFileLock requires a POSIX host")

from session_manager import main as main_module  # noqa: E402
from session_manager.adapters.errors import LockHeldError  # noqa: E402
from session_manager.adapters.flock_file_lock import FlockFileLock  # noqa: E402
from session_manager.config import (  # noqa: E402
    AggregationConfig,
    AppConfig,
    PathsConfig,
    ReceiversConfig,
    ShmConfig,
    StatusConfig,
    SupervisionConfig,
)
from session_manager.constants import PROTO_CONTRACT_DIR_ENV_VAR  # noqa: E402

REPO_PROTO_DIR = Path(__file__).resolve().parents[2] / "libs" / "nexus-proto" / "proto"


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        paths=PathsConfig(
            staging_dir=tmp_path / "staging",
            output_dir=tmp_path / "output",
            journal_dir=tmp_path / "journal",
            run_dir=tmp_path / "run",
            socket_path=tmp_path / "run" / "session-manager.sock",
            lock_path=tmp_path / "run" / "session-manager.lock",
        ),
        shm=ShmConfig(
            name=f"nx-test-{uuid.uuid4().hex[:16]}", arena_bytes=1 << 20, slot_bytes=4096
        ),
        aggregation=AggregationConfig(
            poll_interval_s=0.05, stall_timeout_s=1.0, shm_crosscheck=True
        ),
        receivers=ReceiversConfig(count=0, ports=(), binary_path=""),
        supervision=SupervisionConfig(),
        status=StatusConfig(refresh_interval_s=0.05, force_terminal=False),
    )


async def _wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(0.02)

    await asyncio.wait_for(_poll(), timeout=timeout)


async def test_run_acquires_lock_creates_segment_binds_socket_and_shuts_down_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(PROTO_CONTRACT_DIR_ENV_VAR, str(REPO_PROTO_DIR))
    config = _config(tmp_path)
    shutdown_event = asyncio.Event()

    run_task = asyncio.create_task(main_module.run(config, shutdown_event=shutdown_event))
    try:
        await _wait_until(lambda: config.paths.socket_path.exists())

        # the segment exists and the lock is held while running
        shared_memory.SharedMemory(name=config.shm.name, create=False).close()
        with pytest.raises(LockHeldError):
            FlockFileLock(config.paths.lock_path).acquire()

        shutdown_event.set()
        exit_code = await asyncio.wait_for(run_task, timeout=5)
    finally:
        if not run_task.done():
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)

    assert exit_code == 0
    assert not config.paths.socket_path.exists()

    with pytest.raises(FileNotFoundError):
        shared_memory.SharedMemory(name=config.shm.name, create=False)

    # the lock was released -- a fresh acquire now succeeds
    released_lock = FlockFileLock(config.paths.lock_path)
    released_lock.acquire()
    released_lock.release()
