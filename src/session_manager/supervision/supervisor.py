import asyncio
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Generic

import structlog

from session_manager.ports.protocols import Clock, ProcessHandle, ProcessSpawner
from session_manager.supervision.constants import (
    CRASH_LOOP_MAX_RESTARTS,
    CRASH_LOOP_WINDOW_SECONDS,
    DEFAULT_BACKOFF_SCHEDULE_SECONDS,
    SHUTDOWN_TIMEOUT_SECONDS,
)

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class ChildSpec:
    name: str
    argv: Sequence[str]
    env: dict[str, str] | None = None


class ProcessSupervisor(Generic[ProcessHandle]):
    def __init__(
        self,
        specs: list[ChildSpec],
        spawner: ProcessSpawner[ProcessHandle],
        clock: Clock,
        backoff_schedule: Sequence[float] = DEFAULT_BACKOFF_SCHEDULE_SECONDS,
        crash_loop_max_restarts: int = CRASH_LOOP_MAX_RESTARTS,
        crash_loop_window_seconds: float = CRASH_LOOP_WINDOW_SECONDS,
        shutdown_timeout_seconds: float = SHUTDOWN_TIMEOUT_SECONDS,
    ) -> None:
        self._specs: list[ChildSpec] = specs
        self._spawner: ProcessSpawner[ProcessHandle] = spawner
        self._clock: Clock = clock
        self._backoff_schedule: Sequence[float] = backoff_schedule
        self._crash_loop_max_restarts: int = crash_loop_max_restarts
        self._crash_loop_window_seconds: float = crash_loop_window_seconds
        self._shutdown_timeout_seconds: float = shutdown_timeout_seconds
        self._processes: dict[str, ProcessHandle] = {}
        self._degraded: set[str] = set()
        self._stopping: bool = False
        self._stop_event: asyncio.Event = asyncio.Event()

    async def run(self) -> None:
        if not self._specs:
            logger.info("supervisor_has_no_children_to_manage")
            return
        async with asyncio.TaskGroup() as task_group:
            for spec in self._specs:
                task_group.create_task(self._supervise_child(spec))

    def is_degraded(self, name: str) -> bool:
        return name in self._degraded

    async def _supervise_child(self, spec: ChildSpec) -> None:
        consecutive_failures = 0
        exit_timestamps: deque[float] = deque(maxlen=self._crash_loop_max_restarts)

        while not self._stopping:
            started_at = self._clock.now()
            try:
                process = await self._spawner.spawn(spec.argv, spec.env)
            except OSError as error:
                logger.error("child_spawn_failed", name=spec.name, error=str(error))
                exit_code = None
            else:
                self._processes[spec.name] = process
                exit_code = await self._spawner.wait(process)
                self._processes.pop(spec.name, None)

            if self._stopping:
                return

            uptime = self._clock.now() - started_at
            if uptime > self._backoff_schedule[-1]:
                consecutive_failures = 0

            logger.warning("child_exited", name=spec.name, exit_code=exit_code, uptime=uptime)

            exit_timestamps.append(self._clock.now())
            if (
                len(exit_timestamps) == self._crash_loop_max_restarts
                and self._clock.now() - exit_timestamps[0] < self._crash_loop_window_seconds
            ):
                self._degraded.add(spec.name)
                logger.error("child_degraded", name=spec.name)
                return

            delay_index = min(consecutive_failures, len(self._backoff_schedule) - 1)
            delay = self._backoff_schedule[delay_index]
            consecutive_failures += 1
            await self._wait_for_backoff_or_stop(delay)

    async def _wait_for_backoff_or_stop(self, delay: float) -> None:
        sleep_task: asyncio.Task[Any] = asyncio.ensure_future(self._clock.sleep(delay))
        stop_task: asyncio.Task[Any] = asyncio.ensure_future(self._stop_event.wait())
        _done, pending = await asyncio.wait(
            {sleep_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def _wait_tolerating_missing_process(self, process: ProcessHandle) -> None:
        try:
            await self._spawner.wait(process)
        except ProcessLookupError:
            pass

    async def shutdown(self) -> None:
        if self._stopping:
            return
        self._stopping = True
        self._stop_event.set()

        processes = list(self._processes.values())
        for process in processes:
            try:
                self._spawner.terminate(process)
            except ProcessLookupError:
                pass

        if processes:
            # Shielded: if the timeout wins the race below and wait_all gets
            # cancelled, that must not cancel the underlying per-process wait
            # (and, through it, the shared process-exit future) — the
            # escalation below still needs to await that same future.
            shielded_waits = [
                asyncio.shield(self._wait_tolerating_missing_process(process))
                for process in processes
            ]
            wait_all: asyncio.Task[Any] = asyncio.ensure_future(asyncio.gather(*shielded_waits))
            timeout_task: asyncio.Task[Any] = asyncio.ensure_future(
                self._clock.sleep(self._shutdown_timeout_seconds)
            )
            _done, pending = await asyncio.wait(
                {wait_all, timeout_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.sleep(0)

        remaining = list(self._processes.values())
        for process in remaining:
            try:
                self._spawner.kill(process)
            except ProcessLookupError:
                pass
        for process in remaining:
            await self._wait_tolerating_missing_process(process)
