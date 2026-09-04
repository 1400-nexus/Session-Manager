import asyncio
from collections.abc import Callable

from session_manager.supervision.supervisor import ChildSpec, ProcessSupervisor
from tests.fakes.fake_clock import FakeClock
from tests.fakes.fake_spawner import FakeProcess, FakeSpawner

POLL_TIMEOUT_SECONDS = 1


class NeverResolvingClock:
    def __init__(self) -> None:
        self._now: float = 0.0
        self.pending_sleeps: list[asyncio.Future[None]] = []

    def now(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.pending_sleeps.append(waiter)
        await waiter


async def _wait_until(predicate: Callable[[], bool]) -> None:
    while not predicate():
        await asyncio.sleep(0)


async def _wait_for(predicate: Callable[[], bool]) -> None:
    await asyncio.wait_for(_wait_until(predicate), timeout=POLL_TIMEOUT_SECONDS)


async def test_child_restarts_on_exact_backoff_schedule() -> None:
    spec = ChildSpec(name="worker", argv=["worker"])
    spawner = FakeSpawner()
    clock = FakeClock()
    supervisor = ProcessSupervisor([spec], spawner, clock, backoff_schedule=(1.0, 2.0, 4.0))

    run_task = asyncio.create_task(supervisor.run())
    try:
        await _wait_for(lambda: len(spawner.spawned) >= 1)
        spawner.exit(spawner.spawned[0], 1)
        await _wait_for(lambda: clock.now() == 1.0)

        await _wait_for(lambda: len(spawner.spawned) >= 2)
        spawner.exit(spawner.spawned[1], 1)
        await _wait_for(lambda: clock.now() == 1.0 + 2.0)

        await _wait_for(lambda: len(spawner.spawned) >= 3)
        spawner.exit(spawner.spawned[2], 1)
        await _wait_for(lambda: clock.now() == 1.0 + 2.0 + 4.0)

        await _wait_for(lambda: len(spawner.spawned) >= 4)
    finally:
        await supervisor.shutdown()
        run_task.cancel()
        await asyncio.gather(run_task, return_exceptions=True)


async def test_a_spawn_failure_is_treated_like_a_crash_and_retried() -> None:
    spec = ChildSpec(name="worker", argv=["missing-binary"])
    spawner = FakeSpawner()
    clock = FakeClock()
    supervisor = ProcessSupervisor([spec], spawner, clock, backoff_schedule=(1.0,))

    spawner.fail_next_spawn(FileNotFoundError("missing-binary"))

    run_task = asyncio.create_task(supervisor.run())
    try:
        await _wait_for(lambda: clock.now() == 1.0)
        await _wait_for(lambda: len(spawner.spawned) >= 1)
        assert not run_task.done()
    finally:
        await supervisor.shutdown()
        run_task.cancel()
        await asyncio.gather(run_task, return_exceptions=True)


async def test_five_rapid_deaths_produce_degraded_and_stop_restarts() -> None:
    spec = ChildSpec(name="crashy", argv=["crashy"])
    spawner = FakeSpawner()
    clock = FakeClock()
    supervisor = ProcessSupervisor(
        [spec],
        spawner,
        clock,
        backoff_schedule=(0.01,),
        crash_loop_max_restarts=5,
        crash_loop_window_seconds=60.0,
    )

    run_task = asyncio.create_task(supervisor.run())
    try:
        for death_index in range(5):
            await _wait_for(lambda: len(spawner.spawned) > death_index)
            spawner.exit(spawner.spawned[death_index], 1)

        await _wait_for(lambda: supervisor.is_degraded("crashy"))

        spawned_count_at_degraded = len(spawner.spawned)
        await asyncio.sleep(0.05)
        assert len(spawner.spawned) == spawned_count_at_degraded
    finally:
        await supervisor.shutdown()
        run_task.cancel()
        await asyncio.gather(run_task, return_exceptions=True)


async def test_other_children_keep_running_while_one_is_degraded() -> None:
    crashy = ChildSpec(name="crashy", argv=["crashy"])
    stable = ChildSpec(name="stable", argv=["stable"])
    spawner = FakeSpawner()
    clock = FakeClock()
    supervisor = ProcessSupervisor(
        [crashy, stable],
        spawner,
        clock,
        backoff_schedule=(0.01,),
        crash_loop_max_restarts=5,
        crash_loop_window_seconds=60.0,
    )

    def crashy_processes() -> list[FakeProcess]:
        return [process for process in spawner.spawned if process.argv == crashy.argv]

    def stable_processes() -> list[FakeProcess]:
        return [process for process in spawner.spawned if process.argv == stable.argv]

    run_task = asyncio.create_task(supervisor.run())
    try:
        await _wait_for(lambda: len(stable_processes()) >= 1)

        for death_index in range(5):
            await _wait_for(lambda: len(crashy_processes()) > death_index)
            spawner.exit(crashy_processes()[death_index], 1)

        await _wait_for(lambda: supervisor.is_degraded("crashy"))

        assert not supervisor.is_degraded("stable")
        assert len(stable_processes()) >= 1
        assert not stable_processes()[-1].exit_code.done()
    finally:
        await supervisor.shutdown()
        run_task.cancel()
        await asyncio.gather(run_task, return_exceptions=True)


async def test_shutdown_leaves_nothing_alive() -> None:
    spec_a = ChildSpec(name="a", argv=["a"])
    spec_b = ChildSpec(name="b", argv=["b"])
    spawner = FakeSpawner()
    clock = FakeClock()
    supervisor = ProcessSupervisor([spec_a, spec_b], spawner, clock)

    run_task = asyncio.create_task(supervisor.run())
    try:
        await _wait_for(lambda: len(spawner.spawned) >= 2)

        await asyncio.wait_for(supervisor.shutdown(), timeout=POLL_TIMEOUT_SECONDS)

        assert len(spawner.spawned) == 2
        assert all(process.terminated for process in spawner.spawned)
        assert all(process.exit_code.done() for process in spawner.spawned)

        await asyncio.wait_for(run_task, timeout=POLL_TIMEOUT_SECONDS)
        assert run_task.done()
    finally:
        if not run_task.done():
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)


async def test_shutdown_is_idempotent() -> None:
    spec = ChildSpec(name="worker", argv=["worker"])
    spawner = FakeSpawner()
    clock = FakeClock()
    supervisor = ProcessSupervisor([spec], spawner, clock)

    run_task = asyncio.create_task(supervisor.run())
    try:
        await _wait_for(lambda: len(spawner.spawned) >= 1)

        await asyncio.wait_for(supervisor.shutdown(), timeout=POLL_TIMEOUT_SECONDS)
        await asyncio.wait_for(supervisor.shutdown(), timeout=POLL_TIMEOUT_SECONDS)

        assert len(spawner.spawned) == 1
    finally:
        if not run_task.done():
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)


async def test_shutdown_during_backoff_window_exits_without_waiting_out_the_delay() -> None:
    spec = ChildSpec(name="worker", argv=["worker"])
    spawner = FakeSpawner()
    clock = NeverResolvingClock()
    supervisor = ProcessSupervisor([spec], spawner, clock, backoff_schedule=(30.0,))

    run_task = asyncio.create_task(supervisor.run())
    try:
        await _wait_for(lambda: len(spawner.spawned) >= 1)
        spawner.exit(spawner.spawned[0], 1)

        await _wait_for(lambda: len(clock.pending_sleeps) >= 1)

        await asyncio.wait_for(supervisor.shutdown(), timeout=POLL_TIMEOUT_SECONDS)
        await asyncio.wait_for(run_task, timeout=POLL_TIMEOUT_SECONDS)

        assert clock.now() == 0.0
    finally:
        if not run_task.done():
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)


async def test_shutdown_tolerates_a_process_the_os_already_reaped() -> None:
    spawner = FakeSpawner()
    clock = FakeClock()
    supervisor: ProcessSupervisor[FakeProcess] = ProcessSupervisor([], spawner, clock)

    ghost = FakeProcess(["ghost"], None)
    ghost.reaped = True
    supervisor._processes["ghost"] = ghost

    await asyncio.wait_for(supervisor.shutdown(), timeout=POLL_TIMEOUT_SECONDS)


async def test_run_with_no_specs_is_a_clean_noop() -> None:
    spawner = FakeSpawner()
    clock = FakeClock()
    supervisor: ProcessSupervisor[FakeProcess] = ProcessSupervisor([], spawner, clock)

    await asyncio.wait_for(supervisor.run(), timeout=POLL_TIMEOUT_SECONDS)

    assert spawner.spawned == []
