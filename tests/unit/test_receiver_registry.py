from session_manager.domain.ids import ReceiverId
from session_manager.services.constants import MISSED_HEARTBEAT_LIMIT
from session_manager.services.receiver_registry import ReceiverRegistry
from tests.fakes.fake_clock import FakeClock

HEARTBEAT_INTERVAL_S = 5.0


def _registry(clock: FakeClock) -> ReceiverRegistry:
    return ReceiverRegistry(clock, heartbeat_interval_s=HEARTBEAT_INTERVAL_S)


def test_register_adds_receiver_as_active() -> None:
    registry = _registry(FakeClock())
    registry.register(ReceiverId(1))
    assert registry.active_receivers() == frozenset({ReceiverId(1)})


def test_any_alive_reflects_active_receivers() -> None:
    registry = _registry(FakeClock())
    assert registry.any_alive() is False
    registry.register(ReceiverId(1))
    assert registry.any_alive() is True


def test_refresh_keeps_receiver_alive_across_heartbeats() -> None:
    clock = FakeClock()
    registry = _registry(clock)
    registry.register(ReceiverId(1))

    for _ in range(5):
        clock.advance(HEARTBEAT_INTERVAL_S - 0.001)
        registry.refresh(ReceiverId(1))

    assert registry.active_receivers() == frozenset({ReceiverId(1)})


def test_receiver_drops_out_after_exactly_the_configured_timeout() -> None:
    clock = FakeClock()
    registry = _registry(clock)
    registry.register(ReceiverId(1))

    timeout_s = HEARTBEAT_INTERVAL_S * MISSED_HEARTBEAT_LIMIT
    clock.advance(timeout_s - 0.001)
    assert registry.active_receivers() == frozenset({ReceiverId(1)})

    clock.advance(0.002)
    assert registry.active_receivers() == frozenset()
    assert registry.any_alive() is False


def test_remove_drops_a_receiver_immediately() -> None:
    registry = _registry(FakeClock())
    registry.register(ReceiverId(1))
    registry.remove(ReceiverId(1))
    assert registry.active_receivers() == frozenset()


def test_removing_an_unknown_receiver_is_a_noop() -> None:
    registry = _registry(FakeClock())
    registry.remove(ReceiverId(999))
    assert registry.active_receivers() == frozenset()


def test_a_dead_receiver_no_longer_affects_active_receivers_after_purge() -> None:
    clock = FakeClock()
    registry = _registry(clock)
    registry.register(ReceiverId(1))
    registry.register(ReceiverId(2))

    clock.advance(HEARTBEAT_INTERVAL_S * MISSED_HEARTBEAT_LIMIT)
    registry.refresh(ReceiverId(2))

    assert registry.active_receivers() == frozenset({ReceiverId(2)})
