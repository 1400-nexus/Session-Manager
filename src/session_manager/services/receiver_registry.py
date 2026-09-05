from session_manager.domain.ids import ReceiverId
from session_manager.ports.protocols import Clock
from session_manager.services.constants import MISSED_HEARTBEAT_LIMIT


class ReceiverRegistry:
    """Tracks which receivers are connected and alive.

    `ReceiverHello` registers, `Heartbeat` refreshes; three missed heartbeats
    (not one) drops a receiver, so a single delayed heartbeat under load
    doesn't flap it dead. There is no explicit disconnect hook from the IPC
    layer -- a dead connection simply stops refreshing and ages out here,
    the same design file-monitor's `SenderRegistry` uses.

    `active_receivers` doubles as `ProgressAggregator`'s `live_receivers`
    callable, and `any_alive` as `PosixShm`'s `probe_receiver_alive` --
    "does any receiver currently answer on the UDS socket" is exactly what
    this registry already tracks.
    """

    def __init__(self, clock: Clock, heartbeat_interval_s: float) -> None:
        self._clock: Clock = clock
        self._timeout_s: float = heartbeat_interval_s * MISSED_HEARTBEAT_LIMIT
        self._last_seen: dict[ReceiverId, float] = {}

    def register(self, receiver_id: ReceiverId) -> None:
        self._last_seen[receiver_id] = self._clock.now()

    def refresh(self, receiver_id: ReceiverId) -> None:
        self.register(receiver_id)

    def remove(self, receiver_id: ReceiverId) -> None:
        self._last_seen.pop(receiver_id, None)

    def active_receivers(self) -> frozenset[ReceiverId]:
        self._purge_expired()
        return frozenset(self._last_seen)

    def any_alive(self) -> bool:
        return bool(self.active_receivers())

    def _purge_expired(self) -> None:
        now = self._clock.now()
        expired = [
            receiver_id
            for receiver_id, last_seen in self._last_seen.items()
            if now - last_seen >= self._timeout_s
        ]
        for receiver_id in expired:
            del self._last_seen[receiver_id]
