from session_manager.domain.ids import ReceiverId


class HandshakeError(Exception):
    def __init__(self, field_name: str) -> None:
        super().__init__(f"expected receiver_hello, got {field_name}")
        self.field_name: str = field_name


class UnknownReceiverError(KeyError):
    def __init__(self, receiver_id: ReceiverId) -> None:
        super().__init__(f"no connected peer for receiver_id {receiver_id}")
        self.receiver_id: ReceiverId = receiver_id


class ProtoHashMismatchError(Exception):
    def __init__(self, receiver_id: ReceiverId, reported_hash: bytes, expected_hash: bytes) -> None:
        super().__init__(
            f"receiver_id {receiver_id} reported proto_hash {reported_hash.hex()}, "
            f"expected {expected_hash.hex()}"
        )
        self.receiver_id: ReceiverId = receiver_id
        self.reported_hash: bytes = reported_hash
        self.expected_hash: bytes = expected_hash


class SendQueueFullError(Exception):
    def __init__(self, receiver_id: ReceiverId) -> None:
        super().__init__(f"send queue full for receiver_id {receiver_id}")
        self.receiver_id: ReceiverId = receiver_id
