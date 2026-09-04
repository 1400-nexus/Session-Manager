import rx_pb2
from google.protobuf.message import Message

from session_manager.ipc.constants import ENVELOPE_ONEOF_GROUP_NAME
from session_manager.ipc.message_types import FIELD_NAME_BY_MESSAGE_TYPE


def encode(payload: Message) -> bytes:
    field_name = FIELD_NAME_BY_MESSAGE_TYPE[type(payload)]
    envelope = rx_pb2.RxEnvelope(**{field_name: payload})
    serialized: bytes = envelope.SerializeToString()
    return serialized


def decode(raw: bytes) -> tuple[str, Message]:
    envelope = rx_pb2.RxEnvelope()
    envelope.ParseFromString(raw)
    field_name = envelope.WhichOneof(ENVELOPE_ONEOF_GROUP_NAME)
    if field_name is None:
        raise ValueError("RxEnvelope has no message set")
    return field_name, getattr(envelope, field_name)
