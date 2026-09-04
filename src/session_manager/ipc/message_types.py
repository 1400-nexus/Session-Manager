import ipc_pb2
import rx_pb2
from google.protobuf.message import Message

from session_manager.ipc.constants import (
    BLOCK_DECODED_FIELD_NAME,
    CONFIG_FIELD_NAME,
    HEARTBEAT_FIELD_NAME,
    MANIFEST_SEEN_FIELD_NAME,
    PURGE_SESSION_FIELD_NAME,
    RECEIVER_HELLO_FIELD_NAME,
    RECEIVER_STATS_FIELD_NAME,
    SESSION_OPEN_FIELD_NAME,
)

FIELD_NAME_BY_MESSAGE_TYPE: dict[type[Message], str] = {
    rx_pb2.ReceiverHello: RECEIVER_HELLO_FIELD_NAME,
    rx_pb2.ManifestSeen: MANIFEST_SEEN_FIELD_NAME,
    rx_pb2.BlockDecoded: BLOCK_DECODED_FIELD_NAME,
    rx_pb2.ReceiverStats: RECEIVER_STATS_FIELD_NAME,
    ipc_pb2.Heartbeat: HEARTBEAT_FIELD_NAME,
    rx_pb2.Config: CONFIG_FIELD_NAME,
    rx_pb2.SessionOpen: SESSION_OPEN_FIELD_NAME,
    rx_pb2.PurgeSession: PURGE_SESSION_FIELD_NAME,
}
