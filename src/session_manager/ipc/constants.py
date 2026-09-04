# SOCK_SEQPACKET silently truncates anything beyond this size with no error,
# so it stays well above the largest Envelope seen today.
RECV_BUFFER_BYTES = 65536

SEND_QUEUE_MAXSIZE = 64
INCOMING_QUEUE_MAXSIZE = 256

# Both ipc.Envelope and rx.RxEnvelope name their oneof group "msg".
ENVELOPE_ONEOF_GROUP_NAME = "msg"

# RxEnvelope oneof field names — the wire contract with the C++ receivers.
# receiver_hello is the RX-side handshake message (file-monitor's Envelope
# uses sender_hello for the same role on the TX side).
RECEIVER_HELLO_FIELD_NAME = "receiver_hello"
MANIFEST_SEEN_FIELD_NAME = "manifest_seen"
BLOCK_DECODED_FIELD_NAME = "block_decoded"
RECEIVER_STATS_FIELD_NAME = "receiver_stats"
HEARTBEAT_FIELD_NAME = "heartbeat"
CONFIG_FIELD_NAME = "config"
SESSION_OPEN_FIELD_NAME = "session_open"
PURGE_SESSION_FIELD_NAME = "purge_session"
