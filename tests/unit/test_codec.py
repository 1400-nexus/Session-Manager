import ipc_pb2
import pytest
import rx_pb2

from session_manager.ipc.codec import decode, encode


def test_encode_decode_round_trip_receiver_hello() -> None:
    original = rx_pb2.ReceiverHello(receiver_id=3, pid=1234, listen_port=9100, proto_hash=b"abc")
    field_name, decoded = decode(encode(original))
    assert field_name == "receiver_hello"
    assert decoded == original


def test_encode_decode_round_trip_block_decoded() -> None:
    original = rx_pb2.BlockDecoded(session_id="s-1", receiver_id=2, block_ids=[1, 2, 5])
    field_name, decoded = decode(encode(original))
    assert field_name == "block_decoded"
    assert decoded == original


def test_encode_decode_round_trip_heartbeat_from_the_ipc_package() -> None:
    original = ipc_pb2.Heartbeat(process_id=42, timestamp_unix_ms=1_700_000_000_000)
    field_name, decoded = decode(encode(original))
    assert field_name == "heartbeat"
    assert decoded == original


def test_decode_rejects_envelope_with_no_message_set() -> None:
    empty_envelope = rx_pb2.RxEnvelope()
    with pytest.raises(ValueError, match="RxEnvelope has no message set"):
        decode(empty_envelope.SerializeToString())
