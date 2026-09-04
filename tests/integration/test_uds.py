import asyncio
import socket
from collections.abc import Callable
from pathlib import Path

import ipc_pb2
import pytest
import rx_pb2

from session_manager.domain.ids import ReceiverId
from session_manager.ipc import codec
from session_manager.ipc.constants import RECV_BUFFER_BYTES, SEND_QUEUE_MAXSIZE
from session_manager.ipc.errors import SendQueueFullError, UnknownReceiverError
from session_manager.ipc.uds import UdsIpcServer

pytestmark = pytest.mark.skipif(
    not hasattr(socket, "AF_UNIX"), reason="AF_UNIX sockets require a POSIX host"
)

EXPECTED_PROTO_HASH = b"expected-proto-hash"


async def connect_client(socket_path: Path) -> socket.socket:
    loop = asyncio.get_running_loop()
    while True:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        client.setblocking(False)
        try:
            await loop.sock_connect(client, str(socket_path))
            return client
        except (ConnectionRefusedError, FileNotFoundError):
            client.close()
            await asyncio.sleep(0.01)


async def send_hello(
    client: socket.socket, receiver_id: int, proto_hash: bytes = EXPECTED_PROTO_HASH
) -> None:
    loop = asyncio.get_running_loop()
    hello = rx_pb2.ReceiverHello(
        receiver_id=receiver_id,
        pid=1000 + receiver_id,
        listen_port=9100 + receiver_id,
        proto_hash=proto_hash,
    )
    await loop.sock_sendall(client, codec.encode(hello))


async def test_serve_creates_missing_parent_directory(tmp_path: Path) -> None:
    socket_path = tmp_path / "nested" / "run" / "session-manager.sock"
    server = UdsIpcServer(socket_path, expected_proto_hash=EXPECTED_PROTO_HASH)
    serve_task = asyncio.create_task(server.serve())
    try:
        await asyncio.wait_for(_wait_until(lambda: socket_path.exists()), timeout=1)
    finally:
        serve_task.cancel()
        await asyncio.gather(serve_task, return_exceptions=True)


async def test_serve_unlinks_a_stale_socket_file(tmp_path: Path) -> None:
    socket_path = tmp_path / "session-manager.sock"
    socket_path.write_text("stale")
    server = UdsIpcServer(socket_path, expected_proto_hash=EXPECTED_PROTO_HASH)
    serve_task = asyncio.create_task(server.serve())
    try:
        client = await asyncio.wait_for(connect_client(socket_path), timeout=1)
        client.close()
    finally:
        serve_task.cancel()
        await asyncio.gather(serve_task, return_exceptions=True)


async def test_two_peers_exchange_messages_both_ways(tmp_path: Path) -> None:
    socket_path = tmp_path / "session-manager.sock"
    server = UdsIpcServer(socket_path, expected_proto_hash=EXPECTED_PROTO_HASH)
    serve_task = asyncio.create_task(server.serve())
    loop = asyncio.get_running_loop()
    incoming = server.incoming()

    try:
        await asyncio.wait_for(_wait_until(lambda: socket_path.exists()), timeout=1)

        client_a = await connect_client(socket_path)
        await send_hello(client_a, receiver_id=1)
        receiver_a, hello_a_raw = await asyncio.wait_for(anext(incoming), timeout=1)
        assert receiver_a == ReceiverId(1)
        assert codec.decode(hello_a_raw)[0] == "receiver_hello"

        client_b = await connect_client(socket_path)
        await send_hello(client_b, receiver_id=2)
        receiver_b, hello_b_raw = await asyncio.wait_for(anext(incoming), timeout=1)
        assert receiver_b == ReceiverId(2)
        assert codec.decode(hello_b_raw)[0] == "receiver_hello"

        heartbeat = ipc_pb2.Heartbeat(process_id=1, timestamp_unix_ms=42)
        await loop.sock_sendall(client_a, codec.encode(heartbeat))
        receiver_from_a, raw_from_a = await asyncio.wait_for(anext(incoming), timeout=1)
        assert receiver_from_a == ReceiverId(1)
        assert codec.decode(raw_from_a) == ("heartbeat", heartbeat)

        block_decoded = rx_pb2.BlockDecoded(session_id="s1", receiver_id=1, block_ids=[0, 3, 7])
        await loop.sock_sendall(client_a, codec.encode(block_decoded))
        receiver_blocks, raw_blocks = await asyncio.wait_for(anext(incoming), timeout=1)
        assert receiver_blocks == ReceiverId(1)
        assert codec.decode(raw_blocks) == ("block_decoded", block_decoded)

        session_open = rx_pb2.SessionOpen(
            session_id="s1",
            dest_path="/staging/s1",
            total_blocks=8,
            k=200,
            n=255,
            block_bytes=280000,
        )
        await server.send(ReceiverId(2), codec.encode(session_open))
        received = await asyncio.wait_for(loop.sock_recv(client_b, RECV_BUFFER_BYTES), timeout=1)
        assert codec.decode(received) == ("session_open", session_open)

        client_a.close()
        await asyncio.wait_for(_wait_until(lambda: ReceiverId(1) not in server._peers), timeout=1)

        with pytest.raises(UnknownReceiverError):
            await server.send(ReceiverId(1), b"anything")

        assert not serve_task.done()

        purge = rx_pb2.PurgeSession(session_id="s1", reason="verified")
        await server.send(ReceiverId(2), codec.encode(purge))
        received_after = await asyncio.wait_for(
            loop.sock_recv(client_b, RECV_BUFFER_BYTES), timeout=1
        )
        assert codec.decode(received_after) == ("purge_session", purge)

        client_b.close()
    finally:
        serve_task.cancel()
        await asyncio.gather(serve_task, return_exceptions=True)


async def test_mismatched_proto_hash_is_refused(tmp_path: Path) -> None:
    socket_path = tmp_path / "session-manager.sock"
    server = UdsIpcServer(socket_path, expected_proto_hash=EXPECTED_PROTO_HASH)
    serve_task = asyncio.create_task(server.serve())

    loop = asyncio.get_running_loop()
    try:
        await asyncio.wait_for(_wait_until(lambda: socket_path.exists()), timeout=1)

        client = await connect_client(socket_path)
        await send_hello(client, receiver_id=1, proto_hash=b"stale-proto-hash")

        response = await asyncio.wait_for(loop.sock_recv(client, RECV_BUFFER_BYTES), timeout=1)
        assert response == b""

        assert ReceiverId(1) not in server._peers
        with pytest.raises(UnknownReceiverError):
            await server.send(ReceiverId(1), b"anything")
        assert not serve_task.done()

        client.close()
    finally:
        serve_task.cancel()
        await asyncio.gather(serve_task, return_exceptions=True)


async def test_full_send_queue_raises_send_queue_full_error(tmp_path: Path) -> None:
    socket_path = tmp_path / "session-manager.sock"
    server = UdsIpcServer(socket_path, expected_proto_hash=EXPECTED_PROTO_HASH)
    serve_task = asyncio.create_task(server.serve())

    try:
        await asyncio.wait_for(_wait_until(lambda: socket_path.exists()), timeout=1)

        client = await connect_client(socket_path)
        await send_hello(client, receiver_id=1)
        await asyncio.wait_for(_wait_until(lambda: ReceiverId(1) in server._peers), timeout=1)

        payload = codec.encode(ipc_pb2.Heartbeat(process_id=1, timestamp_unix_ms=1))

        with pytest.raises(SendQueueFullError):
            for _ in range(SEND_QUEUE_MAXSIZE + 1):
                await server.send(ReceiverId(1), payload)

        client.close()
    finally:
        serve_task.cancel()
        await asyncio.gather(serve_task, return_exceptions=True)


async def test_write_loop_deregisters_peer_on_os_error() -> None:
    server = UdsIpcServer(Path("unused.sock"), expected_proto_hash=EXPECTED_PROTO_HASH)
    receiver_id = ReceiverId(1)
    send_queue: asyncio.Queue[bytes] = asyncio.Queue()
    server._peers[receiver_id] = send_queue

    server_side, client_side = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    server_side.setblocking(False)
    client_side.close()

    send_queue.put_nowait(b"payload")
    await asyncio.wait_for(server._write_loop(receiver_id, server_side, send_queue), timeout=1)

    assert receiver_id not in server._peers
    server_side.close()


async def test_a_write_failure_tears_down_the_whole_peer() -> None:
    server = UdsIpcServer(Path("unused.sock"), expected_proto_hash=EXPECTED_PROTO_HASH)
    server_side, client_side = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    server_side.setblocking(False)
    client_side.setblocking(False)

    peer_task = asyncio.create_task(server._handle_peer(server_side))
    try:
        loop = asyncio.get_running_loop()
        hello = rx_pb2.ReceiverHello(
            receiver_id=1, pid=1234, listen_port=9101, proto_hash=EXPECTED_PROTO_HASH
        )
        await loop.sock_sendall(client_side, codec.encode(hello))
        await asyncio.wait_for(_wait_until(lambda: ReceiverId(1) in server._peers), timeout=1)

        # Breaks only the write direction; client_side otherwise stays open.
        server_side.shutdown(socket.SHUT_WR)

        payload = codec.encode(ipc_pb2.Heartbeat(process_id=1, timestamp_unix_ms=1))
        await server.send(ReceiverId(1), payload)

        await asyncio.wait_for(_wait_until(lambda: ReceiverId(1) not in server._peers), timeout=1)
        await asyncio.wait_for(peer_task, timeout=1)
        assert peer_task.done()
    finally:
        client_side.close()
        if not peer_task.done():
            peer_task.cancel()
            await asyncio.gather(peer_task, return_exceptions=True)


async def test_a_reconnecting_peer_keeps_its_live_queue_when_the_stale_one_tears_down(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "session-manager.sock"
    server = UdsIpcServer(socket_path, expected_proto_hash=EXPECTED_PROTO_HASH)
    serve_task = asyncio.create_task(server.serve())
    loop = asyncio.get_running_loop()

    try:
        await asyncio.wait_for(_wait_until(lambda: socket_path.exists()), timeout=1)

        first = await connect_client(socket_path)
        await send_hello(first, receiver_id=1)
        await asyncio.wait_for(_wait_until(lambda: ReceiverId(1) in server._peers), timeout=1)
        stale_queue = server._peers[ReceiverId(1)]

        second = await connect_client(socket_path)
        await send_hello(second, receiver_id=1)
        await asyncio.wait_for(
            _wait_until(lambda: server._peers.get(ReceiverId(1)) is not stale_queue), timeout=1
        )
        live_queue = server._peers[ReceiverId(1)]

        first.close()
        await asyncio.sleep(0.05)

        assert server._peers.get(ReceiverId(1)) is live_queue

        purge = rx_pb2.PurgeSession(session_id="s1", reason="verified")
        await server.send(ReceiverId(1), codec.encode(purge))
        received = await asyncio.wait_for(loop.sock_recv(second, RECV_BUFFER_BYTES), timeout=1)
        assert codec.decode(received) == ("purge_session", purge)

        second.close()
    finally:
        serve_task.cancel()
        await asyncio.gather(serve_task, return_exceptions=True)


async def _wait_until(predicate: Callable[[], bool]) -> None:
    while not predicate():
        await asyncio.sleep(0.01)
