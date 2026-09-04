import asyncio
import functools
import socket
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import structlog

from session_manager.domain.ids import ReceiverId
from session_manager.ipc import codec, handshake
from session_manager.ipc.constants import (
    INCOMING_QUEUE_MAXSIZE,
    RECEIVER_HELLO_FIELD_NAME,
    RECV_BUFFER_BYTES,
    SEND_QUEUE_MAXSIZE,
)
from session_manager.ipc.errors import (
    HandshakeError,
    ProtoHashMismatchError,
    SendQueueFullError,
    UnknownReceiverError,
)

logger = structlog.get_logger(__name__)


class UdsIpcServer:
    def __init__(self, socket_path: Path, expected_proto_hash: bytes) -> None:
        self._socket_path: Path = socket_path
        self._expected_proto_hash: bytes = expected_proto_hash
        self._peers: dict[ReceiverId, asyncio.Queue[bytes]] = {}
        self._peer_tasks: set[asyncio.Task[None]] = set()
        self._incoming: asyncio.Queue[tuple[ReceiverId, bytes]] = asyncio.Queue(
            maxsize=INCOMING_QUEUE_MAXSIZE
        )

    async def serve(self) -> None:
        self._socket_path.parent.mkdir(parents=True, exist_ok=True)
        self._socket_path.unlink(missing_ok=True)

        listen_socket = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        listen_socket.setblocking(False)
        listen_socket.bind(str(self._socket_path))
        listen_socket.listen()

        loop = asyncio.get_running_loop()
        try:
            while True:
                connection, _ = await loop.sock_accept(listen_socket)
                connection.setblocking(False)
                task = asyncio.create_task(self._handle_peer(connection))
                self._peer_tasks.add(task)
                task.add_done_callback(self._on_peer_task_done)
        finally:
            listen_socket.close()
            for task in self._peer_tasks:
                task.cancel()
            await asyncio.gather(*self._peer_tasks, return_exceptions=True)

    def _on_peer_task_done(self, task: asyncio.Task[None]) -> None:
        self._peer_tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error("peer_task_failed", error=str(error))

    async def _handle_peer(self, connection: socket.socket) -> None:
        loop = asyncio.get_running_loop()
        receiver_id: ReceiverId | None = None
        send_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)
        writer_task: asyncio.Task[None] | None = None
        try:
            while True:
                raw = await loop.sock_recv(connection, RECV_BUFFER_BYTES)
                if not raw:
                    break

                if len(raw) == RECV_BUFFER_BYTES:
                    logger.warning(
                        "recv_buffer_exactly_full_possible_truncation",
                        receiver_id=receiver_id,
                        buffer_bytes=RECV_BUFFER_BYTES,
                    )

                if receiver_id is None:
                    receiver_id, writer_task = self._complete_handshake(connection, send_queue, raw)

                if self._incoming.full():
                    logger.warning("incoming_queue_full", receiver_id=receiver_id)
                await self._incoming.put((receiver_id, raw))
        except HandshakeError as error:
            logger.warning("peer_handshake_failed", error=str(error))
        except ProtoHashMismatchError as error:
            logger.error("peer_proto_hash_mismatch", error=str(error))
        finally:
            if receiver_id is not None:
                if self._peers.get(receiver_id) is send_queue:
                    self._peers.pop(receiver_id, None)
                logger.info("peer_disconnected", receiver_id=receiver_id)
            if writer_task is not None:
                writer_task.cancel()
            connection.close()

    def _complete_handshake(
        self, connection: socket.socket, send_queue: asyncio.Queue[bytes], raw: bytes
    ) -> tuple[ReceiverId, asyncio.Task[None]]:
        field_name, message = codec.decode(raw)
        if field_name != RECEIVER_HELLO_FIELD_NAME:
            raise HandshakeError(field_name)
        hello = cast(Any, message)
        receiver_id = ReceiverId(hello.receiver_id)
        handshake.verify_proto_hash(receiver_id, hello.proto_hash, self._expected_proto_hash)
        self._peers[receiver_id] = send_queue
        writer_task = asyncio.create_task(self._write_loop(receiver_id, connection, send_queue))
        writer_task.add_done_callback(functools.partial(self._on_writer_task_done, receiver_id))
        logger.info("peer_connected", receiver_id=receiver_id)
        return receiver_id, writer_task

    def _on_writer_task_done(self, receiver_id: ReceiverId, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error("peer_writer_task_failed", receiver_id=receiver_id, error=str(error))

    async def _write_loop(
        self, receiver_id: ReceiverId, connection: socket.socket, queue: asyncio.Queue[bytes]
    ) -> None:
        loop = asyncio.get_running_loop()
        try:
            while True:
                payload = await queue.get()
                await loop.sock_sendall(connection, payload)
        except OSError as error:
            logger.error("peer_write_failed", receiver_id=receiver_id, error=str(error))
            if self._peers.get(receiver_id) is queue:
                self._peers.pop(receiver_id, None)
            try:
                connection.shutdown(socket.SHUT_RD)
            except OSError:
                pass

    async def send(self, receiver_id: ReceiverId, payload: bytes) -> None:
        queue = self._peers.get(receiver_id)
        if queue is None:
            raise UnknownReceiverError(receiver_id)
        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:
            raise SendQueueFullError(receiver_id) from None

    def incoming(self) -> AsyncIterator[tuple[ReceiverId, bytes]]:
        return self._iter_incoming()

    async def _iter_incoming(self) -> AsyncIterator[tuple[ReceiverId, bytes]]:
        while True:
            yield await self._incoming.get()
