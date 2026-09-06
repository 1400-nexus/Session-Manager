"""Throwaway stand-in for a C++ receiver, for exercising the RX wire contract.

Structural precedent: file-monitor's tests/integration/stub_sender.py. Must
not import anything from session_manager.services or session_manager.domain
-- only ipc.codec, ipc.handshake, ipc.constants. If this shared the real
domain/service code it would agree with the manager by construction (same
block_byte_range formula, same validation) and prove nothing; every piece of
wire-contract arithmetic below (block offsets, file_hash, the manifest shape)
is reimplemented independently, the same rule file-monitor's stub follows.
"""

import argparse
import asyncio
import hashlib
import os
import socket
import sys
from pathlib import Path
from typing import Any, cast

import blake3
import common_pb2
import ipc_pb2
import rx_pb2

from session_manager.ipc import codec, handshake
from session_manager.ipc.constants import RECV_BUFFER_BYTES, SESSION_OPEN_FIELD_NAME

DEFAULT_PROTO_DIR = "libs/nexus-proto/proto"

# Fixed FEC shape for every synthetic manifest this stub builds. Deliberately
# trivial -- only block_byte_range's arithmetic (k * symbol_bytes per block)
# needs to hold, not anything resembling a real transfer's parameters.
K = 1
N = 2
SYMBOL_BYTES = 64
BLOCK_BYTES = K * SYMBOL_BYTES

# Every milestone this stub supports runs exactly three receivers
# cooperating on one session ("three stubs, all blocks"); a stub's shard is
# its residue class mod this constant, the same convention file-monitor's
# dispatcher uses for senders.
RECEIVER_COUNT = 3

HEARTBEAT_INTERVAL_SECONDS = 1.0
RECONNECT_DELAY_SECONDS = 0.5
SESSION_OPEN_TIMEOUT_SECONDS = 10.0

# Paced deliberately, not for realism: reporting a whole shard in one
# instantaneous batch leaves no window for a milestone script to land a
# mid-transfer kill inside. One BlockDecoded per block, spaced out, gives
# an externally observable "still in flight" period of shard_size * this
# many seconds.
BLOCK_REPORT_PACING_SECONDS = 0.02


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Throwaway stand-in for a C++ receiver, for exercising the RX wire contract."
    )
    parser.add_argument("--receiver-id", type=int, required=True)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--proto-dir", type=Path, default=Path(DEFAULT_PROTO_DIR))
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--blocks", type=int, required=True)
    parser.add_argument(
        "--withhold", default="", help="comma-separated block ids never to report as decoded"
    )
    parser.add_argument(
        "--corrupt", default="", help="comma-separated block ids to corrupt before writing"
    )
    return parser.parse_args()


def _parse_id_list(raw: str) -> frozenset[int]:
    return frozenset(int(part) for part in raw.split(",") if part.strip())


def _make_durable(file_descriptor: int) -> None:
    # fdatasync where it exists (Linux) -- the file is pre-allocated so its
    # size never changes and a metadata sync is not needed; fsync otherwise.
    if hasattr(os, "fdatasync"):
        os.fdatasync(file_descriptor)
    else:
        os.fsync(file_descriptor)


def _relpath_for(session_id: str) -> str:
    return f"stub-{session_id}.bin"


def _block_content(session_id: str, block_id: int) -> bytes:
    # Pure function of (session_id, block_id): every stub -- and the same
    # stub across a reconnect -- derives byte-identical content with no
    # coordination, which is what lets 3 independent processes agree on one
    # file_hash without ever exchanging bytes.
    seed = hashlib.sha256(f"{session_id}:{block_id}".encode()).digest()
    repeats = BLOCK_BYTES // len(seed) + 1
    return (seed * repeats)[:BLOCK_BYTES]


def _corrupted(content: bytes) -> bytes:
    return bytes([content[0] ^ 0xFF]) + content[1:]


def _file_hash(session_id: str, total_blocks: int) -> bytes:
    hasher = blake3.blake3()
    for block_id in range(total_blocks):
        hasher.update(_block_content(session_id, block_id))
    return hasher.digest()


def _block_byte_range(block_id: int) -> tuple[int, int]:
    # Independent reimplementation of domain.blocks.block_byte_range (which
    # this stub must not import): every block here is exactly BLOCK_BYTES,
    # since --blocks * BLOCK_BYTES is always the manifest's exact file_size,
    # so there is deliberately never a shorter final block to special-case.
    return block_id * BLOCK_BYTES, BLOCK_BYTES


def _shard_blocks(receiver_id: int, total_blocks: int) -> list[int]:
    residue = receiver_id % RECEIVER_COUNT
    return [block_id for block_id in range(total_blocks) if block_id % RECEIVER_COUNT == residue]


def _build_manifest(session_id: str, total_blocks: int) -> Any:
    return common_pb2.Manifest(
        session_id=session_id,
        filepath=_relpath_for(session_id),
        file_size=total_blocks * BLOCK_BYTES,
        file_hash=_file_hash(session_id, total_blocks),
        k=K,
        n=N,
        block_bytes=SYMBOL_BYTES,
        total_blocks=total_blocks,
    )


async def connect(socket_path: Path) -> socket.socket:
    loop = asyncio.get_running_loop()
    client = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    client.setblocking(False)
    await loop.sock_connect(client, str(socket_path))
    return client


async def send_hello(client: socket.socket, receiver_id: int, proto_hash: bytes) -> None:
    loop = asyncio.get_running_loop()
    hello = rx_pb2.ReceiverHello(
        receiver_id=receiver_id,
        pid=os.getpid(),
        listen_port=0,  # unused: this stub never receives real UDP FEC packets
        proto_hash=proto_hash,
    )
    await loop.sock_sendall(client, codec.encode(hello))


async def send_manifest_seen(client: socket.socket, receiver_id: int, manifest: Any) -> None:
    loop = asyncio.get_running_loop()
    manifest_seen = rx_pb2.ManifestSeen(receiver_id=receiver_id, manifest=manifest)
    await loop.sock_sendall(client, codec.encode(manifest_seen))


async def send_block_decoded(
    client: socket.socket, receiver_id: int, session_id: str, block_ids: list[int]
) -> None:
    if not block_ids:
        return
    loop = asyncio.get_running_loop()
    message = rx_pb2.BlockDecoded(
        session_id=session_id, receiver_id=receiver_id, block_ids=block_ids
    )
    await loop.sock_sendall(client, codec.encode(message))


async def heartbeat_loop(client: socket.socket) -> None:
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
        await loop.sock_sendall(client, codec.encode(ipc_pb2.Heartbeat()))


async def _wait_for_session_open(client: socket.socket, session_id: str) -> str:
    # dest_path is absolute -- the manager resolves it against its own
    # staging_dir and this stub must write to exactly that path, never
    # reconstruct one from its own idea of a staging root (see rx.proto's
    # SessionOpen.dest_path doc comment; that coupling is what broke this
    # stub the first time around).
    loop = asyncio.get_running_loop()
    while True:
        raw = await loop.sock_recv(client, RECV_BUFFER_BYTES)
        if not raw:
            raise ConnectionError("disconnected while waiting for SessionOpen")
        field_name, message = codec.decode(raw)
        if field_name == SESSION_OPEN_FIELD_NAME and cast(Any, message).session_id == session_id:
            return cast(str, cast(Any, message).dest_path)


async def _drain_until_disconnected(client: socket.socket) -> None:
    loop = asyncio.get_running_loop()
    while True:
        raw = await loop.sock_recv(client, RECV_BUFFER_BYTES)
        if not raw:
            return


async def _write_and_report_blocks(
    client: socket.socket,
    receiver_id: int,
    session_id: str,
    dest_path: str,
    total_blocks: int,
    withheld: frozenset[int],
    corrupted: frozenset[int],
) -> None:
    with open(dest_path, "r+b") as handle:
        for block_id in _shard_blocks(receiver_id, total_blocks):
            if block_id in withheld:
                continue
            content = _block_content(session_id, block_id)
            if block_id in corrupted:
                content = _corrupted(content)
            offset, _length = _block_byte_range(block_id)
            handle.seek(offset)
            handle.write(content)
            # Durable BEFORE the report. session-manager journals every
            # BlockDecoded and, on an adopting restart, counts a journaled
            # block as done and never expects it again -- so a block reported
            # while its bytes are still in a buffer becomes a hole in the
            # recovered file that only surfaces as a hash mismatch at the very
            # end. This is the contract (rx.proto BlockDecoded,
            # docs/RECEIVER_CONTRACT.md §5); a real receiver carries the same
            # cost on its hot path.
            handle.flush()
            _make_durable(handle.fileno())
            await send_block_decoded(client, receiver_id, session_id, [block_id])
            await asyncio.sleep(BLOCK_REPORT_PACING_SECONDS)
    print(f"[receiver {receiver_id}] finished reporting its shard for {session_id}", flush=True)


class _Progress:
    """Survives across reconnects within one `run()` call.

    Only the FIRST successful ManifestSeen for a session_id gets a
    SessionOpen back -- a resend after a reconnect is a duplicate the
    authority silently drops (SessionAuthority.handle_manifest_seen) -- so a
    reconnecting stub must know not to wait for one that will never come,
    and must have cached the dest_path that SessionOpen carried the first
    time, since nothing will hand it over again.
    """

    def __init__(self) -> None:
        self.dest_path: str | None = None


async def _report_then_drain(
    client: socket.socket,
    args: argparse.Namespace,
    progress: _Progress,
    heartbeat_task: asyncio.Task[None],
) -> None:
    try:
        if progress.dest_path is None:
            progress.dest_path = await asyncio.wait_for(
                _wait_for_session_open(client, args.session_id),
                timeout=SESSION_OPEN_TIMEOUT_SECONDS,
            )

        await _write_and_report_blocks(
            client,
            args.receiver_id,
            args.session_id,
            progress.dest_path,
            args.blocks,
            _parse_id_list(args.withhold),
            _parse_id_list(args.corrupt),
        )

        await _drain_until_disconnected(client)
    finally:
        # Same reason as file-monitor's stub_sender.receive_loop: the
        # TaskGroup below only exits once every child task finishes, and
        # heartbeat_loop never finishes on its own.
        heartbeat_task.cancel()


async def run_once(args: argparse.Namespace, progress: _Progress, manifest: Any) -> None:
    proto_hash = handshake.compute_proto_hash(args.proto_dir)
    client = await connect(args.socket)
    try:
        await send_hello(client, args.receiver_id, proto_hash)
        print(f"[receiver {args.receiver_id}] connected, proto_hash={proto_hash.hex()}", flush=True)
        await send_manifest_seen(client, args.receiver_id, manifest)

        async with asyncio.TaskGroup() as task_group:
            heartbeat_task = task_group.create_task(heartbeat_loop(client))
            task_group.create_task(_report_then_drain(client, args, progress, heartbeat_task))
    finally:
        client.close()


async def run(args: argparse.Namespace) -> int:
    manifest = _build_manifest(args.session_id, args.blocks)
    progress = _Progress()
    while True:
        try:
            await run_once(args, progress, manifest)
        except* (OSError, ConnectionError) as exception_group:
            for error in exception_group.exceptions:
                # str(error) alone can be empty for some OSError subclasses,
                # which previously produced an undiagnosable bare
                # "disconnected: " with no text at all -- the type name is
                # never empty.
                print(
                    f"[receiver {args.receiver_id}] disconnected: {type(error).__name__}: {error}",
                    flush=True,
                )
        # A wrong proto_hash never gets past this point either -- the server
        # closes the connection right after Hello, every time, so this stub
        # just keeps retrying forever like a real receiver would against a
        # persistently stale contract. The milestone script kills the
        # process when it's done asserting, the same way it kills every
        # other stub; this loop has no self-terminating condition.
        await asyncio.sleep(RECONNECT_DELAY_SECONDS)


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
