"""Cross-service proto_hash contract.

Both the Python services (file-monitor, session-manager) and the C++
sender/receiver must compute a byte-identical hash of the shared .proto
contract, so that a Hello carrying a stale hash is refused before any data
flows. The algorithm, in
enough detail to implement independently in any language:

1. List every file matching `*.proto` directly in the contract directory
   (no recursion).
2. Sort those filenames lexicographically (plain byte-wise ASCII sort).
3. Read each file's raw bytes — no decoding, no comment stripping, no
   whitespace normalization, no transformation of any kind.
4. Feed the raw bytes of each file, in that sorted order, into a single
   BLAKE3-256 hash, with no separator between files.
5. The digest is the standard 32-byte BLAKE3 output.

Trade-off: because the hash covers raw bytes, a comment-only edit to a
.proto file changes the hash and forces every process to rebuild against
the new contract. That is accepted deliberately — a reproducible check that
occasionally over-fires beats a "canonicalized" one that quietly diverges
between a Python and a C++ implementation and blocks all integration.

This is exempt from the project's brevity rule: it is a cross-language
contract another team implements from this prose in C++, not documentation
of the Python below it, so it must stand on its own without reading the code.
"""

from pathlib import Path

import blake3

from session_manager.domain.ids import ReceiverId
from session_manager.ipc.errors import ProtoHashMismatchError


def compute_proto_hash(proto_dir: Path) -> bytes:
    hasher = blake3.blake3()
    for path in sorted(proto_dir.glob("*.proto")):
        hasher.update(path.read_bytes())
    return hasher.digest()


def verify_proto_hash(receiver_id: ReceiverId, reported_hash: bytes, expected_hash: bytes) -> None:
    if reported_hash != expected_hash:
        raise ProtoHashMismatchError(receiver_id, reported_hash, expected_hash)
