# Receiver ↔ session-manager contract

Everything a C++ receiver needs to talk to `session-manager`. You should be able
to implement against this document without reading the Python. Where it points
at a source file, that file is the authority and this is a summary.

Contract pin: **`nexus-proto` at `a83fb3b`**. Build your generated code from that
commit. The two commits after `cef65a6` are comment-only (`SessionOpen.dest_path`
documented as absolute; `SessionOpen` documented as idempotent) but `proto_hash`
covers raw `.proto` bytes, so they still change the hash — see the handshake
section.

The file payload never crosses into `session-manager`. You FEC-decode blocks and
write the bytes to disk yourself; `session-manager` aggregates your per-block
reports, hashes the finished file, and publishes it. It moves bitmaps and
counters, not file data.

---

## 1. Transport

- **`AF_UNIX`, `SOCK_SEQPACKET`.** Connect to the path in `[paths].socket_path`
  (`/run/nexus/session-manager.sock` in the container).
- **Message boundaries are preserved by the kernel.** One `send()` = one
  `recv()` = one message. There is **no length prefix** and you must not add
  one. Do not use `SOCK_STREAM`.
- Every message on the wire — both directions — is a serialized
  **`nexus.rx.RxEnvelope`** (`rx.proto`), a `oneof` over the message types
  below. Decode by `WhichOneof`.
- The manager's receive buffer per datagram is **65536 bytes**
  (`ipc/constants.py:RECV_BUFFER_BYTES`). Your messages are far smaller than
  this; it is a truncation guard, not a target.
- One connection per receiver. Keep it open for the process lifetime.

---

## 2. Handshake

The **first** message you send on the connection must be `ReceiverHello`.
Anything else closes the connection (`peer_handshake_failed`).

```
message ReceiverHello {
  uint32 receiver_id  = 1;  // your identity: 0, 1, 2, ... distinct per receiver
  uint32 pid          = 2;  // informational
  uint32 listen_port  = 3;  // informational (your UDP data-plane port)
  bytes  proto_hash   = 4;  // 32 bytes, computed — see below
}
```

### proto_hash

The algorithm is specified in full, language-independent, in the module
docstring of **`src/session_manager/ipc/handshake.py`**. In brief:

1. List every `*.proto` directly in the contract directory (no recursion).
2. Sort those filenames lexicographically (byte-wise ASCII).
3. Read each file's **raw bytes** — no comment stripping, no whitespace
   normalization, no transformation.
4. Feed the raw bytes of each file, in that sorted order, into **one BLAKE3-256**
   hash, with **no separator** between files.
5. The digest is the standard **32-byte** BLAKE3 output.

The manager computes the same hash at startup over
`NEXUS_PROTO_CONTRACT_DIR` (`libs/nexus-proto/proto`). If your `proto_hash` does
not match **byte for byte**, `verify_proto_hash` raises `ProtoHashMismatchError`
and the manager closes your connection (`peer_proto_hash_mismatch` in its log).
Other receivers on their own connections are unaffected.

A comment-only edit to any `.proto` changes this hash. That is deliberate — a
reproducible check that occasionally over-fires beats a canonicalized one that
quietly diverges between C++ and Python.

---

## 3. Message flow

```
you → manager                     manager → you
─────────────                     ────────────
ReceiverHello  ───────────────►
                              ◄── SessionOpen   (one per already-open session,
                                                 if any exist at connect time)
ManifestSeen   ───────────────►   (per session, first time you see its Manifest)
                              ◄── SessionOpen   (for that session)
BlockDecoded   ───────────────►   (repeatedly, as you decode blocks)
ReceiverStats  ───────────────►   (repeatedly, health counters — optional)
Heartbeat      ───────────────►   (every ~1 s, throughout, from just after Hello)
```

`Config` and `PurgeSession` exist in `rx.proto` but the manager **does not send
them today** (see §6). Do not block waiting for a `Config`.

### Ordering rules

| If you… | then… |
|---|---|
| send anything before `ReceiverHello` | connection closed immediately. |
| send `BlockDecoded` for a session the manager has never seen a `ManifestSeen` for | it is logged `block_decoded_for_unknown_session` and **dropped**. You must send `ManifestSeen` first. |
| send `ManifestSeen` and start writing before `SessionOpen` arrives | the staged file may not exist yet — the manager allocates it *before* it sends `SessionOpen`. Wait for `SessionOpen` on the first sighting. |
| stop sending `Heartbeat` for 15 s | the registry expires you (see §5, property 4). |
| never send `ReceiverStats` | fine — the status display just shows zero counters for you. |
| send a `block_id` ≥ `total_blocks` | that id is silently ignored; the rest of the message is still processed. |
| report the same block twice, or two receivers report one block | idempotent — counted once, no error. |

The manager's dispatch is single-threaded and processes messages in arrival
order, so "first `ManifestSeen` wins and creates the session" is not a race.

---

## 4. Message reference

All are `nexus.rx.*` unless noted. R→M = receiver to manager.

### `ManifestSeen` (R→M)

```
message ManifestSeen {
  uint32 receiver_id = 1;                  // ignored — see note below
  nexus.common.Manifest manifest = 2;      // the manifest you received over UDP
}
```

Sent the first time you see a session's `Manifest` on the data plane. The first
`ManifestSeen` for a `session_id` makes the manager create the session, allocate
the staged file, carve its shm region, and reply with `SessionOpen`. Later
`ManifestSeen` for the same `session_id` (yours or another receiver's) are
duplicates — the manager replies to **that** receiver with the existing
`SessionOpen` and does nothing else.

`Manifest` (`nexus.common`, `common.proto`) fields you consume:
`session_id`, `filepath` (relative — you do **not** use this for writing, see
§5), `file_size`, `file_hash` (BLAKE3-256 of the whole file — this is what the
manager verifies against), `k`, `n`, `block_bytes` (**symbol** size, see §5),
`total_blocks`.

### `SessionOpen` (M→R)

```
message SessionOpen {
  string session_id          = 1;
  string dest_path           = 2;  // ABSOLUTE — write decoded bytes here
  uint32 total_blocks        = 3;
  uint32 k                   = 4;
  uint32 n                   = 5;
  uint32 block_bytes         = 6;  // SYMBOL size (echoes Manifest.block_bytes)
  uint64 block_table_offset  = 7;  // byte offset into the shm segment
  uint64 bitmap_offset       = 8;  // byte offset into the shm segment
}
```

The manager's acknowledgement that the session exists and where it lives.
Idempotent — see §5, property 2.

### `BlockDecoded` (R→M)

```
message BlockDecoded {
  string session_id          = 1;
  uint32 receiver_id         = 2;   // ignored — the manager uses the connection
  repeated uint32 block_ids  = 3;   // blocks you have fully FEC-decoded
}
```

**The authoritative progress record.** Send it as you decode — one block per
message or a batch, whatever suits your pipeline. The manager, per in-range
`block_id`: appends a journal record (for crash recovery), then folds the id
into the session's decoded set. When the set reaches `total_blocks` the manager
hashes `dest_path` and either publishes (`session_verified` path →
`session_published`) or quarantines (`hash_mismatch` → `session_quarantined`).

### `ReceiverStats` (R→M)

```
message ReceiverStats {
  uint32 receiver_id           = 1;  // ignored
  uint64 pkts_ok               = 2;
  uint64 crc_fail              = 3;
  uint64 bad_magic             = 4;
  uint64 unparsable            = 5;
  uint64 duplicates            = 6;
  uint64 no_session            = 7;
  uint64 arena_exhausted       = 8;
  uint64 kernel_drops          = 9;
  uint32 arena_high_water_pct  = 10;
}
```

Optional receiver-level health, shown in the status display. Send it on a timer
(a few seconds). `crc_fail`, `kernel_drops`, and `arena_exhausted` are rendered
prominently when non-zero — they are three unrelated failure modes (corrupting
link, host too slow to drain the socket, decode falling behind) and the operator
needs to see which one they have.

### `Heartbeat` (R→M) — `nexus.ipc.Heartbeat`

```
message Heartbeat {
  uint32 process_id        = 1;
  uint64 timestamp_unix_ms = 2;
}
```

Reused from `ipc.proto`. Send one roughly every second. See §5, property 4.

### `Config` (M→R), `PurgeSession` (M→R)

Defined in `rx.proto`, **not sent by the current manager**. `Config` is the
intended carrier for `shm_name` / `staging_dir` / `journal_dir`; until it is
wired, see §6.

### Note on `receiver_id` inside message bodies

The manager identifies you by the `receiver_id` in your `ReceiverHello` and the
connection it arrived on — "trust the connection, not the payload". The
`receiver_id` fields inside `BlockDecoded` and `ReceiverStats` are **ignored**.
Set them or don't; it changes nothing. Two receivers must not share a
`receiver_id` — the second `ReceiverHello` with a given id replaces the first in
the peer table.

---

## 5. The four properties you must respect

### Property 1 — `SessionOpen.dest_path` is absolute and authoritative

Write decoded block bytes to `dest_path` exactly as given. It is an absolute
local path the manager resolved against its own staging directory. Block `i`
occupies bytes `[i * k * block_bytes, (i+1) * k * block_bytes)`; the last block
is shorter when `file_size` is not a multiple.

**Do not** build a path from `Manifest.filepath` (which is relative) and your own
idea of a staging root. The manager knows its staging directory; you don't, and
you don't need to. If you reconstruct one and it differs from the manager's, you
write a correct file to the wrong place, the manager hashes an empty
pre-allocated file at the right place, and the session fails
`HASH_MISMATCH` with the bytes nowhere near the quarantine.

### Property 2 — `SessionOpen` is idempotent and may arrive more than once

You will receive a `SessionOpen` for a session:

- once as the reply to your `ManifestSeen`;
- again if you send a duplicate `ManifestSeen` (e.g. after a reconnect);
- again for **every** open session immediately after a fresh `ReceiverHello`
  (this is how a receiver that restarts mid-transfer, or connects late, learns
  the session exists at all).

Every copy carries the same `session_id`, `dest_path`, and offsets. Treat a
repeat as a **no-op**. Do not re-initialise state, re-open the file, re-zero a
buffer, or log an error. `rx.proto`'s `SessionOpen` comment says this too.

If you crash and reconnect, remember the `dest_path` you were given — the
manager will **not** re-broadcast a `SessionOpen` in response to a duplicate
`ManifestSeen` for a session it already knows; it sends one, but you must not
*depend* on a fresh one to resume.

### Property 3 — `Manifest.block_bytes` is the SYMBOL size

`block_bytes` (in both `Manifest` and `SessionOpen`) is the size of one FEC
**symbol**, not one block. A block is `k` symbols: **`block_size = k *
block_bytes`**. With `k = 200` and `block_bytes = 1400`, a block is 280 KB.

Get this backwards and every block-to-byte-offset calculation is off by a factor
of `k`. The transfer "completes" and writes garbage that the final hash catches
as `HASH_MISMATCH` with no other symptom.

(This is `SessionSpec.block_bytes` in the Python: `k * symbol_bytes`. The wire
field name is a known trap; it is documented in `common.proto` and
`domain/models.py`.)

### Property 4 — Heartbeat is required

Send `Heartbeat` every ~1 s. The registry
(`services/receiver_registry.py`, `services/constants.py`) drops you after
**`HEARTBEAT_INTERVAL_SECONDS` × `MISSED_HEARTBEAT_LIMIT` = 5 × 3 = 15 s** of
silence.

Being dropped has two consequences beyond disappearing from the status display:

1. `probe_receiver_alive()` returns false once no receiver is alive.
2. On a **manager process restart**, `probe_receiver_alive()` is what decides
   adopt-vs-create. If you are the only receiver and you have gone silent, the
   restarted manager sees a segment with nobody using it, **reinitialises** it,
   and zeroes the completion state you were relying on. Keep heartbeating across
   a manager restart — reconnect, re-`ReceiverHello`, resume heartbeats — and
   the restart **adopts** the segment instead.

---

## 6. Shared memory

The completion segment is a **cross-language contract**. The layout is in
**`src/session_manager/adapters/shm_layout.py`** (packing/parsing) and
**`src/session_manager/adapters/constants.py`** (the format strings). All of it
is **little-endian and standard-size (no alignment padding) by choice, not by
accident** — a native ordering that happens to match on one build machine must
not be mistaken for the contract.

### Segment header — `SHM_HEADER_FORMAT = "<4sI16sIIIQ"`

Maps directly onto a packed C++ struct. Offsets are from the start of the
segment.

| Field | C++ type | Offset | Width | Value |
|---|---|---:|---:|---|
| `magic` | `char[4]` | 0 | 4 | `"NXRX"` (`0x4E 0x58 0x52 0x58`) |
| `version` | `uint32_t` LE | 4 | 4 | `1` (`SHM_VERSION`) |
| `boot_id` | `uint8_t[16]` | 8 | 16 | random, regenerated each manager start |
| `owner_pid` | `uint32_t` LE | 24 | 4 | manager PID |
| `slot_bytes` | `uint32_t` LE | 28 | 4 | `[shm].slot_bytes` |
| `slot_count` | `uint32_t` LE | 32 | 4 | `arena_bytes / slot_bytes` |
| `session_table_offset` | `uint64_t` LE | 36 | 8 | `64` (`SHM_SESSION_TABLE_OFFSET`) |

Packed size is **44 bytes**; bytes 44–63 are reserved. **Validity is `magic` +
`version` only.** The rest is informational — `boot_id` / `owner_pid` let an
operator spot a segment left by a previous boot.

### Session table — at offset 64

A `uint32` LE count, followed by that many fixed entries
(`SHM_SESSION_ENTRY_FORMAT = "<40sQQQ"`, **64 bytes** each). Reserved region is
`SHM_SESSION_TABLE_BYTES = 4096`.

| Field | C++ type | Offset in entry | Width |
|---|---|---:|---:|
| `session_id` | `char[40]` | 0 | 40 (UTF-8, `\0`-padded) |
| `total_blocks` | `uint64_t` LE | 40 | 8 |
| `block_table_offset` | `uint64_t` LE | 48 | 8 |
| `bitmap_offset` | `uint64_t` LE | 56 | 8 |

The manager writes this table when it opens a session and re-reads it on an
adopting restart to learn which sessions are already running. You can read it,
or you can take the same offsets straight from `SessionOpen` fields 7 and 8 —
they are identical.

### Per-session regions

For each session the manager carves two equal regions, each
`ceil(total_blocks / 8)` bytes:

- **`block_table`** at `block_table_offset`
- **`bitmap`** at `bitmap_offset` — bit `i` (LSB-first within each byte) set =
  block `i` complete.

The manager's optional cross-check reads a **popcount of the `bitmap` region**
and compares it to its UDS-derived decoded count. It never writes these regions
for a running session (it zeroes the `bitmap` once at session open).

### The segment name

You need the segment's name to `shm_open` it. `SessionOpen` does **not** carry
it and the `Config` message that would is not sent yet (§4). For now: take it
out of band from `[shm].name` (env `NEXUS_SHM_NAME`, default `nexus-rx`) and it
**must** equal the manager's. The staged file you get from `dest_path`, so you
do **not** need `staging_dir`; you never touch the journal, so you do **not**
need `journal_dir`.

---

## 7. The open question — for you to answer

**Does your receiver write the shm completion `bitmap`, or is `BlockDecoded`
over UDS the only progress path?**

- The UDS `BlockDecoded` stream is authoritative and sufficient on its own. A
  working receiver can ignore shm entirely for progress reporting and still
  drive a session to `VERIFIED`.
- The shm cross-check exists only to catch a disagreement between the two. It is
  **off by default** (`[aggregation].shm_crosscheck = false`) and, when on, warns
  **once per session** rather than every poll.
- If your answer is **no**, the cross-check code
  (`ProgressAggregator._cross_check` / `_bitmap_popcount`, `ShmReader.bitmap_for`,
  `ShmReader.block_table_seen`) will be **deleted** — it is dead weight otherwise.
- If your answer is **yes**, keep it, and confirm the bit ordering above so the
  popcount and your writes agree.

Reply with yes/no and we'll settle it before integration step 2.
