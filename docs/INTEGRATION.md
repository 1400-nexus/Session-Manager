# Integration plan — session-manager ↔ receivers

Run top to bottom. Each step isolates one variable, so a failure points at one
thing. Do not skip ahead; step N assumes N−1 passed.

Prerequisite: the `receiver` repo has a buildable C++ binary. As of this
writing it does not (one commit, README only), so integration cannot start.
Steps 1–2 are the first things to do once it exists.

Everything here is **native** (`python -m session_manager.main`, a real
`nexus-receiver` binary). The graded machines run native. Containers come after
step 6.

---

## Step 1 — Contract hash agreement

**Proves:** every process that opens a UDS connection computes the same
`proto_hash` from `nexus-proto@7f406db`. One minute of work; nothing downstream
can work if this is wrong.

**This is not just a receiver concern.** `file-monitor` is the UDS server on the
TX side: a sender connects with `SenderHello` carrying `proto_hash` and
`file-monitor` **refuses the connection** on a mismatch (its milestone 3 proves
exactly that). `session-manager` does the same for receivers with
`ReceiverHello`. A mismatch is a **closed connection**, not a subtle
misbehaviour — so every UDS peer in the system (A's senders, B's receivers,
both Python services) must build against the **same commit**:

| process | connects to | must be on |
|---|---|---|
| `file-monitor` | — (server) | `7f406db` |
| N senders | `file-monitor` | `7f406db` |
| `session-manager` | — (server) | `7f406db` |
| N receivers | `session-manager` | `7f406db` |

As of this writing the `sender` repo is pinned at `60bd06e` (five commits
behind `7f406db`) — **that sender will be refused by `file-monitor`**, and it
is also missing `AssignSession.source_path` and the `Manifest.sender_id`
shard-residue comment. It must be bumped before step 6; see
`file-monitor/docs/SENDER_CONTRACT.md`.

**Run:**

```bash
# session-manager side (file-monitor is identical, its own ipc/handshake.py)
cd session-manager
python -c "from session_manager.ipc.handshake import compute_proto_hash; \
          from pathlib import Path; \
          print(compute_proto_hash(Path('libs/nexus-proto/proto')).hex())"
```

B computes the same hash in C++ over the same `.proto` directory (the algorithm
is in `RECEIVER_CONTRACT.md §2` / `ipc/handshake.py`) and prints it hex.

At `nexus-proto@7f406db` this is:

```
38cac339d495241ae757fbeec84a6ecdc5377f838798ff9df1e19650bcff20df
```

(Recompute rather than trusting this line — it changes with any `.proto` edit,
including a comment.)

**Pass:** all sides print the same 64-char hex string, equal to the value above.

**Failure means:** one of —
- Different `nexus-proto` commit. Check `git -C libs/nexus-proto rev-parse HEAD`
  everywhere; all must be `7f406db`.
- The implementation stripped comments or normalized whitespace. The algorithm
  hashes **raw bytes**.
- Filenames sorted by something other than byte-wise ASCII, or a separator added
  between files.
- Line-ending translation on checkout (`.gitattributes` in `nexus-proto` pins
  `*.proto` to LF; confirm the checkout didn't convert to CRLF).

**Check first:** the commit hashes. It's almost always that.

---

## Step 2 — One real receiver, two stubs

**Proves:** the handshake and the IPC layer — framing, `RxEnvelope` decode,
message routing — work with a real C++ peer, without yet trusting its decode
pipeline.

**Run:**

```bash
cd session-manager
NEXUS_CONFIG=... python -m session_manager.main &      # or a milestone-style temp config
# receiver 0: the real binary
nexus-receiver --receiver-id 0 --socket <sock> ...
# receivers 1 and 2: stubs
python tests/integration/stub_receiver.py --receiver-id 1 --socket <sock> \
    --session-id s1 --blocks 12
python tests/integration/stub_receiver.py --receiver-id 2 --socket <sock> \
    --session-id s1 --blocks 12
```

Feed all three the same synthetic session (`s1`, 12 blocks). The stubs write
deterministic content derived from `(session_id, block_id)` — the real receiver,
given the same synthetic `Manifest`, must produce byte-identical block content
for the block ids it owns (`block_id % 3 == 0`).

**Pass:** manager log shows `peer_connected` for all three `receiver_id`s,
`session_opened session_id=s1`, and `BlockDecoded` folding in from all three
without `block_decoded_for_unknown_session`. The session reaches `VERIFIED` /
`session_published`.

**Failure means:**
- No `peer_connected` for receiver 0, `peer_proto_hash_mismatch` in the log →
  step 1 regressed or the receiver hard-codes a hash.
- `peer_handshake_failed` → the receiver's first message wasn't `ReceiverHello`,
  or it framed with a length prefix, or it used `SOCK_STREAM`.
- `peer_connected` then immediate `peer_disconnected` → the receiver sent one
  message and closed, or crashed. Check the receiver's own log.
- `block_decoded_for_unknown_session` from receiver 0 → it sent `BlockDecoded`
  before `ManifestSeen`, or with a `session_id` that doesn't match.
- Session never completes, `session_stalled` fires → receiver 0's block ids
  don't cover its shard, or it's reporting ids ≥ `total_blocks` (silently
  dropped).
- Session completes but `HASH_MISMATCH` → go to step 3, that's what it's for.

**Check first:** the receiver's own stdout/stderr. A handshake failure is
usually visible there before it is in the manager log.

---

## Step 3 — Block accounting matches the stub

**Proves:** the real receiver's decode + write is byte-correct, isolated from
multi-receiver coordination.

**Run:** the step-2 setup, but afterwards compare, for the block ids receiver 0
owned:

```bash
# what the manager published
b3sum <output_dir>/<file>
# what a stub would have produced for the whole synthetic file
python -c "import sys; sys.path.insert(0,'tests/integration'); import stub_receiver as s; \
          print(s._file_hash('s1', 12).hex())"
```

Also diff the bytes receiver 0 wrote against `stub_receiver._block_content('s1',
block_id)` for each id it owned.

**Pass:** published file hash equals the stub's `_file_hash`, and receiver 0's
per-block bytes match `_block_content` exactly.

**Failure means:**
- Whole file wrong, all blocks → receiver is writing at the wrong offsets.
  Almost always **Property 3**: it treated `block_bytes` as the block size
  instead of the symbol size. Offset should be `block_id * k * block_bytes`.
- Only receiver 0's blocks wrong → its FEC decode is producing wrong bytes, or
  it's writing symbols in the wrong order within a block.
- File hash wrong but individual blocks look right → a boundary bug on the last
  (short) block, or an off-by-one in `total_blocks`.
- `dest_path` empty / file at the wrong location → **Property 1**: it
  reconstructed a path from `Manifest.filepath`.

**Check first:** one block's raw bytes at its offset in `dest_path`, against
`_block_content`. That tells you offset-bug vs decode-bug immediately.

---

## Step 4 — Three real receivers

**Proves:** multi-receiver aggregation — disjoint shards, union covers every
block, no double-counting.

**Run:** three real `nexus-receiver` processes, `--receiver-id 0/1/2`, same
session, shard split `block_id % 3 == receiver_id`.

**Pass:** session reaches `VERIFIED`; manager log shows `blocks_decoded`
climbing to `total_blocks` with contributions from all three; no
`shm_bitmap_diverges_from_uds` if the cross-check is on (it's off by default).

**Failure means:**
- Session stalls short of `total_blocks` → a shard gap. Two receivers using the
  same residue, or one computing its shard from its process id instead of the
  `receiver_id` the manager assigned. This is the same class of bug as
  `Manifest.sender_id` on the TX side.
- `blocks_decoded` briefly exceeds `total_blocks` in a snapshot → not possible
  via the decoded set (it's a set); if you see it, the receiver is sending ids ≥
  `total_blocks` and something downstream isn't clamping. Report it.
- One receiver's `BlockDecoded` never arrives → check its heartbeat; if the
  registry expired it (15 s silent) its reports still count but it shows `dead`
  in the status display, which is a symptom worth chasing.

**Check first:** which block ids are missing at the stall (`session_stalled`
logs `missing_blocks_preview`). Their residue class tells you which receiver.

---

## Step 5 — Kill the manager process mid-transfer

**Proves:** adopt-vs-create, journal + sidecar recovery, and a verified publish
after a restart — the distinctive claim of the shm design.

**Kill the process, not the container.** `docker kill` / `docker compose
restart` recreates the container, which gives it a fresh `/dev/shm` — that is a
cold start and proves nothing. Send `SIGKILL` to the `python -m
session_manager.main` PID while the three receivers keep running.

**Run:**

```bash
# ... three receivers mid-transfer, session partway to total_blocks ...
kill -9 <manager_pid>
# receivers detect the dropped connection, keep heartbeating, reconnect
python -m session_manager.main &        # SAME config, same socket, same shm name
```

**Pass:** the restarted manager's log shows `session_recovered
decoded_blocks=<N>` (N = what the journal had) and `session_manager_listening
adopted=True`, **not** `adopted=False`. No `session_spec_missing_on_recovery`.
The session then completes and `session_published`, and the output file hash is
correct.

**Failure means:**
- `adopted=False` on the restart → `probe_receiver_alive()` was false when
  `create_or_adopt` ran. Either the receivers didn't reconnect within the adopt
  grace period (`ADOPT_GRACE_PERIOD_SECONDS`, 1.5 s), or they stopped
  heartbeating on disconnect instead of reconnecting. The receiver must treat a
  dropped connection as "reconnect and resume", not "session over".
- `adopted=True` but `session_spec_missing_on_recovery` → the spec sidecar
  (`<journal_dir>/<session_id>.spec.json`) was lost. Check the journal dir is on
  a persistent volume, not a container-local path.
- `adopted=True`, recovery logged, but session never completes → the reconnected
  receivers aren't re-reporting their outstanding blocks. They should re-run
  their report loop against the new connection (re-reporting already-decoded
  blocks is idempotent and expected).
- Restart exits `lock_held_by_another_process` → the killed manager's `flock`
  wasn't released. `SIGKILL` releases it (kernel drops the fd); if you see this,
  the old process didn't actually die.

**Check first:** `adopted=True` vs `False` in the restart log. Everything else
follows from that.

---

## Step 6 — Full path

**Proves:** the whole pipeline end to end.

**Run:** `file-monitor` + N senders → `router` (impairment) → N receivers +
`session-manager`. Drop one small file (a few blocks) into file-monitor's watch
directory.

**Pass:** the file lands in `session-manager`'s output directory and its
BLAKE3-256 hash equals the source file's.

**Failure means:** by this point every component has been tested in isolation,
so a failure here is a **contract seam**:
- File never starts → file-monitor didn't dispatch, or senders aren't
  transmitting. Not session-manager's side.
- Transfer stalls → the `router` is dropping more than the FEC can recover
  (`n − k` symbols per block), or misrouting. Turn impairment off and retry.
- Completes but `HASH_MISMATCH` → an FEC parameter mismatch. `k` / `n` /
  `symbol_bytes` must be identical across file-monitor's config and both C++
  binaries' build. This is the one that produces a "successful" transfer of
  garbage.
- Completes, hash matches, but slowly → tuning, not correctness. Out of scope
  for integration sign-off.

**Check first:** whether the same file transfers with `router` impairment set to
zero. That splits "FEC/transport" from "session-manager".

---

## After step 6 — containers

Only once native passes end to end:

```bash
cd session-manager
docker compose up -d
docker compose ps          # (healthy)
docker compose logs        # session_manager_listening, then `status` every 5 s
```

The container is not on the graded path, but a container-only dependency
creeping in is the defect that surfaces on the last day. Re-run
`scripts/run_milestones.sh` native after any container change.
