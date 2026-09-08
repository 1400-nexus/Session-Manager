# Nexus — Person C: the complete guide

Everything in the two Python services: what each piece does, why it is shaped
that way, how it connects, how to run it, how it is proven, and every bug worth
remembering.

One document. Read it front to back once; after that use the table of contents.

---

## Contents

1. [Scope and ownership](#1-scope-and-ownership)
2. [The one idea everything follows from](#2-the-one-idea-everything-follows-from)
3. [Topology](#3-topology)
4. [Architecture: ports and adapters](#4-architecture-ports-and-adapters)
5. [Repos, submodules, pins](#5-repos-submodules-pins)
6. [`file_monitor` — TX planner and dispatcher](#6-file_monitor--tx-planner-and-dispatcher)
7. [`session_manager` — RX reassembly authority](#7-session_manager--rx-reassembly-authority)
8. [The shared machinery](#8-the-shared-machinery)
9. [Cross-language contracts](#9-cross-language-contracts)
10. [Configuration reference](#10-configuration-reference)
11. [Running it](#11-running-it)
12. [How it is proven](#12-how-it-is-proven)
13. [Bug log](#13-bug-log)
14. [What is still open](#14-what-is-still-open)
15. [Defence questions](#15-defence-questions)
16. [Reading order in the code](#16-reading-order-in-the-code)

---

## 1. Scope and ownership

Person C owns the **control plane**: two Python services, one on each machine,
plus the supporting infrastructure.

| Deliverable | Graded? |
|---|---|
| `file-monitor` — TX planner, dispatcher, supervisor | **yes**, 1 of 8 processes |
| `session-manager` — RX authority, status display, supervisor | **yes**, 1 of 8 processes |
| `nexus-proto` custodianship | contract |
| Container images, compose, milestone harnesses | no |
| `SENDER_CONTRACT.md`, `RECEIVER_CONTRACT.md`, `INTEGRATION.md` | handoff |

A owns the C++ sender. B owns the C++ receiver. Neither is Person C's code.

---

## 2. The one idea everything follows from

The link is **strictly one-way**. No ACK, no NACK, no retransmit request.

Two consequences, and every decision below traces back to them:

**Reliable delivery is mathematically impossible.** You cannot guarantee a file
arrives. You can only drive the failure probability down by spending bandwidth
on redundancy. What the system guarantees instead is that it always *knows and
reports* whether it succeeded, and names exactly what failed.

**Therefore: convert every failure mode into one uniform kind.** Packet loss, bit
flips, misrouting, a crashed sender — all become **erasures**, and erasures are
solved by coding.

| Failure | Converted into | Solved by |
|---|---|---|
| Packet loss | erasure | Reed–Solomon, any K of N |
| Bit flip | CRC32C rejects it → erasure | Reed–Solomon |
| Burst loss | spread across many blocks | interleaving, then RS |
| Misrouting | *nothing* — receivers own nothing | stateless receiver design |
| Sender crash | partial erasure | cross-shard repair symbols |

Neither Python service touches payload bytes. They plan, dispatch, supervise,
aggregate and report.

> **The sentence to be able to say out loud:** the 1 GB payload never crosses the
> Python boundary, on either machine.

---

## 3. Topology

| Machine | Process | Language | Owner |
|---|---|---|---|
| TX | `file_monitor` | Python | **C** |
| TX | `sender` ×3 | C++ | A |
| RX | `receiver` ×3 | C++ | B |
| RX | `session_manager` | Python | **C** |

Eight processes. The brief requires at least one Python and one C++ process per
machine; this satisfies it honestly rather than decoratively.

```
   TX machine                        router / one-way link         RX machine
┌────────────────────┐                                     ┌──────────────────────┐
│ watch dir          │                                     │  /dev/shm/nexus-rx   │
│      │ inotify     │                                     │   arena + bitmap     │
│      ▼             │                                     │      ▲               │
│ file_monitor  (py) │                                     │      │ writes        │
│      │ UDS         │                                     │ receiver ×3   (c++)  │
│      ▼             │                                     │      │ UDS           │
│ sender ×3    (c++) │ ═══ UDP · RS-coded · interleaved ══▶ │      ▼               │
└────────────────────┘                                     │ session_manager (py) │
                                                           │      │ verified only │
                                                           │      ▼               │
                                                           │  output dir          │
                                                           └──────────────────────┘
```

---

## 4. Architecture: ports and adapters

Both services use the same four-layer shape. The dependency arrow only ever
points inward.

```
main.py           ← the ONLY file that knows both adapters and services
   │
services/         ← orchestration; depends on ports only
   │
ports/            ← Protocol classes; depends on domain only
   │
domain/           ← pure. imports nothing internal, no third party
```

`adapters/` sits beside `services/` and implements the ports. Neither imports
the other.

**How the boundary is actually enforced:** `mypy --strict` plus review. There is
no `import-linter` and no `Makefile` in either repo — an earlier plan called for
one and it was never added. The layering holds, but it holds by discipline, not
by tooling. If it ever starts to slip, adding an `import-linter` contract is the
cheap fix.

The checks that do exist are run directly:

    ruff check . && ruff format --check .
    mypy --strict src tests
    pytest

### Why this shape, and not something simpler

Not fashion. One specific fact: **you cannot test the interesting failures
against real infrastructure.**

You cannot make a shm segment exist-but-stale on demand. You cannot make a
receiver die at the exact moment the manager is mid-verify. You cannot produce a
1 GB file two hundred times in a fortnight.

Each of those becomes a three-line test against a fake — but only if the
boundary is a `Protocol` a fake can implement. That is the entire justification,
and it paid for itself repeatedly.

### The rule that keeps `domain/` useful

`domain/` is pure. **Time arrives as two floats, not as a `Clock`.** Progress
arrives as a `set[BlockId]`, not as a shm handle.

That is why `progress.is_stalled` is tested in microseconds instead of by
waiting eight seconds, and why the whole unit suite runs in about a second.

### Contract tests: why the fakes are trustworthy

A fake that always succeeds makes tests pass that would fail in production. So
four boundaries have **one suite run against both the fake and the real
adapter**:

| Suite | Fake | Real |
|---|---|---|
| `test_shm_contract.py` | `FakeShm` | `PosixShm` |
| `test_file_store_contract.py` | `FakeFileStore` | `LocalFileStore` |
| `test_journal_contract.py` | `FakeJournal` | `AppendJournal` |
| `test_session_spec_store_contract.py` | fake | `JsonSessionSpecStore` |

If a fake drifts from reality, the suite fails against one of them. That is what
stops a fake becoming a comfortable fiction.

Every fake also has **failure injection** — raise on the Nth call, raise once
then succeed, hang until released. A fake that cannot fail is worse than no
fake.

---

## 5. Repos, submodules, pins

```
nexus-proto              ← the contract, all four .proto files + generated code
  ├── file-monitor       ← libs/nexus-proto submodule
  └── session-manager    ← libs/nexus-proto submodule
sender                   ← A's, C++
receiver                 ← B's, C++
```

**Current pin: `7f406db`. Contract digest: `38cac339d495241ae757fbeec84a6ecdc5377f838798ff9df1e19650bcff20df`.**

Both Python services are on that pin and compute that digest. Any process that
opens a UDS connection must build against the same commit, or the handshake is
refused.

### Shared Python code is copied, not extracted

Two services, one owner, two weeks — submodule ceremony for shared code would
cost more than the duplication. `SHARED_CODE.md` in `session-manager` is the
ledger, and it names **five** behaviours where a bug fixed in one must be fixed
in both: peer identity on reconnect, write-failure teardown, the interruptible
backoff sleep, bind-before-adopt, and the `resource_tracker` double-unregister.

### The generated protobuf is flat

`ipc_pb2.py` does `import common_pb2`, so the generated modules must sit on
`sys.path` as **top-level modules**, not inside a package.

- dev: `PYTHONPATH=libs/nexus-proto/generated/python`
- wheel: force-included at the wheel root in `pyproject.toml`
- image: installed with the wheel

It is `import rx_pb2`, never `from session_manager.pb import rx_pb2`.

---

## 6. `file_monitor` — TX planner and dispatcher

**Job:** notice a file, plan how to shard it, hand assignments to three senders,
supervise them. It reads file bytes only to hash them; it never puts them in a
message and never touches the network.

### Modules

| Module | Responsibility |
|---|---|
| `domain/ids.py` | `NewType` wrappers so a block id cannot be passed where a symbol id belongs |
| `domain/models.py` | `SourceFile`, `FecParams` (`block_size = k × symbol_bytes`), `BlockPlan`, `ShardAssignment` — all frozen |
| `domain/planning.py` | **pure**: `calculate_block_count`, `compute_block_plans`, `blocks_for_shard`, `derive_shard_assignments`. No K selection — `k` comes straight from config into `FecParams`, unchanged |
| `adapters/inotify_events.py` | `IN_CLOSE_WRITE` + `IN_MOVED_TO`, with an `IN_Q_OVERFLOW` rescan |
| `adapters/blake3_hasher.py` | chunked hash, offloaded to a thread |
| `services/watcher.py` | 200 ms debounce per path; emits stable paths, acts on nothing |
| `services/planner.py` | builds the `Manifest` and `AssignSession` protobufs |
| `services/dispatcher.py` | the pipeline: hash → plan → build → send |
| `services/registry.py` | which senders are connected and alive |
| `ipc/uds.py` | `SOCK_SEQPACKET` server |
| `supervision/supervisor.py` | spawn, health, restart with backoff |
| `main.py` | composition root |

### Flow, end to end

1. A file lands in the watched directory. `inotify_events` yields it on
   `IN_CLOSE_WRITE` — **not** `IN_CREATE`, which fires before the file is
   written.
2. `watcher` debounces 200 ms. Five rapid events on one path produce one
   emission.
3. `dispatcher` stats and hashes it (BLAKE3, on a thread), builds a `SourceFile`.
4. `registry.active_senders()` gives the live set. **Empty → log and stop.** Do
   not dispatch a session nobody will transmit.
5. `planning.derive_shard_assignments` splits the blocks over the live senders.
6. Per sender: `planner.build_manifest` + `build_assign_session`, then
   `ipc.send`.
7. Each sender receives exactly one assignment, mmaps `source_path`, transmits.

### The most consequential detail in this service

`Manifest.sender_id` is a **shard residue, not a process identity.**

A sender transmits blocks where `block_id % total_senders == sender_id`, and
`file_monitor` assigns that residue **positionally over the currently live
senders**.

So with senders 0 and 2 alive, it sends residues **0 and 1** — never 0 and 2.

If a sender uses its own process id instead, then the moment any other sender
dies the survivor computes `block_id % 2 == 2`, which is never true. It
transmits **nothing**. Half the file never leaves the machine, with no error on
either side.

Worked example, `total_blocks = 12`:

| Senders alive | Residue | Blocks transmitted |
|---|---|---|
| 0, 1, 2 | 0 | 0, 3, 6, 9 |
| | 1 | 1, 4, 7, 10 |
| | 2 | 2, 5, 8, 11 |
| 0, 2 (1 died) | 0 → sender 0 | 0, 2, 4, 6, 8, 10 |
| | 1 → **sender 2** | 1, 3, 5, 7, 9, 11 |

Sender 2 receives residue **1**, not 2. That is the whole point.

This was a latent bug in an early `planning.py`: it sharded by list index while
a sender would have sharded by id, and those agree only while the live set is
exactly `[0, 1, 2]`. The fix made residue and modulus explicit fields on
`ShardAssignment` so the two sides cannot disagree.

### Failure policy: one assignment per sender, ever

If a send fails, that sender's shard is **dropped and logged** — not
redistributed. The log names residue, modulus and block count, which is enough
to reconstruct exactly which blocks were lost.

Why not retry with a new split? A second `AssignSession` for the same session
with a different modulus would leave a sender holding two conflicting
assignments, and the wire contract has no supersede rule. **Losing a third of a
file loudly beats transmitting the wrong blocks silently.**

---

## 7. `session_manager` — RX reassembly authority

**Job:** centralise reports from three receivers, unify them, verify integrity,
publish only what verified, display status. Its entire authoritative state for a
1 GB transfer is a completion set of ~3,835 integers.

### Modules

| Module | Responsibility |
|---|---|
| `domain/models.py` | `SessionSpec` (the Manifest in our own types), `SessionState`, `ReceiverCounters`, `SessionSnapshot` |
| `domain/progress.py` | **pure**: completion, stall, missing blocks, loss % |
| `domain/paths.py` | `is_unsafe_relpath`, `is_unsafe_filename_component` |
| `domain/blocks.py` | byte range for a block id |
| `adapters/shm_layout.py` | the segment header as a **cross-language** struct |
| `adapters/posix_shm.py` | create-or-adopt, bitmap views |
| `adapters/flock_file_lock.py` | one manager at a time |
| `adapters/local_file_store.py` | `fallocate`, atomic publish, quarantine |
| `adapters/append_journal.py` | fixed-width binary log for crash recovery |
| `adapters/json_session_spec_store.py` | per-session sidecar so a spec survives a crash |
| `services/authority.py` | **sole writer** of session state |
| `services/aggregator.py` | folds `BlockDecoded` into completion |
| `services/verifier.py` | BLAKE3 vs the manifest |
| `services/publisher.py` | verified → output, mismatch → quarantine |
| `services/status_display.py` | the graded status view |
| `services/receiver_registry.py` | receiver liveness |

### Flow

1. A receiver connects, sends `ReceiverHello` with a computed `proto_hash`.
   Mismatch → refused.
2. Manager replies `Config` (shm name, staging dir, journal dir), then replays
   `SessionOpen` for every already-open session — so a **late joiner or a
   reconnecting receiver** learns what exists.
3. First `ManifestSeen` for an unknown session: validate → `SessionSpec` →
   `fallocate` staging → `init_session` in shm → **save the spec sidecar** →
   broadcast `SessionOpen`. A *duplicate* `ManifestSeen` gets `SessionOpen` sent
   back to that receiver rather than dropped.
4. Receivers write bytes at their offsets, **`fdatasync`**, then report
   `BlockDecoded`.
5. Manager **journals each block, then** folds it into the completion set.
6. Complete → `verifier` hashes → pass: atomic rename into output; fail:
   quarantine, log both digests, **never delete**.
7. Terminal state → `PurgeSession` so receivers release slots. `SessionAuthority`
   is the sole emitter, via one function: `domain/purge_policy.terminal_reason()`
   maps `(progress, hash verdict, stall timeout)` → `PUBLISHED` / `QUARANTINED`
   / `INCOMPLETE` / still-live (no defaulted timeout — config is the only
   source). The completion path calls it after verifying; a 5-second **sweep**
   in the authority calls it for every open session, which is the only thing
   that catches the session that just went *silent* — `INCOMPLETE` is the
   absence of `BlockDecoded`, so nothing event-driven ever fires it. A session
   is announced exactly once. On `INCOMPLETE`, purge first moves the partial
   into `quarantine/` (`Publisher.quarantine_incomplete`), named
   `<stem>.<session_id><ext>` so two stalled transfers of one filename in a
   run don't clobber each other, with a `<that name>.incomplete.json` beside
   it — `{session_id, total_blocks, decoded_blocks, missing_block_ids}`, the
   full missing list read from the journal — because a partially-received
   file is evidence of what the link delivered, same as a hash mismatch, not
   scratch to reap. That report is written (atomically) *before* purge
   **tears down the durable footprint** — shm session-table entry, spec
   sidecar, journal file — so an adopting restart cannot re-recover it and
   re-broadcast `SessionOpen` for something the receivers were told is
   finished, and the journal is only unlinked once the report derived from
   it is safe. If that ordering is ever broken by a crash — sidecar left but
   partial gone — `_recover` refuses to adopt the session (logs
   `session_staged_file_missing_on_recovery` with the remedy), rather than
   rebuilding a session whose bytes no longer exist. A late `BlockDecoded`
   after purge is dropped at debug; a resent `ManifestSeen` is ignored. On
   adopt, a recovered session's stall clock starts at the adoption instant
   (the aggregator stamps `last_progress_at` when `_recover` registers it),
   not its original open time — otherwise a restart purges every live
   transfer on the first sweep.
   The status display's **Idle** column shows time-since-last-block, styled
   yellow past half the stall timeout and red past 90%.

### Adopt-vs-create: the sharpest edge in the system

If the manager restarts while receivers are alive, it must **adopt** the
segment. Receivers hold live slot indices; zeroing that memory corrupts three
running processes mid-transfer, and the symptom is decoded blocks turning to
garbage with no error anywhere.

The decision table, identical in `FakeShm` and `PosixShm`:

| Segment | Receiver answers? | Decision |
|---|---|---|
| absent | — | **CREATE** |
| valid header | yes | **ADOPT** — do not zero |
| valid header | no | **REINITIALISE** |
| wrong magic/version | either | **REINITIALISE** |

Wrong magic beats a live receiver: a receiver mapped to an incompatible layout
is worse than no receiver, because it is writing into fields that mean something
different.

Three things make adopt safe:

- **`flock` first**, before anything touches shm. Two managers on one segment is
  the worst available bug, so it is made structurally impossible.
- **Journal replay** rebuilds the completion set in milliseconds instead of
  re-hashing a gigabyte. Safe because destination writes are idempotent — fixed
  offset, fixed content — so replaying an applied record is harmless. *That is
  why `append_journal.py` is 147 lines and not a write-ahead protocol.*
- **The spec sidecar.** A recovered session needs its `SessionSpec` to verify and
  publish. Without it, adopt recovers the blocks and produces a session that
  never completes — worse than failing loudly, because it looks fine.

**The lost-sidecar subtlety.** If a sidecar is lost, the session stays *known but
unregistered* — deliberately **not** treated as unknown. Treating it as unknown
looks like graceful degradation, but a resent `ManifestSeen` would recreate it
and `init_session` would zero a bitmap a live receiver is writing into. Losing
verify-and-publish for one session beats corrupting an in-flight one.

**Container subtlety.** Restarting the whole container always wipes `/dev/shm`,
so that is always a cold start and proves nothing. Only killing the *process*
inside a living container exercises adopt.

### Why the journal write comes before the fold

Append first, then add to the in-memory set. Fold first and crash in between,
and the block is reported complete but absent from the journal — on restart the
manager thinks it is missing while the bytes are on disk, and stalls a session
that was fine.

### The durability requirement on receivers

A receiver must make a block's bytes durable **before** reporting
`BlockDecoded`.

Found by a milestone run: the stub used a buffered handle, so after `kill -9`
the manager recovered 200 journaled blocks whose bytes were still in userspace,
verified a file with a hole, and reported `hash_mismatch`.

**The tell:** `decoded_blocks=200` equalled `JOURNAL_SYNC_BATCH_SIZE` exactly.
Without spotting that, the obvious move is to distrust the journal and re-verify
on adopt — which defeats the journal's entire purpose and leaves the real bug in
the receiver.

Now a documented contract clause, because a C++ receiver has the identical
window and no way to infer the requirement.

### Two shm ports, not one

`ShmReader` and `ShmWriter` are separate `Protocol`s. The aggregator reads a
bitmap; the authority writes session state. One combined interface would let the
wrong component hold a **writable** view into receiver memory — the split makes
the mistake unwriteable rather than merely discouraged.

### UDS is authoritative; the bitmap is a cross-check

Completion is built from `BlockDecoded` over UDS. That is what satisfies the
brief's IPC requirement, and it is sufficient alone.

The shm bitmap comparison is a removable optimisation, and it is **off in the
shipped `config.toml`** because no receiver writes bits yet.

Note the asymmetry: `DEFAULT_SHM_CROSSCHECK` in `constants.py` is `True` — that
is the value used when the key is *absent*. So "off by default" is true of the
shipped config and false of the code constant. If you ever hand someone a config
without that key, the check turns on and starts warning. A milestone run proved it genuinely
removable — everything passed with it reporting zero. It also warns **once per
session**, not once per poll: a permanently-firing warning trains everyone to
ignore the log.

### The status display is graded

The brief gives this process three duties: centralise the reports, unify them,
**and display the status**. The third is as much a deliverable as the others.

Split into a **pure renderer** (`SessionSnapshot → renderable`) and a thin
driver. That is why `SessionSnapshot` exists — the renderer never reaches into
live state, so it is unit-testable with no terminal.

`crc_fail`, `kernel_drops` and `arena_exhausted` render bold-red when non-zero,
because those three distinguish three completely different problems: the router
corrupting packets, the host being too slow to read them, and the decode path
falling behind the wire. The fixes have nothing in common, so the display is
what tells you which one you have.

**TTY branching:** with a terminal, `rich.Live` tables. Without one, a single
structured `status` log event every 5 s carrying the same fields. A process
cannot distinguish "someone attached" from "someone is tailing logs" — it is the
same stream — so allocating a PTY does not give you both behaviours, it just
picks the wrong one for the common case.

---

## 8. The shared machinery

Copied between both services. `SHARED_CODE.md` is the ledger.

### `ipc/uds.py` — Unix domain sockets

**`SOCK_SEQPACKET`, not `SOCK_STREAM`.** The kernel preserves message
boundaries, so there is no length-prefix framing anywhere in the system. Less
code, one fewer class of bug. If you are writing framing logic, you reached for
the wrong socket type.

Three behaviours that each took real work:

- **Peer identity on reconnect.** A reconnecting peer overwrites the old entry.
  The old connection's cleanup checks `self._peers.get(peer_id) is my_queue`
  before removing — otherwise it deletes the *new* connection's queue on its way
  out.
- **Write-failure teardown.** A failed write deregisters the peer **and**
  `shutdown(SHUT_RD)` so the blocked `sock_recv` returns and the peer task
  exits. Otherwise you hold a peer you cannot send to that still looks alive.
- **`send` raises rather than logging.** A dropped `AssignSession` means a
  sender never gets its shard. The dispatcher must be able to react, and it
  cannot react to a log line.

Also: bounded queues (an unbounded queue between a fast producer and a stalled
consumer is a memory leak with extra steps), stale socket unlink on startup, and
a truncation warning when a datagram exactly fills `RECV_BUFFER_BYTES` — on
SEQPACKET the remainder is silently discarded.

### `ipc/handshake.py` — the contract hash

Every process reports a `proto_hash` in its hello; a mismatch is refused. This
catches "someone regenerated the contract and did not rebuild" in one second
instead of two hours.

The algorithm is deliberately trivial to reimplement: `.proto` files **sorted by
filename**, **raw bytes**, **no separator**, **BLAKE3-256**, **32 bytes**.

An earlier version canonicalised the text — stripping comments, collapsing
whitespace. Replaced, because replicating that exactly in C++ is unreasonably
fragile: one difference means every connection is refused with a symptom
pointing at a hash rather than at the cause.

**Accepted trade-off:** a comment-only `.proto` edit now changes the hash and
forces everyone to rebuild. A reproducible check that occasionally over-fires
beats an irreproducible one that blocks all integration.

**The CRLF trap.** The hash is over raw bytes, so a Windows checkout with CRLF
`.proto` files computes a *different* hash at the *same commit*. `git submodule
status` shows the right pin, the files look identical, and the handshake refuses
anyway. Hence `.gitattributes` with `eol=lf` in both repos.

### `supervision/supervisor.py`

**It does not know what it supervises.** Opaque `ChildSpec`s; the two services
differ only in the specs they pass. The moment "sender" appears in it, you are
maintaining two supervisors in one class — and this is exactly the code that had
to be reused.

- **Backoff is per child**, indexed by that child's consecutive failures.
- **Crash-loop detection uses a sliding window.** A cumulative counter would trip
  on a process that died five times over two weeks.
- **Backoff sleep is interruptible.** An `asyncio.Event` set by `shutdown()` races
  the sleep. Without it, a child in a 30 s backoff delays shutdown past the
  container grace period and gets `SIGKILL`ed — silently undoing the graceful
  shutdown.
- **`init: true` in compose.** These processes spawn children; PID 1 does no
  `SIGCHLD` reaping, so zombies accumulate and process counts quietly lie.

### Shutdown, and why not `task.cancel()`

An early version had the signal handler cancel the currently-running task.
`except* CancelledError` caught it, but the task stayed in a cancelling state,
so `await supervisor.shutdown()` in the `finally` raised at its first await
point. Children were never terminated and the socket never unlinked — the exact
opposite of the handler's purpose.

The fix: an `asyncio.Event` set by the handler, a watcher task that cancels the
workers, and cleanup performed **after** the `TaskGroup` exits, in a
non-cancelled context.

### `config.py`

Frozen dataclasses **split per consumer** — `PathsConfig`, `ShmConfig`,
`AggregationConfig` and so on — not one god object. Passing a single `Config`
into every constructor is the most common way dependency inversion quietly dies.

Note two details: `file-monitor` reuses the domain model `FecParams` directly as
`AppConfig.fec` rather than defining a separate config type, and
`session-manager` has **no** FEC config at all — those values arrive
per-session in the Manifest.

Every value overridable by a `NEXUS_*` environment variable. The prefix matters:
`SOCKET_PATH` colliding between two services in one compose stack is a nasty
afternoon.

Relative paths resolve against **the config file's directory**, not the process
CWD, so behaviour does not depend on where it was launched from.

Validation refuses to start and **names the offending key**. Two worth knowing:

- `symbol_bytes <= 1442` — 1500 MTU minus 20 IP, 8 UDP, 12 frame prefix, and
  protobuf tags. Fragmentation on a lossy link is disastrous: one lost fragment
  kills the whole datagram.
- **staging and output on the same filesystem** (`st_dev` compared).
  Publication is `os.replace`, atomic within one filesystem and a silent copy
  across two.

---

## 9. Cross-language contracts

Three things another language must agree with byte for byte, each documented in
prose meant to be implementable *from the prose*.

| Contract | Where | Consumed by |
|---|---|---|
| `proto_hash` algorithm | `ipc/handshake.py` docstring | A's sender, B's receiver |
| shm header + session table | `adapters/shm_layout.py`, `adapters/constants.py` | B's receiver |
| Wire and IPC messages | `nexus-proto` | everyone |

The layout, verified with `struct.calcsize`:

| Item | Value |
|---|---|
| `SHM_HEADER_FORMAT` | `"<4sI16sIIIQ"` → **44 bytes** |
| magic / version | `NXRX` / 1 |
| `SHM_SESSION_TABLE_OFFSET` | 64 |
| `SHM_SESSION_TABLE_BYTES` | 4096 reserved |
| `SHM_SESSION_ENTRY_FORMAT` | `"<40sQQQ"` → **64 bytes** |
| `SHM_SESSION_ID_BYTES` | 40 |

**Explicit little-endian by choice** — native ordering that happens to match
today is not a contract.

### The properties teammates cannot infer

In `SENDER_CONTRACT.md` and `RECEIVER_CONTRACT.md` precisely because nothing in
the schema implies them:

- `Manifest.sender_id` is a shard **residue**, not a process id.
- `Manifest.block_bytes` carries the **symbol** size; a block is `k × block_bytes`.
- `AssignSession.source_path` is **absolute**; do not reconstruct from
  `Manifest.filepath`.
- `SessionOpen.dest_path` is **absolute** and authoritative.
- `SessionOpen` is **idempotent** and may be re-sent at any time.
- `BlockDecoded` requires bytes to be **durable first**.
- `sender_bps_limit == 0` means **unlimited**, because proto3 cannot distinguish
  unset from zero.

---

## 10. Configuration reference

### `file-monitor/config.toml`

| Key | Default | Note |
|---|---|---|
| `paths.watch_path` | `./watch` | the inotify target; the only detection path |
| `paths.socket_path` | `./run/file-monitor.sock` | must be on a real filesystem, not a Windows bind mount |
| `pacing.rate_ceiling_bps` | 20,000,000 | hot-reloadable |
| `pacing.rate_floor_bps` | 5,000,000 | |
| `fec.k` / `fec.n` / `fec.symbol_bytes` | 200 / 255 / 1400 | **cross-service contract**; a mismatch does not fail loudly, it writes garbage |
| `senders.target_host` / `base_port` | `127.0.0.1` / 9000 | |
| `senders.sender_count` | 0 | 0 = supervise nothing; the C++ binary is in another repo |

### `session-manager/config.toml`

| Key | Default | Note |
|---|---|---|
| `paths.staging_dir` / `output_dir` | `./staging` / `./output` | **same filesystem**, validated |
| `paths.journal_dir` | `./journal` | also holds the spec sidecars |
| `paths.lock_path` | `./run/session-manager.lock` | `flock` target |
| `shm.name` | `nexus-rx` | receivers learn it from `Config` |
| `shm.arena_bytes` | 268,435,456 | 256 MiB → compose needs `shm_size: 512m` |
| `shm.slot_bytes` | 4,194,304 | 4 MiB → 64 slots; `MIN_ARENA_SLOTS = 4` |
| `aggregation.poll_interval_s` | 1.0 | how often `BlockDecoded` folds are turned into snapshots / COMPLETE |
| `aggregation.shm_crosscheck` | **false** | off until a receiver writes the bitmap. **`DEFAULT_SHM_CROSSCHECK` in code is `True`** — the value if the key is absent |
| `purge.sweep_interval_seconds` | 5.0 | `SessionAuthority` sweeps open sessions this often for a terminal state |
| `purge.stall_timeout_seconds` | 60.0 | no `BlockDecoded` for this long → `INCOMPLETE` + `PurgeSession`; also drives the status display's Idle column styling. **Placeholder** pending A's sender pacing |
| `receivers.count` | 0 | 0 = supervise nothing |
| `status.refresh_interval_s` | 0.5 | Live tables only; the no-TTY path logs every 5 s |
| `status.force_terminal` | false | **false = auto-detect**, not "off" — see the bug log |

**Deliberately absent on the RX side:** `fec.k`, `fec.n`, `fec.symbol_bytes`.
They arrive per-session in the Manifest. Pinning them would give a receiver that
decodes correctly until someone retunes K, then writes garbage that looks like
packet loss.

---

## 11. Running it

### Native

```bash
git clone --recurse-submodules <repo> && cd <repo>
pip install -e ".[dev]"
export PYTHONPATH=libs/nexus-proto/generated/python
python -m file_monitor.main        # or session_manager.main
```

`NEXUS_CONFIG` selects the config file; a bad config exits **78** with one line,
not a traceback.

### Container

```bash
docker compose up -d
docker compose ps            # must reach (healthy)
docker compose logs -f
docker compose stop          # must return well inside the grace period
```

Settings that are not optional:

| Setting | Why |
|---|---|
| `init: true` | children are spawned; PID 1 does no `SIGCHLD` reaping |
| `stop_grace_period: 30s` | long enough for `supervisor.shutdown()` |
| `shm_size: 512m` (RX) | container default is 64 MB; the arena is 256 MB |
| one volume for staging+output | publication is `os.replace`; across volumes it silently becomes a copy |
| named volumes, not Windows bind mounts | UDS cannot be created there; inotify does not propagate |

**The native path must keep working.** The graded machines run native, and a
container-only dependency creeping in is exactly the defect that surfaces on the
last day.

### Milestones

```bash
bash scripts/run_milestones.sh
```

---

## 12. How it is proven

Three layers, each proving something the others cannot.

| Layer | Scope | Speed | Proves |
|---|---|---|---|
| Unit | `domain/` only, no I/O | ms | planning arithmetic, bitmap logic, stall detection |
| Contract | one suite, fake + real | s | the fakes match reality |
| Milestone | real processes, sockets, `/dev/shm` | min | the thing works |

Current counts — **on Linux**, which is the graded target:

| Repo | Linux | Windows (POSIX tests skipped) | Milestones |
|---|---|---|---|
| `file-monitor` | 112 passed | 103 passed, 8 skipped | 3 |
| `session-manager` | 265 passed | 251 passed, 12 skipped | 5 |

The skips are `AF_UNIX`/`SOCK_SEQPACKET`, `fcntl.flock` and `posix_fallocate` —
POSIX-only by nature, not disabled. A bare number is misleading if you develop
on Windows: the local suite is green while a third of the transport layer never
ran.

### `file-monitor` milestones

| # | Name in `run_milestones.sh` | Proves |
|---|---|---|
| 1 | three senders, disjoint and complete | shards cover every block exactly once |
| 2 | a dead sender degrades cleanly | modulus recomputed over live senders |
| 3 | a wrong contract hash is refused | handshake rejection, others unaffected |

### `session-manager` milestones

| # | Name in `run_milestones.sh` | Proves |
|---|---|---|
| 1 | three stubs, all blocks → VERIFIED | full path to a published, hash-matching file |
| 2 | withheld blocks → stall timeout → INCOMPLETE | stall fires, nothing published, partial quarantined with a missing-blocks report |
| 3 | one corrupted block → HASH_MISMATCH, quarantined | evidence retained, output empty |
| 4 | **`kill -9` the manager mid-transfer → restart adopts, recovers, verify** | adopt + journal + sidecar recovery |
| 5 | wrong `proto_hash` → refused, other stubs unaffected | one bad peer cannot disrupt healthy ones |

**Milestone 4 is the one that matters.** It is the only test exercising
adopt-vs-create end to end with live peers, and it caught the durability bug no
unit test could.

### The stub peers

`stub_sender.py` and `stub_receiver.py` stand in for A's and B's C++ processes.
Each is restricted to importing only `ipc.codec`, `ipc.handshake` and
`ipc.constants` — **never** `services/` or `domain/`. If a stub shared the
planning code it would agree with the service by construction and prove nothing.

They compute `proto_hash` themselves rather than using a literal, and derive
their block sets with the same arithmetic the C++ will use.

---

## 13. Bug log

The bugs are the part worth remembering, because each one names a class of
mistake rather than a typo.

| Bug | Symptom | Root cause | Lesson |
|---|---|---|---|
| Shard by index, not residue | survivor transmits nothing after a sender dies | two sides computing the same value from different inputs | make the shared value an explicit field |
| `NewType`s declared but unused | type checker caught nothing | ids declared `str`, models used `int` | a type that is not used is not a check |
| `task.cancel()` shutdown | children never terminated, socket never unlinked | cleanup awaited inside a cancelling task | shut down with an Event, clean up outside the cancel |
| Non-interruptible backoff | shutdown exceeded the grace period → `SIGKILL` | sleep did not observe the stop flag | every long sleep needs a wakeup |
| Fresh sessions unregistered | `block_decoded_for_unknown_session` forever | only the recovery path registered with the aggregator | one path, or two paths diverge |
| Late joiner never told | receiver times out and reconnects forever | `SessionOpen` broadcast once, before it connected | idempotent state must be replayable |
| Relative `dest_path` | receiver opened a path that did not exist | both sides had to agree a base directory | send absolute paths across a boundary |
| Buffered writes before report | recovered file had a hole; `hash_mismatch` | journaled a block whose bytes were not durable | `decoded_blocks == JOURNAL_SYNC_BATCH_SIZE` was the tell |
| CRLF `.proto` | same pin, different hash, handshake refused | hash is over raw bytes | `.gitattributes` with `eol=lf` |
| `resource_tracker` double-unregister | `KeyError` traceback on every clean shutdown | `unlink()` and our detach both notified it | noise you dismiss will later mask something real |
| **80-column truncation** | headers mangled in `docker compose logs` | rendered at one width only | **no test would have caught this** |
| **`force_terminal=False`** | status display rendered **nothing** | in Rich, `False` means *force off*, not *auto-detect* | **no test would have caught this** |

The last two are the important ones. Every content and styling assertion passed
while the output was mangled or absent. **A visual deliverable needs eyeballing,
at more than one width, as part of its definition of done.**

---

## 14. What is still open

Nothing blocking is Person C's code.

**A's sender** — `main.cpp` is still the CLion template; the wire header and CRC
are done. Pinned five commits behind, so `file-monitor` will refuse the
connection outright. Needs `SENDER_CONTRACT.md` and the pin.

**B's receiver** — repo is one commit with a README. Needs
`RECEIVER_CONTRACT.md`, and the newest property will surprise him: bytes must be
durable before reporting `BlockDecoded`.

**One open question for B:** does the receiver write the shm completion bitmap,
or is `BlockDecoded` over UDS the only progress path? The cross-check is off and
quiet either way — keep it if he writes bits, delete it if not.

**Defined but unsent:** `UpdateRate` and `Abort` on the TX side. Both are in the
schema and neither is dispatched yet.

**Integration** (`INTEGRATION.md`) starts at a contract-hash check and adds one
variable at a time. Nothing past step 1 can begin until a C++ binary connects.

---

## 15. Defence questions

If you can answer these cold, you understand your own system.

1. Why is `session_manager` Python and not C++?
2. What is `Manifest.sender_id`, and what breaks if a sender misreads it?
3. Why `SOCK_SEQPACKET` rather than `SOCK_STREAM`?
4. When must the manager adopt a shm segment rather than reinitialise, and what
   happens if it gets that wrong?
5. Why does the journal append come before the in-memory fold?
6. Why is `proto_hash` over raw bytes instead of canonicalised text, and what is
   the cost of that choice?
7. Why are `ShmReader` and `ShmWriter` separate protocols?
8. Why does `send()` raise instead of logging?
9. What does `sender_bps_limit == 0` mean, and why is that ambiguous?
10. Why must staging and output live on the same filesystem?
11. Why is a lost spec sidecar treated as "known but unregistered" rather than
    unknown?
12. Why does a failed send drop a sender's shard instead of redistributing it?
13. Why is the shm bitmap cross-check disabled by default?
14. Why must a receiver `fdatasync` before reporting `BlockDecoded`?

---

## 16. Reading order in the code

Follow a request end to end rather than module by module.

**TX:** `main.py` → `services/watcher.py` → `services/dispatcher.py` →
`domain/planning.py` → `services/planner.py` → `ipc/uds.py`

**RX:** `main.py` → `ipc/uds.py` → `services/authority.py` →
`adapters/posix_shm.py` → `services/aggregator.py` → `services/verifier.py` →
`services/publisher.py`

Start at `main.py` both times. It is the only file that knows both adapters and
services, so it is the only place the whole shape is visible at once.
