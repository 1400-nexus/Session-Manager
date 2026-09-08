# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Project Overview

**nexus-session-manager** is the **RX-side reassembly authority** for the Nexus
one-way file-transfer system, and the **sole writer of session state** on the
receiving machine. It accepts each receiver's `ManifestSeen`, opens the session,
aggregates per-block `BlockDecoded` reports from three C++ receiver processes
over Unix Domain Sockets, verifies the finished file against the manifest's
BLAKE3 hash, publishes only what verified, and displays live status. One of
eight processes in a live transfer.

**The file payload never crosses the Python boundary.** Receivers FEC-decode
blocks and write the bytes to disk themselves. This service moves bitmaps and
counters — which blocks are done, how many packets each receiver dropped — and
hashes the finished file once, at the end, to verify it. The status display is
a graded deliverable in its own right, not a debugging aid.

Multi-service system:

- **file-monitor**: TX side — detects files, plans FEC encoding, dispatches
  sender assignments
- **session-manager** (this repo): aggregates receiver progress, verifies,
  publishes, displays status
- **sender ×3** (A, C++) transmit; **receiver ×3** (B, C++) FEC-decode and write
- **router**: network-impairment test harness (loss, corruption, misrouting),
  used independently

The system uses a custom UDP-based FEC protocol with Reed–Solomon coding for
lossy networks. The link is strictly one-way: no ACK, no NACK, no retransmit
request.

## Commands

```bash
# PYTHONPATH must include the flat generated protobuf modules
export PYTHONPATH=libs/nexus-proto/generated/python   # or: cp .env.example .env && source .env

python -m session_manager.main            # reads ./config.toml; NEXUS_CONFIG=other.toml overrides

mypy --strict src/session_manager tests
ruff check src tests
ruff format --check src tests
pytest                                    # POSIX-only tests (AF_UNIX, fcntl, posix_fallocate) skip on Windows
bash scripts/run_milestones.sh            # 5 end-to-end scenarios, native, PASS/FAIL table
```

A real run is **POSIX-only** — `fcntl.flock`, `os.posix_fallocate` and
`multiprocessing.shared_memory` unlinking are load-bearing. `mypy` / `ruff` /
the unit suite run on Windows; the milestone harness and the POSIX integration
tests (`tests/integration/test_uds.py` and friends) need Linux or a container.

### Proto generation

The `.proto` definitions live in `libs/nexus-proto` (git submodule); generated
Python is committed there. Regenerate with `libs/nexus-proto/compile.sh`
(`compile.bat` on Windows) only when the contract changes, and bump the pin.

## Documentation — read these before re-deriving from the code

| File | What it covers |
|---|---|
| `docs/GUIDE.md` | The whole control plane in one pass — architecture, ports-and-adapters and why, both service walkthroughs, the shared machinery, cross-language contracts, config reference, how it is proven, a bug log. Verified against the code. **Start here.** |
| `docs/RECEIVER_CONTRACT.md` | What B's C++ receiver implements against — transport, the `proto_hash` handshake, message flow, the properties that bite, the shm header layout. |
| `docs/INTEGRATION.md` | The step-by-step integration order, one variable at a time, with the failure diagnosis for each step. |
| `docs/PURGE_ARC.md` | What a terminal session leaves on disk beyond the `PurgeSession` trigger — the INCOMPLETE quarantine + missing-blocks report, session-id filenames, `_recover`'s refusal of a session with no staged file (+ remedy), the render-time FAILED state. Read before touching `authority.purge` / `_tear_down` / `_recover` or the quarantine adapters. |
| `SHARED_CODE.md` | What was copied from `file-monitor` vs written fresh, and the behaviours where a bug fixed in one must be fixed in both. |

## Design Principles & Standards

### Stack Requirements

- **Python 3.11+** (this project uses 3.11+ features like `tomllib`)
- **Type hints mandatory** on every function signature and class attribute
- **`mypy --strict`** must pass—no untyped `Any` unless explicitly justified
- **`typing.Protocol`** for structural interfaces—no ABCs unless nominal inheritance is genuinely required
- **`dataclasses`** with `frozen=True` for immutability (all domain models are frozen)
- **`typing.NewType`** for every ID type—never pass raw `str`/`int` as identifier across boundaries
- **pytest** with `hypothesis` for property-based testing

### Design Patterns & Abstraction

**Reach for a design pattern before writing ad-hoc logic:**

- **Strategy** via Protocol + injected callables/objects
- **Factory** for constructing instances behind a Protocol
- **Repository Protocol** for data access
- **Decorator** for layering behavior without modifying core logic
- **Observer** for event/pub-sub flows
- **Chain of Responsibility** for pipelines
- **Builder** for complex object assembly
- **Adapter** for wrapping incompatible interfaces (see `adapters/` directory)
- **Command** for encapsulating actions

**Core principles:**

- **Program to protocols, not implementations.** Every component that could vary is defined as a `typing.Protocol`; consumers depend on the protocol type. Concrete classes are wired via constructor injection.
- **Push abstraction as high as it usefully goes.** Domain logic must not import infrastructure (I/O, frameworks, DB clients, network calls) directly—those sit behind Protocols and get injected.
- **No god classes or god modules.** A class/module with more than one reason to change should be split. Single Responsibility is non-negotiable.
- **Favor composition over inheritance.** Use inheritance only for genuine is-a hierarchies; otherwise compose behavior via injected collaborators.
- **Open/Closed in practice:** New behavior should be addable by adding a new class implementing the Protocol, not by editing existing conditionals.

### SOLID Principles

- **S** (Single Responsibility): One reason to change per class/module. Split anything doing more.
- **O** (Open/Closed): Open for extension, closed for modification. New behavior = new class implementing the Protocol, not edited conditionals.
- **L** (Liskov Substitution): Any class implementing a Protocol must be substitutable for it without breaking callers. No raising `NotImplementedError` in an implementation.
- **I** (Interface Segregation): Many small, focused Protocols over one broad one. No client forced to depend on methods it doesn't use.
- **D** (Dependency Inversion): Depend on Protocols, not concretions. Wiring happens via constructor injection.

### Naming Conventions

- Follow **PEP 8**: `snake_case` for modules/functions/variables; `PascalCase` for classes/Protocols; `UPPER_SNAKE_CASE` for constants
- Protocols use plain names like `Clock`, `Hasher`, not `ClockProtocol` or `IClock`
- Names must be descriptive and unabbreviated—no `mgr`, `calc`, `svc`, `cfg` (write `app_config`), `fh`/`f` (write `file_handle`), `env` (write `environment`), `conn` (write `connection`)
- File name matches its primary type in `snake_case` (e.g., `blake3_hasher.py` defines `Blake3Hasher`)

### Coding Standards

- **Type hints on every function signature and class attribute**—no implicit `Any`. This includes every instance attribute assigned in `__init__`, even when its type is trivially inferable from an already-typed parameter (`self.path: Path = path`, not `self.path = path`)—mypy tolerates the omission, but the standard here is stricter than what mypy requires
- **Models are frozen** (immutable) dataclasses; entities with identity may be mutable but still fully typed
- **IDs are dedicated types** (`NewType` or frozen wrapper), never bare `str`/`int`
- **Dependency injection for everything**—no constructing collaborators inline inside business logic
- **Comments record decisions and hazards, not what the code says.** Code must be self-documenting through naming and structure—but a comment that defends a correct-looking-wrong line is not redundant with the code, it is protecting it from a future "fix".
  - **Remove:** comments that restate what the code says, narrate obvious steps, or duplicate a docstring or a test name.
  - **Keep:** cross-language or cross-service contracts; explanations of why a line that looks wrong is correct; recorded trade-offs; pending dependencies on another repo (with what to change when they land); and anything whose absence would make a future "simplification" look safe.
  - This distinction is load-bearing, not stylistic. An over-trim under the old blanket "no comments" rule once deleted the `proto_hash` algorithm spec from `file-monitor/src/file_monitor/ipc/handshake.py`—a cross-language contract the C++ side implements from that prose—and it had to be restored. Same failure mode removed four decision/hazard comments from `session-manager/src/session_manager/domain/models.py` (block/symbol size wire trap, gauge-vs-counter aggregation, pending `rx_pb2` enum, frozen-dataclass mutable-container reach); also restored.
- **No magic values—every literal becomes a named constant**, and constants live in a dedicated `constants.py`, never inline in the file that uses them:
  - One `constants.py` per package (`ipc/constants.py`, `services/constants.py`, `adapters/constants.py`, `domain/constants.py` if domain ever needs one); top-level modules (`config.py`, `main.py`) share `session_manager/constants.py`
  - This covers **string literals used as keys or dispatch discriminators**, not just numbers—TOML keys, env var names, protobuf oneof field names (e.g., `RECEIVER_HELLO_FIELD_NAME`, `ENVELOPE_ONEOF_GROUP_NAME` in `ipc/constants.py`). If the same literal is compared, looked up, or must match an external contract in more than one place, it is a constant, imported from one place, not retyped at each site
  - Exception: purely descriptive/presentational strings—structlog event names, human-readable prose inside a raised exception's message—stay inline. They carry no control-flow weight and keeping them at the call site is what makes them greppable against actual log/error output
- **No duplicate logic.** If two methods have the same body (e.g., `register`/`refresh` on a registry), one calls the other—never copy the implementation. Prefer a data table (list/dict of tuples) plus a loop over N near-identical `if`/`elif` blocks doing the same shape of work (see `config.py`'s `ENV_OVERRIDES`)—adding a case becomes a table row, not a new branch
- **Keep functions/methods short and single-purpose**; extract instead of nesting
- **Always push for optimization.** Prefer efficient algorithms and data structures; consider time/space complexity. Avoid unnecessary copies, redundant iteration, premature materialization, and redundant system calls (e.g., check `is_dir()` before falling back to `exists()`, not both unconditionally). Optimization must never compromise design principles above.

## Architecture

Strict hexagonal, dependency arrow pointing inward only: `domain/` (pure) ←
`ports/` (Protocol definitions) ← `services/` (orchestration) and `adapters/`
(concrete implementations). `main.py` is the only file that knows both adapters
and services. There is no `import-linter` and no `Makefile` — the boundary is
held by `mypy --strict` plus review. `docs/GUIDE.md` §4 has the full picture;
§7 walks every module of this service.

IPC is `AF_UNIX` / `SOCK_SEQPACKET` (message boundaries preserved, no framing).
Peers are anonymous until `ReceiverHello` passes the message-type check and the
`proto_hash` check; only then keyed by `ReceiverId`. Message dispatch is a
registry keyed by `oneof` field name, never a chain of `isinstance` checks.
Two envelope types on this side: `nexus.ipc.Envelope` (TX-authored messages the
manager receives) and `nexus.rx.RxEnvelope` (manager → receiver).

The generated protobuf modules are **flat on `sys.path`** (`rx_pb2.py` does
`import common_pb2`), never a package — `import rx_pb2`, not
`from session_manager.pb import rx_pb2`. Set up by `.env.example` (`PYTHONPATH`)
and `pyproject.toml` (hatch force-include at the wheel root); `[tool.mypy]` has
`mypy_path = "src"` and per-module `ignore_missing_imports` for the `*_pb2`
names.

## The few things a task must not get wrong

Each is a decision `docs/GUIDE.md` explains in full; the failure mode is why it
is load-bearing.

- **`flock` before anything touches shm.** Two managers on one completion
  segment is the worst bug available here. `SessionAuthority.start()` takes the
  lock before `create_or_adopt`.
- **Adopt-vs-create never zeroes a live segment.** On restart, if the segment
  exists with a valid header and a receiver answers as alive, the manager
  *adopts* it untouched — receivers hold live slot indices, and zeroing that
  memory corrupts three running processes with no error anywhere. Only
  absent / invalid-header / no-live-receiver reinitialises.
- **Journal append comes before the in-memory fold.** Append the `BlockDecoded`
  record, *then* add the block to the completion set. Reverse it and a crash in
  between leaves a block reported complete but absent from the journal — a
  restart then stalls a session whose bytes are already on disk.
- **Verify before publish; never delete on mismatch.** A completed session is
  BLAKE3-checked against the manifest. Pass → atomic rename into output. Fail →
  quarantine, log both digests, keep the evidence.
- **`ShmReader` and `ShmWriter` stay separate Protocols.** The aggregator reads
  the bitmap; the authority writes session state. Merging them would let the
  wrong component hold a writable view into receiver memory.
- **UDS `BlockDecoded` is authoritative; the shm bitmap is a removable
  cross-check.** Completion is built from the UDS stream — that alone satisfies
  the brief. The bitmap comparison is off in the shipped `config.toml` (no
  receiver writes bits yet) and everything passes without it. Do not make
  anything depend on the bitmap.

From the shared machinery, each already paid for in debugging (`SHARED_CODE.md`):
peer identity on reconnect (`self._peers.get(id) is my_queue` before removing),
write-failure teardown (`shutdown(SHUT_RD)`), the interruptible backoff sleep,
bind-before-adopt, and the `resource_tracker` double-unregister. A fix to any of
these must be mirrored in `file-monitor`.

### FEC parameters are not configured here

`k` / `n` / `symbol_bytes` are deliberately absent from `config.toml` — they
arrive per-session in the `Manifest`. Pinning them would give a receiver that
decodes correctly until someone retunes `k` on the TX side, then writes garbage
that looks like packet loss. `block_size = k * symbol_bytes`, and the wire field
`Manifest.block_bytes` carries the **symbol** size, not the block size.

## Dependencies and Constraints

**Do NOT:**

- Use `SOCK_STREAM` (use `SOCK_SEQPACKET` for message boundaries)
- Chain `isinstance` checks for message dispatch (registry keyed by `oneof` field name)
- Put protobuf types in the domain layer (wire format stays out of business logic)
- Introduce a second hash algorithm for the same kind of check (BLAKE3 everywhere)
- Zero or reinitialise a shm segment without the adopt-vs-create decision
- Use pybind11 or custom C++ bindings; use `tc netem` for impairment (use nexus-router)

**DO:**

- Inject dependencies as protocols; keep `domain/` pure (time as two floats, progress as `set[BlockId]`)
- Validate config at load time and fail loudly, naming the offending key
- Use `asyncio.to_thread` for CPU-bound work (hashing)
- Test the interesting failures against fakes with failure injection, and run the contract suites (fake + real adapter) so a fake cannot drift into fiction
