# nexus-session-manager

The RX-side reassembly authority for Nexus, a one-way file-transfer system for
lossy networks. It is the sole writer of session state on the receiving machine:
it accepts each receiver's report that a manifest was seen, opens the session,
tracks per-block completion aggregated over Unix Domain Sockets from N receiver
processes, verifies the finished file against the manifest's BLAKE3 hash, and
publishes it. One of the eight graded processes in a live transfer.

**The file payload never crosses the Python boundary.** Receivers FEC-decode
blocks and write the bytes to disk themselves. This service moves bitmaps and
counters — which blocks are done, how many packets each receiver dropped — not
file data. It hashes the finished file once, at the end, to verify it.

## Architecture

In a live transfer there are eight processes: a `file-monitor` on the sending
machine and the N sender child processes it supervises (3 by default), and this
`session-manager` on the receiving machine and the N receiver processes it in
turn supervises (3 by default, once a real receiver binary exists —
`receivers.count` below). A separate `router` process is used independently as a
network-impairment test harness and is not part of a normal transfer.

Within this repo, `session-manager` follows hexagonal architecture: `domain/`
holds pure logic (block-to-byte-offset math, completion/stall predicates) with
no I/O; `ports/` defines the Protocol interfaces domain and services depend on;
`adapters/` implements them against real infrastructure (POSIX shared memory,
`fcntl` locks, an append-only journal, BLAKE3, Unix sockets); `services/`
orchestrates the session authority, the progress aggregator, the verifier and
publisher, receiver-liveness tracking, and the status display; `main.py` is the
composition root that wires everything into one process under an
`asyncio.TaskGroup`.

The receiver-facing wire contract is in
[`docs/RECEIVER_CONTRACT.md`](docs/RECEIVER_CONTRACT.md); the integration plan is
in [`docs/INTEGRATION.md`](docs/INTEGRATION.md).

## Setup

```bash
git submodule update --init          # pulls in libs/nexus-proto (pinned a83fb3b)
cp .env.example .env && source .env  # sets PYTHONPATH for the generated protobuf code
pip install -e '.[dev]'
python -m session_manager.main       # reads ./config.toml by default
```

`NEXUS_CONFIG=/path/to/config.toml python -m session_manager.main` points at a
different config file. POSIX only — `fcntl`, `os.posix_fallocate` and
`multiprocessing.shared_memory` unlinking are load-bearing.

## Testing

```bash
pytest                                    # unit + integration
mypy --strict src/session_manager tests
ruff check src tests && ruff format --check src tests
scripts/run_milestones.sh                 # end-to-end, native, PASS/FAIL table
```

`scripts/run_milestones.sh` starts a real `session-manager` and three stub
receivers (`tests/integration/stub_receiver.py`) against a throwaway config and
exercises five scenarios:

1. three receivers, all blocks → `VERIFIED`, published file hash matches source
2. one receiver withholds blocks → stall timeout → `INCOMPLETE`, output empty
3. one receiver corrupts a block's bytes → `HASH_MISMATCH`, staged file
   quarantined, output empty
4. `kill -9` the manager mid-transfer → restart **adopts** the segment, recovers
   the spec from the sidecar and blocks from the journal, session still verifies
5. a receiver with the wrong `proto_hash` → refused, the other receivers
   unaffected

## Configuration

TOML file (default `config.toml`), overridden by `NEXUS_`-prefixed environment
variables, then validated at load. Relative paths resolve against the *config
file's own directory*, not the process's working directory.

| Section.key | Env var | Default | Notes |
|---|---|---|---|
| `paths.staging_dir` | `NEXUS_STAGING_DIR` | `./staging` | Decoded blocks are written here. **Must share a filesystem with `output_dir`** — publish is `os.replace`. |
| `paths.output_dir` | `NEXUS_OUTPUT_DIR` | `./output` | Verified files are moved here by atomic rename. |
| `paths.journal_dir` | `NEXUS_JOURNAL_DIR` | `./journal` | Append-only decode journal + per-session spec sidecars, for restart recovery. Keep on a persistent volume. |
| `paths.run_dir` | `NEXUS_RUN_DIR` | `./run` | Holds the socket and lock. |
| `paths.socket_path` | `NEXUS_SOCKET_PATH` | `./run/session-manager.sock` | `AF_UNIX`, `SOCK_SEQPACKET`. |
| `paths.lock_path` | `NEXUS_LOCK_PATH` | `./run/session-manager.lock` | `fcntl` single-instance lock. |
| `shm.name` | `NEXUS_SHM_NAME` | `nexus-rx` | Completion segment name. **Receivers that write the bitmap must use the same value.** |
| `shm.arena_bytes` | `NEXUS_SHM_ARENA_BYTES` | `268435456` | Segment size (256 MB). A container needs `shm_size` ≥ this. |
| `shm.slot_bytes` | `NEXUS_SHM_SLOT_BYTES` | `4194304` | Must divide `arena_bytes`; arena must hold ≥ 4 slots. |
| `aggregation.poll_interval_s` | `NEXUS_AGGREGATION_POLL_INTERVAL_S` | `1.0` | How often completion state is recomputed. |
| `aggregation.stall_timeout_s` | `NEXUS_AGGREGATION_STALL_TIMEOUT_S` | `8.0` | No progress for this long → `INCOMPLETE`. Must be `> poll_interval_s`. |
| `aggregation.shm_crosscheck` | `NEXUS_AGGREGATION_SHM_CROSSCHECK` | `false` | Cross-check the UDS decoded count against the shm bitmap popcount. Off until a receiver actually writes the bitmap; the UDS `BlockDecoded` stream is authoritative on its own. |
| `receivers.count` | `NEXUS_RECEIVERS_COUNT` | `0` | Receiver child processes to supervise. `0` = supervise nothing (the binary lives in another repo). |
| `receivers.ports` | `NEXUS_RECEIVERS_PORTS` | `9100,9101,9102` | Must have ≥ `count` entries. |
| `receivers.binary_path` | `NEXUS_RECEIVERS_BINARY_PATH` | `./bin/nexus-receiver` | Required when `count > 0`. |
| `status.refresh_interval_s` | `NEXUS_STATUS_REFRESH_INTERVAL_S` | `0.5` | Live table redraw cadence (TTY only). |
| `status.force_terminal` | `NEXUS_STATUS_FORCE_TERMINAL` | `false` | `false` = auto-detect a TTY; `true` = force `rich` rendering even with no TTY. |
| — | `NEXUS_CONFIG` | `config.toml` | Which config file to load. |
| — | `NEXUS_PROTO_CONTRACT_DIR` | `libs/nexus-proto/proto` | `.proto` source files hashed at startup for the handshake. |

FEC parameters (`k` / `n` / `symbol_bytes`) are deliberately **not** config —
they arrive per-session in the `Manifest`. Pinning them here would produce a
receiver that decodes correctly until someone retunes `k` on the TX side, then
writes garbage that looks like packet loss.

The `nexus-proto` pin and `proto_hash` are a cross-service contract: a mismatch
does not fail loudly on its own, it refuses every receiver's handshake. Check
`git -C libs/nexus-proto rev-parse HEAD` — it must be `a83fb3b`.

## Running in a container

`docker compose up` builds and runs `session-manager` alone (`Dockerfile`,
`compose.yml`). Native execution (`python -m session_manager.main`) remains fully
supported and is what the graded machines run.

Three things are specific to this service:

- **`shm_size: 512m`** in `compose.yml` — the completion arena is 256 MB and the
  container default `/dev/shm` is 64 MB, so `create_or_adopt` would fail at
  startup without it.
- **One named volume for all of `/var/nexus`** — `staging_dir` and `output_dir`
  must share a filesystem (`os.replace`), and `config.py` validates `st_dev`
  equality at startup, so two volumes would fail fast.
- **`receivers.count = 0`** — the C++ receiver binary is in another repo, so the
  supervisor manages nothing.

There is no `tty: true`: a PTY makes `rich.Live` repaint the status tables into
`docker compose logs` on every refresh. Without one, the display logs a single
`status` event every few seconds instead. For the live tables, run interactively
(`docker compose run --rm session-manager`) or set `NEXUS_STATUS_FORCE_TERMINAL=1`.
