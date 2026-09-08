#!/usr/bin/env bash
# Runs the five stub-receiver milestones (see Step 13 of the build guide)
# against a real session-manager process and exits non-zero if any assertion
# fails. Uses a temporary directory for all runtime state, so it never
# touches the repo working tree. POSIX-only (FlockFileLock needs fcntl).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

export PYTHONPATH="$REPO_ROOT/libs/nexus-proto/generated/python"

if ! python3 -c "import session_manager" >/dev/null 2>&1; then
    echo "session_manager is not importable -- run: pip install -e '.[dev]'" >&2
    exit 1
fi
if ! python3 -c "import rx_pb2" >/dev/null 2>&1; then
    echo "rx_pb2 is not importable -- did you run: git submodule update --init?" >&2
    exit 1
fi

# Blocks for milestones 1, 2, 3, 5: small and fast, correctness only.
BLOCKS=12
# Milestone 4 needs enough blocks that (a) each stub is still well short of
# finishing its shard when the manager gets killed, and (b) enough
# BlockDecoded reports have already landed, summed across all three
# receivers, to cross AppendJournal's 200-append auto-flush threshold before
# the kill -- otherwise the surviving journal could legitimately be empty
# (appends are buffered; only sync(), which a SIGKILL never runs, or that
# threshold, makes them durable) and there would be nothing to recover.
MILESTONE_4_BLOCKS=600

WORK_DIR="$(mktemp -d)"
STAGING_DIR="$WORK_DIR/staging"
OUTPUT_DIR="$WORK_DIR/output"
JOURNAL_DIR="$WORK_DIR/journal"
RUN_DIR="$WORK_DIR/run"
LOG_DIR="$WORK_DIR/logs"
CONFIG_PATH="$WORK_DIR/config.toml"
SOCKET_PATH="$RUN_DIR/session-manager.sock"
LOCK_PATH="$RUN_DIR/session-manager.lock"
SHM_NAME="nx-milestones-$$"

mkdir -p "$STAGING_DIR" "$OUTPUT_DIR" "$JOURNAL_DIR" "$RUN_DIR" "$LOG_DIR"

cat > "$CONFIG_PATH" << EOF
[paths]
staging_dir = "$STAGING_DIR"
output_dir = "$OUTPUT_DIR"
journal_dir = "$JOURNAL_DIR"
run_dir = "$RUN_DIR"
socket_path = "$SOCKET_PATH"
lock_path = "$LOCK_PATH"

[shm]
name = "$SHM_NAME"
arena_bytes = 1048576
slot_bytes = 4096

[aggregation]
poll_interval_s = 0.2
# Off: no receiver here writes the shm bitmap, so with it on every session
# "diverges" (bitmap 0 vs a climbing UDS count) forever. The UDS
# BlockDecoded stream is authoritative on its own.
shm_crosscheck = false

[purge]
# Fast so milestone 2's withheld-block session is swept to INCOMPLETE well
# inside its 10s log-wait; still comfortably longer than the gap between two
# BlockDecoded reports from an active stub (milestones 1/4).
sweep_interval_seconds = 0.3
stall_timeout_seconds = 2.0

[receivers]
count = 0
ports = []
binary_path = ""

[status]
refresh_interval_s = 5.0
force_terminal = false
EOF

MANAGER_PID=""
STUB_PIDS=()
MILESTONE_NAMES=()
MILESTONE_RESULTS=()

cleanup() {
    local pid
    for pid in "${STUB_PIDS[@]:-}"; do
        [ -n "$pid" ] && kill "$pid" >/dev/null 2>&1 || true
    done
    if [ -n "$MANAGER_PID" ]; then
        kill -9 "$MANAGER_PID" >/dev/null 2>&1 || true
    fi
    for pid in "${STUB_PIDS[@]:-}" "$MANAGER_PID"; do
        [ -n "$pid" ] && wait "$pid" >/dev/null 2>&1 || true
    done
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT
# See file-monitor's run_milestones.sh for why INT/TERM exit explicitly
# rather than just relying on the EXIT trap to run once and resume: a
# resumed script would run on from here against a WORK_DIR cleanup() already
# deleted.
trap 'exit 130' INT
trap 'exit 143' TERM

start_manager() {
    # $1 = log file
    NEXUS_CONFIG="$CONFIG_PATH" python3 -m session_manager.main > "$1" 2>&1 &
    MANAGER_PID=$!
    local i
    for i in $(seq 1 100); do
        if [ -S "$SOCKET_PATH" ]; then
            return 0
        fi
        sleep 0.1
    done
    echo "session-manager did not create the socket in time"
    return 1
}

stop_manager() {
    if [ -n "$MANAGER_PID" ]; then
        kill "$MANAGER_PID" >/dev/null 2>&1 || true
        wait "$MANAGER_PID" >/dev/null 2>&1 || true
        MANAGER_PID=""
    fi
}

stop_all_stubs() {
    local pid
    for pid in "${STUB_PIDS[@]:-}"; do
        if [ -n "$pid" ]; then
            kill "$pid" >/dev/null 2>&1 || true
            wait "$pid" >/dev/null 2>&1 || true
        fi
    done
    STUB_PIDS=()
}

# Full reset before a milestone that needs a cold, empty start -- everything
# EXCEPT milestone 4's mid-milestone restart, which deliberately calls
# start_manager again without this, since surviving state is the entire point.
reset_state() {
    stop_all_stubs
    stop_manager
    rm -rf "$STAGING_DIR" "$OUTPUT_DIR" "$JOURNAL_DIR"
    mkdir -p "$STAGING_DIR" "$OUTPUT_DIR" "$JOURNAL_DIR"
}

LAST_STUB_PID=""

start_stub() {
    # $1=timeout seconds, $2=receiver-id, $3=session-id, $4=blocks, $5=log file,
    # rest = extra stub_receiver.py args (--withhold/--corrupt).
    # Sets LAST_STUB_PID rather than echoing it -- same reason as
    # file-monitor's start_stub: called via command substitution this would
    # run in a subshell, orphaning the backgrounded job from this shell's
    # job table before it could ever be `wait`-ed on.
    local timeout_seconds="$1" receiver_id="$2" session_id="$3" blocks="$4" log_file="$5"
    shift 5
    timeout "$timeout_seconds" python3 tests/integration/stub_receiver.py \
        --receiver-id "$receiver_id" --socket "$SOCKET_PATH" \
        --session-id "$session_id" --blocks "$blocks" "$@" > "$log_file" 2>&1 &
    LAST_STUB_PID=$!
}

interruptible_sleep() {
    # A plain foreground `sleep N` blocks bash from noticing a trapped signal
    # until N elapses. Backgrounding it and waiting on the wait builtin
    # instead is interruptible -- see file-monitor's run_milestones.sh.
    local sleep_pid
    sleep "$1" &
    sleep_pid=$!
    wait "$sleep_pid" 2>/dev/null
    kill "$sleep_pid" >/dev/null 2>&1 || true
}

wait_for_log() {
    local log_file="$1" pattern="$2" timeout_seconds="$3"
    local iterations=$((timeout_seconds * 10))
    local i
    for i in $(seq 1 "$iterations"); do
        grep -q "$pattern" "$log_file" 2>/dev/null && return 0
        sleep 0.1
    done
    grep -q "$pattern" "$log_file" 2>/dev/null
}

output_dir_is_empty() {
    [ -z "$(ls -A "$OUTPUT_DIR" 2>/dev/null)" ]
}

# Independently verifies a published output file's bytes against
# stub_receiver.py's own deterministic content formula -- not the manager's
# formula, which is the entire point of an end-to-end check like this.
verify_output_hash() {
    local session_id="$1" total_blocks="$2" output_path="$3"
    python3 - "$session_id" "$total_blocks" "$output_path" << 'PYEOF'
import sys
sys.path.insert(0, "tests/integration")
import blake3
import stub_receiver as sr

session_id, total_blocks, output_path = sys.argv[1], int(sys.argv[2]), sys.argv[3]

expected = sr._file_hash(session_id, total_blocks)
with open(output_path, "rb") as f:
    actual = blake3.blake3(f.read()).digest()

if actual != expected:
    print(f"FAIL: output hash mismatch: expected {expected.hex()}, got {actual.hex()}")
    sys.exit(1)
print(f"output hash verified for {session_id}")
PYEOF
}

record() {
    MILESTONE_NAMES+=("$1")
    MILESTONE_RESULTS+=("$2")
}

# ------------------------------------------------------------------------
# Milestone 1: three stubs, all blocks -> VERIFIED
# ------------------------------------------------------------------------
milestone_1() {
    set +e
    reset_state
    if ! start_manager "$LOG_DIR/m1_manager.log"; then
        set -e
        return 1
    fi

    start_stub 20 0 m1 "$BLOCKS" "$LOG_DIR/m1_stub0.log"
    local pid0=$LAST_STUB_PID
    start_stub 20 1 m1 "$BLOCKS" "$LOG_DIR/m1_stub1.log"
    local pid1=$LAST_STUB_PID
    start_stub 20 2 m1 "$BLOCKS" "$LOG_DIR/m1_stub2.log"
    local pid2=$LAST_STUB_PID
    STUB_PIDS=("$pid0" "$pid1" "$pid2")

    local ok=0
    if ! wait_for_log "$LOG_DIR/m1_manager.log" "session_published" 10; then
        echo "FAIL: milestone 1 never saw session_published"
        ok=1
    elif [ ! -f "$OUTPUT_DIR/stub-m1.bin" ]; then
        echo "FAIL: output file missing for milestone 1"
        ok=1
    elif ! verify_output_hash m1 "$BLOCKS" "$OUTPUT_DIR/stub-m1.bin"; then
        ok=1
    fi

    stop_all_stubs
    set -e
    return "$ok"
}

# ------------------------------------------------------------------------
# Milestone 2: withheld blocks -> stall timeout -> INCOMPLETE
# ------------------------------------------------------------------------
milestone_2() {
    set +e
    reset_state
    if ! start_manager "$LOG_DIR/m2_manager.log"; then
        set -e
        return 1
    fi

    # Block 0 is always in receiver 0's shard (0 % 3 == 0).
    start_stub 20 0 m2 "$BLOCKS" "$LOG_DIR/m2_stub0.log" --withhold 0
    local pid0=$LAST_STUB_PID
    start_stub 20 1 m2 "$BLOCKS" "$LOG_DIR/m2_stub1.log"
    local pid1=$LAST_STUB_PID
    start_stub 20 2 m2 "$BLOCKS" "$LOG_DIR/m2_stub2.log"
    local pid2=$LAST_STUB_PID
    STUB_PIDS=("$pid0" "$pid1" "$pid2")

    local ok=0
    if ! wait_for_log "$LOG_DIR/m2_manager.log" "session_stalled" 10; then
        echo "FAIL: milestone 2 never saw session_stalled"
        ok=1
    fi
    if ! grep -q "session_stalled" "$LOG_DIR/m2_manager.log" 2>/dev/null \
        || ! grep "session_stalled" "$LOG_DIR/m2_manager.log" | grep -q "session_id=m2"; then
        echo "FAIL: session_stalled log line missing session_id=m2"
        ok=1
    fi
    if ! output_dir_is_empty; then
        echo "FAIL: output directory not empty after milestone 2"
        ok=1
    fi
    # The partial is evidence, not scratch: it moves to quarantine/ with the
    # session id before the extension (stub-m2.m2.bin) and a report of exactly
    # which blocks it lacks (block 0, withheld above).
    if [ ! -f "$STAGING_DIR/quarantine/stub-m2.m2.bin" ]; then
        echo "FAIL: incomplete partial was not quarantined for milestone 2"
        ok=1
    fi
    if ! python3 - "$STAGING_DIR/quarantine/stub-m2.m2.bin.incomplete.json" << 'PYEOF'
import json, sys
report = json.load(open(sys.argv[1]))
assert report["session_id"] == "m2", report
assert report["missing_block_ids"] == [0], report
assert report["decoded_blocks"] == report["total_blocks"] - 1, report
PYEOF
    then
        echo "FAIL: milestone 2 incomplete report missing or wrong"
        ok=1
    fi

    stop_all_stubs
    set -e
    return "$ok"
}

# ------------------------------------------------------------------------
# Milestone 3: one corrupted block -> HASH_MISMATCH, quarantined
# ------------------------------------------------------------------------
milestone_3() {
    set +e
    reset_state
    if ! start_manager "$LOG_DIR/m3_manager.log"; then
        set -e
        return 1
    fi

    start_stub 20 0 m3 "$BLOCKS" "$LOG_DIR/m3_stub0.log" --corrupt 0
    local pid0=$LAST_STUB_PID
    start_stub 20 1 m3 "$BLOCKS" "$LOG_DIR/m3_stub1.log"
    local pid1=$LAST_STUB_PID
    start_stub 20 2 m3 "$BLOCKS" "$LOG_DIR/m3_stub2.log"
    local pid2=$LAST_STUB_PID
    STUB_PIDS=("$pid0" "$pid1" "$pid2")

    local ok=0
    if ! wait_for_log "$LOG_DIR/m3_manager.log" "session_quarantined" 10; then
        echo "FAIL: milestone 3 never saw session_quarantined"
        ok=1
    fi
    if ! output_dir_is_empty; then
        echo "FAIL: output directory not empty after milestone 3"
        ok=1
    fi
    if [ ! -f "$STAGING_DIR/quarantine/stub-m3.bin" ]; then
        echo "FAIL: quarantined file missing for milestone 3"
        ok=1
    fi

    stop_all_stubs
    set -e
    return "$ok"
}

# ------------------------------------------------------------------------
# Milestone 4: kill -9 the manager mid-transfer -> restart adopts, recovers
# the spec from the sidecar and blocks from the journal, and the session
# still completes and verifies with correct bytes.
# ------------------------------------------------------------------------
milestone_4() {
    set +e
    reset_state
    if ! start_manager "$LOG_DIR/m4_manager_1.log"; then
        set -e
        return 1
    fi

    start_stub 60 0 m4 "$MILESTONE_4_BLOCKS" "$LOG_DIR/m4_stub0.log"
    local pid0=$LAST_STUB_PID
    start_stub 60 1 m4 "$MILESTONE_4_BLOCKS" "$LOG_DIR/m4_stub1.log"
    local pid1=$LAST_STUB_PID
    start_stub 60 2 m4 "$MILESTONE_4_BLOCKS" "$LOG_DIR/m4_stub2.log"
    local pid2=$LAST_STUB_PID
    STUB_PIDS=("$pid0" "$pid1" "$pid2")

    # See MILESTONE_4_BLOCKS above for why 2s: comfortably past the journal's
    # durability threshold, comfortably short of any stub finishing its shard.
    interruptible_sleep 2

    echo "killing manager pid $MANAGER_PID with SIGKILL (simulating a crash)..."
    kill -9 "$MANAGER_PID" >/dev/null 2>&1 || true
    wait "$MANAGER_PID" >/dev/null 2>&1 || true
    MANAGER_PID=""

    # Deliberately no reset_state here -- the staged file, the journal, and
    # the shm segment surviving the crash is the entire point. The three
    # stubs above are also still alive and already hold this session's
    # SessionOpen, so they reconnect and keep reporting once the restarted
    # manager's socket comes back up, the same way real receivers would.
    if ! start_manager "$LOG_DIR/m4_manager_2.log"; then
        echo "FAIL: manager did not restart"
        set -e
        return 1
    fi

    local ok=0
    # These lines are written only after the restarted manager waits out its
    # adopt grace period (ADOPT_GRACE_PERIOD_SECONDS, ~1.5s), calls
    # authority.start(), and runs recovery -- so wait for them rather than
    # grepping a log that start_manager only waited for the socket on.
    if ! wait_for_log "$LOG_DIR/m4_manager_2.log" "session_recovered" 15; then
        echo "FAIL: session_recovered was never logged on restart"
        ok=1
    fi
    if ! wait_for_log "$LOG_DIR/m4_manager_2.log" "adopted=True" 15; then
        echo "FAIL: restart did not adopt (expected adopted=True in the log)"
        ok=1
    fi
    if grep -q "session_spec_missing_on_recovery" "$LOG_DIR/m4_manager_2.log"; then
        echo "FAIL: session spec was lost on recovery"
        ok=1
    fi

    if ! wait_for_log "$LOG_DIR/m4_manager_2.log" "session_published" 30; then
        echo "FAIL: session m4 never completed and verified after the restart"
        ok=1
    elif ! verify_output_hash m4 "$MILESTONE_4_BLOCKS" "$OUTPUT_DIR/stub-m4.bin"; then
        ok=1
    fi

    stop_all_stubs
    set -e
    return "$ok"
}

# ------------------------------------------------------------------------
# Milestone 5: wrong proto_hash -> refused, other stubs unaffected
# ------------------------------------------------------------------------
milestone_5() {
    set +e
    reset_state
    if ! start_manager "$LOG_DIR/m5_manager.log"; then
        set -e
        return 1
    fi

    start_stub 20 0 m5 "$BLOCKS" "$LOG_DIR/m5_stub0.log"
    local pid0=$LAST_STUB_PID
    start_stub 20 1 m5 "$BLOCKS" "$LOG_DIR/m5_stub1.log"
    local pid1=$LAST_STUB_PID
    start_stub 20 2 m5 "$BLOCKS" "$LOG_DIR/m5_stub2.log"
    local pid2=$LAST_STUB_PID
    STUB_PIDS=("$pid0" "$pid1" "$pid2")

    if ! wait_for_log "$LOG_DIR/m5_manager.log" "session_published" 10; then
        echo "FAIL: milestone 5's good stubs never completed"
        set -e
        return 1
    fi

    local bad_proto_dir="$WORK_DIR/bad-proto"
    mkdir -p "$bad_proto_dir"
    cp "$REPO_ROOT"/libs/nexus-proto/proto/*.proto "$bad_proto_dir/"
    # The hash covers raw file bytes, so a comment-only edit changes it --
    # the documented trade-off in ipc/handshake.py.
    printf '\n// tampered for milestone 5\n' >> "$bad_proto_dir/rx.proto"

    # This stub never gets past the handshake, so it never exits on its own
    # (its reconnect loop retries forever); `timeout` killing it (exit 124)
    # is the expected outcome here, not a failure.
    timeout 3 python3 tests/integration/stub_receiver.py \
        --receiver-id 9 --socket "$SOCKET_PATH" --proto-dir "$bad_proto_dir" \
        --session-id m5-bad --blocks "$BLOCKS" > "$LOG_DIR/m5_bad_stub.log" 2>&1 || true

    interruptible_sleep 0.3

    local ok=0
    if ! grep -q "peer_proto_hash_mismatch" "$LOG_DIR/m5_manager.log"; then
        echo "FAIL: manager log is missing peer_proto_hash_mismatch"
        ok=1
    fi
    for pid in "$pid0" "$pid1" "$pid2"; do
        if ! kill -0 "$pid" >/dev/null 2>&1; then
            echo "FAIL: original stub pid $pid is no longer running"
            ok=1
        fi
    done
    if ! kill -0 "$MANAGER_PID" >/dev/null 2>&1; then
        echo "FAIL: session-manager process is no longer running"
        ok=1
    fi

    stop_all_stubs
    set -e
    return "$ok"
}

echo "=== Milestone 1: three stubs, all blocks -> VERIFIED ==="
if milestone_1; then record "milestone-1" "PASS"; else record "milestone-1" "FAIL"; fi
echo

echo "=== Milestone 2: withheld blocks -> stall -> INCOMPLETE ==="
if milestone_2; then record "milestone-2" "PASS"; else record "milestone-2" "FAIL"; fi
echo

echo "=== Milestone 3: corrupted block -> HASH_MISMATCH ==="
if milestone_3; then record "milestone-3" "PASS"; else record "milestone-3" "FAIL"; fi
echo

echo "=== Milestone 4: kill -9 mid-transfer -> adopt, recover, verify ==="
if milestone_4; then record "milestone-4" "PASS"; else record "milestone-4" "FAIL"; fi
echo

echo "=== Milestone 5: wrong proto_hash -> refused ==="
if milestone_5; then record "milestone-5" "PASS"; else record "milestone-5" "FAIL"; fi
echo

echo "==================== RESULTS ===================="
overall=0
for i in "${!MILESTONE_NAMES[@]}"; do
    printf "%-14s %s\n" "${MILESTONE_NAMES[$i]}" "${MILESTONE_RESULTS[$i]}"
    if [ "${MILESTONE_RESULTS[$i]}" != "PASS" ]; then
        overall=1
    fi
done
echo "==================================================="

exit "$overall"
