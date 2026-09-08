# The purge arc — what changed around `PurgeSession`

`SessionAuthority` decides a transfer session is terminal (published /
quarantined / incomplete) and tells the C++ receivers with `PurgeSession`.
That trigger — `domain/purge_policy.terminal_reason()` as the one decision
point, plus the 5-second sweep in the authority — is walked in
[`GUIDE.md`](GUIDE.md) §7. This note is the surrounding behaviour: what a
terminal session leaves on disk, and the four ways that can look wrong if you
meet it cold.

## INCOMPLETE quarantines the partial, with a report

A stalled transfer's partly-written file used to be left in the staging tree
with no metadata. It is evidence of what the one-way link delivered — a
mismatch keeps its bytes, and so should this — so on `INCOMPLETE` the
authority moves the partial into `quarantine/` (via
`Publisher.quarantine_incomplete`) and writes `<partial>.incomplete.json`
beside it: `{session_id, total_blocks, decoded_blocks, missing_block_ids}`,
the **complete** missing list, read from the journal.

The report matters because `purge()` then destroys the journal.
`_tear_down()` unlinks it (that is what stops an adopting restart replaying a
finished session). Once it is gone, the partial on disk is just bytes with
holes — the `.incomplete.json` is the only record of *which* holes. So the
report is written, and `fsync`'d, **before** `_tear_down` runs.

Two orderings here are load-bearing and both are commented in
`services/authority.py` — read the code, not a paraphrase:

- `purge()` runs `_preserve_incomplete_partial()` before `_tear_down()`, and
  that step **raises rather than returns** on failure, so a report that could
  not be written keeps the journal instead of stranding an uninterpretable
  partial.
- inside `_tear_down()`, shm first (it gates recovery), then the spec sidecar
  **strictly before** the journal — the comment block spells out the
  crash-window for each direction.

## Quarantine filenames carry the session id

A quarantined file is named `<stem>.<session_id><ext>` —
`report.a3f9c1d2e4b50678.bin`, not `report.bin`. `session_id` is
`secrets.token_hex(8)`, unique per transfer. This is deliberate: the move is
`os.replace`, which silently overwrites, so two failures of the same filename
in one run — a re-drop after a mismatch, two stalls of one file — would
destroy the first artifact. The id keeps every artifact distinct and ties it
back to the `session_id` in the logs. Both quarantine paths (hash mismatch
and incomplete) share `adapters/quarantine_paths.quarantine_name`; its
docstring has the rationale, and `tests/unit/test_quarantine_paths.py` is the
only place the naming is pinned with literal strings.

The milestone harness uses short ids (`m2`, `m3`), so its fixtures read
`stub-m2.m2.bin`; production is the 16-hex-char form.

## `_recover` refuses a session whose staged file is gone

**If you are reading this because of a `session_staged_file_missing_on_recovery`
log line:** on an adopting restart, `_recover()` found a session with a valid
spec sidecar and journal, but its staged file is not on disk. That is the
signature of a previous run that got through the quarantine move but died
before `_tear_down` cleared the sidecar/journal/shm entry (see the raise-on-
failure point above). Adopting it would rebuild a session whose bytes no
longer exist and re-broadcast `SessionOpen` for one the receivers were
already sent `PurgeSession` for, so `_recover` refuses it: it stays in
`_known` only (a resent `ManifestSeen` is then a no-op), and the
sidecar/journal/shm entry are left untouched.

**Remedy** (also in the log line): restore the partial from `quarantine/` to
the staging path to resume the session, or delete its `*.spec.json` sidecar
to abandon it. Either way the next restart is clean.

## FAILED is a render-time state, not a stored one

`SessionState.FAILED` never appears in a `SessionSnapshot`. The status
display derives it at render time (`status_display._display_state`): a
session still `OPEN`/`COMPLETE` in the aggregator that `authority.is_purged()`
reports as purged is a half-finished purge — the quarantine move succeeded
but the report write raised, so `_tear_down` was skipped and the sweep now
skips the session forever (it is in `_purged`). Without the derivation it
renders as a live `OPEN` row with a climbing Idle time. It is shown FAILED
with a blank Idle cell instead. Read-only: nothing re-evaluates the session
and no `PurgeSession` is re-sent; `session_sweep_evaluation_failed` is in the
logs.
