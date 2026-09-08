# Answers from C — items 1-3

Replying to B's five-item split. Items 4 and 5 are A's and are untouched here.

Short version: **1 is "no"**, and you were right that it dissolves the
negotiation rather than resolving it — but not quite for free. **2 is "yes"**,
field 9. **3 is defined below**, including the "incomplete" trigger that wasn't
written down anywhere.

---

## 1. Does the receiver write `nxrx`'s completion bitmap? — **No.**

`BlockDecoded` over UDS is the sole progress path. The bitmap is not a
fallback, not a cross-check, not optional-if-convenient. Don't write it.

This isn't a new decision, it's an old one finally made explicit: the design
note has said "the session manager maps read-only except for the session
table" since the architecture doc, and the plan was always that shm progress
was a *removable* cross-check. Your finding that C's own code never defines
the bit semantics anywhere real is the evidence that it was never load-bearing
— nothing could have depended on semantics that don't exist.

**The reason it's the right answer, not just the convenient one.** The only
argument for the bitmap is manager-restart recovery: if `session_manager`
dies mid-transfer, where does progress come from? Not the bitmap.
`adapters/append_journal.py` already journals every block *before* folding it
into the completion set, and that ordering is deliberate — the journal, not
shm, is the recovery record, and unlike shm it survives a receiver restart and
a machine reboot too. The bitmap would only have covered the window between a
receiver decoding a block and C journalling it, and that window is closed by
journal-before-fold, not by a second data path. A cross-check that costs a
negotiated 320 MiB layout, two cross-language constants and an 8-byte
alignment contract is not removable. It's coupling.

### What this kills

- `receiver_block_table_offset` — withdrawn, not landing.
- `RECEIVER_BLOCK_ENTRY_BYTES` and `RECEIVER_ARENA_BYTES` as shared
  constants — gone. `sizeof(BlockEntry)` becomes a number only you need to
  know. (It is 1064, and I did verify that by compiling your header rather
  than reading it — 1060 bytes of members, `alignas` 8, rounded to 1064. Your
  own docstring's 1044 is stale, as you said.)
- The 8-byte alignment contract between my allocator and your atomics.
- My per-session bump allocator, and with it the `region_end >
  arena_bytes - RECEIVER_ARENA_BYTES` bounds check. `SessionOpen.7` and `.8`
  are deprecated in the same commit.

### What it does **not** kill — one correction to "if no, nothing changes"

Something does change: **the segment still has to grow.** Your block table and
your slot arena still need somewhere to live, and today `arena_bytes` = 256 MiB
is the whole segment. Same arithmetic as your proposal, same answer:

| | bytes |
|---|---|
| header (4096, `alignas`) + session table (8 × 400) | 7,296 |
| your block table, 8 × 3835 × 1064 | 32,643,520 |
| your slot arena | 268,435,456 |
| **minimum** | **301,086,272** (287.14 MiB) |

So **320 MiB** stands — I'm taking your number. What changes is that it stops
being a negotiated layout and becomes one integer in my `config.toml`. The RX
container is already `shm_size: 512m`, so no compose change is needed.

> **Repo note (added on commit, not part of C's reply) — the sizing table's
> first row is B's numbers, not C's, and the derivation is wrong.**
>
> - `header (4096, alignas)` is B's `alignas(4096) ShmHeader`. **C's header
>   reserved region is 64 bytes** (44 packed + 20 pad; `SHM_SESSION_TABLE_OFFSET`).
> - `session table (8 × 400)` is B's `SessionEntry` × `MAX_SESSIONS`. **C's
>   session table is 63 entries × 64 bytes** (`SHM_SESSION_ENTRY_FORMAT =
>   "<40sQQQ"`) in a 4096-byte reserved region.
> - C's actual C-side boundary today is **`64 + 4096 = 4160`**, not 7,296.
>
> The 320 MiB total is unaffected — it's dominated by B's slot arena and
> block table; the prefix is negligible at 4,160 or 7,296.
>
> **Decision this implies: two session tables, not one.** C's 64-byte
> recovery table (written on `init_session` / re-read on adopting restart —
> the milestone-4 path) stays C-private, below `receiver_region_offset`. B's
> ~400-byte table lives entirely above it, in B's region. Different
> consumers, different lifetimes — and a struct both sides parse is exactly
> the silent-corruption class §1 exists to remove (`proto_hash` does not
> cover the shm header). So `MAX_SESSIONS` is **not** a negotiated number:
> C's 63 and B's 8 are independent ceilings and the effective concurrent-
> session limit is the lower of the two. That belongs in
> `RECEIVER_CONTRACT.md`, not in a negotiation. What C's header change then
> needs from B is **nothing about the table** — only the two offset fields
> (`receiver_region_offset`, `total_size`) and B's `open()`-time tail check.

### The boundary, which is now the only thing we share

One line instead of a layout:

```
[0, receiver_region_offset)      mine:  header + session table
[receiver_region_offset, total_size)   yours: lay it out however you like
```

I create the segment (I have to — `Config` carries `shm_name` to you, and
`posix_shm.py`'s create-or-adopt is the tested path behind the milestone-4
recovery). I write `receiver_region_offset` and `total_size` into the header.
You read both and fail loudly at `open()` if the tail is too small for
`total_blocks` rather than trusting me to have sized it right.

That also settles the two-creators ambiguity from last round: **one creator,
me; every offset published in the header; nobody computes a layout
independently.** Your `compute_arena_offset(uint32_t total_blocks)` signature
still needs to change, since your arena offset stops depending on
`total_blocks` — but that's now entirely inside your half.

One naming trap survives and is worth an explicit line, because it would fail
silently: in your header `arena_bytes` means *the slot arena*, and
`slot_count = arena_bytes / SLOT_SIZE`. If "320 MiB" ever reaches that
expression, `slot_count` computes as 218,453 instead of 174,762 and
`slot_offset()` runs 64 MiB past the end of the mapping. Keep `arena_bytes`
meaning the arena; the segment total needs a different word.

---

## 2. `SessionOpen` has no `file_size` — **yes, adding it. Field 9.**

You're right and the failure mode is exactly as you describe: a receiver that
connects after another receiver already sent `ManifestSeen` never saw the
Manifest, so `SessionOpen` is its only source of session parameters. This is
not a rare path — it's the documented one. `SessionOpen` is idempotent and
re-sent on `ReceiverHello` for every open session, precisely so a late joiner
or a reconnecting receiver can pick up an in-flight session. Without
`file_size` that receiver can't size its staging view or bounds-check the
final short block, so the replay path was decorative.

`uint64 file_size = 9`, verbatim from `Manifest.file_size`. Caveat on the
number: 7 and 8 are the only field numbers I have in front of me here, so
confirm 9 is genuinely next-free against the tree before we commit.

**`proto_hash`:** bumping, and everything above goes in **one commit** so you
and A re-pin once. A is already five behind; I'm not making that two separate
catch-ups. Exact digest and the command that produced it are in
`rx.proto.patch.md` — I'm not quoting a hash I computed anywhere other than a
clean Linux checkout, and neither of you should pin against one that came out
of a chat window. If item 5 needs a wire change, tell me before I push and it
rides along in the same bump.

---

## 3. `PurgeSession` — triggers defined, including "incomplete"

Trigger is on entering a terminal state, which is now three named conditions
rather than two-and-a-gap. Your guess matched mine on the first two.

| Reason | Condition |
|---|---|
| `PUBLISHED` | all `total_blocks` decoded, BLAKE3 matches the manifest, atomic rename into `output_dir` completed |
| `QUARANTINED` | all `total_blocks` decoded, BLAKE3 mismatched, artifact moved to quarantine with both digests logged |
| `INCOMPLETE` | `decoded_blocks < total_blocks` and no `BlockDecoded` for this session for `stall_timeout` seconds |

**On `INCOMPLETE`, the one you flagged.** It was never going to be an explicit
end-of-stream signal — the link is one-way, `SessionComplete` goes from the
sender to `file_monitor` on the *other machine*, and no RX process ever learns
that the sender stopped. So it has to be a stall, and it turns out it doesn't
need inventing: `domain/progress.py` already has a stall predicate. The change
is promoting stall from a display state to a terminal one.

Two details that matter:

- **The clock runs from `opened_at` when no block ever arrived.** A Manifest
  that arrives followed by nothing at all must still terminate, or the session
  holds a staging file and a session-table slot forever.
- **A complete session is never stalled, however old.** Otherwise a slow
  verifier gets its session declared `INCOMPLETE` for a file we actually have.

`stall_timeout` defaults to **60s** and is configurable. That number is a
placeholder chosen to be obviously-too-long rather than obviously-too-short.
It has to exceed the worst-case gap between two consecutive *decodable* blocks
under the degraded router, not between two packets — under heavy loss a tail
block can sit at K-1 symbols for a long while as other blocks keep completing.
The defensible number needs A's pacing (`rate_limit_bps` and the stripe
schedule), which is the same gap you flagged. I'd rather ship 60s and tighten
it after the first real run than argue it in the abstract.

### Wire semantics you can build against

- Sent once per session on entering a terminal state, broadcast to all
  connected receivers.
- **Idempotent.** May be re-sent. An unknown `session_id` is a no-op, not an
  error — same rule as `SessionOpen`.
- After it, C sends nothing further for that `session_id` and will not reuse
  the id. You may free your block table, slots and session-table entry.
- **No shm ordering hazard.** You write block bytes to the staging file and
  `fdatasync` before reporting `BlockDecoded`, so by the time I verify and
  publish I'm reading the filesystem, not your arena. Purge can't race a read
  of yours.
- A `BlockDecoded` in flight when purge is sent gets dropped on my side at
  debug level, not warn. Don't treat it as an error on yours either.

> **Repo note (added on commit, not part of C's reply) — `PurgeSession.reason`
> is a wire break, not a new field.** `rx.proto.patch.md` changes field 2 from
> `string reason` to `enum PurgeReason reason`. §2 above ("I added it beyond
> your ask") and "What I still need from you" #3 read as if `reason` is being
> added; it already exists as `string reason = 2` at the current pin, carrying
> `"verified"` / `"hash_mismatch"` / `"incomplete"` — three values the manager
> emits today (`SessionAuthority._PURGE_REASON_WIRE`) and RECEIVER_CONTRACT.md
> §4 documents. Retyping field 2 breaks any peer already parsing the string.
> **Vetoing the enum is reasonable** — the string form works, the receiver's
> action is identical for all three reasons, and it keeps the proto commit to
> the one field that isn't optional (`file_size = 9`). If it goes ahead,
> `_PURGE_REASON_WIRE` and the `purge_policy.py` module docstring both flip.

---

## What has actually been done

Landed as reviewable artifacts, not as commits — the repos aren't in front of
me right now, so treat these as ready-to-apply rather than applied:

1. **`rx.proto.patch.md`** — the exact `SessionOpen` and `PurgeSession` diff,
   the deprecation comments, and the `proto_hash` recompute command.
2. **`purge_policy.py`** — pure module for
   `session_manager/domain/purge_policy.py`. No I/O, no clock of its own,
   caller passes `now`. Fits alongside `domain/progress.py`.
3. **`test_purge_policy.py`** — 15 tests, all passing, covering each edge case
   above: never-received-a-block, complete-but-unverified, a late block
   resetting the stall clock, and a complete session not being stallable.

## What I still need from you

1. **Confirm you're not writing the bitmap today**, in either the code or a
   branch. If there's a write already in there, it has to come out, or C will
   eventually read a region nobody maintains.
2. **Confirm `receiver_region_offset` + `total_size` from the header is
   enough**, and that you'll hard-fail `open()` on a too-small tail rather
   than trusting my sizing.
3. **Veto or accept `PurgeSession.reason`** — I added it beyond your ask.
4. **Confirm 9 is next-free in `SessionOpen`** against the actual tree.

Nothing here is pushed. Say yes and I'll open the `nexus-proto` commit.
