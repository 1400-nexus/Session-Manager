# Cross-team pending — what C is waiting on

State that would otherwise live in a chat thread. One line per item: who owes
what, and which queued change it unblocks. Full context in
[`ANSWERS_FROM_C_002.md`](ANSWERS_FROM_C_002.md) (C's reply to B) and
[`rx.proto.patch.md`](rx.proto.patch.md) (the exact proto diff + `proto_hash`
command).

## Landed in `nexus-proto@7f757c5`

- **Item A** — one `nexus-proto` commit: `SessionOpen.file_size = 9`;
  `SessionOpen.7`/`.8` deprecated (still populated, wire-present); and
  **`PurgeSession.reason` `string` → `enum`** (taken, not vetoed). New digest
  `5b1483b9…4e6ec`; A + B re-pinned once. `reserved 7, 8` still to come, one
  release later.
- **Item B** — shm header v2: `receiver_region_offset` + `total_size` added,
  `slot_bytes` + `slot_count` dropped, `SHM_VERSION` 1→2. No session-table
  change. B confirmed: no bitmap write exists (CompletionBitmap reserved,
  never written), receivers never open the manager segment, tails are
  hard-failed on open.
- **Item D** — `arena_bytes` → `segment_bytes`, value 256 → 320 MiB.
  `compose.yml` unchanged (`shm_size: 512m` already covers it).

## Still queued

- **Item C** — delete C's `_next_offset` per-session bump allocator + the
  `region_end > segment_bytes` check. Gated on `reserved 7, 8` (fields are
  only deprecated in this release, still populated). One release later.

## Waiting on B

| Owes | What | Unblocks |
|---|---|---|
| B | **Nothing — FYI.** Don't read the header's `slot_bytes` / `slot_count`: they carry C's config value (`4194304` / `64`), C never reads them back, they are not B's `SLOT_SIZE` geometry. `RECEIVER_CONTRACT.md` §6 has the warning, but `shm_layout.py`, `constants.py`, `GUIDE.md` §10 and `README.md` still present them as ordinary header fields — item B removes them. | — |
| B | Confirm the receiver does **not** write the completion bitmap, in code or a branch. If a write exists it must come out. | confirms `ANSWERS_FROM_C_002.md` §1 |
| B | Confirm `receiver_region_offset` + `total_size` from the header are sufficient, and that B hard-fails `open()` when the tail is too small for `total_blocks` rather than trusting C's sizing. | **queue item B** |
| B | Veto or accept `PurgeSession.reason` as an enum. The `string` form (`"verified"` / `"hash_mismatch"` / `"incomplete"`) works today and vetoing is reasonable. | folds into **queue item A** |
| B | Confirm field **9** is genuinely next-free in `SessionOpen` against the current tree (C had only 7 and 8 in front of it). | **queue item A** |
| B | Clarify what "`PurgeSession` unimplemented on C's side" referred to — C emits it today (`SessionAuthority.purge` / `_PURGE_REASON_WIRE`, and before that `main.py`'s `purge()` closure). Possibly a stale read of an older checkout. | — |

## Waiting on A

| Owes | What | Unblocks |
|---|---|---|
| A | `proto_hash` bumps in the same commit as item A — A must re-pin. Before C pushes it: does the XOR cross-shard repair tier need a wire change? If yes it rides in the same bump; if it lands later it forces a second coordinated re-pin. | **queue item A** (rides along) or a second re-pin |

## Not pending — already settled

- The two-table decision (C's 64-byte recovery table stays C-private; B's table
  lives above `receiver_region_offset`). `MAX_SESSIONS` is not negotiated —
  C's 63 and B's ceiling are independent, effective limit is the lower.
  (`ANSWERS_FROM_C_002.md` §1 repo note, `RECEIVER_CONTRACT.md` §6.)
- `PurgeSession` trigger conditions and wire semantics — implemented and
  documented (`ANSWERS_FROM_C_002.md` §3, `RECEIVER_CONTRACT.md` §4).
- 320 MiB segment total — C takes B's number; no `compose.yml` change
  (`shm_size: 512m` already covers it).
