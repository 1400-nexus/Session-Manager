# Cross-team pending — what C is waiting on

State that would otherwise live in a chat thread. One line per item: who owes
what, and which queued change it unblocks. Full context in
[`ANSWERS_FROM_C_002.md`](ANSWERS_FROM_C_002.md) (C's reply to B) and
[`rx.proto.patch.md`](rx.proto.patch.md) (the exact proto diff + `proto_hash`
command).

## The queue

Nothing below is landed. Each is "B/A says yes, then C opens the commit."

| Item | Change | Reference |
|---|---|---|
| **A** | one `nexus-proto` commit, `proto_hash` bump, A + B re-pin once: `SessionOpen.file_size = 9`; `SessionOpen.7`/`.8` → `deprecated` then `reserved`; **`PurgeSession.reason` `string` → `enum`** (a wire break — vetoable) | `rx.proto.patch.md` |
| **B** | shm header: add `receiver_region_offset` + `total_size`, drop `slot_bytes` + `slot_count`, `SHM_VERSION` 1→2. ~15 files, ~120–150 lines + B's C++ struct. No session-table change. | `ANSWERS_FROM_C_002.md` §1 |
| **C** | delete C's `_next_offset` per-session bump allocator + the `region_end > arena_bytes` check | gated on A's `reserved 7, 8` |
| **D** | `arena_bytes` → `segment_bytes` (~20 files, mechanical); value 256 → 320 MiB. Pairs with B. | `ANSWERS_FROM_C_002.md` §1 |

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
