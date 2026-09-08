# `nexus-proto` / `rx.proto` — the one change

One commit, one `proto_hash` bump. A and B re-pin **once**.

## `SessionOpen`

```proto
message SessionOpen {
  // ... fields 1-6 unchanged (session_id, dest_path, total_blocks, and the
  //     RS params). Verify 9 is genuinely the next free number before
  //     committing — 7 and 8 are the only ones I have in front of me.

  // DEPRECATED. The receiver does not write either of these regions;
  // BlockDecoded over UDS is the only progress path (ANSWERS_FROM_C_002 §1).
  // Kept wire-present and still populated with valid offsets for one
  // release so nothing breaks mid-re-pin. Becomes `reserved 7, 8;` once A
  // and B have both moved past this commit. Do not read.
  uint64 block_table_offset = 7 [deprecated = true];
  uint64 bitmap_offset      = 8 [deprecated = true];

  // Size of the file in bytes, verbatim from Manifest.file_size.
  //
  // Required. SessionOpen is idempotent and may be re-sent at any time,
  // including to a receiver that connects after another receiver already
  // sent ManifestSeen. For that receiver, SessionOpen is its *only* source
  // of session parameters — it never saw the Manifest. Without file_size it
  // cannot size its staging view or bounds-check the final (short) block.
  uint64 file_size = 9;
}
```

## `PurgeSession`

```proto
message PurgeSession {
  string session_id = 1;

  // Why the session ended. Diagnostic only — the receiver's action is the
  // same for all three. Included so a receiver log line can distinguish
  // "we finished" from "we gave up", which is otherwise invisible on the
  // RX side. Not something B asked for; say if you'd rather not have it.
  PurgeReason reason = 2;
}

enum PurgeReason {
  PURGE_REASON_UNSPECIFIED = 0;
  PURGE_REASON_PUBLISHED   = 1;  // all blocks decoded, BLAKE3 matched, published
  PURGE_REASON_QUARANTINED = 2;  // all blocks decoded, BLAKE3 mismatched
  PURGE_REASON_INCOMPLETE  = 3;  // stalled below total_blocks, gave up
}
```

## What is deliberately **not** in this change

`receiver_block_table_offset` — withdrawn. See ANSWERS_FROM_C_002 §1.

## `proto_hash` after the bump

Unchanged algorithm: `.proto` files sorted by filename, raw bytes
concatenated with no separator, BLAKE3-256, 32 bytes.

```bash
# from the nexus-proto repo root, on a clean checkout
python3 - <<'PY'
import pathlib, blake3
files = sorted(pathlib.Path('.').rglob('*.proto'), key=lambda p: p.name)
h = blake3.blake3()
for f in files:
    h.update(f.read_bytes())
print(' '.join(f.name for f in files))
print(h.hexdigest())
PY
```

The new digest goes in this doc and in both `CLAUDE.md` files the moment the
commit lands — I am not quoting a hash here that I cannot compute against the
real tree, and neither of you should pin against a hash that came from a chat
window rather than from a checkout.

**Check CRLF before hashing.** `.gitattributes` covers this, but a Windows
checkout that predates it will hash differently for identical content. Hash on
Linux, or on a fresh clone.
