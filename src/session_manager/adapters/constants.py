CHUNK_SIZE = 1048576

# Enough to hold a pid written by FlockFileLock.
LOCK_PID_READ_BYTES = 32

STAGED_FILE_MODE = 0o644
QUARANTINE_SUBDIR_NAME = "quarantine"

# Journal record: session_id (fixed, null-padded), block_id (u32), offset
# (u64), length (u32) -- CRC32 is computed over exactly these packed bytes
# and appended separately (JOURNAL_CRC_FORMAT).
JOURNAL_SESSION_ID_BYTES = 16
JOURNAL_FIELDS_FORMAT = f"<{JOURNAL_SESSION_ID_BYTES}sIQI"
JOURNAL_CRC_FORMAT = "<I"
JOURNAL_FILENAME_SUFFIX = ".journal"

# fdatasync per append would dominate the write path at ~200 blocks/sec, so
# appends batch and sync() is the explicit durability barrier; this is the
# fallback auto-sync threshold if a caller appends without ever calling it.
JOURNAL_SYNC_BATCH_SIZE = 200

# Shared-memory completion segment header. The C++ receivers parse these exact
# bytes, so the format is a cross-language contract: explicit little-endian
# and standard packing (no native alignment padding). See adapters/shm_layout.py.
# The real contract is magic, version and session_table_offset. The two u32s
# between owner_pid and session_table_offset -- slot_bytes, slot_count -- are a
# slot model the manager does not use -- not the receiver's slot geometry, do
# not read or derive from them; removed in the next shm header revision.
SHM_HEADER_FORMAT = "<4sI16sIIIQ"
SHM_MAGIC = b"NXRX"
SHM_VERSION = 1
SHM_SESSION_TABLE_OFFSET = 64

# Session table entry: session_id (fixed, null-padded), total_blocks (u64),
# block_table_offset (u64), bitmap_offset (u64).
SHM_SESSION_ID_BYTES = 40
SHM_SESSION_ENTRY_FORMAT = f"<{SHM_SESSION_ID_BYTES}sQQQ"

# Bytes reserved for the session table; per-session block-table and bitmap
# regions start after it.
SHM_SESSION_TABLE_BYTES = 4096

# SessionSpec sidecar: one JSON file per session, next to its journal file.
# Python-only (unlike the shm header/session table above, nothing in the
# C++ receivers reads this), so it carries no wire-format constraints.
SESSION_SPEC_FILENAME_SUFFIX = ".spec.json"

# Incomplete-session report: written into quarantine/ next to a partial the
# sweep gave up on, `<relpath>` + this suffix. Records which blocks the
# partial holds and which it lacks, since the journal is unlinked right after.
INCOMPLETE_REPORT_FILENAME_SUFFIX = ".incomplete.json"
