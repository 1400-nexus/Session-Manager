CHUNK_SIZE = 1048576

# Enough to hold a pid written by FlockFileLock.
LOCK_PID_READ_BYTES = 32

STAGED_FILE_MODE = 0o644
QUARANTINE_SUBDIR_NAME = "quarantine"

# Shared-memory completion segment header. The C++ receivers parse these exact
# bytes, so the format is a cross-language contract: explicit little-endian
# and standard packing (no native alignment padding). See adapters/shm_layout.py.
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
