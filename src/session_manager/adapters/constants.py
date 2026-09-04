CHUNK_SIZE = 1048576

# Shared-memory completion segment header. The C++ receivers parse these exact
# bytes, so the format is a cross-language contract: explicit little-endian
# and standard packing (no native alignment padding). See adapters/shm_layout.py.
SHM_HEADER_FORMAT = "<4sI16sIIIQ"
SHM_MAGIC = b"NXRX"
SHM_VERSION = 1
SHM_SESSION_TABLE_OFFSET = 64
