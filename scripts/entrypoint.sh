#!/bin/sh
# Validates the container's minimum configuration and prepares its runtime
# directories, then hands off to the application. No orchestration, no
# retries, no supervision -- that belongs to session_manager itself.
set -eu

EX_CONFIG=78

missing=""
[ -z "${NEXUS_CONFIG:-}" ] && missing="$missing NEXUS_CONFIG"
[ -z "${NEXUS_STAGING_PATH:-}" ] && missing="$missing NEXUS_STAGING_PATH"
[ -z "${NEXUS_SOCKET_PATH:-}" ] && missing="$missing NEXUS_SOCKET_PATH"
[ -z "${NEXUS_PROTO_CONTRACT_DIR:-}" ] && missing="$missing NEXUS_PROTO_CONTRACT_DIR"

if [ -n "$missing" ]; then
    echo "entrypoint: missing required environment variable(s):$missing" >&2
    exit "$EX_CONFIG"
fi

# A missing or empty contract directory means the process would start,
# compute a hash over nothing, and refuse every receiver's handshake -- fail
# here instead, with a message naming the path, rather than as a mysterious
# proto_contract_missing after the fact.
proto_file_found=""
if [ -d "$NEXUS_PROTO_CONTRACT_DIR" ]; then
    for candidate in "$NEXUS_PROTO_CONTRACT_DIR"/*.proto; do
        if [ -f "$candidate" ]; then
            proto_file_found="$candidate"
            break
        fi
    done
fi
if [ -z "$proto_file_found" ]; then
    echo "entrypoint: NEXUS_PROTO_CONTRACT_DIR ($NEXUS_PROTO_CONTRACT_DIR) does not exist or" \
        "contains no .proto files" >&2
    exit "$EX_CONFIG"
fi

mkdir -p "$NEXUS_STAGING_PATH" "$(dirname "$NEXUS_SOCKET_PATH")"

exec "$@"
