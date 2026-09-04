# syntax=docker/dockerfile:1
FROM python:3.12-slim AS builder

RUN pip install --no-cache-dir uv

WORKDIR /build
COPY pyproject.toml ./
COPY src ./src
COPY config.toml ./
COPY libs/nexus-proto ./libs/nexus-proto

# libs/nexus-proto/generated/python must be present at build time: hatchling
# force-includes it into the wheel at the sys.path root (see pyproject.toml),
# since the generated modules are flat (rx_pb2.py does `import common_pb2`)
# and must never end up nested inside a package.
RUN uv pip install --system --no-cache .
# uv is a build tool, not a runtime dependency -- remove it so the wholesale
# /usr/local/bin copy below doesn't carry it into the runtime image.
RUN pip uninstall -y uv


FROM python:3.12-slim AS runtime

RUN groupadd --gid 10001 nexus \
    && useradd --uid 10001 --gid nexus --no-create-home --shell /usr/sbin/nologin nexus

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY config.toml /etc/nexus/config.toml
# main.py hashes the .proto SOURCE files at startup (NEXUS_PROTO_CONTRACT_DIR),
# not the generated *_pb2.py -- without these the container exits immediately
# with proto_contract_missing.
COPY libs/nexus-proto/proto /etc/nexus/proto
COPY scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

RUN mkdir -p /run/nexus /var/nexus/staging && chown -R nexus:nexus /run/nexus /var/nexus

ENV NEXUS_CONFIG=/etc/nexus/config.toml \
    NEXUS_PROTO_CONTRACT_DIR=/etc/nexus/proto \
    NEXUS_STAGING_PATH=/var/nexus/staging \
    NEXUS_SOCKET_PATH=/run/nexus/session-manager.sock \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER nexus
WORKDIR /var/nexus

# The socket existing means the server has bound and is accepting
# connections -- it does NOT mean any receiver is connected, which is
# deliberately not part of health.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD test -S "$NEXUS_SOCKET_PATH" || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["python", "-m", "session_manager.main"]
