# Multi-stage build for single-executable `dash` with embedded React UI (§14.1, optional image)
FROM node:25-slim AS web-builder
WORKDIR /app/web
# Node 25 images no longer bundle corepack; pin the pnpm release the lockfile was produced with.
RUN npm install -g pnpm@11.15.1
COPY web/package.json web/pnpm-lock.yaml web/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile
# .dockerignore keeps host node_modules (platform-specific binaries) out of this copy.
COPY web/ ./
RUN pnpm build

FROM rust:1.97-slim AS rust-builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends git pkg-config g++ && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
# Embedded MCP documentation resources (dash://docs/*) are compiled in from these files.
COPY GLOSSARY.md ./
COPY docs/ ./docs/
COPY crates/ ./crates/
COPY apps/ ./apps/
COPY --from=web-builder /app/web/dist ./web/dist
RUN cargo build --release --bin dash

# Must match the glibc of the rust:*-slim builder (Debian trixie).
FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    # Mounted Workspace volumes are usually owned by the host user.
    && git config --system --add safe.directory '*'
COPY --from=rust-builder /app/target/release/dash /usr/local/bin/dash
COPY docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh
COPY builtins /opt/dash/builtins
COPY fixtures /opt/dash/fixtures

# Run unprivileged; the daemon and its Git operations never need root.
RUN groupadd --system --gid 10001 dash \
    && useradd --system --uid 10001 --gid dash --home-dir /data --shell /usr/sbin/nologin dash \
    && mkdir -p /workspace /data \
    && chown dash:dash /workspace /data \
    && chmod 0755 /usr/local/bin/docker-entrypoint.sh
USER dash

ENV DASH_WORKSPACE=/workspace
ENV DASH_DATA_DIR=/data
ENV HOME=/data
VOLUME ["/workspace", "/data"]
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8080"]

# tini forwards SIGTERM (`docker stop`) to dash, which drains admitted actions before exiting.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
# Inside the container the daemon must listen on the container interface for `-p` publishing to
# reach it; keep the host side loopback-only (`-p 127.0.0.1:8080:8080`) so exposure stays deliberate.
CMD ["serve", "--bind", "0.0.0.0:8080"]
