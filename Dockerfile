# Multi-stage build for single-executable `dash` with embedded React UI
FROM node:25-slim AS web-builder
WORKDIR /app/web
RUN corepack enable pnpm
COPY web/package.json web/pnpm-lock.yaml web/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile
COPY web/ ./
RUN pnpm build

FROM rust:1.97-slim AS rust-builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends git pkg-config g++ && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
COPY apps/ ./apps/
COPY --from=web-builder /app/web/dist ./web/dist
RUN cargo build --release --bin dash

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=rust-builder /app/target/release/dash /usr/local/bin/dash
COPY builtins /opt/dash/builtins
COPY fixtures /opt/dash/fixtures

ENV DASH_WORKSPACE=/workspace
ENV DASH_DATA_DIR=/data
VOLUME ["/workspace", "/data"]
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/dash"]
CMD ["serve", "--bind", "127.0.0.1:8080"]
