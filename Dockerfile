# syntax=docker/dockerfile:1.7

FROM node:24-bookworm-slim AS web-builder
WORKDIR /workspace/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM rust:1.97.1-bookworm AS rust-builder
RUN apt-get update \
    && apt-get install --yes --no-install-recommends libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/ crates/
RUN cargo build --locked --release --bin etl-server-cloudberry

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin pg2cb \
    && install --directory --owner=pg2cb --group=pg2cb /app/web /etc/pg2cb
WORKDIR /app
COPY --from=rust-builder /workspace/target/release/etl-server-cloudberry /usr/local/bin/etl-server-cloudberry
COPY --from=web-builder --chown=pg2cb:pg2cb /workspace/web/dist/ /app/web/
USER pg2cb
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:8080/health/live >/dev/null || exit 1
ENTRYPOINT ["etl-server-cloudberry"]
CMD ["serve", "--config", "/etc/pg2cb/etl-server-cloudberry.toml", "--web-dir", "/app/web"]

