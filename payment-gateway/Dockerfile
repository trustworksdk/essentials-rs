# Build from the workspace root so the shared Cargo.lock is used:
#   docker build -f payment-gateway/Dockerfile .
# Exclusions live in Dockerfile.dockerignore next to this file.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -p payment-gateway \
 && cp target/release/payment-gateway /payment-gateway

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /var/lib/gateway gateway \
 && mkdir -p /var/lib/gateway \
 && chown gateway /var/lib/gateway
COPY --from=build /payment-gateway /usr/local/bin/payment-gateway
USER gateway
ENV GATEWAY__SERVER__BIND=0.0.0.0:8081 \
    GATEWAY__DATABASE__PATH=/var/lib/gateway/payment-gateway.db \
    GATEWAY__LOGGING__FORMAT=json
VOLUME /var/lib/gateway
EXPOSE 8081
# Follows the port in GATEWAY__SERVER__BIND, so overriding that variable moves the check with the service.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD port="${GATEWAY__SERVER__BIND##*:}"; curl -fsS "http://127.0.0.1:${port:-8081}/health" || exit 1
ENTRYPOINT ["/usr/local/bin/payment-gateway"]
