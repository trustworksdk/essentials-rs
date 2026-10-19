# Build from the workspace root so the shared Cargo.lock is used:
#   docker build -f exchange-simulator/Dockerfile .
# (compose.yaml does this.) Exclusions live in Dockerfile.dockerignore next to this file.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -p exchange-simulator \
 && cp target/release/exchange-simulator /exchange-simulator

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /var/lib/exchange exchange \
 && mkdir -p /var/lib/exchange \
 && chown exchange /var/lib/exchange
COPY --from=build /exchange-simulator /usr/local/bin/exchange-simulator
# Ready-made configuration files, chosen with EXCHANGE_CONFIG (e.g. /etc/exchange/finnhub-market.yaml).
COPY exchange-simulator/config/finnhub-market.yaml /etc/exchange/finnhub-market.yaml
USER exchange
ENV EXCHANGE__SERVER__BIND=0.0.0.0:8080 \
    EXCHANGE__DATABASE__PATH=/var/lib/exchange/exchange.db \
    EXCHANGE__LOGGING__FORMAT=json
VOLUME /var/lib/exchange
EXPOSE 8080
# Follows the port in EXCHANGE__SERVER__BIND, so overriding that variable moves the check with the service.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD port="${EXCHANGE__SERVER__BIND##*:}"; curl -fsS "http://127.0.0.1:${port:-8080}/health" || exit 1
ENTRYPOINT ["/usr/local/bin/exchange-simulator"]
