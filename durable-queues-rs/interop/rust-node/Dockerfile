# syntax=docker/dockerfile:1
# The Rust node (the `interop` binary) as a container, so Phase 7 runs both languages under the same limits.
# The build context is the workspace root:
#   docker build -f durable-queues-rs/interop/rust-node/Dockerfile -t durable-queues-rust-node .
FROM rust:1.99-slim-bookworm AS build
WORKDIR /build
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked -p durable-queues-rs --features interop --bin interop \
 && cp target/release/interop /interop

FROM debian:bookworm-slim
COPY --from=build /interop /usr/local/bin/interop
ENTRYPOINT ["/usr/local/bin/interop"]
