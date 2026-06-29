FROM rust:1.95 AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app

COPY --from=builder /app/target/release/tiny-p2p-node /app/tiny-p2p-node

CMD ["./tiny-p2p-node", "--nick", "node1", "--listen", "/ip4/0.0.0.0/tcp/7001", "--topic", "tiny-net"]
