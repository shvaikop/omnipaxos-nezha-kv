FROM rust:latest AS builder

# Stop if a command fails
RUN set -eux

# Faster crates.io access
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

WORKDIR /app

# Copy both repositories
COPY omnipaxos-nezha /app/omnipaxos-nezha
COPY omnipaxos-nezha-kv /app/omnipaxos-nezha-kv

# Build the server
WORKDIR /app/omnipaxos-nezha-kv
RUN cargo build --release --bin server

FROM debian:bookworm-slim AS runtime
WORKDIR /app
COPY --from=builder /app/omnipaxos-nezha-kv/target/release/server /usr/local/bin
EXPOSE 8000
# Keep container alive for debugging
ENTRYPOINT ["sleep", "infinity"]
