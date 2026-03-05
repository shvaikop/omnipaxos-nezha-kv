FROM rust:latest AS builder

# Stop if a command fails
RUN set -eux

# Only fetch crates.io index for used crates
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# cargo-chef will be cached from the second build onwards
RUN cargo install cargo-chef
WORKDIR /app

# Copy both repositories
COPY omnipaxos-nezha /app/omnipaxos-nezha
COPY omnipaxos-nezha-kv /app/omnipaxos-nezha-kv

# Build the server
WORKDIR /app/omnipaxos-nezha-kv
RUN cargo build --release --bin client

FROM debian:bookworm-slim AS runtime
WORKDIR /app
COPY --from=builder /app/omnipaxos-nezha-kv/target/release/client /usr/local/bin
EXPOSE 8000
# Keep container alive for debugging
ENTRYPOINT ["sleep", "infinity"]
