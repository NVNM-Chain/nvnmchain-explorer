# Multi-stage build: compile once, ship a small glibc runtime.

FROM rust:1.97-slim AS builder
WORKDIR /build

# Compile dependencies in their own layer using a stub binary, then overwrite
# with the real sources so application changes only recompile the app crate.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
COPY templates ./templates
# Compiled in via include_str!; the runtime image does not need a copy.
COPY abi ./abi
COPY contracts/layout ./contracts/layout
RUN cargo build --release --locked

# stable (trixie) glibc >= whatever rust:1.97-slim ships, so the binary runs
# regardless of which Debian release the builder image is based on.
FROM debian:stable-slim
RUN useradd --system --no-create-home --uid 10001 app
WORKDIR /app
COPY --from=builder /build/target/release/nvnmchain-explorer /usr/local/bin/nvnmchain-explorer
COPY templates ./templates
COPY deploy/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENV DB_PATH=/data/explorer.db \
    RUST_LOG=nvnmchain_explorer=info \
    NATIVE_SYMBOL=OM
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
