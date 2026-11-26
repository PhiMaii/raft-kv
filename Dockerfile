# Builds the raft-kv binary. Used by docker-compose.yml.

FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY . .
# protoc is vendored, so no system packages are needed.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p cli \
    && cp target/release/raft-kv /usr/local/bin/raft-kv

FROM debian:bookworm-slim
COPY --from=build /usr/local/bin/raft-kv /usr/local/bin/raft-kv
COPY config/docker /etc/raft-kv
EXPOSE 7001
ENTRYPOINT ["raft-kv"]
