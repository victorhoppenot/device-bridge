FROM rust:1.90-slim-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      cmake \
      pkg-config \
      libssl-dev \
      libsasl2-dev \
      zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      libssl3 \
      libsasl2-2 \
      zlib1g \
    && rm -rf /var/lib/apt/lists/*

ENV KAFKA_BROKER_URL=broker:29092 \
    BRIDGE_BIND_ADDR=0.0.0.0:3000

COPY --from=build /build/target/release/device-bridge /usr/local/bin/device-bridge

EXPOSE 3000

CMD ["device-bridge"]
