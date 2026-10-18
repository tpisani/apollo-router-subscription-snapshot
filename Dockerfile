# Router customizado: apollo-router 2.18.0 + subscription_snapshot.
#   docker build -t router-subscription-snapshot router-subscription-snapshot
FROM rust:1.97.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked && cp target/release/router /router

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /router /dist/router
WORKDIR /dist
EXPOSE 4100 8088
ENTRYPOINT ["/dist/router"]
