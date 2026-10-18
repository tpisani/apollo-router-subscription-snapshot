# `subscription_snapshot`: an Apollo Router plugin

A native Apollo Router 2.18.0 plugin that keeps the last event of each
subscription. A client that joins an already open (deduplicated) upstream gets that
state right away (1 ms in the Router pipeline), instead of waiting for the
subgraph's next frame (~2.8 s with a periodic 3 s frame).

```yaml
plugins:
  globo.subscription_snapshot:
    enabled: true
    identity_headers: [authorization]
```

## Usage

A native plugin is compiled into the Router: the official Router binary and image
(`ghcr.io/apollographql/router`) cannot load it. You run a custom Router binary
instead, which is the official Router plus the plugin; its configuration, flags
and behavior are otherwise the same.

### Prerequisites

- **Your subscriptions send the full state and a version in every event.** The
  snapshot may be older than an event already on its way, and the client keeps
  the highest version. With deltas or plain events, a late joiner would get an
  event out of context.
- **A GraphOS key** (`APOLLO_KEY` and `APOLLO_GRAPH_REF`): the Router requires it
  for federated subscriptions. The free plan includes them.
- **To build:** Docker, or Rust 1.97.1 and `protoc`
  (`apt-get install protobuf-compiler libprotobuf-dev`). The first build takes ~20 min.
- **Apollo Router 2.18.0.** The plugin pins the Router version (`=2.18.0`); another
  version needs a new build of the plugin (see [Support and upgrades](#support-and-upgrades)).

### Option 1: build the Router from this repository

The binary in [`src/main.rs`](src/main.rs) is the Router with the plugin. No image
is published, so build it:

```bash
docker build -t my-router .
docker run -p 4000:4000 -e APOLLO_KEY -e APOLLO_GRAPH_REF \
  -v "$PWD/router.yaml:/dist/config/router.yaml:ro" \
  -v "$PWD/supergraph.graphql:/dist/schema/supergraph.graphql:ro" \
  my-router --config /dist/config/router.yaml --supergraph /dist/schema/supergraph.graphql
```

or `cargo build --release`, which produces `target/release/router`.

### Option 2: your own custom Router

To pick the Router version yourself, or to add other native plugins later, create a
Rust project for your Router and depend on the plugin through git. The ready-made
example is in [`examples/custom-router/`](examples/custom-router/) (CI builds it):

```toml
# Cargo.toml
[dependencies]
apollo-router = "=2.18.0"   # exactly the version the plugin is built against
anyhow = "1"
subscription_snapshot = { git = "https://github.com/tpisani/apollo-router-subscription-snapshot", rev = "<commit>", package = "apollo-router-subscription-snapshot" }
```

```rust
// src/main.rs: linking the crate registers the plugin.
use subscription_snapshot as _;

fn main() -> anyhow::Result<()> {
    apollo_router::main()
}
```

Start from this repository's `Cargo.lock` to build with the same dependency versions
that were tested. The example also has a `Dockerfile` for the image.

### Configuration

The minimal configuration is in
[`examples/custom-router/router.yaml`](examples/custom-router/router.yaml); the
plugin part is:

```yaml
subscription:
  enabled: true
  deduplication:
    all:
      enabled: true      # recommended (see below)
plugins:
  globo.subscription_snapshot:
    enabled: true
    identity_headers: [authorization]
```

| Option | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | turns the snapshot on; off, the plugin leaves responses untouched |
| `identity_headers` | `[]` | request headers that are part of the snapshot key; clients that differ in any of them never share a snapshot |

**Deduplication is recommended, not required.** The plugin works either way (both
are covered by the integration tests):

- **With deduplication** (the case it was built for): clients with the same
  operation, variables and propagated headers share one upstream, and a late
  joiner would otherwise wait for its next event. The snapshot fills that wait.
  Set `identity_headers` to the headers propagated to the subgraph, and the plugin
  shares nothing the Router does not already share.
- **Without deduplication:** every client opens its own upstream. The late joiner
  gets the snapshot first, then the events of its own upstream. It only helps if
  your subgraph does not send the current state when a subscription opens.

> **Warning: without deduplication, the snapshot key must cover everything that
> personalizes the response.** Each client used to get its own response from the
> subgraph; with the plugin, clients with the same key share frames. If what the
> subgraph sends depends on anything outside the key (a header not listed in
> `identity_headers`, a cookie, a token claim), a client can get another client's
> frame.

## Repository layout

| Path | What it is |
| --- | --- |
| [`src/lib.rs`](src/lib.rs) | the plugin, with its unit tests |
| [`src/main.rs`](src/main.rs) | the Router binary with the plugin |
| [`tests/router_pipeline.rs`](tests/router_pipeline.rs) | integration tests in the Router pipeline (`TestHarness`) |
| [`examples/router/`](examples/router/) | Router configs with the plugin (passthrough and callback), used by the tests |
| [`examples/custom-router/`](examples/custom-router/) | a custom Router project that depends on the plugin, with a minimal config |
| [`docs/router-upgrades.md`](docs/router-upgrades.md) | the plugin and Apollo Router versions |
| [`testbed/`](testbed/) | **auxiliary:** subgraphs, client, emulator and scripts that test and measure the plugin |

`testbed/` is the POC that motivated the plugin (live scoreboard and statistics on
federated subgraphs) and the bench used to test it; see the
[testbed README](testbed/README.md).

## How it works

- **Where it hooks in:** the `supergraph_service`. A subscription stream starts
  with a marker (`subscribed: true`, no data) that the router service consumes to
  open the multipart response and does not send. So the retained frame goes
  **after** the marker, before the live events.
- **Key:** document + `operationName` + variables + the headers listed in
  `identity_headers`. Different tokens never share a retained frame.
- **What it keeps:** only events with data and without errors.
- **Lifecycle:** the entry exists while there is a subscriber with that key. When
  the last stream ends or is dropped, the snapshot goes away together with the
  upstream.
- **Marking:** the snapshot is sent with `extensions.snapshot: true`. The client
  does not need it (it keeps the highest version); the tests use it to measure.
- **Correctness:** relies on every frame carrying the full state and a version. A
  retained frame older than one in flight is ignored by the client (see the
  [design](testbed/docs/design.md)).

As Apollo recommends, the plugin is registered with a group name
(`register_plugin!("globo", "subscription_snapshot", …)`), so the key under
`plugins:` is `globo.subscription_snapshot`. The group avoids collisions with
Apollo's plugins and with other custom plugins in the same binary.

## Build and test (no GraphOS key needed)

Requires Rust 1.97.1 (apollo-router 2.18.0's minimum, pinned in
`rust-toolchain.toml`; rustup installs it automatically) and `protoc`
(`apt-get install protobuf-compiler libprotobuf-dev`). The first Router build
takes ~20 min.

```bash
cargo fmt --all --check
cargo clippy --release --all-targets --no-deps -- -D warnings
cargo test --release          # unit tests (the integration ones are ignored)
cargo build --release         # target/release/router
./target/release/router config validate examples/router/router-plugin.yaml
docker build -t apollo-router-subscription-snapshot .
```

The integration tests need the testbed subgraphs:

```bash
cd testbed && npm ci && npm run test:router-harness
```

This starts the Node subgraphs (periodic 3 s frame) and runs
`tests/router_pipeline.rs` with the crate's `TestHarness`: the real router
service, deduplication, `graphql_ws` passthrough and multipart, with the plugin
loaded from the config. `TestHarness` uses an unrestricted license only in tests;
it does not serve traffic and does not replace running the binary with GraphOS
([tests with the real Router](testbed/README.md#the-real-apollo-router-with-the-plugin)).

## Conventions

The crate follows the [native plugin examples](https://github.com/apollographql/router/tree/dev/examples)
and the lint rules of the Apollo Router repository:

- A lib with the plugin and a binary that only links it (`use subscription_snapshot as _;`
  is enough for `register_plugin!` to take effect), with the integration tests in `tests/`.
- `Plugin` with `type Config` (`Deserialize` + `JsonSchema`, `deny_unknown_fields`;
  the doc comments become the description in the config's JSON schema) and the hook
  built with `ServiceBuilder` and the layers from `apollo_router::layers`.
- Edition 2024 and the same `rustfmt.toml` (`style_edition = "2024"`), with one
  import per line, grouped into std, external crates and the crate itself.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo doc`
  with `RUSTDOCFLAGS=-D warnings`, like Apollo's `xtask lint`; CI runs all three.
- Hook tests with `tower_test::mock::pair` (the replacement Apollo points to for
  the deprecated `Mock*Service`) and registration tests with `TestHarness`.
- English everywhere: code, comments, messages, docs, file names, tests and commits.

## Support and upgrades

Apollo does not support native plugins or custom binaries. Every new Router
version requires rebuilding and running `npm run test:router-harness`.
[`docs/router-upgrades.md`](docs/router-upgrades.md) compares 20 versions
(1.61 to 2.18) and shows what tends to break:
- **The plugin code:** rarely. It compiled and passed every test on 2.18.0
  without changes.
- **Around it:** often. The Rust version, the Router YAML config (which cannot
  serve two versions at once) and dependencies such as `schemars`.

The guide also has the step-by-step upgrade procedure.

## License

[Apache License 2.0](LICENSE). The plugin links the Apollo Router, which is under the
[Elastic License 2.0](https://github.com/apollographql/router/blob/dev/LICENSE); a
Router binary built with it is subject to that license as well.
