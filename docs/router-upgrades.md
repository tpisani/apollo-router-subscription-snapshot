# The plugin and Apollo Router versions

Question: how often does a new Router version break `subscription_snapshot`?

Method (Oct 2026):
- **Static comparison:** the published source code of 20 versions (1.61.13 and
  the latest patch of each minor from 2.0 to 2.18.0), checking every point the
  plugin depends on.
- **Hands-on test:** the plugin, unchanged, built against 2.18.0 and tested
  (unit, Router pipeline and the licensed Router with 3 instances).

## Release cadence

- A new minor every ~4 weeks: 2.10 (Dec 2025) to 2.18 (Sep 2026), eight minors
  in ten months.
- 2.10 still gets patches (2.10.5 in Jul 2026). This POC used 2.10.5 until it
  was upgraded to 2.18.0 (the latest stable in Oct 2026).
- The minimum Rust version went up 8 times between 2.0 and 2.18 (2.18 requires 1.97.1).

## What changed, point by point

| Point the plugin depends on | Changed in 2.x? | Effect on the plugin |
| --- | --- | --- |
| `trait Plugin`, `PluginInit`, `supergraph::Response::map`, `map_future_with_request_data` | no (same since 1.61) | none |
| `graphql::Response` (`subscribed`, `extensions`, `data`, `errors`) | no | none |
| `register_plugin!` | 2.0 and 2.1 (internally) | none in usage |
| Execution service marks the 1st item (`first.subscribed`) | no | none |
| Router service consumes the 1st item and opens the multipart with the rest only | code touched in 2.2.1, 2.4.0 and 2.14.2; **the rule is still the same** | none (checked on 2.18) |
| `apollo::supergraph::operation_kind` key in the context | renamed in 2.0 (it was `operation_kind` in 1.x) | on 1.x the plugin would not recognize subscriptions and would stop retaining, **with no error** |
| `schemars` dependency (the plugin config's `JsonSchema`) | 0.8 → 1.0 in 2.7.0 | **does not compile** until the version in `Cargo.toml` is changed |
| Minimum Rust version | 8 times | the build fails until the toolchain is updated (happened here: 1.97.0 → 1.97.1 for 2.18) |
| Multipart | large in 2.6.2 and later (end-reason metrics) | none |

**Outside the plugin, but in this POC's files** (the Router YAML config):

| Version | Change | Effect |
| --- | --- | --- |
| 2.3.0 | `subscription.enable_deduplication` → `subscription.deduplication.enabled` | old config invalid |
| 2.15.0 | `deduplication.enabled` → `deduplication.all.enabled` | the Router **refuses to start** with the old config |
| 2.18.0 | `headers.all.request` now requires `operations:` | old config invalid |

`router config upgrade` rewrites all three by itself. But the config migrated to
2.18 is rejected by 2.10.5, so **no single config works for both versions**: it
has to follow the binary.

## Hands-on test results on 2.18.0

- **Plugin:** compiled without changing a single line (only Rust 1.97.1).
- **Tests:** the 8 unit tests and the 4 in the Router pipeline passed. With
  retention, the late-joining client gets the state in 1 ms; without it, in
  2,799 ms, same as 2.10.5.
- **Config:** `router config validate` rejected both POC configs (the
  `headers`). After `router config upgrade`, they passed.
- **Licensed Router** (3 instances + 1 without the plugin, passthrough and callback):
  - every item of the script passed, including the lifecycle and the instance
    failure (143 ms);
  - the duplicated upstream from the Router's deduplication showed up again, this
    time **in callback mode as well**; it still does not affect correctness.

## Conclusion

- **The plugin code rarely breaks.** The plugin API and the shape of the
  subscription stream stayed stable across the whole 2.x line. The only compile
  break in the plugin itself was the `schemars` switch (2.7.0); the rest changed
  only in major versions (1.x → 2.0).
- **What breaks often is around it:**
  - the Rust version, several times a year;
  - the Router YAML config, 3 times since 2.3, twice preventing the Router from
    starting;
  - transitive dependencies, rarely.

  All of this is loud: it fails at build or startup, not in production.
- **The silent risk is the stream's internal contract** (the marker on the 1st
  item and the `operation_kind` key). It is neither documented nor guaranteed by
  Apollo. If it changes, the plugin may stop retaining, as would happen on 1.x, or
  lose the frame, as in the test with the reversed order, with no compile error.
  It did not change in 2.x, and `npm run test:router-harness` exists to catch it.

## The upgrade to 2.18.0

Done as described below, with this outcome:
- **Plugin code:** no changes.
- **Dependency and toolchain:** `apollo-router = "=2.18.0"` and a new `Cargo.lock`;
  Rust pinned to 1.97.1 (`rust-toolchain.toml` and
  `rust:1.97.1-bookworm` in the Dockerfile).
- **Configs:** `router.yaml`, `router-plugin.yaml` and `router-plugin-callback.yaml`
  migrated by hand, to keep the comments: `deduplication.all.enabled` and
  `headers.all.request.operations`. With that, `router config upgrade` has nothing
  left to migrate. The official image for the `apollo` profile also moved to v2.18.0.
- **Tests:** the same as before, all passing (see [validation](../testbed/docs/validation.md)).

## How to upgrade the Router

1. **Change the version:** `apollo-router = "=X.Y.Z"` in `Cargo.toml`.
   Check the minimum Rust version (the crate's `rust-version`) and the one in the
   `Dockerfile`.
2. **Build:** `cargo build --release`. If it breaks, the most likely cause is a
   dependency such as `schemars`; align it with the Router's.
3. **Migrate the config:** `./target/release/router config upgrade examples/router/router-plugin.yaml`
   (and the callback one), review the result and validate it with `config validate`.
4. **Test in the Router pipeline:** `npm run test:router-harness` (in `testbed/`). This
   is what catches a change in the stream contract.
5. **With the GraphOS key:** `npm run test:router-real` and
   `MODE=callback npm run test:router-real` (in `testbed/`).

To test a new version without touching the plugin directory:

```bash
# e.g. from 2.18.0 to X.Y.Z, with its minimum Rust (R); from the repository root
mkdir /tmp/plugin-new && cp -r Cargo.toml Cargo.lock src tests /tmp/plugin-new/
sed -i 's/=2.18.0/=X.Y.Z/' /tmp/plugin-new/Cargo.toml
(cd testbed && PLUGIN_DIR=/tmp/plugin-new CARGO_TOOLCHAIN=R npm run test:router-harness)
(cd /tmp/plugin-new && cargo +R build --release)
mkdir -p /tmp/cfg && /tmp/plugin-new/target/release/router config upgrade examples/router/router-plugin.yaml > /tmp/cfg/router-plugin.yaml   # same for callback
(cd testbed && ROUTER_BIN=/tmp/plugin-new/target/release/router ROUTER_CONFIG_DIR=/tmp/cfg npm run test:router-real)
```

Note: `config upgrade` writes its warnings to the same stdout as the YAML; remove
the lines starting with `{"timestamp"` before using it.
