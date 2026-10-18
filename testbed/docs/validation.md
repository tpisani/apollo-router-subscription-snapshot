# Validation performed

Environment: Linux, Node.js 22.22.0, Hive Gateway 2.15.2, Docker 29.8; apollo-router
**2.18.0** with the plugin (Rust 1.97.1) and the official v2.18.0 image. The
sections below this one were measured with 2.10.5, before the upgrade, and were
redone on 2.18.0 with the same results (next section).

The plugin used to be called `poc.retain_last_frame`, with the `extensions.retainedFrame`
marker, in the `router-plugin/` directory. It was renamed to `subscription_snapshot`,
with the `extensions.snapshot` marker, in the `router-subscription-snapshot/`
directory, without changing its behavior. The results below hold for the same code.
Later, the crate moved to the repository root (lib + bin) and the rest of the POC
to `testbed/`; the paths below are the ones from that time.

## Upgrade to 2.18.0

- **Plugin:** compiles without code changes (`cargo build --release --locked`).
- **Plugin tests:** the 8 unit tests and the 4 in the Router pipeline pass. The
  late-joining client gets the state in 1 ms with retention and in 2,797 ms without.
- **`npm test`:** 44 passed.
- **Configs:** `router config validate` accepts the three migrated configs, both in
  the binary with the plugin and in the official v2.18.0 image (`router.yaml`).
  `router config upgrade` no longer reports anything.
- **The plugin's Docker image** (`rust:1.97.1-bookworm`): builds.
- **Licensed Router** (`npm run test:router-real`, 3 instances + 1 without the plugin):
  passed in both modes.

| | passthrough | callback |
| --- | --- | --- |
| Deduplication (24 clients, 2 matches) | 3 streams per channel and match | 3 |
| Late joiner with the plugin | 5–6 ms | 10–12 ms |
| Late joiner without the plugin | ~2,840 ms¹ | ~2,843 ms |
| Instance taken down | 8 reconnected, 144 ms | 8, 158 ms |
| Lifecycle (scheduled, FULL_TIME with 7 clients, after the end, stale LIVE) | all good, 0 reconnects | same |

¹ In one of the three runs without the plugin the client got the state in 9 ms. It
is the same deviation already recorded in callback mode on 2.10.5, now seen in
passthrough. The cause was not investigated.


## Tests (`npm test`: 44 passed)

- **`tests/domain.test.mjs` (8):**
  - patches and the overlap property (used in the experiments' `delta` mode);
  - a version only when the state changes;
  - atomicity without publishing, and persistence;
  - `SnapshotCache` with `minVersion`;
  - clock derived on the client;
  - stats as final values.
- **`tests/channels.test.mjs` (10):**
  - immediate `STATE`, periodic, and only the newest for a slow consumer;
  - **limiter:** an isolated play goes out right away; 100 commits in ~100 ms
    become at most 3 frames, with the final state; `committedAt` is that of the
    oldest commit not yet sent;
  - releasing listeners and timers;
  - `delta` mode, used in the comparison experiments.
- **`tests/sync.test.mjs` (6):**
  - late joiner right on the first frame;
  - stats converted from the API format;
  - repeated, late, out-of-order or older retained frames ignored;
  - provisional render;
  - retry after a failed base fetch;
  - **property test:** 25 seeds, 3 instances with retained frames of different
    ages, clients switching instances, lost, duplicated and reordered frames. The
    version never goes back and everyone ends on the exact state. Injecting a bug
    (accepting an older frame) makes it fail on seed 1.
- **`tests/experiments.test.mjs` (2):** bandwidth and isolation of the five stats
  policies (numbers in `docs/design.md`).
- **`tests/e2e.test.mjs` (4), with the real Hive Gateway process:**
  - full scenario (`scripts/verify.mjs`) through the edge: stale caches, one stream
    per channel, a burst with a goal in the middle, a scoreboard-only client with 0 B
    of stats, **resuming without any query** and everyone equal to the database;
  - the same scenario directly on Hive;
  - upstreams per channel and match;
  - subgraph restart.
- **`tests/scale.test.mjs` (5), with 3 edges behind a balancer:**

| Test | Result |
| --- | --- |
| 30 clients, 2 matches, 2 channels | 6 streams per channel on the subgraph (3 instances × 2 matches); at most 1 upstream per match on each instance |
| Late joiner on an idle match (periodic 2 s) | **12 ms with retention**, 1,997 ms without |
| Identity in the key (`authorization`) | different tokens: 2 upstreams; only the client with the same token got the retained frame |
| Instance taken down mid-match (12 clients, continuous plays and goals) | 8 clients moved through the balancer; everyone synced again in **145–163 ms** (3 runs); no version went back; exact final state |
| Cleanup | with no subscribers, no instance keeps an upstream or a retained frame; subgraph without streams |

- **`tests/router-scale.test.mjs` (1):** the `scripts/verify-router-scale.mjs` script,
  written for the Routers with the plugin, against the emulator (3 edges + 1
  without retention), to prove the script itself before running it with the
  license: 6 streams per channel for 24 clients (3 instances × 2 matches); late
  joiner in 4–7 ms with a retained frame versus ~1,850 ms without; 2 upstreams for
  2 tokens; 12 clients with an instance taken down, 8 moved and everyone was back
  in 146 ms, without regression; cleanup.

## The real Apollo Router with GraphOS (`npm run test:router-real`)

The binary from `router-subscription-snapshot/` (2.10.5 + plugin) with a GraphOS key
(free plan, test-only graph): 3 instances with retention behind the balancer and 1
without, against the Node subgraphs, periodic 3 s frame. Clients over HTTP multipart
(the Router's only subscription transport for clients). Only the subscriptions
went through the routers; none hit the free plan limit. It ran in both
subscription modes between Router and subgraphs:

| Check | passthrough (WebSocket) | callback (HTTP, `MODE=callback`) |
| --- | --- | --- |
| Deduplication: 24 clients, 2 matches | 3 streams per channel and match (one per instance)¹ | 3 streams per channel and match |
| Late joiner with the plugin | **4–7 ms**, retained frame in all 3 runs | **4–10 ms**, retained frame in all 3 runs |
| Late joiner without the plugin | **2,834–2,839 ms** (periodic) | **2,837–2,847 ms** (periodic) |
| Identity | 2 streams for 2 tokens; only the same token gets the retained frame | same |
| Instance taken down (SIGKILL) with 12 clients | 8 reconnected; everyone back in **142 ms**; no regression; state equal to the database | 8 reconnected; **143 ms**; same |
| Cleanup | no streams on the subgraph | no streams on the subgraph (the subgraph stops on the Router's 404) |

**Lifecycle on the real Router** (step 5 of the script, 2 s kickoff window; same
result in both modes):

| Check | passthrough | callback |
| --- | --- | --- |
| Finished match | `over` phase, 0 subscriptions | same |
| Scheduled | 0 streams before the window; subscribed 23 ms after it; kickoff through the stream | 0 streams; 25 ms |
| `FULL_TIME` with 7 clients deduplicated through the balancer | 1 stream per channel on the subgraph; all ended in 9 ms; 0 reconnects; upstreams closed | 13 ms; 0 reconnects |
| Subscription after the end | only the subgraph's `FULL_TIME` frame, 0 retained frames, and the stream ends | same |
| Stale query saying `LIVE` (cache simulated on the client) | phases `live` → `over` in 9 ms, 0 reconnects | 16 ms |

**Two out-of-pattern measurements, once each, in callback mode** (6 runs of the
script in that mode): in one of them recovery after taking an instance down took
5,005 ms (140–144 ms in the others; 5 s is the client's watchdog for a stalled
stream), and a late joiner **without** the plugin got the state in 10 ms instead of
~2.8 s. They did not repeat in two runs in a row. The cause was not investigated;
the 10 ms matches the duplicated upstream from deduplication (which delivers the
initial frame right away), but the script does not measure upstreams in that step.

¹ **Router deduplication under concurrency (passthrough):** in one of the runs an
instance opened 2 upstreams for the same match (4 instead of 3). The probe
`scripts/probe-dedup.mjs` (8 simultaneous clients on one instance) reproduced it in
1 of 6 rounds, and in 2 of 12 **with the plugin off**, so it comes from the Router;
the extra upstream stays open. In callback mode, 0 of 6. It does not affect
correctness (both upstreams deliver the same frames and the client keeps the
highest version), only efficiency. The script records `extraUpstreams` and only
fails if there is no deduplication.

- **`tests/lifecycle.test.mjs` (7), 3 runs in a row:** the match lifecycle
  through the edge, with the worst-case query cache (never expires) and a 1 s
  window:
  - rules with a fixed clock: phase by time and status; the `SCHEDULED` TTL capped
    by the window, checked as a property at every instant before it;
  - the subgraph's real `Cache-Control`: 300 s when far, ≤ the time to the window
    when close, 2 s in the window, 3600 s at `FULL_TIME`;
  - finished: 1 query per channel, 0 subscriptions;
  - scheduled with a stale `SCHEDULED` in the cache: 0 streams before the window,
    subscribed less than 500 ms after it; a delayed match keeps subscribing;
    `FULL_TIME` closes both channels without reconnecting;
  - stale cache saying `LIVE`: the first frame brings `FULL_TIME` and everything
    closes in less than 1 s;
  - 5 deduplicated clients: `FULL_TIME` ends all of them and the upstream closes.

  **Mutations checked:** removing the `SCHEDULED` TTL cap fails 2 tests;
  deciding by status only (ignoring the time) fails 4, including the stale cache one.
- **`tests/callback.test.mjs` (1):** the callback/1.0 protocol on the subgraph
  side, against a fake `/callback`: `check` before answering `{ data: null }`,
  frames as `next`, and a 404 from the Router closes the stream without
  `complete`. This test found a wrong `complete` after the 404, since fixed.

## Router plugin (repository root, `globo.subscription_snapshot`)

- **`cargo test --release` (8):** includes releasing at the end of the stream; the
  marker passes through untouched; the first subscriber gets no retained frame and
  feeds the store; the late joiner gets the retained frame right after the marker,
  with `subscribed` open, and then the live events; different keys do not share;
  errors and the final item are not retained; the entry goes away when the last
  subscriber leaves; responses that are not an open subscription pass through
  untouched; the key includes variables and identity.
- **`npm run test:router-harness` (4):** the fourth test, `FULL_TIME` in the Router
  pipeline: both deduplicated clients get the final frame and the multipart
  closes; the upstream closes; a client arriving later gets the subgraph's final
  state and the stream ends. This test found a problem in the plugin: the retained
  entry only went away when the client was dropped, not when the stream ended, and
  a new client got the frame of an already closed upstream. Fixed (the entry goes
  away on the final item or at the end of the stream), with a unit test.
- **`npm run test:router-harness` (first 3), 3 runs in a row:** the plugin in the
  Router pipeline through the crate's `TestHarness` (router service,
  deduplication, `graphql_ws` passthrough to the Node subgraphs, multipart),
  periodic 3 s frame:

| Test | Result |
| --- | --- |
| Deduplication and retention | 1 stream on the subgraph for 2 clients; late joiner in **1 ms** with `extensions.snapshot`, at the same version as the first; both get the next goal; with no subscribers, the upstream closes |
| Without retention (`enabled: false`) | late joiner in **2,799 ms**, waiting for the periodic frame |
| Identity | different `authorization`: 2 upstreams, and only the client with the same token gets the retained frame |

  **Mutation checked:** inserting the retained frame before the marker makes two
  tests fail; the late joiner gets `{"data":null}` and the frame is lost.
- **Binary:** `cargo build --release` produces `target/release/router` (2.10.5).
  `router config validate router/router-plugin.yaml` passes, with and without
  `RETAIN_ENABLED`; an unknown field in the plugin is rejected by the schema.
- **Without a key**, the binary stops at startup: `license violation ...
  ["Federated subscriptions"]`, as expected. With a deliberately invalid key,
  GraphOS answers `ACCESS_DENIED`: the binary on the host reaches the uplink
  through the session proxy. `npm run test:router-real` fails at that point with
  the Router's message, without leaving processes behind.
- **Docker image** (`router-subscription-snapshot/Dockerfile`, with Rust 1.97 at the
  time): builds, and `config validate` passes inside it. The `apollo-plugin` profile
  starts every service, but here, without a proxy in the containers, the routers
  keep trying to fetch the license (`fetch failed from all 2 uplink endpoints`) and
  do not open the port. That is why the real test in the session is
  `test:router-real`, with the binary on the host.
- **Free plan limit**, read in the Router 2.10.5 code: a per-instance `RateLimit`
  in the router service, 503 `ROUTER_FREE_PLAN_RATE_LIMIT_REACHED`. The script only
  sends the subscriptions through the routers and stops with that message if the
  limit is reached.

## Benchmark (`npm run bench`, 10 s)

100 scoreboard-only clients + 50 with both channels; 20 plays/s with a burst of
200 plays/s; a goal every 2 s; the match already in progress (400 plays before).

| | 1 instance | 3 instances (`EDGES=3`) |
| --- | --- | --- |
| Converged | 150/150 | 150/150 |
| Scoreboard p50 / p99 | 10 / 19 ms | 14 / 21 ms |
| Stats p50 / p99 | 478 / 512 ms | 482 / 505 ms |
| Streams on the subgraph per channel | 1 | 3 |
| Joins served with a retained frame | 169 | 184 (61 + 62 + 61) |
| Snapshots during the match | 0 | 0 |
| Scoreboard-only client | 443 B/s, 0 of stats | 471 B/s, 0 of stats |
| Client with both channels | + 4,441 B/s of stats | + 4,433 B/s |

In the previous version, with the adaptive patch, the full client got ~2,423 B/s
of stats in the same benchmark. The full state costs ~1.8× in bandwidth here.

## Docker and Router

- Compose (subgraphs with the simulator + Hive + edge with retention) with the
  current code: `scripts/verify.mjs` against `localhost:4010` passed, and
  `npm run client` showed live scoreboard and stats, with 3 joins served by a
  retained frame. `RETAIN=0` turns retention off.
- `router config validate router/router.yaml` with the official 2.10.0 image:
  valid.

## Not verified

- **Response cache with Redis** (`--profile apollo`) with the licensed Router.
- **The exact cause of the duplicated upstream** in the Router's passthrough
  deduplication (see above); it would be a case to report to Apollo.
- **The compose `apollo-plugin` profile with the key:** the containers cannot reach
  GraphOS in this session; the same scenario ran with the binaries on the host.
- **A real balancer** (ALB, NGINX, Envoy): the one in the test is a simple
  round-robin with streaming. On a real LB, check the idle timeout (larger than the
  periodic frame) and that multipart responses are not buffered.
- **Subgraph replicas with a bus.**
