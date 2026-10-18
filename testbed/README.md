# Testbed: real-time federated GraphQL (scoreboard and stats)

> **Auxiliary directory.** The product of this repository is the
> [`subscription_snapshot`](../README.md) plugin, at the root. This is the POC that
> motivated it and the bench that tests and measures it: subgraphs, client, Router
> emulator and scripts. Every `npm` command below runs inside `testbed/`.

A POC of live sports data with federated GraphQL: scoreboard and stats in
**separate subgraphs and subscriptions**, each with its own sending policy,
compatible with the Apollo Router's subscription deduplication and with caching on
every query. It runs without a license with Hive Gateway and an edge that emulates
the Router's deduplication and cache.

- **Scoreboard:** every change goes out right away, with the full state (~150 B).
  A goal arrives in ~10 ms.
- **Stats:** also with the full state, with a limiter: right away when the match
  is quiet, and at most 2 frames/s in a burst.
- **A single rule on the client:** keep the frame with the highest version. There
  is no gap, history or recovery query.
- **Retention of the last frame**, in a native Apollo Router plugin
  ([`subscription_snapshot`](../README.md), at the root) and in the emulator: a
  client joining an already open upstream gets the state right away (1 ms in the
  Router pipeline, versus ~2.8 s waiting for the periodic frame).
- **Several instances:** each one deduplicates and retains its own upstreams; an
  instance that goes down is absorbed by the others in ~150 ms, with no version
  regression.
- **A match list subscribes to the scoreboard only** and gets no stats bytes at
  all.

- **Lifecycle:** a scheduled or finished match uses the query only; the
  subscription starts 15 min before kickoff and ends at `FULL_TIME`. The status
  cache never delays the start ([page](docs/scenarios/status-and-kickoff.html)).

Interactive step-by-step scenarios: [`docs/scenarios/`](docs/scenarios/).
Why two channels, full state, retention and several instances, with the experiments:
[`docs/design.md`](docs/design.md). What was run:
[`docs/validation.md`](docs/validation.md).

## Start here

- **Per-channel sending policies (hub, limiter, periodic frame, queue):** [`subgraphs/channels.mjs`](subgraphs/channels.mjs)
- **Client (`StateSync`, the same on both channels):** [`client/sync.mjs`](client/sync.mjs)
- **The `subscription_snapshot` Router plugin (Rust):** [`../src/lib.rs`](../src/lib.rs)
- **Retention, identity in the key and cache in the emulator:** [`emulator/edge.mjs`](emulator/edge.mjs)
- **Several instances behind a balancer:** [`emulator/balancer.mjs`](emulator/balancer.mjs), [`tests/scale.test.mjs`](tests/scale.test.mjs)
- **Domain (scoreboard, clock, stats):** [`subgraphs/sports.mjs`](subgraphs/sports.mjs)
- **Schemas of the `scores`, `stats` and `catalog` subgraphs:** [`subgraphs/schemas.mjs`](subgraphs/schemas.mjs)
- **Design experiments:** [`tests/experiments.test.mjs`](tests/experiments.test.mjs), [`scripts/experiments.mjs`](scripts/experiments.mjs)

## Operations

```graphql
# Scoreboard: same variables for everyone → deduplicable
subscription Scoreboard($matchId: ID!) {
  scoreboardUpdates(matchId: $matchId) { version committedAt state { status home away clock { running seconds updatedAt } } }
}
# Stats: full state, at most 1 frame every 500 ms in a burst
subscription Stats($matchId: ID!) {
  statsUpdates(matchId: $matchId) { version committedAt state { home { possession shots } away { possession shots } } }
}
# Cacheable snapshots: "a state at some version >= minVersion"
query { scoreboard(matchId: "m1", minVersion: 0) { version state { home away } } }
query { matchStats(matchId: "m1", minVersion: 812) { version state { home { possession } } } }
# Static data (1 h cache)
query { match(id: "m1") { home { name } away { name } players { id name number } } }
```

## Requirements

Node.js 22.13+ (uses `node:sqlite`; CI uses Node 24). Docker is optional. The
Apollo Router requires `APOLLO_KEY`/`APOLLO_GRAPH_REF` with a license for
federated subscriptions and response cache.

## Run without a license (Hive Gateway + edge)

```bash
npm ci
npm run compose-schema   # generates gateway/supergraph.graphql
SIMULATE=m1 npm start    # subgraphs scores :4001, catalog :4002, stats :4003, simulating match m1
npm run gateway          # Hive Gateway :4000
npm run edge             # dedup + retention + query cache (worst case) :4010, multipart like the Router
```

In another terminal:

```bash
npm run client                         # match screen: scoreboard + stats
CHANNELS=scoreboard npm run client     # match list: scoreboard only
```

Without `SIMULATE`, use `npm run demo` to generate plays and goals through the mutations.
Metrics: `curl localhost:4001/metrics` (streams, frames, snapshots) and
`curl localhost:4010/metrics` (upstreams per operation, cache).

With Docker: `docker compose up --build -d` starts everything, with match m1 simulated.

## Run with the Apollo Router

```bash
cp .env.example .env     # fill in APOLLO_KEY and APOLLO_GRAPH_REF
docker compose --profile apollo up --build -d   # Router on :4100
GRAPHQL_URL=http://localhost:4100/graphql npm run client
npm run test:router      # the same scenario as the tests, against the Router
```

### The real Apollo Router with the plugin

The binary is the crate at the repository root (`cargo build --release` there, or
`test:router-real` builds it by itself the first time). The configs live in
[`../examples/router/`](../examples/router/).

```bash
npm run test:router-harness   # no license: the plugin in the Router pipeline (TestHarness)
npm run test:router-real      # with APOLLO_KEY/APOLLO_GRAPH_REF: 3 routers + balancer on the host
MODE=callback npm run test:router-real   # the same with the subgraphs in callback mode
# or, on a local machine, through compose:
docker compose --profile apollo-plugin up --build -d && npm run test:router-scale
# with an instance taken down mid-match:
KILL_CMD="docker compose --profile apollo-plugin stop router-b" npm run test:router-scale
```

The Router refuses federated subscriptions without a GraphOS key
("license violation"); the free plan includes federated subscriptions. Removing
that check would violate the license (ELv2), so the real test needs the key:

1. Create a graph in GraphOS and generate a key.
2. Set `APOLLO_KEY` and `APOLLO_GRAPH_REF` as environment variables, never in the
   repository. In a Claude Code on the web session: environment menu → Edit →
   environment variables; a new session gets them. Locally: `.env` (outside
   git) or `export`.

`test:router-real` starts the Node subgraphs, 3 routers with the plugin (4101–4103),
1 without retention (4104) and the balancer (4200), runs
`scripts/verify-router-scale.mjs` with an instance taken down mid-match (`KILL=0`
turns it off) and shuts everything down. It is the way to go in a cloud session:
the processes inherit the session's proxy and CA, and Docker containers do not;
inside them the Router cannot reach GraphOS and keeps trying to fetch the license,
without opening the port.

**Callback mode:** `router-plugin-callback.yaml` swaps passthrough for the Router's
callback mode (`/callback` on the same port); the Node subgraphs implement the
callback/1.0 protocol (`subgraphs/app.mjs`). The plugin acts on the stream that
goes to the client, so it behaves the same in both modes.

**Deduplication under concurrency:** `scripts/probe-dedup.mjs` measures how many
upstreams an instance opens when several clients join at the same time
(`MODE`, `CLIENTS`, `TRIALS`, `RETAIN_ENABLED=false`). In passthrough, the Router
sometimes opens one extra, even with the plugin off (see
[validation](docs/validation.md)).

**Free plan limit:** the Router limits requests per instance
(503 `ROUTER_FREE_PLAN_RATE_LIMIT_REACHED`); each subscription counts once, even
if it lasts minutes. That is why the script sends the mutations and the base
queries straight to the subgraphs and only the subscriptions go through the
routers (about 40 per instance in total). If the limit is reached, the script
stops and says so.

The compose `apollo-plugin` profile starts (same ports as `test:router-real`):

| Service | Port | Role |
| --- | --- | --- |
| `router-a`, `router-b`, `router-c` | 4101–4103 | Router with the plugin on |
| `router-plain` | 4104 | the same Router with `RETAIN_ENABLED=false` |
| `balancer` | 4200 | round-robin without affinity in front of a, b and c |
| `subgraphs` | 4001–4003 | `/metrics` with open streams per match |

`scripts/verify-router-scale.mjs` measures through the client and the subgraphs:

1. **Deduplication per instance:** streams on the subgraph = instances × matches ×
   channels.
2. **Late joiner on an idle match:** with the plugin, it gets the retained frame
   right away (< 300 ms); without it, it waits for the periodic frame.
3. **Identity:** different tokens produce different upstreams and do not get each
   other's retained frame.
4. **Live match through the balancer**, optionally with an instance taken down:
   no version goes back and everyone ends on the database state.
5. **Cleanup:** with no subscribers, no stream stays open on the subgraph.

The same script runs in `npm test` against the emulator
(`tests/router-scale.test.mjs`), to make sure the script itself is right.

To make the retention effect more visible, raise the periodic frame:
`SCOREBOARD_PERIODIC_MS=3000 docker compose --profile apollo-plugin up -d`.

What tends to break when upgrading the Router (20 versions compared, plugin tested
on 2.18): [`../docs/router-upgrades.md`](../docs/router-upgrades.md).

## Tests, experiments and benchmark

```bash
npm test                 # 44 tests: domain, channels, client, experiments, end to end and several instances
(cd .. && cargo test --release)   # the plugin's unit tests
npm run bench            # simulated match through the edge: 100 scoreboard-only clients + 50 full ones
EDGES=3 npm run bench    # the same with 3 instances behind a balancer
SCORE_ONLY=0 FULL=200 STORM_RATE=400 npm run bench
```

## Deliberate limits

- No authentication: `matchId` is just a selector.
- In-memory hubs: one process per subgraph. With replicas, feed the hub from a
  bus after the commit.
- Retention in the Apollo Router requires a custom build with a native plugin,
  not supported by Apollo.
- Scoreboard and stats are not transactionally consistent with each other: the
  stats may lag up to ~0.5 s behind the scoreboard (see
  [design](docs/design.md#consistency-across-channels)).
- The plugin ran with the real Router and GraphOS, on 3 instances, in passthrough
  and callback modes; the response cache with Redis has not yet. See [validation](docs/validation.md).
