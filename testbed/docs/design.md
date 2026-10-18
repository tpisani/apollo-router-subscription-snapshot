# Design: live scoreboard and stats

## Summary

- **Two channels, two subscriptions:** `scoreboardUpdates(matchId)` (subgraph
  `scores`) and `statsUpdates(matchId)` (subgraph `stats`).
- **Every frame carries the full state and a version:** `STATE { version, state }`.
  The client keeps the one with the highest version. There is no gap, history,
  patch or recovery query.
- **Each channel has its own pace.** The scoreboard sends on every commit, right
  away. The stats go through a limiter: right away when the match is quiet, and at
  most 1 frame every 500 ms in a burst. Both channels also send a periodic frame.
- **The subscriptions only take `matchId`:** the Router deduplicates, with one
  upstream per channel and match on each instance.
- **Retention of the last frame** (native Router plugin at the [repository root](../../README.md), and
  the same behavior in the emulator `emulator/edge.mjs`): a client joining an
  already open upstream gets the state right away, instead of waiting for the
  periodic frame.
- **Several instances:** each one deduplicates and retains only its own upstreams,
  and that is enough. Nobody needs to share the last state.

## Why two channels

The experiments (`tests/experiments.test.mjs`) showed that the main reason is not
the obvious one. What protects a goal from a stats burst is **limiting the
frequency** of the stats, not splitting the channels. Splitting is worth it because:

1. **Each screen gets only what it shows.** A match list subscribes to the
   scoreboard only and gets ~300–470 B/s per match. On a single stream it would
   also get the stats.
2. **Each channel has its own policy:** the scoreboard goes out right away, the
   stats have the limiter.
3. **Independent versions, caches and failures.**
4. **Different owners:** they are different subgraphs, and the Router does
   passthrough for both.

| | Scoreboard | Stats |
| --- | --- | --- |
| Changes | a few times per match | ~20 plays/s, bursts of 200/s |
| State size | ~150 B | ~1–2 KB (2 teams + 22 players) |
| Sending | one `STATE` per commit, right away | `STATE` with a 500 ms limiter |
| Periodic | `STATE` every 1 s | `STATE` every 1 s |
| Slow subscriber queue | newest only | newest only |
| Cacheable snapshot (first render only) | `scoreboard(minVersion)`: 2 s | `matchStats(minVersion)`: 5 s |

The clock is not sent every second: `Clock { running, seconds, updatedAt }`,
and the client advances it on its own.

## Why full state, and not patch

The experiments measured five policies for the stats (60 s, 20 plays/s;
goal p95 with 30 goals during a burst on a 16 KB/s link):

| Policy | B/s per client | Goal p95 on the same link |
| --- | --- | --- |
| full state on every play | 26,707 | 74 s |
| patch on every play | 5,866 | 7.7 s |
| patch every 500 ms | 2,135 | 170 ms |
| the smaller one (patch or state) every 500 ms | 2,132 | 80 ms |
| **full state every 500 ms (adopted, with limiter)** | **2,797** | **80 ms** |

**The limiter is essential.** Without it, full state on every play saturates the
link and the goal waits tens of seconds.

**With the limiter, the full state costs a bit more bandwidth than the patch:**
+31% in this experiment, which starts with an empty state. In the benchmark, where
every player is already in the state, it was 4,441 versus 2,423 B/s, almost 2×. In
exchange, the design gets much simpler:

- **No gap and no recovery:** a lost frame is fixed by the next one; there is no
  `from`, `CHECKPOINT` or recovery snapshot.
- **Retention is possible:** the last frame can be retained and resent by whoever
  sits in between (Router, edge). Resending an old patch is useless.
- **A client with a single rule,** the same on both channels (`StateSync`).

If the state grows a lot (dozens of metrics per player, heat map) and the mobile
clients' bandwidth matters, the patch pays off again. The hub keeps the `delta`
mode so this comparison stays reproducible.

### The limiter

The hub, in `state` mode with `throttleMs`:
- **Quiet:** if nothing went out in the last 500 ms, the frame goes out right away
  (an isolated play arrives in ms).
- **Burst:** otherwise, it goes out at the end of the window, with the latest
  state. In a burst, that gives at most 2 frames/s.
- **`committedAt`:** is that of the oldest commit not yet sent, so the measured
  latency is that of the frame's oldest data.

## Client (`StateSync`)

1. Opens the subscription (deduplicated). In parallel, it optionally fetches the
   `minVersion: 0` base, possibly stale, only to draw the screen right away as
   provisional.
2. On each frame: if the version is higher than the local one, it replaces the
   state; otherwise, it ignores it. That covers older retained frames, late
   periodic frames and duplicates.
3. It is synced on the first frame of each connection.
4. Drop or 5 s without frames: reconnects keeping the state. The next frame solves it.

## Retention of the last frame (Router plugin)

The Router has no configuration to resend the last event to whoever joins a
deduplicated upstream:
- **Rhai:** does not keep state across requests.
- **Coprocessor:** does not create a frame when the client connects.

That is why it is a **native Rust plugin** in a custom build
(repository root, `globo.subscription_snapshot`, apollo-router 2.18.0).
`emulator/edge.mjs` does the same in the emulator.

- **Retention:** keeps the last event (with data and without errors) of each key
  and delivers it first to whoever joins. The retained frame goes away when the
  key's last subscriber leaves, together with the upstream.
- **Where the frame goes in:** in the `supergraph_service`, **after** the first
  item of the stream. In a subscription, the first item is a marker without data
  that the router service consumes to open the multipart and does not send. Put
  before it, the retained frame is swallowed and the client gets `{"data":null}`;
  the integration test fails exactly like that when the order is reversed.
- **Identity in the key:** the configured headers (e.g. `authorization`) become
  part of the retention key. In the Router, the same header is propagated to the
  subgraph, so deduplication also splits by identity. Clients with different
  identities share neither the upstream nor the retained frame.
- **Marking:** the retained frame goes out with `extensions.snapshot: true`, used
  only by the tests.
- **Correctness:** it only works because every frame carries the full state and a
  version. A retained frame older than one in flight is ignored by the client.

Measured in the Router pipeline (`npm run test:router-harness`, periodic 3 s
frame): the late joiner got the state in **1 ms with the plugin, versus 2,799 ms
without it**, with a single stream on the subgraph for both clients. With the real
Router and GraphOS, on 3 instances (`npm run test:router-real`), in passthrough
(WebSocket) and callback (HTTP) modes between Router and subgraphs: 4–10 ms versus
~2,840 ms, and ~142 ms for everyone to be back after taking an instance down. The
plugin acts on the stream that goes to the client (always multipart), so it does
not depend on the mode. In the emulator (`tests/scale.test.mjs`, periodic 2 s):
12 ms versus 1,997 ms.

## Several instances

```
clients ──LB──┬── instance A ─ 1 upstream per (channel, match) A has in use ─┐
              ├── instance B ─ same                                          ├── subgraphs
              └── instance C ─ same                                          ┘
```

- **Deduplication per instance:** the streams on the subgraph are **instances ×
  active matches × channels**, not clients. Measured: 30 clients on 2 matches
  behind 3 instances produced 6 streams per channel.
- **Retention per instance, without sharing:** the retained frame exists exactly
  when it is needed. The first client on an instance opens the upstream and gets
  the subgraph's initial frame; a client joining an already open upstream gets the
  frame that instance has already kept.
- **Switching instances (reconnect through the LB):** the new instance may have a
  retained frame slightly older than the client's. The version prevents
  regression; the property test and the failure test assert that.
- **An instance going down:** the clients reconnect through the balancer and land
  on a live instance, which already has the retained frame. Measured: everyone
  synced again in ~150 ms, with no version regression.
- **No session affinity.**
- **What is shared:** the query response cache (Redis, in the Router), since the
  `minVersion` contract holds on any instance. And, if the subgraphs have
  replicas, a bus that takes each commit to the hubs of all of them. The periodic
  frame reads the state from the database and fixes losses on that bus.

## Lifecycle: query or subscription

A match that has not started or is already over does not need a subscription. The
client (`client/lifecycle.mjs`) decides like this (rules in `subgraphs/sports.mjs`):

- **The time decides when to start:** before `kickoff − 15 min`, query only; from
  then on, subscription, whatever status the query returned. A delayed match is
  covered: the client is already subscribed.
- **The status decides when to stop:** after the `FULL_TIME` frame the subgraph
  ends the scoreboard stream; the Router closes every deduplicated client, the
  plugin drops the retention and the client also closes the stats. It does not
  reconnect.
- **Cache by status:** the scoreboard query sends `SCHEDULED` with a `max-age` of
  up to 5 min, never past `kickoff − 15 min` (`no-store` under 1 s); live, 2 s;
  `FULL_TIME`, 1 h. A stale `SCHEDULED` delays nobody, because the clock decides; a
  stale `LIVE` only costs a short subscription, which closes on the first
  `FULL_TIME` frame.
- **Limit:** a match brought forward to before the window without `scheduleMatch`
  is only noticed when the window opens.

Explanatory page with a simulator: [`scenarios/status-and-kickoff.html`](scenarios/status-and-kickoff.html).

## Consistency across channels

Each channel is exact at a version; there is no transaction across channels. The
stats may lag up to ~0.5 s behind the scoreboard. That is why goals live only on
the scoreboard.

## Limits

- **Response cache not run with the licensed Router:** the plugin really ran, on
  3 instances with GraphOS (`npm run test:router-real`, see
  [validation](validation.md)); the response cache with Redis (`--profile apollo`)
  did not.
- **Plugin not supported by Apollo:** it has to be rebuilt for every Router
  version and has to track the internal shape of the subscription stream.
- **Identity:** the edge uses the identity only in the key. In a real Router,
  authorization has to be enforced on the subgraph too.
- **List with many matches:** one channel per competition would reduce the number
  of subscriptions.

## Evolution on this branch

1. `15a917e`: order feed with `after` in the subscription, incompatible with
   deduplication.
2. `0f06630`: shared stream + paginated, cacheable event log.
3. `9a87874`: sports domain, two channels, adaptive patch on the stats.
4. `76ef488`: full state with a limiter on both channels, retention of the last
   frame and several instances behind a balancer (emulated).
5. Current: the retention plugin in Rust, tested in the Router pipeline, and the
   profile with 3 custom routers behind a balancer.
