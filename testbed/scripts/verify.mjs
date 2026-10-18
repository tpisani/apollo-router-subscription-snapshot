import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { createSync, statsFromWire, runSync } from '../client/sync.mjs';
import { graphql, snapshotApi, transports } from '../client/transport.mjs';
import { playGenerator } from '../subgraphs/sports.mjs';

async function until(predicate, message, ms = 10_000) {
  for (const end = Date.now() + ms; Date.now() < end; await delay(10)) if (await predicate()) return;
  throw new Error(`Timeout: ${message}`);
}

// End-to-end scenario against any endpoint (emulated edge or Apollo Router):
// two channels per match, stale caches, late joiners on deduplicated streams,
// a stats burst with a goal in the middle, resuming without a query and exact convergence.
//   metrics(): the subgraphs' metrics (/metrics of scores and stats).
export async function verify({ url, transport = 'multipart', metrics, headers = {}, expectDedup = true, expectStaleCache = true, log = () => {} }) {
  const matchId = `verify-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
  const subscribe = transports[transport];
  const next = playGenerator(matchId, 5);
  const clients = [];
  const goalLatencies = []; const statsLatencies = [];
  let scoreVersion = 0, statsVersion = 0;
  const mutate = async (query, variables, field) => (await graphql(url, query, { matchId, ...variables }))[field];
  const startPeriod = async period => { scoreVersion = (await mutate('mutation($matchId: ID!, $p: Int!) { startPeriod(matchId: $matchId, period: $p) { version } }', { p: period }, 'startPeriod')).version; };
  const goal = async side => { scoreVersion = (await mutate('mutation($matchId: ID!, $s: Side!) { scoreGoal(matchId: $matchId, side: $s) { version } }', { s: side }, 'scoreGoal')).version; };
  const plays = async n => { statsVersion = await mutate('mutation($matchId: ID!, $e: [StatEventInput!]!) { recordStats(matchId: $matchId, events: $e) }', { e: Array.from({ length: n }, next) }, 'recordStats'); };

  // A client may subscribe to one channel or both; each channel is a subscription.
  const connect = (name, channels, existing = {}) => {
    const controller = new AbortController();
    const client = { name, syncs: {}, bytes: { scoreboard: 0, stats: 0 }, errors: [], runs: [] };
    for (const channel of channels) {
      const sync = existing[channel] ?? createSync(channel, { matchId, api: snapshotApi(url, channel, matchId, { headers }), });
      client.syncs[channel] = sync;
      client.runs.push(runSync({ sync, signal: controller.signal, wait: () => 100,
        connect: async function* (signal) {
          for await (const frame of subscribe(url, channel, matchId, { signal, headers, onBytes: n => { client.bytes[channel] += n; } })) {
            if (frame.committedAt) (channel === 'scoreboard' ? goalLatencies : statsLatencies).push(Date.now() - frame.committedAt);
            yield frame;
          }
        },
        onError: error => { client.errors.push(error); log(`${name}/${channel}: reconnecting (${error.message})`); } }));
    }
    client.close = async () => { controller.abort(); await Promise.all(client.runs); };
    clients.push(client); return client;
  };
  const synced = (client, channel, version) => client.syncs[channel].synced && client.syncs[channel].version >= version;
  const truth = async () => ({
    scoreboard: (await graphql(url, `query($m: ID!, $v: Int!) { scoreboard(matchId: $m, minVersion: $v) { version state { home away status } } }`, { m: matchId, v: scoreVersion })).scoreboard,
    stats: await snapshotApi(url, 'stats', matchId).snapshot(statsVersion)
  });

  try {
    // 1. Match starting; warms up the shared keys and lets them go stale.
    await startPeriod(1); await plays(30);
    const warm = { score: await snapshotApi(url, 'scoreboard', matchId).snapshot(0), stats: await snapshotApi(url, 'stats', matchId).snapshot(0) };
    await goal('HOME'); await plays(40);
    if (expectStaleCache) {
      assert.equal((await snapshotApi(url, 'scoreboard', matchId).snapshot(0)).version, warm.score.version, 'the shared scoreboard should come from the cache, stale');
      assert.equal((await snapshotApi(url, 'stats', matchId).snapshot(0)).version, warm.stats.version, 'the shared stats should come from the cache, stale');
    }

    // 2. Different screens: A shows everything; B is a match list (scoreboard only).
    const a = connect('A', ['scoreboard', 'stats']);
    const b = connect('B', ['scoreboard']);
    await until(() => synced(a, 'scoreboard', scoreVersion) && synced(a, 'stats', statsVersion) && synced(b, 'scoreboard', scoreVersion), 'A and B synced');
    assert.equal(a.syncs.scoreboard.state.home, 1);
    for (const channel of ['scoreboard', 'stats']) {
      assert.ok(a.syncs[channel].stats.snapshots <= 1, `${channel}: only the provisional base; the first frame already carries the state`);
    }
    log(`A stats: ${JSON.stringify(a.syncs.stats.stats)}`);

    // 3. Stats late joiner on an already deduplicated stream.
    const c = connect('C', ['stats']);
    await until(() => synced(c, 'stats', statsVersion), 'C synced');
    if (expectDedup && metrics) {
      await delay(300);
      const m = await metrics();
      assert.equal(m.scoreboard.activeStreams, 1, `one scoreboard stream on the subgraph for 2 clients (got ${m.scoreboard.activeStreams})`);
      assert.equal(m.stats.activeStreams, 1, `one stats stream on the subgraph for 2 clients (got ${m.stats.activeStreams})`);
    }

    // 4. Stats burst (600 plays in ~1.5 s) with a goal in the middle.
    goalLatencies.length = 0;
    const framesBefore = a.syncs.stats.stats.frames;
    const storm = (async () => { for (let i = 0; i < 30; i++) { await plays(20); await delay(50); } })();
    await delay(700); await goal('AWAY'); await storm;
    await until(() => [a, b].every(x => x.syncs.scoreboard.version === scoreVersion), 'goal on the scoreboard clients');
    await until(() => [a, c].every(x => synced(x, 'stats', statsVersion)), 'stats converge after the burst');
    const statsFrames = a.syncs.stats.stats.frames - framesBefore;
    assert.ok(statsFrames < 30, `600 plays become few frames (${statsFrames}), not 600`);
    assert.equal(b.bytes.stats, 0, 'a scoreboard-only client gets no stats');

    // 5. Resuming after a drop: the state is kept and the next frame solves it, without a query.
    await a.close();
    await plays(50); await goal('HOME');
    const snapshots = a.syncs.stats.stats.snapshots + a.syncs.scoreboard.stats.snapshots;
    const a2 = connect('A (resumed)', ['scoreboard', 'stats'], a.syncs);
    await until(() => synced(a2, 'stats', statsVersion) && synced(a2, 'scoreboard', scoreVersion), 'A resumed');
    assert.equal(a.syncs.stats.stats.snapshots + a.syncs.scoreboard.stats.snapshots, snapshots, 'resumed without any query');

    // 6. Everyone equal to the database (queried with minVersion = last written version).
    const live = clients.filter(x => x !== a);
    await until(() => live.every(x => Object.entries(x.syncs).every(([ch, sync]) => sync.version === (ch === 'scoreboard' ? scoreVersion : statsVersion))), 'everyone on the latest version');
    const t = await truth();
    for (const client of live) {
      if (client.syncs.scoreboard) assert.deepEqual([client.syncs.scoreboard.version, client.syncs.scoreboard.state.home, client.syncs.scoreboard.state.away], [t.scoreboard.version, t.scoreboard.state.home, t.scoreboard.state.away], client.name);
      if (client.syncs.stats) {
        assert.equal(client.syncs.stats.version, t.stats.version, client.name);
        assert.deepEqual(client.syncs.stats.state, statsFromWire(t.stats.state), `${client.name} differs from the database`);
      }
    }
    const pct = (values, p) => values.sort((x, y) => x - y)[Math.min(values.length - 1, Math.floor(values.length * p))];
    return {
      matchId, versions: { scoreboard: scoreVersion, stats: statsVersion },
      goalLatencyMs: { p50: pct(goalLatencies, 0.5), max: pct(goalLatencies, 1), n: goalLatencies.length },
      statsLatencyMs: { p50: pct(statsLatencies, 0.5), p95: pct(statsLatencies, 0.95), n: statsLatencies.length },
      bytes: Object.fromEntries(clients.filter(x => x !== a).map(x => [x.name, x.bytes])),
      stormStatsFrames: statsFrames
    };
  } finally {
    await Promise.all(clients.map(x => x.close()));
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  // Against a licensed Apollo Router (docker compose --profile apollo up) or the edge.
  // Use an instance with no other clients on the same match.
  const url = process.env.GRAPHQL_URL ?? 'http://localhost:4100/graphql';
  const scoresMetrics = process.env.SCORES_METRICS_URL ?? 'http://localhost:4001/metrics';
  const statsMetrics = process.env.STATS_METRICS_URL ?? 'http://localhost:4003/metrics';
  const result = await verify({
    url, transport: process.env.TRANSPORT ?? 'multipart',
    metrics: async () => ({ scoreboard: (await (await fetch(scoresMetrics)).json()).scoreboard, stats: (await (await fetch(statsMetrics)).json()).stats }),
    expectDedup: process.env.EXPECT_DEDUP !== '0', expectStaleCache: process.env.EXPECT_CACHE !== '0', log: console.log
  });
  console.log(JSON.stringify(result, null, 2));
  console.log('OK: stale caches, deduplicated late joiners, burst isolated from the scoreboard, resume and convergence'
);
}
