import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { startStack } from '../emulator/stack.mjs';
import { startEdge } from '../emulator/edge.mjs';
import { createSync, runSync } from '../client/sync.mjs';
import { snapshotApi, transports } from '../client/transport.mjs';
import { scoreboardOps, applyStatEvents, playGenerator } from '../subgraphs/sports.mjs';

// Several instances: 3 independent edges (each with its own deduplication and retention,
// like Router instances) behind a round-robin balancer without affinity.
// Periodic 2 s frame so that the retention effect is visible.
async function until(predicate, message, ms = 10_000) {
  for (const end = Date.now() + ms; Date.now() < end; await delay(10)) if (predicate()) return;
  throw new Error(`Timeout: ${message}`);
}

let stack;
test.before(async () => {
  stack = await startStack({
    edgeCount: 3, edge: { identityHeaders: ['authorization'] },
    scoreboard: { periodicMs: 2000 }, stats: { periodicMs: 2000, throttleMs: 200 }
  });
});
test.after(async () => { await stack?.stop(); });

// A client with one or two channels; records every version seen to prove
// they never go back, even when switching instances.
function client(url, matchId, channels, { headers = {} } = {}) {
  const controller = new AbortController();
  const c = { syncs: {}, seen: {}, errors: [], runs: [], failedAt: {}, frameAt: {} };
  for (const channel of channels) {
    const sync = createSync(channel, { matchId, api: snapshotApi(url, channel, matchId, { headers }) });
    c.syncs[channel] = sync; c.seen[channel] = [];
    c.runs.push(runSync({ sync, signal: controller.signal, wait: () => 100, stallMs: 5000,
      connect: signal => transports.multipart(url, channel, matchId, { signal, headers }),
      onUpdate: view => {
        if (view.version !== null) c.seen[channel].push(view.version);
        if (view.synced) c.frameAt[channel] = Date.now();
      },
      onError: error => { c.errors.push(error); c.failedAt[channel] = Date.now(); } }));
  }
  c.synced = () => Object.values(c.syncs).every(s => s.synced);
  // Every channel got a frame after its last error, i.e. it reconnected and resynced.
  c.recovered = () => c.synced() && channels.every(channel => !(c.failedAt[channel] >= c.frameAt[channel]));
  c.close = async () => { controller.abort(); await Promise.all(c.runs); };
  return c;
}
const monotonic = list => list.every((v, i) => i === 0 || v >= list[i - 1]);
const sum = (edges, op) => edges.reduce((n, e) => n + (e.metrics.upstreamByOperation[op] ?? 0), 0);

test('deduplication per instance: upstreams = instances × matches × channels, not clients', { timeout: 60_000 }, async () => {
  const matches = ['scale-a', 'scale-b'];
  const clients = Array.from({ length: 30 }, (_, i) => client(stack.balancer.url, matches[i % 2], ['scoreboard', 'stats']));
  try {
    await until(() => clients.every(c => c.synced()), '30 clients synced');
    const perEdge = stack.edges.map(e => e.metrics.upstreamByOperation);
    console.log(JSON.stringify({ upstreamsPerInstance: perEdge, subgraphStreams: { scoreboard: stack.subgraphs.metrics.scoreboard.activeStreams, stats: stack.subgraphs.metrics.stats.activeStreams } }));
    for (const ops of perEdge) for (const op of ['Scoreboard', 'Stats']) assert.ok((ops[op] ?? 0) <= matches.length, 'at most 1 upstream per match on each instance');
    assert.ok(perEdge.filter(ops => Object.keys(ops).length).length >= 2, 'the balancer spread the clients');
    assert.equal(stack.subgraphs.metrics.scoreboard.activeStreams, sum(stack.edges, 'Scoreboard'));
    assert.equal(stack.subgraphs.metrics.stats.activeStreams, sum(stack.edges, 'Stats'));
    assert.ok(stack.subgraphs.metrics.stats.activeStreams <= stack.edges.length * matches.length);
  } finally { await Promise.all(clients.map(c => c.close())); }
});

test('retention: a late joiner on an idle scope gets the state right away, without waiting for the periodic frame', { timeout: 60_000 }, async () => {
  const plain = await startEdge({ upstream: stack.url, retain: false, name: 'no-retention' });
  const measure = async (url, matchId) => {
    stack.subgraphs.stores.scoreboard.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, Date.now()));
    const first = client(url, matchId, ['scoreboard']);
    await until(() => first.synced(), 'first client');               // opens the upstream; gets the initial frame
    const t0 = Date.now();
    const late = client(url, matchId, ['scoreboard']);
    await until(() => late.synced(), 'late joiner', 5000);
    const ms = Date.now() - t0;
    await Promise.all([first.close(), late.close()]);
    return ms;
  };
  try {
    const retained = await measure(stack.edges[0].url, 'idle-retained');
    const notRetained = await measure(plain.url, 'idle-no-retention');
    console.log(JSON.stringify({ lateJoinerMs: { withRetention: retained, withoutRetention: notRetained, periodicMs: 2000 } }));
    assert.ok(retained < 300, `with retention: ${retained} ms`);
    assert.ok(notRetained > 1000, `without retention it waits for the periodic frame: ${notRetained} ms`);
    assert.ok(stack.edges[0].metrics.retainedReplays >= 1);
  } finally { await plain.close(); }
});

test('identity in the key: different tokens share neither the upstream nor the retained frame', { timeout: 60_000 }, async () => {
  const edge = stack.edges[0];
  const matchId = 'identity';
  const before = { upstreams: edge.metrics.upstreamByOperation.Scoreboard ?? 0, replays: edge.metrics.retainedReplays };
  const a1 = client(edge.url, matchId, ['scoreboard'], { headers: { authorization: 'Bearer a' } });
  await until(() => a1.synced(), 'a1');
  const b = client(edge.url, matchId, ['scoreboard'], { headers: { authorization: 'Bearer b' } });
  const a2 = client(edge.url, matchId, ['scoreboard'], { headers: { authorization: 'Bearer a' } });
  try {
    await until(() => b.synced() && a2.synced(), 'b and a2');
    assert.equal((edge.metrics.upstreamByOperation.Scoreboard ?? 0) - before.upstreams, 2, 'one upstream per identity');
    assert.equal(edge.metrics.retainedReplays - before.replays, 1, 'only a2 (same identity as a1) got the retained frame');
  } finally { await Promise.all([a1, b, a2].map(c => c.close())); }
});

test('an instance goes down mid-match: clients move through the balancer, with no version regression', { timeout: 90_000 }, async () => {
  const matchId = 'instance-down';
  const { stores } = stack.subgraphs;
  const next = playGenerator(matchId, 4);
  stores.scoreboard.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, Date.now()));
  const clients = Array.from({ length: 12 }, () => client(stack.balancer.url, matchId, ['scoreboard', 'stats']));
  let killedAt = 0; let recovery = null;
  try {
    await until(() => clients.every(c => c.synced()), '12 clients synced');
    const t0 = Date.now(); let goals = 0;
    while (Date.now() - t0 < 4000) {
      stores.stats.update(matchId, s => applyStatEvents(s, [next(), next()], matchId));
      if (Math.floor((Date.now() - t0) / 1000) > goals) { stores.scoreboard.update(matchId, s => scoreboardOps.scoreGoal(s, { side: 'HOME' }, Date.now())); goals++; }
      if (!killedAt && Date.now() - t0 > 1500) {
        // Time until every client is back in sync (reconnection + snapshot). Watched
        // from before the kill, since the clients can reconnect before killEdge returns;
        // the drop shows up as a client error, because `synced` only resets on
        // reconnection and the snapshot comes right after, so polling it can miss it.
        const errorsBefore = clients.reduce((n, c) => n + c.errors.length, 0);
        killedAt = Date.now();
        recovery = (async () => {
          await until(() => clients.reduce((n, c) => n + c.errors.length, 0) > errorsBefore, 'drop noticed', 3000);
          await until(() => clients.every(c => c.recovered()), 'recovery', 10_000);
          return Date.now() - killedAt;
        })();
        recovery.catch(() => {}); // awaited below; no unhandled rejection meanwhile
        await stack.killEdge(1);
      }
      await delay(50);
    }
    const final = { scoreboard: stores.scoreboard.version(matchId), stats: stores.stats.version(matchId) };
    await until(() => clients.every(c => c.syncs.scoreboard.version === final.scoreboard && c.syncs.stats.version === final.stats), 'everyone on the final version', 15_000);
    const migrated = clients.filter(c => c.errors.length);
    const recoveryMs = await recovery;
    console.log(JSON.stringify({ clients: clients.length, migrated: migrated.length, recoveryMs, upstreamsPerInstance: stack.edges.map(e => e.metrics.upstreamByOperation), lb: stack.balancer.metrics }));
    assert.ok(migrated.length >= 1, 'some client was on the killed instance');
    assert.ok(recoveryMs < 1500, `recovery in ${recoveryMs} ms, without waiting for the 2 s periodic frame`);
    for (const c of clients) for (const channel of ['scoreboard', 'stats']) assert.ok(monotonic(c.seen[channel]), `${channel} went back`);
    for (const c of clients) {
      assert.deepEqual(c.syncs.stats.state, stores.stats.read(matchId).state);
      assert.equal(c.syncs.scoreboard.state.home, stores.scoreboard.read(matchId).state.home);
    }
  } finally { await Promise.all(clients.map(c => c.close())); }
});

test('cleanup: with no subscribers, no instance keeps an upstream or a retained frame', { timeout: 30_000 }, async () => {
  const alive = stack.edges.filter((_, i) => i !== 1);
  await until(() => alive.every(e => e.metrics.upstreamSubscriptions === 0 && e.metrics.retainedEntries === 0), 'edges clean');
  await until(() => stack.subgraphs.metrics.scoreboard.activeStreams === 0 && stack.subgraphs.metrics.stats.activeStreams === 0, 'subgraph clean'
);
});
