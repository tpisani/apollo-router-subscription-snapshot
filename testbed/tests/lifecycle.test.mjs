import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { matchPhase, scoreboardMaxAge } from '../subgraphs/sports.mjs';
import { startStack } from '../emulator/stack.mjs';
import { watchMatch } from '../client/lifecycle.mjs';
import { graphql, snapshotApi, transports } from '../client/transport.mjs';

async function until(predicate, message, ms = 8000) {
  for (const end = Date.now() + ms; Date.now() < end; await delay(10)) if (await predicate()) return;
  throw new Error(`Timeout: ${message}`);
}

// ---- Rules with a fixed clock ----------------------------------------------------
const T = Date.parse('2026-10-18T19:00:00Z');   // kickoff
const MIN = 60_000, LEAD = 15 * MIN;

test('phase: the time decides when to subscribe, the status decides when to stop', () => {
  assert.deepEqual(matchPhase({ status: 'SCHEDULED', kickoff: T }, T - 2 * 60 * MIN), { phase: 'before', subscribeAt: T - LEAD });
  assert.deepEqual(matchPhase({ status: 'SCHEDULED', kickoff: T }, T - LEAD), { phase: 'live' }, 'right at the window');
  assert.deepEqual(matchPhase({ status: 'SCHEDULED', kickoff: T }, T + 20 * MIN), { phase: 'live' }, 'late: keeps subscribing');
  assert.deepEqual(matchPhase({ status: 'SCHEDULED', kickoff: null }, T), { phase: 'live' }, 'no kickoff time: subscribes, to be safe');
  assert.deepEqual(matchPhase({ status: 'LIVE', kickoff: T }, T - 2 * 60 * MIN), { phase: 'live' }, 'started early: the status wins');
  assert.deepEqual(matchPhase({ status: 'HALF_TIME', kickoff: T }, T + 50 * MIN), { phase: 'live' });
  assert.deepEqual(matchPhase({ status: 'FULL_TIME', kickoff: T }, T + 2 * 60 * MIN), { phase: 'over' });
});

test('scoreboard cache: SCHEDULED never goes past kickoff − lead; FULL_TIME is long', () => {
  const ttl = (state, now) => scoreboardMaxAge(state, now, { live: 2 });
  const scheduled = { status: 'SCHEDULED', kickoff: T };
  assert.equal(ttl(scheduled, T - 3 * 60 * MIN), 300, 'far from kickoff: the 5 min cap');
  assert.equal(ttl(scheduled, T - LEAD - 90_000), 90, 'close: only until the window');
  assert.equal(ttl(scheduled, T - LEAD - 400), 0, 'less than 1 s: no-store');
  assert.equal(ttl(scheduled, T - LEAD + 1), 2, 'in the window: short, like live');
  assert.equal(ttl({ status: 'SCHEDULED', kickoff: null }, T), 2);
  assert.equal(ttl({ status: 'LIVE', kickoff: T }, T + MIN), 2);
  assert.equal(ttl({ status: 'FULL_TIME', kickoff: T }, T + 2 * 60 * MIN), 3600);
  // Property: for any instant before the window, the response expires by the window.
  for (let now = T - 3 * 60 * MIN; now < T - LEAD; now += 7_777) assert.ok(now + ttl(scheduled, now) * 1000 <= T - LEAD, `expires after the window at ${now}`);
});

// ---- End to end through the edge (worst-case query cache: never expires) ---------
// Short window (1 s lead) so the test does not wait 15 min.
const lead = 1000;
let stack;
test.before(async () => { stack = await startStack({ liveLeadMs: lead, scoreboard: { periodicMs: 500 }, stats: { periodicMs: 500, throttleMs: 100 } }); });
test.after(async () => { await stack?.stop(); });

const scoresUrl = () => `http://127.0.0.1:${stack.subgraphs.ports.scores}/graphql`;
const mutate = (query, variables) => graphql(scoresUrl(), query, variables);
const schedule = (m, kickoff) => mutate('mutation($m: ID!, $k: Float!) { scheduleMatch(matchId: $m, kickoff: $k) { version } }', { m, k: kickoff });
const start = m => mutate('mutation($m: ID!) { startPeriod(matchId: $m, period: 1) { version } }', { m });
const goal = m => mutate('mutation($m: ID!) { scoreGoal(matchId: $m, side: HOME) { version } }', { m });
const finish = m => mutate('mutation($m: ID!) { endPeriod(matchId: $m, final: true) { version } }', { m });
const streams = (channel, m) => stack.subgraphs.metrics[channel].activeByMatch[m] ?? 0;

function watch(matchId) {
  const url = stack.edge.url; const phases = []; const errors = [];
  const w = watchMatch({ matchId, lead, api: channel => snapshotApi(url, channel, matchId),
    connect: (channel, signal) => transports.multipart(url, channel, matchId, { signal }),
    onPhase: p => phases.push(p), onError: e => errors.push(e), runOptions: { wait: () => 100 } });
  return Object.assign(w, { phases, errors });
}
const cacheControl = async m => (await fetch(scoresUrl(), { method: 'POST', headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ query: 'query($m: ID!) { scoreboard(matchId: $m) { version } }', variables: { m } }) })).headers.get('cache-control');

test("the subgraph's Cache-Control by status and time", { timeout: 30_000 }, async () => {
  await schedule('cc-far', Date.now() + 3 * 60 * MIN);
  assert.equal(await cacheControl('cc-far'), 'public, max-age=300');
  await schedule('cc-near', Date.now() + lead + 30_000);
  const near = Number(/max-age=(\d+)/.exec(await cacheControl('cc-near'))[1]);
  assert.ok(near <= 30 && near >= 28, `close to kickoff: ${near} s`);
  await schedule('cc-window', Date.now() + lead / 2);
  assert.equal(await cacheControl('cc-window'), 'public, max-age=2');
  await start('cc-end'); await finish('cc-end');
  assert.equal(await cacheControl('cc-end'), 'public, max-age=3600');
});

test('finished match: query only, no subscription', { timeout: 30_000 }, async () => {
  const m = `ended-${Date.now()}`;
  await start(m); await goal(m); await finish(m);
  const opened = { scoreboard: stack.subgraphs.metrics.scoreboard.streamsOpened, stats: stack.subgraphs.metrics.stats.streamsOpened };
  const w = watch(m);
  await w.done;
  assert.deepEqual(w.phases, ['over']);
  assert.equal(w.counts.subscriptions, 0);
  assert.equal(w.counts.queries, 2, 'one query per channel');
  assert.equal(w.syncs.scoreboard.state.status, 'FULL_TIME');
  assert.equal(w.syncs.scoreboard.state.home, 1);
  assert.deepEqual({ scoreboard: stack.subgraphs.metrics.scoreboard.streamsOpened, stats: stack.subgraphs.metrics.stats.streamsOpened }, opened, 'no stream opened on the subgraph');
});

test('before kickoff with a stale SCHEDULED in the cache: subscribes at the window; delay covered; until FULL_TIME', { timeout: 30_000 }, async () => {
  const m = `scheduled-${Date.now()}`;
  const kickoff = Date.now() + lead + 700;
  await schedule(m, kickoff);
  // Warms the edge cache with SCHEDULED; it never expires (worst case).
  await snapshotApi(stack.edge.url, 'scoreboard', m).snapshot(0);
  const w = watch(m);
  try {
    await until(() => w.phase === 'before', 'before phase');
    assert.equal(w.counts.subscriptions, 0);
    assert.equal(streams('scoreboard', m), 0, 'before the window, no stream');

    // In the window (kickoff − lead), subscribes even with the cache saying SCHEDULED.
    await until(() => w.phase === 'live' && w.syncs.scoreboard.synced && w.syncs.stats.synced, 'subscribed in the window', 5000);
    const subscribedAt = Date.now();
    assert.ok(subscribedAt >= kickoff - lead, 'did not subscribe before the window');
    assert.ok(subscribedAt - (kickoff - lead) < 500, `subscribed ${subscribedAt - (kickoff - lead)} ms after the window`);
    assert.equal(streams('scoreboard', m), 1); assert.equal(streams('stats', m), 1);
    const cached = await snapshotApi(stack.edge.url, 'scoreboard', m).snapshot(0);
    assert.equal(cached.state.status, 'SCHEDULED', 'the cache is still stale, and it does not matter');

    // Delay: past kickoff time and not started yet; the client keeps subscribing.
    await until(() => Date.now() > kickoff + 600, 'past kickoff time', 3000);
    assert.equal(w.syncs.scoreboard.state.status, 'SCHEDULED');
    assert.equal(w.phase, 'live');
    await start(m);
    await until(() => w.syncs.scoreboard.state.status === 'LIVE', 'kickoff through the stream');
    await goal(m);
    await until(() => w.syncs.scoreboard.state.home === 1, 'goal');

    // FULL_TIME: the scoreboard ends on the subgraph, the client closes both channels.
    await finish(m);
    await w.done;
    assert.equal(w.phase, 'over');
    assert.deepEqual(w.phases, ['before', 'live', 'over']);
    assert.equal(w.syncs.scoreboard.state.status, 'FULL_TIME');
    await until(() => streams('scoreboard', m) === 0 && streams('stats', m) === 0, 'streams closed on the subgraph');
    assert.equal(w.errors.length, 0, 'no reconnect');
  } finally { await w.close(); }
});

test('stale cache saying LIVE for an already finished match: the first frame brings FULL_TIME and closes', { timeout: 30_000 }, async () => {
  const m = `stale-live-${Date.now()}`;
  await start(m);
  await snapshotApi(stack.edge.url, 'scoreboard', m).snapshot(0);   // cache: LIVE, forever
  await goal(m); await finish(m);
  const t0 = Date.now();
  const w = watch(m);
  await w.done;
  assert.deepEqual(w.phases, ['live', 'over']);
  assert.equal(w.syncs.scoreboard.state.status, 'FULL_TIME');
  assert.equal(w.syncs.scoreboard.state.home, 1);
  assert.ok(Date.now() - t0 < 1000, `closed in ${Date.now() - t0} ms`);
  assert.equal(w.errors.length, 0, 'no reconnect');
  await until(() => streams('scoreboard', m) === 0 && streams('stats', m) === 0 && stack.edge.metrics.upstreamSubscriptions === 0, 'everything closed');
});

test('several deduplicated clients: FULL_TIME ends all of them and releases the upstream', { timeout: 30_000 }, async () => {
  const m = `dedup-end-${Date.now()}`;
  await start(m);
  const ws = Array.from({ length: 5 }, () => watch(m));
  try {
    await until(() => ws.every(w => w.syncs.scoreboard.synced && w.syncs.stats.synced), 'everyone subscribed');
    assert.equal(streams('scoreboard', m), 1, 'one upstream for the 5');
    await finish(m);
    await Promise.all(ws.map(w => w.done));
    assert.ok(ws.every(w => w.phase === 'over' && w.errors.length === 0));
    await until(() => streams('scoreboard', m) === 0 && streams('stats', m) === 0, 'upstreams closed'
);
  } finally { await Promise.all(ws.map(w => w.close())); }
});
