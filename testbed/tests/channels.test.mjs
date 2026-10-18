import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { Store } from '../subgraphs/store.mjs';
import { ChannelHub, LiveStream } from '../subgraphs/channels.mjs';
import { applyPatch } from '../subgraphs/patch.mjs';
import { initialScoreboard, initialStats, scoreboardOps, applyStatEvents, playGenerator } from '../subgraphs/sports.mjs';

function setup(mode, options = {}, initial = mode === 'state' ? initialScoreboard : initialStats) {
  let hub;
  const store = new Store(':memory:', { initial, onCommit: c => hub.publish(c) });
  hub = new ChannelHub({ store, mode, periodicMs: 60_000, ...options });
  return { store, hub, open: () => new LiveStream({ hub, key: 'm1' }) };
}
const statEvent = playGenerator('m1', 5);
const play = (store, n = 1) => { for (let i = 0; i < n; i++) store.update('m1', s => applyStatEvents(s, [statEvent()], 'm1')); };

test('scoreboard: immediate STATE on open and on every commit, with the full state', async () => {
  const { store, open } = setup('state');
  store.update('m1', s => scoreboardOps.startPeriod(s, { period: 1 }, 0));
  const stream = open();
  const first = (await stream.next()).value;
  assert.deepEqual([first.kind, first.version, first.state.status], ['STATE', 1, 'LIVE']);
  const pending = stream.next();
  store.update('m1', s => scoreboardOps.scoreGoal(s, { side: 'HOME' }, 0));
  const goal = (await pending).value;
  assert.deepEqual([goal.version, goal.state.home, goal.state.lastEvent.type], [2, 1, 'GOAL']);
  assert.ok(goal.committedAt > 0);
  await stream.return(); store.close();
});

test('periodic frames with the state (late joiners and loss are solved without a query)', async () => {
  const { store, open } = setup('state', { periodicMs: 20 });
  const stream = open(); await stream.next();
  const periodic = (await stream.next()).value;
  assert.deepEqual([periodic.kind, periodic.version, periodic.committedAt], ['STATE', 0, null]);
  await stream.return(); store.close();
});

test('scoreboard: a slow consumer only gets the newest STATE', async () => {
  const { store, open } = setup('state');
  const stream = open(); await stream.next();
  store.update('m1', s => scoreboardOps.startPeriod(s, { period: 1 }, 0));
  for (let i = 0; i < 5; i++) store.update('m1', s => scoreboardOps.scoreGoal(s, { side: 'AWAY' }, 0));
  assert.equal(stream.queue.length, 1);
  assert.equal((await stream.next()).value.state.away, 5);
  await stream.return(); store.close();
});

test('delta mode (experiments): 200 commits in a window become one DELTA; contiguous windows', async () => {
  const { store, open } = setup('delta', { coalesceMs: 40, adaptive: false });
  const stream = open();
  assert.deepEqual((await stream.next()).value.kind, 'CHECKPOINT');
  play(store, 200);
  const d1 = (await stream.next()).value;
  assert.deepEqual([d1.kind, d1.from, d1.version], ['DELTA', 0, 200]);
  assert.deepEqual(applyPatch(initialStats(), d1.patch), store.read('m1').state, 'merged patch = final state');
  const leaves = o => Object.values(o).reduce((n, v) => n + (v && typeof v === 'object' ? leaves(v) : 1), 0);
  assert.ok(d1.patch.length <= leaves(store.read('m1').state), `patch bounded by the fields (${d1.patch.length}), not by the 200 plays`);
  play(store, 3);
  const d2 = (await stream.next()).value;
  assert.deepEqual([d2.from, d2.version], [200, 203]);
  await stream.return(); store.close();
});

test('delta mode (experiments): a slow consumer gets merged DELTAs, without loss', async () => {
  const { store, open } = setup('delta', { coalesceMs: 5, adaptive: false });
  const stream = open(); await stream.next();
  for (let w = 0; w < 6; w++) { play(store, 20); await delay(15); }
  assert.ok(stream.queue.length <= 2, `queue ${stream.queue.length}`);
  const merged = stream.queue.find(f => f.kind === 'DELTA');
  assert.deepEqual([merged.from, merged.version], [0, 120]);
  assert.deepEqual(applyPatch(initialStats(), merged.patch), store.read('m1').state);
  await stream.return(); store.close();
});

test('return() releases the listener and the timers even while waiting', async () => {
  const { hub, open, store } = setup('delta', { coalesceMs: 50 });
  const stream = open(); await stream.next();
  const waiting = stream.next();
  play(store, 1); // pending window with a timer
  assert.equal(hub.size, 1);
  await stream.return();
  assert.equal(hub.size, 0); assert.equal(hub.keys.size, 0);
  assert.deepEqual(await waiting, { done: true, value: undefined });
  store.close();
});

test('delta mode (experiments): each window sends the smaller one, patch or full state', async () => {
  const { store, open } = setup('delta', { coalesceMs: 20 });
  play(store, 400); // state already populated (every player), as in the middle of a match
  const stream = open();
  try {
    await stream.next();
    play(store, 2);
    assert.equal((await stream.next()).value.kind, 'DELTA', 'few fields changed: patch');
    play(store, 300);
    const burst = (await stream.next()).value;
    assert.deepEqual([burst.kind, burst.version, burst.patch], ['STATE', 702, null], 'almost everything changed: full state');
    assert.deepEqual(burst.state, store.read('m1').state);
  } finally { await stream.return(); store.close(); }
});

test('delta mode (experiments): STATE supersedes pending DELTAs in the queue', async () => {
  const { store, open } = setup('delta', { coalesceMs: 5 });
  play(store, 400);
  const stream = open();
  try {
    await stream.next();
    play(store, 2); await delay(15);     // DELTA in the queue
    play(store, 300); await delay(15);   // STATE in the queue
    assert.deepEqual(stream.queue.map(f => f.kind), ['STATE']);
  } finally { await stream.return(); store.close(); }
});


// Stats in production: full state with a limiter.
test('limiter: quiet goes out right away; a burst sends at most 1 per window, with the latest state', async () => {
  const { store, open } = setup('state', { throttleMs: 60 }, initialStats);
  play(store, 400);
  const stream = open();
  try {
    await stream.next();
    const t0 = Date.now();
    play(store, 1);
    const calm = (await stream.next()).value;
    assert.ok(Date.now() - t0 < 20, 'an isolated play goes out right away');
    assert.equal(calm.version, 401);
    const burstStart = Date.now();
    for (let i = 0; i < 10; i++) { play(store, 10); await delay(10); } // ~100 ms burst
    const frames = [];
    while (true) {
      const frame = (await stream.next()).value; frames.push({ frame, at: Date.now() - burstStart });
      if (frame.version === 501) break;
    }
    assert.ok(frames.length <= 3, `a burst of 100 commits in ~100 ms became ${frames.length} frames`);
    assert.deepEqual(frames.at(-1).frame.state, store.read('m1').state, 'the last frame has the final state');
  } finally { await stream.return(); store.close(); }
});

test('limiter: committedAt is that of the oldest commit not yet sent', async () => {
  const { store, open } = setup('state', { throttleMs: 50 }, initialStats);
  const stream = open();
  try {
    await stream.next();
    play(store, 1); await stream.next();                 // opens the window
    const first = Date.now(); play(store, 1);
    await delay(20); play(store, 1);
    const frame = (await stream.next()).value;
    assert.equal(frame.version, 3);
    assert.ok(Math.abs(frame.committedAt - first) < 5, "measures the delay of the frame's oldest data"
);
  } finally { await stream.return(); store.close(); }
});
