import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { diff, applyPatch, mergePatches } from '../subgraphs/patch.mjs';
import { Store, SnapshotCache } from '../subgraphs/store.mjs';
import { initialScoreboard, initialStats, scoreboardOps, clockSeconds, applyStatEvents, playGenerator } from '../subgraphs/sports.mjs';

test('patch: diff + applyPatch rebuild the state; merge keeps the last value', () => {
  const a = { home: { shots: 1, passes: 10 }, players: { h1: { passes: 3 } } };
  const b = { home: { shots: 2, passes: 10 }, players: { h1: { passes: 4 }, h2: { passes: 1 } } };
  const patch = diff(a, b);
  assert.deepEqual(applyPatch(structuredClone(a), patch), b);
  assert.deepEqual(mergePatches([{ path: ['x'], value: 1 }], [{ path: ['x'], value: 2 }, { path: ['y'], value: 3 }]),
    [{ path: ['x'], value: 2 }, { path: ['y'], value: 3 }]);
});

// The property that allows applying a DELTA on top of a snapshot taken mid-window.
test('patch: a merged patch (from, to] applied at any version v with from <= v < to gives the state at to', () => {
  const next = playGenerator('m1', 3);
  const states = [initialStats()];
  const patches = [];
  for (let i = 0; i < 300; i++) {
    const s = structuredClone(states.at(-1));
    applyStatEvents(s, [next()], 'm1');
    patches.push(diff(states.at(-1), s)); states.push(s);
  }
  let x = 9; const rand = n => (x = (x * 1103515245 + 12345) % 2 ** 31) % n;
  for (let trial = 0; trial < 500; trial++) {
    const from = rand(290), to = from + 1 + rand(10), v = from + rand(to - from);
    const merged = mergePatches(...patches.slice(from, to));
    assert.deepEqual(applyPatch(structuredClone(states[v]), merged), states[to], `from ${from} v ${v} to ${to}`);
    assert.deepEqual(applyPatch(applyPatch(structuredClone(states[v]), merged), merged), states[to], 'idempotent');
  }
});

test('store: the version only goes up when the state changes; patch with final values', () => {
  const published = [];
  const store = new Store(':memory:', { initial: initialStats, onCommit: c => published.push(c) });
  const c1 = store.update('m1', s => applyStatEvents(s, [{ side: 'HOME', playerId: 'h1', type: 'PASS' }], 'm1'));
  assert.equal(c1.version, 1);
  assert.ok(c1.patch.some(p => p.path.join('.') === 'home.passes' && p.value === 1));
  assert.equal(store.update('m1', () => {}), null, 'no change, no new version');
  assert.equal(store.version('m1'), 1); assert.equal(published.length, 1);
  store.close();
});

test('store: a failed transaction neither changes nor publishes; the state survives a restart', () => {
  const dir = mkdtempSync(join(tmpdir(), 'sports-')); let store; let published = 0;
  try {
    store = new Store(join(dir, 'db.sqlite'), { initial: initialScoreboard, onCommit: () => published++ });
    assert.throws(() => store.update('m1', s => { s.home = 9; throw new Error('provider down'); }), /provider/);
    assert.equal(store.read('m1').state.home, 0); assert.equal(published, 0);
    store.update('m1', s => scoreboardOps.startPeriod(s, { period: 1 }, 0));
    store.close();
    store = new Store(join(dir, 'db.sqlite'), { initial: initialScoreboard });
    assert.equal(store.read('m1').version, 1); assert.equal(store.read('m1').state.status, 'LIVE');
  } finally { store?.close(); rmSync(dir, { recursive: true, force: true }); }
});

test('snapshot cache: any version >= minVersion; never below; TTL', () => {
  let now = 0;
  const store = new Store(':memory:', { initial: initialScoreboard });
  const cache = new SnapshotCache(store, 1000, () => now);
  store.update('m1', s => scoreboardOps.startPeriod(s, { period: 1 }, 0));
  assert.equal(cache.get('m1', 0).version, 1);
  store.update('m1', s => scoreboardOps.scoreGoal(s, { side: 'HOME' }, 0));
  assert.equal(cache.get('m1', 0).version, 1, 'stale, but satisfies minVersion 0');
  assert.equal(cache.get('m1', 2).version, 2, 'minVersion above the cache forces a read');
  assert.throws(() => cache.get('m1', 3), error => error.code === 'NOT_YET');
  now = 5000; store.update('m1', s => scoreboardOps.scoreGoal(s, { side: 'AWAY' }, 0));
  assert.equal(cache.get('m1', 0).version, 3, 'TTL expired');
  store.close();
});

test('scoreboard: the clock is derived on the client; only start, pause, goal and end produce a version', () => {
  const s = initialScoreboard();
  scoreboardOps.startPeriod(s, { period: 1 }, 1_000_000);
  assert.equal(clockSeconds(s.clock, 1_000_000 + 754_000), 754, '12:34 without any commit');
  scoreboardOps.scoreGoal(s, { side: 'AWAY' }, 1_000_000 + 754_000);
  assert.deepEqual([s.home, s.away, s.lastEvent], [0, 1, { type: 'GOAL', side: 'AWAY', minute: 12 }]);
  scoreboardOps.endPeriod(s, { final: false }, 1_000_000 + 2_700_000);
  assert.equal(s.status, 'HALF_TIME');
  assert.equal(clockSeconds(s.clock, 9e12), 2700, 'stopped at half time');
  scoreboardOps.startPeriod(s, { period: 2 }, 5_000_000);
  assert.equal(clockSeconds(s.clock, 5_000_000 + 60_000), 2760);
  assert.throws(() => scoreboardOps.scoreGoal(initialScoreboard(), { side: 'HOME' }, 0), /outside a match/);
});

test('stats: ingestion increments become final values; derived possession', () => {
  const s = initialStats();
  applyStatEvents(s, [
    { side: 'HOME', playerId: 'h7', type: 'PASS' }, { side: 'HOME', playerId: 'h7', type: 'SHOT_ON_TARGET' },
    { side: 'AWAY', playerId: 'a2', type: 'FOUL' }, { side: 'HOME', playerId: 'h3', type: 'CORNER' }
  ], 'm1');
  assert.deepEqual(s.home, { possession: 75, touches: 3, shots: 1, shotsOnTarget: 1, passes: 1, corners: 1, fouls: 0 });
  assert.deepEqual(s.players.h7, { touches: 2, passes: 1, shots: 1 });
  assert.throws(() => applyStatEvents(s, [{ side: 'HOME', playerId: 'zz', type: 'PASS' }], 'm1'), /Invalid event/);
});

test('patch: an object and a field with the same prefix follow the order of the last write'
, () => {
  const merged = mergePatches(
    [{ path: ['players', 'h1', 'passes'], value: 5 }],
    [{ path: ['players', 'h1'], value: { passes: 0, touches: 0 } }],
    [{ path: ['players', 'h1', 'passes'], value: 1 }]);
  assert.deepEqual(applyPatch({ players: {} }, merged), { players: { h1: { passes: 1, touches: 0 } } });
});
