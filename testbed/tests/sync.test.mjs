import test from 'node:test';
import assert from 'node:assert/strict';
import { Store, SnapshotCache } from '../subgraphs/store.mjs';
import { initialScoreboard, initialStats, scoreboardOps, applyStatEvents, playGenerator } from '../subgraphs/sports.mjs';
import { StateSync, createSync, statsFromWire, runSession } from '../client/sync.mjs';

const matchId = 'm1';
const statsOut = state => ({ ...state, players: Object.entries(state.players).map(([id, p]) => ({ id, ...p })) });

// API over the real store with a cache that never expires (worst case).
function cachedApi(store, shape = s => s) {
  const snapshots = new SnapshotCache(store, 60_000);
  const cache = new Map(); const calls = { snapshot: 0 };
  return {
    calls,
    snapshot: async min => {
      calls.snapshot++;
      if (!cache.has(min)) { const d = snapshots.get(matchId, min); cache.set(min, { matchId, version: d.version, state: shape(d.state) }); }
      return structuredClone(cache.get(min));
    }
  };
}
const frameOf = (doc, shape = s => s) => ({ matchId, kind: 'STATE', version: doc.version, state: shape(doc.state), committedAt: null });

test('scoreboard: a late joiner is right on the first frame, with no query beyond the base', async () => {
  const store = new Store(':memory:', { initial: initialScoreboard }); const api = cachedApi(store);
  store.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, 0));
  await api.snapshot(0); // stale base in the cache (version 1)
  const goal = store.update(matchId, s => scoreboardOps.scoreGoal(s, { side: 'HOME' }, 0));
  const sync = createSync('scoreboard', { matchId, api });
  sync.initialize(await sync.startBase());
  assert.deepEqual([sync.version, sync.state.home], [1, 0], 'stale provisional base');
  await sync.accept(frameOf(goal));
  assert.deepEqual([sync.version, sync.state.home, sync.synced], [2, 1, true]);
  store.close();
});

test('stats: frame with the full state, converted from the API format', async () => {
  const store = new Store(':memory:', { initial: initialStats });
  const next = playGenerator(matchId, 2);
  for (let i = 0; i < 30; i++) store.update(matchId, s => applyStatEvents(s, [next()], matchId));
  const sync = createSync('stats', { matchId, api: { snapshot: async () => { throw new Error('should not query'); } } });
  await sync.accept(frameOf(store.read(matchId), statsOut));
  assert.deepEqual(sync.state, store.read(matchId).state);
  assert.deepEqual(statsFromWire(statsOut(store.read(matchId).state)), store.read(matchId).state);
  store.close();
});

test('repeated, late, out-of-order or older retained frames are ignored', async () => {
  const sync = new StateSync({ matchId, api: { snapshot: async () => { throw new Error('should not query'); } } });
  const frame = (version, home) => ({ matchId, kind: 'STATE', version, state: { ...initialScoreboard(), home } });
  for (const [v, h] of [[3, 2], [1, 0], [3, 2], [2, 1], [5, 3], [4, 2]]) await sync.accept(frame(v, h));
  assert.deepEqual([sync.version, sync.state.home, sync.stats.stale], [5, 3, 3]);
});

test('the provisional base shows up before the first frame, without marking synced', async () => {
  const store = new Store(':memory:', { initial: initialScoreboard }); const api = cachedApi(store);
  store.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, 0));
  const sync = new StateSync({ matchId, api }); const views = [];
  let release; const gate = new Promise(resolve => { release = resolve; });
  async function* frames() { await gate; yield frameOf(store.read(matchId)); }
  const session = runSession(sync, frames(), { onUpdate: view => views.push(view) }).catch(() => {});
  await new Promise(resolve => setTimeout(resolve, 10));
  assert.deepEqual([views[0].version, views[0].synced], [1, false]);
  release(); await session;
  assert.equal(views.at(-1).synced, true);
  store.close();
});

test('a failure fetching the base is not kept: the next attempt retries', async () => {
  let fail = true;
  const sync = new StateSync({ matchId, api: { snapshot: async () => { if (fail) throw new Error('network'); return { matchId, version: 1, state: initialScoreboard() }; } } });
  await assert.rejects(sync.startBase(), /network/);
  fail = false;
  assert.equal((await sync.startBase()).version, 1);
});

// Property: several "edges" (instances) with retained frames of different ages;
// the client switches instances at any time and gets lost, duplicated,
// reordered and older retained frames. The version never goes back and, after
// one last frame, everyone is on the exact database state.
test('property: switching instances with retained frames of different ages, without regression', async () => {
  for (let seed = 1; seed <= 25; seed++) {
    let x = seed; const rand = () => (x = (x * 1103515245 + 12345) % 2 ** 31) / 2 ** 31;
    const store = new Store(':memory:', { initial: initialStats });
    const next = playGenerator(matchId, seed);
    const edges = Array.from({ length: 3 }, () => ({ retained: null, inflight: [] }));
    const clients = Array.from({ length: 6 }, () => ({ sync: createSync('stats', { matchId, api: cachedApi(store, statsOut) }), edge: 0, seen: [] }));
    const deliver = async (client, frame) => { await client.sync.accept(frame); client.seen.push(client.sync.version); };
    for (let step = 0; step < 300; step++) {
      const r = rand();
      if (r < 0.5) {
        store.update(matchId, s => applyStatEvents(s, [next()], matchId));
        const frame = frameOf(store.read(matchId), statsOut);
        for (const edge of edges) if (rand() < 0.85) { edge.inflight.push(frame); if (rand() < 0.1) edge.inflight.push(frame); }
      } else if (r < 0.8) {
        const edge = edges[Math.floor(rand() * edges.length)];
        if (edge.inflight.length) {
          const [frame] = edge.inflight.splice(Math.floor(rand() * Math.min(3, edge.inflight.length)), 1);
          if (!edge.retained || frame.version > edge.retained.version || rand() < 0.3) edge.retained = frame; // the retained one may be older
          for (const client of clients) if (edges[client.edge] === edge) await deliver(client, frame);
        }
      } else {
        const client = clients[Math.floor(rand() * clients.length)];
        client.edge = Math.floor(rand() * edges.length); client.sync.synced = false; // reconnect on another instance
        if (edges[client.edge].retained) await deliver(client, edges[client.edge].retained);
      }
    }
    const final = frameOf(store.read(matchId), statsOut);
    for (const client of clients) {
      await deliver(client, final);
      assert.ok(client.seen.every((v, i) => i === 0 || v >= client.seen[i - 1]), `seed ${seed}: version went back`
);
      assert.equal(client.sync.version, store.version(matchId), `seed ${seed}`);
      assert.deepEqual(client.sync.state, store.read(matchId).state, `seed ${seed}`);
    }
    store.close();
  }
});
