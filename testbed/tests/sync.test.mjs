import test from 'node:test';
import assert from 'node:assert/strict';
import { Store, SnapshotCache } from '../subgraphs/store.mjs';
import { initialScoreboard, initialStats, scoreboardOps, applyStatEvents, playGenerator } from '../subgraphs/sports.mjs';
import { StateSync, createSync, statsFromWire, runSession } from '../client/sync.mjs';

const matchId = 'm1';
const statsOut = state => ({ ...state, players: Object.entries(state.players).map(([id, p]) => ({ id, ...p })) });

// API sobre o store real com um cache que nunca expira (pior caso).
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

test('placar: late joiner fica certo no primeiro frame, sem query além da base', async () => {
  const store = new Store(':memory:', { initial: initialScoreboard }); const api = cachedApi(store);
  store.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, 0));
  await api.snapshot(0); // base antiga no cache (versão 1)
  const goal = store.update(matchId, s => scoreboardOps.scoreGoal(s, { side: 'HOME' }, 0));
  const sync = createSync('scoreboard', { matchId, api });
  sync.initialize(await sync.startBase());
  assert.deepEqual([sync.version, sync.state.home], [1, 0], 'base provisória antiga');
  await sync.accept(frameOf(goal));
  assert.deepEqual([sync.version, sync.state.home, sync.synced], [2, 1, true]);
  store.close();
});

test('estatísticas: frame com o estado inteiro, convertido do formato da API', async () => {
  const store = new Store(':memory:', { initial: initialStats });
  const next = playGenerator(matchId, 2);
  for (let i = 0; i < 30; i++) store.update(matchId, s => applyStatEvents(s, [next()], matchId));
  const sync = createSync('stats', { matchId, api: { snapshot: async () => { throw new Error('não deveria consultar'); } } });
  await sync.accept(frameOf(store.read(matchId), statsOut));
  assert.deepEqual(sync.state, store.read(matchId).state);
  assert.deepEqual(statsFromWire(statsOut(store.read(matchId).state)), store.read(matchId).state);
  store.close();
});

test('frames repetidos, atrasados, fora de ordem ou retidos mais velhos são ignorados', async () => {
  const sync = new StateSync({ matchId, api: { snapshot: async () => { throw new Error('não deveria consultar'); } } });
  const frame = (version, home) => ({ matchId, kind: 'STATE', version, state: { ...initialScoreboard(), home } });
  for (const [v, h] of [[3, 2], [1, 0], [3, 2], [2, 1], [5, 3], [4, 2]]) await sync.accept(frame(v, h));
  assert.deepEqual([sync.version, sync.state.home, sync.stats.stale], [5, 3, 3]);
});

test('base provisória aparece antes do primeiro frame, sem marcar synced', async () => {
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

test('falha ao buscar a base não fica guardada: a próxima tentativa refaz', async () => {
  let fail = true;
  const sync = new StateSync({ matchId, api: { snapshot: async () => { if (fail) throw new Error('rede'); return { matchId, version: 1, state: initialScoreboard() }; } } });
  await assert.rejects(sync.startBase(), /rede/);
  fail = false;
  assert.equal((await sync.startBase()).version, 1);
});

// Propriedade: vários "edges" (instâncias) com frames retidos de idades diferentes;
// o cliente troca de instância a qualquer momento e recebe frames perdidos,
// duplicados, reordenados e retidos mais velhos. A versão nunca regride e, depois de
// um último frame, todos estão no estado exato do banco.
test('propriedade: troca de instância com frames retidos de idades diferentes, sem regressão', async () => {
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
          if (!edge.retained || frame.version > edge.retained.version || rand() < 0.3) edge.retained = frame; // retido pode ser mais velho
          for (const client of clients) if (edges[client.edge] === edge) await deliver(client, frame);
        }
      } else {
        const client = clients[Math.floor(rand() * clients.length)];
        client.edge = Math.floor(rand() * edges.length); client.sync.synced = false; // reconexão em outra instância
        if (edges[client.edge].retained) await deliver(client, edges[client.edge].retained);
      }
    }
    const final = frameOf(store.read(matchId), statsOut);
    for (const client of clients) {
      await deliver(client, final);
      assert.ok(client.seen.every((v, i) => i === 0 || v >= client.seen[i - 1]), `seed ${seed}: versão regrediu`);
      assert.equal(client.sync.version, store.version(matchId), `seed ${seed}`);
      assert.deepEqual(client.sync.state, store.read(matchId).state, `seed ${seed}`);
    }
    store.close();
  }
});
