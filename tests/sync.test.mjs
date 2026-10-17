import test from 'node:test';
import assert from 'node:assert/strict';
import { Store, SnapshotCache } from '../src/store.mjs';
import { mergePatches } from '../src/patch.mjs';
import { initialScoreboard, initialStats, scoreboardOps, applyStatEvents, playGenerator } from '../src/sports.mjs';
import { StateSync, DeltaSync, runSession } from '../client/sync.mjs';

const matchId = 'm1';
const statsOut = state => ({ ...state, players: Object.entries(state.players).map(([id, p]) => ({ id, ...p })) });

// API sobre o store real com um cache que nunca expira e ignora Cache-Control:
// o pior caso para "tolerante a cache". Erros não são cacheados.
function cachedApi(store, shape = s => s) {
  const snapshots = new SnapshotCache(store, 60_000);
  const cache = new Map(); const calls = { snapshot: 0, hits: 0, mins: [] };
  return {
    calls, cache,
    snapshot: async min => {
      calls.snapshot++; calls.mins.push(min);
      if (cache.has(min)) { calls.hits++; return structuredClone(cache.get(min)); }
      const doc = snapshots.get(matchId, min);
      const value = { matchId, version: doc.version, state: shape(doc.state) };
      cache.set(min, value); return structuredClone(value);
    }
  };
}
const truthStats = store => store.read(matchId).state;

test('placar: late joiner fica certo no primeiro frame, sem query além da base', async () => {
  const store = new Store(':memory:', { initial: initialScoreboard });
  const api = cachedApi(store);
  store.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, 0));
  await api.snapshot(0); // base antiga no cache (versão 1)
  const goal = store.update(matchId, s => scoreboardOps.scoreGoal(s, { side: 'HOME' }, 0));
  const sync = new StateSync({ matchId, api });
  sync.initialize(await sync.startBase());
  assert.deepEqual([sync.version, sync.state.home], [1, 0], 'base provisória antiga');
  await sync.accept({ matchId, kind: 'STATE', version: goal.version, state: goal.state });
  assert.deepEqual([sync.version, sync.state.home, sync.synced], [2, 1, true]);
  assert.equal(sync.stats.reseeds, 0);
  store.close();
});

test('placar: frames repetidos, atrasados ou fora de ordem são ignorados', async () => {
  const sync = new StateSync({ matchId, api: { snapshot: async () => { throw new Error('não deveria consultar'); } } });
  const frame = (version, home) => ({ matchId, kind: 'STATE', version, state: { ...initialScoreboard(), home } });
  for (const [v, h] of [[3, 2], [1, 0], [3, 2], [2, 1], [5, 3], [4, 2]]) await sync.accept(frame(v, h));
  assert.deepEqual([sync.version, sync.state.home], [5, 3]);
});

test('estatísticas: base antiga + DELTA adiantado = snapshot com minVersion (uma chave nova)', async () => {
  const store = new Store(':memory:', { initial: initialStats }); const api = cachedApi(store, statsOut);
  const next = playGenerator(matchId, 2);
  for (let i = 0; i < 10; i++) store.update(matchId, s => applyStatEvents(s, [next()], matchId));
  await api.snapshot(0);
  for (let i = 0; i < 30; i++) store.update(matchId, s => applyStatEvents(s, [next()], matchId));
  const sync = new DeltaSync({ matchId, api });
  await sync.accept({ matchId, kind: 'DELTA', from: 35, version: 40, patch: [] });
  assert.deepEqual(api.calls.mins, [0, 0, 40]);
  assert.equal(sync.version, 40); assert.deepEqual(sync.state, truthStats(store));
  store.close();
});

test('estatísticas: snapshot no meio da janela + DELTA que a cobre é aplicado sem query', async () => {
  const store = new Store(':memory:', { initial: initialStats }); const api = cachedApi(store, statsOut);
  const next = playGenerator(matchId, 4); const patches = [];
  const play = () => patches.push(store.update(matchId, s => applyStatEvents(s, [next()], matchId)).patch);
  for (let i = 0; i < 5; i++) play();
  const sync = new DeltaSync({ matchId, api });
  sync.initialize(await api.snapshot(0));                       // cliente na versão 5
  for (let i = 0; i < 4; i++) play();                          // head 9
  const window = mergePatches(...patches.slice(3));            // janela (3, 9]: começou antes da versão 5
  const calls = api.calls.snapshot;
  await sync.accept({ matchId, kind: 'DELTA', from: 3, version: 9, patch: window });
  assert.equal(api.calls.snapshot, calls, 'sobreposição aplicada sem snapshot');
  assert.equal(sync.version, 9); assert.deepEqual(sync.state, truthStats(store));
  store.close();
});

test('estatísticas: cache que viola o contrato (versão < minVersion) é rejeitado', async () => {
  const sync = new DeltaSync({ matchId, api: { snapshot: async () => ({ matchId, version: 1, state: statsOut(initialStats()) }) } });
  sync.initialize({ matchId, version: 1, state: statsOut(initialStats()) });
  await assert.rejects(sync.accept({ matchId, kind: 'CHECKPOINT', version: 7 }), /violou minVersion/);
});

test('base provisória aparece antes do primeiro frame, sem marcar synced', async () => {
  const store = new Store(':memory:', { initial: initialScoreboard }); const api = cachedApi(store);
  store.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, 0));
  const sync = new StateSync({ matchId, api }); const views = [];
  let release; const gate = new Promise(resolve => { release = resolve; });
  async function* frames() { await gate; yield { matchId, kind: 'STATE', version: 1, state: store.read(matchId).state }; }
  const session = runSession(sync, frames(), { onUpdate: view => views.push(view) }).catch(() => {});
  await new Promise(resolve => setTimeout(resolve, 10));
  assert.deepEqual([views[0].version, views[0].synced], [1, false]);
  release(); await session;
  assert.equal(views.at(-1).synced, true);
  store.close();
});

// Propriedade: janelas de tamanho aleatório como as do hub, frames perdidos,
// duplicados e reordenados, clientes entrando e reconectando a qualquer momento,
// snapshots tirados no meio de janelas e cache eterno. Todos terminam no estado
// exato depois de um último DELTA/CHECKPOINT.
test('propriedade: estatísticas convergem com perda, duplicação, reordenação, STATE/DELTA e cache eterno', async () => {
  for (let seed = 1; seed <= 25; seed++) {
    let x = seed; const rand = () => (x = (x * 1103515245 + 12345) % 2 ** 31) / 2 ** 31;
    const store = new Store(':memory:', { initial: initialStats }); const api = cachedApi(store, statsOut);
    const next = playGenerator(matchId, seed);
    const clients = []; const inflight = [];
    let delivered = 0; let pending = null;
    const flush = () => {
      if (!pending) return;
      // Como o hub adaptativo: às vezes a janela vai como estado inteiro.
      const frame = rand() < 0.2
        ? { matchId, kind: 'STATE', version: pending.version, state: statsOut(store.read(matchId).state) }
        : { matchId, kind: 'DELTA', ...pending };
      delivered = pending.version; pending = null;
      if (rand() < 0.15) return;                                  // perdido
      inflight.push(frame); if (rand() < 0.1) inflight.push(frame); // duplicado
    };
    for (let step = 0; step < 250; step++) {
      const r = rand();
      if (r < 0.6) {
        const c = store.update(matchId, s => applyStatEvents(s, [next()], matchId));
        pending = pending ? { ...pending, version: c.version, patch: mergePatches(pending.patch, c.patch) } : { from: delivered, version: c.version, patch: c.patch };
        if (rand() < 0.25) flush();
      } else if (r < 0.66) inflight.push({ matchId, kind: 'CHECKPOINT', version: delivered });
      else if (r < 0.72) {
        if (rand() < 0.5) await api.snapshot(0);                  // alguém aquece a base compartilhada
        clients.push(new DeltaSync({ matchId, api }));
      } else if (r < 0.75 && clients.length) clients[Math.floor(rand() * clients.length)].synced = false;
      else if (inflight.length) {
        const [frame] = inflight.splice(Math.floor(rand() * Math.min(3, inflight.length)), 1);
        for (const client of clients) await client.accept(frame);
      }
    }
    flush();
    for (const frame of inflight) for (const client of clients) await client.accept(frame);
    clients.push(new DeltaSync({ matchId, api }));
    for (const client of clients) {
      await client.accept({ matchId, kind: 'CHECKPOINT', version: delivered });
      assert.equal(client.version, store.version(matchId), `seed ${seed}`);
      assert.deepEqual(client.state, truthStats(store), `seed ${seed}`);
    }
    store.close();
  }
});
