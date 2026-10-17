import test from 'node:test';
import assert from 'node:assert/strict';
import { Store, SnapshotCache } from '../src/store.mjs';
import { Synchronizer, GapError, runSession } from '../client/sync.mjs';

const scope = 'loja-42';

// API sobre o store real com um cache que nunca expira e ignora Cache-Control:
// o pior caso para "tolerante a cache". Erros não são cacheados (como no Router).
function cachedApi(store, snapshots = new SnapshotCache(store)) {
  const cache = new Map();
  const calls = { snapshot: 0, changes: 0, hits: 0 };
  const cached = (key, fn) => {
    if (cache.has(key)) { calls.hits++; return structuredClone(cache.get(key)); }
    const value = fn(); cache.set(key, value); return structuredClone(value);
  };
  return {
    calls, cache,
    snapshot: async min => { calls.snapshot++; return cached(`s:${min}`, () => snapshots.get(scope, Number(min))); },
    changes: async (after, through) => { calls.changes++; return cached(`c:${after}:${through}`, () => store.changes(scope, after, through)); }
  };
}
const truth = store => ({ cursor: store.snapshot(scope).cursor, orders: store.snapshot(scope).orders });
const checkpoint = store => ({ scopeId: scope, kind: 'CHECKPOINT', cursor: String(store.head(scope)), change: null });
const changeFrame = change => ({ scopeId: scope, kind: 'CHANGE', cursor: change.cursor, change });

test('base antiga do cache + histórico imutável até o alvo do stream = estado exato', async () => {
  const store = new Store(); const api = cachedApi(store);
  store.change(scope, '1', 'PENDING');
  await api.snapshot('0'); // aquece a chave compartilhada no cursor 1
  store.change(scope, '1', 'PAID'); store.change(scope, '2', 'PENDING'); store.change(scope, '2', null, true);
  const sync = new Synchronizer({ scopeId: scope, api });
  await sync.accept(checkpoint(store));
  assert.deepEqual({ cursor: String(sync.cursor), orders: sync.view().orders }, truth(store));
  assert.equal(api.calls.hits, 1, 'a base veio do cache, antiga');
  store.close();
});

test('evento contíguo é aplicado direto do frame, sem query', async () => {
  const store = new Store(); const api = cachedApi(store);
  const sync = new Synchronizer({ scopeId: scope, api });
  await sync.accept(checkpoint(store));
  const before = { ...api.calls };
  await sync.accept(changeFrame(store.change(scope, '1', 'PAID')));
  assert.deepEqual(api.calls, before);
  assert.equal(sync.stats.live, 1);
  store.close();
});

test('late joiner: primeiro frame é um CHANGE no meio do stream', async () => {
  const store = new Store(); const api = cachedApi(store);
  for (let i = 0; i < 5; i++) store.change(scope, String(i), 'PENDING');
  const sync = new Synchronizer({ scopeId: scope, api });
  await sync.accept(changeFrame(store.change(scope, '9', 'PAID')));
  assert.deepEqual({ cursor: String(sync.cursor), orders: sync.view().orders }, truth(store));
  store.close();
});

test('histórico expirado sob a base em cache: snapshot com minCursor e chave nova', async () => {
  const store = new Store(); const api = cachedApi(store);
  store.change(scope, '1', 'PENDING'); await api.snapshot('0');
  for (let i = 0; i < 5; i++) store.change(scope, '1', 'PAID');
  store.prune(scope, 4);
  const sync = new Synchronizer({ scopeId: scope, api });
  await sync.accept(checkpoint(store));
  assert.equal(sync.stats.reseeds, 1);
  assert.ok(api.cache.has(`s:${store.head(scope)}`));
  assert.deepEqual({ cursor: String(sync.cursor), orders: sync.view().orders }, truth(store));
  store.close();
});

test('distância maior que maxReplay troca replay por snapshot', async () => {
  const store = new Store(); const api = cachedApi(store);
  await api.snapshot('0');
  for (let i = 0; i < 50; i++) store.change(scope, String(i % 3), 'PAID');
  const sync = new Synchronizer({ scopeId: scope, api, maxReplay: 10 });
  await sync.accept(checkpoint(store));
  assert.equal(sync.stats.pages, 0); assert.equal(sync.stats.reseeds, 1);
  assert.equal(sync.cursor, 50);
  store.close();
});

test('páginas alinhadas: clientes em cursores diferentes compartilham chaves de cache', async () => {
  const store = new Store(); const api = cachedApi(store);
  for (let i = 0; i < 250; i++) store.change(scope, String(i % 4), 'PENDING');
  const fromZero = new Synchronizer({ scopeId: scope, api });
  fromZero.initialize({ scopeId: scope, cursor: '0', orders: [] });
  fromZero.target = 37; await fromZero.catchUp();          // estado no cursor 37
  const late = new Synchronizer({ scopeId: scope, api });
  late.initialize({ scopeId: scope, cursor: '37', orders: fromZero.view().orders });
  fromZero.target = 250; await fromZero.catchUp();         // (0,100] (100,200] (200,250]
  const keys = api.cache.size;
  late.target = 250; await late.catchUp();                  // pede as mesmas páginas alinhadas
  assert.equal(api.cache.size, keys, 'nenhuma chave nova: tudo veio do cache');
  assert.deepEqual(late.view().orders, fromZero.view().orders);
  store.close();
});

test('cache que viola o contrato (snapshot abaixo do minCursor) é detectado', async () => {
  const store = new Store();
  store.change(scope, '1', 'PAID'); store.change(scope, '1', 'SHIPPED');
  const broken = { snapshot: async () => ({ scopeId: scope, cursor: '0', orders: [] }), changes: async () => { const e = new Error('x'); e.code = 'CURSOR_EXPIRED'; throw e; } };
  const sync = new Synchronizer({ scopeId: scope, api: broken });
  await assert.rejects(sync.accept(checkpoint(store)), /violou minCursor/);
  store.close();
});

test('página com buraco ou fora do intervalo é rejeitada', async () => {
  const store = new Store();
  for (let i = 0; i < 3; i++) store.change(scope, '1', 'PAID');
  const api = { snapshot: async () => ({ scopeId: scope, cursor: '0', orders: [] }),
    changes: async (after, through) => { const page = store.changes(scope, after, through); page.events.splice(1, 1); return page; } };
  const sync = new Synchronizer({ scopeId: scope, api });
  await assert.rejects(sync.accept(checkpoint(store)), GapError);
  store.close();
});

test('base provisória aparece antes do primeiro frame e sem marcar synced', async () => {
  const store = new Store(); const api = cachedApi(store);
  store.change(scope, '1', 'PENDING');
  const sync = new Synchronizer({ scopeId: scope, api });
  const views = [];
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  async function* frames() { await gate; yield checkpoint(store); }
  const session = runSession(sync, frames(), { onUpdate: view => views.push(view) }).catch(() => {});
  await new Promise(resolve => setTimeout(resolve, 10));
  assert.deepEqual(views[0], { cursor: '1', synced: false, orders: [{ id: '1', version: 1, status: 'PENDING' }] });
  release(); await session;
  assert.equal(views.at(-1).synced, true);
  store.close();
});

// Propriedade: com cache eterno, frames perdidos, duplicados e reordenados, clientes
// entrando e reconectando em qualquer momento, todos terminam no estado exato
// do banco depois de um checkpoint. Semente fixa para reprodutibilidade.
test('propriedade: perda, duplicação, reordenação, joins tardios e cache eterno', async () => {
  for (let seed = 1; seed <= 25; seed++) {
    let x = seed;
    const rand = () => (x = (x * 1103515245 + 12345) % 2 ** 31) / 2 ** 31;
    const store = new Store(); const api = cachedApi(store);
    const clients = [];
    const inflight = [];
    for (let step = 0; step < 150; step++) {
      const r = rand();
      if (r < 0.55) {
        const id = String(Math.floor(rand() * 6));
        const exists = store.snapshot(scope).orders.some(order => order.id === id);
        const change = exists && rand() < 0.2 ? store.change(scope, id, null, true) : store.change(scope, id, ['PENDING', 'PAID', 'SHIPPED'][Math.floor(rand() * 3)]);
        if (rand() < 0.15) continue;                    // perdido no stream
        inflight.push(changeFrame(change));
        if (rand() < 0.1) inflight.push(changeFrame(change)); // duplicado
      } else if (r < 0.65) {
        inflight.push(checkpoint(store));
      } else if (r < 0.72) {
        if (rand() < 0.5) await api.snapshot('0');      // alguém aquece a base compartilhada
        clients.push(new Synchronizer({ scopeId: scope, api, pageSize: 7, maxReplay: 40 }));
      } else if (r < 0.75 && clients.length) {
        clients[Math.floor(rand() * clients.length)].synced = false; // reconexão: mantém estado
      } else if (r < 0.8) {
        store.prune(scope, Math.floor(store.head(scope) * rand() * 0.5)); // retenção
      } else if (inflight.length) {
        // Entrega fora de ordem: pega um frame qualquer dos pendentes.
        const [frame] = inflight.splice(Math.floor(rand() * Math.min(3, inflight.length)), 1);
        for (const client of clients) await client.accept(frame);
      }
    }
    for (const frame of inflight) for (const client of clients) await client.accept(frame);
    clients.push(new Synchronizer({ scopeId: scope, api }));
    for (const client of clients) {
      await client.accept(checkpoint(store));
      assert.deepEqual({ cursor: String(client.cursor), orders: client.view().orders }, truth(store), `seed ${seed}`);
    }
    store.close();
  }
});

test('falha ao buscar a base não fica guardada: o próximo frame tenta de novo', async () => {
  const store = new Store(); const real = cachedApi(store);
  let fail = true;
  const api = { ...real, snapshot: async min => { if (fail) throw new Error('rede'); return real.snapshot(min); } };
  const sync = new Synchronizer({ scopeId: scope, api });
  await assert.rejects(sync.accept(checkpoint(store)), /rede/);
  fail = false;
  await sync.accept(checkpoint(store));
  assert.equal(sync.cursor, 0);
  store.close();
});
