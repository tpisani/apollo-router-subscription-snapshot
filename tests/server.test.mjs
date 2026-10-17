import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Store, SnapshotCache } from '../src/store.mjs';
import { Hub, LiveStream } from '../src/live.mjs';
import { startSubgraphs } from '../src/app.mjs';
import { multipart } from '../client/transport.mjs';

const scope = 'loja-42';
function setup({ checkpointMs = 60_000, queueLimit = 1000 } = {}) {
  let hub;
  const store = new Store(':memory:', { onCommit: change => hub.publish(change) });
  hub = new Hub({ head: s => store.head(s), checkpointMs });
  const open = () => new LiveStream({ hub, scopeId: scope, queueLimit });
  return { store, hub, open };
}

test('stream: checkpoint imediato na abertura e CHANGE empurrado no commit', async () => {
  const { store, open } = setup();
  store.change(scope, '1', 'PENDING');
  const stream = open();
  assert.deepEqual((await stream.next()).value, { scopeId: scope, kind: 'CHECKPOINT', cursor: '1', change: null });
  const pending = stream.next();
  store.change(scope, '1', 'PAID');
  const frame = (await pending).value;
  assert.equal(frame.kind, 'CHANGE'); assert.equal(frame.cursor, '2'); assert.equal(frame.change.order.status, 'PAID');
  await stream.return(); store.close();
});

test('stream: checkpoints periódicos com o head durável, um timer por escopo', async () => {
  const { store, hub, open } = setup({ checkpointMs: 20 });
  const [a, b] = [open(), open()];
  await a.next(); await b.next();
  store.change(scope, '1', 'PAID');
  const kinds = [];
  for (let i = 0; i < 3; i++) kinds.push((await a.next()).value);
  assert.equal(kinds[0].kind, 'CHANGE');
  assert.deepEqual(kinds.slice(1).map(f => [f.kind, f.cursor]), [['CHECKPOINT', '1'], ['CHECKPOINT', '1']]);
  assert.equal(hub.scopes.size, 1);
  await a.return(); await b.return();
  assert.equal(hub.scopes.size, 0, 'timer removido com o último assinante');
  store.close();
});

test('stream: return() libera o listener mesmo aguardando', async () => {
  const { hub, open, store } = setup();
  const stream = open(); await stream.next();
  const waiting = stream.next();
  assert.equal(hub.size, 1);
  await stream.return();
  assert.equal(hub.size, 0);
  assert.deepEqual(await waiting, { done: true, value: undefined });
  store.close();
});

test('stream: fila cheia vira um único CHECKPOINT com o head (conflação)', async () => {
  const { store, open } = setup({ queueLimit: 5 });
  const stream = open(); await stream.next();
  for (let i = 0; i < 12; i++) store.change(scope, '1', 'PAID');
  const frames = [];
  while (stream.queue.length) frames.push((await stream.next()).value);
  // 1..5 enchem a fila; 6 e 11 a transbordam e a substituem pelo head do momento.
  assert.deepEqual(frames.map(f => [f.kind, f.cursor]), [['CHECKPOINT', '11'], ['CHANGE', '12']]);
  await stream.return(); store.close();
});

test('changes: intervalo fechado e imutável; além do head, expirado e grande demais são recusados', () => {
  const store = new Store();
  for (let i = 0; i < 5; i++) store.change(scope, String(i), 'PENDING');
  const page = store.changes(scope, '1', '4');
  assert.deepEqual(page.events.map(e => e.cursor), ['2', '3', '4']);
  store.change(scope, '9', 'PAID');
  assert.deepEqual(store.changes(scope, '1', '4'), page, 'escritas depois não mudam um intervalo fechado');
  assert.throws(() => store.changes(scope, '0', '7'), error => error.code === 'NOT_YET');
  assert.throws(() => store.changes(scope, '0', '501'), error => error.code === 'BAD_USER_INPUT');
  store.prune(scope, 3);
  assert.throws(() => store.changes(scope, '2', '4'), error => error.code === 'CURSOR_EXPIRED');
  assert.equal(store.changes(scope, '3', '6').events.length, 3);
  store.close();
});

test('snapshot cache: serve qualquer estado >= minCursor e nunca abaixo dele', () => {
  const store = new Store(); let now = 0;
  const cache = new SnapshotCache(store, 1000, () => now);
  store.change(scope, '1', 'PENDING');
  assert.equal(cache.get(scope, 0).cursor, '1');
  store.change(scope, '1', 'PAID');
  assert.equal(cache.get(scope, 0).cursor, '1', 'antigo, mas satisfaz minCursor 0');
  assert.equal(cache.get(scope, 2).cursor, '2', 'minCursor acima do cache força leitura');
  assert.throws(() => cache.get(scope, 3), error => error.code === 'NOT_YET');
  now = 2000; store.change(scope, '1', 'SHIPPED');
  assert.equal(cache.get(scope, 0).cursor, '3', 'TTL expirado');
  store.close();
});

test('escrita atômica: falha não altera projeção nem publica', () => {
  let published = 0;
  const store = new Store(':memory:', { onCommit: () => published++ });
  store.db.exec("CREATE TRIGGER fail_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'falha'); END;");
  assert.throws(() => store.change(scope, '1', 'PAID'), /falha/);
  assert.deepEqual(store.snapshot(scope), { scopeId: scope, cursor: '0', orders: [] });
  assert.equal(published, 0);
  store.close();
});

test('persistência e sequências independentes por escopo', () => {
  const dir = mkdtempSync(join(tmpdir(), 'store-')); let store;
  try {
    store = new Store(join(dir, 'db.sqlite')); store.change(scope, '1', 'PAID'); store.change('outra', '1', 'SHIPPED'); store.close();
    store = new Store(join(dir, 'db.sqlite'));
    assert.equal(store.head(scope), 1); assert.equal(store.head('outra'), 1);
    assert.equal(store.changes(scope, '0', '1').events[0].order.status, 'PAID');
  } finally { store?.close(); rmSync(dir, { recursive: true, force: true }); }
});

test('Cache-Control do subgraph segue o contrato de cada campo', async () => {
  const app = await startSubgraphs({ ordersPort: 0, catalogPort: 0, host: '127.0.0.1' });
  const post = async (port, query) => {
    const response = await fetch(`http://127.0.0.1:${port}/graphql`, { method: 'POST',
      headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query }) });
    await response.json(); return response.headers.get('cache-control');
  };
  const o = app.ports.orders;
  try {
    assert.equal(await post(o, 'mutation { upsertOrder(scopeId: "x", id: "1", status: "PAID") { cursor } }'), 'no-store');
    assert.equal(await post(o, '{ ordersSnapshot(scopeId: "x") { cursor } }'), 'public, max-age=60');
    assert.equal(await post(o, '{ ordersSnapshot(scopeId: "x", minCursor: "1") { cursor } }'), 'public, max-age=86400');
    assert.equal(await post(o, '{ orderChanges(scopeId: "x", after: "0", through: "1") { through } }'), 'public, max-age=31536000, immutable');
    assert.equal(await post(o, '{ a: ordersSnapshot(scopeId: "x") { cursor } b: orderChanges(scopeId: "x", after: "0", through: "1") { through } }'), 'public, max-age=60', 'menor TTL vence');
    assert.equal(await post(o, '{ orderChanges(scopeId: "x", after: "0", through: "9") { through } }'), 'no-store', 'erro nunca é cacheável');
    assert.equal(await post(o, '{ ordersSnapshot(scopeId: "x", minCursor: "9") { cursor } }'), 'no-store');
    assert.equal(await post(app.ports.catalog, '{ _entities(representations: [{ __typename: "Order", id: "1" }]) { ... on Order { label } } }'), 'public, max-age=300');
  } finally { await app.close(); }
});

test('parser multipart: fronteiras e UTF-8 partidos em chunks de 1 byte, fim e interrupção', async () => {
  const messages = [{}, { payload: { data: { orderEvents: { status: 'Expedição' } } } }];
  const wire = '\r\n--graphql' + messages.map(m => `\r\ncontent-type: application/json\r\n\r\n${JSON.stringify(m)}\r\n--graphql`).join('') + '--\r\n';
  const bytes = new TextEncoder().encode(wire);
  async function* chunks() { for (const byte of bytes) yield Uint8Array.of(byte); }
  const result = []; for await (const part of multipart(chunks(), 'graphql')) result.push(part);
  assert.deepEqual(result, messages);
  async function* cut() { yield new TextEncoder().encode('\r\n--graphql\r\ncontent-type: application/json\r\n\r\n{}'); }
  await assert.rejects(async () => { for await (const _ of multipart(cut(), 'graphql')) { await delay(0); } }, /interrompido/);
});
