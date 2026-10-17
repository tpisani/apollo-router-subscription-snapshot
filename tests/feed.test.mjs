import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Store } from '../src/store.mjs';
import { Bus, Feed } from '../src/feed.mjs';
import { FeedState, GapError, runFeed } from '../client/feed-client.mjs';

const scope = 'loja-42';
function setup(options = {}) {
  const bus = new Bus();
  const store = new Store(':memory:', { onCommit: change => bus.publish(change) });
  const open = (after = null, extra = {}) => new Feed({ store, bus, scopeId: scope, after, ...options, ...extra });
  return { bus, store, open };
}
const truth = store => ({ cursor: store.snapshot(scope).cursor, orders: store.snapshot(scope).orders });

test('sem cursor: SNAPSHOT e depois cada commit ao vivo, sem polling', async () => {
  const { store, open } = setup();
  store.change(scope, '1', 'PENDING');
  const feed = open();
  assert.deepEqual((await feed.next()).value, { kind: 'SNAPSHOT', scopeId: scope, cursor: '1', orders: [{ id: '1', version: 1, status: 'PENDING' }], changes: [] });
  const pending = feed.next();
  store.change(scope, '1', 'PAID');
  const frame = (await pending).value;
  assert.equal(frame.kind, 'CHANGES'); assert.equal(frame.cursor, '2');
  assert.equal(frame.changes[0].order.status, 'PAID');
  await feed.return(); store.close();
});

test('com cursor: replay só do que falta, depois ao vivo', async () => {
  const { store, open } = setup();
  for (const status of ['PENDING', 'PAID', 'SHIPPED']) store.change(scope, '1', status);
  store.change(scope, '2', 'PENDING'); store.change(scope, '2', null, true);
  const feed = open('2');
  const first = (await feed.next()).value;
  assert.equal(first.kind, 'CHANGES');
  assert.deepEqual(first.changes.map(change => change.cursor), ['3', '4', '5']);
  await feed.return(); store.close();
});

test('cursor expirado, distante demais ou acima do head recebe SNAPSHOT', () => {
  const { store, open } = setup({ maxReplay: 3 });
  for (let i = 0; i < 6; i++) store.change(scope, String(i), 'PENDING');
  store.prune(scope, 2);
  assert.equal(open('1').initial.kind, 'SNAPSHOT', 'abaixo do floor');
  assert.equal(open('2').initial.kind, 'SNAPSHOT', 'mais que maxReplay eventos');
  assert.equal(open('3').initial.kind, 'CHANGES');
  assert.equal(open('99').initial.kind, 'SNAPSHOT', 'banco recriado');
  assert.throws(() => open('-1'), /Cursor inválido/);
  store.close();
});

test('escritas concorrentes à abertura e ao consumo nunca criam lacuna nem duplicata', async () => {
  const { store, open } = setup();
  const states = [];
  const feeds = [];
  for (let i = 0; i < 200; i++) {
    store.change(scope, String(i % 7), ['PENDING', 'PAID', 'SHIPPED'][i % 3]);
    if (i % 25 === 0) { feeds.push(open()); states.push(new FeedState(scope)); }
    if (i % 3 === 0) store.change(scope, String(i % 7), null, true);
  }
  for (let i = 0; i < feeds.length; i++) {
    while (states[i].cursor !== Number(store.head(scope))) states[i].apply((await feeds[i].next()).value);
    assert.deepEqual(states[i].view(), truth(store));
    await feeds[i].return();
  }
  store.close();
});

test('return() libera o listener mesmo aguardando o próximo commit', async () => {
  const { bus, open, store } = setup();
  const feed = open(); await feed.next();
  const waiting = feed.next();
  assert.equal(bus.size, 1);
  await feed.return();
  assert.equal(bus.size, 0);
  assert.deepEqual(await waiting, { done: true, value: undefined });
  store.close();
});

test('consumidor lento é encerrado com SLOW_CONSUMER em vez de acumular memória', async () => {
  const { open, store } = setup({ queueLimit: 5 });
  const feed = open(); await feed.next();
  for (let i = 0; i < 10; i++) store.change(scope, '1', 'PAID');
  await assert.rejects(feed.next(), error => error.code === 'SLOW_CONSUMER');
  store.close();
});

test('commits acumulados enquanto o consumidor processa saem num único frame', async () => {
  const { open, store } = setup();
  const feed = open(); await feed.next();
  for (let i = 0; i < 4; i++) store.change(scope, String(i), 'PAID');
  const frame = (await feed.next()).value;
  assert.deepEqual(frame.changes.map(change => change.cursor), ['1', '2', '3', '4']);
  await feed.return(); store.close();
});

test('FeedState ignora reentregas e rejeita lacunas e escopos trocados', () => {
  const state = new FeedState(scope);
  assert.throws(() => state.apply({ scopeId: scope, kind: 'CHANGES', cursor: '1', changes: [] }), GapError);
  state.apply({ scopeId: scope, kind: 'SNAPSHOT', cursor: '1', orders: [{ id: 'a', version: 1, status: 'PAID' }] });
  const change = (seq, status) => ({ cursor: String(seq), operation: 'UPSERT', order: { id: 'a', version: seq, status } });
  state.apply({ scopeId: scope, kind: 'CHANGES', cursor: '2', changes: [change(1, 'PENDING'), change(2, 'SHIPPED')] });
  assert.equal(state.orders.get('a').status, 'SHIPPED');
  assert.throws(() => state.apply({ scopeId: scope, kind: 'CHANGES', cursor: '4', changes: [change(4, 'PAID')] }), GapError);
  assert.throws(() => state.apply({ scopeId: scope, kind: 'CHANGES', cursor: '5', changes: [] }), GapError);
  assert.throws(() => state.apply({ scopeId: 'outra', kind: 'SNAPSHOT', cursor: '0', orders: [] }), /escopo/);
});

test('runFeed reconecta com o último cursor aplicado', async () => {
  const { store, open } = setup();
  const state = new FeedState(scope);
  const controller = new AbortController();
  const calls = [];
  let sessions = 0;
  const subscribe = async function* ({ after }) {
    calls.push(after);
    const feed = open(after);
    try {
      yield (await feed.next()).value;
      if (++sessions === 1) { store.change(scope, '1', 'PAID'); yield (await feed.next()).value; throw new Error('queda de rede'); }
      store.change(scope, '1', 'SHIPPED'); yield (await feed.next()).value;
      controller.abort();
    } finally { await feed.return(); }
  };
  store.change(scope, '1', 'PENDING');
  await runFeed({ state, subscribe, signal: controller.signal, wait: () => 0 });
  assert.deepEqual(calls, [null, '2']);
  assert.deepEqual(state.view(), truth(store));
  store.close();
});

test('projeção, head e log revertem juntos e nada é publicado em falha', () => {
  const { store, bus } = setup();
  let published = 0; bus.subscribe(scope, () => published++);
  store.db.exec("CREATE TRIGGER fail_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'falha'); END;");
  assert.throws(() => store.change(scope, '1', 'PAID'), /falha/);
  assert.deepEqual(store.snapshot(scope), { scopeId: scope, cursor: '0', orders: [] });
  assert.equal(published, 0);
  store.close();
});

test('estado e log sobrevivem ao reinício; escopos têm sequências independentes', () => {
  const dir = mkdtempSync(join(tmpdir(), 'feed-')); let store;
  try {
    store = new Store(join(dir, 'db.sqlite')); store.change(scope, '1', 'PAID'); store.change('outra', '1', 'SHIPPED'); store.close();
    store = new Store(join(dir, 'db.sqlite'));
    assert.equal(store.head(scope), 1); assert.equal(store.head('outra'), 1);
    assert.equal(store.catchUp(scope, '0').changes[0].order.status, 'PAID');
  } finally { store?.close(); rmSync(dir, { recursive: true, force: true }); }
});
