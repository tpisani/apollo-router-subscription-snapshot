import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { startStack } from '../scripts/stack.mjs';
import { FeedState, runFeed } from '../client/feed-client.mjs';
import { feedQuery, graphql, wsTransport } from '../client/transport.mjs';

async function until(predicate, message, ms = 5000) {
  for (const end = Date.now() + ms; Date.now() < end; await delay(10)) if (predicate()) return;
  throw new Error(`Timeout: ${message}`);
}

// Um cliente real: graphql-ws -> Hive Gateway -> graphql-ws -> subgraph orders,
// com details.label resolvido no subgraph catalog a cada frame.
function connect(url, scopeId) {
  const state = new FeedState(scopeId);
  const transport = wsTransport(url, { query: feedQuery({ enrich: true }) });
  const controller = new AbortController();
  const frames = []; const errors = []; const latencies = [];
  const done = runFeed({ state, subscribe: transport.subscribe, signal: controller.signal, wait: () => 50,
    onFrame: frame => { frames.push(frame); for (const change of frame.changes) latencies.push(Date.now() - change.committedAt); },
    onError: error => errors.push(error) });
  return { state, transport, frames, errors, latencies,
    async close() { controller.abort(); await transport.dispose(); await done; } };
}

test('gateway federado: snapshot, ao vivo, late joiner, retomada por cursor e enriquecimento', { timeout: 60_000 }, async () => {
  const stack = await startStack();
  const { url, subgraphs } = stack;
  const scopeId = 'loja-e2e';
  const write = (id, status) => graphql(url, `mutation($s: ID!, $id: ID!, $v: String!) {
    upsertOrder(scopeId: $s, id: $id, status: $v) { cursor } }`, { s: scopeId, id, v: status });
  const remove = id => graphql(url, 'mutation($s: ID!, $id: ID!) { deleteOrder(scopeId: $s, id: $id) { cursor } }', { s: scopeId, id });
  const truth = async () => {
    const { ordersSnapshot } = await graphql(url, `query($s: ID!) { ordersSnapshot(scopeId: $s) { cursor orders { id version status details { label } } } }`, { s: scopeId });
    return ordersSnapshot;
  };
  const clients = [];
  try {
    await write('1', 'PENDING'); await write('2', 'PENDING');
    const a = connect(url, scopeId); clients.push(a);
    await until(() => a.state.cursor === 2, 'snapshot inicial');
    assert.equal(a.frames[0].kind, 'SNAPSHOT');
    assert.equal(a.state.orders.get('1').details.label, 'Pedido 1', 'campo do subgraph catalog no frame');

    await write('1', 'PAID');
    await until(() => a.state.orders.get('1').status === 'PAID', 'evento ao vivo');
    assert.equal(a.frames.at(-1).kind, 'CHANGES');

    const b = connect(url, scopeId); clients.push(b); // Late joiner.
    await until(() => b.state.cursor === 3, 'late joiner');
    await remove('2');
    await until(() => [a, b].every(c => c.state.cursor === 4), 'exclusão nos dois clientes');
    assert.deepEqual(a.state.view(), b.state.view());
    assert.equal(a.state.orders.has('2'), false);

    // Derruba a conexão de A (sem aviso) e escreve enquanto ele está fora.
    const resumesBefore = subgraphs.metrics.resumeStarts;
    const framesBefore = a.frames.length;
    a.transport.client.terminate();
    await write('3', 'PENDING'); await write('3', 'SHIPPED');
    await until(() => a.state.cursor === 6, 'retomada após queda');
    assert.equal(subgraphs.metrics.resumeStarts, resumesBefore + 1, 'reconexão usou after, não snapshot');
    assert.ok(a.frames.slice(framesBefore).every(frame => frame.kind === 'CHANGES'), 'sem SNAPSHOT na retomada');
    assert.ok(a.errors.length >= 1);

    // Rajada: todos convergem para o estado do banco, sem lacunas.
    await Promise.all(Array.from({ length: 30 }, (_, i) => write(String(i % 5), ['PENDING', 'PAID', 'SHIPPED'][i % 3])));
    const final = await truth();
    await until(() => clients.every(c => String(c.state.cursor) === final.cursor), 'convergência após rajada');
    for (const client of clients) assert.deepEqual(client.state.view(), { cursor: final.cursor, orders: final.orders });

    const p95 = values => values.sort((x, y) => x - y)[Math.floor(values.length * 0.95)];
    console.log(`latência commit→cliente via gateway: p95 ${p95(a.latencies)} ms em ${a.latencies.length} eventos`);
    assert.ok(p95(b.latencies) < 500, 'entrega ao vivo deve ficar bem abaixo de 500 ms localmente');
  } finally {
    await Promise.all(clients.map(client => client.close()));
    await until(() => subgraphs.metrics.activeFeeds === 0 && subgraphs.bus.size === 0, 'feeds liberados no subgraph').catch(error => {
      assert.fail(`${error.message}: ${JSON.stringify(subgraphs.metrics)}`);
    }).finally(() => stack.stop());
  }
});

test('histórico expirado na reconexão vira SNAPSHOT', { timeout: 30_000 }, async () => {
  const stack = await startStack();
  const { url, subgraphs } = stack;
  const scopeId = 'loja-expirada';
  let client;
  try {
    subgraphs.store.change(scopeId, '1', 'PENDING');
    client = connect(url, scopeId);
    await until(() => client.state.cursor === 1, 'snapshot');
    client.transport.client.terminate();
    subgraphs.store.change(scopeId, '1', 'PAID'); subgraphs.store.change(scopeId, '2', 'PAID');
    subgraphs.store.prune(scopeId, 3);
    await until(() => client.state.cursor === 3, 'recuperação');
    assert.equal(client.frames.at(-1).kind, 'SNAPSHOT');
    assert.equal(client.state.orders.get('1').status, 'PAID');
  } finally { await client?.close(); await stack.stop(); }
});

test('reinício do subgraph: o feed continua sem perder commits', { timeout: 30_000 }, async () => {
  const stack = await startStack();
  const scopeId = 'loja-restart';
  let client;
  try {
    stack.subgraphs.store.change(scopeId, '1', 'PENDING');
    client = connect(stack.url, scopeId);
    await until(() => client.state.cursor === 1, 'snapshot');
    await stack.restartSubgraphs();
    // Commits logo após o reinício, antes de o feed ser restabelecido.
    stack.subgraphs.store.change(scopeId, '1', 'PAID'); stack.subgraphs.store.change(scopeId, '2', 'SHIPPED');
    await until(() => client.state.cursor === 3, 'recuperação após reinício', 15_000);
    const fresh = stack.subgraphs.store.snapshot(scopeId);
    assert.deepEqual(client.state.view().orders.map(({ details, ...order }) => order), fresh.orders);
    stack.subgraphs.store.change(scopeId, '2', null, true);
    await until(() => client.state.cursor === 4 && !client.state.orders.has('2'), 'ao vivo após reinício');
  } finally { await client?.close(); await stack.stop(); }
});
