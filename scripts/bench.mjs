import { setTimeout as delay } from 'node:timers/promises';
import { FeedState, runFeed } from '../client/feed-client.mjs';
import { feedQuery, graphql, wsTransport } from '../client/transport.mjs';
import { startStack } from './stack.mjs';

// Latência commit → aplicação no cliente (committedAt vem do subgraph; mesmo relógio local).
// Sem GRAPHQL_URL, sobe subgraphs + gateway próprios.
const clients = Number(process.env.CLIENTS ?? 50);
const rate = Number(process.env.RATE ?? 50);          // commits por segundo
const seconds = Number(process.env.SECONDS ?? 10);
const enrich = process.env.ENRICH === '1';            // inclui details.label (catalog) em cada frame
// Os subgraphs do modo autônomo rodam no mesmo processo dos clientes: números ilustrativos.
const stack = process.env.GRAPHQL_URL ? null : await startStack({ queueLimit: 10_000 });
const url = process.env.GRAPHQL_URL ?? stack.url;
const scopeId = `bench-${Date.now()}`;

const query = feedQuery({ enrich });

const latencies = []; let reconnects = 0;
const controller = new AbortController();
const sessions = Array.from({ length: clients }, () => {
  const feedState = new FeedState(scopeId);
  const transport = wsTransport(url, { query });
  const done = runFeed({ state: feedState, subscribe: transport.subscribe, signal: controller.signal,
    onFrame: frame => { const now = Date.now(); for (const c of frame.changes) latencies.push(now - c.committedAt); },
    onError: () => reconnects++ });
  return { feedState, transport, done };
});
while (sessions.some(s => s.feedState.cursor === null)) await delay(20);

const total = rate * seconds; const started = Date.now();
for (let i = 0; i < total; i++) {
  const target = started + (i * 1000) / rate;
  if (target > Date.now()) await delay(target - Date.now());
  graphql(url, 'mutation($s: ID!, $id: ID!, $v: String!) { upsertOrder(scopeId: $s, id: $id, status: $v) { cursor } }', { s: scopeId, id: String(i % 100), v: ['PENDING', 'PAID', 'SHIPPED'][i % 3] });
}
const deadline = Date.now() + 10_000;
while (Date.now() < deadline && sessions.some(s => s.feedState.cursor < total)) await delay(50);

controller.abort();
await Promise.all(sessions.map(async s => { await s.transport.dispose(); await s.done; }));
const metrics = stack ? stack.subgraphs.metrics : null;
await stack?.stop();

latencies.sort((a, b) => a - b);
const pct = p => latencies[Math.min(latencies.length - 1, Math.floor(latencies.length * p))];
const converged = sessions.filter(s => s.feedState.cursor === total).length;
console.log(JSON.stringify({
  clients, rate, seconds, enrich, deliveries: latencies.length, expected: clients * total, converged, reconnects,
  latencyMs: { p50: pct(0.5), p95: pct(0.95), p99: pct(0.99), max: latencies.at(-1) },
  framesSent: metrics?.framesSent
}, null, 2));
