import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { startSubgraphs } from '../src/app.mjs';
import { composeSupergraph } from './compose.mjs';
import { startEdge } from './edge.mjs';
import { startBalancer } from './balancer.mjs';

const freePort = () => new Promise(resolve => {
  const server = createServer().listen(0, '127.0.0.1', () => { const { port } = server.address(); server.close(() => resolve(port)); });
});

// Subgraphs no processo do teste (para inspecionar métricas) + Hive Gateway real
// como processo separado + edges (dedup, retenção e cache, ver edge.mjs) na frente
// do gateway. Com edgeCount > 1, um balanceador round-robin na frente dos edges
// simula várias instâncias do Router; o cache de queries é compartilhado (Redis).
export async function startStack({ edge: edgeOptions = {}, edgeCount = 1, ...options } = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'realtime-stack-'));
  const config = { dataDir: dir, ports: { scores: 0, catalog: 0, stats: 0 }, host: '127.0.0.1', ...options };
  let subgraphs = await startSubgraphs(config);
  config.ports = { ...subgraphs.ports };
  const supergraph = join(dir, 'supergraph.graphql');
  writeFileSync(supergraph, composeSupergraph({
    scoresUrl: `http://127.0.0.1:${subgraphs.ports.scores}/graphql`,
    catalogUrl: `http://127.0.0.1:${subgraphs.ports.catalog}/graphql`,
    statsUrl: `http://127.0.0.1:${subgraphs.ports.stats}/graphql`
  }));
  const port = await freePort();
  const gateway = spawn(process.execPath, ['node_modules/@graphql-hive/gateway/dist/bin.js', 'supergraph', supergraph,
    '-c', 'gateway/gateway.config.mjs', '--port', String(port), '--host', '127.0.0.1'],
  { env: { ...process.env, SUPERGRAPH_PATH: supergraph }, stdio: ['ignore', 'pipe', 'pipe'] });
  let logs = ''; gateway.stdout.on('data', b => logs += b); gateway.stderr.on('data', b => logs += b);
  const url = `http://127.0.0.1:${port}/graphql`;
  let edges = []; let balancer;
  const stop = async () => {
    await balancer?.close();
    await Promise.all(edges.map(e => e.close().catch(() => {})));
    if (gateway.exitCode === null) {
      gateway.kill('SIGTERM');
      await Promise.race([new Promise(resolve => gateway.once('exit', resolve)), delay(3000)]);
      if (gateway.exitCode === null) gateway.kill('SIGKILL');
    }
    await subgraphs.close();
    rmSync(dir, { recursive: true, force: true });
  };
  for (let i = 0; i < 200; i++) {
    if (gateway.exitCode !== null) break;
    const ok = await fetch(`http://127.0.0.1:${port}/readiness`).then(r => r.ok, () => false);
    if (ok) {
      const cache = new Map();
      edges = await Promise.all(Array.from({ length: edgeCount }, (_, i) => startEdge({ upstream: url, cache, name: `edge-${i + 1}`, ...edgeOptions })));
      if (edgeCount > 1) balancer = await startBalancer({ targets: edges.map(e => e.url) });
      return {
        url, edge: edges[0], edges, balancer, get subgraphs() { return subgraphs; }, stop, logs: () => logs,
        // Derruba uma instância de edge (como um Router que cai): os clientes dela reconectam pelo balanceador.
        async killEdge(i) { balancer?.remove(edges[i].url); await edges[i].close(); },
        // Simula deploy/crash do subgraph mantendo banco e portas.
        async restartSubgraphs() { await subgraphs.close(); subgraphs = await startSubgraphs(config); }
      };
    }
    await delay(100);
  }
  await stop();
  throw new Error(`Gateway não iniciou:\n${logs}`);
}
