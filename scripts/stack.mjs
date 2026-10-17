import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { startSubgraphs } from '../src/app.mjs';
import { composeSupergraph } from './compose.mjs';
import { startEdge } from './edge.mjs';

const freePort = () => new Promise(resolve => {
  const server = createServer().listen(0, '127.0.0.1', () => { const { port } = server.address(); server.close(() => resolve(port)); });
});

// Subgraphs no processo do teste (para inspecionar métricas) + Hive Gateway real
// como processo separado + edge (dedup e cache, ver edge.mjs) na frente do gateway.
export async function startStack({ edge: edgeOptions = {}, ...options } = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'realtime-stack-'));
  const config = { dbPath: join(dir, 'db.sqlite'), ordersPort: 0, catalogPort: 0, host: '127.0.0.1', ...options };
  let subgraphs = await startSubgraphs(config);
  Object.assign(config, { ordersPort: subgraphs.ports.orders, catalogPort: subgraphs.ports.catalog });
  const supergraph = join(dir, 'supergraph.graphql');
  writeFileSync(supergraph, composeSupergraph({
    ordersUrl: `http://127.0.0.1:${subgraphs.ports.orders}/graphql`,
    catalogUrl: `http://127.0.0.1:${subgraphs.ports.catalog}/graphql`
  }));
  const port = await freePort();
  const gateway = spawn(process.execPath, ['node_modules/@graphql-hive/gateway/dist/bin.js', 'supergraph', supergraph,
    '-c', 'gateway/gateway.config.mjs', '--port', String(port), '--host', '127.0.0.1'],
  { env: { ...process.env, SUPERGRAPH_PATH: supergraph }, stdio: ['ignore', 'pipe', 'pipe'] });
  let logs = ''; gateway.stdout.on('data', b => logs += b); gateway.stderr.on('data', b => logs += b);
  const url = `http://127.0.0.1:${port}/graphql`;
  let edge;
  const stop = async () => {
    await edge?.close();
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
      edge = await startEdge({ upstream: url, ...edgeOptions });
      return {
        url, edge, get subgraphs() { return subgraphs; }, stop, logs: () => logs,
        // Simula deploy/crash do subgraph mantendo banco e portas.
        async restartSubgraphs() { await subgraphs.close(); subgraphs = await startSubgraphs(config); }
      };
    }
    await delay(100);
  }
  await stop();
  throw new Error(`Gateway não iniciou:\n${logs}`);
}
