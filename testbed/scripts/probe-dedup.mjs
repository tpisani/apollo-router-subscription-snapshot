import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { startSubgraphs } from '../subgraphs/app.mjs';
import { composeSupergraph } from './compose.mjs';
import { transports } from '../client/transport.mjs';

// Probe: N simultaneous clients on the same match, on a single instance; how many upstreams the
// Router opens on the subgraph, right after and a few seconds later. MODE=passthrough|callback,
// CLIENTS, TRIALS, RETAIN_ENABLED=false. Requires APOLLO_KEY/APOLLO_GRAPH_REF.
//   MODE=passthrough CLIENTS=8 TRIALS=6 node scripts/probe-dedup.mjs
const mode = process.env.MODE ?? 'passthrough'; const trials = Number(process.env.TRIALS ?? 8); const n = Number(process.env.CLIENTS ?? 4);
if (!process.env.APOLLO_KEY || !process.env.APOLLO_GRAPH_REF) { console.error('Set APOLLO_KEY and APOLLO_GRAPH_REF in the environment.'); process.exit(2); }
// Repository root: the plugin crate and examples/router/.

const root = new URL('../..', import.meta.url).pathname;
const dir = mkdtempSync(join(tmpdir(), 'probe-'));
const app = await startSubgraphs({ dataDir: dir, host: '127.0.0.1', ports: { scores: 0, catalog: 0, stats: 0 }, scoreboard: { periodicMs: 3000 } });
const base = name => `http://127.0.0.1:${app.ports[name]}`;
const sg = join(dir, 's.graphql');
writeFileSync(sg, composeSupergraph({ scoresUrl: `${base('scores')}/graphql`, catalogUrl: `${base('catalog')}/graphql`, statsUrl: `${base('stats')}/graphql` }));
const proc = spawn(join(root, 'target/release/router'), ['--config', join(root, mode === 'callback' ? 'examples/router/router-plugin-callback.yaml' : 'examples/router/router-plugin.yaml'), '--supergraph', sg],
  { env: { ...process.env, ROUTER_LISTEN: '127.0.0.1:4111', ROUTER_HEALTH_LISTEN: '127.0.0.1:8111', ROUTER_CALLBACK_URL: 'http://127.0.0.1:4111/callback', RUST_BACKTRACE: '0' }, stdio: 'ignore' });
while (!(await fetch('http://127.0.0.1:8111/health?ready').then(r => r.ok, () => false))) await delay(200);
const results = [];
for (let t = 0; t < trials; t++) {
  const m = `probe-${t}-${Date.now()}`;
  const controller = new AbortController(); let frames = 0;
  for (let i = 0; i < n; i++) (async () => { try { for await (const _ of transports.multipart('http://127.0.0.1:4111/graphql', 'scoreboard', m, { signal: controller.signal })) frames++; } catch {} })();
  while (frames < n) await delay(20);
  const at = ms => delay(ms).then(() => app.metrics.scoreboard.activeByMatch[m] ?? 0);
  const early = await at(300); const late = await at(4000);
  results.push({ early, late, streamsOpened: app.metrics.scoreboard.streamsOpened });
  controller.abort(); await delay(500);
}
console.log(JSON.stringify({ mode, clients: n, results }));
proc.kill('SIGTERM'); await new Promise(r => proc.once('exit', r)); await app.close(); process.exit(0);
