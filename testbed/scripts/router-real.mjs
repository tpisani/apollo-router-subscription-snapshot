import { spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { startSubgraphs } from '../subgraphs/app.mjs';
import { composeSupergraph } from './compose.mjs';
import { startBalancer } from '../emulator/balancer.mjs';
import { verifyRouterScale } from './verify-router-scale.mjs';

// The real test, without Docker: the binary from the repository root (apollo-router 2.18.0 +
// subscription_snapshot) with GraphOS, as 3 instances with retention behind a
// balancer and 1 without retention, against the Node subgraphs. The processes inherit the
// environment (proxy, CA), which the compose containers do not do in a cloud session.
// Requires APOLLO_KEY and APOLLO_GRAPH_REF in the environment; never prints them.
//   npm run test:router-real                  (subgraphs over WebSocket, passthrough)
//   MODE=callback npm run test:router-real    (subgraphs over HTTP callback)
// Repository root: the plugin crate and examples/router/.
const root = new URL('../..', import.meta.url).pathname;
// ROUTER_BIN: another binary (e.g. the plugin built against a new Router version).
const bin = process.env.ROUTER_BIN ?? join(root, 'target/release/router');
if (!process.env.APOLLO_KEY || !process.env.APOLLO_GRAPH_REF) {
  console.error('Set APOLLO_KEY and APOLLO_GRAPH_REF in the environment (environment settings, never in the repository).');
  process.exit(2);
}
if (!existsSync(bin)) {
  console.log('Building the custom router (the first time takes ~20 min)...');
  if (spawnSync('cargo', ['build', '--release'], { cwd: root, stdio: 'inherit' }).status !== 0) process.exit(1);
}

const mode = process.env.MODE ?? 'passthrough';
if (!['passthrough', 'callback'].includes(mode)) throw new Error(`Invalid MODE: ${mode}`);
// ROUTER_CONFIG_DIR: another directory with both YAMLs (e.g. the output of `router config upgrade`).
const configDir = process.env.ROUTER_CONFIG_DIR ?? join(root, 'examples/router');
const config = join(configDir, mode === 'callback' ? 'router-plugin-callback.yaml' : 'router-plugin.yaml');
const dir = mkdtempSync(join(tmpdir(), 'router-real-'));
const periodicMs = Number(process.env.PERIODIC_MS ?? 3000);
// Short kickoff window (2 s by default, instead of 15 min) for the lifecycle step.
const lead = Number(process.env.LEAD_MS ?? 2000);
const subgraphs = await startSubgraphs({
  dataDir: dir, host: '127.0.0.1', ports: { scores: 0, catalog: 0, stats: 0 },
  scoreboard: { periodicMs }, stats: { periodicMs, throttleMs: 500 }, liveLeadMs: lead
});
const base = name => `http://127.0.0.1:${subgraphs.ports[name]}`;
const supergraph = join(dir, 'supergraph.graphql');
writeFileSync(supergraph, composeSupergraph({ scoresUrl: `${base('scores')}/graphql`, catalogUrl: `${base('catalog')}/graphql`, statsUrl: `${base('stats')}/graphql` }));

const instances = [
  { name: 'router-a', port: 4101, retain: true }, { name: 'router-b', port: 4102, retain: true },
  { name: 'router-c', port: 4103, retain: true }, { name: 'router-plain', port: 4104, retain: false }
].map(i => ({ ...i, url: `http://127.0.0.1:${i.port}/graphql`, health: `http://127.0.0.1:${i.port + 4000}/health?ready`, logs: '' }));
for (const i of instances) {
  i.proc = spawn(bin, ['--config', config, '--supergraph', supergraph], {
    env: { ...process.env, ROUTER_LISTEN: `127.0.0.1:${i.port}`, ROUTER_CALLBACK_URL: `http://127.0.0.1:${i.port}/callback`, ROUTER_HEALTH_LISTEN: `127.0.0.1:${i.port + 4000}`, RETAIN_ENABLED: String(i.retain), RUST_BACKTRACE: '0' },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  for (const s of [i.proc.stdout, i.proc.stderr]) s.on('data', b => { i.logs = (i.logs + b).slice(-20_000); });
}
let lb;
const stop = async () => {
  await lb?.close();
  // A process killed by a signal has a null exitCode and a signalCode set.
  const running = i => i.proc.exitCode === null && i.proc.signalCode === null;
  for (const i of instances) if (running(i)) i.proc.kill('SIGTERM');
  await Promise.all(instances.map(i => running(i) ? new Promise(r => i.proc.once('exit', r)) : null));
  await subgraphs.close();
  rmSync(dir, { recursive: true, force: true });
};
const errors = logs => [...logs.matchAll(/"level":"ERROR","message":"([^"]{0,300})/g)].map(m => m[1]);

let code = 1;
try {
  // Ready = health check answering; the license comes from GraphOS at startup.
  for (const i of instances) {
    const end = Date.now() + 60_000;
    while (!(await fetch(i.health).then(r => r.ok, () => false))) {
      if (i.proc.exitCode !== null || i.proc.signalCode !== null || Date.now() > end) throw new Error(`${i.name} did not become ready:\n${errors(i.logs).join('\n') || i.logs.slice(-2000)}`);
      await delay(250);
    }
  }
  console.log(`mode ${mode}: 4 routers ready (${instances.map(i => `${i.name}:${i.port}`).join(', ')}), periodic frame of ${periodicMs} ms`);
  lb = await startBalancer({ targets: instances.slice(0, 3).map(i => i.url), port: 4200 });
  const killed = instances[1];
  const result = await verifyRouterScale({
    lbUrl: lb.url, routerUrls: instances.slice(0, 3).map(i => i.url), plainUrl: instances[3].url,
    metrics: async () => ({ scoreboard: (await (await fetch(`${base('scores')}/metrics`)).json()).scoreboard, stats: (await (await fetch(`${base('stats')}/metrics`)).json()).stats }),
    direct: { scores: `${base('scores')}/graphql`, stats: `${base('stats')}/graphql` }, lead,
    kill: process.env.KILL === '0' ? undefined : async () => { lb.remove(killed.url); killed.proc.kill('SIGKILL'); },
    log: console.log
  });
  console.log(JSON.stringify({ router: '2.18.0 + subscription_snapshot', mode, instances: 3, periodicMs, ...result }, null, 2));
  console.log(`OK: the real Apollo Router with the plugin, on several instances (mode ${mode})`);
  code = 0;
} catch (error) {
  console.error(`FAILED: ${error.stack ?? error}`
);
  for (const i of instances) { const e = errors(i.logs); if (e.length) console.error(`${i.name}: ${e.slice(-3).join(' | ')}`); }
} finally {
  await stop();
}
process.exit(code);
