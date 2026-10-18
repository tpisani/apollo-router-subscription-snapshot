import { spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { startSubgraphs } from '../subgraphs/app.mjs';
import { composeSupergraph } from './compose.mjs';
import { startBalancer } from '../emulator/balancer.mjs';
import { verifyRouterScale } from './verify-router-scale.mjs';

// O teste de verdade, sem Docker: o binário da raiz do repositório (apollo-router 2.18.0 +
// subscription_snapshot) com GraphOS, em 3 instâncias com retenção atrás de um
// balanceador e 1 sem retenção, contra os subgraphs Node. Os processos herdam o
// ambiente (proxy, CA), o que os containers do compose não fazem numa sessão na nuvem.
// Exige APOLLO_KEY e APOLLO_GRAPH_REF no ambiente; nunca os imprime.
//   npm run test:router-real                  (subgraphs por WebSocket, passthrough)
//   MODE=callback npm run test:router-real    (subgraphs por HTTP callback)
// Raiz do repositório: o crate do plugin e examples/router/.
const root = new URL('../..', import.meta.url).pathname;
// ROUTER_BIN: outro binário (ex.: o plugin compilado contra uma versão nova do Router).
const bin = process.env.ROUTER_BIN ?? join(root, 'target/release/router');
if (!process.env.APOLLO_KEY || !process.env.APOLLO_GRAPH_REF) {
  console.error('Defina APOLLO_KEY e APOLLO_GRAPH_REF no ambiente (configurações do ambiente, nunca no repositório).');
  process.exit(2);
}
if (!existsSync(bin)) {
  console.log('Compilando o router customizado (a primeira vez leva ~20 min)...');
  if (spawnSync('cargo', ['build', '--release'], { cwd: root, stdio: 'inherit' }).status !== 0) process.exit(1);
}

const mode = process.env.MODE ?? 'passthrough';
if (!['passthrough', 'callback'].includes(mode)) throw new Error(`MODE inválido: ${mode}`);
// ROUTER_CONFIG_DIR: outra pasta com os dois YAMLs (ex.: saída de `router config upgrade`).
const configDir = process.env.ROUTER_CONFIG_DIR ?? join(root, 'examples/router');
const config = join(configDir, mode === 'callback' ? 'router-plugin-callback.yaml' : 'router-plugin.yaml');
const dir = mkdtempSync(join(tmpdir(), 'router-real-'));
const periodicMs = Number(process.env.PERIODIC_MS ?? 3000);
// Janela de início curta (padrão 2 s, em vez de 15 min) para a etapa do ciclo de vida.
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
  // Um processo morto por sinal fica com exitCode null e signalCode preenchido.
  const running = i => i.proc.exitCode === null && i.proc.signalCode === null;
  for (const i of instances) if (running(i)) i.proc.kill('SIGTERM');
  await Promise.all(instances.map(i => running(i) ? new Promise(r => i.proc.once('exit', r)) : null));
  await subgraphs.close();
  rmSync(dir, { recursive: true, force: true });
};
const errors = logs => [...logs.matchAll(/"level":"ERROR","message":"([^"]{0,300})/g)].map(m => m[1]);

let code = 1;
try {
  // Pronto = health check respondendo; a licença vem do GraphOS na inicialização.
  for (const i of instances) {
    const end = Date.now() + 60_000;
    while (!(await fetch(i.health).then(r => r.ok, () => false))) {
      if (i.proc.exitCode !== null || i.proc.signalCode !== null || Date.now() > end) throw new Error(`${i.name} não ficou pronto:\n${errors(i.logs).join('\n') || i.logs.slice(-2000)}`);
      await delay(250);
    }
  }
  console.log(`modo ${mode}: 4 routers prontos (${instances.map(i => `${i.name}:${i.port}`).join(', ')}), frame periódico de ${periodicMs} ms`);
  lb = await startBalancer({ targets: instances.slice(0, 3).map(i => i.url), port: 4200 });
  const killed = instances[1];
  const result = await verifyRouterScale({
    lbUrl: lb.url, routerUrls: instances.slice(0, 3).map(i => i.url), plainUrl: instances[3].url,
    metrics: async () => ({ scoreboard: (await (await fetch(`${base('scores')}/metrics`)).json()).scoreboard, stats: (await (await fetch(`${base('stats')}/metrics`)).json()).stats }),
    direct: { scores: `${base('scores')}/graphql`, stats: `${base('stats')}/graphql` }, lead,
    kill: process.env.KILL === '0' ? undefined : async () => { lb.remove(killed.url); killed.proc.kill('SIGKILL'); },
    log: console.log
  });
  console.log(JSON.stringify({ router: '2.18.0 + subscription_snapshot', modo: mode, instancias: 3, periodicoMs: periodicMs, ...result }, null, 2));
  console.log(`OK: Apollo Router de verdade com o plugin, em várias instâncias (modo ${mode})`);
  code = 0;
} catch (error) {
  console.error(`FALHOU: ${error.stack ?? error}`);
  for (const i of instances) { const e = errors(i.logs); if (e.length) console.error(`${i.name}: ${e.slice(-3).join(' | ')}`); }
} finally {
  await stop();
}
process.exit(code);
