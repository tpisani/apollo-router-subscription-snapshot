import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startSubgraphs } from '../src/app.mjs';
import { composeSupergraph } from './compose.mjs';

// Sobe os subgraphs (frame periódico de 3 s, para a retenção ficar visível), compõe o
// supergraph e roda os testes de integração do plugin no pipeline do Router
// (router-plugin/src/harness.rs). Sem licença: o TestHarness do crate é só para testes.
const dir = mkdtempSync(join(tmpdir(), 'router-harness-'));
const subgraphs = await startSubgraphs({
  dataDir: dir, host: '127.0.0.1', ports: { scores: 0, catalog: 0, stats: 0 },
  scoreboard: { periodicMs: 3000 }, stats: { periodicMs: 3000, throttleMs: 500 }
});
const base = name => `http://127.0.0.1:${subgraphs.ports[name]}`;
const supergraph = join(dir, 'supergraph.graphql');
writeFileSync(supergraph, composeSupergraph({ scoresUrl: `${base('scores')}/graphql`, catalogUrl: `${base('catalog')}/graphql`, statsUrl: `${base('stats')}/graphql` }));
const cargo = spawn('cargo', ['test', '--release', '--', '--ignored', '--nocapture', '--test-threads=1'], {
  cwd: new URL('../router-plugin/', import.meta.url), stdio: 'inherit',
  env: { ...process.env, SUPERGRAPH: supergraph, SCORES_URL: base('scores') }
});
const code = await new Promise(resolve => cargo.once('exit', resolve));
await subgraphs.close();
rmSync(dir, { recursive: true, force: true });
process.exit(code ?? 1);
