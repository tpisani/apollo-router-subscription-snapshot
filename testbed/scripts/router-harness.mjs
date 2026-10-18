import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startSubgraphs } from '../subgraphs/app.mjs';
import { composeSupergraph } from './compose.mjs';

// Starts the subgraphs (periodic 3 s frame, so retention is visible), composes the
// supergraph and runs the plugin's integration tests in the Router pipeline
// (tests/router_pipeline.rs, at the repository root). No license: the crate's TestHarness is for tests only.
const dir = mkdtempSync(join(tmpdir(), 'router-harness-'));
const subgraphs = await startSubgraphs({
  dataDir: dir, host: '127.0.0.1', ports: { scores: 0, catalog: 0, stats: 0 },
  scoreboard: { periodicMs: 3000 }, stats: { periodicMs: 3000, throttleMs: 500 }
});
const base = name => `http://127.0.0.1:${subgraphs.ports[name]}`;
const supergraph = join(dir, 'supergraph.graphql');
writeFileSync(supergraph, composeSupergraph({ scoresUrl: `${base('scores')}/graphql`, catalogUrl: `${base('catalog')}/graphql`, statsUrl: `${base('stats')}/graphql` }));
// PLUGIN_DIR: another copy of the plugin (e.g. built against a new Router version,
// to check an upgrade); CARGO_TOOLCHAIN: e.g. 1.97.1.

const toolchain = process.env.CARGO_TOOLCHAIN ? [`+${process.env.CARGO_TOOLCHAIN}`] : [];
const cargo = spawn('cargo', [...toolchain, 'test', '--release', '--', '--include-ignored', '--nocapture', '--test-threads=1'], {
  cwd: process.env.PLUGIN_DIR ?? new URL('../../', import.meta.url), stdio: 'inherit',
  env: { ...process.env, SUPERGRAPH: supergraph, SCORES_URL: base('scores') }
});
const code = await new Promise(resolve => cargo.once('exit', resolve));
await subgraphs.close();
rmSync(dir, { recursive: true, force: true });
process.exit(code ?? 1);
