import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { startSubgraphs } from '../subgraphs/app.mjs';
import { scoreboardOps } from '../subgraphs/sports.mjs';

async function until(predicate, message, ms = 5000) {
  for (const end = Date.now() + ms; Date.now() < end; await delay(10)) if (predicate()) return;
  throw new Error(`Timeout: ${message}`);
}

// Modo callback do Apollo Router, do lado do subgraph: um endpoint falso de
// /callback registra as mensagens e responde como o Router (204 no check, 200 no
// next; 404 quando não há mais clientes).
test('subscription no modo callback: check, frames como next, 404 encerra o stream', { timeout: 30_000 }, async () => {
  const dir = mkdtempSync(join(tmpdir(), 'callback-'));
  const app = await startSubgraphs({ dataDir: dir, host: '127.0.0.1', ports: { scores: 0, catalog: 0, stats: 0 }, scoreboard: { periodicMs: 60_000 } });
  const messages = []; let gone = false;
  const router = createServer(async (req, res) => {
    let raw = ''; for await (const c of req) raw += c;
    const message = JSON.parse(raw); messages.push({ path: req.url, ...message });
    if (message.action === 'check') { res.writeHead(gone ? 404 : 204, { 'subscription-protocol': 'callback/1.0' }); res.end(); return; }
    res.writeHead(gone ? 404 : 200); res.end();
  });
  await new Promise(r => router.listen(0, '127.0.0.1', r));
  const callbackUrl = `http://127.0.0.1:${router.address().port}/callback/sub-1`;
  try {
    const matchId = 'callback-1';
    app.stores.scoreboard.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, Date.now()));
    const response = await fetch(`http://127.0.0.1:${app.ports.scores}/graphql`, { method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query: 'subscription($m: ID!) { scoreboardUpdates(matchId: $m) { version state { home } } }', variables: { m: matchId },
        extensions: { subscription: { callbackUrl, subscriptionId: 'sub-1', verifier: 'v-1', heartbeatIntervalMs: 0 } } }) });
    assert.deepEqual(await response.json(), { data: null }, 'responde { data: null } depois do check');
    assert.equal(messages[0].action, 'check');
    assert.deepEqual([messages[0].kind, messages[0].id, messages[0].verifier], ['subscription', 'sub-1', 'v-1']);

    await until(() => messages.some(m => m.action === 'next'), 'frame inicial');
    const first = messages.find(m => m.action === 'next');
    assert.equal(first.payload.data.scoreboardUpdates.version, 1);
    app.stores.scoreboard.update(matchId, s => scoreboardOps.scoreGoal(s, { side: 'HOME' }, Date.now()));
    await until(() => messages.some(m => m.action === 'next' && m.payload.data.scoreboardUpdates.state.home === 1), 'gol como next');
    assert.equal(app.metrics.scoreboard.activeByMatch[matchId], 1);

    // O Router não tem mais clientes nessa subscription: o próximo next recebe 404 e o stream fecha.
    gone = true;
    app.stores.scoreboard.update(matchId, s => scoreboardOps.scoreGoal(s, { side: 'AWAY' }, Date.now()));
    await until(() => !app.metrics.scoreboard.activeByMatch[matchId], 'stream encerrado no subgraph');
    assert.ok(!messages.some(m => m.action === 'complete'), 'sem complete depois de um 404');
  } finally {
    router.close(); await app.close(); rmSync(dir, { recursive: true, force: true });
  }
});
