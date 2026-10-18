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

// Apollo Router callback mode, on the subgraph side: a fake /callback endpoint
// records the messages and answers like the Router (204 on check, 200 on
// next; 404 when there are no clients left).
test('subscription in callback mode: check, frames as next, 404 ends the stream', { timeout: 30_000 }, async () => {
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
    assert.deepEqual(await response.json(), { data: null }, 'replies { data: null } after the check');
    assert.equal(messages[0].action, 'check');
    assert.deepEqual([messages[0].kind, messages[0].id, messages[0].verifier], ['subscription', 'sub-1', 'v-1']);

    await until(() => messages.some(m => m.action === 'next'), 'initial frame');
    const first = messages.find(m => m.action === 'next');
    assert.equal(first.payload.data.scoreboardUpdates.version, 1);
    app.stores.scoreboard.update(matchId, s => scoreboardOps.scoreGoal(s, { side: 'HOME' }, Date.now()));
    await until(() => messages.some(m => m.action === 'next' && m.payload.data.scoreboardUpdates.state.home === 1), 'goal as next');
    assert.equal(app.metrics.scoreboard.activeByMatch[matchId], 1);

    // The Router has no clients left on this subscription: the next "next" gets a 404 and the stream closes.
    gone = true;
    app.stores.scoreboard.update(matchId, s => scoreboardOps.scoreGoal(s, { side: 'AWAY' }, Date.now()));
    await until(() => !app.metrics.scoreboard.activeByMatch[matchId], 'stream ended on the subgraph');
    assert.ok(!messages.some(m => m.action === 'complete'), 'no complete after a 404'
);
  } finally {
    router.close(); await app.close(); rmSync(dir, { recursive: true, force: true });
  }
});
