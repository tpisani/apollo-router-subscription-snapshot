import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { matchPhase, scoreboardMaxAge } from '../src/sports.mjs';
import { startStack } from '../scripts/stack.mjs';
import { watchMatch } from '../client/lifecycle.mjs';
import { graphql, snapshotApi, transports } from '../client/transport.mjs';

async function until(predicate, message, ms = 8000) {
  for (const end = Date.now() + ms; Date.now() < end; await delay(10)) if (await predicate()) return;
  throw new Error(`Timeout: ${message}`);
}

// ---- Regras com relógio fixo ---------------------------------------------------
const T = Date.parse('2026-10-18T19:00:00Z');   // kickoff
const MIN = 60_000, LEAD = 15 * MIN;

test('fase: o horário decide quando assinar, o status decide quando parar', () => {
  assert.deepEqual(matchPhase({ status: 'SCHEDULED', kickoff: T }, T - 2 * 60 * MIN), { phase: 'before', subscribeAt: T - LEAD });
  assert.deepEqual(matchPhase({ status: 'SCHEDULED', kickoff: T }, T - LEAD), { phase: 'live' }, 'na hora da janela');
  assert.deepEqual(matchPhase({ status: 'SCHEDULED', kickoff: T }, T + 20 * MIN), { phase: 'live' }, 'atrasado: continua assinando');
  assert.deepEqual(matchPhase({ status: 'SCHEDULED', kickoff: null }, T), { phase: 'live' }, 'sem horário: assina, por segurança');
  assert.deepEqual(matchPhase({ status: 'LIVE', kickoff: T }, T - 2 * 60 * MIN), { phase: 'live' }, 'começou antes: o status manda');
  assert.deepEqual(matchPhase({ status: 'HALF_TIME', kickoff: T }, T + 50 * MIN), { phase: 'live' });
  assert.deepEqual(matchPhase({ status: 'FULL_TIME', kickoff: T }, T + 2 * 60 * MIN), { phase: 'over' });
});

test('cache do placar: SCHEDULED nunca passa de kickoff − lead; FULL_TIME é longo', () => {
  const ttl = (state, now) => scoreboardMaxAge(state, now, { live: 2 });
  const scheduled = { status: 'SCHEDULED', kickoff: T };
  assert.equal(ttl(scheduled, T - 3 * 60 * MIN), 300, 'longe do início: o teto de 5 min');
  assert.equal(ttl(scheduled, T - LEAD - 90_000), 90, 'perto: só até a janela');
  assert.equal(ttl(scheduled, T - LEAD - 400), 0, 'menos de 1 s: no-store');
  assert.equal(ttl(scheduled, T - LEAD + 1), 2, 'na janela: curto, como ao vivo');
  assert.equal(ttl({ status: 'SCHEDULED', kickoff: null }, T), 2);
  assert.equal(ttl({ status: 'LIVE', kickoff: T }, T + MIN), 2);
  assert.equal(ttl({ status: 'FULL_TIME', kickoff: T }, T + 2 * 60 * MIN), 3600);
  // Propriedade: para qualquer instante antes da janela, a resposta expira até a janela.
  for (let now = T - 3 * 60 * MIN; now < T - LEAD; now += 7_777) assert.ok(now + ttl(scheduled, now) * 1000 <= T - LEAD, `expira depois da janela em ${now}`);
});

// ---- Ponta a ponta pelo edge (cache de queries no pior caso: nunca expira) ------
// Janela curta (lead de 1 s) para o teste não esperar 15 min.
const lead = 1000;
let stack;
test.before(async () => { stack = await startStack({ liveLeadMs: lead, scoreboard: { periodicMs: 500 }, stats: { periodicMs: 500, throttleMs: 100 } }); });
test.after(async () => { await stack?.stop(); });

const scoresUrl = () => `http://127.0.0.1:${stack.subgraphs.ports.scores}/graphql`;
const mutate = (query, variables) => graphql(scoresUrl(), query, variables);
const schedule = (m, kickoff) => mutate('mutation($m: ID!, $k: Float!) { scheduleMatch(matchId: $m, kickoff: $k) { version } }', { m, k: kickoff });
const start = m => mutate('mutation($m: ID!) { startPeriod(matchId: $m, period: 1) { version } }', { m });
const goal = m => mutate('mutation($m: ID!) { scoreGoal(matchId: $m, side: HOME) { version } }', { m });
const finish = m => mutate('mutation($m: ID!) { endPeriod(matchId: $m, final: true) { version } }', { m });
const streams = (channel, m) => stack.subgraphs.metrics[channel].activeByMatch[m] ?? 0;

function watch(matchId) {
  const url = stack.edge.url; const phases = []; const errors = [];
  const w = watchMatch({ matchId, lead, api: channel => snapshotApi(url, channel, matchId),
    connect: (channel, signal) => transports.multipart(url, channel, matchId, { signal }),
    onPhase: p => phases.push(p), onError: e => errors.push(e), runOptions: { wait: () => 100 } });
  return Object.assign(w, { phases, errors });
}
const cacheControl = async m => (await fetch(scoresUrl(), { method: 'POST', headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ query: 'query($m: ID!) { scoreboard(matchId: $m) { version } }', variables: { m } }) })).headers.get('cache-control');

test('Cache-Control do subgraph conforme status e horário', { timeout: 30_000 }, async () => {
  await schedule('cc-longe', Date.now() + 3 * 60 * MIN);
  assert.equal(await cacheControl('cc-longe'), 'public, max-age=300');
  await schedule('cc-perto', Date.now() + lead + 30_000);
  const near = Number(/max-age=(\d+)/.exec(await cacheControl('cc-perto'))[1]);
  assert.ok(near <= 30 && near >= 28, `perto do início: ${near} s`);
  await schedule('cc-janela', Date.now() + lead / 2);
  assert.equal(await cacheControl('cc-janela'), 'public, max-age=2');
  await start('cc-fim'); await finish('cc-fim');
  assert.equal(await cacheControl('cc-fim'), 'public, max-age=3600');
});

test('partida encerrada: só a query, nenhuma subscription', { timeout: 30_000 }, async () => {
  const m = `encerrada-${Date.now()}`;
  await start(m); await goal(m); await finish(m);
  const opened = { scoreboard: stack.subgraphs.metrics.scoreboard.streamsOpened, stats: stack.subgraphs.metrics.stats.streamsOpened };
  const w = watch(m);
  await w.done;
  assert.deepEqual(w.phases, ['over']);
  assert.equal(w.counts.subscriptions, 0);
  assert.equal(w.counts.queries, 2, 'uma query por canal');
  assert.equal(w.syncs.scoreboard.state.status, 'FULL_TIME');
  assert.equal(w.syncs.scoreboard.state.home, 1);
  assert.deepEqual({ scoreboard: stack.subgraphs.metrics.scoreboard.streamsOpened, stats: stack.subgraphs.metrics.stats.streamsOpened }, opened, 'nenhum stream aberto no subgraph');
});

test('antes do início com SCHEDULED antigo no cache: assina na hora da janela; atraso coberto; até o FULL_TIME', { timeout: 30_000 }, async () => {
  const m = `agendada-${Date.now()}`;
  const kickoff = Date.now() + lead + 700;
  await schedule(m, kickoff);
  // Aquece o cache do edge com SCHEDULED; ele nunca expira (pior caso).
  await snapshotApi(stack.edge.url, 'scoreboard', m).snapshot(0);
  const w = watch(m);
  try {
    await until(() => w.phase === 'before', 'fase before');
    assert.equal(w.counts.subscriptions, 0);
    assert.equal(streams('scoreboard', m), 0, 'antes da janela, nenhum stream');

    // Na janela (kickoff − lead), assina mesmo com o cache dizendo SCHEDULED.
    await until(() => w.phase === 'live' && w.syncs.scoreboard.synced && w.syncs.stats.synced, 'assinou na janela', 5000);
    const subscribedAt = Date.now();
    assert.ok(subscribedAt >= kickoff - lead, 'não assinou antes da janela');
    assert.ok(subscribedAt - (kickoff - lead) < 500, `assinou ${subscribedAt - (kickoff - lead)} ms depois da janela`);
    assert.equal(streams('scoreboard', m), 1); assert.equal(streams('stats', m), 1);
    const cached = await snapshotApi(stack.edge.url, 'scoreboard', m).snapshot(0);
    assert.equal(cached.state.status, 'SCHEDULED', 'o cache continua antigo, e não importa');

    // Atraso: passou do horário e ainda não começou; o cliente segue assinando.
    await until(() => Date.now() > kickoff + 600, 'passar do horário', 3000);
    assert.equal(w.syncs.scoreboard.state.status, 'SCHEDULED');
    assert.equal(w.phase, 'live');
    await start(m);
    await until(() => w.syncs.scoreboard.state.status === 'LIVE', 'apito inicial pelo stream');
    await goal(m);
    await until(() => w.syncs.scoreboard.state.home === 1, 'gol');

    // FULL_TIME: o placar termina no subgraph, o cliente fecha os dois canais.
    await finish(m);
    await w.done;
    assert.equal(w.phase, 'over');
    assert.deepEqual(w.phases, ['before', 'live', 'over']);
    assert.equal(w.syncs.scoreboard.state.status, 'FULL_TIME');
    await until(() => streams('scoreboard', m) === 0 && streams('stats', m) === 0, 'streams fechados no subgraph');
    assert.equal(w.errors.length, 0, 'sem reconexão');
  } finally { await w.close(); }
});

test('cache antigo dizendo LIVE numa partida já encerrada: o primeiro frame traz FULL_TIME e fecha', { timeout: 30_000 }, async () => {
  const m = `live-antigo-${Date.now()}`;
  await start(m);
  await snapshotApi(stack.edge.url, 'scoreboard', m).snapshot(0);   // cache: LIVE, para sempre
  await goal(m); await finish(m);
  const t0 = Date.now();
  const w = watch(m);
  await w.done;
  assert.deepEqual(w.phases, ['live', 'over']);
  assert.equal(w.syncs.scoreboard.state.status, 'FULL_TIME');
  assert.equal(w.syncs.scoreboard.state.home, 1);
  assert.ok(Date.now() - t0 < 1000, `fechou em ${Date.now() - t0} ms`);
  assert.equal(w.errors.length, 0, 'sem reconexão');
  await until(() => streams('scoreboard', m) === 0 && streams('stats', m) === 0 && stack.edge.metrics.upstreamSubscriptions === 0, 'tudo fechado');
});

test('vários clientes deduplicados: o FULL_TIME encerra todos e libera o upstream', { timeout: 30_000 }, async () => {
  const m = `dedup-fim-${Date.now()}`;
  await start(m);
  const ws = Array.from({ length: 5 }, () => watch(m));
  try {
    await until(() => ws.every(w => w.syncs.scoreboard.synced && w.syncs.stats.synced), 'todos assinando');
    assert.equal(streams('scoreboard', m), 1, 'um upstream para os 5');
    await finish(m);
    await Promise.all(ws.map(w => w.done));
    assert.ok(ws.every(w => w.phase === 'over' && w.errors.length === 0));
    await until(() => streams('scoreboard', m) === 0 && streams('stats', m) === 0, 'upstreams fechados');
  } finally { await Promise.all(ws.map(w => w.close())); }
});
