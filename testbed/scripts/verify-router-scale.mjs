import assert from 'node:assert/strict';
import { execSync } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { createSync, runSync, statsFromWire } from '../client/sync.mjs';
import { graphql, snapshotApi, transports } from '../client/transport.mjs';
import { playGenerator } from '../subgraphs/sports.mjs';
import { watchMatch } from '../client/lifecycle.mjs';

// O plano gratuito do GraphOS limita requisições por instância (503
// ROUTER_FREE_PLAN_RATE_LIMIT_REACHED); cada subscription conta uma vez.
const RATE_LIMIT = 'ROUTER_FREE_PLAN_RATE_LIMIT_REACHED';
let rateLimited = null;
async function until(predicate, message, ms = 10_000) {
  for (const end = Date.now() + ms; Date.now() < end; await delay(20)) {
    if (rateLimited) throw new Error(`Limite de requisições do plano gratuito atingido (${RATE_LIMIT}): ${rateLimited}`);
    if (await predicate()) return;
  }
  throw new Error(`Timeout: ${message}`);
}
const median = list => [...list].sort((a, b) => a - b)[Math.floor(list.length / 2)];
const monotonic = list => list.every((v, i) => i === 0 || v >= list[i - 1]);

// Várias instâncias do Router com o plugin subscription_snapshot, medidas pelo
// cliente e pelos subgraphs (/metrics, streams abertos por partida):
//   routerUrls: cada instância com retenção, acessada direto
//   lbUrl:      um balanceador na frente delas (sem afinidade)
//   plainUrl:   uma instância igual, com a retenção desligada (comparação)
//   kill():     opcional, derruba uma instância no meio do jogo
//   direct:     opcional, { scores, stats } dos subgraphs: mutations e snapshots vão
//               direto para eles e só as subscriptions passam pelo Router, para
//               caber no limite de requisições do plano gratuito
// Roda também contra o emulador (tests/router-scale.test.mjs), com edges no lugar dos routers.
//   lead:       opcional, janela de início (ms) configurada nos subgraphs; liga a etapa
//               do ciclo de vida (query × subscription, FULL_TIME)
export async function verifyRouterScale({ lbUrl, routerUrls, plainUrl, metrics, kill, direct, lead, headers = {}, log = () => {} }) {
  rateLimited = null;
  const run = Math.random().toString(36).slice(2, 7);
  const result = {};
  const clients = [];

  const mutations = url => {
    const call = async (query, variables, field, target = url) => (await graphql(target, query, variables, undefined, headers))[field];
    const scores = direct?.scores ?? url; const stats = direct?.stats ?? url;
    return {
      startPeriod: async matchId => (await call('mutation($m: ID!) { startPeriod(matchId: $m, period: 1) { version } }', { m: matchId }, 'startPeriod', scores)).version,
      goal: async matchId => (await call('mutation($m: ID!) { scoreGoal(matchId: $m, side: HOME) { version } }', { m: matchId }, 'scoreGoal', scores)).version,
      schedule: async (matchId, kickoff) => call('mutation($m: ID!, $k: Float!) { scheduleMatch(matchId: $m, kickoff: $k) { version } }', { m: matchId, k: kickoff }, 'scheduleMatch', scores),
      finish: async matchId => (await call('mutation($m: ID!) { endPeriod(matchId: $m, final: true) { version } }', { m: matchId }, 'endPeriod', scores)).version,
      plays: async (matchId, events) => call('mutation($m: ID!, $e: [StatEventInput!]!) { recordStats(matchId: $m, events: $e) }', { m: matchId, e: events }, 'recordStats', stats)
    };
  };
  const api = mutations(lbUrl);

  // Cliente com um ou dois canais; registra versões vistas, frames retidos e o
  // tempo até o primeiro estado.
  const connect = (url, matchId, channels, { headers: clientHeaders = headers } = {}) => {
    const controller = new AbortController();
    const c = { syncs: {}, seen: {}, retained: {}, errors: [], runs: [], t0: Date.now(), syncedAt: {}, failedAt: {}, frameAt: {} };
    for (const channel of channels) {
      const snapshotUrl = direct ? direct[channel === 'scoreboard' ? 'scores' : 'stats'] : url;
      const sync = createSync(channel, { matchId, api: snapshotApi(snapshotUrl, channel, matchId, { headers: clientHeaders }) });
      c.syncs[channel] = sync; c.seen[channel] = []; c.retained[channel] = 0;
      c.runs.push(runSync({ sync, signal: controller.signal, wait: () => 100, stallMs: 5000,
        connect: signal => transports.multipart(url, channel, matchId, { signal, headers: clientHeaders,
          onPayload: payload => { if (payload.extensions?.snapshot) c.retained[channel]++; } }),
        onUpdate: view => {
          if (view.version !== null) c.seen[channel].push(view.version);
          if (view.synced) { c.syncedAt[channel] ??= Date.now(); c.frameAt[channel] = Date.now(); }
        },
        onError: error => { c.errors.push(error); c.failedAt[channel] = Date.now(); if (error.message.includes(RATE_LIMIT)) rateLimited ??= error.message.slice(0, 200); } }));
    }
    c.synced = () => Object.values(c.syncs).every(s => s.synced);
    // Every channel got a frame after its last error, i.e. it reconnected and resynced.
    c.recovered = () => c.synced() && channels.every(channel => !(c.failedAt[channel] >= c.frameAt[channel]));
    c.close = async () => { controller.abort(); await Promise.all(c.runs); };
    clients.push(c);
    return c;
  };
  const streams = async (channel, matchId) => (await metrics())[channel].activeByMatch?.[matchId] ?? 0;
  const closeAll = async list => { await Promise.all(list.map(c => c.close())); };

  try {
    // 1. Deduplicação por instância: 4 clientes por instância e partida, 2 partidas.
    //    Streams no subgraph = instâncias × partidas × canais, não clientes.
    {
      const matches = [`dedup-a-${run}`, `dedup-b-${run}`];
      for (const m of matches) await api.startPeriod(m);
      const list = routerUrls.flatMap(url => matches.flatMap(m => Array.from({ length: 4 }, () => connect(url, m, ['scoreboard', 'stats']))));
      await until(() => list.every(c => c.synced()), 'clientes sincronizados', 20_000);
      await delay(300);
      const counts = {};
      for (const m of matches) counts[m] = { placar: await streams('scoreboard', m), estatisticas: await streams('stats', m) };
      result.deduplicacao = { instancias: routerUrls.length, clientes: list.length, streamsNoSubgraphPorPartida: counts };
      log(`deduplicação: ${JSON.stringify(result.deduplicacao)}`);
      // O esperado é um stream por instância. Com muitos clientes entrando ao mesmo
      // tempo, a deduplicação passthrough do Apollo Router às vezes abre um upstream
      // a mais na mesma instância (visto também com o plugin desligado). Isso não
      // afeta a correção, só a eficiência, então é registrado e não reprovado; falha
      // se a deduplicação não acontecer de fato.
      const perMatch = list.length / matches.length;
      result.deduplicacao.upstreamsExtras = 0;
      for (const m of matches) for (const [canal, n] of Object.entries(counts[m])) {
        assert.ok(n >= routerUrls.length, `${canal} ${m}: ao menos um stream por instância (veio ${n})`);
        assert.ok(n < perMatch, `${canal} ${m}: ${n} streams para ${perMatch} clientes, sem deduplicação`);
        result.deduplicacao.upstreamsExtras += n - routerUrls.length;
      }
      await closeAll(list);
    }

    // 2. Late joiner numa partida parada: com retenção, o estado vem na hora;
    //    sem, espera o frame periódico do subgraph.
    {
      const measure = async (url, label, i) => {
        const matchId = `late-${label}-${i}-${run}`;
        await api.startPeriod(matchId);
        const first = connect(url, matchId, ['scoreboard']);
        await until(() => first.synced(), `primeiro cliente (${label})`);
        await delay(150);                               // o upstream já está aberto e parado
        const late = connect(url, matchId, ['scoreboard']);
        await until(() => late.synced(), `late joiner (${label})`, 10_000);
        await closeAll([first, late]);
        return { ms: late.syncedAt.scoreboard - late.t0, retained: late.retained.scoreboard };
      };
      const withRetention = []; const without = [];
      for (let i = 0; i < 3; i++) {
        withRetention.push(await measure(routerUrls[0], 'retencao', i));
        if (plainUrl) without.push(await measure(plainUrl, 'sem', i));
      }
      result.lateJoiner = {
        comRetencaoMs: withRetention.map(x => x.ms), framesRetidos: withRetention.map(x => x.retained),
        semRetencaoMs: without.map(x => x.ms), framesRetidosSemRetencao: without.map(x => x.retained)
      };
      log(`late joiner: ${JSON.stringify(result.lateJoiner)}`);
      for (const x of withRetention) {
        assert.ok(x.retained >= 1, 'o late joiner recebeu o frame retido');
        assert.ok(x.ms < 300, `com retenção: ${x.ms} ms`);
      }
      for (const x of without) assert.equal(x.retained, 0, 'sem retenção, nenhum frame retido');
      if (without.length) assert.ok(median(without.map(x => x.ms)) > median(withRetention.map(x => x.ms)), 'sem retenção, o late joiner espera mais');
    }

    // 3. Identidade: tokens diferentes não compartilham upstream nem frame retido.
    {
      const matchId = `identidade-${run}`;
      await api.startPeriod(matchId);
      const url = routerUrls[0];
      const a1 = connect(url, matchId, ['scoreboard'], { headers: { ...headers, authorization: 'Bearer a' } });
      await until(() => a1.synced(), 'a1');
      await delay(150);
      const b = connect(url, matchId, ['scoreboard'], { headers: { ...headers, authorization: 'Bearer b' } });
      const a2 = connect(url, matchId, ['scoreboard'], { headers: { ...headers, authorization: 'Bearer a' } });
      await until(() => b.synced() && a2.synced(), 'b e a2');
      await delay(300);
      result.identidade = { streamsNoSubgraph: await streams('scoreboard', matchId), retidos: { b: b.retained.scoreboard, a2: a2.retained.scoreboard } };
      log(`identidade: ${JSON.stringify(result.identidade)}`);
      assert.equal(result.identidade.streamsNoSubgraph, 2, 'um upstream por identidade');
      assert.equal(b.retained.scoreboard, 0, 'b (outro token) não recebeu o frame de a');
      assert.ok(a2.retained.scoreboard >= 1, 'a2 (mesmo token de a1) recebeu o frame retido');
      await closeAll([a1, b, a2]);
    }

    // 4. Jogo ao vivo pelo balanceador; opcionalmente uma instância cai no meio.
    //    Nenhuma versão regride e todos terminam no estado do banco.
    {
      const matchId = `ao-vivo-${run}`;
      const next = playGenerator(matchId, 4);
      let scoreVersion = await api.startPeriod(matchId); let statsVersion = 0;
      const list = Array.from({ length: 12 }, () => connect(lbUrl, matchId, ['scoreboard', 'stats']));
      await until(() => list.every(c => c.synced()), '12 clientes pelo balanceador', 20_000);
      let killed = null;
      const t0 = Date.now(); let goals = 0;
      while (Date.now() - t0 < 5000) {
        statsVersion = await api.plays(matchId, [next(), next()]);
        if (Math.floor((Date.now() - t0) / 1000) > goals) { scoreVersion = await api.goal(matchId); goals++; }
        if (kill && !killed && Date.now() - t0 > 2000) {
          // Watched from before the kill, through the client errors: `synced` only
          // resets on reconnection and the snapshot comes right after, so polling it
          // can miss the drop (see tests/scale.test.mjs).
          const errorsBefore = list.reduce((n, c) => n + c.errors.length, 0);
          killed = { at: Date.now() };
          killed.recovery = (async () => {
            await until(() => list.reduce((n, c) => n + c.errors.length, 0) > errorsBefore, 'queda percebida', 5000);
            await until(() => list.every(c => c.recovered()), 'recuperação', 20_000);
            return Date.now() - killed.at;
          })();
          killed.recovery.catch(() => {}); // awaited below; no unhandled rejection meanwhile
          await kill();
        }
        await delay(50);
      }
      await until(() => list.every(c => c.syncs.scoreboard.version === scoreVersion && c.syncs.stats.version === statsVersion), 'todos na versão final', 20_000);
      const truth = await snapshotApi(direct?.stats ?? lbUrl, 'stats', matchId, { headers }).snapshot(statsVersion);
      for (const c of list) {
        for (const channel of ['scoreboard', 'stats']) assert.ok(monotonic(c.seen[channel]), `${channel} regrediu`);
        assert.equal(c.syncs.stats.version, truth.version);
        assert.deepEqual(c.syncs.stats.state, statsFromWire(truth.state), 'estatísticas diferentes do banco');
        assert.equal(c.syncs.scoreboard.state.home, goals);
      }
      result.aoVivo = {
        clientes: list.length, gols: goals, versoes: { placar: scoreVersion, estatisticas: statsVersion },
        framesRetidosRecebidos: list.reduce((n, c) => n + c.retained.scoreboard + c.retained.stats, 0),
        ...(killed && { instanciaDerrubada: true, reconectaram: list.filter(c => c.errors.length).length, recuperacaoMs: await killed.recovery })
      };
      log(`ao vivo: ${JSON.stringify(result.aoVivo)}`);
      if (killed) assert.ok(result.aoVivo.reconectaram >= 1, 'algum cliente estava na instância derrubada');
      await closeAll(list);
    }

    // 5. Ciclo de vida (com lead): query × subscription pelo horário e pelo status,
    //    e o FULL_TIME encerrando clientes deduplicados em várias instâncias.
    if (lead) {
      const snapshotUrl = channel => direct ? direct[channel === 'scoreboard' ? 'scores' : 'stats'] : lbUrl;
      const watch = (matchId, { url = lbUrl, api = channel => snapshotApi(snapshotUrl(channel), channel, matchId, { headers }) } = {}) => {
        const phases = []; const errors = []; const retainedSeen = { n: 0 };
        const w = watchMatch({ matchId, lead, api,
          connect: (channel, signal) => transports.multipart(url, channel, matchId, { signal, headers,
            onPayload: payload => { if (payload.extensions?.snapshot) retainedSeen.n++; } }),
          onPhase: p => phases.push(p),
          onError: e => { errors.push(e); if (e.message.includes(RATE_LIMIT)) rateLimited ??= e.message.slice(0, 200); },
          runOptions: { wait: () => 100 } });
        return Object.assign(w, { phases, errors, retainedSeen });
      };
      const both = async m => ({ placar: await streams('scoreboard', m), estatisticas: await streams('stats', m) });
      const r = (result.cicloDeVida = {});

      // a) Encerrada: só queries, nenhuma subscription.
      const ended = `fim-antes-${run}`;
      await api.startPeriod(ended); await api.finish(ended);
      const wEnded = watch(ended); await wEnded.done;
      r.encerrada = { fases: wEnded.phases, subscriptions: wEnded.counts.subscriptions };
      assert.deepEqual(wEnded.phases, ['over']); assert.equal(wEnded.counts.subscriptions, 0);

      // b) Agendada: só a query até kickoff − lead; assina na janela pelo relógio.
      const scheduled = `agendada-${run}`;
      const kickoff = Date.now() + lead + 1500;
      await api.schedule(scheduled, kickoff);
      const wSched = watch(scheduled);
      await until(() => wSched.phase === 'before', 'fase before');
      const beforeStreams = await both(scheduled);
      await until(() => wSched.phase === 'live' && wSched.syncs.scoreboard.synced && wSched.syncs.stats.synced, 'assinou na janela', 10_000);
      r.agendada = { fases: [...wSched.phases], streamsAntesDaJanela: beforeStreams, assinouMsDepoisDaJanela: Date.now() - (kickoff - lead) };
      assert.deepEqual(beforeStreams, { placar: 0, estatisticas: 0 });
      await api.startPeriod(scheduled);
      await until(() => wSched.syncs.scoreboard.state.status === 'LIVE', 'apito pelo stream');

      // c) FULL_TIME com vários clientes deduplicados pelo balanceador (várias instâncias).
      const crowd = Array.from({ length: 6 }, () => watch(scheduled));
      await until(() => crowd.every(w => w.syncs.scoreboard.synced && w.syncs.stats.synced), 'clientes no jogo', 20_000);
      const live = await both(scheduled);
      const t0 = Date.now();
      await api.finish(scheduled);
      await Promise.all([wSched, ...crowd].map(w => w.done));
      r.fimDoJogo = {
        clientes: crowd.length + 1, streamsDuranteOJogo: live, todosEncerradosMs: Date.now() - t0,
        reconexoes: [wSched, ...crowd].reduce((n, w) => n + w.errors.length, 0)
      };
      assert.ok([wSched, ...crowd].every(w => w.phase === 'over' && w.syncs.scoreboard.state.status === 'FULL_TIME'));
      assert.equal(r.fimDoJogo.reconexoes, 0, 'ninguém reconectou depois do FULL_TIME');
      await until(async () => { const b = await both(scheduled); return !b.placar && !b.estatisticas; }, 'upstreams fechados no subgraph', 20_000);

      // d) Depois do fim, direto na subscription: estado final do subgraph (não um frame
      //    retido do upstream que fechou) e o stream termina.
      const late = { frames: [], retained: 0 };
      for await (const frame of transports.multipart(routerUrls[0], 'scoreboard', scheduled, { headers,
        onPayload: payload => { if (payload.extensions?.snapshot) late.retained++; } })) late.frames.push(frame.state.status);
      r.depoisDoFim = { frames: late.frames, framesRetidos: late.retained };
      assert.deepEqual(late.frames, ['FULL_TIME']); assert.equal(late.retained, 0);

      // e) Query antiga dizendo LIVE (cache simulado no cliente) numa partida encerrada.
      const stale = `live-antigo-${run}`;
      await api.startPeriod(stale);
      const liveSnapshot = await snapshotApi(snapshotUrl('scoreboard'), 'scoreboard', stale, { headers }).snapshot(0);
      await api.finish(stale);
      const ts = Date.now();
      const wStale = watch(stale, { api: channel => channel === 'scoreboard' ? { snapshot: async () => liveSnapshot } : snapshotApi(snapshotUrl(channel), channel, stale, { headers }) });
      await wStale.done;
      r.liveAntigo = { fases: wStale.phases, fechouEmMs: Date.now() - ts, reconexoes: wStale.errors.length };
      assert.deepEqual(wStale.phases, ['live', 'over']); assert.equal(wStale.errors.length, 0);
      log(`ciclo de vida: ${JSON.stringify(r)}`);
    }

    // 6. Limpeza: sem assinantes, o Router fecha os upstreams (e o plugin solta os frames).
    {
      const ours = m => Object.keys(m).filter(k => k.endsWith(run));
      await until(async () => { const m = await metrics(); return !ours(m.scoreboard.activeByMatch ?? {}).length && !ours(m.stats.activeByMatch ?? {}).length; }, 'upstreams fechados no subgraph', 20_000);
      result.limpeza = 'sem streams no subgraph para as partidas do teste';
    }
    return result;
  } finally {
    await closeAll(clients);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  // Contra o perfil apollo-plugin do compose (docker compose --profile apollo-plugin up):
  //   3 routers com o plugin (4101-4103), um sem retenção (4104), balanceador (4200).
  // KILL_CMD derruba uma instância no meio do jogo, ex.:
  //   KILL_CMD="docker compose --profile apollo-plugin stop router-b" npm run test:router-scale
  const scoresMetrics = process.env.SCORES_METRICS_URL ?? 'http://localhost:4001/metrics';
  const statsMetrics = process.env.STATS_METRICS_URL ?? 'http://localhost:4003/metrics';
  const routerUrls = (process.env.ROUTER_URLS ?? 'http://localhost:4101/graphql,http://localhost:4102/graphql,http://localhost:4103/graphql').split(',');
  const result = await verifyRouterScale({
    lbUrl: process.env.LB_URL ?? 'http://localhost:4200/graphql',
    routerUrls,
    plainUrl: process.env.PLAIN_URL ?? 'http://localhost:4104/graphql',
    metrics: async () => ({ scoreboard: (await (await fetch(scoresMetrics)).json()).scoreboard, stats: (await (await fetch(statsMetrics)).json()).stats }),
    kill: process.env.KILL_CMD ? async () => { execSync(process.env.KILL_CMD, { stdio: 'inherit' }); } : undefined,
    log: console.log
  });
  console.log(JSON.stringify(result, null, 2));
  console.log('OK: deduplicação por instância, frame retido para late joiners, identidade, jogo ao vivo sem regressão e limpeza');
}
