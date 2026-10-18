import assert from 'node:assert/strict';
import { execSync } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { createSync, runSync, statsFromWire } from '../client/sync.mjs';
import { graphql, snapshotApi, transports } from '../client/transport.mjs';
import { playGenerator } from '../subgraphs/sports.mjs';
import { watchMatch } from '../client/lifecycle.mjs';

// The GraphOS free plan limits requests per instance (503
// ROUTER_FREE_PLAN_RATE_LIMIT_REACHED); each subscription counts once.
const RATE_LIMIT = 'ROUTER_FREE_PLAN_RATE_LIMIT_REACHED';
let rateLimited = null;
async function until(predicate, message, ms = 10_000) {
  for (const end = Date.now() + ms; Date.now() < end; await delay(20)) {
    if (rateLimited) throw new Error(`Free plan request limit reached (${RATE_LIMIT}): ${rateLimited}`);
    if (await predicate()) return;
  }
  throw new Error(`Timeout: ${message}`);
}
const median = list => [...list].sort((a, b) => a - b)[Math.floor(list.length / 2)];
const monotonic = list => list.every((v, i) => i === 0 || v >= list[i - 1]);

// Several Router instances with the subscription_snapshot plugin, measured by the
// client and by the subgraphs (/metrics, open streams per match):
//   routerUrls: each instance with retention, reached directly
//   lbUrl:      a balancer in front of them (no affinity)
//   plainUrl:   an identical instance, with retention off (comparison)
//   kill():     optional, takes an instance down mid-match
//   direct:     optional, the subgraphs' { scores, stats }: mutations and snapshots go
//               straight to them and only subscriptions go through the Router, to
//               fit within the free plan's request limit
// Also runs against the emulator (tests/router-scale.test.mjs), with edges in place of routers.
//   lead:       optional, kickoff window (ms) configured in the subgraphs; enables the
//               lifecycle step (query × subscription, FULL_TIME)
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

  // Client with one or two channels; records the versions seen, retained frames and
  // the time to the first state.
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
    // 1. Deduplication per instance: 4 clients per instance and match, 2 matches.
    //    Streams on the subgraph = instances × matches × channels, not clients.
    {
      const matches = [`dedup-a-${run}`, `dedup-b-${run}`];
      for (const m of matches) await api.startPeriod(m);
      const list = routerUrls.flatMap(url => matches.flatMap(m => Array.from({ length: 4 }, () => connect(url, m, ['scoreboard', 'stats']))));
      await until(() => list.every(c => c.synced()), 'clients synced', 20_000);
      await delay(300);
      const counts = {};
      for (const m of matches) counts[m] = { scoreboard: await streams('scoreboard', m), stats: await streams('stats', m) };
      result.deduplication = { instances: routerUrls.length, clients: list.length, subgraphStreamsPerMatch: counts };
      log(`deduplication: ${JSON.stringify(result.deduplication)}`);
      // One stream per instance is expected. With many clients joining at the same
      // time, the Apollo Router's passthrough deduplication sometimes opens one extra
      // upstream on the same instance (also seen with the plugin off). That does not
      // affect correctness, only efficiency, so it is recorded and not failed; it fails
      // if deduplication does not actually happen.
      const perMatch = list.length / matches.length;
      result.deduplication.extraUpstreams = 0;
      for (const m of matches) for (const [channel, n] of Object.entries(counts[m])) {
        assert.ok(n >= routerUrls.length, `${channel} ${m}: at least one stream per instance (got ${n})`);
        assert.ok(n < perMatch, `${channel} ${m}: ${n} streams for ${perMatch} clients, no deduplication`);
        result.deduplication.extraUpstreams += n - routerUrls.length;
      }
      await closeAll(list);
    }

    // 2. Late joiner on an idle match: with retention, the state arrives right away;
    //    without it, it waits for the subgraph's periodic frame.
    {
      const measure = async (url, label, i) => {
        const matchId = `late-${label}-${i}-${run}`;
        await api.startPeriod(matchId);
        const first = connect(url, matchId, ['scoreboard']);
        await until(() => first.synced(), `first client (${label})`);
        await delay(150);                               // the upstream is already open and idle
        const late = connect(url, matchId, ['scoreboard']);
        await until(() => late.synced(), `late joiner (${label})`, 10_000);
        await closeAll([first, late]);
        return { ms: late.syncedAt.scoreboard - late.t0, retained: late.retained.scoreboard };
      };
      const withRetention = []; const without = [];
      for (let i = 0; i < 3; i++) {
        withRetention.push(await measure(routerUrls[0], 'retention', i));
        if (plainUrl) without.push(await measure(plainUrl, 'plain', i));
      }
      result.lateJoiner = {
        withRetentionMs: withRetention.map(x => x.ms), retainedFrames: withRetention.map(x => x.retained),
        withoutRetentionMs: without.map(x => x.ms), retainedFramesWithoutRetention: without.map(x => x.retained)
      };
      log(`late joiner: ${JSON.stringify(result.lateJoiner)}`);
      for (const x of withRetention) {
        assert.ok(x.retained >= 1, 'the late joiner got the retained frame');
        assert.ok(x.ms < 300, `with retention: ${x.ms} ms`);
      }
      for (const x of without) assert.equal(x.retained, 0, 'without retention, no retained frame');
      if (without.length) assert.ok(median(without.map(x => x.ms)) > median(withRetention.map(x => x.ms)), 'without retention, the late joiner waits longer');
    }

    // 3. Identity: different tokens share neither the upstream nor the retained frame.
    {
      const matchId = `identity-${run}`;
      await api.startPeriod(matchId);
      const url = routerUrls[0];
      const a1 = connect(url, matchId, ['scoreboard'], { headers: { ...headers, authorization: 'Bearer a' } });
      await until(() => a1.synced(), 'a1');
      await delay(150);
      const b = connect(url, matchId, ['scoreboard'], { headers: { ...headers, authorization: 'Bearer b' } });
      const a2 = connect(url, matchId, ['scoreboard'], { headers: { ...headers, authorization: 'Bearer a' } });
      await until(() => b.synced() && a2.synced(), 'b and a2');
      await delay(300);
      result.identity = { subgraphStreams: await streams('scoreboard', matchId), retained: { b: b.retained.scoreboard, a2: a2.retained.scoreboard } };
      log(`identity: ${JSON.stringify(result.identity)}`);
      assert.equal(result.identity.subgraphStreams, 2, 'one upstream per identity');
      assert.equal(b.retained.scoreboard, 0, "b (another token) did not get a's frame");
      assert.ok(a2.retained.scoreboard >= 1, 'a2 (same token as a1) got the retained frame');
      await closeAll([a1, b, a2]);
    }

    // 4. Live match through the balancer; optionally an instance goes down midway.
    //    No version goes back and everyone ends on the database state.
    {
      const matchId = `live-${run}`;
      const next = playGenerator(matchId, 4);
      let scoreVersion = await api.startPeriod(matchId); let statsVersion = 0;
      const list = Array.from({ length: 12 }, () => connect(lbUrl, matchId, ['scoreboard', 'stats']));
      await until(() => list.every(c => c.synced()), '12 clients through the balancer', 20_000);
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
            await until(() => list.reduce((n, c) => n + c.errors.length, 0) > errorsBefore, 'drop noticed', 5000);
            await until(() => list.every(c => c.recovered()), 'recovery', 20_000);
            return Date.now() - killed.at;
          })();
          killed.recovery.catch(() => {}); // awaited below; no unhandled rejection meanwhile
          await kill();
        }
        await delay(50);
      }
      await until(() => list.every(c => c.syncs.scoreboard.version === scoreVersion && c.syncs.stats.version === statsVersion), 'everyone on the final version', 20_000);
      const truth = await snapshotApi(direct?.stats ?? lbUrl, 'stats', matchId, { headers }).snapshot(statsVersion);
      for (const c of list) {
        for (const channel of ['scoreboard', 'stats']) assert.ok(monotonic(c.seen[channel]), `${channel} went back`);
        assert.equal(c.syncs.stats.version, truth.version);
        assert.deepEqual(c.syncs.stats.state, statsFromWire(truth.state), 'stats differ from the database');
        assert.equal(c.syncs.scoreboard.state.home, goals);
      }
      result.live = {
        clients: list.length, goals, versions: { scoreboard: scoreVersion, stats: statsVersion },
        retainedFramesReceived: list.reduce((n, c) => n + c.retained.scoreboard + c.retained.stats, 0),
        ...(killed && { instanceKilled: true, reconnected: list.filter(c => c.errors.length).length, recoveryMs: await killed.recovery })
      };
      log(`live: ${JSON.stringify(result.live)}`);
      if (killed) assert.ok(result.live.reconnected >= 1, 'some client was on the killed instance');
      await closeAll(list);
    }

    // 5. Lifecycle (with lead): query × subscription by time and status,
    //    and FULL_TIME ending deduplicated clients across several instances.
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
      const both = async m => ({ scoreboard: await streams('scoreboard', m), stats: await streams('stats', m) });
      const r = (result.lifecycle = {});

      // a) Finished: queries only, no subscription.
      const ended = `ended-before-${run}`;
      await api.startPeriod(ended); await api.finish(ended);
      const wEnded = watch(ended); await wEnded.done;
      r.ended = { phases: wEnded.phases, subscriptions: wEnded.counts.subscriptions };
      assert.deepEqual(wEnded.phases, ['over']); assert.equal(wEnded.counts.subscriptions, 0);

      // b) Scheduled: query only until kickoff − lead; subscribes in the window by the clock.
      const scheduled = `scheduled-${run}`;
      const kickoff = Date.now() + lead + 1500;
      await api.schedule(scheduled, kickoff);
      const wSched = watch(scheduled);
      await until(() => wSched.phase === 'before', 'before phase');
      const beforeStreams = await both(scheduled);
      await until(() => wSched.phase === 'live' && wSched.syncs.scoreboard.synced && wSched.syncs.stats.synced, 'subscribed in the window', 10_000);
      r.scheduled = { phases: [...wSched.phases], streamsBeforeWindow: beforeStreams, subscribedMsAfterWindow: Date.now() - (kickoff - lead) };
      assert.deepEqual(beforeStreams, { scoreboard: 0, stats: 0 });
      await api.startPeriod(scheduled);
      await until(() => wSched.syncs.scoreboard.state.status === 'LIVE', 'kickoff through the stream');

      // c) FULL_TIME with several deduplicated clients through the balancer (several instances).
      const crowd = Array.from({ length: 6 }, () => watch(scheduled));
      await until(() => crowd.every(w => w.syncs.scoreboard.synced && w.syncs.stats.synced), 'clients in the match', 20_000);
      const live = await both(scheduled);
      const t0 = Date.now();
      await api.finish(scheduled);
      await Promise.all([wSched, ...crowd].map(w => w.done));
      r.fullTime = {
        clients: crowd.length + 1, streamsDuringMatch: live, allEndedMs: Date.now() - t0,
        reconnects: [wSched, ...crowd].reduce((n, w) => n + w.errors.length, 0)
      };
      assert.ok([wSched, ...crowd].every(w => w.phase === 'over' && w.syncs.scoreboard.state.status === 'FULL_TIME'));
      assert.equal(r.fullTime.reconnects, 0, 'nobody reconnected after FULL_TIME');
      await until(async () => { const b = await both(scheduled); return !b.scoreboard && !b.stats; }, 'upstreams closed on the subgraph', 20_000);

      // d) After the end, straight to the subscription: the subgraph's final state (not a
      //    retained frame from the upstream that closed) and the stream ends.
      const late = { frames: [], retained: 0 };
      for await (const frame of transports.multipart(routerUrls[0], 'scoreboard', scheduled, { headers,
        onPayload: payload => { if (payload.extensions?.snapshot) late.retained++; } })) late.frames.push(frame.state.status);
      r.afterEnd = { frames: late.frames, retainedFrames: late.retained };
      assert.deepEqual(late.frames, ['FULL_TIME']); assert.equal(late.retained, 0);

      // e) A stale query saying LIVE (cache simulated on the client) for a finished match.
      const stale = `stale-live-${run}`;
      await api.startPeriod(stale);
      const liveSnapshot = await snapshotApi(snapshotUrl('scoreboard'), 'scoreboard', stale, { headers }).snapshot(0);
      await api.finish(stale);
      const ts = Date.now();
      const wStale = watch(stale, { api: channel => channel === 'scoreboard' ? { snapshot: async () => liveSnapshot } : snapshotApi(snapshotUrl(channel), channel, stale, { headers }) });
      await wStale.done;
      r.staleLive = { phases: wStale.phases, closedInMs: Date.now() - ts, reconnects: wStale.errors.length };
      assert.deepEqual(wStale.phases, ['live', 'over']); assert.equal(wStale.errors.length, 0);
      log(`lifecycle: ${JSON.stringify(r)}`);
    }

    // 6. Cleanup: with no subscribers, the Router closes the upstreams (and the plugin drops the frames).
    {
      const ours = m => Object.keys(m).filter(k => k.endsWith(run));
      await until(async () => { const m = await metrics(); return !ours(m.scoreboard.activeByMatch ?? {}).length && !ours(m.stats.activeByMatch ?? {}).length; }, 'upstreams closed on the subgraph', 20_000);
      result.cleanup = 'no streams on the subgraph for the test matches';
    }
    return result;
  } finally {
    await closeAll(clients);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  // Against the compose apollo-plugin profile (docker compose --profile apollo-plugin up):
  //   3 routers with the plugin (4101-4103), one without retention (4104), balancer (4200).
  // KILL_CMD takes an instance down mid-match, e.g.:
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
  console.log('OK: deduplication per instance, retained frame for late joiners, identity, live match without regressions and cleanup'
);
}
