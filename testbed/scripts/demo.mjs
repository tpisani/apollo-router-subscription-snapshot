import { setTimeout as delay } from 'node:timers/promises';
import { graphql } from '../client/transport.mjs';
import { playGenerator } from '../subgraphs/sports.mjs';

// Simula o provedor de dados pela API (mutations): lances a EVENTS_PER_SECOND e um
// gol a cada GOAL_EVERY_S segundos. Rode `npm run client` em outro terminal.
const url = process.env.GRAPHQL_URL ?? 'http://localhost:4010/graphql';
const matchId = process.env.MATCH_ID ?? 'm1';
const seconds = Number(process.env.SECONDS ?? 60);
const perSecond = Number(process.env.EVENTS_PER_SECOND ?? 20);
const goalEvery = Number(process.env.GOAL_EVERY_S ?? 15);
const next = playGenerator(matchId, Date.now() % 1000);
const { scoreboard } = await graphql(url, 'query($m: ID!) { scoreboard(matchId: $m, minVersion: 0) { state { status } } }', { m: matchId });
if (scoreboard.state.status !== 'LIVE') await graphql(url, 'mutation($m: ID!) { startPeriod(matchId: $m, period: 1) { version } }', { m: matchId });
const t0 = Date.now();
for (let tick = 0; Date.now() - t0 < seconds * 1000; tick++) {
  const events = Array.from({ length: Math.max(1, Math.round(perSecond / 4)) }, next);
  const version = (await graphql(url, 'mutation($m: ID!, $e: [StatEventInput!]!) { recordStats(matchId: $m, events: $e) }', { m: matchId, e: events })).recordStats;
  if (tick > 0 && tick % (goalEvery * 4) === 0) {
    const side = Math.random() < 0.5 ? 'HOME' : 'AWAY';
    const goal = (await graphql(url, 'mutation($m: ID!, $s: Side!) { scoreGoal(matchId: $m, side: $s) { version state { home away } } }', { m: matchId, s: side })).scoreGoal;
    console.log(`GOL ${side}: ${goal.state.home} x ${goal.state.away} (placar v${goal.version})`);
  } else if (tick % 4 === 0) console.log(`estatísticas v${version}`);
  await delay(250);
}
