import { scoreboardOps, applyStatEvents, playGenerator } from './sports.mjs';

// Simulates a match's data provider, writing straight into the stores (as an
// ingestor would). Stats: eventsPerSecond plays; scoreboard: an occasional goal.

export function simulateMatch({ stores, matchId = 'm1', eventsPerSecond = 20, goalChancePerSecond = 0.03, seed = 7 }) {
  const next = playGenerator(matchId, seed);
  let x = seed * 31;
  const rand = () => (x = (x * 1103515245 + 12345) % 2 ** 31) / 2 ** 31;
  const now = () => Date.now();
  if (stores.scoreboard.read(matchId).state.status !== 'LIVE') {
    stores.scoreboard.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, now()));
  }
  const tick = Math.max(10, Math.round(1000 / eventsPerSecond));
  const plays = setInterval(() => stores.stats.update(matchId, s => applyStatEvents(s, [next()], matchId)), tick);
  const goals = setInterval(() => {
    if (rand() < goalChancePerSecond) stores.scoreboard.update(matchId, s => scoreboardOps.scoreGoal(s, { side: rand() < 0.5 ? 'HOME' : 'AWAY' }, now()));
  }, 1000);
  return () => { clearInterval(plays); clearInterval(goals); };
}
