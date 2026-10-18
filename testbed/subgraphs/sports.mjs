import { SyncError } from './store.mjs';

// Static data (catalog subgraph): fictional teams and squads.
const roster = (prefix, names) => names.map((name, i) => ({ id: `${prefix}${i + 1}`, name, number: i + 1, side: prefix === 'h' ? 'HOME' : 'AWAY' }));
export const MATCHES = {
  m1: {
    id: 'm1', kickoff: '2026-10-18T19:00:00Z',
    home: { id: 'serrano', name: 'Atlético Serrano', shortName: 'SER' },
    away: { id: 'litoral', name: 'União Litoral', shortName: 'ULI' },
    players: [
      ...roster('h', ['Breno', 'Caio', 'Davi', 'Enzo', 'Fábio', 'Gael', 'Heitor', 'Ícaro', 'Joel', 'Lauro', 'Murilo']),
      ...roster('a', ['Nilo', 'Otávio', 'Pietro', 'Quirino', 'Rafael', 'Saulo', 'Tadeu', 'Ulisses', 'Vítor', 'Wagner', 'Yuri'])
    ]
  }
};
export const playersOf = matchId => MATCHES[matchId]?.players ?? MATCHES.m1.players;

// ---- Lifecycle: when subscribing is worth it ---------------------------------
// The kickoff time decides when to start subscribing; the status only decides when
// to stop. Before kickoff − LIVE_LEAD_MS the query is enough; from then on, subscription
// until FULL_TIME (delays are covered: the client is already subscribed). The same
// value caps the scoreboard cache (scoreboardMaxAge), so no cache serves
// "not started" after the time to start subscribing.
export const LIVE_LEAD_MS = 15 * 60_000;

// 'before': query only, subscribe at subscribeAt; 'live': subscription; 'over': final state.
export function matchPhase({ status, kickoff }, now = Date.now(), lead = LIVE_LEAD_MS) {
  if (status === 'FULL_TIME') return { phase: 'over' };
  if (status !== 'SCHEDULED' || kickoff == null) return { phase: 'live' };   // no kickoff time: subscribe, to be safe
  const subscribeAt = kickoff - lead;
  return now < subscribeAt ? { phase: 'before', subscribeAt } : { phase: 'live' };
}

// Cache-Control (seconds) of the scoreboard query, by status:
//   SCHEDULED before the window: up to `scheduled`, but never past kickoff − lead;
//   inside the window, live or at half time: `live` (short);  FULL_TIME: `final` (long).
export function scoreboardMaxAge(state, now, { live, scheduled = 300, final = 3600, lead = LIVE_LEAD_MS } = {}) {
  const { phase, subscribeAt } = matchPhase(state, now, lead);
  if (phase === 'over') return final;
  if (phase === 'before') return Math.min(scheduled, Math.floor((subscribeAt - now) / 1000));
  return live;
}

// ---- Scoreboard: small, changes rarely, must arrive fast ------------------------
const kickoffOf = matchId => (MATCHES[matchId] ? Date.parse(MATCHES[matchId].kickoff) : null);
export const initialScoreboard = matchId => ({
  status: 'SCHEDULED', period: 0, home: 0, away: 0, kickoff: kickoffOf(matchId),
  // The clock is not sent every second: the client advances `seconds` from
  // `updatedAt` while `running`. Only start, pause and end produce a commit.
  clock: { running: false, seconds: 0, updatedAt: 0 },
  lastEvent: null
});

export function clockSeconds(clock, now = Date.now()) {
  return clock.running ? clock.seconds + Math.floor((now - clock.updatedAt) / 1000) : clock.seconds;
}

export const scoreboardOps = {
  // Schedules or reschedules the kickoff (epoch ms); only before it starts.
  scheduleMatch(state, { kickoff }) {
    if (state.status !== 'SCHEDULED') throw new SyncError('Match already started', 'BAD_USER_INPUT');
    if (!Number.isFinite(kickoff)) throw new SyncError('Invalid kickoff', 'BAD_USER_INPUT');
    state.kickoff = kickoff;
  },
  startPeriod(state, { period }, now) {
    if (!Number.isInteger(period) || period < 1 || period > 2) throw new SyncError('Invalid period', 'BAD_USER_INPUT');
    state.status = 'LIVE'; state.period = period;
    state.clock = { running: true, seconds: (period - 1) * 45 * 60, updatedAt: now };
    state.lastEvent = { type: 'PERIOD_START', side: null, minute: (period - 1) * 45 };
  },
  endPeriod(state, { final }, now) {
    if (state.status !== 'LIVE') throw new SyncError('Match is not in progress', 'BAD_USER_INPUT');
    const seconds = clockSeconds(state.clock, now);
    state.status = final ? 'FULL_TIME' : 'HALF_TIME';
    state.clock = { running: false, seconds, updatedAt: now };
    state.lastEvent = { type: final ? 'FULL_TIME' : 'HALF_TIME', side: null, minute: Math.floor(seconds / 60) };
  },
  scoreGoal(state, { side }, now) {
    if (state.status !== 'LIVE') throw new SyncError('Goal outside a match in progress', 'BAD_USER_INPUT');
    if (side !== 'HOME' && side !== 'AWAY') throw new SyncError('Invalid side', 'BAD_USER_INPUT');
    state[side === 'HOME' ? 'home' : 'away'] += 1;
    state.lastEvent = { type: 'GOAL', side, minute: Math.floor(clockSeconds(state.clock, now) / 60) };
  }
};

// ---- Stats: larger, change several times per second -----------------------------
const teamStats = () => ({ possession: 50, touches: 0, shots: 0, shotsOnTarget: 0, passes: 0, corners: 0, fouls: 0 });
export const initialStats = () => ({ home: teamStats(), away: teamStats(), players: {} });

export const STAT_TYPES = ['TOUCH', 'PASS', 'SHOT', 'SHOT_ON_TARGET', 'CORNER', 'FOUL'];

// Ingestion arrives as increments (what the data provider sends); the state and
// the published patches are final values.
export function applyStatEvents(state, events, matchId) {
  const valid = new Set(playersOf(matchId).map(p => p.id));
  for (const { side, playerId, type } of events) {
    if ((side !== 'HOME' && side !== 'AWAY') || !STAT_TYPES.includes(type) || !valid.has(playerId)) {
      throw new SyncError(`Invalid event: ${side}/${playerId}/${type}`, 'BAD_USER_INPUT');
    }
    const team = state[side === 'HOME' ? 'home' : 'away'];
    const player = state.players[playerId] ??= { touches: 0, passes: 0, shots: 0 };
    team.touches++; player.touches++;
    if (type === 'PASS') { team.passes++; player.passes++; }
    if (type === 'SHOT' || type === 'SHOT_ON_TARGET') { team.shots++; player.shots++; }
    if (type === 'SHOT_ON_TARGET') team.shotsOnTarget++;
    if (type === 'CORNER') team.corners++;
    if (type === 'FOUL') team.fouls++;
  }
  const total = state.home.touches + state.away.touches;
  state.home.possession = total ? Math.round((100 * state.home.touches) / total) : 50;
  state.away.possession = 100 - state.home.possession;
}

// Play generator for the simulator, demo and benchmark (deterministic with a seed).

export function playGenerator(matchId, seed = 1) {
  let x = seed;
  const rand = () => (x = (x * 1103515245 + 12345) % 2 ** 31) / 2 ** 31;
  const players = playersOf(matchId);
  const weights = [['PASS', 0.62], ['TOUCH', 0.25], ['FOUL', 0.05], ['SHOT', 0.04], ['SHOT_ON_TARGET', 0.02], ['CORNER', 0.02]];
  return () => {
    const player = players[Math.floor(rand() * players.length)];
    let r = rand(), type = 'PASS';
    for (const [name, weight] of weights) { if ((r -= weight) <= 0) { type = name; break; } }
    return { side: player.side, playerId: player.id, type };
  };
}
