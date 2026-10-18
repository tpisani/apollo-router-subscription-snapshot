import { SyncError } from './store.mjs';

// Dados estáticos (subgraph catalog): times e elencos fictícios.
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

// ---- Ciclo de vida: quando vale assinar --------------------------------------
// O horário de início decide quando começar a assinar; o status só decide quando
// parar. Antes de kickoff − LIVE_LEAD_MS basta a query; a partir daí, subscription
// até FULL_TIME (atrasos ficam cobertos: o cliente já está assinando). O mesmo
// valor limita o cache do placar (scoreboardMaxAge), então nenhum cache serve
// "não começou" depois da hora de começar a assinar.
export const LIVE_LEAD_MS = 15 * 60_000;

// 'before': só query, assinar em subscribeAt; 'live': subscription; 'over': estado final.
export function matchPhase({ status, kickoff }, now = Date.now(), lead = LIVE_LEAD_MS) {
  if (status === 'FULL_TIME') return { phase: 'over' };
  if (status !== 'SCHEDULED' || kickoff == null) return { phase: 'live' };   // sem horário: assina, por segurança
  const subscribeAt = kickoff - lead;
  return now < subscribeAt ? { phase: 'before', subscribeAt } : { phase: 'live' };
}

// Cache-Control (segundos) da query do placar, pelo status:
//   SCHEDULED antes da janela: até `scheduled`, mas nunca além de kickoff − lead;
//   na janela, ao vivo ou no intervalo: `live` (curto);  FULL_TIME: `final` (longo).
export function scoreboardMaxAge(state, now, { live, scheduled = 300, final = 3600, lead = LIVE_LEAD_MS } = {}) {
  const { phase, subscribeAt } = matchPhase(state, now, lead);
  if (phase === 'over') return final;
  if (phase === 'before') return Math.min(scheduled, Math.floor((subscribeAt - now) / 1000));
  return live;
}

// ---- Placar: pequeno, muda pouco, precisa chegar rápido -------------------------
const kickoffOf = matchId => (MATCHES[matchId] ? Date.parse(MATCHES[matchId].kickoff) : null);
export const initialScoreboard = matchId => ({
  status: 'SCHEDULED', period: 0, home: 0, away: 0, kickoff: kickoffOf(matchId),
  // O relógio não é enviado a cada segundo: o cliente avança `seconds` a partir de
  // `updatedAt` enquanto `running`. Só início, pausa e fim geram commit.
  clock: { running: false, seconds: 0, updatedAt: 0 },
  lastEvent: null
});

export function clockSeconds(clock, now = Date.now()) {
  return clock.running ? clock.seconds + Math.floor((now - clock.updatedAt) / 1000) : clock.seconds;
}

export const scoreboardOps = {
  // Agenda ou remarca o início (epoch ms); só antes de começar.
  scheduleMatch(state, { kickoff }) {
    if (state.status !== 'SCHEDULED') throw new SyncError('Partida já começou', 'BAD_USER_INPUT');
    if (!Number.isFinite(kickoff)) throw new SyncError('kickoff inválido', 'BAD_USER_INPUT');
    state.kickoff = kickoff;
  },
  startPeriod(state, { period }, now) {
    if (!Number.isInteger(period) || period < 1 || period > 2) throw new SyncError('Período inválido', 'BAD_USER_INPUT');
    state.status = 'LIVE'; state.period = period;
    state.clock = { running: true, seconds: (period - 1) * 45 * 60, updatedAt: now };
    state.lastEvent = { type: 'PERIOD_START', side: null, minute: (period - 1) * 45 };
  },
  endPeriod(state, { final }, now) {
    if (state.status !== 'LIVE') throw new SyncError('Partida não está em andamento', 'BAD_USER_INPUT');
    const seconds = clockSeconds(state.clock, now);
    state.status = final ? 'FULL_TIME' : 'HALF_TIME';
    state.clock = { running: false, seconds, updatedAt: now };
    state.lastEvent = { type: final ? 'FULL_TIME' : 'HALF_TIME', side: null, minute: Math.floor(seconds / 60) };
  },
  scoreGoal(state, { side }, now) {
    if (state.status !== 'LIVE') throw new SyncError('Gol fora de jogo em andamento', 'BAD_USER_INPUT');
    if (side !== 'HOME' && side !== 'AWAY') throw new SyncError('Lado inválido', 'BAD_USER_INPUT');
    state[side === 'HOME' ? 'home' : 'away'] += 1;
    state.lastEvent = { type: 'GOAL', side, minute: Math.floor(clockSeconds(state.clock, now) / 60) };
  }
};

// ---- Estatísticas: maiores, mudam várias vezes por segundo ---------------------
const teamStats = () => ({ possession: 50, touches: 0, shots: 0, shotsOnTarget: 0, passes: 0, corners: 0, fouls: 0 });
export const initialStats = () => ({ home: teamStats(), away: teamStats(), players: {} });

export const STAT_TYPES = ['TOUCH', 'PASS', 'SHOT', 'SHOT_ON_TARGET', 'CORNER', 'FOUL'];

// A ingestão chega como incrementos (o que o provedor de dados manda); o estado e
// os patches publicados são valores finais.
export function applyStatEvents(state, events, matchId) {
  const valid = new Set(playersOf(matchId).map(p => p.id));
  for (const { side, playerId, type } of events) {
    if ((side !== 'HOME' && side !== 'AWAY') || !STAT_TYPES.includes(type) || !valid.has(playerId)) {
      throw new SyncError(`Evento inválido: ${side}/${playerId}/${type}`, 'BAD_USER_INPUT');
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

// Gerador de jogadas para o simulador, demo e benchmark (determinístico com semente).
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
