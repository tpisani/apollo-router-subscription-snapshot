import { Store } from '../src/store.mjs';
import { ChannelHub } from '../src/channels.mjs';
import { initialScoreboard, initialStats, scoreboardOps, applyStatEvents, playGenerator } from '../src/sports.mjs';

// Experimentos de desenho com os hubs reais e tempo simulado (o chamador controla
// o relógio, ex.: mock.timers do node:test). Gera uma partida e devolve, para
// cada política, os frames que um assinante receberia, com instante e tamanho
// em bytes no formato entregue (multipart do Router: {"payload":{"data":...}}).
const statsOut = state => ({ ...state, players: Object.entries(state.players).map(([id, p]) => ({ id, ...p })) });
const wire = (field, frame) => JSON.stringify({ payload: { data: { [field]: frame } } }).length;

export const STATS_POLICIES = {
  'estado inteiro a cada lance': { coalesceMs: 0, full: true },
  'estado inteiro a cada 500 ms': { coalesceMs: 500, full: true },
  'patch a cada lance': { coalesceMs: 0, full: false, adaptive: false },
  'patch a cada 500 ms': { coalesceMs: 500, full: false, adaptive: false },
  'o menor por janela de 500 ms (adotado)': { coalesceMs: 500, full: false, adaptive: true }
};

// timeline: [{ at: ms, type: 'goal' | 'stats', count }] além do ritmo base.
export function simulateMatch({ tick, now, seconds = 60, eventsPerSecond = 20, storms = [], goals = [], stepMs = 50 }) {
  const matchId = 'm1';
  const out = { scoreboard: [], stats: Object.fromEntries(Object.keys(STATS_POLICIES).map(k => [k, []])), commits: { scoreboard: [], stats: [] } };
  const hubs = [];
  let scoreHub;
  const scores = new Store(':memory:', { initial: initialScoreboard, clock: now, onCommit: c => { out.commits.scoreboard.push(c); scoreHub.publish(c); } });
  scoreHub = new ChannelHub({ store: scores, mode: 'state', checkpointMs: 1000 });
  const stats = new Store(':memory:', { initial: initialStats, clock: now, onCommit: c => { out.commits.stats.push(c); for (const h of hubs) h.publish(c); } });
  for (const [name, policy] of Object.entries(STATS_POLICIES)) {
    const hub = new ChannelHub({ store: stats, mode: 'delta', checkpointMs: 1000, coalesceMs: policy.coalesceMs, adaptive: policy.adaptive ?? false });
    hubs.push(hub);
    hub.subscribe(matchId, frame => {
      // Política "estado inteiro": no instante do frame o store está exatamente na versão dele.
      const body = (policy.full && frame.kind === 'DELTA') || frame.kind === 'STATE'
        ? { matchId, kind: 'STATE', version: frame.version, committedAt: frame.committedAt, state: statsOut(stats.read(matchId).state) }
        : frame;
      out.stats[name].push({ t: now(), bytes: wire('statsUpdates', body), kind: frame.kind, committedAt: frame.committedAt });
    });
  }
  scoreHub.subscribe(matchId, frame => out.scoreboard.push({ t: now(), bytes: wire('scoreboardUpdates', frame), kind: frame.kind, committedAt: frame.committedAt, goal: frame.state.lastEvent?.type === 'GOAL' && frame.committedAt !== null }));

  const next = playGenerator(matchId, 11);
  scores.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, now()));
  const start = now();
  for (let t = 0; t < seconds * 1000; t += stepMs) {
    const storm = storms.find(s => t >= s.from && t < s.to);
    const perStep = ((storm ? storm.eventsPerSecond : eventsPerSecond) * stepMs) / 1000;
    for (let i = 0; i < perStep; i++) stats.update(matchId, s => applyStatEvents(s, [next()], matchId));
    for (const goal of goals) if (goal.at >= t && goal.at < t + stepMs) scores.update(matchId, s => scoreboardOps.scoreGoal(s, { side: goal.side }, now()));
    tick(stepMs);
  }
  tick(1000);
  const elapsed = (now() - start) / 1000;
  for (const h of [scoreHub, ...hubs]) for (const entry of h.keys.values()) { clearInterval(entry.timer); clearTimeout(entry.flushTimer); }
  scores.close(); stats.close();
  return { ...out, elapsed };
}

export const rate = (frames, elapsed) => ({ framesPerSecond: +(frames.length / elapsed).toFixed(1), bytesPerSecond: Math.round(frames.reduce((n, f) => n + f.bytes, 0) / elapsed) });

// Modelo de enlace FIFO com banda fixa (ex.: celular ruim): um frame só começa a
// ser transmitido quando o anterior terminou. Devolve o instante de chegada de cada frame.
export function throughLink(frames, bytesPerSecond) {
  let free = 0;
  return [...frames].sort((a, b) => a.t - b.t).map(frame => {
    const start = Math.max(frame.t, free);
    free = start + (frame.bytes / bytesPerSecond) * 1000;
    return { ...frame, arrival: free };
  });
}
