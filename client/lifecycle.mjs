import { createSync, runSync, isFinalScoreboard } from './sync.mjs';
import { matchPhase, LIVE_LEAD_MS } from '../src/sports.mjs';

// Ciclo de vida de uma tela de partida: query ou subscription conforme o horário e
// o status.
//   1. Query do placar (minVersion 0, possivelmente do cache) e uma por canal extra.
//   2. FULL_TIME: estado final, nenhuma subscription.
//   3. SCHEDULED antes de kickoff − lead: só a query; agenda a subscription para
//      kickoff − lead. Um SCHEDULED antigo no cache não atrasa nada: o horário decide.
//   4. Na janela, ao vivo ou no intervalo: subscription em todos os canais. Se o
//      cache disse LIVE mas a partida já acabou, o primeiro frame traz FULL_TIME.
//   5. Frame FULL_TIME: o subgraph encerra o placar; o cliente fecha os outros canais.
// api(channel): { snapshot(min) }   connect(channel, signal): frames do canal
export function watchMatch({ matchId, channels = ['scoreboard', 'stats'], api, connect, now = Date.now, lead = LIVE_LEAD_MS,
  signal, onUpdate = () => {}, onPhase = () => {}, onError = () => {}, runOptions = {} }) {
  const syncs = Object.fromEntries(channels.map(channel => [channel, createSync(channel, { matchId, api: api(channel) })]));
  const counts = { queries: 0, subscriptions: 0 };
  let phase = null; let timer = null;
  const live = new AbortController();
  const stop = () => { clearTimeout(timer); live.abort(); };
  signal?.addEventListener('abort', stop, { once: true });
  const setPhase = next => { if (next !== phase) { phase = next; onPhase(next); } };

  const query = async channel => {
    counts.queries++;
    const snapshot = await syncs[channel].api.snapshot(0);
    syncs[channel].initialize(snapshot);
    onUpdate(channel, syncs[channel].view());
  };
  const subscribe = () => Promise.all(channels.map(channel => {
    counts.subscriptions++;
    const run = runSync({ ...runOptions, sync: syncs[channel], signal: live.signal, onError,
      connect: sessionSignal => connect(channel, sessionSignal),
      onUpdate: view => onUpdate(channel, view),
      isFinal: channel === 'scoreboard' ? isFinalScoreboard : () => false });
    // O fim do placar encerra a partida inteira: os outros canais param junto.
    return channel === 'scoreboard' ? run.then(() => { if (isFinalScoreboard(syncs.scoreboard.state)) { setPhase('over'); live.abort(); } }) : run;
  }));

  const done = (async () => {
    await Promise.all(channels.map(query));
    const decision = matchPhase(syncs.scoreboard.state, now(), lead);
    setPhase(decision.phase);
    if (decision.phase === 'over' || live.signal.aborted) return;
    if (decision.phase === 'before') {
      await new Promise(resolve => { timer = setTimeout(resolve, decision.subscribeAt - now()); live.signal.addEventListener('abort', resolve, { once: true }); });
      if (live.signal.aborted) return;
      setPhase('live');
    }
    await subscribe();
  })();
  return { syncs, counts, done, get phase() { return phase; }, close: async () => { stop(); await done.catch(() => {}); } };
}
