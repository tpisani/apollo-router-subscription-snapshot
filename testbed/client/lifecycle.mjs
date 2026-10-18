import { createSync, runSync, isFinalScoreboard } from './sync.mjs';
import { matchPhase, LIVE_LEAD_MS } from '../subgraphs/sports.mjs';

// Lifecycle of a match screen: query or subscription depending on the time and
// the status.
//   1. Scoreboard query (minVersion 0, possibly from the cache) and one per extra channel.
//   2. FULL_TIME: final state, no subscription.
//   3. SCHEDULED before kickoff − lead: query only; schedules the subscription for
//      kickoff − lead. An old SCHEDULED in the cache delays nothing: the clock decides.
//   4. Inside the window, live or at half time: subscription on every channel. If the
//      cache said LIVE but the match is already over, the first frame brings FULL_TIME.
//   5. FULL_TIME frame: the subgraph ends the scoreboard; the client closes the other channels.
// api(channel): { snapshot(min) }   connect(channel, signal): the channel's frames
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
    // The end of the scoreboard ends the whole match: the other channels stop with it.

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
