import { watchMatch } from './lifecycle.mjs';
import { snapshotApi, transports } from './transport.mjs';
import { clockSeconds } from '../subgraphs/sports.mjs';

// Default: local edge (dedup + cache, multipart like the Router). For the Apollo Router,
// GRAPHQL_URL=http://localhost:4100/graphql; for Hive directly, :4000 with TRANSPORT=ws.
// CHANNELS=scoreboard for a match list screen (scoreboard only).
const url = process.env.GRAPHQL_URL ?? 'http://localhost:4010/graphql';
const transport = transports[process.env.TRANSPORT ?? 'multipart'];
const matchId = process.env.MATCH_ID ?? 'm1';
const channels = (process.env.CHANNELS ?? 'scoreboard,stats').split(',');
const controller = new AbortController();
process.once('SIGINT', () => controller.abort());

// The scoreboard drives the lifecycle (query before the kickoff window, subscription
// until FULL_TIME), so it is always present.
if (!channels.includes('scoreboard')) channels.unshift('scoreboard');
const mmss = s => `${String(Math.floor(s / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
const watch = watchMatch({
  matchId, channels, signal: controller.signal,
  api: channel => snapshotApi(url, channel, matchId),
  connect: async function* (channel, signal) {
    for await (const frame of transport(url, channel, matchId, { signal })) {
      if (frame.committedAt && channel === 'scoreboard') console.log(`  scoreboard v${frame.version}: ${frame.state.lastEvent?.type ?? ''} in ${Date.now() - frame.committedAt} ms`);
      yield frame;
    }
  },
  onPhase: phase => console.log({
    before: '— not started yet: query only; subscribes 15 min before kickoff',
    live: '— live: subscription',
    over: '— finished: final state, no subscription'
  }[phase]),
  onError: (error, attempt) => console.error(`reconnecting (attempt ${attempt + 1}): ${error.message}`)
});
const { syncs } = watch;
function render() {
  const sb = syncs.scoreboard?.state, st = syncs.stats?.state;
  const parts = [];
  if (sb) parts.push(`${sb.status} ${mmss(clockSeconds(sb.clock))}  HOME ${sb.home} x ${sb.away} AWAY${syncs.scoreboard.synced ? '' : ' (query)'}`);
  if (st) parts.push(`possession ${st.home.possession}%-${st.away.possession}%  shots ${st.home.shots}-${st.away.shots}  passes ${st.home.passes}-${st.away.passes}${syncs.stats.synced ? '' : ' (query)'}`);
  if (parts.length) console.log(parts.join('  |  '));
}
// The clock ticks on the client: no message arrives every second for that.

const ticker = setInterval(render, 1000);
await watch.done;
clearInterval(ticker); render();
