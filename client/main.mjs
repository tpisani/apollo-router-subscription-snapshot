import { StateSync, DeltaSync, runSync } from './sync.mjs';
import { snapshotApi, transports } from './transport.mjs';
import { clockSeconds } from '../src/sports.mjs';

// Padrão: edge local (dedup + cache, multipart como o Router). Para o Apollo Router,
// GRAPHQL_URL=http://localhost:4100/graphql; para o Hive direto, :4000 com TRANSPORT=ws.
// CHANNELS=scoreboard para uma tela de lista de jogos (só placar).
const url = process.env.GRAPHQL_URL ?? 'http://localhost:4010/graphql';
const transport = transports[process.env.TRANSPORT ?? 'multipart'];
const matchId = process.env.MATCH_ID ?? 'm1';
const channels = (process.env.CHANNELS ?? 'scoreboard,stats').split(',');
const controller = new AbortController();
process.once('SIGINT', () => controller.abort());

const syncs = {
  scoreboard: channels.includes('scoreboard') ? new StateSync({ matchId, api: snapshotApi(url, 'scoreboard', matchId) }) : null,
  stats: channels.includes('stats') ? new DeltaSync({ matchId, api: snapshotApi(url, 'stats', matchId) }) : null
};
const mmss = s => `${String(Math.floor(s / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
function render() {
  const sb = syncs.scoreboard?.state, st = syncs.stats?.state;
  const parts = [];
  if (sb) parts.push(`${sb.status} ${mmss(clockSeconds(sb.clock))}  CASA ${sb.home} x ${sb.away} FORA${syncs.scoreboard.synced ? '' : ' (provisório)'}`);
  if (st) parts.push(`posse ${st.home.possession}%-${st.away.possession}%  chutes ${st.home.shots}-${st.away.shots}  passes ${st.home.passes}-${st.away.passes}${syncs.stats.synced ? '' : ' (provisório)'}`);
  if (parts.length) console.log(parts.join('  |  '));
}
// O relógio avança no cliente: nenhuma mensagem chega a cada segundo para isso.
const ticker = setInterval(render, 1000);
controller.signal.addEventListener('abort', () => clearInterval(ticker));

await Promise.all(Object.entries(syncs).filter(([, sync]) => sync).map(([channel, sync]) => runSync({
  sync, signal: controller.signal,
  connect: async function* (signal) {
    for await (const frame of transport(url, channel, matchId, { signal })) {
      if (frame.committedAt && channel === 'scoreboard') console.log(`  placar v${frame.version}: ${frame.state.lastEvent?.type ?? ''} em ${Date.now() - frame.committedAt} ms`);
      yield frame;
    }
  },
  onError: (error, attempt) => console.error(`${channel}: reconectando (tentativa ${attempt + 1}) a partir da versão ${sync.version}: ${error.message}`)
})));
