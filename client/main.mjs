import { watchMatch } from './lifecycle.mjs';
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

// O placar decide o ciclo de vida (query antes da janela de início, subscription
// até o FULL_TIME), então ele está sempre presente.
if (!channels.includes('scoreboard')) channels.unshift('scoreboard');
const mmss = s => `${String(Math.floor(s / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
const watch = watchMatch({
  matchId, channels, signal: controller.signal,
  api: channel => snapshotApi(url, channel, matchId),
  connect: async function* (channel, signal) {
    for await (const frame of transport(url, channel, matchId, { signal })) {
      if (frame.committedAt && channel === 'scoreboard') console.log(`  placar v${frame.version}: ${frame.state.lastEvent?.type ?? ''} em ${Date.now() - frame.committedAt} ms`);
      yield frame;
    }
  },
  onPhase: phase => console.log({
    before: '— ainda não começou: só a query; assina 15 min antes do início',
    live: '— ao vivo: subscription',
    over: '— encerrada: estado final, sem subscription'
  }[phase]),
  onError: (error, attempt) => console.error(`reconectando (tentativa ${attempt + 1}): ${error.message}`)
});
const { syncs } = watch;
function render() {
  const sb = syncs.scoreboard?.state, st = syncs.stats?.state;
  const parts = [];
  if (sb) parts.push(`${sb.status} ${mmss(clockSeconds(sb.clock))}  CASA ${sb.home} x ${sb.away} FORA${syncs.scoreboard.synced ? '' : ' (query)'}`);
  if (st) parts.push(`posse ${st.home.possession}%-${st.away.possession}%  chutes ${st.home.shots}-${st.away.shots}  passes ${st.home.passes}-${st.away.passes}${syncs.stats.synced ? '' : ' (query)'}`);
  if (parts.length) console.log(parts.join('  |  '));
}
// O relógio avança no cliente: nenhuma mensagem chega a cada segundo para isso.
const ticker = setInterval(render, 1000);
await watch.done;
clearInterval(ticker); render();
