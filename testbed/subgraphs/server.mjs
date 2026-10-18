import { mkdirSync } from 'node:fs';
import { startSubgraphs } from './app.mjs';
import { simulateMatch } from './simulator.mjs';

const dataDir = process.env.DATA_DIR ?? './data';
mkdirSync(dataDir, { recursive: true });
const app = await startSubgraphs({
  dataDir,
  ports: { scores: Number(process.env.SCORES_PORT ?? 4001), catalog: Number(process.env.CATALOG_PORT ?? 4002), stats: Number(process.env.STATS_PORT ?? 4003) },
  scoreboard: { periodicMs: Number(process.env.SCOREBOARD_PERIODIC_MS ?? 1000) },
  stats: { periodicMs: Number(process.env.STATS_PERIODIC_MS ?? 1000), throttleMs: Number(process.env.STATS_THROTTLE_MS ?? 500) }
});
console.log(`scores  http/ws://localhost:${app.ports.scores}/graphql  (métricas em /metrics)`);
console.log(`catalog http://localhost:${app.ports.catalog}/graphql`);
console.log(`stats   http/ws://localhost:${app.ports.stats}/graphql`);
const stop = process.env.SIMULATE ? simulateMatch({ stores: app.stores, matchId: process.env.SIMULATE, eventsPerSecond: Number(process.env.EVENTS_PER_SECOND ?? 20) }) : () => {};
if (process.env.SIMULATE) console.log(`simulando a partida ${process.env.SIMULATE}`);
for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => { stop(); app.close().then(() => process.exit(0)); });
