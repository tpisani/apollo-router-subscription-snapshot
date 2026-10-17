import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { startSubgraphs } from './app.mjs';

const dbPath = process.env.DB_PATH ?? './data/orders.sqlite';
mkdirSync(dirname(dbPath), { recursive: true });
const app = await startSubgraphs({
  dbPath,
  ordersPort: Number(process.env.ORDERS_PORT ?? 4001),
  catalogPort: Number(process.env.CATALOG_PORT ?? 4002),
  checkpointMs: Number(process.env.CHECKPOINT_MS ?? 250),
  queueLimit: Number(process.env.QUEUE_LIMIT ?? 1000),
  snapshotTtlMs: Number(process.env.SNAPSHOT_TTL_MS ?? 120000)
});
console.log(`orders  http/ws://localhost:${app.ports.orders}/graphql  (métricas em /metrics)`);
console.log(`catalog http://localhost:${app.ports.catalog}/graphql`);
for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => app.close().then(() => process.exit(0)));
