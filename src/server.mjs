import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { startSubgraphs } from './app.mjs';

const dbPath = process.env.DB_PATH ?? './data/orders.sqlite';
mkdirSync(dirname(dbPath), { recursive: true });
const app = await startSubgraphs({
  dbPath,
  ordersPort: Number(process.env.ORDERS_PORT ?? 4001),
  catalogPort: Number(process.env.CATALOG_PORT ?? 4002),
  maxReplay: Number(process.env.MAX_REPLAY ?? 1000),
  queueLimit: Number(process.env.QUEUE_LIMIT ?? 1000)
});
console.log(`orders  http/ws://localhost:${app.ports.orders}/graphql`);
console.log(`catalog http://localhost:${app.ports.catalog}/graphql`);
for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => app.close().then(() => process.exit(0)));
