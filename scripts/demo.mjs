import { setTimeout as delay } from 'node:timers/promises';
import { graphql } from '../client/transport.mjs';

// Gera escritas contínuas para observar `npm run client` em outro terminal.
const url = process.env.GRAPHQL_URL ?? 'http://localhost:4010/graphql';
const scopeId = process.env.SCOPE_ID ?? 'loja-42';
const count = Number(process.env.COUNT ?? 20);
const statuses = ['PENDING', 'PAID', 'SHIPPED'];
for (let i = 0; i < count; i++) {
  const id = `order-${i % 4}`;
  const { upsertOrder } = await graphql(url, `mutation($s: ID!, $id: ID!, $v: String!) {
    upsertOrder(scopeId: $s, id: $id, status: $v) { cursor }
  }`, { s: scopeId, id, v: statuses[i % 3] });
  console.log(`commit ${upsertOrder.cursor}: ${id} -> ${statuses[i % 3]}`);
  await delay(Number(process.env.INTERVAL_MS ?? 500));
}
