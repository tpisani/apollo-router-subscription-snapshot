import assert from 'node:assert/strict';
import { graphql, apiFor } from '../client/transport.mjs';
const url = process.env.GRAPHQL_URL ?? 'http://localhost:4000/graphql';
const scope = process.env.SCOPE_ID ?? 'loja-42';
const api = apiFor(url,scope);
async function upsert(status) {
  return graphql(url,'mutation($scope:ID!,$status:String!) { upsertOrder(scopeId:$scope,id:"order-1",status:$status) { cursor } }',{scope,status});
}
await upsert('PENDING');
const before = await api.snapshot(false);
await upsert('PAID');
const cached = await api.snapshot(false);
const head = await api.head();
assert.equal(cached.cursor,before.cursor,'Execute em até 60s para observar o cache');
assert.ok(Number(head)>Number(cached.cursor));
console.log(JSON.stringify({ snapshotCacheado:cached,headAtual:head },null,2));
console.log('O snapshot permanece antigo. O cliente usa histórico para alcançar o head.');
