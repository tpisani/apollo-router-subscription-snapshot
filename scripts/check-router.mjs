import assert from 'node:assert/strict';
import { apiFor, graphql, routerFrames } from '../client/transport.mjs';
import { Synchronizer, runSession } from '../client/sync.mjs';
import { setTimeout as delay } from 'node:timers/promises';
const url = process.env.GRAPHQL_URL ?? 'http://localhost:4000/graphql';
const metricsUrl = process.env.METRICS_URL ?? 'http://localhost:4001/metrics';
const scope = `check-${Date.now()}`;
const api = apiFor(url,scope);
const controllers = [];
const jobs = [];
const clients = [];
const sessionErrors = [];
async function until(predicate) {
  for (let i=0;i<100;i++) { if (await predicate()) return; await delay(100); }
  throw new Error('Timeout na verificação do Router');
}
async function write(status) {
  await graphql(url,'mutation($scope:ID!,$status:String!) { upsertOrder(scopeId:$scope,id:"1",status:$status) { cursor } }',{scope,status});
}
async function connect(existing) {
  const controller = new AbortController(); controllers.push(controller);
  const sync = existing ?? new Synchronizer(api,scope);
  let initialized = false;
  jobs.push(runSession(sync,routerFrames(url,scope,controller.signal),{
    resume:!!existing,onUpdate:()=> {initialized=true;}
  }).catch(error=> { if (!controller.signal.aborted) sessionErrors.push(error); }));
  await until(()=>initialized); clients.push(sync); return sync;
}
try {
  await write('PENDING');
  const old = await api.snapshot(false); // Aquece os dois caches.
  const readsAfterWarmup = (await (await fetch(metricsUrl)).json()).snapshotReads;
  await write('PAID');
  const cached = await api.snapshot(false);
  assert.equal(cached.cursor,old.cursor);
  const readsAfterHit = (await (await fetch(metricsUrl)).json()).snapshotReads;
  assert.equal(readsAfterHit,readsAfterWarmup,'O segundo snapshot deve ser servido pelo cache do Router');
  const first = await connect();
  const second = await connect(); // Late joiner: nenhum snapshot/replay no stream.
  assert.equal(first.orders.get('1').status,'PAID');
  assert.deepEqual(first.view(),second.view());
  await delay(700);
  const metrics = await (await fetch(metricsUrl)).json();
  assert.equal(metrics.activeSubscriptions,1,'Use uma instância limpa e sem outros clientes');
  await write('SHIPPED');
  await until(()=>clients.every(c=>c.orders.get('1')?.status==='SHIPPED'));
  controllers[0].abort();
  await write('PENDING');
  await connect(first); // Retoma o estado existente; recupera mudanças durante a desconexão.
  assert.equal(first.orders.get('1').status,'PENDING');
  const fresh = await api.snapshot(true);
  assert.equal(fresh.cursor,await api.head());
  assert.equal(fresh.orders[0].status,'PENDING');
  assert.equal(sessionErrors.length,0,sessionErrors.map(error=>error.message).join('; '));
  console.log('OK: snapshot antigo, replay, late join, deduplicação, live, fresh e reconexão');
} finally {
  controllers.forEach(c=>c.abort()); await Promise.all(jobs);
}
