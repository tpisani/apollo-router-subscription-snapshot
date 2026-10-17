import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createClient } from 'graphql-ws';
import WebSocket from 'ws';
import { apiFor, graphql, LIVE } from '../client/transport.mjs';
import { Synchronizer, runSession } from '../client/sync.mjs';

// Processo real + SQLite real + GraphQL HTTP + WebSocket do subgraph.
// O Router não é substituído por um fake: sua verificação é npm run test:router.
test('subgraphs reais: cache HTTP, WS, replay, bootstrap e referência federada', {timeout:20000},async () => {
  const dir=mkdtempSync(join(tmpdir(),'subgraphs-example-'));
  const child=spawn(process.execPath,['src/server.mjs'],{env:{...process.env,DB_PATH:join(dir,'db.sqlite')},stdio:['ignore','pipe','pipe']});
  let logs=''; child.stdout.on('data',b=>logs+=b); child.stderr.on('data',b=>logs+=b);
  let client;
  try {
    let ready=false;
    for(let i=0;i<100;i++) {
      if (child.exitCode !== null) throw new Error(logs);
      if (logs.includes('localhost:4002')) {ready=true;break;}
      await delay(50);
    }
    assert.ok(ready,logs);
    const url='http://localhost:4001/graphql',scope='http-integration';
    const api=apiFor(url,scope);
    async function write(status) {
      await graphql(url,'mutation($s:ID!,$v:String!){upsertOrder(scopeId:$s,id:"1",status:$v){cursor}}',{s:scope,v:status});
    }
    await write('PENDING'); const initial=await api.snapshot(false);
    await write('PAID'); assert.deepEqual(await api.snapshot(false),initial);
    const query='query($s:ID!){ordersSnapshot(scopeId:$s){scopeId cursor orders{id version status}}}';
    const response=await fetch(url,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({query,variables:{s:scope}})});
    assert.equal(response.headers.get('cache-control'),'public, max-age=60');
    await response.json();
    const fresh=await api.snapshot(true); assert.equal(fresh.orders[0].status,'PAID');
    const headResponse=await fetch(url,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({query:'{orderHead(scopeId:"http-integration")}'})});
    assert.equal(headResponse.headers.get('cache-control'),'no-store'); await headResponse.json();
    client=createClient({url:'ws://localhost:4001/graphql',webSocketImpl:WebSocket,retryAttempts:0});
    const subscription=client.iterate({query:LIVE,variables:{scope}});
    async function* frames() {for await (const result of subscription) {
      if(result.errors) throw new Error(JSON.stringify(result.errors));
      yield result.data.orderEvents;
    }}
    const sync=new Synchronizer(api,scope);
    let bootstrapped;
    const boot=new Promise(resolve=>bootstrapped=resolve);
    const session=runSession(sync,frames(),{onUpdate:()=>bootstrapped()}).catch(error=>error);
    await boot; assert.equal(sync.orders.get('1').status,'PAID');
    await write('SHIPPED');
    for(let i=0;i<30 && sync.orders.get('1').status!=='SHIPPED';i++) await delay(100);
    assert.equal(sync.orders.get('1').status,'SHIPPED');
    await client.dispose(); await session;
    // O segundo subgraph resolve sua parte da entidade Order via _entities.
    const catalog=await graphql('http://localhost:4002/graphql',
      'query($r:[_Any!]!){_entities(representations:$r){...on Order{id label}}}',{r:[{__typename:'Order',id:'1'}]});
    assert.equal(catalog._entities[0].label,'Pedido 1');
  } finally {
    await client?.dispose();
    if (child.exitCode===null) {
      child.kill('SIGTERM');
      await Promise.race([new Promise(resolve=>child.once('exit',resolve)),delay(3000)]);
      if(child.exitCode===null) child.kill('SIGKILL');
    }
    rmSync(dir,{recursive:true,force:true});
  }
});
