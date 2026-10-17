import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Store, SnapshotCache } from '../src/store.mjs';
import { Synchronizer, runSession } from '../client/sync.mjs';
const scope = 'loja-42';
function setup() {
  const store = new Store(); const cache = new SnapshotCache(store);
  const api = {
    snapshot: async fresh => cache.get(scope,fresh), head: async () => String(store.head(scope)),
    changes: async (a,h) => store.changes(scope,a,h,2)
  };
  return {store,cache,api,sync:new Synchronizer(api,scope)};
}
test('snapshot defasado + replay paginado reconstroem o estado exato, incluindo exclusões',async () => {
  const {store,cache,sync} = setup();
  try {
    store.change(scope,'1','PENDING'); const old = cache.get(scope);
    store.change(scope,'1','PAID'); store.change(scope,'2','PENDING');
    store.change(scope,'1','SHIPPED'); store.change(scope,'2',null,true);
    assert.deepEqual(cache.get(scope),old);
    sync.initialize(old); await sync.catchUp(store.head(scope));
    assert.deepEqual(sync.view(),{cursor:'5',orders:[{id:'1',version:3,status:'SHIPPED'}]});
  } finally {store.close();}
});
test('overlap, duplicatas, reordenação e checkpoint recuperam perdas sem regressão',async () => {
  const {store,sync} = setup();
  try {
    sync.initialize(store.snapshot(scope));
    const a=store.change(scope,'1','PENDING'), b=store.change(scope,'1','PAID');
    await sync.accept({scopeId:scope,kind:'CHANGE',cursor:b.cursor,change:b});
    await sync.accept({scopeId:scope,kind:'CHANGE',cursor:a.cursor,change:a});
    store.change(scope,'1','SHIPPED'); // Evento live perdido.
    await sync.accept({scopeId:scope,kind:'CHECKPOINT',cursor:'3'});
    assert.equal(sync.orders.get('1').status,'SHIPPED'); assert.equal(sync.cursor,3);
  } finally {store.close();}
});
test('CURSOR_EXPIRED ignora cache antigo e reconstrói por snapshot fresh',async () => {
  const {store,cache,sync} = setup();
  try {
    store.change(scope,'1','PENDING'); sync.initialize(cache.get(scope));
    store.change(scope,'1','PAID'); store.prune(scope,2); store.change(scope,'1','SHIPPED');
    assert.equal(cache.get(scope).cursor,'1');
    await sync.catchUp(3);
    assert.equal(sync.cursor,3); assert.equal(sync.orders.get('1').status,'SHIPPED');
  } finally {store.close();}
});
test('o limite fixo H não é movido por escritas entre páginas',async () => {
  const {store,api,sync} = setup();
  try {
    sync.initialize(store.snapshot(scope));
    for(let i=0;i<5;i++) store.change(scope,String(i),'PENDING');
    const original=api.changes; let calls=0;
    api.changes=async (a,h)=> {
      const page=await original(a,h);
      if(++calls===1) store.change(scope,'late','PAID');
      return page;
    };
    await sync.catchUp(5); assert.equal(sync.cursor,5); assert.equal(sync.orders.has('late'),false);
    await sync.accept({scopeId:scope,kind:'CHECKPOINT',cursor:'6'});
    assert.equal(sync.orders.get('late').status,'PAID');
  } finally {store.close();}
});
test('mudanças durante bootstrap e entrada tardia em stream já aberto são recuperadas',async () => {
  const {store,cache,api,sync} = setup();
  try {
    store.change(scope,'1','PENDING'); cache.get(scope);
    store.change(scope,'1','PAID');
    const original=api.snapshot;
    api.snapshot=async fresh=> { const result=await original(fresh); store.change(scope,'1','SHIPPED'); return result; };
    async function* existingStream() {
      yield {scopeId:scope,kind:'CHECKPOINT',cursor:'2'};
      yield {scopeId:scope,kind:'CHECKPOINT',cursor:'3'};
    }
    await assert.rejects(runSession(sync,existingStream()),/encerrada/);
    assert.equal(sync.cursor,3); assert.equal(sync.orders.get('1').status,'SHIPPED');
  } finally {store.close();}
});
test('retomada mantém estado local e recupera escritas feitas desconectado',async () => {
  const {store,sync} = setup();
  try {
    store.change(scope,'1','PENDING'); sync.initialize(store.snapshot(scope));
    store.change(scope,'1','PAID');
    async function* reconnect() {yield {scopeId:scope,kind:'CHECKPOINT',cursor:'2'};}
    await assert.rejects(runSession(sync,reconnect(),{resume:true}),/encerrada/);
    assert.equal(sync.orders.get('1').status,'PAID');
  } finally {store.close();}
});
test('escopos têm sequências e históricos independentes',() => {
  const {store} = setup();
  try {
    store.change(scope,'1','PAID'); store.change('outra-loja','1','SHIPPED');
    assert.equal(store.head(scope),1); assert.equal(store.head('outra-loja'),1);
    assert.equal(store.changes(scope,'0','1').events[0].order.status,'PAID');
  } finally {store.close();}
});
test('projeção, head e histórico são revertidos juntos em falha de escrita',() => {
  const {store} = setup();
  try {
    store.db.exec("CREATE TRIGGER fail_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'failure'); END;");
    assert.throws(()=>store.change(scope,'1','PAID'),/failure/);
    assert.deepEqual(store.snapshot(scope).orders,[]); assert.equal(store.head(scope),0);
  } finally {store.close();}
});
test('estado e histórico sobrevivem ao reinício do processo',() => {
  const dir=mkdtempSync(join(tmpdir(),'sync-example-')); let store;
  try {
    store=new Store(join(dir,'db.sqlite')); store.change(scope,'1','PAID'); store.close();
    store=new Store(join(dir,'db.sqlite'));
    assert.equal(store.snapshot(scope).cursor,'1');
    assert.equal(store.changes(scope,'0','1').events[0].order.status,'PAID');
  } finally {store?.close(); rmSync(dir,{recursive:true,force:true});}
});
test('cliente rejeita mistura de escopos e lacunas não recuperadas',() => {
  const {store,sync}=setup();
  try {
    assert.throws(()=>sync.initialize({scopeId:'outra',cursor:'0',orders:[]}),/escopo/);
    assert.throws(()=>sync.apply({scopeId:scope,cursor:'2'}),/Lacuna/);
  } finally {store.close();}
});
