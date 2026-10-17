import test from 'node:test';
import assert from 'node:assert/strict';
import { multipart } from '../client/transport.mjs';
test('parser multipart aceita fronteiras e caracteres UTF-8 partidos entre chunks',async () => {
  const messages=[{}, {payload:{data:{orderEvents:{status:'Expedição'}}}}];
  const wire=messages.map(m=>`--graphql\r\ncontent-type: application/json\r\n\r\n${JSON.stringify(m)}\r\n`).join('')+'--graphql--\r\n';
  const bytes=new TextEncoder().encode(wire);
  async function* chunks() {for (const byte of bytes) yield Uint8Array.of(byte);}
  const result=[]; for await(const part of multipart(chunks(),'graphql')) result.push(part);
  assert.deepEqual(result,messages);
});
test('parser interrompido falha para acionar reconexão',async () => {
  async function* chunks() {yield new TextEncoder().encode('--graphql\r\ncontent-type: application/json\r\n\r\n{}');}
  await assert.rejects(async()=> {for await (const _ of multipart(chunks(),'graphql')) {}},/interrompido/);
});
