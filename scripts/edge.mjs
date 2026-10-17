import { createServer } from 'node:http';
import { parse, getOperationAST } from 'graphql';
import { createClient } from 'graphql-ws';
import WebSocket from 'ws';

// Borda que reproduz, de propósito no pior caso, os dois comportamentos do Apollo
// Router que este protocolo precisa tolerar. NÃO é o Router; serve para testar
// o protocolo sem licença GraphOS.
//
// 1. Deduplicação de subscriptions: assinaturas com a mesma operação e as mesmas
//    variáveis compartilham um único upstream. Quem entra depois recebe só os
//    frames seguintes; nada é reenviado na entrada.
// 2. Cache de queries: TODA query bem-sucedida é cacheada para sempre, pela
//    operação + variáveis, ignorando Cache-Control. Mais agressivo que o Router
//    (que respeita o max-age do subgraph); se o protocolo funciona aqui, funciona lá.
//
// Respostas com erro não são cacheadas, como no Router.
// Subscriptions saem no protocolo multipart do Router (subscriptionSpec 1.0).
export async function startEdge({ upstream, port = 0, host = '127.0.0.1', heartbeatMs = 5000, cacheQueries = true }) {
  const cache = new Map();
  const streams = new Map();
  const metrics = { cacheHits: 0, cacheMisses: 0, upstreamSubscriptions: 0, subscribers: 0, deduplicatedJoins: 0 };
  const ws = createClient({ url: upstream.replace(/^http/, 'ws'), webSocketImpl: WebSocket, lazy: true, retryAttempts: 5 });

  const part = json => `\r\ncontent-type: application/json\r\n\r\n${JSON.stringify(json)}\r\n--graphql`;

  function join(key, body, res) {
    let stream = streams.get(key);
    if (stream) metrics.deduplicatedJoins++;
    else {
      stream = { subscribers: new Set() };
      streams.set(key, stream); metrics.upstreamSubscriptions++;
      const close = (last) => {
        if (streams.get(key) !== stream) return;
        streams.delete(key); metrics.upstreamSubscriptions--;
        for (const subscriber of stream.subscribers) { if (last) subscriber.write(part(last)); subscriber.end('--\r\n'); }
      };
      stream.dispose = ws.subscribe({ query: body.query, variables: body.variables, operationName: body.operationName }, {
        next: result => { for (const subscriber of stream.subscribers) subscriber.write(part({ payload: result })); },
        error: error => close({ payload: null, errors: [{ message: error?.message ?? 'Upstream encerrado' }] }),
        complete: () => close({ payload: null, errors: [{ message: 'Upstream encerrado' }] })
      });
    }
    res.writeHead(200, { 'content-type': 'multipart/mixed;boundary="graphql";subscriptionSpec="1.0"', 'cache-control': 'no-store' });
    res.write('\r\n--graphql');
    stream.subscribers.add(res); metrics.subscribers++;
    const heartbeat = setInterval(() => res.write(part({})), heartbeatMs);
    res.on('close', () => {
      clearInterval(heartbeat);
      if (!stream.subscribers.delete(res)) return;
      metrics.subscribers--;
      if (!stream.subscribers.size && streams.get(key) === stream) {
        streams.delete(key); metrics.upstreamSubscriptions--; stream.dispose();
      }
    });
  }

  const server = createServer(async (req, res) => {
    if (req.method === 'GET' && req.url === '/metrics') {
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ ...metrics, cacheEntries: cache.size })); return;
    }
    if (req.method !== 'POST' || req.url !== '/graphql') { res.writeHead(404); res.end(); return; }
    try {
      let raw = '';
      for await (const chunk of req) raw += chunk;
      const body = JSON.parse(raw);
      const operation = getOperationAST(parse(body.query), body.operationName)?.operation;
      const key = JSON.stringify([body.query, body.variables ?? {}, body.operationName ?? null]);
      if (operation === 'subscription') return join(key, body, res);
      if (operation === 'query' && cacheQueries && cache.has(key)) {
        metrics.cacheHits++;
        res.writeHead(200, { 'content-type': 'application/json', 'x-edge-cache': 'HIT' }); res.end(cache.get(key)); return;
      }
      const response = await fetch(upstream, { method: 'POST', headers: { 'content-type': 'application/json' }, body: raw });
      const text = await response.text();
      if (operation === 'query' && cacheQueries) {
        metrics.cacheMisses++;
        if (response.ok && !JSON.parse(text).errors?.length) cache.set(key, text);
      }
      res.writeHead(response.status, { 'content-type': 'application/json', 'x-edge-cache': 'MISS' }); res.end(text);
    } catch (error) {
      res.writeHead(400, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ errors: [{ message: error.message }] }));
    }
  });
  await new Promise(resolve => server.listen(port, host, resolve));
  const url = `http://${host}:${server.address().port}/graphql`;
  return {
    url, metrics, cache,
    async close() {
      for (const stream of streams.values()) stream.dispose();
      server.closeAllConnections?.();
      await new Promise(resolve => server.close(resolve));
      await ws.dispose();
    }
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const edge = await startEdge({
    upstream: process.env.UPSTREAM_URL ?? 'http://localhost:4000/graphql',
    port: Number(process.env.EDGE_PORT ?? 4010), host: process.env.EDGE_HOST ?? '0.0.0.0'
  });
  console.log(`edge (dedup + cache de queries) ${edge.url.replace('0.0.0.0', 'localhost')} -> ${process.env.UPSTREAM_URL ?? 'http://localhost:4000/graphql'}`);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => edge.close().then(() => process.exit(0)));
}
