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
//
// Protótipo do que um plugin nativo do Router faria (não existe por configuração):
// 3. Retenção (retain): guarda o último frame de cada upstream deduplicado e o
//    entrega na hora a quem entra nele. Só é correto porque todo frame traz o estado
//    inteiro e uma versão: um frame retido mais velho que um em trânsito é ignorado
//    pelo cliente. Some junto com o upstream quando o último assinante sai.
// 4. Identidade na chave (identityHeaders): os headers listados entram na chave de
//    deduplicação e de retenção, como o Router faz com headers na deduplicação.
//    Clientes com identidades diferentes não compartilham upstream nem frame retido.
//
// Várias instâncias: cada edge deduplica e retém só os seus upstreams (como cada
// instância do Router). `cache` pode ser compartilhado entre instâncias, como o Redis
// do response cache.
export async function startEdge({
  upstream, port = 0, host = '127.0.0.1', heartbeatMs = 5000, cacheQueries = true,
  retain = true, identityHeaders = [], cache = new Map(), name = 'edge'
}) {
  const streams = new Map();
  const metrics = { cacheHits: 0, cacheMisses: 0, upstreamSubscriptions: 0, subscribers: 0, deduplicatedJoins: 0, retainedReplays: 0 };
  const ws = createClient({ url: upstream.replace(/^http/, 'ws'), webSocketImpl: WebSocket, lazy: true, retryAttempts: 5 });

  // Upstreams abertos por operação: com deduplicação, um por (operação, variáveis).
  const currentMetrics = () => {
    const upstreamByOperation = {};
    for (const stream of streams.values()) upstreamByOperation[stream.operation] = (upstreamByOperation[stream.operation] ?? 0) + 1;
    const retainedEntries = [...streams.values()].filter(stream => stream.last).length;
    return { name, ...metrics, cacheEntries: cache.size, upstreamByOperation, retainedEntries };
  };
  const part = json => `\r\ncontent-type: application/json\r\n\r\n${JSON.stringify(json)}\r\n--graphql`;

  function join(key, body, res) {
    let stream = streams.get(key);
    if (stream) metrics.deduplicatedJoins++;
    else {
      stream = { subscribers: new Set(), operation: body.operationName ?? 'anônima' };
      streams.set(key, stream); metrics.upstreamSubscriptions++;
      const close = (last) => {
        if (streams.get(key) !== stream) return;
        streams.delete(key); metrics.upstreamSubscriptions--;
        for (const subscriber of stream.subscribers) { if (last) subscriber.write(part(last)); subscriber.end('--\r\n'); }
      };
      stream.dispose = ws.subscribe({ query: body.query, variables: body.variables, operationName: body.operationName }, {
        next: result => {
          if (retain && !result.errors?.length) stream.last = result;
          for (const subscriber of stream.subscribers) subscriber.write(part({ payload: result }));
        },
        error: error => close({ payload: null, errors: [{ message: error?.message ?? 'Upstream encerrado' }] }),
        // Fim normal (ex.: placar no FULL_TIME): fecha o multipart sem erro, como o Router.
        complete: () => close(null)
      });
    }
    res.writeHead(200, { 'content-type': 'multipart/mixed;boundary="graphql";subscriptionSpec="1.0"', 'cache-control': 'no-store' });
    res.write('\r\n--graphql');
    // Quem entra num upstream já aberto recebe o último estado na hora, em vez de
    // esperar o próximo frame (commit ou periódico).
    // Marcado como o plugin do Router marca: extensions.retainedFrame.
    if (stream.last) { res.write(part({ payload: { ...stream.last, extensions: { ...stream.last.extensions, retainedFrame: true } } })); metrics.retainedReplays++; }
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
      res.end(JSON.stringify(currentMetrics())); return;
    }
    if (req.method !== 'POST' || req.url !== '/graphql') { res.writeHead(404); res.end(); return; }
    try {
      let raw = '';
      for await (const chunk of req) raw += chunk;
      const body = JSON.parse(raw);
      const ast = getOperationAST(parse(body.query), body.operationName);
      const operation = ast?.operation;
      body.operationName ??= ast?.name?.value;
      const identity = identityHeaders.map(header => req.headers[header] ?? null);
      const key = JSON.stringify([body.query, body.variables ?? {}, body.operationName ?? null, identity]);
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
    url, get metrics() { return currentMetrics(); }, cache,
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
    port: Number(process.env.EDGE_PORT ?? 4010), host: process.env.EDGE_HOST ?? '0.0.0.0',
    retain: process.env.RETAIN !== '0', identityHeaders: (process.env.IDENTITY_HEADERS ?? '').split(',').filter(Boolean)
  });
  console.log(`edge (dedup + retenção + cache de queries) ${edge.url.replace('0.0.0.0', 'localhost')} -> ${process.env.UPSTREAM_URL ?? 'http://localhost:4000/graphql'}`);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => edge.close().then(() => process.exit(0)));
}
