import { createServer } from 'node:http';
import { parse, getOperationAST } from 'graphql';
import { createClient } from 'graphql-ws';
import WebSocket from 'ws';

// Edge that reproduces, deliberately in the worst case, the two Apollo Router
// behaviors this protocol has to tolerate. It is NOT the Router; it exists to test
// the protocol without a GraphOS license.
//
// 1. Subscription deduplication: subscriptions with the same operation and the same
//    variables share a single upstream. A late joiner only gets the following
//    frames; nothing is resent on join.
// 2. Query cache: EVERY successful query is cached forever, by
//    operation + variables, ignoring Cache-Control. More aggressive than the Router
//    (which honors the subgraph's max-age); if the protocol works here, it works there.
//
// Error responses are not cached, as in the Router.
// Subscriptions go out in the Router's multipart protocol (subscriptionSpec 1.0).
//
// Prototype of what a native Router plugin would do (not available through config):
// 3. Retention (retain): keeps the last frame of each deduplicated upstream and
//    delivers it right away to whoever joins it. It is only correct because every
//    frame carries the full state and a version: a retained frame older than one in
//    flight is ignored by the client. It goes away with the upstream when the last
//    subscriber leaves.
// 4. Identity in the key (identityHeaders): the listed headers become part of the
//    deduplication and retention key, as the Router does with headers in deduplication.
//    Clients with different identities share neither the upstream nor the retained frame.
//
// Several instances: each edge deduplicates and retains only its own upstreams (like
// each Router instance). `cache` can be shared across instances, like the response
// cache's Redis.
export async function startEdge({
  upstream, port = 0, host = '127.0.0.1', heartbeatMs = 5000, cacheQueries = true,
  retain = true, identityHeaders = [], cache = new Map(), name = 'edge'
}) {
  const streams = new Map();
  const metrics = { cacheHits: 0, cacheMisses: 0, upstreamSubscriptions: 0, subscribers: 0, deduplicatedJoins: 0, retainedReplays: 0 };
  const ws = createClient({ url: upstream.replace(/^http/, 'ws'), webSocketImpl: WebSocket, lazy: true, retryAttempts: 5 });

  // Open upstreams per operation: with deduplication, one per (operation, variables).
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
      stream = { subscribers: new Set(), operation: body.operationName ?? 'anonymous' };
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
        error: error => close({ payload: null, errors: [{ message: error?.message ?? 'Upstream ended' }] }),
        // Normal end (e.g. scoreboard at FULL_TIME): closes the multipart without error, like the Router.
        complete: () => close(null)
      });
    }
    res.writeHead(200, { 'content-type': 'multipart/mixed;boundary="graphql";subscriptionSpec="1.0"', 'cache-control': 'no-store' });
    res.write('\r\n--graphql');
    // A client joining an already open upstream gets the last state right away,
    // instead of waiting for the next frame (commit or periodic).
    // Marked the way the Router plugin marks it: extensions.snapshot.
    if (stream.last) { res.write(part({ payload: { ...stream.last, extensions: { ...stream.last.extensions, snapshot: true } } })); metrics.retainedReplays++; }
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
  console.log(`edge (dedup + retention + query cache)
 ${edge.url.replace('0.0.0.0', 'localhost')} -> ${process.env.UPSTREAM_URL ?? 'http://localhost:4000/graphql'}`);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => edge.close().then(() => process.exit(0)));
}
