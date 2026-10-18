import { createServer, request } from 'node:http';

// Round-robin HTTP balancer with streaming, to simulate several Router instances
// behind an LB. No session affinity: every request (including every subscription
// reconnect) may land on any instance. Forwards the body and the response without
// buffering (multipart needs that). If the chosen instance refuses the connection
// (or the name does not resolve, like a stopped container), it tries the next one,
// like an LB with a health check.
export async function startBalancer({ targets, port = 0, host = '127.0.0.1' }) {
  let next = 0;
  const live = new Set(targets.map(t => new URL(t).origin));
  const metrics = { requests: 0, retries: 0, byTarget: Object.fromEntries([...live].map(t => [t, 0])) };

  function forward(req, res, body, attempt = 0) {
    const pool = [...live];
    if (!pool.length || attempt >= pool.length) { res.writeHead(502); res.end('no instances'); return; }
    const target = new URL(pool[next++ % pool.length]);
    const upstream = request({ hostname: target.hostname, port: target.port, path: req.url, method: req.method, headers: req.headers }, response => {
      metrics.byTarget[target.origin]++;
      res.writeHead(response.statusCode, response.headers);
      response.pipe(res);
      response.on('error', () => res.destroy());
    });
    upstream.on('error', error => {
      if (res.headersSent) { res.destroy(); return; }       // dropped mid-stream: the client reconnects
      if (['ECONNREFUSED', 'ECONNRESET', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH'].includes(error.code)) { metrics.retries++; forward(req, res, body, attempt + 1); return; }
      res.writeHead(502); res.end(error.message);
    });
    res.on('close', () => upstream.destroy());
    upstream.end(body);
  }

  const server = createServer(async (req, res) => {
    if (req.url === '/lb-metrics') { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(metrics)); return; }
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    metrics.requests++;
    forward(req, res, Buffer.concat(chunks));
  });
  await new Promise(resolve => server.listen(port, host, resolve));
  return {
    url: `http://${host}:${server.address().port}/graphql`, metrics,
    remove(target) { live.delete(new URL(target).origin); },
    add(target) { live.add(new URL(target).origin); metrics.byTarget[new URL(target).origin] ??= 0; },
    async close() { server.closeAllConnections?.(); await new Promise(resolve => server.close(resolve)); }
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  // TARGETS=http://router-a:4100,http://router-b:4100 LB_PORT=4200 node emulator/balancer.mjs
  const targets = (process.env.TARGETS ?? '').split(',').filter(Boolean);
  if (!targets.length) throw new Error('TARGETS is empty');
  const lb = await startBalancer({ targets, port: Number(process.env.LB_PORT ?? 4200), host: process.env.LB_HOST ?? '0.0.0.0' });
  console.log(`balancer ${lb.url.replace('0.0.0.0', 'localhost')} -> ${targets.join(', ')} (metrics at /lb-metrics)`
);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => lb.close().then(() => process.exit(0)));
}
