import { createServer, request } from 'node:http';

// Balanceador HTTP round-robin com streaming, para simular várias instâncias do
// Router atrás de um LB. Sem afinidade de sessão: cada requisição (inclusive cada
// reconexão de subscription) pode cair em qualquer instância. Repassa o corpo e a
// resposta sem buffer (multipart precisa disso). Se a instância escolhida recusa a
// conexão, tenta a próxima, como um LB com health check.
export async function startBalancer({ targets, port = 0, host = '127.0.0.1' }) {
  let next = 0;
  const live = new Set(targets.map(t => new URL(t).origin));
  const metrics = { requests: 0, retries: 0, byTarget: Object.fromEntries([...live].map(t => [t, 0])) };

  function forward(req, res, body, attempt = 0) {
    const pool = [...live];
    if (!pool.length || attempt >= pool.length) { res.writeHead(502); res.end('sem instâncias'); return; }
    const target = new URL(pool[next++ % pool.length]);
    const upstream = request({ hostname: target.hostname, port: target.port, path: req.url, method: req.method, headers: req.headers }, response => {
      metrics.byTarget[target.origin]++;
      res.writeHead(response.statusCode, response.headers);
      response.pipe(res);
      response.on('error', () => res.destroy());
    });
    upstream.on('error', error => {
      if (res.headersSent) { res.destroy(); return; }       // caiu no meio do stream: o cliente reconecta
      if (error.code === 'ECONNREFUSED' || error.code === 'ECONNRESET') { metrics.retries++; forward(req, res, body, attempt + 1); return; }
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
