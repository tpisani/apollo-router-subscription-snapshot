import test from 'node:test';
import { startStack } from '../scripts/stack.mjs';
import { startEdge } from '../scripts/edge.mjs';
import { verifyRouterScale } from '../scripts/verify-router-scale.mjs';

// O mesmo roteiro que roda contra os Routers com o plugin (npm run test:router-scale),
// aqui contra o emulador: 3 edges com retenção atrás do balanceador e 1 sem retenção.
// Garante que o roteiro em si está certo antes de rodar com a licença.
test('roteiro de várias instâncias com retenção (emulador)', { timeout: 120_000 }, async () => {
  const stack = await startStack({
    edgeCount: 3, edge: { identityHeaders: ['authorization'] },
    scoreboard: { periodicMs: 2000 }, stats: { periodicMs: 2000, throttleMs: 200 }
  });
  const plain = await startEdge({ upstream: stack.url, retain: false, identityHeaders: ['authorization'], name: 'sem-retencao' });
  try {
    const result = await verifyRouterScale({
      lbUrl: stack.balancer.url, routerUrls: stack.edges.map(e => e.url), plainUrl: plain.url,
      metrics: async () => stack.subgraphs.metrics, kill: () => stack.killEdge(1),
      // Como no Router real com o plano gratuito: só as subscriptions passam pelas instâncias.
      direct: { scores: `http://127.0.0.1:${stack.subgraphs.ports.scores}/graphql`, stats: `http://127.0.0.1:${stack.subgraphs.ports.stats}/graphql` }
    });
    console.log(JSON.stringify(result));
  } finally {
    await plain.close();
    await stack.stop();
  }
});
