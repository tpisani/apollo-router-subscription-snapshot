import test from 'node:test';
import { startStack } from '../emulator/stack.mjs';
import { startEdge } from '../emulator/edge.mjs';
import { verifyRouterScale } from '../scripts/verify-router-scale.mjs';

// The same script that runs against the Routers with the plugin (npm run test:router-scale),
// here against the emulator: 3 edges with retention behind the balancer and 1 without retention.
// Makes sure the script itself is right before running it with the license.
test('multi-instance script with retention (emulator)', { timeout: 120_000 }, async () => {
  const stack = await startStack({
    edgeCount: 3, edge: { identityHeaders: ['authorization'] },
    scoreboard: { periodicMs: 2000 }, stats: { periodicMs: 2000, throttleMs: 200 }, liveLeadMs: 2000
  });
  const plain = await startEdge({ upstream: stack.url, retain: false, identityHeaders: ['authorization'], name: 'no-retention' });
  try {
    const result = await verifyRouterScale({
      lbUrl: stack.balancer.url, routerUrls: stack.edges.map(e => e.url), plainUrl: plain.url,
      metrics: async () => stack.subgraphs.metrics, kill: () => stack.killEdge(1), lead: 2000,
      // As with the real Router on the free plan: only subscriptions go through the instances.

      direct: { scores: `http://127.0.0.1:${stack.subgraphs.ports.scores}/graphql`, stats: `http://127.0.0.1:${stack.subgraphs.ports.stats}/graphql` }
    });
    console.log(JSON.stringify(result));
  } finally {
    await plain.close();
    await stack.stop();
  }
});
