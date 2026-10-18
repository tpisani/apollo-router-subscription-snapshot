import { defineConfig } from '@graphql-hive/gateway';

// O gateway fala graphql-ws com os subgraphs que têm subscription (placar e
// estatísticas) e expõe aos clientes graphql-ws e SSE no mesmo /graphql.
const ws = { options: { subscriptions: { kind: 'ws', location: '/graphql' } } };
export const gatewayConfig = defineConfig({
  supergraph: process.env.SUPERGRAPH_PATH ?? './gateway/supergraph.graphql',
  transportEntries: { scores: ws, stats: ws },
  // Erros de domínio (BAD_USER_INPUT, NOT_YET) precisam chegar ao cliente.
  maskedErrors: false
});
