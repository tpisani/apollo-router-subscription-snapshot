import { defineConfig } from '@graphql-hive/gateway';

// O gateway fala graphql-ws com o subgraph orders para subscriptions e expõe
// aos clientes graphql-ws, SSE e multipart no mesmo endpoint /graphql.
export const gatewayConfig = defineConfig({
  supergraph: process.env.SUPERGRAPH_PATH ?? './gateway/supergraph.graphql',
  transportEntries: {
    orders: { options: { subscriptions: { kind: 'ws', location: '/graphql' } } }
  },
  // Erros de domínio (BAD_USER_INPUT, SLOW_CONSUMER) precisam chegar ao cliente.
  maskedErrors: false
});
