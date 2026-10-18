import { defineConfig } from '@graphql-hive/gateway';

// The gateway speaks graphql-ws with the subgraphs that have subscriptions
// (scoreboard and stats) and exposes graphql-ws and SSE to clients on the same /graphql.
const ws = { options: { subscriptions: { kind: 'ws', location: '/graphql' } } };
export const gatewayConfig = defineConfig({
  supergraph: process.env.SUPERGRAPH_PATH ?? './gateway/supergraph.graphql',
  transportEntries: { scores: ws, stats: ws },
  // Domain errors (BAD_USER_INPUT, NOT_YET) must reach the client.

  maskedErrors: false
});
