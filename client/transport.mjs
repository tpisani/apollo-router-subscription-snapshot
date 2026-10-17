import { createClient } from 'graphql-ws';
import WebSocket from 'ws';

// enrich=true pede details.label ao subgraph catalog em cada frame: o gateway faz
// uma busca de entidade por assinante e por frame, o que domina a latência
// (ver docs/design.md). O caminho quente fica só com campos do próprio evento.
export function feedQuery({ enrich = false } = {}) {
  const state = `id version status${enrich ? ' details { label }' : ''}`;
  return `subscription Feed($scopeId: ID!, $after: ID) {
  orderFeed(scopeId: $scopeId, after: $after) {
    scopeId kind cursor
    orders { ${state} }
    changes { scopeId cursor operation committedAt order { ${state} } }
  }
}`;
}

export async function graphql(url, query, variables, signal) {
  const response = await fetch(url, { method: 'POST', signal,
    headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query, variables }) });
  const result = await response.json();
  if (!response.ok || result.errors?.length) {
    const error = new Error(result.errors?.[0]?.message ?? `HTTP ${response.status}`);
    error.code = result.errors?.[0]?.extensions?.code; throw error;
  }
  return result.data;
}

export const toWs = url => url.replace(/^http/, 'ws');

// Conexão graphql-ws com detecção de conexão morta: ping a cada keepAlive ms e,
// sem pong em pongTimeout ms, fecha o socket (o feed então reconecta).
export function wsTransport(url, { keepAlive = 5_000, pongTimeout = 3_000, query = feedQuery() } = {}) {
  let pongTimer;
  const client = createClient({
    url: toWs(url), webSocketImpl: WebSocket, keepAlive, retryAttempts: 0, lazy: true,
    on: {
      ping: (received) => { if (!received) pongTimer = setTimeout(() => client.terminate(), pongTimeout); },
      pong: (received) => { if (received) clearTimeout(pongTimer); },
      closed: () => clearTimeout(pongTimer)
    }
  });
  return {
    client,
    async *subscribe(variables) {
      for await (const result of client.iterate({ query, variables })) {
        if (result.errors?.length) {
          const error = new Error(result.errors[0].message); error.code = result.errors[0].extensions?.code; throw error;
        }
        yield result.data.orderFeed;
      }
    },
    dispose: () => client.dispose()
  };
}
