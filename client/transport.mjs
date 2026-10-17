import { createClient } from 'graphql-ws';
import WebSocket from 'ws';

// enrich=true pede details.label (subgraph catalog). Sem deduplicação, o gateway
// resolve isso por assinante e por frame; ver docs/design.md.
const stateFields = enrich => `id version status${enrich ? ' details { label }' : ''}`;
const changeFields = enrich => `scopeId cursor operation committedAt order { ${stateFields(enrich)} }`;

// Mesma operação e mesmas variáveis para todo cliente do escopo: deduplicável.
export const eventsQuery = ({ enrich = false } = {}) => `subscription OrderEvents($scopeId: ID!) {
  orderEvents(scopeId: $scopeId) { scopeId kind cursor change { ${changeFields(enrich)} } }
}`;

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

// Cada query só tem campos de um contrato cacheável, para o Cache-Control da
// resposta não ser reduzido por outro campo.
export function queryApi(url, scopeId, { enrich = false, signal } = {}) {
  return {
    snapshot: async minCursor => (await graphql(url, `query Snapshot($scopeId: ID!, $min: ID!) {
      ordersSnapshot(scopeId: $scopeId, minCursor: $min) { scopeId cursor orders { ${stateFields(enrich)} } } }`,
    { scopeId, min: minCursor }, signal)).ordersSnapshot,
    changes: async (after, through) => (await graphql(url, `query Changes($scopeId: ID!, $after: ID!, $through: ID!) {
      orderChanges(scopeId: $scopeId, after: $after, through: $through) { scopeId after through events { ${changeFields(enrich)} } } }`,
    { scopeId, after, through }, signal)).orderChanges
  };
}

// Parser incremental de multipart/mixed (protocolo de subscriptions HTTP do Apollo
// Router). Não assume que um chunk TCP corresponde a uma parte.
export async function* multipart(body, boundary) {
  const marker = `--${boundary}`;
  const decoder = new TextDecoder(); let pending = ''; let started = false;
  for await (const chunk of body) {
    pending += decoder.decode(chunk, { stream: true });
    if (pending.length > 4_000_000) throw new Error('Parte multipart excedeu 4 MB');
    if (!started) {
      const start = pending.indexOf(marker);
      if (start < 0) continue;
      pending = pending.slice(start + marker.length); started = true;
    }
    while (true) {
      if (pending.startsWith('--')) return;
      const end = pending.indexOf(marker);
      if (end < 0) break;
      const part = pending.slice(0, end); pending = pending.slice(end + marker.length);
      const split = part.indexOf('\r\n\r\n');
      const json = split >= 0 ? part.slice(split + 4).trim() : '';
      if (json) yield JSON.parse(json);
    }
  }
  throw new Error('Stream multipart interrompido');
}

const fail = errors => Object.assign(new Error(errors[0]?.message ?? 'Erro na subscription'), { code: errors[0]?.extensions?.code });

export async function* multipartEvents(url, scopeId, { signal, enrich = false } = {}) {
  const response = await fetch(url, { method: 'POST', signal,
    headers: { 'content-type': 'application/json', accept: 'multipart/mixed;subscriptionSpec="1.0", application/json' },
    body: JSON.stringify({ query: eventsQuery({ enrich }), variables: { scopeId } }) });
  const type = response.headers.get('content-type') ?? '';
  const boundary = /boundary\s*=\s*(?:"([^"]+)"|([^;\s]+))/i.exec(type);
  if (!response.ok || !boundary) throw new Error(`Subscription HTTP ${response.status}: ${await response.text()}`);
  for await (const message of multipart(response.body, boundary[1] ?? boundary[2])) {
    if (message.errors?.length) throw fail(message.errors);       // erro de transporte do Router
    const payload = message.payload;
    if (!payload) continue;                                       // {} = heartbeat
    if (payload.errors?.length) throw fail(payload.errors);
    if (payload.data?.orderEvents) yield payload.data.orderEvents;
  }
}

// graphql-ws (Hive Gateway e subgraph direto). Um client por conexão, encerrado no abort.
export async function* wsEvents(url, scopeId, { signal, enrich = false } = {}) {
  const client = createClient({ url: url.replace(/^http/, 'ws'), webSocketImpl: WebSocket, retryAttempts: 0, lazy: true });
  const iterator = client.iterate({ query: eventsQuery({ enrich }), variables: { scopeId } });
  const abort = () => client.terminate();
  signal?.addEventListener('abort', abort, { once: true });
  try {
    for await (const result of iterator) {
      if (result.errors?.length) throw fail(result.errors);
      yield result.data.orderEvents;
    }
    if (signal?.aborted) throw new Error('Abortado');
  } finally {
    signal?.removeEventListener('abort', abort);
    await iterator.return?.(); await client.dispose();
  }
}

export const transports = { multipart: multipartEvents, ws: wsEvents };
