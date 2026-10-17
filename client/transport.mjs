import { createClient } from 'graphql-ws';
import WebSocket from 'ws';

const SCOREBOARD = 'status period home away clock { running seconds updatedAt } lastEvent { type side minute }';
const TEAM = 'possession touches shots shotsOnTarget passes corners fouls';
const STATS = `home { ${TEAM} } away { ${TEAM} } players { id touches passes shots }`;

// Uma subscription e uma query por canal. Todas as variáveis são só a partida,
// então todo cliente da mesma partida manda a mesma operação: deduplicável.
export const CHANNELS = {
  scoreboard: {
    field: 'scoreboardUpdates',
    subscription: `subscription Scoreboard($matchId: ID!) {
      scoreboardUpdates(matchId: $matchId) { matchId kind version committedAt state { ${SCOREBOARD} } } }`,
    snapshotField: 'scoreboard',
    snapshot: `query ScoreboardSnapshot($matchId: ID!, $min: Int!) {
      scoreboard(matchId: $matchId, minVersion: $min) { matchId version state { ${SCOREBOARD} } } }`
  },
  stats: {
    field: 'statsUpdates',
    subscription: `subscription Stats($matchId: ID!) {
      statsUpdates(matchId: $matchId) { matchId kind version committedAt state { ${STATS} } } }`,
    snapshotField: 'matchStats',
    snapshot: `query StatsSnapshot($matchId: ID!, $min: Int!) {
      matchStats(matchId: $matchId, minVersion: $min) { matchId version state { ${STATS} } } }`
  }
};

export async function graphql(url, query, variables, signal, headers = {}) {
  const response = await fetch(url, { method: 'POST', signal,
    headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify({ query, variables }) });
  const result = await response.json();
  if (!response.ok || result.errors?.length) {
    const error = new Error(result.errors?.[0]?.message ?? `HTTP ${response.status}`);
    error.code = result.errors?.[0]?.extensions?.code; throw error;
  }
  return result.data;
}

export function snapshotApi(url, channel, matchId, { signal, headers, onBytes = () => {} } = {}) {
  const { snapshot, snapshotField } = CHANNELS[channel];
  return {
    snapshot: async min => {
      const data = await graphql(url, snapshot, { matchId, min }, signal, headers);
      onBytes(JSON.stringify(data).length);
      return data[snapshotField];
    }
  };
}

// Parser incremental de multipart/mixed (protocolo de subscriptions HTTP do Apollo Router).
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
      if (json) yield { message: JSON.parse(json), bytes: json.length };
    }
  }
  throw new Error('Stream multipart interrompido');
}

const fail = errors => Object.assign(new Error(errors[0]?.message ?? 'Erro na subscription'), { code: errors[0]?.extensions?.code });

// onBytes recebe o tamanho de cada frame entregue, para medir banda por canal.
export async function* multipartFrames(url, channel, matchId, { signal, headers = {}, onBytes = () => {} } = {}) {
  const { subscription, field } = CHANNELS[channel];
  const response = await fetch(url, { method: 'POST', signal,
    headers: { 'content-type': 'application/json', accept: 'multipart/mixed;subscriptionSpec="1.0", application/json', ...headers },
    body: JSON.stringify({ query: subscription, variables: { matchId } }) });
  const type = response.headers.get('content-type') ?? '';
  const boundary = /boundary\s*=\s*(?:"([^"]+)"|([^;\s]+))/i.exec(type);
  if (!response.ok || !boundary) throw new Error(`Subscription HTTP ${response.status}: ${await response.text()}`);
  for await (const { message, bytes } of multipart(response.body, boundary[1] ?? boundary[2])) {
    if (message.errors?.length) throw fail(message.errors);       // erro de transporte do Router
    const payload = message.payload;
    if (!payload) continue;                                       // {} = heartbeat
    if (payload.errors?.length) throw fail(payload.errors);
    if (payload.data?.[field]) { onBytes(bytes); yield payload.data[field]; }
  }
}

export async function* wsFrames(url, channel, matchId, { signal, onBytes = () => {} } = {}) {
  const { subscription, field } = CHANNELS[channel];
  const client = createClient({ url: url.replace(/^http/, 'ws'), webSocketImpl: WebSocket, retryAttempts: 0, lazy: true });
  const iterator = client.iterate({ query: subscription, variables: { matchId } });
  const abort = () => client.terminate();
  signal?.addEventListener('abort', abort, { once: true });
  try {
    for await (const result of iterator) {
      if (result.errors?.length) throw fail(result.errors);
      onBytes(JSON.stringify(result).length);
      yield result.data[field];
    }
    if (signal?.aborted) throw new Error('Abortado');
  } finally {
    signal?.removeEventListener('abort', abort);
    await iterator.return?.(); await client.dispose();
  }
}

export const transports = { multipart: multipartFrames, ws: wsFrames };
