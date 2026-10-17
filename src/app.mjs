import { createServer } from 'node:http';
import { join } from 'node:path';
import { parse, validate, execute, subscribe, GraphQLError } from 'graphql';
import { buildSubgraphSchema } from '@apollo/subgraph';
import { WebSocketServer } from 'ws';
import { useServer } from 'graphql-ws/use/ws';
import { Store, SnapshotCache, SyncError } from './store.mjs';
import { ChannelHub, LiveStream } from './channels.mjs';
import { scoresSDL, statsSDL, catalogSDL } from './schemas.mjs';
import { MATCHES, initialScoreboard, initialStats, scoreboardOps, applyStatEvents } from './sports.mjs';

// TTLs que cada subgraph declara via Cache-Control. Todos são seguros pelo contrato
// ">= minVersion"; o valor só decide quão antiga pode vir a base provisória.
export const TTL = {
  scoreboardShared: 2, statsShared: 5,   // minVersion 0: chave compartilhada, render imediato
  bounded: 60,                           // minVersion > 0: chave nova a cada versão pedida
  catalog: 3600                          // times e elencos
};

function wrap(fn) {
  return (...args) => {
    try { return fn(...args); }
    catch (error) { throw new GraphQLError(error.message, { extensions: { code: error.code ?? 'INTERNAL_SERVER_ERROR' } }); }
  };
}
const hint = (context, seconds) => context?.cacheHints?.push(seconds);
const minVersionOf = value => {
  if (!Number.isSafeInteger(value) || value < 0) throw new SyncError('minVersion inválido', 'BAD_USER_INPUT');
  return value;
};
const statsOut = state => ({ ...state, players: Object.entries(state.players).map(([id, p]) => ({ id, ...p })) });

function channelMetrics() {
  return { commits: 0, snapshotRequests: 0, streamsOpened: 0, activeStreams: 0, framesEmitted: 0, framesSent: 0, merges: 0 };
}

function liveField(hub, metrics, resolve) {
  return {
    subscribe: wrap((_, { matchId }) => {
      if (typeof matchId !== 'string' || !matchId || matchId.length > 100) throw new SyncError('Partida inválida', 'BAD_USER_INPUT');
      metrics.streamsOpened++; metrics.activeStreams++;
      const stream = new LiveStream({ hub, key: matchId, onClose: () => metrics.activeStreams--, onMerge: () => metrics.merges++ });
      return {
        async next() { const result = await stream.next(); if (!result.done) metrics.framesSent++; return result; },
        return: () => stream.return(),
        throw: error => stream.throw(error),
        [Symbol.asyncIterator]() { return this; }
      };
    }),
    resolve
  };
}

export function createScoresSchema({ store, snapshots, hub, metrics }) {
  const mutate = (op) => wrap((_, args) => {
    const change = store.update(args.matchId, state => scoreboardOps[op](state, args, store.clock()));
    return change ? { matchId: args.matchId, version: change.version, state: change.state } : { matchId: args.matchId, ...store.read(args.matchId) };
  });
  return buildSubgraphSchema([{ typeDefs: parse(scoresSDL), resolvers: {
    Query: {
      scoreboard: wrap((_, { matchId, minVersion }, context) => {
        metrics.snapshotRequests++;
        const min = minVersionOf(minVersion);
        const doc = snapshots.get(matchId, min);
        hint(context, min === 0 ? TTL.scoreboardShared : TTL.bounded);
        return { matchId, version: doc.version, state: doc.state };
      })
    },
    Mutation: { startPeriod: mutate('startPeriod'), endPeriod: mutate('endPeriod'), scoreGoal: mutate('scoreGoal') },
    Subscription: { scoreboardUpdates: liveField(hub, metrics, frame => frame) }
  } }]);
}

export function createStatsSchema({ store, snapshots, hub, metrics }) {
  return buildSubgraphSchema([{ typeDefs: parse(statsSDL), resolvers: {
    Query: {
      matchStats: wrap((_, { matchId, minVersion }, context) => {
        metrics.snapshotRequests++;
        const min = minVersionOf(minVersion);
        const doc = snapshots.get(matchId, min);
        hint(context, min === 0 ? TTL.statsShared : TTL.bounded);
        return { matchId, version: doc.version, state: statsOut(doc.state) };
      })
    },
    Mutation: {
      recordStats: wrap((_, { matchId, events }) => {
        if (events.length > 1000) throw new SyncError('Lote grande demais', 'BAD_USER_INPUT');
        return (store.update(matchId, state => applyStatEvents(state, events, matchId)) ?? store.read(matchId)).version;
      })
    },
    Subscription: { statsUpdates: liveField(hub, metrics, frame => (frame.kind === 'STATE' ? { ...frame, state: statsOut(frame.state) } : frame)) }
  } }]);
}

export function createCatalogSchema() {
  const match = id => MATCHES[id] ?? null;
  return buildSubgraphSchema([{ typeDefs: parse(catalogSDL), resolvers: {
    Query: {
      match: (_, { id }, context) => { hint(context, TTL.catalog); return match(id); },
      matches: (_, __, context) => { hint(context, TTL.catalog); return Object.values(MATCHES); }
    },
    Match: { __resolveReference: ({ id }, context) => { hint(context, TTL.catalog); return match(id); } },
    Team: { __resolveReference: ({ id }, context) => { hint(context, TTL.catalog); return Object.values(MATCHES).flatMap(m => [m.home, m.away]).find(t => t.id === id); } }
  } }]);
}

// Cache-Control por resposta = menor TTL entre os campos resolvidos. Sem dica,
// com erro ou em mutation: no-store.
function graphqlHandler(schema, extra = () => false) {
  return async (req, res) => {
    if (req.method === 'GET' && req.url === '/health') { res.end('ok'); return; }
    if (extra(req, res)) return;
    if (req.method !== 'POST' || req.url !== '/graphql') { res.writeHead(404); res.end(); return; }
    let cacheControl = 'no-store';
    try {
      let raw = '';
      for await (const chunk of req) {
        raw += chunk;
        if (raw.length > 1_000_000) throw new Error('Request too large');
      }
      const body = JSON.parse(raw);
      const document = parse(body.query);
      const errors = validate(schema, document);
      const contextValue = { cacheHints: [] };
      const result = errors.length ? { errors }
        : await execute({ schema, document, contextValue, variableValues: body.variables, operationName: body.operationName });
      const hints = contextValue.cacheHints;
      if (!result.errors?.length && hints.length && Math.min(...hints) > 0) cacheControl = `public, max-age=${Math.min(...hints)}`;
      res.writeHead(200, { 'content-type': 'application/json', 'cache-control': cacheControl });
      res.end(JSON.stringify(result));
    } catch (error) {
      res.writeHead(400, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      res.end(JSON.stringify({ errors: [{ message: error.message }] }));
    }
  };
}

const listen = (server, port, host) => new Promise((resolve, reject) => {
  server.once('error', reject); server.listen(port, host, () => resolve(server.address().port));
});

// Os três subgraphs num processo (por conveniência local), cada um com seu banco.
export async function startSubgraphs({
  dataDir = null, ports = { scores: 4001, catalog: 4002, stats: 4003 }, host = '0.0.0.0',
  scoreboard = { checkpointMs: 1000 }, stats = { checkpointMs: 1000, coalesceMs: 500 }, snapshotTtlMs = 1000
} = {}) {
  const dbPath = name => (dataDir ? join(dataDir, `${name}.sqlite`) : ':memory:');
  const metrics = { scoreboard: channelMetrics(), stats: channelMetrics() };
  const hubs = {};
  const stores = {
    scoreboard: new Store(dbPath('scores'), { initial: initialScoreboard, onCommit: c => { metrics.scoreboard.commits++; hubs.scoreboard.publish(c); } }),
    stats: new Store(dbPath('stats'), { initial: initialStats, onCommit: c => { metrics.stats.commits++; hubs.stats.publish(c); } })
  };
  hubs.scoreboard = new ChannelHub({ store: stores.scoreboard, mode: 'state', ...scoreboard, onFrame: () => metrics.scoreboard.framesEmitted++ });
  hubs.stats = new ChannelHub({ store: stores.stats, mode: 'delta', ...stats, onFrame: () => metrics.stats.framesEmitted++ });
  const snapshots = { scoreboard: new SnapshotCache(stores.scoreboard, snapshotTtlMs), stats: new SnapshotCache(stores.stats, snapshotTtlMs) };

  const current = () => Object.fromEntries(Object.entries(metrics).map(([name, m]) =>
    [name, { ...m, snapshotDbReads: snapshots[name].reads, hubListeners: hubs[name].size }]));
  const withMetrics = (req, res) => {
    if (req.method !== 'GET' || req.url !== '/metrics') return false;
    res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(current())); return true;
  };
  const scoresSchema = createScoresSchema({ store: stores.scoreboard, snapshots: snapshots.scoreboard, hub: hubs.scoreboard, metrics: metrics.scoreboard });
  const statsSchema = createStatsSchema({ store: stores.stats, snapshots: snapshots.stats, hub: hubs.stats, metrics: metrics.stats });
  const servers = {
    scores: createServer(graphqlHandler(scoresSchema, withMetrics)),
    catalog: createServer(graphqlHandler(createCatalogSchema())),
    stats: createServer(graphqlHandler(statsSchema, withMetrics))
  };
  const ws = [
    useServer({ schema: scoresSchema, execute, subscribe }, new WebSocketServer({ server: servers.scores, path: '/graphql' })),
    useServer({ schema: statsSchema, execute, subscribe }, new WebSocketServer({ server: servers.stats, path: '/graphql' }))
  ];
  const bound = {};
  for (const name of Object.keys(servers)) bound[name] = await listen(servers[name], ports[name] ?? 0, host);
  return {
    stores, hubs, ports: bound, get metrics() { return current(); },
    async close() {
      await Promise.all(ws.map(w => w.dispose()));
      await Promise.all(Object.values(servers).map(server => new Promise(resolve => { server.closeAllConnections?.(); server.close(resolve); })));
      for (const name of Object.keys(hubs)) for (const key of [...hubs[name].keys.keys()]) {
        const entry = hubs[name].keys.get(key); clearInterval(entry.timer); clearTimeout(entry.flushTimer);
      }
      Object.values(stores).forEach(store => store.close());
    }
  };
}
