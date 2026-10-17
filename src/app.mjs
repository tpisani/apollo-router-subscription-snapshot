import { createServer } from 'node:http';
import { parse, validate, execute, subscribe, GraphQLError } from 'graphql';
import { buildSubgraphSchema } from '@apollo/subgraph';
import { WebSocketServer } from 'ws';
import { useServer } from 'graphql-ws/use/ws';
import { Store, SnapshotCache } from './store.mjs';
import { Hub, LiveStream } from './live.mjs';
import { ordersSDL, catalogSDL } from './schemas.mjs';

// TTLs que o subgraph declara ao Router via Cache-Control. Todos são seguros pelo
// contrato do campo; o valor só decide quanto replay um cliente pode precisar.
export const TTL = {
  sharedSnapshot: 60,           // minCursor "0": base compartilhada, pode estar antiga
  boundedSnapshot: 86_400,      // minCursor > 0: satisfaz o mínimo para sempre
  changes: 31_536_000,          // intervalo fechado: imutável
  catalog: 300                  // dado de referência, consistência eventual
};

function wrap(fn) {
  return (...args) => {
    try { return fn(...args); }
    catch (error) { throw new GraphQLError(error.message, { extensions: { code: error.code ?? 'INTERNAL_SERVER_ERROR' } }); }
  };
}
const hint = (context, seconds) => context?.cacheHints?.push(seconds);
const withDetails = order => ({ ...order, details: { id: order.id } });
const changeOut = change => ({ ...change, order: withDetails(change.order) });

function parseCursor(value) {
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n < 0) throw Object.assign(new Error('Cursor inválido'), { code: 'BAD_USER_INPUT' });
  return n;
}

export function createOrdersSchema({ store, snapshots, hub, metrics, queueLimit }) {
  return buildSubgraphSchema([{ typeDefs: parse(ordersSDL), resolvers: {
    Query: {
      ordersSnapshot: wrap((_, { scopeId, minCursor }, context) => {
        metrics.snapshotRequests++;
        const min = parseCursor(minCursor);
        const snapshot = snapshots.get(scopeId, min);
        hint(context, min === 0 ? TTL.sharedSnapshot : TTL.boundedSnapshot);
        return { ...snapshot, orders: snapshot.orders.map(withDetails) };
      }),
      orderChanges: wrap((_, { scopeId, after, through }, context) => {
        metrics.changeRequests++;
        const page = store.changes(scopeId, after, through);
        hint(context, TTL.changes);
        return { ...page, events: page.events.map(changeOut) };
      })
    },
    Mutation: {
      upsertOrder: wrap((_, { scopeId, id, status }) => changeOut(store.change(scopeId, id, status))),
      deleteOrder: wrap((_, { scopeId, id }) => changeOut(store.change(scopeId, id, null, true)))
    },
    Subscription: {
      orderEvents: {
        subscribe: wrap((_, { scopeId }) => {
          store.head(scopeId); // Valida o escopo antes de abrir o stream.
          metrics.liveStreamsOpened++; metrics.activeLiveStreams++;
          const stream = new LiveStream({ hub, scopeId, queueLimit,
            onClose: () => metrics.activeLiveStreams--, onConflate: () => metrics.conflations++ });
          return {
            async next() { const result = await stream.next(); if (!result.done) metrics.framesSent++; return result; },
            return: () => stream.return(),
            throw: error => stream.throw(error),
            [Symbol.asyncIterator]() { return this; }
          };
        }),
        resolve: frame => ({ ...frame, change: frame.change && changeOut(frame.change) })
      }
    }
  } }]);
}

export function createCatalogSchema(metrics) {
  return buildSubgraphSchema([{ typeDefs: parse(catalogSDL), resolvers: {
    Query: { catalogHealth: () => 'ok' },
    Order: {
      __resolveReference: ({ id }, context) => {
        metrics.catalogLookups++; hint(context, TTL.catalog);
        return { id, label: `Pedido ${id}` };
      }
    }
  } }]);
}

// Cache-Control por resposta = menor TTL entre os campos resolvidos. Sem dica,
// com erro ou em mutation: no-store. Funciona com variáveis, aliases e fragmentos.
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
      if (!result.errors?.length && hints.length && Math.min(...hints) > 0) {
        const maxAge = Math.min(...hints);
        cacheControl = `public, max-age=${maxAge}${maxAge >= TTL.changes ? ', immutable' : ''}`;
      }
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

export async function startSubgraphs({
  dbPath = ':memory:', ordersPort = 4001, catalogPort = 4002, host = '0.0.0.0',
  checkpointMs = 250, queueLimit = 1000, snapshotTtlMs = 120_000
} = {}) {
  const metrics = {
    commits: 0, snapshotRequests: 0, snapshotDbReads: 0, changeRequests: 0, catalogLookups: 0,
    liveStreamsOpened: 0, activeLiveStreams: 0, framesSent: 0, conflations: 0
  };
  let hub;
  const store = new Store(dbPath, { onCommit: change => { metrics.commits++; hub.publish(change); } });
  hub = new Hub({ head: scope => store.head(scope), checkpointMs });
  const snapshots = new SnapshotCache(store, snapshotTtlMs);
  const ordersSchema = createOrdersSchema({ store, snapshots, hub, metrics, queueLimit });
  const currentMetrics = () => ({ ...metrics, snapshotDbReads: snapshots.reads, hubListeners: hub.size });
  const orders = createServer(graphqlHandler(ordersSchema, (req, res) => {
    if (req.method !== 'GET' || req.url !== '/metrics') return false;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify(currentMetrics())); return true;
  }));
  const catalog = createServer(graphqlHandler(createCatalogSchema(metrics)));
  const wsServer = new WebSocketServer({ server: orders, path: '/graphql' });
  const ws = useServer({ schema: ordersSchema, execute, subscribe }, wsServer);
  const ports = { orders: await listen(orders, ordersPort, host), catalog: await listen(catalog, catalogPort, host) };
  return {
    store, hub, snapshots, ports, get metrics() { return currentMetrics(); },
    async close() {
      await ws.dispose();
      await Promise.all([orders, catalog].map(server => new Promise(resolve => { server.closeAllConnections?.(); server.close(resolve); })));
      store.close();
    }
  };
}
