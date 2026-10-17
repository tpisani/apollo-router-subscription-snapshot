import { createServer } from 'node:http';
import { parse, validate, execute, subscribe, GraphQLError } from 'graphql';
import { buildSubgraphSchema } from '@apollo/subgraph';
import { WebSocketServer } from 'ws';
import { useServer } from 'graphql-ws/use/ws';
import { Store } from './store.mjs';
import { Bus, Feed } from './feed.mjs';
import { ordersSDL, catalogSDL } from './schemas.mjs';

function wrap(fn) {
  return (...args) => {
    try { return fn(...args); }
    catch (error) { throw new GraphQLError(error.message, { extensions: { code: error.code ?? 'INTERNAL_SERVER_ERROR' } }); }
  };
}

const withDetails = order => ({ ...order, details: { id: order.id } });
const frameOut = frame => ({
  ...frame,
  orders: frame.orders?.map(withDetails) ?? null,
  changes: frame.changes.map(change => ({ ...change, order: withDetails(change.order) }))
});

export function createOrdersSchema({ store, bus, metrics, maxReplay, queueLimit }) {
  return buildSubgraphSchema([{ typeDefs: parse(ordersSDL), resolvers: {
    Query: {
      ordersSnapshot: wrap((_, { scopeId }) => {
        const snapshot = store.snapshot(scopeId);
        return { ...snapshot, orders: snapshot.orders.map(withDetails) };
      })
    },
    Mutation: {
      upsertOrder: wrap((_, { scopeId, id, status }) => {
        const change = store.change(scopeId, id, status); return { ...change, order: withDetails(change.order) };
      }),
      deleteOrder: wrap((_, { scopeId, id }) => {
        const change = store.change(scopeId, id, null, true); return { ...change, order: withDetails(change.order) };
      })
    },
    Subscription: {
      orderFeed: {
        subscribe: wrap((_, { scopeId, after }) => {
          metrics.feedsOpened++; metrics.activeFeeds++;
          if (after == null) metrics.snapshotStarts++; else metrics.resumeStarts++;
          const feed = new Feed({ store, bus, scopeId, after, maxReplay, queueLimit, onClose: () => metrics.activeFeeds-- });
          return {
            async next() {
              const result = await feed.next();
              if (!result.done) metrics.framesSent++;
              return result;
            },
            return: () => feed.return(),
            throw: error => feed.throw(error),
            [Symbol.asyncIterator]() { return this; }
          };
        }),
        resolve: frameOut
      }
    }
  } }]);
}

export function createCatalogSchema() {
  return buildSubgraphSchema([{ typeDefs: parse(catalogSDL), resolvers: {
    Query: { catalogHealth: () => 'ok' },
    Order: { __resolveReference: ({ id }) => ({ id, label: `Pedido ${id}` }) }
  } }]);
}

function graphqlHandler(schema, extra = () => false) {
  return async (req, res) => {
    if (req.method === 'GET' && req.url === '/health') { res.end('ok'); return; }
    if (extra(req, res)) return;
    if (req.method !== 'POST' || req.url !== '/graphql') { res.writeHead(404); res.end(); return; }
    res.setHeader('cache-control', 'no-store');
    try {
      let raw = '';
      for await (const chunk of req) {
        raw += chunk;
        if (raw.length > 1_000_000) throw new Error('Request too large');
      }
      const body = JSON.parse(raw);
      const document = parse(body.query);
      const errors = validate(schema, document);
      const result = errors.length ? { errors }
        : await execute({ schema, document, variableValues: body.variables, operationName: body.operationName });
      res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(result));
    } catch (error) {
      res.writeHead(400, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ errors: [{ message: error.message }] }));
    }
  };
}

const listen = (server, port, host) => new Promise((resolve, reject) => {
  server.once('error', reject); server.listen(port, host, () => resolve(server.address().port));
});

export async function startSubgraphs({
  dbPath = ':memory:', ordersPort = 4001, catalogPort = 4002, host = '0.0.0.0',
  maxReplay = 1000, queueLimit = 1000
} = {}) {
  const bus = new Bus();
  const metrics = { commits: 0, feedsOpened: 0, activeFeeds: 0, snapshotStarts: 0, resumeStarts: 0, framesSent: 0 };
  const store = new Store(dbPath, { onCommit: change => { metrics.commits++; bus.publish(change); } });
  const ordersSchema = createOrdersSchema({ store, bus, metrics, maxReplay, queueLimit });
  const orders = createServer(graphqlHandler(ordersSchema, (req, res) => {
    if (req.method !== 'GET' || req.url !== '/metrics') return false;
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ ...metrics, busListeners: bus.size })); return true;
  }));
  const catalog = createServer(graphqlHandler(createCatalogSchema()));
  const wsServer = new WebSocketServer({ server: orders, path: '/graphql' });
  const ws = useServer({ schema: ordersSchema, execute, subscribe }, wsServer);
  const ports = { orders: await listen(orders, ordersPort, host), catalog: await listen(catalog, catalogPort, host) };
  return {
    store, bus, metrics, ports,
    async close() {
      await ws.dispose();
      await Promise.all([orders, catalog].map(server => new Promise(resolve => { server.closeAllConnections?.(); server.close(resolve); })));
      store.close();
    }
  };
}
