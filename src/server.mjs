import { createServer } from 'node:http';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { parse, validate, execute, subscribe, getOperationAST, GraphQLError } from 'graphql';
import { buildSubgraphSchema } from '@apollo/subgraph';
import { WebSocketServer } from 'ws';
import { useServer } from 'graphql-ws/use/ws';
import { setTimeout as delay } from 'node:timers/promises';
import { Store, SnapshotCache } from './store.mjs';
import { ordersSDL, catalogSDL } from './schemas.mjs';

const dbPath = process.env.DB_PATH ?? './data/orders.sqlite';
mkdirSync(dirname(dbPath),{ recursive: true });
const store = new Store(dbPath);
const snapshots = new SnapshotCache(store,Number(process.env.SNAPSHOT_TTL_MS ?? 120000));
const metrics = { activeSubscriptions: 0, subscriptionsStarted: 0, snapshotReads: 0, headReads: 0, historyReads: 0 };
const catalogCache = new Map();
function wrap(fn) {
  return (...args) => {
    try { return fn(...args); }
    catch (error) { throw new GraphQLError(error.message,{ extensions: { code: error.code ?? 'INTERNAL_SERVER_ERROR' } }); }
  };
}
async function* frames(scopeId) {
  metrics.activeSubscriptions++; metrics.subscriptionsStarted++;
  let cursor = store.head(scopeId);
  console.log(JSON.stringify({ event: 'subscription_open', scopeId, ...metrics }));
  try {
    while (true) {
      // Periódico, não um "hello" exclusivo da abertura: late joiners também recebem.
      await delay(500);
      const head = store.head(scopeId);
      try {
        while (cursor < head) {
          const page = store.changes(scopeId,String(cursor),String(head));
          for (const change of page.events) {
            cursor = Number(change.cursor);
            yield { orderEvents: { scopeId, kind: 'CHANGE', cursor: change.cursor, change } };
          }
        }
      } catch (error) {
        if (error.code !== 'CURSOR_EXPIRED') throw error;
        // O cliente fará fresh snapshot ao descobrir que seu histórico expirou.
        cursor = head;
      }
      yield { orderEvents: { scopeId, kind: 'CHECKPOINT', cursor: String(head), change: null } };
    }
  } finally { metrics.activeSubscriptions--; }
}
const schema = buildSubgraphSchema([{ typeDefs: parse(ordersSDL), resolvers: {
  Query: {
    ordersSnapshot: wrap((_,{ scopeId, fresh }) => { metrics.snapshotReads++; return snapshots.get(scopeId,fresh); }),
    orderHead: wrap((_,{ scopeId }) => { metrics.headReads++; return String(store.head(scopeId)); }),
    orderChanges: wrap((_,{ scopeId, after, through, first }) => {
      metrics.historyReads++; return store.changes(scopeId,after,through,first);
    }),
    // Consulta federada ilustrativa separada da projeção sincronizada.
    order: (_, { id }) => store.snapshot('loja-42').orders.find(order => order.id === id) ?? null
  },
  Mutation: {
    upsertOrder: wrap((_,{ scopeId,id,status }) => store.change(scopeId,id,status)),
    deleteOrder: wrap((_,{ scopeId,id }) => store.change(scopeId,id,null,true))
  },
  Subscription: { orderEvents: { subscribe: (_, { scopeId }) => frames(scopeId) } },
  Order: { __resolveReference: ({ id }) => store.snapshot('loja-42').orders.find(order => order.id === id) }
} }]);
const catalogSchema = buildSubgraphSchema([{ typeDefs: parse(catalogSDL), resolvers: {
  Query: { catalogHealth: () => 'ok' },
  Order: {
    __resolveReference: ({ id }) => ({ id }),
    label: ({ id }) => {
      const cached = catalogCache.get(id);
      if (cached?.expires > Date.now()) return cached.label;
      const label = `Pedido ${id}`; // Simula uma leitura de API externa com cache de 5 min.
      catalogCache.set(id,{ label, expires: Date.now()+300000 });
      return label;
    }
  }
} }]);

function httpGraphQL(schema, port, withMetrics = false) {
  const server = createServer(async (req,res) => {
    if (req.method === 'GET' && req.url === '/health') { res.end('ok'); return; }
    if (withMetrics && req.method === 'GET' && req.url === '/metrics') {
      res.setHeader('content-type','application/json'); res.end(JSON.stringify(metrics)); return;
    }
    if (req.method !== 'POST' || req.url !== '/graphql') { res.writeHead(404); res.end(); return; }
    try {
      let raw = '';
      for await (const chunk of req) {
        raw += chunk;
        if (raw.length > 1_000_000) throw new Error('Request too large');
      }
      const body = JSON.parse(raw);
      const document = parse(body.query);
      const operation = getOperationAST(document,body.operationName);
      const fields = operation?.selectionSet.selections ?? [];
      const snapshotOnly = operation?.operation === 'query' && fields.length > 0 &&
        fields.every(field => field.kind === 'Field' && field.name.value === 'ordersSnapshot' &&
          !field.arguments?.some(arg => arg.name.value === 'fresh' &&
            (arg.value.kind !== 'BooleanValue' || arg.value.value === true)));
      // Controle por resposta do subgraph; head/histórico/fresh nunca entram no response cache.
      res.setHeader('cache-control',snapshotOnly ? 'public, max-age=60' : 'no-store');
      const errors = validate(schema,document);
      const result = errors.length ? {errors} : await execute({ schema, document, variableValues: body.variables, operationName: body.operationName });
      if (result.errors?.length) res.setHeader('cache-control','no-store');
      res.setHeader('content-type','application/json'); res.end(JSON.stringify(result));
    } catch (error) {
      res.writeHead(400,{ 'content-type': 'application/json', 'cache-control': 'no-store' });
      res.end(JSON.stringify({ errors: [{ message: error.message }] }));
    }
  });
  server.listen(port,'0.0.0.0',() => console.log(`GraphQL http://localhost:${port}/graphql`));
  return server;
}
const ordersServer = httpGraphQL(schema,4001,true);
const catalogServer = httpGraphQL(catalogSchema,4002);
const wsServer = new WebSocketServer({ server: ordersServer, path: '/graphql' });
const wsCleanup = useServer({ schema, execute, subscribe },wsServer);
process.on('SIGTERM',async () => {
  await wsCleanup.dispose(); ordersServer.close(); catalogServer.close(); store.close();
});
