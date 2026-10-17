import { Synchronizer, runSync } from './sync.mjs';
import { queryApi, transports } from './transport.mjs';

// Padrão: edge local (dedup + cache, como o Router). Para o Apollo Router use
// GRAPHQL_URL=http://localhost:4100/graphql; para o Hive direto, :4000 com TRANSPORT=ws.
const url = process.env.GRAPHQL_URL ?? 'http://localhost:4010/graphql';
const transport = transports[process.env.TRANSPORT ?? 'multipart'];
const scopeId = process.env.SCOPE_ID ?? 'loja-42';
const enrich = process.env.ENRICH === '1';
const sync = new Synchronizer({ scopeId, api: queryApi(url, scopeId, { enrich }) });
const controller = new AbortController();
process.once('SIGINT', () => controller.abort());

let last = '';
await runSync({
  sync, signal: controller.signal,
  connect: async function* (signal) {
    for await (const frame of transport(url, scopeId, { signal, enrich })) {
      if (frame.kind === 'CHANGE') console.log(`  ao vivo: cursor ${frame.cursor}, ${Date.now() - frame.change.committedAt} ms após o commit`);
      yield frame;
    }
  },
  onUpdate: view => {
    const line = JSON.stringify(view.orders);
    const label = view.synced ? 'sincronizado' : 'provisório (base possivelmente antiga)';
    if (line + label !== last) console.log(`[cursor ${view.cursor} · ${label}] ${line}`);
    last = line + label;
  },
  onError: (error, attempt) => console.error(`Reconectando (tentativa ${attempt + 1}) a partir do cursor ${sync.cursor}: ${error.message}`)
});
