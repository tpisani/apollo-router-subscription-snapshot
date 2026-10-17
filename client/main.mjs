import { FeedState, runFeed } from './feed-client.mjs';
import { feedQuery, wsTransport } from './transport.mjs';

const url = process.env.GRAPHQL_URL ?? 'http://localhost:4000/graphql';
const scopeId = process.env.SCOPE_ID ?? 'loja-42';
const state = new FeedState(scopeId);
const transport = wsTransport(url, { query: feedQuery({ enrich: process.env.ENRICH === '1' }) });
const controller = new AbortController();
process.once('SIGINT', () => { controller.abort(); transport.dispose(); });

await runFeed({
  state, subscribe: transport.subscribe, signal: controller.signal,
  onFrame: frame => {
    const lag = frame.changes.map(change => Date.now() - change.committedAt);
    const info = frame.kind === 'SNAPSHOT' ? 'snapshot' : `${frame.changes.length} mudança(s), latência ${lag.join('/')} ms`;
    console.log(`[${frame.kind} → cursor ${state.cursor}] ${info}`);
    console.log(JSON.stringify(state.view().orders));
  },
  onError: (error, attempt) => console.error(`Reconectando (tentativa ${attempt + 1}) a partir do cursor ${state.cursor}: ${error.message}`)
});
