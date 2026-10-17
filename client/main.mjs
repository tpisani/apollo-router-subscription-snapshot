import { setTimeout as delay } from 'node:timers/promises';
import { Synchronizer, runSession } from './sync.mjs';
import { apiFor, routerFrames } from './transport.mjs';

const url = process.env.GRAPHQL_URL ?? 'http://localhost:4000/graphql';
const scope = process.env.SCOPE_ID ?? 'loja-42';
const sync = new Synchronizer(null,scope);
let resume = false, stopping = false, controller;
process.on('SIGINT',() => { stopping = true; controller?.abort(); });
while (!stopping) {
  controller = new AbortController();
  // Watchdog também detecta conexões silenciosas sem checkpoints.
  let lastFrame = Date.now();
  const watchdog = setInterval(() => { if (Date.now()-lastFrame > 15000) controller.abort(); },1000);
  sync.api = apiFor(url,scope,controller.signal);
  let lastView = '';
  try {
    await runSession(sync,routerFrames(url,scope,controller.signal),{ resume, onUpdate: view => {
      lastFrame = Date.now(); resume = true;
      const serialized = JSON.stringify(view);
      if (serialized !== lastView) { console.log(serialized); lastView = serialized; }
    } });
  } catch (error) { if (!stopping) console.error(`Reconectando: ${error.message}`); }
  finally { clearInterval(watchdog); controller.abort(); }
  if (!stopping) await delay(1000);
}
