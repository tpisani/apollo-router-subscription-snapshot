export const STATE = 'id version status';
export const CHANGE = `scopeId cursor operation order { ${STATE} }`;
export const LIVE = `subscription Live($scope: ID!) { orderEvents(scopeId:$scope) { scopeId kind cursor change { ${CHANGE} } } }`;
export async function graphql(url, query, variables, signal) {
  const response = await fetch(url,{ method: 'POST', signal,
    headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query,variables }) });
  const result = await response.json();
  if (!response.ok || result.errors?.length) {
    const error = new Error(result.errors?.[0]?.message ?? `HTTP ${response.status}`);
    error.code = result.errors?.[0]?.extensions?.code; throw error;
  }
  return result.data;
}
export function apiFor(url, scope, signal) {
  return {
    snapshot: async fresh => (await graphql(url,
      `query Snapshot($scope:ID!) { ordersSnapshot(scopeId:$scope${fresh ? ',fresh:true' : ''}) { scopeId cursor orders { ${STATE} } } }`,{scope},signal)).ordersSnapshot,
    head: async () => (await graphql(url,'query Head($scope:ID!) { orderHead(scopeId:$scope) }',{scope},signal)).orderHead,
    changes: async (after,through) => (await graphql(url,
      `query Changes($scope:ID!,$after:ID!,$through:ID!) { orderChanges(scopeId:$scope,after:$after,through:$through,first:2) { events { ${CHANGE} } nextCursor hasMore through } }`,
      {scope,after,through},signal)).orderChanges
  };
}
// Parser incremental: não assume que um chunk TCP corresponde a uma parte JSON.
export async function* multipart(body, boundary) {
  const marker = `--${boundary}`;
  const decoder = new TextDecoder(); let pending = ''; let started = false;
  for await (const chunk of body) {
    pending += decoder.decode(chunk,{stream:true});
    if (started && pending.startsWith('--')) return;
    if (pending.length > 4_000_000) throw new Error('Buffer multipart excedeu 4 MB; reconectar');
    if (!started) {
      const start = pending.indexOf(marker);
      if (start < 0) continue;
      pending = pending.slice(start+marker.length); started = true;
    }
    while (true) {
      const end = pending.indexOf(marker);
      if (end < 0) break;
      const part = pending.slice(0,end); pending = pending.slice(end+marker.length);
      const split = part.indexOf('\r\n\r\n');
      if (split >= 0 && part.slice(split+4).trim()) yield JSON.parse(part.slice(split+4).trim());
      if (pending.startsWith('--')) return;
    }
  }
  throw new Error('Stream multipart interrompido');
}
export async function* routerFrames(url, scope, signal) {
  const response = await fetch(url,{ method:'POST', signal,
    headers: { 'content-type':'application/json', accept:'multipart/mixed;subscriptionSpec="1.0"' },
    body: JSON.stringify({ query:LIVE,variables:{scope} }) });
  const type = response.headers.get('content-type') ?? '';
  const boundary = /boundary\s*=\s*(?:"([^"]+)"|([^;\s]+))/i.exec(type);
  if (!response.ok || !boundary) throw new Error(`Subscription HTTP ${response.status}: ${await response.text()}`);
  for await (const message of multipart(response.body,boundary[1] ?? boundary[2])) {
    const payload = message.payload ?? message;
    if (payload.errors?.length) throw new Error(JSON.stringify(payload.errors));
    if (payload.data?.orderEvents) yield payload.data.orderEvents;
    // {} é heartbeat de transporte; não é checkpoint de negócio.
  }
}
