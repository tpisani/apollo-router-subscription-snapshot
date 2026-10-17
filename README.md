# GraphQL federado em tempo real, com deduplicação no Router e queries cacheáveis

POC de entrega de dados em tempo real com GraphQL federado: dois subgraphs
(orders e catalog), SQLite persistente, Apollo Router (ou o Hive Gateway,
de código aberto, para rodar sem licença).

- **Ao vivo por push:** cada commit chega ao cliente em poucos ms, sem polling
  e sem query no caminho quente.
- **Compatível com a deduplicação do Router:** a subscription
  `orderEvents(scopeId)` não tem estado por cliente; 50 clientes viram 1
  stream no subgraph.
- **Tolerante a cache:** toda query responde um fato que nunca deixa de ser
  verdade, então pode vir de qualquer cache, com qualquer TTL.
- **Estado inicial consistente e atual:** um snapshot antigo do cache serve
  só como base; o alvo ("agora") vem sempre do stream, e o que falta vem de
  intervalos imutáveis do log.

O raciocínio completo está em [`docs/design.md`](docs/design.md), e o que foi
executado em [`docs/validation.md`](docs/validation.md).

## Comece por aqui

- **Algoritmo do cliente:** [`client/sync.mjs`](client/sync.mjs)
- **Contratos das queries e Cache-Control por campo:** [`src/store.mjs`](src/store.mjs), [`src/app.mjs`](src/app.mjs)
- **Stream compartilhado (push, checkpoints, conflação):** [`src/live.mjs`](src/live.mjs)
- **Apollo Router:** [`router/router.yaml`](router/router.yaml)
- **Emulação de dedup + cache usada nos testes:** [`scripts/edge.mjs`](scripts/edge.mjs)
- **Cenário de ponta a ponta (testes e `test:router`):** [`scripts/verify.mjs`](scripts/verify.mjs)

## Protocolo em uma tela

```graphql
# Igual para todos os clientes do escopo → deduplicável
subscription { orderEvents(scopeId: "loja-42") { kind cursor change { cursor operation order { id version status } } } }

# Cacheáveis: os contratos não mudam com o tempo
query { ordersSnapshot(scopeId: "loja-42", minCursor: "0") { cursor orders { id version status } } }  # base compartilhada
query { ordersSnapshot(scopeId: "loja-42", minCursor: "812") { cursor orders { id version status } } } # estado >= 812
query { orderChanges(scopeId: "loja-42", after: "800", through: "812") { events { cursor operation order { id version status } } } }
```

1. Abre o stream e, em paralelo, busca a base `minCursor: "0"` (provavelmente
   do cache, possivelmente antiga) para renderizar na hora como provisória.
2. O primeiro frame do stream (CHANGE, ou CHECKPOINT a cada 250 ms) dá o alvo `T`.
3. Aplica os eventos `(base, T]` em páginas alinhadas e imutáveis. Se o
   histórico expirou ou a distância é grande, pede `minCursor: T` (uma chave
   nova).
4. Sincronizado. Daí em diante, cada CHANGE é aplicado direto do frame. Uma
   lacuna ou um checkpoint adiantado disparam o catch-up pelo histórico.

## Requisitos

Node.js 22.13+ (usa `node:sqlite`; o CI usa Node 24). Docker é opcional. O
Apollo Router exige `APOLLO_KEY`/`APOLLO_GRAPH_REF` com licença para
subscriptions federadas e response cache.

## Executar sem licença (Hive Gateway + edge)

```bash
npm ci
npm run compose-schema   # gera gateway/supergraph.graphql
npm start                # subgraphs: orders :4001 (HTTP+WS, /metrics), catalog :4002
npm run gateway          # Hive Gateway :4000
npm run edge             # dedup + cache de queries (pior caso) :4010, multipart como o Router
```

Em outros terminais:

```bash
npm run client           # sincroniza pelo edge e imprime cada frame com a latência
npm run demo             # escritas contínuas
```

Abra vários `npm run client`: o edge mostra `deduplicatedJoins` e o subgraph
mantém `activeLiveStreams: 1` (`curl localhost:4010/metrics`,
`curl localhost:4001/metrics`). Para o Hive direto, sem dedup:
`GRAPHQL_URL=http://localhost:4000/graphql TRANSPORT=ws npm run client`.

Com Docker: `docker compose up --build -d` sobe subgraphs, gateway e edge nas
mesmas portas.

## Executar com o Apollo Router

```bash
cp .env.example .env     # preencha APOLLO_KEY e APOLLO_GRAPH_REF
docker compose --profile apollo up --build -d   # Router em :4100, Redis para o response cache
GRAPHQL_URL=http://localhost:4100/graphql npm run client
npm run test:router      # mesmo cenário dos testes, contra o Router (use uma instância sem outros clientes)
```

`test:router` exige que a base compartilhada venha do cache, antiga, e que
dois clientes gerem um único stream no subgraph. Desative essas checagens com
`EXPECT_CACHE=0` / `EXPECT_DEDUP=0`.

## Testes e benchmark

```bash
npm test                 # 25 testes: unitários, propriedade e ponta a ponta com o Hive Gateway real
MODE=edge npm run bench  # clientes multipart pelo edge (dedup + cache)
MODE=hive npm run bench  # clientes graphql-ws direto no Hive (sem dedup)
ENRICH=1 npm run bench   # inclui details.label (subgraph catalog) em cada frame
```

## Limites deliberados

- Sem autenticação: `scopeId` é um seletor. Em produção, autorize o escopo no
  `subscribe` e nas queries, e considere os headers na chave de deduplicação
  e de cache.
- Bus em memória: um processo de subgraph. Para réplicas, troque o bus por
  um adaptador distribuído (mesma interface) e o SQLite por um banco
  compartilhado.
- O histórico não é podado automaticamente; `Store.prune()` existe para os
  testes de retenção.
- O cliente de terminal guarda o estado em memória. Para sobreviver a um
  reinício, persista estado e cursor juntos.
- A deduplicação e o response cache do Router não foram executados aqui (sem
  licença); foram emulados, e a config foi validada com o binário oficial.
