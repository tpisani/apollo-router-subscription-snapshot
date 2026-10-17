# GraphQL federado em tempo real: placar e estatísticas

POC de dados esportivos ao vivo com GraphQL federado: placar e estatísticas em
**subgraphs e subscriptions separados**, cada um com a sua política de envio,
compatível com a deduplicação de subscriptions do Apollo Router e com cache em
todas as queries. Roda sem licença com o Hive Gateway e um edge que emula a
deduplicação e o cache do Router.

- **Placar:** cada mudança sai na hora, com o estado inteiro (~150 B). O gol
  chega em ~10 ms. Quem entra depois fica certo no primeiro frame, sem query.
- **Estatísticas:** ~20 lances/s viram 2 frames/s, cada um com o patch ou o
  estado, o que for menor. Perdeu algo? Um snapshot com versão mínima resolve,
  sem histórico.
- **Uma lista de jogos assina só o placar** e não recebe nenhum byte de
  estatísticas.

Por que dois canais, com os experimentos que sustentam a decisão:
[`docs/design.md`](docs/design.md). O que foi executado:
[`docs/validation.md`](docs/validation.md).

## Comece por aqui

- **Políticas de envio por canal (hub, agrupamento, fila):** [`src/channels.mjs`](src/channels.mjs)
- **Patches por valor final (idempotentes e aplicáveis com sobreposição):** [`src/patch.mjs`](src/patch.mjs)
- **Cliente (`StateSync` para o placar, `DeltaSync` para as estatísticas):** [`client/sync.mjs`](client/sync.mjs)
- **Domínio (placar, relógio, estatísticas):** [`src/sports.mjs`](src/sports.mjs)
- **Schemas dos subgraphs `scores`, `stats` e `catalog`:** [`src/schemas.mjs`](src/schemas.mjs)
- **Experimentos de desenho:** [`tests/experiments.test.mjs`](tests/experiments.test.mjs), [`scripts/experiments.mjs`](scripts/experiments.mjs)

## Operações

```graphql
# Placar: mesmas variáveis para todos → deduplicável
subscription Scoreboard($matchId: ID!) {
  scoreboardUpdates(matchId: $matchId) { version committedAt state { status home away clock { running seconds updatedAt } } }
}
# Estatísticas: um frame por janela de 500 ms
subscription Stats($matchId: ID!) {
  statsUpdates(matchId: $matchId) { kind from version patch { path value } state { home { possession shots } } }
}
# Snapshots cacheáveis: "um estado em alguma versão >= minVersion"
query { scoreboard(matchId: "m1", minVersion: 0) { version state { home away } } }
query { matchStats(matchId: "m1", minVersion: 812) { version state { home { possession } } } }
# Estáticos (cache de 1 h)
query { match(id: "m1") { home { name } away { name } players { id name number } } }
```

## Requisitos

Node.js 22.13+ (usa `node:sqlite`; o CI usa Node 24). Docker é opcional. O
Apollo Router exige `APOLLO_KEY`/`APOLLO_GRAPH_REF` com licença para
subscriptions federadas e response cache.

## Executar sem licença (Hive Gateway + edge)

```bash
npm ci
npm run compose-schema   # gera gateway/supergraph.graphql
SIMULATE=m1 npm start    # subgraphs scores :4001, catalog :4002, stats :4003, simulando a partida m1
npm run gateway          # Hive Gateway :4000
npm run edge             # dedup + cache de queries (pior caso) :4010, multipart como o Router
```

Em outro terminal:

```bash
npm run client                         # tela da partida: placar + estatísticas
CHANNELS=scoreboard npm run client     # lista de jogos: só placar
```

Sem `SIMULATE`, use `npm run demo` para gerar lances e gols pelas mutations.
Métricas: `curl localhost:4001/metrics` (streams, frames, snapshots) e
`curl localhost:4010/metrics` (upstreams por operação, cache).

Com Docker: `docker compose up --build -d` sobe tudo, com a partida m1 simulada.

## Executar com o Apollo Router

```bash
cp .env.example .env     # preencha APOLLO_KEY e APOLLO_GRAPH_REF
docker compose --profile apollo up --build -d   # Router em :4100
GRAPHQL_URL=http://localhost:4100/graphql npm run client
npm run test:router      # o mesmo cenário dos testes, contra o Router
```

## Testes, experimentos e benchmark

```bash
npm test                 # 30 testes: domínio, canais, cliente, experimentos e ponta a ponta
npm run bench            # partida simulada pelo edge: 100 clientes só de placar + 50 completos
SCORE_ONLY=0 FULL=200 STORM_RATE=400 npm run bench
```

## Limites deliberados

- Sem autenticação: `matchId` é só um seletor.
- Hubs em memória: um processo por subgraph. Com réplicas, alimente o hub por um
  bus depois do commit.
- Placar e estatísticas não são consistentes entre si numa transação: as
  estatísticas podem estar até ~0,5 s atrás do placar (ver
  [design](docs/design.md#consistência-entre-canais)).
- A deduplicação e o response cache do Router foram emulados; a config foi
  validada com o binário oficial, não executada.
