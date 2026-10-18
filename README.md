# GraphQL federado em tempo real: placar e estatísticas

POC de dados esportivos ao vivo com GraphQL federado: placar e estatísticas em
**subgraphs e subscriptions separados**, cada um com a sua política de envio,
compatível com a deduplicação de subscriptions do Apollo Router e com cache em
todas as queries. Roda sem licença com o Hive Gateway e um edge que emula a
deduplicação e o cache do Router.

- **Placar:** cada mudança sai na hora, com o estado inteiro (~150 B). O gol
  chega em ~10 ms.
- **Estatísticas:** também com o estado inteiro, com um limitador: na hora quando
  o jogo está calmo, e no máximo 2 frames/s numa rajada.
- **Uma regra só no cliente:** fica com o frame de maior versão. Não há lacuna,
  histórico nem query de recuperação.
- **Retenção do último frame**, num plugin nativo do Apollo Router
  ([`router-plugin/`](router-plugin/)) e no emulador: quem entra num upstream já
  aberto recebe o estado na hora (1 ms no pipeline do Router, contra ~2,8 s
  esperando o frame periódico).
- **Várias instâncias:** cada uma deduplica e retém os seus upstreams; uma
  instância que cai é absorvida pelas outras em ~150 ms, sem regressão de versão.
- **Uma lista de jogos assina só o placar** e não recebe nenhum byte de
  estatísticas.

- **Ciclo de vida:** partida agendada ou encerrada usa só a query; a subscription
  começa 15 min antes do início e termina no `FULL_TIME`. O cache do status nunca
  atrasa o início ([página](docs/cenarios/status-e-kickoff.html)).

Cenários passo a passo, interativos: [`docs/cenarios/`](docs/cenarios/).
Por que dois canais, estado inteiro, retenção e várias instâncias, com os experimentos:
[`docs/design.md`](docs/design.md). O que foi executado:
[`docs/validation.md`](docs/validation.md).

## Comece por aqui

- **Políticas de envio por canal (hub, limitador, frame periódico, fila):** [`src/channels.mjs`](src/channels.mjs)
- **Cliente (`StateSync`, igual nos dois canais):** [`client/sync.mjs`](client/sync.mjs)
- **Plugin do Router `poc.retain_last_frame` (Rust):** [`router-plugin/src/retain.rs`](router-plugin/src/retain.rs)
- **Retenção, chave com identidade e cache no emulador:** [`scripts/edge.mjs`](scripts/edge.mjs)
- **Várias instâncias atrás de um balanceador:** [`scripts/balancer.mjs`](scripts/balancer.mjs), [`tests/scale.test.mjs`](tests/scale.test.mjs)
- **Domínio (placar, relógio, estatísticas):** [`src/sports.mjs`](src/sports.mjs)
- **Schemas dos subgraphs `scores`, `stats` e `catalog`:** [`src/schemas.mjs`](src/schemas.mjs)
- **Experimentos de desenho:** [`tests/experiments.test.mjs`](tests/experiments.test.mjs), [`scripts/experiments.mjs`](scripts/experiments.mjs)

## Operações

```graphql
# Placar: mesmas variáveis para todos → deduplicável
subscription Scoreboard($matchId: ID!) {
  scoreboardUpdates(matchId: $matchId) { version committedAt state { status home away clock { running seconds updatedAt } } }
}
# Estatísticas: estado inteiro, no máximo 1 frame a cada 500 ms numa rajada
subscription Stats($matchId: ID!) {
  statsUpdates(matchId: $matchId) { version committedAt state { home { possession shots } away { possession shots } } }
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
npm run edge             # dedup + retenção + cache de queries (pior caso) :4010, multipart como o Router
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

### Router customizado com o plugin de retenção

```bash
npm run test:router-harness   # sem licença: o plugin no pipeline do Router (TestHarness)
npm run test:router-real      # com APOLLO_KEY/APOLLO_GRAPH_REF: 3 routers + balanceador no host
MODE=callback npm run test:router-real   # o mesmo com os subgraphs no modo callback
# ou, numa máquina local, pelo compose:
docker compose --profile apollo-plugin up --build -d && npm run test:router-scale
```

Detalhes em [`router-plugin/README.md`](router-plugin/README.md).

## Testes, experimentos e benchmark

```bash
npm test                 # 44 testes: domínio, canais, cliente, experimentos, ponta a ponta e várias instâncias
(cd router-plugin && cargo test --release)   # 8 testes de unidade do plugin
npm run bench            # partida simulada pelo edge: 100 clientes só de placar + 50 completos
EDGES=3 npm run bench    # o mesmo com 3 instâncias atrás de um balanceador
SCORE_ONLY=0 FULL=200 STORM_RATE=400 npm run bench
```

## Limites deliberados

- Sem autenticação: `matchId` é só um seletor.
- Hubs em memória: um processo por subgraph. Com réplicas, alimente o hub por um
  bus depois do commit.
- A retenção no Apollo Router exige um build customizado com plugin nativo, sem
  suporte da Apollo.
- Placar e estatísticas não são consistentes entre si numa transação: as
  estatísticas podem estar até ~0,5 s atrás do placar (ver
  [design](docs/design.md#consistência-entre-canais)).
- O plugin rodou com o Router de verdade e GraphOS, em 3 instâncias, nos modos
  passthrough e callback; o response cache com Redis ainda não. Ver [validação](docs/validation.md).
