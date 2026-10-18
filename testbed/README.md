# Testbed: GraphQL federado em tempo real (placar e estatísticas)

> **Diretório auxiliar.** O produto deste repositório é o plugin
> [`subscription_snapshot`](../README.md), na raiz. Aqui fica a POC que o motivou e
> a bancada que o testa e mede: subgraphs, cliente, emulador do Router e roteiros.
> Todos os comandos `npm` abaixo rodam dentro de `testbed/`.

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
  ([`subscription_snapshot`](../README.md), na raiz) e no emulador: quem entra num upstream já
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

- **Políticas de envio por canal (hub, limitador, frame periódico, fila):** [`subgraphs/channels.mjs`](subgraphs/channels.mjs)
- **Cliente (`StateSync`, igual nos dois canais):** [`client/sync.mjs`](client/sync.mjs)
- **Plugin do Router `subscription_snapshot` (Rust):** [`../src/lib.rs`](../src/lib.rs)
- **Retenção, chave com identidade e cache no emulador:** [`emulator/edge.mjs`](emulator/edge.mjs)
- **Várias instâncias atrás de um balanceador:** [`emulator/balancer.mjs`](emulator/balancer.mjs), [`tests/scale.test.mjs`](tests/scale.test.mjs)
- **Domínio (placar, relógio, estatísticas):** [`subgraphs/sports.mjs`](subgraphs/sports.mjs)
- **Schemas dos subgraphs `scores`, `stats` e `catalog`:** [`subgraphs/schemas.mjs`](subgraphs/schemas.mjs)
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

### Apollo Router de verdade com o plugin

O binário é o crate da raiz do repositório (`cargo build --release` lá, ou o
`test:router-real` compila sozinho na primeira vez). As configs ficam em
[`../examples/router/`](../examples/router/).

```bash
npm run test:router-harness   # sem licença: o plugin no pipeline do Router (TestHarness)
npm run test:router-real      # com APOLLO_KEY/APOLLO_GRAPH_REF: 3 routers + balanceador no host
MODE=callback npm run test:router-real   # o mesmo com os subgraphs no modo callback
# ou, numa máquina local, pelo compose:
docker compose --profile apollo-plugin up --build -d && npm run test:router-scale
# com uma instância derrubada no meio do jogo:
KILL_CMD="docker compose --profile apollo-plugin stop router-b" npm run test:router-scale
```

O Router recusa subscriptions federadas sem uma chave do GraphOS
("license violation"); o plano gratuito inclui subscriptions federadas. Remover essa
verificação violaria a licença (ELv2), então o teste real precisa da chave:

1. Crie um graph no GraphOS e gere uma chave.
2. Defina `APOLLO_KEY` e `APOLLO_GRAPH_REF` como variáveis de ambiente, nunca no
   repositório. Numa sessão do Claude Code na web: menu do ambiente → Editar →
   variáveis de ambiente; uma sessão nova já as recebe. Localmente: `.env` (fora
   do git) ou `export`.

O `test:router-real` sobe os subgraphs Node, 3 routers com o plugin (4101–4103), 1
sem retenção (4104) e o balanceador (4200), roda `scripts/verify-router-scale.mjs`
com uma instância derrubada no meio do jogo (`KILL=0` desliga) e encerra tudo. É o
caminho numa sessão na nuvem: os processos herdam o proxy e a CA da sessão, e os
containers do Docker não; dentro deles o Router não alcança o GraphOS e fica
tentando buscar a licença, sem abrir a porta.

**Modo callback:** `router-plugin-callback.yaml` troca o passthrough pelo callback
do Router (`/callback` na mesma porta); os subgraphs Node implementam o protocolo
callback/1.0 (`subgraphs/app.mjs`). O plugin age no stream que vai ao cliente,
então se comporta igual nos dois modos.

**Deduplicação sob concorrência:** `scripts/probe-dedup.mjs` mede quantos
upstreams uma instância abre quando vários clientes entram ao mesmo tempo
(`MODE`, `CLIENTS`, `TRIALS`, `RETAIN_ENABLED=false`). No passthrough, o Router às
vezes abre um a mais, inclusive com o plugin desligado (ver
[validação](docs/validation.md)).

**Limite do plano gratuito:** o Router limita as requisições por instância
(503 `ROUTER_FREE_PLAN_RATE_LIMIT_REACHED`); cada subscription conta uma vez,
mesmo durando minutos. Por isso o roteiro manda as mutations e as queries de base
direto aos subgraphs e só as subscriptions passam pelos routers (cerca de 40 por
instância no total). Se o limite for atingido, o roteiro para e diz isso.

O perfil `apollo-plugin` do compose sobe (portas iguais às do `test:router-real`):

| Serviço | Porta | Papel |
| --- | --- | --- |
| `router-a`, `router-b`, `router-c` | 4101–4103 | Router com o plugin ligado |
| `router-plain` | 4104 | o mesmo Router com `RETAIN_ENABLED=false` |
| `balancer` | 4200 | round-robin sem afinidade na frente de a, b e c |
| `subgraphs` | 4001–4003 | `/metrics` com streams abertos por partida |

`scripts/verify-router-scale.mjs` mede pelo cliente e pelos subgraphs:

1. **Deduplicação por instância:** streams no subgraph = instâncias × partidas ×
   canais.
2. **Late joiner numa partida parada:** com o plugin, recebe o frame retido logo
   (< 300 ms); sem, espera o frame periódico.
3. **Identidade:** tokens diferentes geram upstreams diferentes e não recebem o
   frame retido um do outro.
4. **Jogo ao vivo pelo balanceador**, opcionalmente com uma instância derrubada:
   nenhuma versão regride e todos terminam no estado do banco.
5. **Limpeza:** sem assinantes, nenhum stream fica aberto no subgraph.

O mesmo roteiro roda no `npm test` contra o emulador
(`tests/router-scale.test.mjs`), para garantir que ele próprio está certo.

Para deixar o efeito da retenção mais visível, aumente o frame periódico:
`SCOREBOARD_PERIODIC_MS=3000 docker compose --profile apollo-plugin up -d`.

O que costuma quebrar ao atualizar o Router (20 versões comparadas, plugin testado
na 2.18): [`../docs/router-upgrades.md`](../docs/router-upgrades.md).

## Testes, experimentos e benchmark

```bash
npm test                 # 44 testes: domínio, canais, cliente, experimentos, ponta a ponta e várias instâncias
(cd .. && cargo test --release)   # testes de unidade do plugin
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
