# Validação realizada

Ambiente: Linux, Node.js 22.22.0, Hive Gateway 2.15.2, Apollo Router 2.10.0
(validação de config), apollo-router 2.10.5 com o plugin (Rust 1.97), Docker 29.8.

## Testes (`npm test`: 44 aprovados)

- **`tests/domain.test.mjs` (8):**
  - patches e a propriedade de sobreposição (usados no modo `delta` dos
    experimentos);
  - versão só quando o estado muda;
  - atomicidade sem publicação e persistência;
  - `SnapshotCache` com `minVersion`;
  - relógio derivado no cliente;
  - estatísticas como valores finais.
- **`tests/channels.test.mjs` (10):**
  - `STATE` imediato, periódico, e só o mais novo para quem está lento;
  - **limitador:** lance isolado sai na hora; 100 commits em ~100 ms viram no
    máximo 3 frames, com o estado final; `committedAt` é o do commit mais antigo
    ainda não enviado;
  - liberação de listeners e timers;
  - modo `delta`, usado nos experimentos de comparação.
- **`tests/sync.test.mjs` (6):**
  - late joiner certo no primeiro frame;
  - estatísticas convertidas do formato da API;
  - frames repetidos, atrasados, fora de ordem ou retidos mais velhos ignorados;
  - render provisório;
  - nova tentativa após falha da base;
  - **teste de propriedade:** 25 sementes, 3 instâncias com frames retidos de
    idades diferentes, clientes trocando de instância, frames perdidos,
    duplicados e reordenados. A versão nunca regride e todos terminam no estado
    exato. Injetar um bug (aceitar frame mais velho) o faz falhar na semente 1.
- **`tests/experiments.test.mjs` (2):** banda e isolamento das cinco políticas
  de estatísticas (números em `docs/design.md`).
- **`tests/e2e.test.mjs` (4), com o processo real do Hive Gateway:**
  - cenário completo (`scripts/verify.mjs`) pelo edge: caches antigos, um stream
    por canal, rajada com gol no meio, cliente só de placar com 0 B de
    estatísticas, **retomada sem nenhuma query** e todos iguais ao banco;
  - o mesmo cenário direto no Hive;
  - upstreams por canal e partida;
  - reinício dos subgraphs.
- **`tests/scale.test.mjs` (5), com 3 edges atrás de um balanceador:**

| Teste | Resultado |
| --- | --- |
| 30 clientes, 2 partidas, 2 canais | 6 streams por canal no subgraph (3 instâncias × 2 partidas); máximo 1 upstream por partida em cada instância |
| Late joiner numa partida parada (periódico de 2 s) | **12 ms com retenção**, 1.997 ms sem |
| Identidade na chave (`authorization`) | tokens diferentes: 2 upstreams; só o cliente com o mesmo token recebeu o frame retido |
| Instância derrubada no meio do jogo (12 clientes, lances e gols contínuos) | 8 clientes migraram pelo balanceador; todos sincronizados de novo em **145–163 ms** (3 rodadas); nenhuma versão regrediu; estado final exato |
| Limpeza | sem assinantes, nenhuma instância guarda upstream nem frame retido; subgraph sem streams |

- **`tests/router-scale.test.mjs` (1):** o roteiro `scripts/verify-router-scale.mjs`,
  feito para os Routers com o plugin, contra o emulador (3 edges + 1 sem
  retenção), para provar o próprio roteiro antes de rodá-lo com a licença:
  6 streams por canal para 24 clientes (3 instâncias × 2 partidas); late joiner
  em 4–7 ms com frame retido contra ~1.850 ms sem; 2 upstreams para 2 tokens;
  12 clientes com uma instância derrubada, 8 migraram e todos voltaram em 146 ms,
  sem regressão; limpeza.

## Apollo Router de verdade com GraphOS (`npm run test:router-real`)

O binário de `router-plugin/` (2.10.5 + plugin) com uma chave do GraphOS (plano
gratuito, graph só de teste): 3 instâncias com retenção atrás do balanceador e 1
sem, contra os subgraphs Node, frame periódico de 3 s. Clientes em HTTP multipart
(o único transporte de subscription do Router para clientes). Só as subscriptions
passaram pelos routers; nenhuma bateu no limite do plano gratuito. Rodou nos dois
modos de subscription entre Router e subgraphs:

| Verificação | passthrough (WebSocket) | callback (HTTP, `MODE=callback`) |
| --- | --- | --- |
| Deduplicação: 24 clientes, 2 partidas | 3 streams por canal e partida (um por instância)¹ | 3 streams por canal e partida |
| Late joiner com o plugin | **4–7 ms**, frame retido nas 3 rodadas | **4–10 ms**, frame retido nas 3 rodadas |
| Late joiner sem o plugin | **2.834–2.839 ms** (periódico) | **2.837–2.847 ms** (periódico) |
| Identidade | 2 streams para 2 tokens; só o mesmo token recebe o frame retido | igual |
| Instância derrubada (SIGKILL) com 12 clientes | 8 reconectaram; todos de volta em **142 ms**; sem regressão; estado igual ao banco | 8 reconectaram; **143 ms**; igual |
| Limpeza | sem streams no subgraph | sem streams no subgraph (o subgraph para no 404 do Router) |

¹ **Deduplicação do Router sob concorrência (passthrough):** numa das execuções
uma instância abriu 2 upstreams para a mesma partida (4 em vez de 3). A sonda
`scripts/probe-dedup.mjs` (8 clientes simultâneos numa instância) reproduziu em
1 de 6 rodadas, e em 2 de 12 **com o plugin desligado**, ou seja, vem do Router;
o upstream extra continua aberto. No modo callback, 0 de 6. Não afeta a correção
(os dois upstreams entregam os mesmos frames e o cliente fica com a maior
versão), só a eficiência. O roteiro registra `upstreamsExtras` e só reprova se
não houver deduplicação.

- **`tests/lifecycle.test.mjs` (7), 3 execuções seguidas:** o ciclo de vida da
  partida pelo edge, com o cache de queries no pior caso (nunca expira) e janela de
  1 s:
  - regras com relógio fixo: fase por horário e status; TTL do `SCHEDULED` limitado
    pela janela, verificado como propriedade em todo instante antes dela;
  - `Cache-Control` real do subgraph: 300 s longe, ≤ o tempo até a janela perto,
    2 s na janela, 3600 s no `FULL_TIME`;
  - encerrada: 1 query por canal, 0 subscriptions;
  - agendada com `SCHEDULED` antigo no cache: 0 streams antes da janela, assinou
    menos de 500 ms depois dela; jogo atrasado segue assinando; `FULL_TIME` fecha
    os dois canais sem reconexão;
  - cache antigo dizendo `LIVE`: o primeiro frame traz `FULL_TIME` e tudo fecha em
    menos de 1 s;
  - 5 clientes deduplicados: o `FULL_TIME` encerra todos e o upstream fecha.

  **Mutações conferidas:** tirar o teto do TTL do `SCHEDULED` reprova 2 testes;
  decidir só pelo status (ignorando o horário) reprova 4, inclusive o de cache
  antigo.
- **`tests/callback.test.mjs` (1):** o protocolo callback/1.0 do lado do
  subgraph, contra um `/callback` falso: `check` antes de responder
  `{ data: null }`, frames como `next`, e um 404 do Router fecha o stream sem
  `complete`. Esse teste achou um `complete` indevido depois do 404, corrigido.

## Plugin do Router (`router-plugin/`, `poc.retain_last_frame`)

- **`cargo test --release` (8):** inclui a liberação no fim do stream; o marcador passa intacto; o primeiro assinante
  não recebe frame retido e alimenta o store; o late joiner recebe o retido logo
  depois do marcador, com `subscribed` aberto, e depois os eventos ao vivo;
  chaves diferentes não compartilham; erros e o item final não são retidos; a
  entrada some quando o último assinante sai; respostas que não são subscription
  aberta passam intactas; a chave inclui variáveis e identidade.
- **`npm run test:router-harness` (4):** o quarto teste, `FULL_TIME` no
  pipeline do Router: os dois clientes deduplicados recebem o frame final e o
  multipart fecha; o upstream fecha; quem chega depois recebe o estado final do
  subgraph e o stream termina. Esse teste achou um problema no plugin: a entrada
  retida só saía quando o cliente era descartado, não quando o stream terminava,
  e um cliente novo recebia o frame de um upstream já fechado. Corrigido (a entrada
  sai no item final ou no fim do stream), com teste de unidade.
- **`npm run test:router-harness` (3 primeiros), 3 execuções seguidas:** o plugin no
  pipeline do Router pelo `TestHarness` do crate (router service, deduplicação,
  passthrough `graphql_ws` para os subgraphs Node, multipart), frame periódico de
  3 s:

| Teste | Resultado |
| --- | --- |
| Deduplicação e retenção | 1 stream no subgraph para 2 clientes; late joiner em **1 ms** com `retainedFrame`, na mesma versão do primeiro; os dois recebem o gol seguinte; sem assinantes, o upstream fecha |
| Sem retenção (`enabled: false`) | late joiner em **2.799 ms**, esperando o periódico |
| Identidade | `authorization` diferente: 2 upstreams, e só o cliente com o mesmo token recebe o frame retido |

  **Mutação conferida:** inserir o frame retido antes do marcador faz dois testes
  falharem; o late joiner recebe `{"data":null}` e o frame se perde.
- **Binário:** `cargo build --release` gera `target/release/router` (2.10.5).
  `router config validate router/router-plugin.yaml` passa, com e sem
  `RETAIN_ENABLED`; um campo desconhecido no plugin é rejeitado pelo schema.
- **Sem chave**, o binário para na inicialização: `license violation ...
  ["Federated subscriptions"]`, como esperado. Com uma chave inválida de
  propósito, o GraphOS responde `ACCESS_DENIED`: o binário no host alcança o
  uplink pelo proxy da sessão. `npm run test:router-real` falha nesse ponto com
  a mensagem do Router, sem deixar processos.
- **Imagem Docker** (`router-plugin/Dockerfile`, Rust 1.97): compila e
  `config validate` passa dentro dela. O perfil `apollo-plugin` sobe todos os
  serviços, mas aqui, sem proxy nos containers, os routers ficam tentando buscar
  a licença (`fetch failed from all 2 uplink endpoints`) e não abrem a porta. Por
  isso o teste real na sessão é o `test:router-real`, com o binário no host.
- **Limite do plano gratuito**, lido no código do Router 2.10.5: `RateLimit` por
  instância no router service, 503 `ROUTER_FREE_PLAN_RATE_LIMIT_REACHED`. O
  roteiro passa só as subscriptions pelos routers e para com essa mensagem se o
  limite for atingido.

## Benchmark (`npm run bench`, 10 s)

100 clientes só de placar + 50 com os dois canais; 20 lances/s com uma rajada
de 200 lances/s; um gol a cada 2 s; o jogo já em andamento (400 lances antes).

| | 1 instância | 3 instâncias (`EDGES=3`) |
| --- | --- | --- |
| Convergiram | 150/150 | 150/150 |
| Placar p50 / p99 | 10 / 19 ms | 14 / 21 ms |
| Estatísticas p50 / p99 | 478 / 512 ms | 482 / 505 ms |
| Streams no subgraph por canal | 1 | 3 |
| Entradas servidas com frame retido | 169 | 184 (61 + 62 + 61) |
| Snapshots durante o jogo | 0 | 0 |
| Cliente só de placar | 443 B/s, 0 de estatísticas | 471 B/s, 0 de estatísticas |
| Cliente com os dois canais | + 4.441 B/s de estatísticas | + 4.433 B/s |

Na versão anterior, com patch adaptativo, o cliente completo recebia ~2.423 B/s
de estatísticas no mesmo benchmark. O estado inteiro custa ~1,8× em banda aqui.

## Docker e Router

- Compose (subgraphs com simulador + Hive + edge com retenção) com o código
  atual: `scripts/verify.mjs` contra `localhost:4010` passou, e `npm run client`
  mostrou placar e estatísticas ao vivo, com 3 entradas servidas por frame
  retido. `RETAIN=0` desliga a retenção.
- `router config validate router/router.yaml` com a imagem oficial 2.10.0:
  válido.

## Não verificado

- **Response cache com Redis** (`--profile apollo`) com o Router licenciado.
- **A causa exata do upstream duplicado** na deduplicação passthrough do Router
  (ver acima); seria um caso para reportar à Apollo.
- **O perfil `apollo-plugin` do compose com a chave:** os containers não alcançam
  o GraphOS nesta sessão; o mesmo cenário rodou com os binários no host.
- **Balanceador real** (ALB, NGINX, Envoy): o do teste é um round-robin simples
  com streaming. Num LB real, confira o timeout de ociosidade (maior que o frame
  periódico) e que as respostas multipart não são bufferizadas.
- **Réplicas dos subgraphs com bus.**
