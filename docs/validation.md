# Validação realizada

Ambiente: Linux, Node.js 22.22.0, Hive Gateway 2.15.2, Apollo Router 2.10.0
(somente validação de config), Docker 29.8.

## Executado com sucesso

- `npm test`: **25 testes aprovados**, 5 execuções seguidas sem falha.
  - `tests/sync.test.mjs`, cliente contra o store real com um cache que nunca
    expira e ignora `Cache-Control`:
    - base antiga + histórico = estado exato;
    - evento ao vivo sem query;
    - late joiner que começa com um CHANGE;
    - histórico expirado → snapshot com `minCursor`;
    - `maxReplay`;
    - páginas alinhadas compartilhadas entre cursores diferentes;
    - cache que viola o contrato e página com buraco são rejeitados;
    - render provisório antes do primeiro frame;
    - nova tentativa após falha da base;
    - **teste de propriedade**: 25 sementes com perda, duplicação e
      reordenação de frames, joins tardios, reconexões e poda do log.
      Introduzir de propósito um bug (aplicar CHANGE com lacuna) o faz falhar
      na semente 1.
  - `tests/server.test.mjs`:
    - checkpoint imediato e push no commit;
    - checkpoints periódicos com um timer por escopo;
    - `return()` libera o listener;
    - conflação exata da fila;
    - contratos de `changes` (imutável, `NOT_YET`, `CURSOR_EXPIRED`, limite de 500);
    - `SnapshotCache` com `minCursor`;
    - atomicidade sem publicação;
    - persistência;
    - `Cache-Control` de cada campo, incluindo o menor TTL entre campos e
      `no-store` em erros;
    - parser multipart com chunks de 1 byte.
  - `tests/e2e.test.mjs`, com o processo real do Hive Gateway:
    - cenário completo (`scripts/verify.mjs`) pelo edge (deduplicação + cache
      de tudo, multipart): base antiga confirmada, late joiner com **um único
      stream no subgraph**, ao vivo sem queries, retomada sem snapshot,
      histórico expirado, rajada de 40 mutations concorrentes e liberação de
      todos os streams;
    - o mesmo cenário direto no Hive (graphql-ws, sem deduplicação nem cache,
      com `details.label` federado);
    - reinício do subgraph com 3 clientes no stream deduplicado;
    - rajada maior que a fila do stream (conflação confirmada pela métrica),
      recuperada pelo histórico.
- `docker compose up` (subgraphs + Hive + edge em containers):
  - `scripts/verify.mjs` passou contra o edge em `localhost:4010`, com a
    asserção de stream único no subgraph;
  - `npm run client` + `npm run demo`: base provisória, depois sincronizado,
    eventos ao vivo em 5–6 ms.
  - Neste sandbox a imagem foi construída com o CA do proxy local injetado
    fora do repositório.
- `router config validate router/router.yaml` com a imagem oficial
  `ghcr.io/apollographql/router:v2.10.0`: válido. Na primeira tentativa a
  validação recusou `enable_deduplication`; a chave correta é
  `subscription.deduplication.enabled`.
- `docker compose --profile apollo config`: válido.

## Benchmark (`npm run bench`, 5 s)

Os clientes entram com a base compartilhada aquecida no cursor 100 e o head
em 500, então todos começam 400 eventos atrás. Clientes, subgraphs e edge
rodam num mesmo processo Node, e o gateway em outro: os números servem para
comparar modos, não como capacidade.

| Modo | Clientes | Commits/s | Ao vivo p50 / p99 | Bootstrap p50 | Streams no subgraph | Queries no subgraph (bootstrap) | Queries durante o ao vivo |
| --- | --- | --- | --- | --- | --- | --- | --- |
| edge (dedup + cache) | 50 | 50 | 5 / 11 ms | 491 ms | 1 | 1 snapshot + 8 páginas | 0 |
| edge + `details.label` | 50 | 50 | 7 / 13 ms | 622 ms | 1 | 1 + 13 | 0 |
| Hive direto | 50 | 50 | 7 / 14 ms | 892 ms | 50 | 3 + 6 | 0 |
| Hive direto + `details.label` | 50 | 50 | 47 / 92 ms | 1887 ms | 50 | 4 + 6 | 0 |
| edge (dedup + cache) | 200 | 100 | 40 / 106 ms | 1298 ms | 1 | 1 + 8 | 0 |

Todos os clientes convergiram para o head em todas as rodadas (100% das
entregas). No modo edge, os 50 clientes fizeram cerca de 250 queries, das
quais o edge serviu ~200 do cache. As páginas alinhadas fazem clientes
diferentes pedirem as mesmas chaves.

Com o head estável, ou seja, sem uma base antiga, o bootstrap pelo edge
ficou em p50 135 ms com `CHECKPOINT_MS=250`, e em 1156 ms com 1000 ms. Por isso
o padrão ficou em 250 ms.

## Não verificado

- **Apollo Router em execução:** sem licença GraphOS, o binário recusa
  iniciar ("license violation: Federated subscriptions"). A deduplicação e o
  response cache foram emulados (`scripts/edge.mjs`). Rode
  `npm run test:router` num ambiente licenciado.
- Que o Router respeite o `max-age` de cada resposta do subgraph no response
  cache: o protocolo não depende disso (o edge cacheia tudo para sempre), mas
  a taxa de acerto depende.
- Bus distribuído e múltiplas réplicas do subgraph: só a interface existe.
