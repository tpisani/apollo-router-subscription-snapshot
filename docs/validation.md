# Validação realizada

Ambiente: Linux, Node.js 22.22.0, Hive Gateway 2.15.2, Docker 29.8.

## Executado com sucesso

- `npm test`: **14 testes aprovados**, 5 execuções seguidas sem falha.
  - 11 testes de feed/store: SNAPSHOT e ao vivo, replay por cursor, escolha
    entre replay e snapshot (expirado, distante, acima do head, inválido),
    200 escritas intercaladas com aberturas de feed sem lacuna nem duplicata,
    liberação do listener em `return()`, `SLOW_CONSUMER`, lote de eventos
    acumulados, regras do `FeedState`, reconexão do `runFeed` com o último
    cursor, rollback atômico sem publicação e persistência após reinício.
  - 3 testes de ponta a ponta com o **processo real do Hive Gateway**:
    cliente graphql-ws → gateway → graphql-ws → subgraph, com `details.label`
    resolvido no subgraph catalog. Cobre late joiner, exclusão, queda abrupta
    da conexão (`terminate`) com retomada por `after` (confirmada pela métrica
    `resumeStarts`), rajada de 30 mutations concorrentes, histórico expirado
    → SNAPSHOT, reinício do subgraph com commits durante a queda, e
    `activeFeeds = 0` / `busListeners = 0` ao final.
- `docker compose up`: subgraphs + gateway em containers, `npm run client` e
  `npm run demo` contra `localhost:4000`, dados preservados no volume entre
  execuções e reinício do container dos subgraphs com o cliente conectado.
  (Neste sandbox a imagem foi construída com o CA do proxy local injetado
  fora do repositório; o `Dockerfile` versionado não foi alterado.)

## Latência medida (`npm run bench`)

Mesma máquina; clientes e subgraphs no mesmo processo Node, gateway em outro.
Os números servem para comparar configurações, não como capacidade.

| Clientes | Commits/s | Seleção | Entregas | p50 | p95 | p99 | máx |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 50 | 50 | só evento | 12.500/12.500 | 6 ms | 14 ms | 23 ms | 35 ms |
| 50 | 50 | `details { label }` | 12.500/12.500 | 146 ms | 295 ms | 307 ms | 319 ms |
| 200 | 100 | só evento | 100.000/100.000 | 61 ms | 227 ms | 299 ms | 463 ms |

Todos os clientes convergiram para o head, sem reconexões. Na versão de
`main`, só a espera do polling já soma até 500 ms antes de o evento sair do
subgraph.

## Não verificado

- Apollo Router: federated subscriptions exigem licença GraphOS. O snippet em
  `docs/design.md` não foi executado.
- Bus distribuído / múltiplas réplicas do subgraph: só a interface está
  pronta; o adaptador não foi implementado.
