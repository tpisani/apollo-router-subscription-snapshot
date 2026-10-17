# GraphQL federado: feed de pedidos em tempo real

POC de entrega de dados em tempo real com GraphQL federado: dois subgraphs
(orders e catalog), SQLite persistente e o Hive Gateway (código aberto).
Cada commit vai do banco ao cliente por push, sem polling, normalmente em
poucos milissegundos. O cliente retoma de onde parou com um cursor, sem perder
nem duplicar eventos.

> Este branch é uma implementação alternativa à de `main`. A comparação está
> em [`docs/design.md`](docs/design.md#o-que-mudou-em-relação-à-versão-em-main).

## Comece por aqui

- **Feed sem lacunas, contrapressão:** [`src/feed.mjs`](src/feed.mjs)
- **Projeção + log + decisão replay/snapshot:** [`src/store.mjs`](src/store.mjs)
- **Subgraphs HTTP/WS:** [`src/app.mjs`](src/app.mjs)
- **Cliente (estado, reconexão, ping/pong):** [`client/feed-client.mjs`](client/feed-client.mjs), [`client/transport.mjs`](client/transport.mjs)
- **Gateway:** [`gateway/gateway.config.mjs`](gateway/gateway.config.mjs)
- **Decisões, limites e medições:** [`docs/design.md`](docs/design.md), [`docs/validation.md`](docs/validation.md)

## Como funciona

```graphql
subscription Feed($scopeId: ID!, $after: ID) {
  orderFeed(scopeId: $scopeId, after: $after) {
    scopeId kind cursor
    orders { id version status }
    changes { cursor operation committedAt order { id version status } }
  }
}
```

- `after: null`: recebe `SNAPSHOT` (estado + cursor da mesma transação) e
  depois `CHANGES` a cada commit.
- `after: "<cursor>"`: recebe só os eventos que faltam (ou `SNAPSHOT` se o
  histórico não cobrir) e continua ao vivo.
- O servidor se registra no bus antes de ler o banco, então não há janela
  entre o frame inicial e o ao vivo. O cliente aplica somente `cursor + 1`,
  ignora reentregas e reconecta do último cursor válido em qualquer anomalia.

## Requisitos

Node.js 22.13+ (usa `node:sqlite`; o CI usa Node 24). Nenhuma licença ou
chave externa. Docker é opcional.

## Executar localmente

```bash
npm ci
npm run compose-schema   # gera gateway/supergraph.graphql
npm start                # subgraphs: orders :4001 (HTTP+WS), catalog :4002
npm run gateway          # Hive Gateway :4000 (outro terminal)
```

Em outro terminal, o cliente (imprime cada frame e a latência commit → cliente):

```bash
npm run client
ENRICH=1 npm run client  # inclui details.label vindo do subgraph catalog
```

E em outro, gere escritas:

```bash
npm run demo             # COUNT=20 INTERVAL_MS=500 por padrão
```

Experimente parar e subir de novo o `npm start` durante o demo: o cliente
continua sem perder commits.

Métricas didáticas do subgraph: `http://localhost:4001/metrics` (feeds ativos,
listeners no bus, retomadas vs. snapshots, frames enviados).

## Executar com Docker Compose

```bash
docker compose up --build -d
npm run client
npm run demo
docker compose down      # o banco fica no volume orders-data
```

## Testes e benchmark

```bash
npm test                 # unitários + ponta a ponta com o Hive Gateway real
CLIENTS=50 RATE=50 SECONDS=10 npm run bench
ENRICH=1 npm run bench   # mede o custo do campo federado por frame
```

`npm test` sobe os subgraphs e um processo real do gateway em portas livres e
cobre: snapshot, ao vivo, late joiner, exclusão, queda da conexão com retomada
por `after`, rajada concorrente, histórico expirado, reinício do subgraph,
consumidor lento, liberação de listeners e atomicidade da escrita.

## Operações

```graphql
mutation { upsertOrder(scopeId: "loja-42", id: "order-1", status: "PAID") { cursor } }
mutation { deleteOrder(scopeId: "loja-42", id: "order-1") { cursor } }
query { ordersSnapshot(scopeId: "loja-42") { cursor orders { id status details { label } } } }
```

## Limites deliberados

- Sem autenticação: `scopeId` é um seletor, não uma autorização.
- Bus em memória: um processo de subgraph. Para réplicas, troque por
  Redis/NATS/`LISTEN/NOTIFY` (mesma interface) e o SQLite por um banco
  compartilhado. Ver [design](docs/design.md#escalar-além-do-poc).
- O histórico não é podado automaticamente; `Store.prune()` existe para os
  testes de retenção.
- O cliente de terminal guarda o estado em memória. Para sobreviver a um
  reinício, persista estado e cursor juntos.
- `maskedErrors: false` no gateway expõe os erros de domínio ao cliente. Em
  produção, exponha apenas os códigos previstos.
