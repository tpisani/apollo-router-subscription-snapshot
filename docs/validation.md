# Validação realizada

Ambiente: Node.js 24.19.0, Apollo Router 2.10.0, Linux.

## Executado com sucesso

- `npm ci` / instalação a partir do lockfile.
- `npm test`: **13 testes aprovados**, incluindo servidor real HTTP/WS e SQLite.
- `npm run compose-schema`: dois subgraphs compostos sem erros.
- Binário oficial: `router config validate router/router.yaml`, configuração válida.

Os testes incluem estado antigo no cache, replay paginado, exclusões,
duplicatas, reordenação, detecção de lacunas por checkpoint, mudanças durante
bootstrap, cliente tardio, retomada, retenção expirada, isolamento por escopo,
rollback atômico, persistência após reinício, multipart em chunks de um byte e
resolução de referência de entidade no segundo subgraph.

## Limite da validação

A execução do Router com response caching e federated subscriptions foi
tentada. O binário recusou habilitar os recursos por não haver uma chave/licença
GraphOS disponível neste ambiente. Portanto **cache Redis e deduplicação reais
do Router não foram verificados de ponta a ponta**. Não confundir os testes do
protocolo e dos subgraphs com essa verificação.

Docker não está disponível neste ambiente; `docker compose up` não foi
executado aqui. O arquivo Compose fornece a montagem para seu ambiente local.

Execute `npm run test:router` contra uma instância com os recursos habilitados.
Esse script exige um snapshot cacheado, dois clientes convergentes, uma única
subscription de origem, eventos posteriores, snapshot fresh e reconexão.

## Publicação

Destino autorizado pelo proprietário:
https://github.com/tpisani/poc-graphql-subscriptions, branch `main`.
O pacote ZIP entregue antes da publicação mantém o estado inicial do exemplo.
