# Router customizado com `poc.retain_last_frame`

Apollo Router 2.18.0 com um plugin nativo que guarda o último evento de cada
subscription deduplicada. Quem entra num upstream já aberto recebe esse estado na
hora, em vez de esperar o próximo frame do subgraph.

O comportamento é o mesmo do emulador (`scripts/edge.mjs`), agora dentro do Router:

- **Onde entra:** no `supergraph_service`. O stream de uma subscription começa com
  um marcador (`subscribed: true`, sem dados) que o router service consome para
  abrir o multipart e não envia. Por isso o frame retido vai **depois** do
  marcador, antes dos eventos ao vivo.
- **Chave:** documento + `operationName` + variáveis + os headers de
  `identity_headers`. Tokens diferentes nunca compartilham frame retido.
- **O que guarda:** só eventos com dados e sem erros.
- **Ciclo de vida:** a entrada existe enquanto houver assinante com a chave. Quando
  o último stream é descartado, o frame retido some junto com o upstream.
- **Marcação:** o frame retido sai com `extensions.retainedFrame: true`. O cliente
  não precisa dela (ele fica com a maior versão); os testes a usam para medir.
- **Correção:** depende de todo frame trazer o estado inteiro e uma versão. Um frame
  retido mais velho que um em trânsito é ignorado pelo cliente.

```yaml
plugins:
  poc.retain_last_frame:
    enabled: true
    identity_headers: [authorization]
```

## Compilar e testar (sem licença)

Precisa de Rust 1.97.1 (o mínimo do apollo-router 2.18.0, fixado em `rust-toolchain.toml`; o rustup instala sozinho) e `protoc` (`apt-get install protobuf-compiler libprotobuf-dev`). A primeira
compilação do Router leva ~20 min.

```bash
cd router-plugin
cargo test --release          # testes de unidade do plugin
cargo build --release         # target/release/router
./target/release/router config validate ../router/router-plugin.yaml
```

## No pipeline do Router, sem licença

```bash
npm run test:router-harness
```

Sobe os subgraphs Node (frame periódico de 3 s) e roda `src/harness.rs` com o
`TestHarness` do crate: router service, deduplicação, passthrough `graphql_ws` e
multipart de verdade, com o plugin carregado pela config. Mede, numa instância,
deduplicação, late joiner com e sem retenção, eventos ao vivo depois do frame
retido, identidade e limpeza. O `TestHarness` usa uma licença irrestrita só em
testes; ele não serve tráfego nem substitui rodar o binário com GraphOS.

## Rodar de verdade (exige GraphOS)

O Router recusa subscriptions federadas sem uma chave do GraphOS
("license violation"); o plano gratuito inclui subscriptions federadas. Remover essa
verificação violaria a licença (ELv2), então o teste real precisa da chave.

1. Crie um graph no GraphOS e gere uma chave.
2. Defina `APOLLO_KEY` e `APOLLO_GRAPH_REF` como variáveis de ambiente, nunca no
   repositório. Numa sessão do Claude Code na web: menu do ambiente → Editar →
   variáveis de ambiente; uma sessão nova já as recebe. Localmente: `.env` (fora
   do git) ou `export`.
3. Rode o teste de verdade, com o binário no host:

```bash
npm run test:router-real                 # subgraphs por WebSocket (passthrough)
MODE=callback npm run test:router-real   # subgraphs por HTTP callback
```

   Sobe os subgraphs Node, 3 routers com o plugin (4101–4103), 1 sem retenção
   (4104) e o balanceador (4200), roda `scripts/verify-router-scale.mjs` com uma
   instância derrubada no meio do jogo (`KILL=0` desliga) e encerra tudo. É o
   caminho numa sessão na nuvem: os processos herdam o proxy e a CA da sessão, e
   os containers do Docker não; dentro deles o Router não alcança o GraphOS e
   fica tentando buscar a licença, sem abrir a porta.

   Numa máquina local, o mesmo pelo compose:

```bash
docker compose --profile apollo-plugin up --build -d
npm run test:router-scale
# com uma instância derrubada no meio do jogo:
KILL_CMD="docker compose --profile apollo-plugin stop router-b" npm run test:router-scale
```

**Modo callback:** `router/router-plugin-callback.yaml` troca o passthrough pelo
callback do Router (`/callback` na mesma porta); os subgraphs Node implementam o
protocolo callback/1.0 (`src/app.mjs`). O plugin age no stream que vai ao cliente,
então se comporta igual nos dois modos.

**Deduplicação sob concorrência:** `scripts/probe-dedup.mjs` mede quantos
upstreams uma instância abre quando vários clientes entram ao mesmo tempo
(`MODE`, `CLIENTS`, `TRIALS`, `RETAIN_ENABLED=false`). No passthrough, o Router às
vezes abre um a mais, inclusive com o plugin desligado (ver
[validação](../docs/validation.md)).

**Limite do plano gratuito:** o Router limita as requisições por instância
(503 `ROUTER_FREE_PLAN_RATE_LIMIT_REACHED`); cada subscription conta uma vez,
mesmo durando minutos. Por isso o roteiro manda as mutations e as queries de base
direto aos subgraphs e só as subscriptions passam pelos routers (cerca de 40 por
instância no total). Se o limite for atingido, o roteiro para e diz isso.

O perfil do compose sobe (portas iguais às do `test:router-real`):

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

## Suporte e atualizações

A Apollo não dá suporte a plugins nativos nem a binários customizados. Cada versão
nova do Router exige recompilar e rodar `npm run test:router-harness`.
[`docs/router-upgrades.md`](../docs/router-upgrades.md) compara 20 versões
(1.61 a 2.18) e mostra o que costuma quebrar:
- **O código do plugin:** raramente. Ele compilou e passou em todos os testes na
  2.18.0 sem mudanças.
- **Em volta dele:** com frequência. A versão do Rust, a config YAML do Router (que
  não serve para duas versões ao mesmo tempo) e dependências como o `schemars`.

O guia também traz o passo a passo de atualização.
