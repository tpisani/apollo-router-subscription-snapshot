# `subscription_snapshot`: plugin do Apollo Router

Plugin nativo do Apollo Router 2.18.0 que guarda o último evento de cada
subscription deduplicada. Quem entra num upstream já aberto recebe esse estado na
hora (1 ms no pipeline do Router), em vez de esperar o próximo frame do subgraph
(~2,8 s com frame periódico de 3 s).

```yaml
plugins:
  globo.subscription_snapshot:
    enabled: true
    identity_headers: [authorization]
```

## Estrutura do repositório

| Caminho | O que é |
| --- | --- |
| [`src/lib.rs`](src/lib.rs) | o plugin, com os testes de unidade |
| [`src/main.rs`](src/main.rs) | o binário do Router com o plugin |
| [`tests/router_pipeline.rs`](tests/router_pipeline.rs) | testes de integração no pipeline do Router (`TestHarness`) |
| [`examples/router/`](examples/router/) | configs do Router com o plugin (passthrough e callback) |
| [`docs/router-upgrades.md`](docs/router-upgrades.md) | o plugin e as versões do Apollo Router |
| [`testbed/`](testbed/) | **auxiliar:** subgraphs, cliente, emulador e roteiros que testam e medem o plugin |

O `testbed/` é a POC que motivou o plugin (placar e estatísticas ao vivo em
subgraphs federados) e a bancada para testá-lo; ver o
[README do testbed](testbed/README.md).

## Como funciona

- **Onde entra:** no `supergraph_service`. O stream de uma subscription começa com
  um marcador (`subscribed: true`, sem dados) que o router service consome para
  abrir o multipart e não envia. Por isso o frame retido vai **depois** do
  marcador, antes dos eventos ao vivo.
- **Chave:** documento + `operationName` + variáveis + os headers de
  `identity_headers`. Tokens diferentes nunca compartilham frame retido.
- **O que guarda:** só eventos com dados e sem erros.
- **Ciclo de vida:** a entrada existe enquanto houver assinante com a chave. Quando
  o último stream termina ou é descartado, o snapshot some junto com o upstream.
- **Marcação:** o snapshot sai com `extensions.snapshot: true`. O cliente
  não precisa dela (ele fica com a maior versão); os testes a usam para medir.
- **Correção:** depende de todo frame trazer o estado inteiro e uma versão. Um frame
  retido mais velho que um em trânsito é ignorado pelo cliente (ver o
  [desenho](testbed/docs/design.md)).

Como recomenda a Apollo, o plugin é registrado com um nome de grupo
(`register_plugin!("globo", "subscription_snapshot", …)`), então a chave
em `plugins:` é `globo.subscription_snapshot`. O grupo evita colisão com
plugins da Apollo e com outros plugins customizados no mesmo binário.

## Compilar e testar (sem licença)

Precisa de Rust 1.97.1 (o mínimo do apollo-router 2.18.0, fixado em
`rust-toolchain.toml`; o rustup instala sozinho) e `protoc`
(`apt-get install protobuf-compiler libprotobuf-dev`). A primeira compilação do
Router leva ~20 min.

```bash
cargo fmt --all --check
cargo clippy --release --all-targets --no-deps -- -D warnings
cargo test --release          # testes de unidade (os de integração ficam ignorados)
cargo build --release         # target/release/router
./target/release/router config validate examples/router/router-plugin.yaml
docker build -t apollo-router-subscription-snapshot .
```

Os testes de integração precisam dos subgraphs do testbed:

```bash
cd testbed && npm ci && npm run test:router-harness
```

Sobe os subgraphs Node (frame periódico de 3 s) e roda `tests/router_pipeline.rs`
com o `TestHarness` do crate: router service, deduplicação, passthrough
`graphql_ws` e multipart de verdade, com o plugin carregado pela config. O
`TestHarness` usa uma licença irrestrita só em testes; ele não serve tráfego nem
substitui rodar o binário com GraphOS
([testes com o Router de verdade](testbed/README.md#apollo-router-de-verdade-com-o-plugin)).

## Convenções

O crate segue os [exemplos de plugins nativos](https://github.com/apollographql/router/tree/dev/examples)
e as regras de lint do repositório do Apollo Router:

- Uma lib com o plugin e um binário que só a linka (`use subscription_snapshot as _;`
  basta para o `register_plugin!` valer), com os testes de integração em `tests/`.
- `Plugin` com `type Config` (`Deserialize` + `JsonSchema`, `deny_unknown_fields`;
  os doc comments viram a descrição no JSON schema da config) e hook montado com
  `ServiceBuilder` e as layers do `apollo_router::layers`.
- Edition 2024 e o mesmo `rustfmt.toml` (`style_edition = "2024"`), com um import
  por linha, agrupados em std, crates externos e o próprio crate.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` e `cargo doc`
  com `RUSTDOCFLAGS=-D warnings`, como o `xtask lint` da Apollo; o CI roda os três.
- Testes do hook com `tower_test::mock::pair` (o substituto que a Apollo indica para
  os `Mock*Service`, obsoletos) e do registro com o `TestHarness`.
- Código, comentários e mensagens em inglês.

## Suporte e atualizações

A Apollo não dá suporte a plugins nativos nem a binários customizados. Cada versão
nova do Router exige recompilar e rodar `npm run test:router-harness`.
[`docs/router-upgrades.md`](docs/router-upgrades.md) compara 20 versões
(1.61 a 2.18) e mostra o que costuma quebrar:
- **O código do plugin:** raramente. Ele compilou e passou em todos os testes na
  2.18.0 sem mudanças.
- **Em volta dele:** com frequência. A versão do Rust, a config YAML do Router (que
  não serve para duas versões ao mesmo tempo) e dependências como o `schemars`.

O guia também traz o passo a passo de atualização.
