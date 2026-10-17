# Router customizado com `poc.retain_last_frame`

Apollo Router 2.10.5 com um plugin nativo que guarda o último evento de cada
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

Precisa de Rust 1.94.1+ (exigido pelas dependências do `Cargo.lock`; testado com 1.97) e `protoc` (`apt-get install protobuf-compiler`). A primeira
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
3. Suba o perfil e rode o roteiro:

```bash
docker compose --profile apollo-plugin up --build -d
npm run test:router-scale
# com uma instância derrubada no meio do jogo:
KILL_CMD="docker compose --profile apollo-plugin stop router-b" npm run test:router-scale
```

O perfil sobe:

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

## Suporte

A Apollo não dá suporte a plugins nativos nem a binários customizados. Manter o
plugin significa recompilar a cada versão do Router e acompanhar mudanças internas,
como a forma do stream da subscription descrita acima.
