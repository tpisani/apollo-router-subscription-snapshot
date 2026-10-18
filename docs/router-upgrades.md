# O plugin e as versões do Apollo Router

Pergunta: com que frequência uma versão nova do Router quebra o
`subscription_snapshot`?

Método (out/2026):
- **Comparação estática:** o código-fonte publicado de 20 versões (1.61.13 e o
  último patch de cada minor de 2.0 a 2.18.0), conferindo cada ponto de que o
  plugin depende.
- **Teste prático:** o plugin, sem mudar nada, compilado contra a 2.18.0 e testado
  (unidade, pipeline do Router e o Router licenciado em 3 instâncias).

## Ritmo de versões

- Um minor novo a cada ~4 semanas: 2.10 (dez/2025) a 2.18 (set/2026), oito minors
  em dez meses.
- A 2.10 ainda recebe patches (2.10.5 em jul/2026). Este POC usou a 2.10.5 até
  ser atualizado para a 2.18.0 (a estável mais recente em out/2026).
- A versão mínima do Rust subiu 8 vezes entre 2.0 e 2.18 (a 2.18 exige 1.97.1).

## O que mudou, ponto a ponto

| Ponto de que o plugin depende | Mudou em 2.x? | Efeito no plugin |
| --- | --- | --- |
| `trait Plugin`, `PluginInit`, `supergraph::Response::map`, `map_future_with_request_data` | não (iguais desde 1.61) | nenhum |
| `graphql::Response` (`subscribed`, `extensions`, `data`, `errors`) | não | nenhum |
| `register_plugin!` | 2.0 e 2.1 (por dentro) | nenhum no uso |
| Execution service marca o 1º item (`first.subscribed`) | não | nenhum |
| Router service consome o 1º item e abre o multipart só com o resto | código mexido em 2.2.1, 2.4.0 e 2.14.2; **a regra continua a mesma** | nenhum (conferido em 2.18) |
| Chave `apollo::supergraph::operation_kind` no contexto | renomeada na 2.0 (na 1.x era `operation_kind`) | na 1.x o plugin não reconheceria subscriptions e deixaria de reter, **sem erro** |
| Dependência `schemars` (o `JsonSchema` da config do plugin) | 0.8 → 1.0 na 2.7.0 | **não compila** até trocar a versão no `Cargo.toml` |
| Versão mínima do Rust | 8 vezes | o build falha até atualizar o toolchain (aconteceu aqui: 1.97.0 → 1.97.1 para a 2.18) |
| Multipart | grande na 2.6.2 e depois (métricas de motivo de fim) | nenhum |

**Fora do plugin, mas nos arquivos deste POC** (a config YAML do Router):

| Versão | Mudança | Efeito |
| --- | --- | --- |
| 2.3.0 | `subscription.enable_deduplication` → `subscription.deduplication.enabled` | config antiga inválida |
| 2.15.0 | `deduplication.enabled` → `deduplication.all.enabled` | o Router **se recusa a iniciar** com a config antiga |
| 2.18.0 | `headers.all.request` passa a exigir `operations:` | config antiga inválida |

`router config upgrade` reescreve as três sozinho. Mas a config migrada para a
2.18 é rejeitada pela 2.10.5, então **não existe uma config que sirva às duas
versões**: ela tem de acompanhar o binário.

## Resultado do teste prático na 2.18.0

- **Plugin:** compilou sem mudar nenhuma linha (só o Rust 1.97.1).
- **Testes:** os 8 de unidade e os 4 no pipeline do Router passaram. Com retenção,
  o cliente que entra depois recebe o estado em 1 ms; sem, em 2.799 ms, iguais à
  2.10.5.
- **Config:** `router config validate` reprovou as duas configs do POC (o
  `headers`). Com `router config upgrade`, passaram.
- **Router licenciado** (3 instâncias + 1 sem plugin, passthrough e callback):
  - todos os itens do roteiro passaram, inclusive o ciclo de vida e a queda de
    instância (143 ms);
  - o upstream duplicado da deduplicação do Router apareceu de novo, desta vez
    **também no modo callback**; continua não afetando a correção.

## Conclusão

- **O código do plugin raramente quebra.** A API de plugins e a forma do stream de
  subscription ficaram estáveis em toda a linha 2.x. A única quebra de compilação
  do próprio plugin foi a troca do `schemars` (2.7.0); o resto mudou só em versões
  major (1.x → 2.0).
- **O que quebra com frequência fica em volta dele:**
  - a versão do Rust, várias vezes por ano;
  - a config YAML do Router, 3 vezes desde a 2.3, duas delas impedindo o Router
    de iniciar;
  - dependências transitivas, raramente.

  Tudo isso é barulhento: falha no build ou na inicialização, e não em produção.
- **O risco silencioso é o contrato interno do stream** (o marcador no 1º item e a
  chave `operation_kind`). Ele não é documentado nem garantido pela Apollo. Se mudar,
  o plugin pode deixar de reter, como aconteceria na 1.x, ou perder o frame, como no
  teste com a ordem invertida, sem nenhum erro de compilação. Não mudou em 2.x, e
  `npm run test:router-harness` existe para pegar isso.

## A atualização para a 2.18.0

Feita como descrito abaixo, com o resultado:
- **Código do plugin:** nenhuma mudança.
- **Dependência e toolchain:** `apollo-router = "=2.18.0"` e um `Cargo.lock` novo;
  Rust fixado em 1.97.1 (`rust-toolchain.toml` e
  `rust:1.97.1-bookworm` no Dockerfile).
- **Configs:** `router.yaml`, `router-plugin.yaml` e `router-plugin-callback.yaml`
  migradas à mão, para manter os comentários: `deduplication.all.enabled` e
  `headers.all.request.operations`. Com isso, `router config upgrade` não tem mais
  nada a migrar. A imagem oficial do perfil `apollo` também foi para a v2.18.0.
- **Testes:** os mesmos de antes, todos passando (ver [validação](../testbed/docs/validation.md)).

## Como atualizar o Router

1. **Trocar a versão:** `apollo-router = "=X.Y.Z"` no `Cargo.toml`.
   Ver a versão mínima do Rust (`rust-version` do crate) e a do
   `Dockerfile`.
2. **Compilar:** `cargo build --release`. Se quebrar, o mais provável é uma
   dependência como o `schemars`; alinhar com a do Router.
3. **Migrar a config:** `./target/release/router config upgrade examples/router/router-plugin.yaml`
   (e a de callback), revisar o resultado e validar com `config validate`.
4. **Testar no pipeline do Router:** `npm run test:router-harness` (em `testbed/`). É o que pega
   mudança no contrato do stream.
5. **Com a chave do GraphOS:** `npm run test:router-real` e
   `MODE=callback npm run test:router-real` (em `testbed/`).

Para testar uma versão nova sem mexer na pasta do plugin:

```bash
# ex.: da 2.18.0 para X.Y.Z, com o Rust mínimo dela (R); na raiz do repositório
mkdir /tmp/plugin-novo && cp -r Cargo.toml Cargo.lock src tests /tmp/plugin-novo/
sed -i 's/=2.18.0/=X.Y.Z/' /tmp/plugin-novo/Cargo.toml
(cd testbed && PLUGIN_DIR=/tmp/plugin-novo CARGO_TOOLCHAIN=R npm run test:router-harness)
(cd /tmp/plugin-novo && cargo +R build --release)
mkdir -p /tmp/cfg && /tmp/plugin-novo/target/release/router config upgrade examples/router/router-plugin.yaml > /tmp/cfg/router-plugin.yaml   # idem callback
(cd testbed && ROUTER_BIN=/tmp/plugin-novo/target/release/router ROUTER_CONFIG_DIR=/tmp/cfg npm run test:router-real)
```

Atenção: `config upgrade` escreve os avisos no mesmo stdout do YAML; remova as
linhas que começam com `{"timestamp"` antes de usar.
