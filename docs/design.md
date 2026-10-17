# Desenho: placar e estatísticas ao vivo

## Resumo

- **Dois canais, duas subscriptions:** `scoreboardUpdates(matchId)` (subgraph
  `scores`) e `statsUpdates(matchId)` (subgraph `stats`).
- **Todo frame traz o estado inteiro e uma versão:** `STATE { version, state }`.
  O cliente fica com o de maior versão. Não há lacuna, histórico, patch nem
  query de recuperação.
- **Cada canal tem o seu ritmo.** O placar envia a cada commit, na hora. As
  estatísticas passam por um limitador: na hora quando o jogo está calmo, e no
  máximo 1 frame a cada 500 ms numa rajada. Os dois canais também mandam um
  frame periódico.
- **As subscriptions só têm `matchId`:** o Router deduplica, com um upstream por
  canal e partida em cada instância.
- **Retenção do último frame** (plugin nativo do Router em `router-plugin/`, e o
  mesmo comportamento no emulador `scripts/edge.mjs`): quem entra num upstream já
  aberto recebe o estado na hora, em vez de esperar o frame periódico.
- **Várias instâncias:** cada uma deduplica e retém só os seus upstreams, e isso
  basta. Ninguém precisa compartilhar o último estado.

## Por que dois canais

Os experimentos (`tests/experiments.test.mjs`) mostraram que o motivo principal
não é o óbvio. O que protege o gol de uma rajada de estatísticas é **limitar a
frequência** das estatísticas, não separar os canais. Separar vale porque:

1. **Cada tela recebe só o que mostra.** Uma lista de jogos assina só o placar e
   recebe ~300–470 B/s por partida. Num stream único receberia também as
   estatísticas.
2. **Cada canal tem a sua política:** o placar sai na hora, as estatísticas têm
   o limitador.
3. **Versões, caches e falhas independentes.**
4. **Donos diferentes:** são subgraphs diferentes, e o Router faz passthrough
   para os dois.

| | Placar | Estatísticas |
| --- | --- | --- |
| Muda | poucas vezes por partida | ~20 lances/s, rajadas de 200/s |
| Tamanho do estado | ~150 B | ~1–2 KB (2 times + 22 jogadores) |
| Envio | um `STATE` a cada commit, na hora | `STATE` com limitador de 500 ms |
| Periódico | `STATE` a cada 1 s | `STATE` a cada 1 s |
| Fila de assinante lento | só o mais novo | só o mais novo |
| Snapshot cacheável (só para o primeiro desenho) | `scoreboard(minVersion)`: 2 s | `matchStats(minVersion)`: 5 s |

O relógio não é enviado a cada segundo: `Clock { running, seconds, updatedAt }`,
e o cliente avança sozinho.

## Por que estado inteiro, e não patch

Os experimentos mediram cinco políticas para as estatísticas (60 s, 20 lances/s;
gol p95 com 30 gols durante uma rajada num enlace de 16 KB/s):

| Política | B/s por cliente | Gol p95 no mesmo enlace |
| --- | --- | --- |
| estado inteiro a cada lance | 26.707 | 74 s |
| patch a cada lance | 5.866 | 7,7 s |
| patch a cada 500 ms | 2.135 | 170 ms |
| o menor (patch ou estado) a cada 500 ms | 2.132 | 80 ms |
| **estado inteiro a cada 500 ms (adotado, com limitador)** | **2.797** | **80 ms** |

**O limitador é indispensável.** Sem ele, estado inteiro a cada lance satura o
enlace e o gol espera dezenas de segundos.

**Com o limitador, o estado inteiro custa um pouco mais de banda que o patch:**
+31% nesse experimento, que começa com o estado vazio. No benchmark, em que todos
os jogadores já estão no estado, foram 4.441 contra 2.423 B/s, quase 2×. Em
troca, o desenho fica bem mais simples:

- **Sem lacuna nem recuperação:** um frame perdido é corrigido pelo próximo; não
  existem `from`, `CHECKPOINT` nem snapshot de recuperação.
- **Retenção possível:** o último frame pode ser retido e reenviado por quem está
  no meio (Router, edge). Reenviar um patch antigo não serve para nada.
- **Cliente com uma regra só,** igual nos dois canais (`StateSync`).

Se o estado crescer muito (dezenas de métricas por jogador, mapa de calor) e a
banda dos clientes móveis pesar, o patch volta a compensar. O hub mantém o modo
`delta` para que essa comparação continue reproduzível.

### O limitador

O hub, no modo `state` com `throttleMs`:
- **Calmo:** se nada saiu nos últimos 500 ms, o frame sai na hora (um lance
  isolado chega em ms).
- **Rajada:** senão, sai no fim da janela, com o estado mais recente. Na rajada,
  isso dá no máximo 2 frames/s.
- **`committedAt`:** é o do commit mais antigo ainda não enviado, então a
  latência medida é a do pior dado do frame.

## Cliente (`StateSync`)

1. Abre a subscription (deduplicada). Em paralelo, busca opcionalmente a base
   `minVersion: 0`, possivelmente antiga, só para desenhar a tela na hora como
   provisória.
2. A cada frame: se a versão é maior que a local, substitui o estado; se não,
   ignora. Isso cobre frames retidos mais velhos, periódicos atrasados e
   duplicatas.
3. Fica sincronizado no primeiro frame de cada conexão.
4. Queda ou 5 s sem frames: reconecta mantendo o estado. O próximo frame resolve.

## Retenção do último frame (plugin do Router)

O Router não tem configuração para reenviar o último evento a quem entra num
upstream deduplicado:
- **Rhai:** não guarda estado entre requisições.
- **Coprocessor:** não cria um frame quando o cliente se conecta.

Por isso é um **plugin nativo em Rust** num build customizado
(`router-plugin/`, `poc.retain_last_frame`, apollo-router 2.10.5).
`scripts/edge.mjs` faz o mesmo no emulador.

- **Retenção:** guarda o último evento (com dados e sem erros) de cada chave e o
  entrega primeiro a quem entra. O frame retido some quando o último assinante da
  chave sai, junto com o upstream.
- **Onde o frame entra:** no `supergraph_service`, **depois** do primeiro item do
  stream. Numa subscription, o primeiro item é um marcador sem dados que o router
  service consome para abrir o multipart e não envia. Inserido antes dele, o frame
  retido é engolido e o cliente recebe `{"data":null}`; o teste de integração
  falha exatamente assim quando a ordem é invertida.
- **Chave com identidade:** os headers configurados (ex.: `authorization`) entram
  na chave de retenção. No Router, o mesmo header é propagado ao subgraph, então a
  deduplicação também separa por identidade. Clientes com identidades diferentes
  não compartilham upstream nem frame retido.
- **Marcação:** o frame retido sai com `extensions.retainedFrame: true`, usado só
  pelos testes.
- **Correção:** só funciona porque todo frame traz o estado inteiro e uma versão.
  Um frame retido mais velho que um em trânsito é ignorado pelo cliente.

Medido no pipeline do Router (`npm run test:router-harness`, frame periódico de
3 s): o late joiner recebeu o estado em **1 ms com o plugin, contra 2.799 ms
sem**, com um único stream no subgraph para os dois clientes. No emulador
(`tests/scale.test.mjs`, periódico de 2 s): 12 ms contra 1.997 ms.

## Várias instâncias

```
clientes ──LB──┬── instância A ─ 1 upstream por (canal, partida) que A tem em uso ─┐
               ├── instância B ─ idem                                               ├── subgraphs
               └── instância C ─ idem                                               ┘
```

- **Deduplicação por instância:** os streams no subgraph são **instâncias ×
  partidas ativas × canais**, não clientes. Medido: 30 clientes em 2 partidas
  atrás de 3 instâncias geraram 6 streams por canal.
- **Retenção por instância, sem compartilhar:** o frame retido existe exatamente
  quando é necessário. Quem é o primeiro numa instância abre o upstream e recebe o
  frame inicial do subgraph; quem entra num upstream já aberto recebe o frame que
  essa instância já guardou.
- **Troca de instância (reconexão pelo LB):** a nova instância pode ter um frame
  retido um pouco mais velho que o do cliente. A versão impede a regressão; o
  teste de propriedade e o teste de queda afirmam isso.
- **Queda de uma instância:** os clientes reconectam pelo balanceador e caem numa
  instância viva, que já tem o frame retido. Medido: todos sincronizados de novo
  em ~150 ms, sem regressão de versão.
- **Sem afinidade de sessão.**
- **O que é compartilhado:** o response cache das queries (Redis, no Router), já
  que o contrato `minVersion` é verdadeiro em qualquer instância. E, se os
  subgraphs tiverem réplicas, um bus que leve cada commit aos hubs de todas elas.
  O frame periódico lê o estado do banco e corrige perdas desse bus.

## Consistência entre canais

Cada canal é exato numa versão; entre canais não há transação. As estatísticas
podem estar até ~0,5 s atrás do placar. Por isso gols ficam só no placar.

## Limites

- **Apollo Router com licença não executado aqui:** o plugin rodou no pipeline do
  Router pelo `TestHarness` do crate (deduplicação, WebSocket e multipart reais,
  uma instância). Várias instâncias do binário com GraphOS atrás de um
  balanceador estão prontas (`npm run test:router-real`), mas dependem da chave;
  o response cache com Redis (`--profile apollo`) também.
- **Plugin sem suporte da Apollo:** é preciso recompilar a cada versão do Router e
  acompanhar a forma interna do stream de subscription.
- **Identidade:** o edge usa a identidade só na chave. Num Router real, a
  autorização precisa valer no subgraph também.
- **Lista com muitas partidas:** um canal por competição reduziria o número de
  subscriptions.

## Evolução neste branch

1. `15a917e`: feed de pedidos com `after` na subscription, incompatível com a
   deduplicação.
2. `0f06630`: stream compartilhado + log de eventos paginado e cacheável.
3. `9a87874`: domínio esportivo, dois canais, patch adaptativo nas estatísticas.
4. `76ef488`: estado inteiro com limitador nos dois canais, retenção do último
   frame e várias instâncias atrás de um balanceador (emulados).
5. Atual: o plugin de retenção em Rust, testado no pipeline do Router, e o perfil
   com 3 routers customizados atrás de um balanceador.
