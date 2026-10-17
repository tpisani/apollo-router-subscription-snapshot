# Desenho: placar e estatísticas ao vivo

## A pergunta

Placar e estatísticas têm frequências de atualização diferentes. Devem ser
subscriptions diferentes?

**Sim.** Mas os experimentos mostraram que o motivo principal não é o que parece à
primeira vista. Separar não é o que impede uma rajada de estatísticas de atrasar o
gol: quem faz isso é agrupar as estatísticas por janela. Separar vale porque:

1. **Cada tela recebe só o que mostra.** Uma lista de jogos assina só o placar e
   recebe ~290–440 B/s por partida. Num stream único receberia também as
   estatísticas: ~2.400 B/s, **8× mais**, para não exibir nada disso.
2. **Cada canal tem a sua política de envio.** O placar sai a cada commit, na
   hora, com o estado inteiro. As estatísticas saem uma vez por janela de 500 ms,
   com o menor entre o patch e o estado. Num stream único, uma política serviria
   mal um dos dois.
3. **Versões, caches e falhas independentes.** Cada canal tem o seu contador de
   versão, o seu snapshot cacheável (com TTL próprio) e a sua recuperação.
   Perder um frame de estatísticas pede um snapshot de estatísticas e não toca
   no placar. O teste de ponta a ponta afirma isso: zero recuperações no placar
   durante a rajada.
4. **Donos diferentes.** São subgraphs diferentes (`scores` e `stats`), como
   costumam ser os provedores e times reais. Cada um declara a sua subscription;
   o Router faz passthrough para os dois.

Uma operação GraphQL de subscription tem um único campo raiz, então "uma
subscription com tudo" exigiria um campo com união de tipos de frame. As
variáveis seriam as mesmas para todos e a deduplicação continuaria funcionando,
mas os quatro pontos acima se perderiam.

## Os canais

| | Placar (`scoreboardUpdates`) | Estatísticas (`statsUpdates`) |
| --- | --- | --- |
| Subgraph | `scores` | `stats` |
| Muda | poucas vezes por partida (gol, início, intervalo, fim) | ~20 vezes/s, rajadas de 200/s |
| Tamanho | ~150 B | ~1–2 KB (2 times + 22 jogadores) |
| Urgência | máxima (gol) | tolera ~0,5 s |
| Frame | `STATE`: o estado inteiro, a cada commit | um por janela de 500 ms: `DELTA` (valores finais dos campos alterados) ou `STATE`, o que for menor |
| Periódico | `STATE` a cada 1 s | `CHECKPOINT { version }` a cada 1 s |
| Fila de um assinante lento | só o `STATE` mais novo | `DELTA`s fundidos num só; `STATE` supera tudo |
| Recuperação | nenhuma: o próximo frame resolve | `matchStats(minVersion: v)` |
| Snapshot cacheável | `scoreboard(minVersion)`: 2 s na chave 0, 60 s com versão | `matchStats(minVersion)`: 5 s na chave 0, 60 s com versão |

Dados que não mudam durante o jogo (times, elencos) ficam no subgraph `catalog`,
com cache de 1 h, fora dos canais ao vivo.

### O relógio não é enviado a cada segundo

`Clock { running, seconds, updatedAt }`: `seconds` vale no instante
`updatedAt`; enquanto `running`, o cliente avança sozinho. Só início, pausa e
fim geram commit. Um relógio enviado por push seria 1 frame/s por cliente para
um dado que o cliente calcula.

## Protocolo de cada canal

As subscriptions têm só `matchId` como variável: todos os clientes da partida
mandam a mesma operação e o Router deduplica (um upstream por canal e partida).
Ninguém recebe um frame inicial próprio. Como não há histórico, a recuperação é
sempre um snapshot com versão mínima.

### Placar (`StateSync`)

- Cada frame `STATE` traz o estado inteiro e a versão. O cliente fica com o de
  maior versão e ignora os outros.
- **Late joiner:** mostra a base cacheada (`minVersion: 0`) como provisória;
  o primeiro frame, um commit ou o periódico de 1 s, já é o estado correto.
  **Nenhuma query de recuperação.**
- **Perda, duplicata, reordenação, reconexão:** o próximo frame resolve.

### Estatísticas (`DeltaSync`)

- **`DELTA { from, version, patch }`:** aplicável quando
  `from ≤ versão local < version`. Os patches levam valores finais, nunca
  incrementos, então um patch que cobre `(from, version]` aplicado sobre o
  estado em qualquer versão desse intervalo dá o estado exato em `version`. Um
  snapshot tirado no meio da janela continua aplicável.
- **`STATE { version, state }`:** substitui tudo, como no placar. O hub manda
  `STATE` quando o estado inteiro é menor que o patch da janela.
- **`CHECKPOINT { version }`:** até onde o **stream** entregou, não o head do
  banco. Um commit ainda dentro da janela não pode fazer o cliente achar que
  perdeu algo; o teste "sem reseed espúrio" cobre isso.
- **`DELTA` com `from` maior que a versão local, ou `CHECKPOINT` acima dela:**
  faltou algo. O cliente pede `matchStats(minVersion: version)`, uma chave nova,
  e substitui o estado.

### Por que continua correto com cache e deduplicação

- O "agora" vem do stream. Uma query responde "um estado em alguma versão ≥
  minVersion", e isso continua verdadeiro para sempre; qualquer cache pode
  servi-la.
- Clientes que entram juntos recebem o mesmo frame, pedem o mesmo `minVersion`
  e compartilham a chave de cache.
- O cliente rejeita um snapshot abaixo do `minVersion` pedido (um cache quebrado).

## O que os experimentos mostraram

Os experimentos estão em `tests/experiments.test.mjs`: hubs reais, tempo
simulado e 60 s de partida a 20 lances/s.

**E1, banda por cliente (bytes/s no formato multipart do Router):**

| Política das estatísticas | frames/s | B/s |
| --- | --- | --- |
| estado inteiro a cada lance | 20,7 | 26.707 |
| patch a cada lance | 20,7 | 5.866 |
| estado inteiro a cada 500 ms | 3 | 2.797 |
| patch a cada 500 ms | 3 | 2.135 |
| **o menor por janela de 500 ms (adotado)** | **3** | **2.132** |

O placar: 1 frame/s, 292 B/s.

**E2, isolamento:** 30 gols durante uma rajada de 200 lances/s, num enlace de
16 KB/s (celular com sinal ruim), modelo FIFO:

| No enlace | gol p50 | gol p95 | gol máx |
| --- | --- | --- | --- |
| só o placar | 18 ms | 18 ms | 35 ms |
| placar + estado inteiro a cada lance | 53 s | 74 s | 75 s |
| placar + patch a cada lance | 4,2 s | 7,7 s | 8,0 s |
| placar + patch a cada 500 ms | 18 ms | 170 ms | 176 ms |
| placar + estado inteiro a cada 500 ms | 18 ms | 80 ms | 80 ms |
| **placar + o menor por janela (adotado)** | **18 ms** | **80 ms** | **80 ms** |

Conclusões:

- **Agrupar por janela é o que protege o gol.** Mandar estatísticas a cada
  lance satura o enlace, e o gol espera segundos atrás delas, com stream único ou
  separado, se o mesmo cliente assina as duas no mesmo enlace.
- **Na rajada, o patch fica maior que o estado.** Quando quase todos os campos
  mudam numa janela, a lista `{path, value}` é mais verbosa que o estado inteiro
  (p95 170 ms contra 80 ms). Daí a política adotada: por janela, envia o menor.
  Em jogo normal ela tem o tamanho do patch; na rajada, o do estado.
- **Separar elimina o atraso para quem não precisa de estatísticas** (18 ms).
  Para quem precisa das duas no mesmo enlace, o que limita o atraso é a política
  das estatísticas, não a separação.

**Sistema real** (`npm run bench`, pelo edge, 150 clientes, rajada de 200
lances/s): placar p50 10 ms / p99 20 ms, estatísticas p50 508 ms, um stream por
canal no subgraph para todos os clientes, nenhum snapshot durante o jogo, e os
clientes só de placar receberam 0 B de estatísticas.

## Consistência entre canais

Cada canal é consistente consigo mesmo: estado exato numa versão. Entre canais
não há transação: logo depois de um gol, a tela pode mostrar o placar novo e as
estatísticas da janela anterior por até ~0,5 s. Por isso o gol fica só no placar
e as estatísticas não contam gols. Se uma tela precisar de um número que dependa
dos dois, ou ele pertence a um único canal, ou a tela aceita essa defasagem.

## Limites e próximos passos

- **Lista de jogos com muitas partidas:** hoje são N subscriptions de placar (uma
  por partida, todas deduplicadas). Para dezenas de partidas, um canal por
  competição (`liveScores(competitionId)`) com o mesmo protocolo de `STATE` por
  partida reduz conexões.
- **Autorização:** `matchId` é só um seletor; os headers entram na chave de
  deduplicação do Router.
- **Réplicas:** cada subgraph tem um hub em memória; com réplicas, o hub precisa
  ser alimentado por um bus (Redis, NATS) depois do commit. A versão e o
  snapshot com `minVersion` cobrem perdas desse bus.
- **Apollo Router:** a config (`router/router.yaml`) foi validada com o binário
  2.10, mas não executada (exige licença GraphOS). A deduplicação e o cache foram
  emulados (`scripts/edge.mjs`); `npm run test:router` roda o mesmo cenário contra
  um Router licenciado.

## Evolução neste branch

1. `15a917e`: feed de pedidos com `after` na subscription (incompatível com a
   deduplicação).
2. `0f06630`: stream compartilhado + log de eventos paginado e cacheável.
3. Atual: domínio esportivo, dois canais com políticas próprias, só snapshots
   com versão mínima (sem histórico), frames adaptativos nas estatísticas.
