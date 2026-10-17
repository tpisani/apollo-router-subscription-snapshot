# Validação realizada

Ambiente: Linux, Node.js 22.22.0, Hive Gateway 2.15.2, Apollo Router 2.10.0
(só validação de config), Docker 29.8.

## Testes (`npm test`: 30 aprovados)

- **`tests/domain.test.mjs` (8):**
  - patches, incluindo a **propriedade de sobreposição**: 500 janelas
    aleatórias `(from, to]` aplicadas sobre estados em versões intermediárias
    dão o estado exato em `to`, e reaplicar não muda nada;
  - compactação ao fundir patches;
  - versão só quando o estado muda;
  - atomicidade sem publicação e persistência;
  - `SnapshotCache` com `minVersion`;
  - relógio derivado no cliente;
  - incrementos da ingestão virando valores finais.
- **`tests/channels.test.mjs` (9):**
  - placar: `STATE` imediato, periódico e só o mais novo para quem está lento;
  - estatísticas: 200 commits viram um `DELTA` com no máximo uma entrada por
    campo, janelas contíguas;
  - `CHECKPOINT` com o entregue e não o head (sem reseed espúrio);
  - fusão sem perda na fila;
  - escolha adaptativa (`DELTA` em jogo normal, `STATE` na rajada) e `STATE`
    superando a fila;
  - liberação de listeners e timers.
- **`tests/sync.test.mjs` (7):**
  - late joiner de placar certo no primeiro frame;
  - frames de placar fora de ordem ignorados;
  - estatísticas com base antiga mais `DELTA` adiantado (um snapshot com
    `minVersion`) e snapshot no meio da janela aplicado sem query;
  - cache que viola o contrato rejeitado;
  - render provisório;
  - **teste de propriedade:** 25 sementes com perda, duplicação, reordenação,
    frames `STATE`/`DELTA`, joins tardios, reconexões e cache eterno. Injetar
    um bug (aplicar `DELTA` com lacuna) o faz falhar na semente 2.
- **`tests/experiments.test.mjs` (2):** E1 (banda) e E2 (isolamento num enlace
  limitado); números em `docs/design.md`.
- **`tests/e2e.test.mjs` (4), com o processo real do Hive Gateway:**
  - cenário completo (`scripts/verify.mjs`) pelo edge, com deduplicação e cache
    de tudo:
    - caches antigos confirmados;
    - placar sincronizado sem recuperação;
    - um stream por canal no subgraph;
    - rajada de 600 lances com gol no meio sem afetar o placar e em menos de 30
      frames;
    - cliente só de placar com 0 B de estatísticas;
    - retomada com no máximo um snapshot;
    - todos iguais ao banco;
  - o mesmo cenário direto no Hive (graphql-ws);
  - 20 clientes em 2 partidas → exatamente 2 upstreams de placar e 2 de
    estatísticas;
  - reinício dos subgraphs com os dois canais se recuperando.

Bugs que os testes encontraram durante o desenvolvimento, já corrigidos:
- `applyPatch` compartilhava objetos com o patch, o que corrompia estados
  (pego pela propriedade de sobreposição);
- o patch fundido acumulava entradas redundantes de jogador novo (pego pelo
  limite de tamanho);
- o patch é maior que o estado durante rajadas (pego pelo experimento E2), o
  que levou ao frame adaptativo.

## Docker Compose

Subgraphs (com `SIMULATE=m1`) + Hive Gateway + edge em containers:
- `npm run client`: relógio avançando no cliente, gols chegando em 2–4 ms;
- `CHANNELS=scoreboard npm run client`;
- `scripts/verify.mjs` contra `localhost:4010`: passou.

No simulador, 690 commits de estatísticas viraram 31 frames. Neste sandbox a
imagem foi construída com o CA do proxy local injetado fora do repositório.

`router config validate router/router.yaml` com `ghcr.io/apollographql/router:v2.10.0`:
válido. `docker compose --profile apollo config`: válido.

## Benchmark (`npm run bench`, 10 s, pelo edge)

100 clientes só de placar + 50 com placar e estatísticas; 20 lances/s com uma
rajada de 200 lances/s; um gol a cada 2 s.

| Métrica | Valor |
| --- | --- |
| Convergiram para a última versão | 150/150 |
| Latência do placar | p50 10 ms, p99 20 ms, máx 21 ms |
| Latência das estatísticas | p50 508 ms, p99 527 ms (janela de 500 ms) |
| Streams no subgraph | 1 de placar, 1 de estatísticas |
| Frames emitidos | 15 de placar, 30 de estatísticas (614 lances) |
| Snapshots durante o jogo | 0 |
| Cliente só de placar | 444 B/s de placar, 0 de estatísticas |
| Cliente com os dois | 444 B/s de placar, 2.423 B/s de estatísticas |

Subgraphs e clientes rodam no mesmo processo: os números servem para comparar,
não como capacidade.

## Não verificado

- Apollo Router em execução: sem licença GraphOS o binário recusa iniciar
  ("license violation: Federated subscriptions"). Rode `npm run test:router`
  num ambiente licenciado.
- Bus distribuído e réplicas dos subgraphs.
- O modelo de enlace do E2 é uma simulação FIFO de banda fixa, não uma rede real.
