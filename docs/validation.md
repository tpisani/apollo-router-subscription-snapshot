# Validação realizada

Ambiente: Linux, Node.js 22.22.0, Hive Gateway 2.15.2, Apollo Router 2.10.0
(só validação de config), Docker 29.8.

## Testes (`npm test`: 35 aprovados, 3 execuções seguidas)

- **`tests/domain.test.mjs` (8):**
  - patches e a propriedade de sobreposição (usados no modo `delta` dos
    experimentos);
  - versão só quando o estado muda;
  - atomicidade sem publicação e persistência;
  - `SnapshotCache` com `minVersion`;
  - relógio derivado no cliente;
  - estatísticas como valores finais.
- **`tests/channels.test.mjs` (10):**
  - `STATE` imediato, periódico, e só o mais novo para quem está lento;
  - **limitador:** lance isolado sai na hora; 100 commits em ~100 ms viram no
    máximo 3 frames, com o estado final; `committedAt` é o do commit mais antigo
    ainda não enviado;
  - liberação de listeners e timers;
  - modo `delta`, usado nos experimentos de comparação.
- **`tests/sync.test.mjs` (6):**
  - late joiner certo no primeiro frame;
  - estatísticas convertidas do formato da API;
  - frames repetidos, atrasados, fora de ordem ou retidos mais velhos ignorados;
  - render provisório;
  - nova tentativa após falha da base;
  - **teste de propriedade:** 25 sementes, 3 instâncias com frames retidos de
    idades diferentes, clientes trocando de instância, frames perdidos,
    duplicados e reordenados. A versão nunca regride e todos terminam no estado
    exato. Injetar um bug (aceitar frame mais velho) o faz falhar na semente 1.
- **`tests/experiments.test.mjs` (2):** banda e isolamento das cinco políticas
  de estatísticas (números em `docs/design.md`).
- **`tests/e2e.test.mjs` (4), com o processo real do Hive Gateway:**
  - cenário completo (`scripts/verify.mjs`) pelo edge: caches antigos, um stream
    por canal, rajada com gol no meio, cliente só de placar com 0 B de
    estatísticas, **retomada sem nenhuma query** e todos iguais ao banco;
  - o mesmo cenário direto no Hive;
  - upstreams por canal e partida;
  - reinício dos subgraphs.
- **`tests/scale.test.mjs` (5), com 3 edges atrás de um balanceador:**

| Teste | Resultado |
| --- | --- |
| 30 clientes, 2 partidas, 2 canais | 6 streams por canal no subgraph (3 instâncias × 2 partidas); máximo 1 upstream por partida em cada instância |
| Late joiner numa partida parada (periódico de 2 s) | **12 ms com retenção**, 1.997 ms sem |
| Identidade na chave (`authorization`) | tokens diferentes: 2 upstreams; só o cliente com o mesmo token recebeu o frame retido |
| Instância derrubada no meio do jogo (12 clientes, lances e gols contínuos) | 8 clientes migraram pelo balanceador; todos sincronizados de novo em **145–163 ms** (3 rodadas); nenhuma versão regrediu; estado final exato |
| Limpeza | sem assinantes, nenhuma instância guarda upstream nem frame retido; subgraph sem streams |

## Benchmark (`npm run bench`, 10 s)

100 clientes só de placar + 50 com os dois canais; 20 lances/s com uma rajada
de 200 lances/s; um gol a cada 2 s; o jogo já em andamento (400 lances antes).

| | 1 instância | 3 instâncias (`EDGES=3`) |
| --- | --- | --- |
| Convergiram | 150/150 | 150/150 |
| Placar p50 / p99 | 10 / 19 ms | 14 / 21 ms |
| Estatísticas p50 / p99 | 478 / 512 ms | 482 / 505 ms |
| Streams no subgraph por canal | 1 | 3 |
| Entradas servidas com frame retido | 169 | 184 (61 + 62 + 61) |
| Snapshots durante o jogo | 0 | 0 |
| Cliente só de placar | 443 B/s, 0 de estatísticas | 471 B/s, 0 de estatísticas |
| Cliente com os dois canais | + 4.441 B/s de estatísticas | + 4.433 B/s |

Na versão anterior, com patch adaptativo, o cliente completo recebia ~2.423 B/s
de estatísticas no mesmo benchmark. O estado inteiro custa ~1,8× em banda aqui.

## Docker e Router

- Compose (subgraphs com simulador + Hive + edge com retenção) com o código
  atual: `scripts/verify.mjs` contra `localhost:4010` passou, e `npm run client`
  mostrou placar e estatísticas ao vivo, com 3 entradas servidas por frame
  retido. `RETAIN=0` desliga a retenção.
- `router config validate router/router.yaml` com a imagem oficial 2.10.0:
  válido.

## Não verificado

- **Apollo Router em execução:** sem licença, ele recusa iniciar. A
  deduplicação, o cache e a **retenção** foram emulados; o plugin em Rust foi
  só esboçado.
- **Balanceador real** (ALB, NGINX, Envoy): o do teste é um round-robin simples
  com streaming. Num LB real, confira o timeout de ociosidade (maior que o frame
  periódico) e que as respostas multipart não são bufferizadas.
- **Réplicas dos subgraphs com bus.**
