# Decisões do exemplo

## Qual estado possui a garantia?

O conjunto de pedidos de um escopo, incluindo status, versão, inclusão e
exclusão, pertence a uma projeção. Um snapshot desse conjunto e o seu cursor
são lidos na mesma transação SQLite. Uma escrita modifica a projeção, grava o
evento e avança o head numa única transação `BEGIN IMMEDIATE`.

Não se obtém essa garantia combinando campos arbitrários vindos de vários
subgraphs. Se uma tela exigir consistência conjunta entre pedidos, pagamentos
e estoque, construa uma projeção adequada ou explicite garantias independentes
por domínio. Um cursor não estabelece uma transação distribuída.

## Por que manter duas consultas de recuperação?

O snapshot fornece uma base potencialmente antiga. `orderHead` fixa a posição
durável até a qual o cliente deve recuperar mudanças. `orderChanges` pagina
somente o intervalo `(after, through]`; escritas posteriores não deslocam o
limite durante a paginação.

O head é lido depois de o cliente receber um frame e depois da query do
snapshot. Neste exemplo ambos leem o mesmo banco, sem réplicas defasadas:
portanto H é maior ou igual a C e à posição do primeiro frame. Em arquiteturas
com réplicas, essa relação precisa ser implementada explicitamente.

## Por que não colocar `after` na subscription?

Clientes com cursores diferentes não devem depender de um único stream para
replay individual. Um cliente que entra numa conexão deduplicada já aberta
recebe os próximos frames, não a sequência que o primeiro cliente recebeu na
abertura. A subscription deste exemplo representa apenas um recorte
compartilhável; cada cliente faz sua própria recuperação por queries.

Um checkpoint de domínio é emitido periodicamente. Ele contém o head durável
e continua sendo emitido mesmo sem mudanças de negócio. Um heartbeat vazio do
protocolo multipart indica apenas transporte vivo e não substitui esse dado.

## Política de cache

| Caminho | Cache interno | Cabeçalho do subgraph para Router |
| --- | --- | --- |
| `ordersSnapshot` | 120 s, objeto inteiro | `public, max-age=60` se for a única operação cacheável |
| `ordersSnapshot(fresh:true)` | Ignorado | `no-store` |
| `orderHead` | Nenhum | `no-store` |
| `orderChanges` | Nenhum no exemplo | `no-store` |
| Mutations | Nenhum | `no-store` |
| `Order.label` no catalog | 300 s | `no-store` no exemplo |

O TTL de 60 s do Router não significa que os dados tenham no máximo 60 s de
idade: uma resposta vinda de um cache interno já antigo pode começar um novo
TTL externo. A consistência aqui depende do cursor e da retenção do histórico,
não da soma presumida dos TTLs. As duas camadas podem manter snapshots
diferentes e ainda assim válidos como bases para replay.

`fresh:true` muda a operação/chave que o Router consulta e faz o subgraph
ignorar seu cache interno. Essa rota precisa ser mantida sem cache em qualquer
camada adicional, inclusive CDN ou proxy que você acrescente.

Este servidor calcula Cache-Control de forma conservadora: somente campos
raiz diretos de snapshot sem `fresh:true` recebem TTL. Fragmentos e argumentos
variáveis de `fresh` ficam sem cache, mesmo quando poderiam ser cacheáveis.
Isso reduz a complexidade do exemplo sem arriscar cachear head ou fresh.

## Fontes externas e publicação confiável

Se alterações vierem de outro banco, use uma integração confiável para
transportá-las até a projeção: por exemplo, outbox transacional ou CDC. Uma
escrita num banco seguida de uma publicação best-effort pode perder eventos
em uma falha entre essas operações.

Na ingestão, mantenha um identificador de evento de origem para deduplicar
reentregas; o demo recebe mutations diretamente e não implementa esse adaptador.
APIs sem eventos confiáveis precisam de polling e reconciliação, com frescor
limitado pelo que a integração consegue observar.

O evento contém o estado versionado de `OrderState`. O resolver não consulta
uma API cacheada depois de emitir a versão para reconstruir esses campos.
Caso precise de enriquecimento federado, classifique quais campos aceitam
consistência eventual e quais precisam da mesma versão do evento.

## Falhas e recuperação

- Evento duplicado ou antigo: descartado se `seq <= cursor`.
- Evento seguinte contíguo: aplicado diretamente.
- Evento adiantado ou checkpoint acima do cursor: replay do intervalo faltante.
- Desconexão: abre novo stream e recupera desde o cursor aplicado.
- Cursor anterior à retenção: snapshot fresh e recuperação até novo head.
- Página vazia antes de H: falha explícita, sem fingir que o cliente sincronizou.
- Stream sem progresso: watchdog encerra a conexão e inicia recuperação.

O snapshot inicial pode ser usado para uma primeira renderização imediata,
mas o cliente de terminal só imprime a visão após a primeira recuperação.

## Deduplicação e autorização

A configuração deixa a deduplicação no padrão do Router 2.10. Não ignora
headers de autorização nem mistura identidades. Neste laboratório não há
autenticação: dois clientes do mesmo scope e da mesma operação podem
compartilhar a conexão. Em produção, avalie os critérios de equivalência da
versão utilizada. Nunca retire contexto de autorização só para obter maior
taxa de compartilhamento.

Deduplicação reduz conexões/eventos de origem; não fornece armazenamento,
replay por cliente, atualização do cache interno ou confirmação de aplicação.
