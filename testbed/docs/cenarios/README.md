# Páginas de cenários

Diagramas interativos, passo a passo, de como o cliente chega ao estado atual em
cada situação. Abra os arquivos direto no navegador (as fontes vêm do Google
Fonts; sem internet, a página usa as fontes do sistema).

- [`retencao.html`](retencao.html): **desenho atual.** Estado inteiro nos dois
  canais, retenção do último frame por instância e várias instâncias atrás de um
  balanceador. Oito cenários: primeiro cliente numa instância, late joiner,
  gol, rajada, reconexão em outra instância, queda de instância, identidade e
  limpeza.
- [`status-e-kickoff.html`](status-e-kickoff.html): **ciclo de vida da partida.**
  Quando a tela usa só a query, quando assina e quando para: o horário de início
  decide começar, o `FULL_TIME` decide parar, e o cache do `SCHEDULED` nunca passa
  da janela. Simulador com as mesmas regras do código, gráfico do TTL e cenários
  testados.
- [`patch-por-janela.html`](patch-por-janela.html): desenho anterior (commit
  `9a87874`), com `DELTA` nas estatísticas. Fica como registro da comparação.
