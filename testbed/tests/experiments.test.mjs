import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import { simulateMatch, rate, throughLink, STATS_POLICIES } from '../scripts/experiments.mjs';

// Experimentos que orientam o desenho. Imprimem os números (veja docs/validation.md)
// e afirmam só as relações que sustentam as decisões.
function run(options) {
  mock.timers.enable({ apis: ['setInterval', 'setTimeout', 'Date'], now: 1_800_000_000_000 });
  try { return simulateMatch({ tick: ms => mock.timers.tick(ms), now: () => Date.now(), ...options }); }
  finally { mock.timers.reset(); }
}
const ADOPTED = 'o menor por janela de 500 ms (adotado)';

test('E1 banda: o que cada tipo de cliente recebe por segundo', () => {
  const r = run({ seconds: 60, eventsPerSecond: 20, goals: [{ at: 20_000, side: 'HOME' }, { at: 41_300, side: 'AWAY' }] });
  const scoreboard = rate(r.scoreboard, r.elapsed);
  const stats = Object.fromEntries(Object.entries(r.stats).map(([name, frames]) => [name, rate(frames, r.elapsed)]));
  console.log(JSON.stringify({ commitsPorSegundo: { placar: +(r.commits.scoreboard.length / r.elapsed).toFixed(2), estatisticas: +(r.commits.stats.length / r.elapsed).toFixed(1) }, placar: scoreboard, estatisticas: stats }, null, 1));

  // Placar: ~1 frame/s (periódico) + os gols; poucos bytes.
  assert.ok(scoreboard.framesPerSecond <= 1.2 && scoreboard.bytesPerSecond < 400);
  // Agrupar por janela reduz frames de ~20/s para ~2/s.
  assert.ok(stats[ADOPTED].framesPerSecond <= 3.2 && stats['patch a cada lance'].framesPerSecond >= 19);
  // Mandar o estado inteiro a cada lance é a pior política; o menor por janela é o menor total.
  const bytes = Object.fromEntries(Object.entries(stats).map(([k, v]) => [k, v.bytesPerSecond]));
  assert.equal(Math.min(...Object.values(bytes)), bytes[ADOPTED]);
  assert.ok(bytes['estado inteiro a cada lance'] > 10 * bytes[ADOPTED]);
  // Um cliente que só mostra o placar (lista de jogos) num stream único com tudo
  // receberia as estatísticas junto: muitas vezes mais bytes que o canal dele.
  const combinedForScoreboardOnly = scoreboard.bytesPerSecond + bytes[ADOPTED];
  console.log(`cliente só de placar: ${scoreboard.bytesPerSecond} B/s separado vs ${combinedForScoreboardOnly} B/s num stream único`);
  assert.ok(combinedForScoreboardOnly > 5 * scoreboard.bytesPerSecond);
});

test('E2 isolamento: gols no meio de uma rajada de estatísticas, num enlace de 16 KB/s', () => {
  // Rajada: ataque com 200 lances/s por 3 s. Um gol a cada 97 ms dentro dela, para
  // medir a distribuição do atraso (onde o gol cai em relação aos frames grandes).
  const goals = Array.from({ length: 30 }, (_, i) => ({ at: 39_600 + i * 97, side: i % 2 ? 'HOME' : 'AWAY' }));
  const r = run({ seconds: 50, eventsPerSecond: 20, storms: [{ from: 39_500, to: 42_500, eventsPerSecond: 200 }], goals });
  const link = 16_000; // ~128 kbit/s: celular com sinal ruim
  const latencies = frames => {
    const values = throughLink(frames, link).filter(f => f.goal).map(f => f.arrival - f.committedAt).sort((a, b) => a - b);
    return { p50: Math.round(values[Math.floor(values.length / 2)]), p95: Math.round(values[Math.floor(values.length * 0.95)]), max: Math.round(values.at(-1)) };
  };
  const scoreboardOnly = latencies(r.scoreboard);
  const shared = Object.fromEntries(Object.entries(r.stats).map(([name, frames]) => [name, latencies([...r.scoreboard, ...frames])]));
  console.log(JSON.stringify({ enlaceBytesPorSegundo: link, gols: goals.length, latenciaDoGolMs: { 'só placar no enlace': scoreboardOnly, 'placar + estatísticas no mesmo enlace': shared } }, null, 1));

  // Sem estatísticas no enlace, o gol chega só com o tempo de transmissão do próprio frame.
  assert.ok(scoreboardOnly.max < 50);
  // No mesmo enlace, o gol espera os bytes de estatísticas na frente (head-of-line).
  // Estado inteiro a cada lance satura o enlace: segundos de atraso.
  assert.ok(shared['estado inteiro a cada lance'].p50 > 1000);
  // Patch por janela mantém a fila curta: o pior caso fica em centenas de ms, não segundos.
  assert.ok(shared[ADOPTED].max < 500);
  // Escolher o menor por janela nunca é pior que só patch ou só estado inteiro.
  assert.ok(shared[ADOPTED].p95 <= shared['estado inteiro a cada 500 ms'].p95);
  assert.ok(shared[ADOPTED].p95 <= shared['patch a cada 500 ms'].p95);
  assert.equal(Object.keys(STATS_POLICIES).length, 5);
});
