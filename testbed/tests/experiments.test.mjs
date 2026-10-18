import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import { simulateMatch, rate, throughLink, STATS_POLICIES } from '../scripts/experiments.mjs';

// Experiments that guide the design. They print the numbers (see docs/validation.md)
// and only assert the relations that back the decisions.
function run(options) {
  mock.timers.enable({ apis: ['setInterval', 'setTimeout', 'Date'], now: 1_800_000_000_000 });
  try { return simulateMatch({ tick: ms => mock.timers.tick(ms), now: () => Date.now(), ...options }); }
  finally { mock.timers.reset(); }
}
const ADOPTED = 'the smaller one per 500 ms window (adopted)';

test('E1 bandwidth: what each kind of client gets per second', () => {
  const r = run({ seconds: 60, eventsPerSecond: 20, goals: [{ at: 20_000, side: 'HOME' }, { at: 41_300, side: 'AWAY' }] });
  const scoreboard = rate(r.scoreboard, r.elapsed);
  const stats = Object.fromEntries(Object.entries(r.stats).map(([name, frames]) => [name, rate(frames, r.elapsed)]));
  console.log(JSON.stringify({ commitsPerSecond: { scoreboard: +(r.commits.scoreboard.length / r.elapsed).toFixed(2), stats: +(r.commits.stats.length / r.elapsed).toFixed(1) }, scoreboard, stats }, null, 1));

  // Scoreboard: ~1 frame/s (periodic) + the goals; few bytes.
  assert.ok(scoreboard.framesPerSecond <= 1.2 && scoreboard.bytesPerSecond < 400);
  // Grouping by window cuts frames from ~20/s to ~2/s.
  assert.ok(stats[ADOPTED].framesPerSecond <= 3.2 && stats['patch on every play'].framesPerSecond >= 19);
  // Sending the full state on every play is the worst policy; the smaller one per window is the smallest total.
  const bytes = Object.fromEntries(Object.entries(stats).map(([k, v]) => [k, v.bytesPerSecond]));
  assert.equal(Math.min(...Object.values(bytes)), bytes[ADOPTED]);
  assert.ok(bytes['full state on every play'] > 10 * bytes[ADOPTED]);
  // A client that only shows the scoreboard (match list) on a single stream with everything
  // would get the stats too: many times more bytes than its own channel.
  const combinedForScoreboardOnly = scoreboard.bytesPerSecond + bytes[ADOPTED];
  console.log(`scoreboard-only client: ${scoreboard.bytesPerSecond} B/s separate vs ${combinedForScoreboardOnly} B/s on a single stream`);
  assert.ok(combinedForScoreboardOnly > 5 * scoreboard.bytesPerSecond);
});

test('E2 isolation: goals in the middle of a stats burst, on a 16 KB/s link', () => {
  // Burst: an attack with 200 plays/s for 3 s. A goal every 97 ms inside it, to
  // measure the delay distribution (where the goal lands relative to the large frames).
  const goals = Array.from({ length: 30 }, (_, i) => ({ at: 39_600 + i * 97, side: i % 2 ? 'HOME' : 'AWAY' }));
  const r = run({ seconds: 50, eventsPerSecond: 20, storms: [{ from: 39_500, to: 42_500, eventsPerSecond: 200 }], goals });
  const link = 16_000; // ~128 kbit/s: a phone with poor signal
  const latencies = frames => {
    const values = throughLink(frames, link).filter(f => f.goal).map(f => f.arrival - f.committedAt).sort((a, b) => a - b);
    return { p50: Math.round(values[Math.floor(values.length / 2)]), p95: Math.round(values[Math.floor(values.length * 0.95)]), max: Math.round(values.at(-1)) };
  };
  const scoreboardOnly = latencies(r.scoreboard);
  const shared = Object.fromEntries(Object.entries(r.stats).map(([name, frames]) => [name, latencies([...r.scoreboard, ...frames])]));
  console.log(JSON.stringify({ linkBytesPerSecond: link, goals: goals.length, goalLatencyMs: { 'scoreboard only on the link': scoreboardOnly, 'scoreboard + stats on the same link': shared } }, null, 1));

  // With no stats on the link, the goal arrives with just its own frame's transmission time.
  assert.ok(scoreboardOnly.max < 50);
  // On the same link, the goal waits for the stats bytes ahead of it (head-of-line).
  // Full state on every play saturates the link: seconds of delay.
  assert.ok(shared['full state on every play'].p50 > 1000);
  // Patch per window keeps the queue short: the worst case stays at hundreds of ms, not seconds.
  assert.ok(shared[ADOPTED].max < 500);
  // Picking the smaller one per window is never worse than patch only or full state only.
  assert.ok(shared[ADOPTED].p95 <= shared['full state every 500 ms'].p95);
  assert.ok(shared[ADOPTED].p95 <= shared['patch every 500 ms'].p95);

  assert.equal(Object.keys(STATS_POLICIES).length, 5);
});
