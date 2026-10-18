import { composeServices } from '@apollo/composition';
import { parse } from 'graphql';
import { writeFileSync } from 'node:fs';
import { scoresSDL, statsSDL, catalogSDL } from '../src/schemas.mjs';

// Supergraph para o Hive Gateway e o Apollo Router: placar e estatísticas em
// subgraphs separados, cada um com sua subscription.
export function composeSupergraph({
  scoresUrl = process.env.SCORES_URL ?? 'http://localhost:4001/graphql',
  catalogUrl = process.env.CATALOG_URL ?? 'http://localhost:4002/graphql',
  statsUrl = process.env.STATS_URL ?? 'http://localhost:4003/graphql'
} = {}) {
  const result = composeServices([
    { name: 'scores', url: scoresUrl, typeDefs: parse(scoresSDL) },
    { name: 'catalog', url: catalogUrl, typeDefs: parse(catalogSDL) },
    { name: 'stats', url: statsUrl, typeDefs: parse(statsSDL) }
  ]);
  if (result.errors) throw new Error(result.errors.map(error => error.message).join('\n'));
  return result.supergraphSdl;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const out = process.argv[2] ?? 'gateway/supergraph.graphql';
  writeFileSync(out, composeSupergraph());
  console.log(`Supergraph composto em ${out}`);
}
