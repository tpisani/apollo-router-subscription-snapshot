import { composeServices } from '@apollo/composition';
import { parse } from 'graphql';
import { writeFileSync } from 'node:fs';
import { ordersSDL, catalogSDL } from '../src/schemas.mjs';

// Gera o supergraph usado pelo Hive Gateway (e aceito também pelo Apollo Router).
export function composeSupergraph({
  ordersUrl = process.env.ORDERS_URL ?? 'http://localhost:4001/graphql',
  catalogUrl = process.env.CATALOG_URL ?? 'http://localhost:4002/graphql'
} = {}) {
  const result = composeServices([
    { name: 'orders', url: ordersUrl, typeDefs: parse(ordersSDL) },
    { name: 'catalog', url: catalogUrl, typeDefs: parse(catalogSDL) }
  ]);
  if (result.errors) throw new Error(result.errors.map(error => error.message).join('\n'));
  return result.supergraphSdl;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const out = process.argv[2] ?? 'gateway/supergraph.graphql';
  writeFileSync(out, composeSupergraph());
  console.log(`Supergraph composto em ${out}`);
}
