import { composeServices } from '@apollo/composition';
import { parse } from 'graphql';
import { writeFileSync } from 'node:fs';
import { ordersSDL, catalogSDL } from '../src/schemas.mjs';
const result = composeServices([
  { name:'orders', url:process.env.ORDERS_URL ?? 'http://subgraphs:4001/graphql', typeDefs:parse(ordersSDL) },
  { name:'catalog', url:process.env.CATALOG_URL ?? 'http://subgraphs:4002/graphql', typeDefs:parse(catalogSDL) }
]);
if (result.errors) { console.error(result.errors); process.exit(1); }
writeFileSync('router/supergraph.graphql',result.supergraphSdl);
console.log('Supergraph composto em router/supergraph.graphql');
