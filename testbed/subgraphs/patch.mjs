// Patches por valor final: [{ path: ['home', 'shots'], value: 7 }].
// Nunca incrementos. Isso dá duas propriedades que o protocolo usa:
//   - idempotência: aplicar o mesmo patch duas vezes não muda nada;
//   - sobreposição: um patch que cobre (from, to] aplicado sobre um estado em
//     qualquer versão v com from <= v < to resulta no estado exato em `to`.
//     (Campos alterados em (from, v] já estão em v, e o patch traz o valor deles em `to`.)
const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);

export function diff(before, after, path = [], out = []) {
  const keys = new Set([...Object.keys(before ?? {}), ...Object.keys(after ?? {})]);
  for (const key of keys) {
    const a = before?.[key], b = after?.[key];
    if (isObject(a) && isObject(b)) diff(a, b, [...path, key], out);
    else if (JSON.stringify(a) !== JSON.stringify(b)) out.push({ path: [...path, key], value: structuredClone(b ?? null) });
  }
  return out;
}

export function applyPatch(state, patch) {
  for (const { path, value } of patch) {
    let node = state;
    for (const key of path.slice(0, -1)) {
      if (!isObject(node[key])) node[key] = {};
      node = node[key];
    }
    // Cópia: o valor pode ser um objeto (ex.: jogador novo) e não pode ficar
    // compartilhado com o patch, que é reaplicado e enviado a outros assinantes.
    node[path.at(-1)] = structuredClone(value);
  }
  return state;
}

// Junta patches consecutivos mantendo o último valor de cada caminho, compactado:
// um campo escrito dentro de um objeto já presente (ex.: jogador novo) é dobrado
// nele, e um objeto novo descarta entradas antigas dos seus descendentes. Assim o
// patch fundido tem no máximo uma entrada por campo, nunca uma por evento.
// delete + set mantém cada entrada na posição da sua última escrita.
export function mergePatches(...patches) {
  const byPath = new Map();
  for (const patch of patches) for (const entry of patch) {
    let folded = false;
    for (let i = entry.path.length - 1; i > 0 && !folded; i--) {
      const ancestorKey = JSON.stringify(entry.path.slice(0, i));
      const ancestor = byPath.get(ancestorKey);
      if (ancestor && isObject(ancestor.value)) {
        const value = applyPatch(structuredClone(ancestor.value), [{ path: entry.path.slice(i), value: entry.value }]);
        byPath.delete(ancestorKey); byPath.set(ancestorKey, { path: ancestor.path, value });
        folded = true;
      }
    }
    if (folded) continue;
    const key = JSON.stringify(entry.path);
    if (isObject(entry.value)) {
      const prefix = key.slice(0, -1) + ',';
      for (const other of [...byPath.keys()]) if (other.startsWith(prefix)) byPath.delete(other);
    }
    byPath.delete(key); byPath.set(key, entry);
  }
  return [...byPath.values()];
}
