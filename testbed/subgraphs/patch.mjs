// Final-value patches: [{ path: ['home', 'shots'], value: 7 }].
// Never increments. That gives two properties the protocol relies on:
//   - idempotence: applying the same patch twice changes nothing;
//   - overlap: a patch covering (from, to] applied on top of a state at any
//     version v with from <= v < to results in the exact state at `to`.
//     (Fields changed in (from, v] are already in v, and the patch carries their value at `to`.)
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
    // Copy: the value may be an object (e.g. a new player) and must not stay
    // shared with the patch, which is reapplied and sent to other subscribers.
    node[path.at(-1)] = structuredClone(value);
  }
  return state;
}

// Merges consecutive patches keeping the last value of each path, compacted:
// a field written inside an object already present (e.g. a new player) is folded
// into it, and a new object drops older entries for its descendants. So the
// merged patch has at most one entry per field, never one per event.
// delete + set keeps each entry at the position of its last write.

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
