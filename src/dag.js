// DAG utilities: validation, topological levels, critical path.
export function validate(wf) {
  const ids = new Set(); for (const t of wf.tasks) { if (ids.has(t.id)) throw new Error(`Duplicate task id "${t.id}"`); ids.add(t.id); }
  for (const t of wf.tasks) for (const d of t.deps || []) if (!ids.has(d)) throw new Error(`Task "${t.id}" depends on unknown task "${d}"`);
  topo(wf); return true;
}
/** Kahn's algorithm. Returns ids in order; throws on cycle. */
export function topo(wf) {
  const indeg = new Map(wf.tasks.map((t) => [t.id, (t.deps || []).length])), out = [], q = wf.tasks.filter((t) => !(t.deps || []).length).map((t) => t.id);
  const kids = children(wf);
  while (q.length) { const id = q.shift(); out.push(id); for (const k of kids.get(id)) { indeg.set(k, indeg.get(k) - 1); if (!indeg.get(k)) q.push(k); } }
  if (out.length !== wf.tasks.length) { const stuck = wf.tasks.filter((t) => !out.includes(t.id)).map((t) => t.id); throw new Error(`Cycle detected among: ${stuck.join(", ")}`); }
  return out;
}
export function children(wf) { const m = new Map(wf.tasks.map((t) => [t.id, []])); for (const t of wf.tasks) for (const d of t.deps || []) m.get(d).push(t.id); return m; }
/** Depth of each task (longest path from a root) — used for layout. */
export function levels(wf) { const lv = new Map(), byId = new Map(wf.tasks.map((t) => [t.id, t])); for (const id of topo(wf)) lv.set(id, Math.max(0, ...(byId.get(id).deps || []).map((d) => lv.get(d) + 1))); return lv; }
/** Critical path by expected duration. */
export function criticalPath(wf) {
  const byId = new Map(wf.tasks.map((t) => [t.id, t])), fin = new Map(), prev = new Map();
  for (const id of topo(wf)) { const t = byId.get(id); let best = 0, bp = null; for (const d of t.deps || []) if (fin.get(d) > best) { best = fin.get(d); bp = d; } fin.set(id, best + (t.duration || 1)); prev.set(id, bp); }
  let end = [...fin.entries()].sort((a, b) => b[1] - a[1])[0][0]; const path = []; while (end) { path.unshift(end); end = prev.get(end); }
  return { path, length: Math.max(...fin.values()) };
}
