// Real async executor: run a DAG of async functions with concurrency, retries, backoff and timeouts.
import { validate, children } from "./dag.js";
import { backoff } from "./retry.js";
const sleep = (s) => new Promise((r) => setTimeout(r, s * 1000));
export async function execute(wf, handlers, { concurrency = 4, onEvent = () => {} } = {}) {
  validate(wf); const kids = children(wf), left = new Map(wf.tasks.map((t) => [t.id, new Set(t.deps || [])])), results = {}, byId = new Map(wf.tasks.map((t) => [t.id, t]));
  const ready = wf.tasks.filter((t) => !(t.deps || []).length).map((t) => t.id); let active = 0, failed = null;
  return new Promise((resolve, reject) => {
    const pump = () => {
      if (failed) return; if (!ready.length && !active) return resolve(results);
      while (active < concurrency && ready.length) { const id = ready.shift(); active++; runTask(id).then(() => { active--; for (const k of kids.get(id)) { const s = left.get(k); s.delete(id); if (!s.size) ready.push(k); } pump(); }, (e) => { failed = e; reject(e); }); }
    };
    const runTask = async (id) => {
      const t = byId.get(id), fn = handlers[id]; if (!fn) throw new Error(`No handler for task "${id}"`);
      for (let attempt = 1; ; attempt++) {
        onEvent({ type: "TaskStarted", task: id, attempt });
        try {
          const p = fn({ results, attempt }); const out = t.timeout ? await Promise.race([p, sleep(t.timeout).then(() => { throw new Error(`timeout after ${t.timeout}s`); })]) : await p;
          results[id] = out; onEvent({ type: "TaskCompleted", task: id, attempt }); return;
        } catch (e) {
          if (attempt > (t.retries ?? 3)) { onEvent({ type: "TaskFailed", task: id, attempt, error: e.message, final: true }); throw Object.assign(new Error(`Task "${id}" failed: ${e.message}`), { task: id }); }
          const d = backoff(attempt, t.backoff || { base: 0.05, cap: 2 }); onEvent({ type: "TaskFailed", task: id, attempt, error: e.message, retryIn: d }); await sleep(d);
        }
      }
    };
    pump();
  });
}
