// Deterministic workflow engine on a virtual clock: worker pool, retries with backoff+jitter,
// timeouts, worker crashes with heartbeat recovery, and an append-only event history that can be replayed.
import { Heap } from "./lib/heap.js";
import { rng } from "./lib/rng.js";
import { validate, children } from "./dag.js";
import { backoff } from "./retry.js";

export class Engine {
  constructor({ workers = 4, seed = 1, heartbeat = 2, failRate = 0 } = {}) {
    this.r = rng(seed); this.now = 0; this.timers = new Heap((a, b) => a.at - b.at || a.seq - b.seq); this.seq = 0;
    this.workers = Array.from({ length: workers }, (_, i) => ({ id: `w${i + 1}`, busy: null, alive: true, done: 0 }));
    this.heartbeat = heartbeat; this.failRate = failRate; this.runs = new Map(); this.history = []; this.listeners = [];
  }
  on(fn) { this.listeners.push(fn); }
  record(type, data) { const e = { seq: this.history.length + 1, t: +this.now.toFixed(3), type, ...data }; this.history.push(e); for (const l of this.listeners) l(e); return e; }
  at(delay, fn) { this.timers.push({ at: this.now + delay, seq: this.seq++, fn }); }
  setWorkers(n) { while (this.workers.length < n) this.workers.push({ id: `w${this.workers.length + 1}`, busy: null, alive: true, done: 0 }); for (const w of this.workers.slice(n)) if (!w.busy) w.alive = false; this.workers = this.workers.filter((w, i) => i < n || w.busy); this.dispatch(); }

  start(wf, { id } = {}) {
    validate(wf); const runId = id || `run-${this.runs.size + 1}`;
    const run = { id: runId, wf, status: "running", started: this.now, tasks: new Map(wf.tasks.map((t) => [t.id, { def: t, state: "waiting", attempt: 0, deps: new Set(t.deps || []) }])), kids: children(wf), queue: [] };
    this.runs.set(runId, run); this.record("WorkflowStarted", { run: runId, name: wf.name });
    for (const [tid, ts] of run.tasks) if (!ts.deps.size) this.enqueue(run, tid);
    this.dispatch(); return runId;
  }
  enqueue(run, tid) { const ts = run.tasks.get(tid); ts.state = "queued"; ts.queuedAt = this.now; run.queue.push(tid); this.record("TaskScheduled", { run: run.id, task: tid, attempt: ts.attempt + 1 }); }
  dispatch() {
    for (const w of this.workers) {
      if (!w.alive || w.busy) continue;
      // pick highest-priority queued task across runs (priority then FIFO)
      let best = null; for (const run of this.runs.values()) for (const tid of run.queue) { const p = run.tasks.get(tid).def.priority || 0; if (!best || p > best.p) best = { run, tid, p }; }
      if (!best) return; best.run.queue.splice(best.run.queue.indexOf(best.tid), 1); this.execute(w, best.run, best.tid);
    }
  }
  execute(w, run, tid) {
    const ts = run.tasks.get(tid), d = ts.def; ts.attempt++; ts.state = "running"; ts.worker = w.id; ts.startedAt = this.now; const token = {};
    w.busy = { run: run.id, task: tid, token, since: this.now }; ts.token = token;
    this.record("TaskStarted", { run: run.id, task: tid, attempt: ts.attempt, worker: w.id });
    const dur = Math.max(0.05, (d.duration || 1) * (0.7 + this.r() * 0.6)), timeout = d.timeout || Infinity;
    const fails = this.r() < Math.min(0.95, (d.failRate || 0) + this.failRate);
    if (dur > timeout) this.at(timeout, () => token.dead || this.finish(w, run, tid, token, false, `timeout after ${timeout}s`));
    else this.at(dur, () => token.dead || this.finish(w, run, tid, token, !fails, fails ? (d.error || "handler threw: transient error") : null));
  }
  finish(w, run, tid, token, ok, err) {
    const ts = run.tasks.get(tid); token.dead = true; w.busy = null; w.done++; ts.dur = this.now - ts.startedAt;
    if (ok) {
      ts.state = "done"; this.record("TaskCompleted", { run: run.id, task: tid, attempt: ts.attempt, worker: w.id, ms: Math.round(ts.dur * 1000) });
      for (const k of run.kids.get(tid)) { const ks = run.tasks.get(k); ks.deps.delete(tid); if (!ks.deps.size && ks.state === "waiting") this.enqueue(run, k); }
      this.checkDone(run);
    } else this.fail(run, tid, err, w.id);
    this.dispatch();
  }
  fail(run, tid, err, worker) {
    const ts = run.tasks.get(tid), max = ts.def.retries ?? 3;
    if (ts.attempt <= max) {
      const delay = backoff(ts.attempt, ts.def.backoff || { base: 0.4, cap: 8 }, this.r); ts.state = "backoff"; ts.retryAt = this.now + delay;
      this.record("TaskFailed", { run: run.id, task: tid, attempt: ts.attempt, worker, error: err, retryIn: +delay.toFixed(3) });
      this.at(delay, () => { this.record("TimerFired", { run: run.id, task: tid }); this.enqueue(run, tid); this.dispatch(); });
    } else {
      ts.state = "failed"; this.record("TaskFailed", { run: run.id, task: tid, attempt: ts.attempt, worker, error: err, final: true });
      // downstream tasks can never run
      const stack = [...run.kids.get(tid)]; while (stack.length) { const k = stack.pop(), ks = run.tasks.get(k); if (ks.state === "waiting") { ks.state = "skipped"; stack.push(...run.kids.get(k)); } }
      this.checkDone(run);
    }
  }
  checkDone(run) {
    if (run.status !== "running") return; const st = [...run.tasks.values()].map((t) => t.state);
    if (st.every((s) => s === "done")) { run.status = "completed"; run.ended = this.now; this.record("WorkflowCompleted", { run: run.id, makespan: +(this.now - run.started).toFixed(3) }); }
    else if (st.every((s) => ["done", "failed", "skipped"].includes(s))) { run.status = "failed"; run.ended = this.now; this.record("WorkflowFailed", { run: run.id, failed: [...run.tasks].filter(([, t]) => t.state === "failed").map(([k]) => k) }); }
  }
  /** Kill a worker: its in-flight task is lost and recovered after the heartbeat timeout. */
  crash(wid) {
    const w = this.workers.find((x) => x.id === wid); if (!w || !w.alive) return; w.alive = false; this.record("WorkerLost", { worker: wid });
    if (w.busy) { const { run: rid, task, token } = w.busy; token.dead = true; w.busy = null; const run = this.runs.get(rid);
      this.at(this.heartbeat, () => { this.record("HeartbeatTimeout", { run: rid, task, worker: wid }); this.fail(run, task, `worker ${wid} lost (heartbeat timeout)`, wid); this.dispatch(); }); }
    this.at(this.heartbeat * 2.5, () => { w.alive = true; this.record("WorkerJoined", { worker: wid }); this.dispatch(); });
  }
  /** Advance the virtual clock by dt seconds, firing timers in order. */
  advance(dt) { const end = this.now + dt; while (this.timers.size && this.timers.peek().at <= end) { const t = this.timers.pop(); this.now = t.at; t.fn(); } this.now = end; }
  /** Run until every workflow finishes (or a time limit). */
  drain(limit = 1e5) { while (this.timers.size && this.now < limit) { const t = this.timers.pop(); this.now = t.at; t.fn(); } return this; }
  stats() { const h = this.history; return { completed: h.filter((e) => e.type === "TaskCompleted").length, retries: h.filter((e) => e.type === "TaskFailed" && !e.final).length, failed: h.filter((e) => e.type === "TaskFailed" && e.final).length, runs: this.runs.size, busy: this.workers.filter((w) => w.busy).length }; }
}

/** Rebuild workflow state purely from history (event sourcing). */
export function replay(history) {
  const runs = new Map();
  for (const e of history) {
    if (e.type === "WorkflowStarted") runs.set(e.run, { id: e.run, name: e.name, status: "running", tasks: {} });
    const r = runs.get(e.run); if (!r) continue; const t = e.task && (r.tasks[e.task] ||= { state: "waiting", attempts: 0 });
    switch (e.type) {
      case "TaskScheduled": t.state = "queued"; break;
      case "TaskStarted": t.state = "running"; t.attempts = e.attempt; t.worker = e.worker; break;
      case "TaskCompleted": t.state = "done"; break;
      case "TaskFailed": t.state = e.final ? "failed" : "backoff"; t.lastError = e.error; break;
      case "WorkflowCompleted": r.status = "completed"; r.makespan = e.makespan; break;
      case "WorkflowFailed": r.status = "failed"; break;
    }
  }
  return runs;
}
