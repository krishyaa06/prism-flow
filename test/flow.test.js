import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { Engine, replay, execute, topo, validate, criticalPath, backoff, nextRuns, parseCron } from "../src/index.js";
const wf = JSON.parse(readFileSync(new URL("../examples/ml-pipeline.json", import.meta.url)));

test("topological order respects dependencies and cycles are rejected", () => {
  const o = topo(wf); for (const t of wf.tasks) for (const d of t.deps || []) assert.ok(o.indexOf(d) < o.indexOf(t.id));
  assert.throws(() => validate({ tasks: [{ id: "a", deps: ["b"] }, { id: "b", deps: ["a"] }] }), /Cycle/);
  assert.throws(() => validate({ tasks: [{ id: "a", deps: ["zzz"] }] }), /unknown task/);
  assert.deepEqual(criticalPath(wf).path, ["extract_events", "validate", "features", "train_xgb", "evaluate", "publish_model", "notify"]);
});
test("engine completes the workflow and never runs a task before its deps", () => {
  const e = new Engine({ workers: 3, seed: 5 }); e.start(wf); e.drain();
  const run = [...e.runs.values()][0]; assert.equal(run.status, "completed");
  const doneAt = {}; for (const ev of e.history) { if (ev.type === "TaskCompleted") doneAt[ev.task] = ev.t; if (ev.type === "TaskStarted") for (const d of wf.tasks.find((t) => t.id === ev.task).deps || []) assert.ok(doneAt[d] <= ev.t, `${ev.task} before ${d}`); }
  // never more than 3 concurrent
  let cur = 0, max = 0; for (const ev of e.history) { if (ev.type === "TaskStarted") max = Math.max(max, ++cur); if (ev.type === "TaskCompleted" || ev.type === "TaskFailed") cur--; } assert.ok(max <= 3);
});
test("retries with backoff, then permanent failure skips downstream", () => {
  const e = new Engine({ workers: 2, seed: 2 });
  e.start({ name: "x", tasks: [{ id: "a", failRate: 1, retries: 2 }, { id: "b", deps: ["a"] }, { id: "c" }] }); e.drain();
  const run = [...e.runs.values()][0]; assert.equal(run.status, "failed");
  assert.equal(e.history.filter((x) => x.type === "TaskStarted" && x.task === "a").length, 3);
  assert.equal(run.tasks.get("b").state, "skipped"); assert.equal(run.tasks.get("c").state, "done");
});
test("timeouts count as failures", () => {
  const e = new Engine({ seed: 1 }); e.start({ tasks: [{ id: "slow", duration: 10, timeout: 1, retries: 0 }] }); e.drain();
  assert.match(e.history.find((x) => x.type === "TaskFailed").error, /timeout/);
});
test("worker crash is recovered via heartbeat timeout", () => {
  const e = new Engine({ workers: 2, seed: 3 }); e.start(wf); e.advance(0.3);
  const busy = e.workers.find((w) => w.busy); e.crash(busy.id); e.drain();
  assert.ok(e.history.some((x) => x.type === "HeartbeatTimeout")); assert.equal([...e.runs.values()][0].status, "completed");
});
test("replay rebuilds the same final state from history", () => {
  const e = new Engine({ workers: 4, seed: 9 }); e.start(wf); e.drain();
  const r = replay(e.history).get("run-1"), live = e.runs.get("run-1");
  assert.equal(r.status, live.status); for (const [id, t] of live.tasks) assert.equal(r.tasks[id].state, t.state);
  const e2 = new Engine({ workers: 4, seed: 9 }); e2.start(wf); e2.drain(); assert.deepEqual(e2.history, e.history, "deterministic");
});
test("backoff grows exponentially and is capped", () => {
  assert.equal(backoff(1, { base: 1, jitter: "none" }), 1); assert.equal(backoff(4, { base: 1, jitter: "none" }), 8); assert.equal(backoff(20, { base: 1, cap: 30, jitter: "none" }), 30);
  for (let i = 0; i < 50; i++) assert.ok(backoff(3, { base: 1 }) <= 4);
});
test("cron next runs", () => {
  const from = new Date("2026-01-05T08:52:00Z"); // Monday
  assert.deepEqual(nextRuns("*/15 9-17 * * mon-fri", from, 3).map((d) => d.toISOString()), ["2026-01-05T09:00:00.000Z", "2026-01-05T09:15:00.000Z", "2026-01-05T09:30:00.000Z"]);
  assert.equal(nextRuns("@monthly", from, 1)[0].toISOString(), "2026-02-01T00:00:00.000Z");
  assert.equal(nextRuns("0 0 29 2 *", from, 1)[0].toISOString(), "2028-02-29T00:00:00.000Z");
  assert.throws(() => parseCron("61 * * * *"), /out of range/);
});
test("real async executor with retries", async () => {
  let tries = 0; const out = await execute({ tasks: [{ id: "a" }, { id: "b", deps: ["a"], retries: 3, backoff: { base: 0.001 } }] }, { a: async () => 2, b: async ({ results }) => { if (++tries < 3) throw new Error("flaky"); return results.a * 21; } });
  assert.equal(out.b, 42); assert.equal(tries, 3);
});
