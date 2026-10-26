#!/usr/bin/env node
import { readFileSync, writeFileSync } from "node:fs";
import { Engine, replay, criticalPath, nextRuns, validate } from "../src/index.js";
import { c, table, header, fail } from "../src/lib/cli.js";

const [cmd, ...rest] = process.argv.slice(2);
const flag = (n, d) => { const i = rest.indexOf("--" + n); return i >= 0 ? rest[i + 1] : d; };
const pos = rest.filter((x, i) => !x.startsWith("--") && !(rest[i - 1] || "").startsWith("--"));
const HELP = `${c.bold("flow")} — Prism Flow workflow engine

  flow run <workflow.json> [--workers 3] [--seed 1] [--fail 0.1] [--history out.json]
                                   run on the virtual clock, print Gantt + stats
  flow validate <workflow.json>    check DAG, print critical path
  flow replay <history.json>       rebuild state from an event history
  flow cron "<expr>" [--n 5]       next fire times (UTC) for a cron expression
`;
const load = (f) => { if (!f) fail("missing file argument"); return JSON.parse(readFileSync(f, "utf8")); };
const COL = { done: c.green, failed: c.red, skipped: c.dim, backoff: c.yellow };
try {
  if (!cmd || cmd === "-h" || cmd === "--help") console.log(HELP);
  else if (cmd === "validate") { const wf = load(pos[0]); validate(wf); const cp = criticalPath(wf); console.log(c.green("✔ valid DAG") + c.dim(` · ${wf.tasks.length} tasks`)); console.log(c.dim("critical path: ") + cp.path.join(c.dim(" → ")) + c.dim(` (${cp.length.toFixed(1)}s expected)`)); }
  else if (cmd === "run") {
    const wf = load(pos[0]), e = new Engine({ workers: +flag("workers", 3), seed: +flag("seed", 1), failRate: +flag("fail", 0) });
    e.start(wf); e.drain(); const run = [...e.runs.values()][0], span = (run.ended ?? e.now) - run.started;
    console.log(header(wf.name || "workflow", `· ${wf.tasks.length} tasks · ${e.workers.length} workers`) + "\n");
    const W = 46, spans = {}; let open = {};
    for (const ev of e.history) { if (ev.type === "TaskStarted") open[ev.task] = ev; if ((ev.type === "TaskCompleted" || ev.type === "TaskFailed") && open[ev.task]) { (spans[ev.task] ||= []).push({ a: open[ev.task].t, b: ev.t, ok: ev.type === "TaskCompleted", w: open[ev.task].worker }); delete open[ev.task]; } }
    const rows = wf.tasks.map((t) => { const line = Array(W).fill(c.dim("·")); for (const s of spans[t.id] || []) { const a = Math.floor((s.a / span) * (W - 1)), b = Math.max(a, Math.floor((s.b / span) * (W - 1))); for (let i = a; i <= b; i++) line[i] = s.ok ? c.accent("█") : c.red("▓"); } const st = run.tasks.get(t.id); return [t.id, line.join(""), (COL[st.state] || ((x) => x))(st.state), st.attempt, (spans[t.id] || []).map((s) => s.w).join(",")]; });
    console.log(table(rows, { head: ["task", `timeline 0 → ${span.toFixed(2)}s`, "state", "tries", "worker"] }));
    const s = e.stats(); console.log("\n" + c.dim("status ") + (run.status === "completed" ? c.green(run.status) : c.red(run.status)) + c.dim("  makespan ") + span.toFixed(2) + "s" + c.dim("  retries ") + s.retries + c.dim("  events ") + e.history.length);
    const hf = flag("history"); if (hf) { writeFileSync(hf, JSON.stringify(e.history, null, 1)); console.log(c.dim(`history → ${hf}`)); }
  } else if (cmd === "replay") {
    const runs = replay(load(pos[0]));
    for (const r of runs.values()) { console.log(header(r.id, `${r.name || ""} · ${r.status}`) + "\n"); console.log(table(Object.entries(r.tasks).map(([k, t]) => [k, (COL[t.state] || ((x) => x))(t.state), t.attempts, t.worker || "", t.lastError ? c.dim(t.lastError) : ""]), { head: ["task", "state", "attempts", "worker", "last error"] })); }
  } else if (cmd === "cron") {
    const expr = pos[0]; if (!expr) fail('usage: flow cron "*/15 9-17 * * mon-fri"');
    console.log(header("cron", expr) + "\n"); for (const d of nextRuns(expr, flag("from") ? new Date(flag("from")) : new Date(), +flag("n", 5))) console.log("  " + c.accent("●") + " " + d.toISOString().replace("T", " ").slice(0, 16) + c.dim(" UTC  " + d.toUTCString().slice(0, 3)));
  } else fail(`unknown command "${cmd}"\n\n${HELP}`);
} catch (e) { fail(e.message); }
