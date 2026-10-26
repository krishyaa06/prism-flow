import { Engine, replay, levels, nextRuns } from "../src/index.js";
import { $, h, esc, fmt, toast, Loop, bindLoop, LogView, Kpis, Chart, fit, css, hexA } from "./pro.js";

const WF = {
  name: "nightly-ml-pipeline",
  tasks: [
    { id: "extract_orders", duration: 1.2 }, { id: "extract_users", duration: 0.9 }, { id: "extract_events", duration: 1.6, failRate: 0.1 },
    { id: "validate", deps: ["extract_orders", "extract_users", "extract_events"], duration: 0.6 },
    { id: "features", deps: ["validate"], duration: 2.0, priority: 2 }, { id: "report", deps: ["validate"], duration: 0.7 },
    { id: "train_xgb", deps: ["features"], duration: 2.6, timeout: 4.5 }, { id: "train_mlp", deps: ["features"], duration: 2.2, timeout: 4.5 },
    { id: "evaluate", deps: ["train_xgb", "train_mlp"], duration: 0.8 }, { id: "publish_model", deps: ["evaluate"], duration: 0.5, retries: 5 },
    { id: "notify", deps: ["publish_model", "report"], duration: 0.3 },
  ],
};
const E = new Engine({ workers: 3, seed: 21, failRate: 0.12 });
const log = new LogView($("#log"), 300);
const TONE = { WorkflowStarted: "acc", TaskCompleted: "ok", TaskFailed: "warn", WorkflowCompleted: "ok", WorkflowFailed: "bad", WorkerLost: "bad", HeartbeatTimeout: "bad", WorkerJoined: "ok", TaskStarted: "", TaskScheduled: "dim", TimerFired: "dim" };
const spans = []; const open = new Map(); let done = 0, lastDone = 0;
E.on((e) => {
  if (e.type === "TaskStarted") open.set(e.run + e.task, { w: e.worker, a: e.t, task: e.task, run: e.run });
  if (e.type === "TaskCompleted" || e.type === "TaskFailed") { const o = open.get(e.run + e.task); if (o) { spans.push({ ...o, b: e.t, ok: e.type === "TaskCompleted" }); open.delete(e.run + e.task); } if (e.type === "TaskCompleted") done++; }
  if (e.type === "HeartbeatTimeout") open.delete(e.run + e.task);
  if (spans.length > 400) spans.shift();
  if (e.type === "TaskScheduled" || e.type === "TimerFired") return;
  const extra = e.type === "TaskFailed" ? ` <span class="${e.final ? "bad" : "warn"}">${esc(e.error)}${e.final ? " · giving up" : ` · retry in ${e.retryIn}s`}</span>` : e.worker ? ` <span class="dim">on ${e.worker}${e.ms ? " · " + e.ms + "ms" : ""}${e.attempt > 1 ? " · attempt " + e.attempt : ""}</span>` : e.makespan ? ` <span class="dim">makespan ${e.makespan}s</span>` : "";
  log.html(`<span class="dim">#${e.seq}</span> <span class="${TONE[e.type] || ""}">${e.type}</span> ${esc(e.task || e.run || "")}${extra}`, `t+${e.t.toFixed(1)}s`);
  $("#evn").textContent = `${E.history.length} events`;
  if (e.type === "WorkflowCompleted" || e.type === "WorkflowFailed") setTimeout(() => newRun(), 1600 / loop.speed);
});
let cur = null; const LV = levels(WF);
function newRun() { cur = E.start(WF); $("#runid").textContent = cur; }

const kp = new Kpis($("#kpis"), [
  { key: "runs", label: "Runs", tone: "acc" }, { key: "ok", label: "Completed", tone: "ok" }, { key: "tasks", label: "Tasks done" },
  { key: "retries", label: "Retries", tone: "warn" }, { key: "busy", label: "Busy workers" }, { key: "span", label: "Makespan", fmt: (v) => v.toFixed(2) + "s" },
]);
const ch1 = new Chart($("#ch1"), { series: [{ label: "tasks/s", color: "--accent" }], min: 0, points: 90, fmtY: (v) => v.toFixed(1) });
const ch2 = new Chart($("#ch2"), { series: [{ label: "queued", color: "--accent2" }, { label: "busy", color: "#22e3a1" }], min: 0, points: 90, fmtY: (v) => v.toFixed(0) });

const STC = { waiting: "#2a3354", queued: "#4f8bff", running: "#00d2ff", done: "#22e3a1", backoff: "#ffb547", failed: "#ff5c8a", skipped: "#3b405a" };
function drawDag() {
  const { ctx, w, h: H } = fit($("#dag")); ctx.clearRect(0, 0, w, H); if (!cur) return; const run = E.runs.get(cur);
  const vertical = w < 600, maxL = Math.max(...LV.values()), cols = new Map();
  for (const t of WF.tasks) { const l = LV.get(t.id); if (!cols.has(l)) cols.set(l, []); cols.get(l).push(t.id); }
  const P = new Map(), nw = vertical ? Math.min(150, w / Math.max(...[...cols.values()].map((c) => c.length)) - 12) : Math.min(140, (w - 40) / (maxL + 1) - 18), nh = 40;
  for (const [l, ids] of cols) ids.forEach((id, i) => { const main = 30 + (l / Math.max(1, maxL)) * ((vertical ? H : w) - 60 - (vertical ? nh : nw)); const cross = vertical ? (w / ids.length) * (i + 0.5) : ((i + 1) / (ids.length + 1)) * (H - 20) + 10; P.set(id, vertical ? { x: cross - nw / 2, y: main } : { x: main, y: cross - nh / 2 }); });
  // edges
  for (const t of WF.tasks) for (const d of t.deps || []) {
    const a = P.get(d), b = P.get(t.id), ds = run.tasks.get(d).state, flowing = ds === "done" && ["queued", "running", "waiting"].includes(run.tasks.get(t.id).state);
    const [x1, y1, x2, y2] = vertical ? [a.x + nw / 2, a.y + nh, b.x + nw / 2, b.y] : [a.x + nw, a.y + nh / 2, b.x, b.y + nh / 2];
    ctx.strokeStyle = ds === "done" ? "#22e3a155" : "#ffffff1a"; ctx.lineWidth = 1.4; ctx.beginPath(); ctx.moveTo(x1, y1);
    if (vertical) ctx.bezierCurveTo(x1, (y1 + y2) / 2, x2, (y1 + y2) / 2, x2, y2); else ctx.bezierCurveTo((x1 + x2) / 2, y1, (x1 + x2) / 2, y2, x2, y2); ctx.stroke();
    if (flowing) { const k = (E.now * 0.8) % 1, q = (u) => { const m = 1 - u; return vertical ? [m * m * m * x1 + 3 * m * m * u * x1 + 3 * m * u * u * x2 + u * u * u * x2, m * m * m * y1 + 3 * m * m * u * ((y1 + y2) / 2) + 3 * m * u * u * ((y1 + y2) / 2) + u * u * u * y2] : [m * m * m * x1 + 3 * m * m * u * ((x1 + x2) / 2) + 3 * m * u * u * ((x1 + x2) / 2) + u * u * u * x2, m * m * m * y1 + 3 * m * m * u * y1 + 3 * m * u * u * y2 + u * u * u * y2]; }; const [px, py] = q(k); ctx.fillStyle = "#22e3a1"; ctx.beginPath(); ctx.arc(px, py, 2.6, 0, 7); ctx.fill(); }
  }
  for (const t of WF.tasks) {
    const p = P.get(t.id), s = run.tasks.get(t.id), col = STC[s.state];
    if (s.state === "running") { ctx.shadowColor = col; ctx.shadowBlur = 16 + 6 * Math.sin(E.now * 6); }
    ctx.fillStyle = hexA(col, s.state === "waiting" || s.state === "skipped" ? 0.35 : 0.16); ctx.strokeStyle = col; ctx.lineWidth = s.state === "running" ? 2 : 1.2;
    ctx.beginPath(); ctx.roundRect(p.x, p.y, nw, nh, 9); ctx.fill(); ctx.stroke(); ctx.shadowBlur = 0;
    if (s.state === "running") { const frac = Math.min(1, (E.now - s.startedAt) / (t.duration || 1)); ctx.fillStyle = hexA(col, 0.5); ctx.fillRect(p.x + 6, p.y + nh - 6, (nw - 12) * frac, 2.5); }
    ctx.fillStyle = s.state === "waiting" || s.state === "skipped" ? "#8d94ad" : "#e8ecfa"; ctx.font = "600 11px JetBrains Mono, monospace"; ctx.textAlign = "left"; ctx.textBaseline = "middle";
    let label = t.id; while (ctx.measureText(label).width > nw - 14 && label.length > 4) label = label.slice(0, -2) + "…"; ctx.fillText(label, p.x + 8, p.y + 15);
    ctx.font = "10px JetBrains Mono, monospace"; ctx.fillStyle = col; ctx.fillText(s.state + (s.worker && s.state === "running" ? " · " + s.worker : "") + (s.attempt > 1 ? " · try " + s.attempt : ""), p.x + 8, p.y + 29);
  }
}
const WCOL = ["#7c5cff", "#00d2ff", "#22e3a1", "#ffb547", "#ff5c8a", "#4f8bff", "#c06bff", "#5ce1e6"];
function drawGantt() {
  const { ctx, w, h: H } = fit($("#gantt")); ctx.clearRect(0, 0, w, H);
  const ws = E.workers, top = 30, lh = Math.min(26, (H - top - 8) / ws.length), L = 44, win = 20, t0 = E.now - win, X = (t) => L + ((t - t0) / win) * (w - L - 10);
  ctx.font = "10px JetBrains Mono, monospace"; ctx.textBaseline = "middle";
  ws.forEach((wk, i) => {
    const y = top + i * lh; ctx.fillStyle = wk.alive ? "#8d94ad" : "#ff5c8a"; ctx.textAlign = "left"; ctx.fillText(wk.id + (wk.alive ? "" : " ✕"), 8, y + lh / 2);
    ctx.fillStyle = "#ffffff08"; ctx.fillRect(L, y + 2, w - L - 10, lh - 4);
    const seg = (a, b, col, ok, label) => { const x1 = Math.max(L, X(a)), x2 = X(b); if (x2 < L) return; ctx.fillStyle = ok ? hexA(col, 0.75) : "#ff5c8acc"; ctx.beginPath(); ctx.roundRect(x1, y + 4, Math.max(2, x2 - x1 - 1), lh - 8, 4); ctx.fill(); if (x2 - x1 > 60) { ctx.fillStyle = "#06070d"; ctx.textAlign = "left"; ctx.fillText(label.slice(0, Math.floor((x2 - x1 - 8) / 6.2)), x1 + 5, y + lh / 2); } };
    for (const s of spans) if (s.w === wk.id) seg(s.a, s.b, WCOL[+s.run.split("-")[1] % WCOL.length], s.ok, s.task);
    for (const o of open.values()) if (o.w === wk.id) seg(o.a, E.now, WCOL[+o.run.split("-")[1] % WCOL.length], true, o.task);
  });
  ctx.strokeStyle = "#ffffff30"; ctx.beginPath(); ctx.moveTo(X(E.now), top - 6); ctx.lineTo(X(E.now), H - 4); ctx.stroke();
}
let acc = 0, tps = 0;
function render() {
  const s = E.stats(), runs = [...E.runs.values()], last = runs.filter((r) => r.ended != null).at(-1);
  kp.set({ runs: runs.length, ok: runs.filter((r) => r.status === "completed").length, tasks: s.completed, retries: s.retries, busy: s.busy, span: last ? last.ended - last.started : 0 });
  drawDag(); drawGantt();
}
const loop = new Loop((dt) => {
  E.advance(dt); acc += dt;
  if (acc >= 0.5) { const q = runsQueued(); tps = tps * 0.7 + ((done - lastDone) / acc) * 0.3; ch1.push(tps); ch2.push(q, E.stats().busy); lastDone = done; acc = 0; }
  render();
});
const runsQueued = () => [...E.runs.values()].reduce((a, r) => a + r.queue.length, 0);
bindLoop(loop, $("#sim"));
$("#start").onclick = () => { E.start(WF); toast("Started a concurrent run — workers are shared"); if (!loop.running) loop.start(); };
$("#crash").onclick = () => { const w = E.workers.find((x) => x.busy && x.alive); if (!w) return toast("No busy worker right now"); E.crash(w.id); toast(`${w.id} crashed — its task returns after the ${E.heartbeat}s heartbeat timeout`); };
$("#workers").oninput = (e) => { E.setWorkers(+e.target.value); $("#wo").textContent = e.target.value; };
$("#chaos").oninput = (e) => { E.failRate = +e.target.value / 100; $("#co").textContent = e.target.value + "%"; };
$("#replay").onclick = () => {
  const t0 = performance.now(), R = replay(E.history), ms = performance.now() - t0; let diffs = 0, n = 0;
  for (const [id, run] of E.runs) { const r = R.get(id); for (const [tid, t] of run.tasks) { n++; const rs = r.tasks[tid]?.state || "waiting"; if (rs !== t.state) diffs++; } }
  $("#rep").innerHTML = `<div><span class="${diffs ? "bad" : "ok"}">${diffs ? "✗" : "✓"} ${R.size} runs · ${n} task states rebuilt from ${E.history.length} events</span></div><div class="dim">replay took ${ms.toFixed(2)}ms · ${diffs} mismatches</div>` + [...R.values()].slice(-3).map((r) => `<div><span class="t">${r.id}</span> <span class="${r.status === "completed" ? "ok" : r.status === "failed" ? "bad" : "acc"}">${r.status}</span> ${r.makespan ? `<span class="dim">${r.makespan}s</span>` : ""}</div>`).join("");
};
const cronUpd = () => { try { $("#cronout").innerHTML = nextRuns($("#cron").value, new Date(), 6).map((d) => `<div><span class="acc">●</span> ${d.toISOString().replace("T", " ").slice(0, 16)} <span class="dim">UTC · ${d.toUTCString().slice(0, 3)}</span></div>`).join(""); } catch (e) { $("#cronout").innerHTML = `<div class="bad">${esc(e.message)}</div>`; } };
$("#cron").oninput = cronUpd; cronUpd();
newRun(); render(); loop.start(); setTimeout(() => $("#replay").click(), 4000);
addEventListener("resize", render);
