// 5-field cron parser (minute hour day-of-month month day-of-week) with next-run calculation.
const RANGES = [[0, 59], [0, 23], [1, 31], [1, 12], [0, 6]];
const NAMES = [null, null, null, ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]];
const MACROS = { "@hourly": "0 * * * *", "@daily": "0 0 * * *", "@weekly": "0 0 * * 0", "@monthly": "0 0 1 * *", "@yearly": "0 0 1 1 *" };
function field(src, i) {
  const [lo, hi] = RANGES[i], set = new Set();
  for (const part of src.toLowerCase().split(",")) {
    const m = part.match(/^(\*|[a-z0-9]+(?:-[a-z0-9]+)?)(?:\/(\d+))?$/); if (!m) throw new Error(`Bad cron field "${src}"`);
    const val = (s) => { const n = NAMES[i]?.indexOf(s); const v = n >= 0 ? n + (i === 3 ? 1 : 0) : Number(s); if (!Number.isInteger(v) || v < lo || v > hi + (i === 4 ? 1 : 0)) throw new Error(`Value "${s}" out of range ${lo}-${hi}`); return i === 4 && v === 7 ? 0 : v; };
    let a = lo, b = hi; if (m[1] !== "*") { const [x, y] = m[1].split("-"); a = val(x); b = y !== undefined ? val(y) : m[2] ? hi : a; }
    const step = m[2] ? Number(m[2]) : 1; if (step < 1) throw new Error("Step must be ≥ 1");
    for (let v = a; v <= b; v += step) set.add(v);
  }
  return set;
}
export function parseCron(expr) {
  const e = MACROS[expr.trim()] || expr.trim(); const parts = e.split(/\s+/); if (parts.length !== 5) throw new Error(`Cron needs 5 fields, got ${parts.length}`);
  const [min, hour, dom, mon, dow] = parts.map(field);
  return { min, hour, dom, mon, dow, domStar: parts[2] === "*", dowStar: parts[4] === "*" };
}
/** Next fire times after `from` (UTC). Vixie semantics: if both DOM and DOW restricted, either matches. */
export function nextRuns(expr, from = new Date(), n = 5) {
  const c = typeof expr === "string" ? parseCron(expr) : expr, out = []; const d = new Date(from.getTime()); d.setUTCSeconds(0, 0); d.setUTCMinutes(d.getUTCMinutes() + 1);
  let guard = 0;
  while (out.length < n && guard++ < 600000) {
    if (!c.mon.has(d.getUTCMonth() + 1)) { d.setUTCMonth(d.getUTCMonth() + 1, 1); d.setUTCHours(0, 0); continue; }
    const domOk = c.dom.has(d.getUTCDate()), dowOk = c.dow.has(d.getUTCDay());
    const dayOk = c.domStar && c.dowStar ? true : c.domStar ? dowOk : c.dowStar ? domOk : domOk || dowOk;
    if (!dayOk) { d.setUTCDate(d.getUTCDate() + 1); d.setUTCHours(0, 0); continue; }
    if (!c.hour.has(d.getUTCHours())) { d.setUTCHours(d.getUTCHours() + 1, 0); continue; }
    if (!c.min.has(d.getUTCMinutes())) { d.setUTCMinutes(d.getUTCMinutes() + 1); continue; }
    out.push(new Date(d)); d.setUTCMinutes(d.getUTCMinutes() + 1);
  }
  return out;
}
