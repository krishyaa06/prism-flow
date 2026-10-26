# Prism Flow — Usage

## Install

```bash
npm install github:krishyaa06/prism-flow
# or run the CLI directly
npx github:krishyaa06/prism-flow --help
```

## JavaScript API

```js
import { Engine, replay, execute } from "prism-flow";

const wf = { name: "etl", tasks: [
  { id: "extract", duration: 1, retries: 3 },
  { id: "transform", deps: ["extract"], timeout: 5 },
  { id: "load", deps: ["transform"], backoff: { base: 0.5, cap: 10, jitter: "full" } },
]};

// 1) deterministic simulation on a virtual clock
const e = new Engine({ workers: 3, seed: 1, failRate: 0.1 });
e.on((ev) => console.log(ev.t, ev.type, ev.task ?? ""));
e.start(wf); e.drain();
console.log(replay(e.history).get("run-1").status); // "completed"

// 2) run real async handlers with the same semantics
const out = await execute(wf, {
  extract: async () => fetch("https://api.example.com/rows").then((r) => r.json()),
  transform: async ({ results }) => results.extract.map(clean),
  load: async ({ results }) => db.insert(results.transform),
}, { concurrency: 4 });
```

| Export | What it does |
|---|---|
| `new Engine({ workers, seed, heartbeat, failRate })` | Virtual-clock engine |
| `engine.start(wf)` → `runId` | Validate and start a workflow run |
| `engine.advance(dt)` / `engine.drain()` | Advance time / run to completion |
| `engine.crash(workerId)` · `engine.setWorkers(n)` | Chaos and scaling |
| `engine.history` · `engine.on(fn)` · `engine.stats()` | Event log, subscription, counters |
| `replay(history)` | Rebuild run/task state from events |
| `execute(wf, handlers, { concurrency })` | Run real async handlers |
| `validate` · `topo` · `levels` · `criticalPath` | DAG utilities |
| `backoff(attempt, { base, cap, factor, jitter })` | Retry delay |
| `parseCron(expr)` · `nextRuns(expr, from, n)` | Cron |

## CLI

```bash
$ npx github:krishyaa06/prism-flow validate examples/ml-pipeline.json
$ npx github:krishyaa06/prism-flow run examples/ml-pipeline.json --workers 3 --fail 0.2 --history h.json
$ npx github:krishyaa06/prism-flow replay h.json
$ npx github:krishyaa06/prism-flow cron "*/15 9-17 * * mon-fri" --n 5
```

| Command | Description |
|---|---|
| `flow run <wf.json> [--workers] [--seed] [--fail] [--history f]` | Simulate, print a Gantt chart and stats |
| `flow validate <wf.json>` | Validate the DAG and print the critical path |
| `flow replay <history.json>` | Rebuild state from a saved history |
| `flow cron "<expr>" [--n] [--from]` | Next fire times in UTC |

## Live simulator

Open [https://krishyaa06.github.io/prism-flow/](https://krishyaa06.github.io/prism-flow/) or serve the repo root locally (`npx serve .`). The simulator imports the same modules from `src/`.
