# exp01: durable LangGraph on HPX supervision dispatch

**Status:** not started. Design and gates are in [`docs/plan.md`](../../docs/plan.md).

## Question

Can a LangGraph agent whose nodes run on HPX localities survive worker kill, worker freeze, worker replacement and coordinator restart with no lost step, no double commit, no accepted stale result, and exactly-once outbox effects?

## Contents (planned)

| Path | Purpose |
| --- | --- |
| `graph.py` | `research_agent` test graph (plan section 10) |
| `fixtures/` | stub LLM responses keyed by prompt hash |
| `run_exp01.py` | harness: launch root, coordinator and workers, inject F0 to F9, write JSON lines |
| `*.sbatch` | Rostam launchers in HPX connect mode |
| `analyze_exp01.py` | gates G1 to G5 and reported metrics from the JSON lines |
| `_runs/` | raw run output, ignored by git |

Curated `aggregate.json` files next to this write-up are tracked; raw runs are not.
