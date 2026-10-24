# exp01: durable LangGraph on HPX supervision dispatch

**Status:** phase 3 (coordinator, commit fence, effects, F0). Design and gates are in [`docs/plan.md`](../../docs/plan.md).

## Question

Can a LangGraph agent whose nodes run on HPX localities survive worker kill, worker freeze, worker replacement and coordinator restart with no lost step, no double commit, no accepted stale result, and exactly-once outbox effects?

## Contents

| Path | Purpose |
| --- | --- |
| `agent_nodes.py` | node functions of `research_agent`; run in-process for the baseline and on HPX workers; no LangGraph import |
| `graph.py` | the `research_agent` graph (plan section 10), the in-process node runner, state hash |
| `fixtures/research_agent.json` | stub LLM responses keyed by prompt hash |
| `make_fixtures.py` | records the fixtures from a deterministic generator (there is no real model) |
| `run_f0.py` | F0: the same runs as a stock LangGraph baseline and on HPX workers; compares state hashes and outbox keys |
| `f0.sbatch` | runs `run_f0.py` on one Rostam node |
| `_runs/` | raw run output, ignored by git |

Planned for phase 4: `run_exp01.py` (F1 to F9) and `analyze_exp01.py` (gates G1 to G5).

## Running F0

After building the extension and root (top-level README) and installing `requirements.txt`:

```bash
python experiments/01_durable_langgraph/run_f0.py --out experiments/01_durable_langgraph/_runs/f0
```

On Rostam: `HPX_INSTALL=<hpx install> LANGX_VENV=<venv> sbatch experiments/01_durable_langgraph/f0.sbatch`.

`make_fixtures.py` only needs to run again if a prompt changes; a prompt without a recorded response is an error.
