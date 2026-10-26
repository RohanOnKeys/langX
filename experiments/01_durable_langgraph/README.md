# exp01: durable LangGraph on HPX supervision dispatch

**Status:** F0 passes on 2 workers ([`phase3.md`](../../docs/results/phase3.md)); F1 to F9 are phase 4. Design and gates are in [`docs/plan.md`](../../docs/plan.md).

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
| `run_exp01.py` | F1 to F9: one process per trial, fresh island, one injected fault, JSON lines output |
| `analyze_exp01.py` | gates G1 to G5 and metrics from the trial records; writes `aggregate.json` |
| `exp01.sbatch` | runs every scenario on one Rostam node, then the analysis |
| `_runs/` | raw run output, ignored by git |

## Running F0

After building the extension and root (top-level README) and installing `requirements.txt`:

```bash
python experiments/01_durable_langgraph/run_f0.py --out experiments/01_durable_langgraph/_runs/f0
```

On Rostam: `HPX_INSTALL=<hpx install> LANGX_VENV=<venv> sbatch experiments/01_durable_langgraph/f0.sbatch`.

## Running the failure matrix

```bash
python experiments/01_durable_langgraph/run_exp01.py --scenario F2 --trials 10 --out experiments/01_durable_langgraph/_runs/exp01
python experiments/01_durable_langgraph/analyze_exp01.py experiments/01_durable_langgraph/_runs/exp01/exp01.jsonl
```

The injection for each scenario is described at the top of `run_exp01.py`. F7 needs `langgraph-checkpoint-sqlite` (`requirements.txt`). On Rostam: `HPX_INSTALL=<hpx install> LANGX_VENV=<venv> sbatch experiments/01_durable_langgraph/exp01.sbatch`, with `TRIALS` and `SCENARIOS` to narrow it.

`make_fixtures.py` only needs to run again if a prompt changes; a prompt without a recorded response is an error.
