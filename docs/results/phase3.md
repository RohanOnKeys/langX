# Phase 3 coordinator, commit fence and effects on HPX master (cc2be553)

**Date:** 2026-10-25. **Cluster:** Rostam, partition `medusa`, one node per job. **HPX:** master
`cc2be553fac9b1f2a372f4277d7c708e70bf7eb4`. **Plan:** [`docs/plan.md`](../plan.md)
section 16, phase 3.

Code: [`python/src/langx/`](../../python/src/langx/) (`coordinator.py`, `effects.py`,
`stub_llm.py`, `island.py`), [`experiments/01_durable_langgraph/`](../../experiments/01_durable_langgraph/).
Earlier results: [`phase0.md`](phase0.md), [`phase1.md`](phase1.md), [`phase2.md`](phase2.md).

## Summary

* **Exit criterion met: F0 on 2 workers equals the baseline.** The same `research_agent` run, once
  as stock LangGraph in one process and once with every node on HPX workers behind the commit
  fence, gave the same final state hash in 5/5 runs, the same outbox effect keys, no duplicate
  key, one commit per task (115 tasks) and no retry or rejected result.
* **The coordinator and fence are covered without HPX too.** 42 unit tests drive every path through
  `Coordinator.execute` with a fake runtime: commit, fence and retry, timeout retry, each commit
  fence rejection, re-execution of an accepted task, node errors and the retry bound.
* **The stub LLM refuses unrecorded prompts.** A first integration test asked for a width with no
  recorded fixtures and stopped with `StubMiss`, as designed.

## Setup

| Item | Value |
|---|---|
| Topology | `langx_root` (locality 0), workers `w1`, `w2`, coordinator in the experiment process, one node |
| Python stack | Python 3.12.3, LangGraph 1.2.16, venv `langx` |
| Detection hook | poll timeout 500 ms on every locality |
| Graph | `research_agent`: plan, search fan-out (Send), reduce, critique, loop or finalize |
| F0 parameters | width 8, 2 loops, 50 ms search latency, 5 runs |
| Checkpointer | `InMemorySaver` |
| Model | stub LLM replaying 48 recorded responses (`fixtures/research_agent.json`, from `make_fixtures.py`) |

| Job | Purpose |
|---|---|
| 218811 | build, unit and integration tests |
| 218812 | F0, 5 runs |
| 219461 | F0 integration test alone, with the subprocess output shown |
| 219462 | unit and integration tests after the test fix |

## What was built

| Component | Role |
|---|---|
| `EpochTable` | the coordinator's own record of live (locality, epoch); a fenced worker is never un-fenced |
| `Coordinator.execute` | picks a live worker round robin, dispatches with an attempt id, retries elsewhere after `TargetFenced` or `DispatchFailed` (fencing the worker) or a timeout (not fencing), up to `max_attempts`; node errors are raised, not retried |
| `Coordinator.commit` | commit fence under a per-task lock: accept only if nothing was accepted for (run, step, node), the reply carries this attempt id and the dispatching (locality, epoch), that pair is live in the table, and HPX still admits the worker; then apply the outbox |
| `Coordinator.remote` | async LangGraph node that runs a node function on a worker; the task key is (thread id, superstep, node key) |
| `Outbox`, `EffectLog` | Tier A: intents applied after the fence, keyed by `sha256(run, step, node, index)`, unique in an SQLite log |
| `DirectSink` | Tier B: a node writes its effect itself, `naive` or `keyed` |
| `StubLLM` | replays responses by prompt hash; a miss is an error; record mode builds fixtures |
| `langx.island` | launches root and workers; moved from the tests so the experiment and spikes share it |

The baseline runs the same node functions through the same keyed outbox, so the two runs differ
only in where each node executes.

## Results

### F0 (job 218812)

| Check | Result |
|---|---|
| Final state hash, HPX against baseline | equal, 5/5 |
| Outbox effect keys | equal, 5 and 5 |
| Duplicate effect keys | 0 |
| Tasks committed | 115 (23 per run), each once; 58 on `w1`, 57 on `w2` |
| Retries, rejected results | 0 |
| Shutdown | both workers clean |
| Wall time per run | baseline 0.112 to 0.118 s, HPX 0.414 to 0.417 s |

The HPX runs are slower mainly because of where the search nodes run, not dispatch cost. The
baseline runs the 8 searches of a loop in parallel threads, about 50 ms per loop. Each HPX worker
runs one node at a time (plan 8.3), so 8 searches on 2 workers take 4 rounds of 50 ms per loop.
Dispatch overhead is measured separately (next step).

### Tests

| Job | Unit | Integration | Note |
|---|---|---|---|
| 218811 | 42/42 | 8/9 | the F0 test asked for width 4, which has no recorded fixtures |
| 219462 | 42/42 | 9/9 | the F0 test uses width 3; `run_f0.py` now rejects unrecorded widths up front |

## Not established

* behaviour under failure; F1 to F9 are phase 4;
* recovery from a checkpoint: the checkpointer is `InMemorySaver`, and F7 needs a durable one
  (SQLite, plan 6.1);
* Tier B direct effects inside a run; `DirectSink` is unit tested only;
* F0 at other widths, loop counts and latencies, and with more workers;
* the split of the HPX round trip between HPX and the Python side;
* any cross-node measurement.

## Claims and non-claims

Supported:

> On HPX master `cc2be553`, on one Rostam node, a LangGraph agent whose nodes all run on two HPX
> workers through the langx coordinator and commit fence produced the same final state and the
> same outbox effects as stock LangGraph in 5/5 failure-free runs, with every task committed once.

Not claimed:

* no claim about behaviour under failure;
* no performance claim; the wall times above reflect worker count, not dispatch cost.

## Next step

Split the round trip into HPX and Python costs, then phase 4: the failure harness for F1 to F9.
