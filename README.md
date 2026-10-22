<p align="center"><img src="docs/assets/banner.png" alt="langX" width="600"></p>

# langX

Research prototype: LangGraph agent steps executed on HPX localities, with HPX supervision fencing and a coordinator-side commit fence, so that a worker failure neither loses nor double-commits a step.

**Status:** phase 0 (spikes) is done and phase 1 (bindings core) is in review. The full experiment plan is in [`docs/plan.md`](docs/plan.md).

## Results

| Phase | Write-up |
| --- | --- |
| 0: spikes on HPX supervision | [`docs/results/phase0.md`](docs/results/phase0.md) |
| 1: bindings core | [`docs/results/phase1.md`](docs/results/phase1.md) |

Upstream report from these results: [TheHPXProject/hpx#7825](https://github.com/TheHPXProject/hpx/issues/7825).

## The question

Agent frameworks such as LangGraph checkpoint state between steps, but they leave failure handling for the processes that run those steps to the user. When a worker dies or freezes in the middle of a step, the framework can re-run the step, and any side effect the step already caused can happen twice.

HPX now ships native locality supervision (`components/supervision_dispatch`): heartbeat-based failure detection, per-epoch terminal latching, and fenced dispatch that refuses work for a target already declared failed. langX asks whether that mechanism, plus a small commit fence on the coordinator, is enough to give a real agent framework these guarantees:

- no committed step is lost when a worker is killed
- no step is committed twice
- a frozen worker that wakes up late cannot commit a stale result
- effects routed through an outbox are applied exactly once

## Architecture

```text
Root, HPX locality 0            work-free, anchors the runtime, out of failure scope
Coordinator (Python + HPX)      LangGraph graph, proxy nodes, epoch table,
                                commit fence, outbox, checkpointer
Workers 1..N (Python + HPX)     execute node functions through one generic
                                HPX action, stateless between calls
```

All localities except the root connect late, in HPX connect mode. LangGraph keeps ownership of the graph, supersteps and checkpoints. Each graph node is a proxy that dispatches the real node function to a worker through `dispatch_work`, checks the result against the commit fence, and only then hands it back to LangGraph.

## What is tested

A deterministic `research_agent` graph (stub LLM with recorded responses, parallel fan-out, conditional loop, one outbox effect) runs under a failure matrix:

| ID | Failure |
| --- | --- |
| F0 | none |
| F1, F2 | worker killed before or during a step |
| F3 | worker killed after a direct side effect, before replying |
| F4 | worker frozen past detection, then resumed |
| F5 | two of four workers killed in one superstep |
| F6 | worker killed and replaced by a new locality |
| F7 | coordinator killed and restarted from checkpoint |
| F8 | worker frozen briefly, below detection |
| F9 | dispatch abandoned on timeout, original result arrives late |

Correctness gates: final state hash equal to a failure-free run, zero duplicate outbox effects, zero accepted stale commits, every run completes. Detection latency, recovery latency, dispatch overhead and scaling are reported alongside.

## What langX does not claim

- No exactly-once guarantee for side effects a worker performs directly. Those are at-least-once unless the external system honors an idempotency key, and the results report this separately.
- No recovery from loss of the HPX root.
- No deterministic scheduling, no accelerator placement, no speedup claim.
- No real LLM calls. All model responses are replayed from fixtures.
- Not a production runtime, and not a replacement for Ray.

## Relationship to RayX and Ray

langX builds on [RayX](https://github.com/byekrang/RayX), a research prototype in which Ray actors host HPX localities in-process. RayX experiment 70 established the supervision mechanism this project depends on: native detection of a killed locality, specific `target_fenced` dispatch outcomes, `force_disconnect`, and replacement localities, reproduced on HPX master `cc2be553`.

The core failure matrix runs without Ray, so that every recovery can be attributed to HPX supervision and the commit fence rather than to Ray actor restarts. Ray remains the intended outer layer: placement, and whole-island restart for failures HPX cannot recover from, such as loss of the root.

A reference snapshot of the RayX experiments and results is kept in [`rayx/`](rayx/README.md).

## Layout

The layout follows RayX.

```text
python/                     _langx pybind11 extension (CMake) and the langx package
  src/langx/
    runtime.py              facade over the HPX extension
    coordinator.py          proxy nodes, epoch table, commit fence, retry
    worker.py               run_node serving on one locality
    effects.py              outbox, effect log, direct sink
    stub_llm.py             recorded-response LLM stub
hpx_impl/                   native root locality and shared C++ headers
experiments/NN_name/        one folder per experiment: write-up, runner, sbatch, curated aggregates
bench/                      smoke and repo-integrity helpers
tests/unit/                 import-light tests, no HPX needed
tests/integration/          tests that need the built extension, skipped otherwise
docs/plan.md                experiment plan: claims, design, failure matrix, gates, phases
docs/results/               one write-up per phase
rayx/                       reference snapshot of RayX experiments and results
```

## Build (once implemented)

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt -r requirements-python.txt -r requirements-dev.txt
cmake -S python -B python/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DPYBIND11_FINDPYTHON=ON \
  -DCMAKE_PREFIX_PATH="<hpx-install>;$(python -m pybind11 --cmakedir)" \
  -DPython_EXECUTABLE="$(which python)"
cmake --build python/build
PYTHONPATH=python/src pytest tests
```

Only `tests/unit` and the markdown link check run in CI. Integration tests and experiments need HPX and run locally or on Rostam.

## Requirements (planned)

- HPX master with `supervision_dispatch`, `force_disconnect`, networking and the TCP parcelport enabled (tested target: `cc2be553`)
- Python 3.12, pybind11, LangGraph (version to be pinned)
- Slurm cluster for multi-node runs (Rostam at LSU)

## License

Not yet chosen.
