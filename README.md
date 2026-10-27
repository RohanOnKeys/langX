<p align="center"><img src="docs/assets/banner.png" alt="langX" width="100%"></p>

# langX

Research prototype: LangGraph agent steps executed on HPX localities, with HPX supervision fencing and a coordinator-side commit fence, so that a worker failure neither loses nor double-commits a step.

**Status:** phases 0 to 3 are done; phase 4 (failure harness, F1 to F9) is next. The full experiment plan is in [`docs/plan.md`](docs/plan.md).

## Results

| Phase | Write-up |
| --- | --- |
| 0: spikes on HPX supervision | [`docs/results/phase0.md`](docs/results/phase0.md) |
| 1: bindings core | [`docs/results/phase1.md`](docs/results/phase1.md) |
| 2: coordinator loss | [`docs/results/phase2.md`](docs/results/phase2.md) |
| 3: coordinator, commit fence, effects; F0 | [`docs/results/phase3.md`](docs/results/phase3.md) |
| dispatch overhead split: HPX vs Python | [`docs/results/overhead.md`](docs/results/overhead.md) |

Upstream reports from these results: [hpx#7825](https://github.com/TheHPXProject/hpx/issues/7825) and the tracking issue [hpx#7834](https://github.com/TheHPXProject/hpx/issues/7834).

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

Files derived from RayX name the source file and author in their header.

## Layout

```text
python/                     _langx extension and the langx package (CMake builds both _langx and langx_root)
  src/langx/
    _langx.cpp              pybind11 bridge to an in-process HPX locality
    runtime.py              asyncio facade over _langx: dispatch, fencing, membership, shutdown
    worker.py               worker entry point: python -m langx.worker
    coordinator.py          proxy nodes, epoch table, commit fence, retry
    effects.py              outbox, effect log, direct sink
    stub_llm.py             recorded-response LLM stub
    island.py               launches langx_root and workers on one host
hpx_impl/                   native C++ outside Python: the root locality and the membership action
tests/unit/                 no HPX needed; run in CI
tests/integration/          live island on one host; needs the built extension
experiments/01_durable_langgraph/  the experiment: graph, harness, sbatch, curated aggregates (phases 3 to 5)
spikes/phaseN/              code behind docs/results/phaseN.md; not the langx API
bench/                      Rostam test job, overhead benchmark, markdown link check
bench/setup/                Rostam environment, HPX build, venv setup
bench/hpx_repro/            pure C++ reproducers for HPX issues (#7825, #7834)
docs/plan.md                experiment plan: claims, design, failure matrix, gates, phases
docs/results/               one write-up per phase
docs/reproduce.md           the command and jobs behind every result
```

## Build and test

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt -r requirements-build.txt -r requirements-dev.txt
cmake -S python -B python/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DPYBIND11_FINDPYTHON=ON \
  -DCMAKE_PREFIX_PATH="<hpx-install>;$(python -m pybind11 --cmakedir)" \
  -DPython_EXECUTABLE="$(which python)"
cmake --build python/build          # python/src/langx/_langx*.so and python/build/langx_root
PYTHONPATH=python/src pytest tests
```

On Rostam, set up once with `bench/setup/` (HPX, venv, environment); [`docs/reproduce.md`](docs/reproduce.md) lists the command behind every result. The same build and both test suites run as one job: `HPX_INSTALL=<hpx install> LANGX_VENV=<venv> sbatch bench/rostam_test.sbatch`.

Only `tests/unit` and the markdown link check run in CI. Integration tests, spikes and experiments need HPX and run on Rostam.

## Requirements

- HPX master with `supervision_dispatch`, `force_disconnect`, networking and the TCP parcelport enabled (tested: `cc2be553`)
- Python 3.12, pybind11, LangGraph 1.2.16
- Slurm cluster for multi-node runs (Rostam at LSU)

## License

Not yet chosen.
