# Phase 0 spikes

The code behind [`docs/results/phase0.md`](../../docs/results/phase0.md). Kept so those results can be reproduced. It is not the langx API: the bindings were rewritten in [`python/`](../../python/) and [`hpx_impl/`](../../hpx_impl/) in phase 1, and the HPX reproducers were rewritten in [`bench/hpx_repro/`](../../bench/hpx_repro/).

| Spike | Question | Where |
| --- | --- | --- |
| F | Can a Python connector call `dispatch_work` with a generic `run_node` action on another connector? Can a connector call `discover_and_join` and `force_disconnect` itself? | `p0_coord.py` `spike_f`, `spike_a` |
| A' | SIGKILL to coordinator-side detection latency on one node; `snapshot_peers()` after detection; an in-flight dispatch | `spike_a` |
| B | SIGSTOP past detection, then SIGCONT: does the fence hold, does the late result arrive, does the target-side re-check admit the resumed worker? | `spike_b` |
| B8 | SIGSTOP shorter than one poll window: is a healthy worker evicted? | `spike_b8` |
| D | LangGraph async proxies: concurrency at width 32, no retry, resume re-running only the failed branch | `p0_spike_d.py`, no HPX |

## Files

```text
native/p0_ext.cpp          _p0 pybind11 module, loaded by coordinator and workers
native/p0_root.cpp         work-free root (locality 0), watchdogged shutdown
native/p0_membership.hpp   root-side force_disconnect action
native/repro_leave.cpp     first C++ reproducer of hpx#7825 (needs p0_root)
p0_worker.py               worker locality, Python node handler on one OS thread
p0_coord.py                coordinator locality, runs F, A', B, B8 and sends the signals
p0_common.py               files, ports, HPX arguments, connector shutdown
run_phase0.py              launches islands, sweeps the poll timeout, writes summary.json
phase0.sbatch              build, smoke test, optional shutdown check, sweep, tarball
repro_leave.sbatch         runs repro_leave.cpp against p0_root
p0_spike_d.py              spike D
```

## Running on Rostam

From this folder:

```bash
module load python/3.12.3
source /work/rohan/venvs/rayx/bin/activate        # Python 3.12 with pybind11

# spikes A', B, B8, F on one node
HPX_INSTALL=/work/rohan/apps/hpx-master-install P0_VENV=/work/rohan/venvs/rayx \
  sbatch phase0.sbatch

# spike D, no HPX
python -m venv /work/rohan/venvs/langx-p0d312
source /work/rohan/venvs/langx-p0d312/bin/activate
pip install langgraph==1.2.16
python p0_spike_d.py --out spike_d.json
```

Options for `phase0.sbatch`: `P0_POLLS` (default `250,500,1000`), `P0_REPS` (default 5), `P0_SKIP_BUILD=1`, `P0_SHUTDOWN_REPS`, `P0_SHUTDOWN_MODES`, `P0_SHUTDOWN_WAITS`. Results go to `_runs/<job>/`, which git ignores.

## Caveats

- All processes share one node, so one clock covers every timestamp.
- The coordinator sends the signals itself. Detection is read only from its HPX supervision state.
- The poll timeout is set with `set_failure_detection_poll_timeout_for_testing` on every locality before `init()`. Detection latencies only mean something next to the poll value used.
