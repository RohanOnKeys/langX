# Phase 1 bindings core on HPX master (cc2be553)

**Date:** 2026-10-22. **Cluster:** Rostam, partition `medusa`, one node per job. **HPX:** master
`cc2be553fac9b1f2a372f4277d7c708e70bf7eb4`. **Plan:** [`docs/plan.md`](../plan.md)
section 16, phase 1.

Code: branch `phase1-bindings`, [pull request #2](https://github.com/RohanOnKeys/langX/pull/2).
Phase 0 results: [`phase0.md`](phase0.md).

## Summary

* **Exit criterion met.** A Python coordinator on a root plus workers discovers the workers,
  reads their epochs and dispatches nodes to them. Unit tests 18/18, integration tests 8/8.
* **The three phase 0 design changes work.** The root runs `force_disconnect` on the
  coordinator's request, a dispatch whose target is fenced in flight is abandoned instead of
  waiting forever, and the island shuts down cleanly with a pause after `supervision::finalize()`.
* **New finding: a dead observer marks a healthy target failed.** A worker that joined an older
  worker at init and then died left the older worker fenced on its own state within 0.1 s (4/4).
  With workers joining no peers at init, no survivor was fenced (0/4).

## Setup

| Item | Value |
|---|---|
| HPX source | `TheHPXProject/hpx` master at `cc2be553`, existing install at `/work/rohan/apps/hpx-master-install` |
| Toolchain | gcc 15.1.0, cmake 3.29.2, boost 1.91.0, hwloc 2.12.0 (Rostam modules) |
| Python stack | Python 3.12.3, pybind11, pytest (venv `langx`) |
| Topology | `langx_root` (locality 0), workers `w1`, `w2`, coordinator in the test process, all late-connecting, one node |
| Transport | TCP parcelport on `localhost` |
| Detection hook | poll timeout 500 ms on every locality |

| Job | Purpose |
|---|---|
| 216761 | first build and full test run |
| 216762 | survivor fencing diagnostic, workers joining at init |
| 216785 | survivor fencing diagnostic with and without joins at init, full test run |

## What was built

| Component | Path | Role |
|---|---|---|
| `_langx` | `python/src/langx/_langx.cpp` | connect-mode start and stop, supervision init and finalize, join, admission, peer state, `run_node` under `dispatch_work`, completion pipe for asyncio |
| `langx_root` | `hpx_impl/root/langx_root.cpp` | work-free root (locality 0) |
| membership action | `hpx_impl/langx/membership.hpp` | root-side `force_disconnect` on request |
| runtime | `python/src/langx/runtime.py` | async `dispatch_node`, `TargetFenced` and `DispatchFailed`, in-flight abandonment, bounded finalize with pause |
| worker | `python/src/langx/worker.py` | dotted-path nodes, errors as values, replies tagged with attempt id, locality and epoch |

Results reach Python through a pipe that the asyncio loop watches, so no HPX thread takes the
GIL. Python nodes run on one dedicated OS thread per worker.

## Results

### Tests

| Job | Unit | Integration | Notes |
|---|---|---|---|
| 216761 | 18/18 | 6/8 | built first time; two failures, below |
| 216785 | 18/18 | 8/8 | after both fixes |

| Integration test | 216785 |
|---|---|
| joins workers with their epochs | pass |
| dispatch runs on the target, attempt id and epoch echoed | pass |
| node error returned as a value | pass |
| 4 nodes of 200 ms over 2 workers in under 0.6 s | pass |
| in-flight dispatch abandoned after SIGKILL, within 10 poll windows | pass |
| dispatch to a fenced worker refused | pass |
| root `force_disconnect` of the killed worker, survivor still serves | pass |
| whole island shuts down cleanly (workers, coordinator, root) | pass |

The two failures in job 216761:

* **Refusal reported as abandonment (langX).** With the target already fenced, the fence watch
  returned before the refusal arrived through the completion pipe. `dispatch_node` now checks
  admission before sending.
* **Healthy survivor fenced (HPX behaviour).** After `w2` was killed and removed, `w1` refused
  work through its own target-side check. Investigated below.

### Survivor fenced by a dead observer

[`spikes/phase1/diag_survivor_fence.py`](../../spikes/phase1/diag_survivor_fence.py): kill one worker, then dispatch to the other every 100 ms for 4 s,
then have the root `force_disconnect` the dead one and dispatch for 2 s more. `w2` starts after
`w1`.

| Worker discovery at init | Joined at init | Killed | Survivor | Survivor after the kill | After root `force_disconnect` | Runs |
|---|---|---|---|---|---|---|
| 5000 ms | `w1`: root; `w2`: root, `w1` | `w2` | `w1` | fenced from 0.1 s | fenced | 2/2 (216762), 2/2 (216785) |
| 5000 ms | same | `w1` | `w2` | ok | ok | 2/2, 2/2 |
| 0 ms | none | `w2` | `w1` | ok | ok | 2/2 (216785) |
| 0 ms | none | `w1` | `w2` | ok | ok | 2/2 (216785) |

In the fenced runs the coordinator's own view of the survivor was also fenced.

**Cause.** In `libs/full/supervision/src/server/supervision_manager_server.cpp` at `cc2be553`,
`publish_event(target, ...)` delivers the event to every observer of `target`, and if any
delivery fails it calls `record_error(target, ...)`, which sets `target`'s state to `failed`. When
`w2` joins `w1`, `w2` becomes an observer of `w1`. Once `w2` is dead, the next event `w1`
publishes on itself fails to reach `w2`, and `w1` is marked failed. The timing (fenced within
0.1 s) fits the periodic `running` heartbeat, but the triggering publication was not traced. The comment in `record_error` says the transition is deliberate,
so this may be intended rather than a defect.

**Workaround.** Workers call `supervision::init()` with a 0 ms discovery pass and join no peers;
only the coordinator joins workers.

## Not established

* which publication triggers it, and how the failure reaches the coordinator's view;
* any cross-node measurement.

Settled after this write-up:

* the same behaviour reproduces in pure C++, 5/5 with B joining A and 0/5 without (job 217755,
  [`bench/hpx_repro/`](../../bench/hpx_repro/)); reported in
  [hpx#7834](https://github.com/TheHPXProject/hpx/issues/7834);
* killing the coordinator, or a clean coordinator exit, fences every worker it joined
  ([`phase2.md`](phase2.md)).

## Claims and non-claims

Supported:

> On HPX master `cc2be553`, on one Rostam node, the langX bindings let a Python coordinator join
> Python-hosted workers, dispatch nodes to them under `dispatch_work`, abandon a dispatch whose
> target is fenced in flight, have the root `force_disconnect` a killed worker, and shut the whole
> island down cleanly. A worker that joined an older worker and then died left the older worker
> fenced on its own state; with no joins between workers, it did not.

Not claimed:

* no commit fence, outbox or exactly-once result; that is phase 3;
* no claim that the observer behaviour is an HPX defect;
* no performance or scaling claim.

## Roadmap impact

* **F7 (coordinator crash) needs a design.** The coordinator observes every worker, so by the same
  mechanism its death would fence them all. F7 needs a worker re-init with a new epoch, or an HPX
  change. To be raised with the maintainers.
* **Phase 2 is partly done.** Dotted-path import, JSON payloads, errors as values and attempt id
  tagging went into phase 1.

## Next step

Ask the maintainers whether a failed observer delivery is meant to fail the target. Start phase 2
with the worker lifecycle and a coordinator-crash test.
