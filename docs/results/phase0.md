# Phase 0 spikes on HPX master (cc2be553)

**Date:** 2026-10-22. **Cluster:** Rostam, partition `medusa`, one node per job. **HPX:** master
`cc2be553fac9b1f2a372f4277d7c708e70bf7eb4`. **Plan:** [`docs/plan.md`](../plan.md)
section 16, phase 0.

Code: [`spikes/phase0` on branch `phase0-spikes`](https://github.com/RohanOnKeys/langX/tree/phase0-spikes/spikes/phase0). Raw run directories stay on Rostam under `spikes/phase0/_runs/<job>/`.

## Summary

Phase 0 answers the plan's [VERIFY] items for spikes A', B, B8, D and F before any phase 1 code
depends on them.

* **Gate 0 is met.** SIGKILL was detected in under 3 s at every tested poll timeout, and
  `dispatch_work` called from Python works.
* **Python dispatch works.** A generic `run_node` action, dispatched with
  `hpx::supervision::dispatch_work` from a Python connector, ran on the intended worker in 15/15
  islands. Round trip on one node is about 0.5 ms.
* **SIGKILL detection takes about three poll windows** (0.80 s, 1.59 s and 2.89 s at 250, 500 and
  1000 ms). A dispatch already in flight to the killed worker never completes or fails; it is
  only released by the caller's own timeout.
* **A resumed SIGSTOP worker is not fenced by the target-side re-check.** Its late result arrives
  as `ok`, and with the caller-side check bypassed it admits and runs new work. Only the
  coordinator's view rejects it.
* **A connector cannot call `force_disconnect`, but the root can do it on request.** The
  connector call fails with `invalid_status` in 15/15 islands. A root action invoked by the
  coordinator removed the killed worker with `rc = 0` in 3/3 islands.
* **The whole island shuts down cleanly once the killed worker is removed.** With connectors
  leaving through `supervision::finalize()` then `hpx::disconnect`, the root and every surviving
  connector exited cleanly in 3/3 islands. Without the removal the root did not exit.
* **Connectors abort in `hpx::stop` when `supervision::finalize()` leaves detection checks
  pending.** The failure-detection loop exits on the stop flag without waiting for its
  outstanding `await_terminal` calls, and their continuations fire during runtime teardown. A
  pause longer than the poll timeout between `finalize()` and `hpx::disconnect` removed the abort
  (15/15 workers aborted without it, 15/15 clean with it).
* **LangGraph async proxies behave as the plan needs.** Width 32 runs concurrently, and resume
  after a branch failure re-runs only that branch, on Python 3.12.3 and LangGraph 1.2.16.

## Setup

| Item | Value |
|---|---|
| HPX source | `TheHPXProject/hpx` master at `cc2be553`, existing install at `/work/rohan/apps/hpx-master-install` |
| HPX options ON | `DISTRIBUTED_RUNTIME`, `NETWORKING`, `PARCELPORT_TCP`, `SUPERVISION`, `FORCE_DISCONNECT` |
| Toolchain | gcc 15.1.0, cmake 3.29.2, boost 1.91.0, hwloc 2.12.0 (Rostam modules) |
| Python stack, HPX spikes | Python 3.12.3, pybind11 (venv `rayx`) |
| Python stack, spike D | Python 3.12.3, LangGraph 1.2.16 (venv `langx-p0d312`) |
| Topology | work-free root (locality 0), coordinator and workers `w1`, `w2`, `w3`, all late-connecting, one node |
| Transport | TCP parcelport on `localhost` |
| Detection hook | `set_failure_detection_poll_timeout_for_testing` on every locality before `init()` |
| Clock | `CLOCK_MONOTONIC` on one node for every timestamp |
| Repository | `RohanOnKeys/langX`, branch `phase0-spikes` |

**Native build:** one pybind11 module (`_p0`) loaded by the coordinator and every worker, so
`run_node` and its `make_fenced_action_t` wrapper are registered by the same shared object in
every locality. The root is a separate executable. A plain action for root-side
`force_disconnect` is defined in one header included by both. The hosting pattern follows RayX
exp70 Slice 3B (`connector_ext.cpp`, `root_supervised.cpp`).

**Island sequence:** F, then A' on `w1`, then B on `w2`, then B8 on `w3`. The coordinator sends
the signals itself. Every detection result is read from the coordinator's own supervision state
(`check_admission` and the shadow `lifecycle_state`), never from having sent the signal.

| Job | Islands | Purpose |
|---|---|---|
| 216190 | 1 smoke, 15 sweep (5 per poll timeout) | all spikes, poll 250, 500, 1000 ms |
| 216201 | 1 smoke, 3 sweep | shutdown, root given time to exit |
| 216727 | 1 smoke, 3 shutdown (post), 3 shutdown (direct), 3 sweep | root `force_disconnect`, connector disconnect, exit order |
| 216728 | 1 smoke, 5 shutdown (no pause), 5 shutdown (1 s pause), 3 sweep | connector abort in `hpx::stop` |

Every island in every job completed with no harness error.

## Results by spike

### Spike F: Python dispatch

| Check | Result |
|---|---|
| `dispatch_work<run_node>` to each worker | ran on the intended locality and PID, 15/15 islands |
| Round trip, echo node, 200 calls per island | p50 0.45 to 0.52 ms |
| Node that raises | returned as an error value, not an exception, 15/15 |
| Epoch admission, join epoch 1 | epochs 0, 1 and 2 all `admitted` and dispatched |
| 32 nodes of 100 ms over 3 workers | 1.108 s wall, 11 rounds of 100 ms |
| `discover_and_join` from the coordinator | joined all workers in every island |
| `force_disconnect` from the coordinator | `rc = -1`, `HPX(invalid_status)`, 15/15 |

`check_admission` does not reject a wrong epoch, so the epoch passed to `dispatch_work` is not
checked against the join epoch. Each worker runs one node at a time, because Python nodes run on
one OS thread per worker.

### Spike A': SIGKILL detection

Detection is the time from SIGKILL to the coordinator's `check_admission` returning
`rejected_fenced`, polled every 5 ms. The shadow's `failed` timestamp agrees with it to within
about 4 ms.

| Poll timeout | Detection median | Min to max | Ratio to poll | Islands |
|---|---|---|---|---|
| 250 ms | 0.804 s | 0.784 to 0.809 s | 3.2 | 5 |
| 500 ms | 1.593 s | 1.577 to 1.599 s | 3.2 | 5 |
| 1000 ms | 2.891 s | 2.790 to 2.901 s | 2.9 | 5 |

Jobs 216201, 216727 and 216728 gave 1.583 to 1.603 s at 500 ms (9 more islands).

| Check | Result |
|---|---|
| Dispatch in flight at the kill (3 s node) | no result and no error until the 30 s caller timeout, 15/15 |
| `snapshot_peers()` after detection | still lists the killed worker, 15/15 |
| Dispatch after detection | `target_fenced`, from the caller-side check |
| Dispatch after detection, caller-side check bypassed | dispatch error, target unreachable |

### Root-side `force_disconnect` (job 216727)

After detection, the coordinator invoked a plain action on the root that calls
`hpx::force_disconnect` on the killed worker.

| Check | Result |
|---|---|
| Root `force_disconnect` | `rc = 0`, no error, 6/6 (jobs 216727 and 216728) |
| Membership on the root after | 4 localities (5 before) |
| Membership on the coordinator after | killed worker gone |
| `snapshot_peers()` on the coordinator after | still lists the killed worker |
| Dispatch to the removed worker | `target_fenced`; with the caller-side check bypassed, dispatch error |
| Dispatch to a surviving worker | `ok` |

### Spike B: SIGSTOP past detection, then SIGCONT

The worker was stopped 200 ms into an 800 ms node and resumed 1 s after detection.

| Poll timeout | Detection median | Min to max | Islands |
|---|---|---|---|
| 250 ms | 0.545 s | 0.484 to 0.550 s | 5 |
| 500 ms | 0.982 s | 0.962 to 1.680 s | 5 |
| 1000 ms | 2.830 s | 2.682 to 2.850 s | 5 |

| Check | Result |
|---|---|
| Node in flight at the stop | result arrived as `ok` after SIGCONT, after the fence, 15/15 |
| Coordinator view after resume | `rejected_fenced`, 15/15 |
| Dispatch after resume | `target_fenced`, from the caller-side check, 15/15 |
| Dispatch after resume, caller-side check bypassed | admitted and executed by the worker, 15/15 |
| Resumed worker's view of itself | `running`, 15/15 |

The target-side re-check in `invoke_fenced_action` consults the worker's own state, which never
saw the `failed` event the coordinator published on its shadow. SIGSTOP detection was faster than
SIGKILL detection at 250 and 500 ms. The cause is not known.

### Spike B8: SIGSTOP shorter than detection

| Stop length | Evictions | Trials |
|---|---|---|
| one poll window (0.25, 0.5, 1.0 s) | 0 | 15 |

### Spike D: LangGraph proxies, no HPX

| Check | Result |
|---|---|
| Async proxy nodes in one superstep, widths 3, 8, 16, 32 | all concurrent; 205 to 209 ms wall for 200 ms nodes |
| Branch failure, no retry policy | failing branch invoked once |
| Resume from `InMemorySaver` after branch 16 of 32 failed | only branch 16 re-ran; 32 results, no duplicates |

Same results on Python 3.9.23 with LangGraph 0.6.11.

## Shutdown

Jobs 216190 and 216201: every connector left with `os._exit`, without `supervision::finalize()`
or `hpx::disconnect`.

| Job | Islands | Root |
|---|---|---|
| 216190 | 15 sweep | reached past `supervision::finalize()` in 14/15; harness killed it 0.5 s later |
| 216201 | smoke and 3 sweep | no exit within 10 s, 4/4 |

Job 216727: every connector called `supervision::finalize()` and then `hpx::disconnect`, under a
10 s watchdog. The harness stopped the workers, waited for them, then stopped the root. Times are
from the worker stop signal.

| Islands | Disconnect | Killed worker | Connectors | Root |
|---|---|---|---|---|
| 3 sweep | posted, then `hpx::stop` | `w1`, removed by root | coordinator, `w2`, `w3` clean, 9/9, within 110 ms | clean, 3/3, 50 to 100 ms after its stop signal |
| smoke and 3 shutdown | posted, then `hpx::stop` | none | coordinator clean 4/4; workers SIGABRT in `hpx::disconnect`, 12/12, 106 to 232 ms | clean, 4/4, after the workers had exited |
| 3 shutdown | run on an HPX thread and awaited | none | every connector hit the 10 s watchdog in `hpx::disconnect`, 12/12 | clean, 3/3 |

`supervision::finalize()` returned without error on every connector. The aborting workers
reported "The runtime system is not active" from `hpx::detail::get_default_pool`.

### Connector abort in `hpx::stop`

Every aborting worker had the same stack (job 216727, 12 workers):

```text
hpx::local::stop
  hpx::runtime_distributed::~runtime_distributed
    hpx::supervision::server::supervision_manager::~supervision_manager
      future_data_base::run_on_completed
        continuation in libhpx_supervision_dispatch
          hpx::threads::register_thread
            hpx::detail::get_default_pool    -> HPX(invalid_status), abort
```

In `components/supervision_dispatch/src/dispatch_api.cpp` at `cc2be553`,
`failure_detection_loop()` starts one `await_terminal(peer, epoch, poll_timeout)` per joined
peer each sweep, with a `.then(hpx::launch::async, ...)` continuation. When the stop flag is set
it leaves its wait loop without waiting for those futures. `finalize()` waits for the loop task
only. The pending futures are completed when the runtime destroys `supervision_manager`, and
their `launch::async` continuations try to create threads on a runtime that is no longer active.

Job 216728 tested this with a pause between `finalize()` and `hpx::disconnect`, poll timeout
500 ms, no kill:

| Pause | Islands | Workers | Coordinator | Root |
|---|---|---|---|---|
| none | 5 | SIGABRT in `hpx::stop`, 15/15 | clean, 5/5 | clean, 5/5 |
| 1000 ms | 5 | clean, 15/15 | clean, 5/5 | clean, 5/5 |

A pure C++ reproducer with no Python ([`native/repro_leave.cpp`](https://github.com/RohanOnKeys/langX/blob/phase0-spikes/spikes/phase0/native/repro_leave.cpp), job 216729) gives the same
result: a supervised connect-mode locality joined to the root and one running peer aborts in
`hpx::stop` with the same stack in 5/5 runs without a pause, and exits cleanly in 5/5 runs with a
1000 ms pause.

The sweep islands in job 216728 (killed worker removed by the root, no pause) shut down
cleanly as a whole island, 3/3. Whether a connector aborts without the pause depends on whether a
detection sweep is outstanding when `finalize()` returns, not on whether a worker was killed.

## Not established

* why the sweep islands had no outstanding detection sweep at shutdown;
* why `hpx::disconnect` never returns when run on an HPX thread and awaited;
* detection latency at the production default (60 s) without the testing hook;
* detection across nodes, and any measurement that needs more than one clock;
* why SIGSTOP was detected faster than SIGKILL at 250 and 500 ms.

## Claims and non-claims

Supported:

> On HPX master `cc2be553`, on one Rostam node, a Python connector can dispatch a generic
> `run_node` action to another connector through `hpx::supervision::dispatch_work`. With the
> failure-detection poll timeout set by the testing hook, a SIGKILLed worker is fenced on the
> coordinator in about three poll windows, and a SIGSTOPed worker in two to three. A resumed
> worker's late result still arrives, and its own target-side re-check admits new work. A
> connector cannot call `force_disconnect`, but the root can do it on the coordinator's
> request, and the island then shuts down cleanly. LangGraph 1.2.16 runs width 32 async proxies
> concurrently and resumes only the failed branch.

Not claimed:

* no exactly-once, commit fence or outbox result; none of that is built yet;
* no claim about detection latency without the testing hook;
* no claim that the awaited disconnect hang is an HPX defect;
* no cross-node, performance or scaling claim.

## Roadmap impact

**Phase 1 changes.** Three items move into the bindings core:

* **root-side membership action:** the root calls `force_disconnect` on request, because a
  connector cannot. Shown working in job 216727, and needed for a clean shutdown;
* **in-flight abandonment:** the coordinator watches for fencing while a dispatch is outstanding
  and abandons the dispatch itself, because HPX does not fail it;
* **coordinator-owned epoch table and commit fence:** HPX admits any epoch, and the target-side
  re-check admits a resumed worker, so the coordinator is the only place a stale result can be
  rejected. This was planned (section 6.5); phase 0 confirms it is required.

Connector shutdown is `supervision::finalize()`, a pause longer than the failure-detection poll
timeout, then a posted `hpx::disconnect` and `hpx::stop`.

## Next step

The pending `await_terminal` continuations in `finalize()` are reported upstream as
[TheHPXProject/hpx#7825](https://github.com/TheHPXProject/hpx/issues/7825). Phase 1 starts from
the spike bindings; its results are in [`phase1.md`](phase1.md).
