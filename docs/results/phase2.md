# Phase 2 coordinator loss on HPX master (cc2be553)

**Date:** 2026-10-23. **Cluster:** Rostam, partition `medusa`, one node per job. **HPX:** master
`cc2be553fac9b1f2a372f4277d7c708e70bf7eb4`. **Plan:** [`docs/plan.md`](../plan.md)
sections 9.3 and 16, phase 2.

Code: [`bench/coordinator_loss.py`](../../bench/coordinator_loss.py), `--state-log` in
`python/src/langx/worker.py`. Earlier results: [`phase0.md`](phase0.md), [`phase1.md`](phase1.md).

## Summary

* **Losing the coordinator fences every worker, also when it exits cleanly.** Killed, the workers'
  own state turned `failed` 0.05 to 0.12 s later; after a clean exit (`finalize()`, pause,
  disconnect), 1.21 to 1.36 s later. 12/12 workers across 6 runs. The root was fenced the same
  way. A new coordinator can join the fenced workers, but every dispatch to them is refused.
* **A fenced worker cannot re-initialize supervision.** `init()` after `finalize()` fails with
  `invalid lifecycle event transition` (2/2 workers).
* **F7 decision: restart the workers.** The fenced workers are stopped, the root
  `force_disconnect`s them, fresh workers start as new localities and the new coordinator joins
  them. This recovered in 6/6 runs, in 2.3 to 2.6 s.

## Setup

| Item | Value |
|---|---|
| Topology | `langx_root` (locality 0), workers `w1`, `w2` joining no peers at init, coordinator C1 in its own process, replacement coordinator C2 |
| Python stack | Python 3.12.3, venv `langx` |
| Detection hook | poll timeout 500 ms on every locality |
| Worker state | each worker logs its own `lifecycle_state` every 100 ms when it changes (`--state-log`) |

| Job | Purpose |
|---|---|
| 217756 | C1 killed or clean exit, 3 runs each; tests |
| 217757 | in-process re-init of a fenced worker, logs kept |
| 217758 | C1 killed or clean exit with the restart path, 3 runs each; tests |

Sequence per run: C1 joins `w1` and `w2` and dispatches to each (`ok` in every run); C1 is
SIGKILLed or shuts down cleanly; 5 s later C2 starts, joins the old workers and dispatches to
each; then the restart path runs.

## Results

### Workers after the coordinator is gone

| C1 exit | Runs | Workers `failed` after | C2 join | C2 dispatch to old workers | Root, as seen by C2 |
|---|---|---|---|---|---|
| SIGKILL | 6 (jobs 217756, 217758) | 0.05 to 0.12 s, 12/12 | joins, epoch 2 | `fenced`, 12/12 | `failed` |
| clean | 6 (jobs 217756, 217758) | 1.21 to 1.36 s, 12/12 | joins, epoch 2 | `fenced`, 12/12 | `failed` |

The workers never joined C1; C1 joined them. Their own state turns `failed` once C1 is
unreachable, which fits the observer-delivery behaviour found in phase 1: C1's observer
registrations on the workers outlive C1, also after `finalize()`. The root is fenced the same way,
because C1 joined it.

### In-process re-init

On turning `failed`, each worker called `finalize()`, paused 1 s, and called `init()` again:

```text
RuntimeError: invalid lifecycle event transition: HPX(bad_parameter)
```

2/2 workers (job 217757). The worker process exited, so C2 had nothing to join. All 6 re-init runs
in job 217756 also ended with C2 unable to join the workers; their worker logs were not kept.

### Restart path

| C1 exit | Runs | Root `force_disconnect` of old workers | Fresh workers | Kill to first dispatch on fresh workers |
|---|---|---|---|---|
| SIGKILL | 3 | `rc = 0`, 6/6 | `ok`, 6/6 | 2.52 to 2.57 s |
| clean | 3 | `rc = 0`, 6/6 | `ok`, 6/6 | 2.32 to 2.33 s |

The time runs from the start of the restart to the last dispatch on the fresh workers, after
C2 is up. After a SIGKILL, C1's locality stayed in membership, because nothing removed it.

### Tests

Unit 19/19 and integration 8/8 in jobs 217756 and 217758.

## Not established

* why the workers turn `failed` 1.2 to 1.4 s after a clean exit, rather than at once;
* whether a coordinator that leaves the observer registrations can be avoided without losing
  dispatch fencing;
* recovery of a LangGraph run from its checkpoint after the restart; that needs phase 3;
* any cross-node measurement.

## Claims and non-claims

Supported:

> On HPX master `cc2be553`, on one Rostam node, losing the coordinator, by SIGKILL or by a clean
> exit, left every worker it had joined fenced on its own state, and a fenced worker could not
> re-initialize supervision. Stopping those workers, removing them through the root and starting
> fresh workers restored dispatch in 2.3 to 2.6 s.

Not claimed:

* no claim that this is an HPX defect; it is reported in
  [hpx#7834](https://github.com/TheHPXProject/hpx/issues/7834) as a question;
* no claim about run recovery from a checkpoint;
* no performance claim.

## Roadmap impact

* **F7 uses the restart path.** After coordinator loss the harness restarts the workers, and the
  new coordinator also asks the root to `force_disconnect` the dead coordinator. In-flight results
  from the old workers are lost with them; the checkpoint and outbox keys cover the rest
  (plan 9.3).
* **H7 holds with a cost.** A coordinator crash is recoverable without a root restart, but not
  without a worker restart.

## Next step

Phase 3: coordinator, commit fence and effects.
