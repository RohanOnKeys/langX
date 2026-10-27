# Exactly-Once Agent Steps on HPX: Experiment Plan

Working title: durable LangGraph on HPX supervision dispatch.

Status: phases 0 to 3 are done (results in [`docs/results/`](results/)). Phase 4 is next. This plan is the single source of truth for scope; findings from each phase are folded back into it.

---

## 0. Sources

| Source | Read | Notes |
| --- | --- | --- |
| HPX `supervision_dispatch` module reference and manual page | Fully | API surface and semantics |
| HPX sources at `cc2be553` (`dispatch_api.cpp`, `supervision_manager_server.cpp`) | Partly | Read where phases 0 and 1 needed them |
| RayX README and exp70 (README, gap matrix, evidence index, Slice 3B sources) | Read | exp70 by Bita; reproduction on `cc2be553` by Rohan |
| RayX exp66 | Partly | In-process hosting model and transport |
| LangGraph runtime, checkpointer and durable execution docs | Read | Behavior needed here checked in spike D on 1.2.16 |
| langX phase 0 to phase 3 results | Fully | [`phase0.md`](results/phase0.md), [`phase1.md`](results/phase1.md), [`phase2.md`](results/phase2.md), [`phase3.md`](results/phase3.md) |

Rule: a statement marked **[VERIFY]** is an assumption that must be checked before code depends on it. Statements marked **[p0]**, **[p1]** or **[p2]** were measured in that phase.

---

## 1. Goal

Find out whether HPX native locality supervision, plus a small commit fence on the coordinator, is enough to give a real agent framework (LangGraph) step-level failure guarantees that the framework does not provide on its own:

- no committed step is lost when a worker is killed;
- no step is committed twice;
- a frozen worker that wakes up late cannot commit a stale result;
- effects routed through an outbox are applied exactly once.

The result is useful whichever way it comes out. If the guarantees hold, the experiment shows what HPX supervision already supports. If they do not, it shows precisely which piece is missing, and that goes upstream as an issue.

Out of scope: see section 5.

## 1.1 Terminology

| Term | Meaning here | Where it lives |
| --- | --- | --- |
| Agent run | One execution of a LangGraph graph, identified by a `thread_id` | Coordinator and its checkpointer |
| Node | One step of the graph (plan, search, reduce). Not an agent | Graph module, executed on workers |
| Worker | An HPX locality, one OS process, executing node functions. Stateless between calls | `python/src/langx/worker.py` |
| Coordinator | Process running the LangGraph loop, epoch table, commit fence and outbox. A late-connecting locality | `python/src/langx/coordinator.py` |
| Root | Work-free HPX locality 0. Anchors the runtime and AGAS, serves the membership action, runs no application work | `hpx_impl/root/langx_root.cpp` |

Consequences:

- A worker is not an agent. One run spans many workers; one worker serves many runs.
- Workers are stateless, so recovery is re-dispatch from the last committed state.
- The root is a declared single point of failure. Root loss is the case current HPX cannot recover from (exp70 Slice 4).

Checkpoints:

| Kind | What it saves | Used here |
| --- | --- | --- |
| LangGraph checkpoint | Channel values per superstep plus per-task pending writes, keyed by `thread_id` | Yes, standard checkpointer on the coordinator |
| HPX checkpoint | Serialized C++ objects or components | No (follow-on A) |
| Process checkpoint (CRIU) | Whole OS process | No. A restored process is exactly the stale incarnation epoch fencing must reject, and TCP parcelport connections do not survive restore |

---

## 2. Prior work this builds on

### 2.1 Reused from RayX

| From | Author | What it gives this experiment |
| --- | --- | --- |
| exp70 Slice 3B `native/connector_ext.cpp` | Bita | In-process HPX connector under pybind11, `supervision::init()`, GIL release around blocking calls. Starting point for `_langx` |
| exp70 Slice 3B `native/root_supervised.cpp` | Bita | Work-free supervised root. Starting point for `langx_root` |
| exp70 Slice 3B `run_slice3b.py` | Bita | Connect-mode launch, SIGKILL by PID, replacement join, gate structure |
| exp70 reproduction on `cc2be553` | Rohan | Confirms the above on current HPX master |
| exp66 | Bita | HPX runtime on background threads inside a Python process, TCP parcelport, cross-node |
| exp68 | Bita | Exactness methodology: results checked against an independent oracle |

Files derived from RayX carry a header naming the source file and author. RayX itself is read from its own repository; no copy is kept here.

### 2.2 What exp70 established

Slice 3B (Bita on `bec10f39`; reproduced by Rohan on `cc2be553`, 4 runs on 4 nodes, each 16/16 gates):

- SIGKILL of a late-connecting locality is detected by HPX itself. Application code published `failed` zero times.
- After detection, `dispatch_work` returns `HPX(target_fenced)` and the wrapped action is not invoked.
- A replacement joins as a new locality with its own identity and `join_epoch`.
- `hpx::force_disconnect` removes a departed late-connecting locality from AGAS and membership.

Known limits from exp70: root loss is not recoverable (Slice 4B), and dispatch error shapes changed between HPX commits (Slice 4A). The second is why every dispatch exception here goes to the retry path and no code matches on error strings.

### 2.3 Ray is left out

Workers, coordinator and root are plain HPX localities launched under Slurm, so every recovery can be attributed to HPX supervision and the commit fence rather than to Ray actor restarts. Ray returns in follow-on B (section 20).

### 2.4 Dependency order

```text
HPX supervision_dispatch + force_disconnect     (mechanism)
        ^
exp70 Slice 3B harness and bindings             (Bita; reproduced on cc2be553)
        ^
langX: _langx bindings, root membership action  (phases 0 and 1, done)
        ^
langX: worker runtime, coordinator, commit fence, outbox, LangGraph proxies
```

---

## 3. HPX supervision as used here

### 3.1 Lifecycle model

- Events: `unknown`, `started`, `running`, `suspending`, `completed`, `failed`, `losing_locality`.
- `lifecycle_state` holds actor id, last event, timestamp, `event_sequence_number` and `epoch`.
- Transitions are validated; terminal events latch per epoch, and a second terminal publication returns `publish_result::already_terminal`.

### 3.2 Calls used

`init`, `finalize`, `discover_and_join`, `snapshot_peers`, `query_state`, `check_admission`, `await_terminal`, `dispatch_work`, and `hpx::force_disconnect` on the root.

### 3.3 Behavior measured in phases 0 and 1

| Behavior | Finding | Source |
| --- | --- | --- |
| SIGKILL detection | About three poll windows: 0.80, 1.59, 2.89 s at 250, 500, 1000 ms poll timeout | [p0] |
| Dispatch in flight at a kill | Never completes or fails; released only by the caller's timeout | [p0] |
| `snapshot_peers()` after detection | Still lists the killed worker, also after `force_disconnect` | [p0] |
| `check_admission` with a wrong epoch | Admitted; the epoch is not checked against the join epoch | [p0] |
| Resumed SIGSTOP worker | Late result arrives as `ok`; its own target-side re-check admits new work. Only the coordinator's view rejects it | [p0] |
| SIGSTOP shorter than one poll window | No eviction, 15/15 | [p0] |
| `force_disconnect` from a connector | Fails with `invalid_status`, 15/15 | [p0] |
| `force_disconnect` by the root on request | `rc = 0`, membership shrinks, survivors keep serving | [p0], [p1] |
| Connector exit right after `finalize()` | Aborts in `hpx::stop`: pending `await_terminal` continuations fire during teardown. Reported as [hpx#7825](https://github.com/TheHPXProject/hpx/issues/7825) | [p0] |
| Clean island shutdown | Works with: dead worker removed by the root, then `finalize()`, a pause longer than the poll timeout, then posted `hpx::disconnect` and `hpx::stop` | [p0], [p1] |
| Failed delivery to an observer | `publish_event` calls `record_error(target)`, so a dead observer marks a healthy target `failed` | [p1] |
| Coordinator loss | Every worker the coordinator joined turns `failed` on its own state: 0.03 to 0.12 s after a SIGKILL, and 0.02 to 0.12 s after a clean exit's disconnect (not at its `finalize()`); the root too; dispatch from a new coordinator is refused | [p2] |
| Joining a peer whose last state is terminal | Starts a new epoch on the joiner's shadow (`seed_epoch = epoch + 1`), by design | [p2] |
| `init()` on a fenced worker | Fails with `invalid lifecycle event transition`; no in-process recovery | [p2] |
| Worker restart after coordinator loss | Root `force_disconnect` of the old workers, fresh workers joined by the new coordinator: dispatch restored in 2.3 to 3.0 s, 15/15 | [p2] |

### 3.4 Gaps and how this design handles them

| Gap | Consequence | Handling |
| --- | --- | --- |
| Discovery is one-shot | Replacements are not found automatically | Coordinator calls `discover_and_join` again and waits for the expected count |
| Fence checked before the action runs, not at completion | A paused worker can finish after being declared dead | Commit fence on the coordinator (6.5) |
| Target-side re-check uses the worker's own state | A resumed worker admits new work | Coordinator is the only dispatcher; its view and the commit fence reject the worker |
| Epoch not checked by admission | A wrong epoch is not caught by HPX | Coordinator tracks epochs itself (6.3) |
| In-flight dispatch to a dead worker never resolves | Coordinator would hang | `dispatch_node` watches for fencing and abandons the call (7) |
| Fence guards dispatch, not external effects | Effects survive worker death | Outbox and idempotency keys (6.6) |
| Fencing does not trigger `force_disconnect`, and a connector cannot call it | Dead locality stays in membership and blocks clean shutdown | Root membership action on the coordinator's request (6.1) |
| Failed observer delivery fails the target | A worker that joined another worker and then died fences the healthy one | Workers join no peers; only the coordinator joins workers |
| Same mechanism applied to the coordinator | A lost coordinator, killed or exiting cleanly, fences every worker it joined [p2] | F7 restarts the workers (9.3); question for maintainers (19) |
| Production detection timeout is 60 s plus grace | Slow recovery without the testing hook | Use the hook, always report its value |

### 3.5 Design rules that follow

1. **Node errors are values, never exceptions.** `run_node` catches every Python exception and returns an error payload. Any exception from `dispatch_work` is therefore infrastructure and goes to retry.
2. **Never match on one error string.** Map every dispatch exception to `TargetFenced` or `DispatchFailed` and retry.
3. **Abandoned dispatches can still complete.** Every attempt carries an attempt id; at most one result is accepted per (run, step, node); late results are discarded.
4. **Caller-side filtering is an optimization only.** Correctness rests on the coordinator's epoch table and the commit fence.
5. **Epochs only increase and are never bumped to clear a fence.** A node retried on another worker uses that worker's epoch; that is not a bump.

---

## 4. Claim and hypotheses

### 4.1 Claim under test

> A LangGraph agent whose nodes execute on HPX localities, with HPX supervision fencing plus a coordinator-side commit fence, loses no committed step, commits no step twice, and rejects commits from a worker whose epoch is fenced. The final agent state equals a failure-free run. Effects emitted through the outbox are applied exactly once.

### 4.2 What the claim does not say

Exactly-once for arbitrary external side effects is impossible in general.

- **Tier A (outbox effects):** the node returns an effect intent; the coordinator applies it at commit time after the fence check, keyed by idempotency key. Exactly once.
- **Tier B (direct effects):** the node performs the effect on the worker. At-least-once, or effectively once if the external system honors an idempotency key.

Root loss is out of scope. One coordinator at a time is assumed; split-brain coordinators are not tested.

### 4.3 Hypotheses

| ID | Hypothesis | Falsified if |
| --- | --- | --- |
| H1 | HPX-dispatched nodes give the same final state hash as stock LangGraph with the stub LLM | Hashes differ in any failure-free run |
| H2 | A SIGKILLed worker is detected and its in-flight node re-executed on a survivor with no lost step | Run hangs or loses a step |
| H3 | A resumed SIGSTOPed worker cannot commit a stale result | A stale commit is accepted |
| H4 | Tier A effects are applied exactly once in all failure scenarios | Any duplicate or missing application |
| H5 | Failure-free dispatch overhead is small relative to node latency | Overhead above 10 percent of node latency at 100 ms |
| H6 | Throughput scales with worker count for I/O-bound parallel branches | No scaling from 1 to 4 workers |
| H7 | A coordinator crash is recoverable without a root restart: a new coordinator joins as a new locality and resumes from checkpoint | Root restart required, or resume fails |

H7 is tested with the worker restart path (9.3): a coordinator crash needs no root restart, but it does need a worker restart [p2].

---

## 5. Non-goals

- Deterministic scheduling. Addressed only through recorded inputs and outputs.
- Accelerators, GPUs, heterogeneous placement.
- An HPX-backed checkpointer or executor-level LangGraph integration (follow-on A).
- Adapters for other agent frameworks.
- Ray.
- Root-loss recovery (blocked upstream, exp70 Slice 4B).
- Real LLM calls of any kind, and any evaluation of model quality.
- Performance claims beyond dispatch overhead and scaling on this graph.

---

## 6. Architecture

### 6.1 Processes

```text
Slurm allocation (N+2 processes; oversubscribed on one node for development)

  Root, locality 0                 hpx_impl/root/langx_root.cpp
    - work-free; anchors AGAS; expects connecting localities
    - serves the membership action (force_disconnect on request)
    - outside the failure scope

  Coordinator, late-connecting     python/src/langx/coordinator.py
    - LangGraph compiled graph with proxy nodes
    - standard checkpointer (SQLite)
    - epoch table, commit fence, outbox applier, metrics sink
    - the only locality that joins workers

  Workers 1..N, late-connecting    python/src/langx/worker.py
    - init with a 0 ms discovery pass; join no peers
    - serve run_node; import node functions by dotted path
```

One OS process per locality, each with its own GIL. CPU-bound nodes scale by adding localities.

**Membership.** A connector cannot call `force_disconnect` [p0], so the coordinator asks the root through the action in `hpx_impl/langx/membership.hpp`, which calls `hpx::force_disconnect` and returns `rc`, elapsed time and membership after. This is the exp70-proven path.

### 6.2 Data flow for one node execution

```text
1. LangGraph calls proxy node P(state_slice)
2. P picks a live worker W from the epoch table and reads E_W
3. P calls Runtime.dispatch_node(W, node, payload)
     payload carries run_id, step, attempt_id and the state slice
     3a. coordinator admission check -> refuse early with TargetFenced
     3b. dispatch_work<run_node>(W, E_W, ...); on W the authoritative check
         runs, then the Python node function
     3c. if W is fenced while the call is in flight, the call is abandoned
4. W returns (result, locality, epoch, attempt_id, effect_intents[])
5. Commit fence on the coordinator:
     (W, E_W) still live in the coordinator's table, and
     no result already accepted for (run, step, node)?
       yes -> apply outbox intents (keyed), return result to LangGraph
       no  -> discard, go to retry
6. Retry (TargetFenced, DispatchFailed, timeout, or fence failure):
     mark W fenced if the cause is W, pick another live worker, redo from 2
7. LangGraph checkpoints at the superstep boundary as usual
```

### 6.3 Epoch handling

- `epoch_of[worker]` is recorded from the join epoch when the coordinator joins the worker.
- HPX admission does not check the epoch [p0], so the coordinator's table is authoritative for it.
- A replacement worker is a new locality with its own epoch. Results tagged with an old (locality, epoch) are never committable.
- Epoch bumps are reserved for reincarnating the same target (follow-on). They are never used to clear a fence.

### 6.4 Failure detection path

Primary: HPX `failure_detection_loop()` latches `failed` on the coordinator's shadow of the worker. The coordinator sees it through `check_admission` and marks the worker fenced in its own table, permanently.

Secondary: any dispatch exception marks the worker suspect and sends the node to retry. This covers the window before detection fires.

After fencing: request `force_disconnect` from the root, record membership before and after. `snapshot_peers()` is not used to decide liveness, because it keeps listing the dead worker [p0].

### 6.5 Commit fence

> A result is committable only if, at commit time on the coordinator, the producing worker's (locality, epoch) is live in the coordinator's table and no result has already been accepted for the same (run, step, node).

This closes the SIGSTOP window and the abandoned-dispatch window. The check and the accept run under one coordinator lock per (run, step, node). Spike D showed parallel branches in one superstep run concurrently [p0], so the lock is required.

### 6.6 Effects

- **Tier A (outbox):** `effect_intents: list[{key, kind, payload}]`, with `key = sha256(run_id, step, node, index)`, written to an append-only log with a unique constraint on `key`.
- **Tier B (direct):** the node calls the sink itself; the sink runs in `naive` and `keyed` modes.

Both live in `python/src/langx/effects.py`.

### 6.7 Determinism aids

- The stub LLM replays recorded responses keyed by prompt hash (section 11).
- All randomness is seeded; timestamps are excluded from the state hash.
- State hash: canonical JSON, sorted keys, SHA-256.

---

## 7. Python surface

As built in phase 1 (`python/src/langx/runtime.py`, over the `_langx` extension):

```python
class TargetFenced(Exception): ...   # .abandoned is True if fenced in flight
class DispatchFailed(Exception): ... # .error_code; original HPX text kept

class Peer: ...                      # locality, epoch
class NodeResult: ...                # peer, result bytes, t0_ns, t1_ns; .json()

class Runtime:
    def start(self, agas, hpx, threads=4, extra_args=()): ...
    def init(self, discovery_timeout_s=5.0, poll_timeout_s=None): ...
    def finalize(self, timeout_s=10.0, pause_s=None): ...
    def stop(self): ...
    def shutdown(self, timeout_s=10.0, pause_s=None): ...

    def discover_and_join(self, timeout_s=2.0): ...
    def wait_for_peers(self, localities, timeout_s=60.0, pass_timeout_s=2.0): ...
    def snapshot_peers(self): ...
    def membership(self): ...
    def peer_state(self, locality): ...
    def query_epoch(self, locality): ...
    def admitted(self, peer): ...

    async def wait_fenced(self, peer, poll_s=0.01): ...
    async def dispatch_node(self, peer, node, payload, timeout_s=None,
                            abandon_on_fence=True, fence_poll_s=0.01): ...
    async def force_disconnect(self, locality): ...   # served by the root
```

Native side (`python/src/langx/_langx.cpp`):

- One generic action, `run_node`, wrapped by `dispatch_work`. No Python callables are registered as actions.
- Results reach Python through a completion pipe watched by the asyncio loop, so no HPX thread takes the GIL.
- Neither a timeout nor an abandonment cancels the remote call; the commit fence rejects any late result.

---

## 8. Worker runtime

### 8.1 Node resolution

Node functions are named by dotted path (`module:function`) and imported at startup. No function pickling.

### 8.2 Serialization

JSON payloads. Other formats only behind an explicit flag; pickle, if ever added, is documented as trusted-cluster only.

### 8.3 GIL policy

- Python nodes run on one dedicated OS thread per worker, so each worker runs one node at a time [p0].
- HPX threads never hold the GIL while blocked.
- CPU-bound work scales by adding localities. Free-threaded Python is out of scope.

### 8.4 Worker lifecycle

1. Connect to the root; `init()` with a 0 ms discovery pass, joining no peers [p1].
2. Wait to be joined by the coordinator.
3. Serve `run_node`. Errors are returned as values, replies are tagged with attempt id, locality and epoch.
4. Exit: `finalize()`, a pause longer than the poll timeout, then posted `hpx::disconnect` and `hpx::stop` [p0].

---

## 9. LangGraph integration

Tested on LangGraph 1.2.16 in spike D. The version is pinned in `requirements.txt` before any phase 3 result depends on it.

### 9.1 Proxy nodes

```python
def remote(node_name):
    async def proxy(state):
        return await coordinator.execute(node_name, state)  # dispatch, fence, retry, commit
    return proxy

builder.add_node("search", remote("search"))
```

LangGraph keeps supersteps, conditional edges, fan-out and checkpointing.

### 9.2 Parallel branches

Async graphs (`ainvoke`) and async proxies, so HPX waits do not exhaust a sync thread pool. Width 32 ran concurrently, and resume after a branch failure re-ran only that branch [p0].

### 9.3 Coordinator crash recovery

Losing the coordinator fences every worker it joined, and a fenced worker cannot re-init supervision [p2]. F7 therefore restarts the workers. Steps 1, 2 and 4 were run in phase 2; steps 3 and 5 are planned for phase 4:

1. Coordinator is SIGKILLed. In-flight dispatches finish on workers and their replies go nowhere.
2. Harness confirms the old coordinator PID is gone.
3. A new coordinator connects as a new locality and asks the root to `force_disconnect` the old coordinator. Not yet done: in phase 2 the dead coordinator stayed in membership [p2].
4. The harness stops the old workers; the root `force_disconnect`s them; fresh workers start as new localities and the new coordinator joins them (2.3 to 3.0 s on one node [p2]).
5. The new coordinator resumes from the checkpointer by `thread_id`. Completed sibling writes are not re-run; the interrupted node is. Outbox keys prevent re-application. Needs phase 3.

Results produced by the old workers after the coordinator loss are lost with them. An HPX change that lets a coordinator leave without fencing its workers would remove step 4. [hpx#7834](https://github.com/TheHPXProject/hpx/issues/7834) reports the underlying observer behavior and notes that a crashed coordinator would fence its workers; the clean-exit and re-init results from phase 2 are not reported upstream yet.

Run F7 with `sync` durability; report `async` separately.

### 9.4 Retry policy

LangGraph's node retry is disabled for proxy nodes; retry is owned by the coordinator. With no retry policy a failing branch was invoked once [p0].

### 9.5 Fallback

If proxies cannot meet the claim, wrap the runner. If there is no extension point, document it and propose an upstream hook.

---

## 10. Test graph

`research_agent`, in `experiments/01_durable_langgraph/graph.py`:

```text
START
  -> plan        (stub LLM: 3 to 32 sub-queries)
  -> fan_out     (parallel search nodes, simulated latency L, Tier A and Tier B variants)
  -> reduce      (deterministic merge)
  -> critique    (stub LLM: done or loop, max 3 loops)
  -> finalize    (final answer plus one outbox effect "publish")
END
```

Parameters: width W in {3, 8, 16, 32}, latency L in {50 ms, 200 ms, 1 s, 2 s}, loops in {1, 2, 3}.

---

## 11. Stub LLM

`python/src/langx/stub_llm.py`:

- Responses in a JSON fixture keyed by SHA-256 of the canonical prompt.
- A fixture miss is an error. There is no fallback to a real model.

---

## 12. Effect log

| Column | Meaning |
| --- | --- |
| key | idempotency key (unique in keyed mode) |
| run_id, step, node, attempt_id | provenance |
| origin | `outbox` or `direct` |
| worker_locality, epoch | producer |
| committed_at | coordinator monotonic time |

Modes `naive` and `keyed`. Duplicates are found by counting keys.

---

## 13. Failure injection matrix

Signals are sent by the harness using recorded PIDs and timestamped on a monotonic clock.

| ID | Scenario | Injection | Expected outcome |
| --- | --- | --- | --- |
| F0 | No failure | none | Hash equals baseline; one commit per key |
| F1 | Kill before node starts | SIGKILL worker right after it is chosen | Refused or fenced; retry on survivor; hash equals baseline |
| F2 | Kill mid-node | SIGKILL during simulated latency | Detected; in-flight call abandoned; retry; no lost step; Tier A exactly once |
| F3 | Kill after effect, before reply | SIGKILL after Tier B effect | Tier B naive duplicates (documented limit); keyed one; Tier A one |
| F4 | Zombie | SIGSTOP past detection, then SIGCONT | Late result rejected by commit fence; zero stale commits |
| F5 | Multi-kill | Kill 2 of 4 workers in one superstep | Survivors finish; hash equals baseline |
| F6 | Kill and replace | SIGKILL, fencing observed, root `force_disconnect`, start and join a replacement | Replacement receives work; dead locality's results stay rejected; membership shrinks then grows |
| F7 | Coordinator crash | SIGKILL coordinator mid-run, confirm gone, start a new one, resume by `thread_id` | Run completes from checkpoint without root restart; no outbox re-application (subject to 9.3) |
| F8 | Slow, not dead | SIGSTOP shorter than detection | No eviction, or if evicted, results rejected and run still correct; false positive rate measured |
| F9 | Abandoned dispatch | Dispatch timeout shorter than node latency | Late original result discarded; zero duplicate commits |

Baselines: stock LangGraph with the same graph, in one process, and with a separate unfenced worker process.

---

## 14. Metrics and acceptance criteria

### 14.1 Hard gates

- **G1:** final state hash equals baseline in every run of F0 to F2 and F4 to F9, and F3 for Tier A.
- **G2:** zero duplicate Tier A keys in every scenario.
- **G3:** zero accepted stale-epoch commits in F4, F6 and F9.
- **G4a:** every agent run completes within a bounded multiple of the baseline.
- **G4b (recorded, not gated):** exit path for every locality (clean, bounded timeout, killed by harness). Becomes a gate once the shutdown sequence in 8.4 holds across the full matrix.
- **G5:** every detection result comes from HPX supervision state, never from the harness's knowledge of the kill. Application `failed` publications must be zero.

### 14.2 Reported metrics

| Metric | Definition |
| --- | --- |
| Detection latency | kill signal to coordinator seeing the worker fenced |
| Recovery latency | kill signal to next committed step |
| Poll timeout | value set through the testing hook, always reported with detection latency |
| Dispatch overhead | HPX path latency minus in-process call latency, p50 and p99 |
| Overhead ratio | overhead divided by L |
| Throughput | committed nodes per second at width W |
| Scaling efficiency | throughput(N) / (N x throughput(1)) |
| False positive rate | evictions in F8 / trials |

Cross-process timing needs one clock basis: either every timestamp on one node, or per-node clock offsets recorded. Each result states which.

### 14.3 Statistics and records

- At least 30 trials per scenario and parameter point; median, p95, min and max.
- Seeds recorded; warmups discarded.
- Environment recorded: HPX commit and its pinned dependencies, LangGraph, Python, pybind11, compiler, nodes, parcelport, cores, kernel.
- Raw records as JSON lines under `_runs/` (not tracked); curated `aggregate.json` tracked; plots generated by script only.

---

## 15. Rostam setup

1. Development: one node, all localities oversubscribed.
2. Measurement: 2 to 6 nodes (root, coordinator, 1 to 4 workers). Root and coordinator may share a node.
3. HPX: master `cc2be553` with `SUPERVISION`, `FORCE_DISCONNECT`, `NETWORKING`, `PARCELPORT_TCP`. Re-pin only if a newer master is required, and record why.
4. Toolchain: gcc 15.1.0, cmake 3.29.2, boost 1.91.0, hwloc 2.12.0, Python 3.12.3 (as in phases 0 and 1).
5. Launch: HPX connect mode. Root first with `--hpx:expect-connecting-localities`; every other process with `--hpx:agas=<root>:<port>`, `--hpx:hpx=<own>:<port>`, `--hpx:ignore-batch-env`, explicit `--hpx:threads`, `--hpx:bind=none` unless pinning is measured.
6. Transport: TCP parcelport (`localhost` on one node, the cluster subnet across nodes).
7. Startup: wait until the coordinator has joined the expected workers before the first dispatch; fail fast otherwise.
8. Never run two jobs sharing one build directory concurrently; chain with `-d afterany`.
9. Job scripts take paths from the environment (`HPX_INSTALL`, `LANGX_VENV`), as in `bench/rostam_test.sbatch`.
10. Same CPU affinity and nodes for baselines and HPX runs.

---

## 16. Phases

Phases run in order. A phase starts when the previous one meets its exit criterion. Each phase ends with a write-up in `docs/results/`.

### Phase 0: spikes (done)

Python `dispatch_work`, SIGKILL and SIGSTOP behavior, connector-side membership calls, LangGraph async proxies, shutdown. Gate 0 met. Results: [`phase0.md`](results/phase0.md).

### Phase 1: bindings core (done)

`_langx`, `langx_root`, membership action, `Runtime` with abandonment and bounded finalize, worker with dotted-path nodes and errors as values. Exit met: a Python coordinator joins workers, reads epochs and dispatches nodes; unit 18/18, integration 8/8. Results: [`phase1.md`](results/phase1.md).

### Phase 2: worker lifecycle and coordinator loss (done)

Coordinator loss, by SIGKILL or clean exit, fences every worker; a fenced worker cannot re-init; F7 restarts the workers, which recovered in 6/6 runs. C++ reproducers for the observer and related behaviors filed as [hpx#7834](https://github.com/TheHPXProject/hpx/issues/7834). LangGraph pinned to 1.2.16. Results: [`phase2.md`](results/phase2.md).

### Phase 3: coordinator, fence and effects (done)

Coordinator with epoch table, bounded retry and commit fence; keyed outbox and effect log; stub LLM with recorded fixtures; `research_agent`. F0 on 2 workers equals the baseline in 5/5 runs, one commit per task, no duplicate effect. Results: [`phase3.md`](results/phase3.md).

### Phase 4: failure harness

`experiments/01_durable_langgraph/run_exp01.py`: process launcher, PID recording, signal injection at defined points, F1 to F9 automated, JSON lines output. `analyze_exp01.py` computes gates and metrics.

Exit: G1 to G5 pass on one node.

### Phase 5: Rostam measurement

Slurm scripts, baselines, sweeps across nodes, plots.

Exit: results and plots from documented commands, and a subset re-run from a fresh checkout.

### Phase 6: write-up

Short report: claim, method, results, limits. Upstream findings filed or updated as HPX issues.

---

## 17. Repository and conventions

Layout as in the README: `python/` (extension and `langx` package), `hpx_impl/` (root and membership action), `tests/unit/` (no HPX, run in CI), `tests/integration/` (needs the built extension), `experiments/01_durable_langgraph/` (the experiment), `spikes/phaseN/` (code behind each phase write-up), `bench/` (Rostam test job, link check, `setup/` for HPX, venv and environment, `hpx_repro/` reproducers), `docs/` (plan, `results/`, `reproduce.md`).

- No duplicate modules with overlapping responsibility; no `utils/`, `helpers/` or empty placeholder directories.
- Python snake_case modules, PascalCase classes; C++ follows HPX style.
- Docstrings on every public module, class and function: PEP 257 for Python, Doxygen for C++.
- Commit messages: imperative subject of at most 50 characters, blank line, body wrapped at 72. One logical change per commit.
- Feature branches and pull requests; no direct pushes to main.
- Failure-injection tests are marked and excluded from the quick run.

---

## 18. Risks

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Coordinator loss fences every worker | Certain (phase 2) | High for F7 | Worker restart path (9.3); crash case noted in hpx#7834, clean-exit and re-init results not reported yet |
| Detection too slow without the testing hook | High | Medium | Report the hook value; ask for a production knob |
| Shutdown aborts or hangs | Medium (workaround in place) | Medium | 8.4 sequence; harness bounds every exit and records it; track hpx#7825 |
| Dispatch error shapes change between HPX commits | Medium | Medium | Map every exception to retry; never match strings |
| LangGraph internals change | Medium | Medium | Pin the version; proxies use public API only |
| GIL contention on workers | Medium | Medium | One Python thread per worker; scale by localities |
| False positives evict healthy workers | Low | Medium | F8; commit fence keeps correctness |
| Synthetic workload hides real behavior | Medium | Medium | Latency mix across 50 ms to 2 s; limits stated in the report |
| Overclaiming exactly-once | Medium | High | Section 4.2; Tier A and Tier B reported separately |
| Scope creep | High | High | Section 5 |

---

## 19. Open questions for HPX maintainers

1. Is a failed delivery to an observer meant to mark the publishing target `failed` (`record_error` in `publish_event`)? If so, what is the intended pattern for a single coordinator that observes many workers?
2. What is the intended production knob for the failure-detection timeout? Is `set_failure_detection_poll_timeout_for_testing()` acceptable outside tests?
3. Should fencing a dead late-connecting locality trigger `force_disconnect`, or is the explicit root-side call the intended design?
4. Should `finalize()` wait for outstanding `await_terminal` calls ([hpx#7825](https://github.com/TheHPXProject/hpx/issues/7825))?
5. Should admission reject an epoch that does not match the target's current epoch?
6. Is `target_fenced` meant to be the only fence signal, or are more `dispatch_outcome` values planned?
7. Would an "is epoch E still live for target T" check, valid at completion time, fit in HPX? It would replace part of the coordinator-side fence.

---

## 20. Follow-on work

Lettered so they are not confused with phases.

- **A:** executor-level LangGraph integration; HPX-backed checkpointer; step-level state migration.
- **B:** Ray as the outer layer for placement and whole-island restart; root-loss handling if upstream allows; other agent frameworks.
- **C:** observability (HPX performance counters, APEX traces per node); recorded replay.

---

## 21. Definition of done

1. G1 to G5 pass across F0 to F9 on Rostam, at least 30 trials per scenario, or a failing gate is explained and reported upstream.
2. Results, plots and the report come from documented commands on a clean checkout.
3. A subset has been reproduced from a fresh checkout by someone else.
4. Limits stated: Tier B duplicates, root out of scope, detection measured with the testing hook, F7 outcome, no scheduler determinism, no accelerators.
5. Code has docstrings, follows section 17, credits RayX where derived, and is merged through reviewed pull requests.
