# Exactly-Once Agent Steps on HPX: Experiment Plan (v2)

Working title: "Durable LangGraph on HPX supervision dispatch"

Status: draft for discussion with the mentor and HPX maintainers. Implementation has not started.

---

## Changes from v1

v1 was written from the RayX README alone. v2 is revised after reading the exp70 reports and the exp70 reproduction on HPX master `cc2be553`. Main changes:

1. **Coordinator is no longer locality 0.** A work-free HPX root is locality 0. The coordinator and the workers are late-connecting localities. In v1 a coordinator crash was a root crash, which exp70 Slice 4 shows cannot be recovered with current HPX. In v2 it is a connector loss, which exp70 Slice 3B handles natively.
2. **Launch is HPX connect mode,** copied from exp70 Slice 3B. `hpx::force_disconnect` only works for late-connecting localities, so a classic `srun` multi-locality launch would lose it.
3. **Week 0 shrinks.** Spikes A (SIGKILL detection), C (pybind11 plus `supervision::init` in-process) and E (replacement locality) were already answered by exp70 Slice 3B and its reproduction. What remains: detection latency with real timestamps, SIGSTOP, LangGraph proxy behavior, and a Python-side `dispatch_work` path, which nobody has built yet.
4. **Gates and failure matrix corrected.** F6 now includes an explicit `force_disconnect`. G4 is split into run completion and clean process exit, because the root shutdown hang reproduces. Gate 0 is stated as depending on the testing hook.
5. **Provenance corrected.** exp70 is Bita's experiment. The reproduction on `cc2be553` is Rohan's. This experiment is new work that reuses exp70 Slice 3B code as a starting point.
6. **Real-model demo mode dropped** from this experiment.

---

## 0. Provenance and confidence

| Source | Read | Notes |
| --- | --- | --- |
| Agentic_Runtime PDF (Contrail AIX proposal) | Fully | Target of the whole effort |
| RayX README | Fully | |
| RayX exp70 README, gap matrix, evidence index | Read | Author: Bita. Slice 3B is the native `supervision_dispatch` harness |
| RayX exp70 reproduction report on HPX `cc2be553` | Read | Author: Rohan, 2026-10-10 |
| exp70 Slice 3B sources (`run_slice3b.py`, `native/connector_ext.cpp`, `native/root_supervised.cpp`) | Partly | Enough to confirm what they cover (section 2.1) |
| exp70 Slice 3B aggregates (Rohan's `cc2be553` runs) | Read | 4 runs, 4 nodes, each 16/16 gates |
| RayX exp66 report | Partly | In-process hosting model and transport |
| HPX `supervision_dispatch` module reference (index.rst) | Fully | API surface and semantics |
| HPX manual page `supervision_dispatch.rst` | Fully | `plain_worker.cpp` example NOT read |
| DeepWiki page on supervision and lifecycle | Fully | Secondary source |
| LangGraph runtime, checkpointer, durable execution docs | Read | Source code NOT read |
| HPX supervision source code and unit tests | Partly, via exp70 | exp70 cites `dispatch_api.cpp` internals; full read is week 0 |

Rule: every statement marked **[VERIFY]** is an assumption to be checked in week 0 before code depends on it.

---

## 1. Goal

Produce one experiment, large enough to be a credible funding argument, that shows a real agent framework (LangGraph) running on HPX localities with failure semantics that the framework alone does not provide.

| PDF claim | What this experiment shows |
| --- | --- |
| Fault resilience: agents run for hours, failures are inevitable | Worker death mid-run does not lose or duplicate committed agent steps |
| Native checkpointing and recovery | Step-level recovery from the last superstep, without whole-run restart |
| Hardware-level observability | Per-step dispatch, fence, and recovery timings recorded per locality |

Not tested (section 5): deterministic scheduling, accelerator placement, RISC-V, 10x speedup.

---

## 1.1 Terminology

| Term | Meaning here | Where it lives |
| --- | --- | --- |
| Agent (run) | One execution of a LangGraph graph, identified by a `thread_id` | Coordinator plus checkpointer |
| Node | One step of the graph (plan, search, reduce). LangGraph calls nodes "actors" (PregelNode); that is NOT an agent | Graph module, executed on workers |
| Worker | An HPX locality, one OS process, executing node functions. Stateless between calls | Worker processes |
| Coordinator | Process running the LangGraph loop, epoch table, commit fence, outbox | A late-connecting locality (NOT locality 0) |
| Root | Work-free HPX locality 0. Anchors the runtime and AGAS. Runs no application work | Separate process, outside the failure scope |

Consequences:

- A worker is not an agent. One run spans many workers; one worker serves many runs.
- Workers are stateless, so recovery is re-dispatch from the last committed state. Pause, migrate and resume of stateful agents is a later phase.
- The root is a declared single point of failure. This matches exp70: root loss is the case current HPX cannot recover from (Slice 4, section 2.2).

Three kinds of checkpoint:

| Kind | What it saves | Used here |
| --- | --- | --- |
| LangGraph checkpoint | Channel values per superstep plus per-task pending writes, keyed by `thread_id` | Yes, standard checkpointer on the coordinator |
| HPX checkpoint | Serialized C++ objects or components **[VERIFY]** API | No (phase 2) |
| Process checkpoint (CRIU) | Whole OS process | No |

### CRIU

Not used, for these reasons:

1. **It conflicts with supervision.** A restored process is exactly the zombie epoch fencing rejects. AGAS, supervision epochs and parcelport connections live outside one process image.
2. **Networking.** Established TCP connections do not survive restore without special handling. RayX runs use the TCP parcelport.
3. **Privileges.** Needs elevated capabilities that Rostam may not grant **[VERIFY]**.
4. **Cost and portability.** Full address-space dumps tied to kernel and library versions.
5. **Granularity.** Superstep-level application checkpoints are tiny by comparison.

Possible later use: a heavyweight baseline for single-process pause and migrate.

---

## 2. Where RayX stands

### 2.1 What is reused

All reuse is from RayX experiments, credited by author.

| From | Author | What it gives this experiment |
| --- | --- | --- |
| exp70 Slice 3B `native/connector_ext.cpp` | Bita | pybind11 module that hosts a networking HPX locality in-process, calls `hpx::supervision::init()`, and releases the GIL around blocking HPX calls. Template for the Python binding |
| exp70 Slice 3B `native/root_supervised.cpp` | Bita | Work-free supervised root: `discover_and_join`, `dispatch_work` from C++, fencing observation, `force_disconnect`. Template for the root process |
| exp70 Slice 3B `run_slice3b.py` | Bita | Connect-mode launch, SIGKILL injection by PID, replacement join, gate structure |
| exp70 reproduction on `cc2be553` | Rohan | Confirms the above holds on current HPX master, with caveats (section 2.2) |
| exp66 | Bita | HPX runtime brought up on background threads inside a Python process, TCP parcelport, cross-node |
| exp65 | Bita | Demand-ordered late admission of connectors |
| exp68 | Bita | Exactness methodology: results checked bit for bit against an independent oracle |
| exp70 acceptance design | Bita | Fixed acceptance harness, swappable backend |

### 2.2 What exp70 already established (and what it did not)

Established, natively, by Slice 3B (Bita on `bec10f39`, 2026-09-08; reproduced by Rohan on `cc2be553`, 2026-10-10, 4 runs on 4 nodes, each 16/16):

- **SIGKILL of a late-connecting locality is detected by HPX itself.** `failure_detection_loop()`, started by `hpx::supervision::init()`, calls `await_terminal()` per joined peer, confirms silence via `query_state()`'s `event_sequence_number` over three consecutive silent windows, then publishes `failed` against its own shadow of the peer. Application code published `failed` zero times.
- **Fencing is specific.** After detection, `dispatch_work` returned `HPX(target_fenced)` with "the wrapped action was not invoked".
- **A replacement joins as a new locality** with a distinct identity and its own `join_epoch`, and does verified work.
- **`hpx::force_disconnect` removes a departed late-connecting locality**: AGAS resolve fails afterwards, membership shrinks, a later dispatch fails, and a surviving connector also sees the locality gone.

Not established, or known problems:

- **Detection latency is not measured.** The root-side wall timestamps in the aggregates appear quantized, so kill-to-detection time cannot be derived. Production default for the sweep timeout is `default_discovery_timeout` = 60 s, plus up to three grace windows. exp70 shortened it with `set_failure_detection_poll_timeout_for_testing()`.
- **Root shutdown hangs.** Every 3B run ended with `root_exit_path = hung_killed_by_harness`. In at least one run the root never wrote its finish note, so the hang is likely inside `hpx::supervision::finalize()` **[VERIFY]**.
- **Fencing and `force_disconnect` are separate.** Nothing upstream calls the second after the first. The root calls it explicitly.
- **`force_disconnect` only applies to late-connecting localities** (`is_connecting == true`) and cannot remove the root.
- **Root loss is not recoverable** (exp70 Slice 4B: detection plausible, recovery structurally excluded).
- **Slice 4A behavior changed on `cc2be553`.** After root loss, actor calls return `call_error_result` ("Connection reset by peer") in about 6 to 10 ms, versus `call_timeout` at 15 s on `20bc3d4b`. A same-node control shows the change follows the HPX build. The bisect was not run. Relevant here only because it shows dispatch error shapes change between HPX commits: map all dispatch errors to the retry path, never match on one error string.
- **`dispatch_work` was only called from C++ on the root.** No Python caller has used it.
- **SIGSTOP was never tested.**

### 2.3 What is left out

**Ray is not used.** Workers, coordinator and root are plain HPX localities launched under Slurm. If Ray were present, a reviewer could attribute recovery to Ray actor restarts. Note that exp70 Slice 3B killed a Ray-actor-hosted process and detection still came from HPX alone, which supports this isolation.

Ray returns in a later phase (section 21) for placement and island restart.

### 2.4 Order of dependency

```
HPX supervision_dispatch + force_disconnect (the mechanism)
        ^
exp70 Slice 3B harness and bindings (Bita; reproduced on cc2be553)
        ^
this experiment: Python dispatch, commit fence, outbox, LangGraph
        ^
Ray orchestration (later phase)
```

---

## 3. What HPX supervision provides

From the module reference, the manual, and exp70. Items confirmed on hardware by exp70 are marked **[exp70]**.

### 3.1 Lifecycle model

- Events: `unknown`, `started`, `running`, `suspending`, `completed`, `failed`, `losing_locality`.
- `lifecycle_state` holds actor id, last event, timestamp, `event_sequence_number`, and `epoch`.
- Transitions are validated. `is_valid_transition()` forbids a terminal event for a target still in `started`/`running` from a foreign publisher **[exp70]**.
- Terminal events latch per epoch; a second terminal publication returns `publish_result::already_terminal` **[exp70]**.

### 3.2 Core calls

`publish_event`, `query_state`, `register_observer`, `await_terminal`, `remove_target`, `check_admission`.

### 3.3 Dispatch module

- `init()` / `finalize()`: one-shot, idempotent. `init()` creates a registry, publishes `started`, registers a symbol name, runs one `discover_and_join()` pass, and starts the heartbeat and `failure_detection_loop()` **[exp70]**.
- `discover_peers()` / `discover_and_join()`: one-time pull with timeout. Localities that have not called `init()` are silently excluded.
- `join()`: creates a local shadow mirroring the peer.
- `snapshot_peers()`: joined, non-evicted peers.
- `dispatch_work<Action>(target, epoch, args...)`: client-side admission check, then `hpx::colocated` dispatch with an authoritative re-check on the target immediately before the action runs. Throws `target_fenced` if a terminal event is latched for that epoch **[exp70]**.
- `hpx::force_disconnect`: separate API (PR #7447). Removes a late-connecting locality from AGAS and connection caches **[exp70]**.

### 3.4 Known gaps

| Gap | Consequence | Plan |
| --- | --- | --- |
| Discovery is one-shot | Replacements are not found automatically | Coordinator or root calls `discover_and_join` again, or `join` explicitly |
| Fence checked before the action runs, not at completion | A paused worker can finish after being declared dead | Coordinator-side commit fence (6.5) |
| Default epoch 0 is stale after the first publication | Silent misbehavior | Always pass the epoch from `query_state` |
| Fence guards dispatch, not external effects | Effects survive worker death | Outbox and idempotency keys (6.6) |
| Fencing does not trigger `force_disconnect` | Dead locality stays in membership | Explicit `force_disconnect` after fencing (F6) |
| Production detection timeout is 60 s plus grace | Slow recovery outside the testing hook | Use the hook; report it; ask maintainers (section 20) |
| Root shutdown hangs | Clean exit cannot be a hard gate | G4 split (14.1) |

### 3.5 Facts from the manual and their consequences

**Failure detection is heartbeat based and local to the observer.** Each locality republishes `running` at about one third of the poll timeout. The observer publishes `failed` against its own shadow, never against the peer's authoritative state.

Consequences:

1. **SIGKILL is detected** **[exp70]**. Latency still to be measured (spike A').
2. **SIGSTOP is fenced only by the observer's shadow.** When a frozen worker resumes, its own state still says `running`, so the target-side re-check would admit work. Only the coordinator's client-side check and the commit fence reject it. Acceptable because the coordinator is the only dispatcher, but must be tested (F4, spike B).
3. **The commit fence is coordinator-owned.** The client-side check is non-authoritative, and an epoch-0 or removed target is admitted. The coordinator keeps its own table of (worker locality, epoch) -> live or fenced, marks it fenced when it stops trusting a worker, and never flips it back.
4. **Coordinator-side `failed` publication** against a shadow via the raw-id `publish_event` may be possible **[VERIFY]**. Lower priority now that native detection works.

**Startup ordering.** `init()` excludes peers that have not called `init()` yet, and a second `init()` does not rediscover. In connect mode, localities arrive one at a time, so whoever joins peers must call `discover_and_join` again as localities arrive and verify the expected count with `snapshot_peers()` before dispatching. `init()` during `finalize()` fails with `invalid_status`.

**Targets and cleanup.** A target is any `hpx::id_type`, introduced by publishing `started`. `dispatch_work` fails before either admission check, with a non-fenced error, if AGAS cannot resolve the id. `remove_target` is local and publishes nothing; a removed target never republished is admitted.

**Documentation drift to report.** The manual uses `joined_discovery_result`; the reference uses `discovered_peer` and says `joined_peer::target` is the peer locality. The examples read `peer.state` from `snapshot_peers()`, while the reference says `peer_snapshot` holds only `peer_locality` and `join_epoch`. exp70 uses `discovered_peer::join_epoch`, so check the current header and the exp70 code before binding.

**Dispatch outcome semantics.** A fenced dispatch and an action that threw both surface as an exceptional future; only `get_error() == target_fenced` tells them apart. A timed wait that expires does not cancel the dispatch.

Design consequences:

1. **Node errors are returned as values, never thrown.** `run_node` catches every Python exception and returns an error payload. Any exception from `dispatch_work` is then infrastructure (fenced, unresolvable, unreachable, connection reset) and goes to the retry path. Never match on one specific error string; exp70 Slice 4A shows these change between HPX commits.
2. **Abandoned dispatches can complete later.** Tag every attempt with an attempt id; accept at most one result per (run, step, node); discard late results (F9).
3. **Client-side filtering is an optimization only.**

### 3.6 Observers, eviction, epochs, two-tier admission

- `join(peer)` reads the peer's epoch with `query_state`, publishes `started` at that epoch on a shadow, and registers observers on the peer's locality, atomically with rollback.
- Synchronization is push driven; a crashed peer sends nothing, hence heartbeats.
- A terminal mirrored event evicts the peer from `snapshot_peers()` and unregisters its observers. Whether failure detection on a silent peer also evicts it is unclear **[VERIFY]**, record in spike A'.
- Every locality joins every peer: O(N^2) joins and heartbeats. Fine at 5 to 6 localities; note in scaling plots.
- Epochs are per target and only increase. A higher epoch resets the target and clears the latch. Never bump to clear a fence on a running instance.
- A node retried on a different worker uses that worker's epoch. That is not a bump.
- The authoritative check precedes execution, not completion. Hence the commit fence.
- Signature drift: `dispatch_work<Action>(target, epoch, args...)` versus an instance overload `dispatch_work(Action(), target, epoch, args...)`. Use whatever `root_supervised.cpp` compiles against on `cc2be553`.

---

## 4. Claim, hypotheses, and what "exactly-once" means here

### 4.1 Precise claim

> A LangGraph agent whose tool nodes execute on HPX localities, with HPX supervision fencing plus a coordinator-side commit fence, loses no committed step, commits no step twice, and rejects commits from a worker whose epoch has been latched. Final agent state is identical to a failure-free run. Effects emitted through the outbox are applied exactly once.

### 4.2 What the claim does not say

Exactly-once for arbitrary external side effects is impossible in general.

- **Tier A (outbox effects):** the tool returns an effect intent; the coordinator applies it at commit time after the fence check, keyed by idempotency key. Exactly once.
- **Tier B (direct effects):** the tool performs the effect on the worker. At-least-once without an idempotency key honored by the external system; effectively once with one.

Root loss is out of scope. One coordinator at a time is assumed (F7 restarts only after the old coordinator process is confirmed gone); split-brain coordinators are not tested.

### 4.3 Hypotheses

| ID | Hypothesis | Falsified if |
| --- | --- | --- |
| H1 | HPX-dispatched nodes produce the same final state hash as stock LangGraph with a deterministic stub LLM | Hashes differ in any failure-free run |
| H2 | A SIGKILLed worker is detected and its in-flight node re-executed on a survivor with no lost step | Run hangs or loses a step |
| H3 | A resumed SIGSTOPed zombie cannot commit a stale result | A stale commit is accepted |
| H4 | Tier A effects are applied exactly once in all failure scenarios | Any duplicate or missing application |
| H5 | Failure-free dispatch overhead is small relative to tool latency (50 ms to 2 s) | Overhead above 10 percent of node latency at 100 ms (threshold to agree) |
| H6 | Throughput scales with worker count for I/O-bound parallel branches | No scaling from 1 to 4 workers |
| H7 | A coordinator crash is handled as a connector loss: the restarted coordinator joins as a new locality and resumes from checkpoint without root restart | Root restart required, or resume fails |

---

## 5. Non-goals

- Deterministic scheduling. Addressed only through recorded inputs and outputs.
- Accelerators, GPU, RISC-V, Contrail hardware.
- HPX-backed checkpointer (phase 2).
- Executor-level LangGraph integration (phase 2).
- PydanticAI and CrewAI adapters.
- Ray.
- Root-loss recovery (blocked upstream, exp70 Slice 4B).
- Real LLM calls of any kind, including a demo mode.
- Real LLM quality evaluation.

---

## 6. Architecture

### 6.1 Processes

```
Slurm allocation (N+2 processes; oversubscribed on one node for dev)

  Root, locality 0 (C++, derived from exp70 root_supervised.cpp)
    - work-free; anchors AGAS; expects connecting localities
    - membership service: discover_and_join, force_disconnect on request
    - outside the failure scope

  Coordinator (Python + embedded HPX, late-connecting locality)
    - LangGraph compiled graph with proxy nodes
    - standard checkpointer (SQLite), sync durability for F7
    - epoch table, commit fence, outbox applier, metrics sink

  Workers 1..N (Python + embedded HPX, late-connecting localities)
    - generic run_node action
    - imports the graph module by dotted path
```

One OS process per locality, each with its own GIL. CPU-bound nodes scale by adding localities.

**Open design point (spike F):** in exp70, locality 0 calls `discover_and_join` and `force_disconnect`. With the coordinator as a connector, either (a) the coordinator calls them itself, if that works from a connector **[VERIFY]**, or (b) the coordinator asks the root through a small membership action. Prefer (a) if it works; (b) keeps the exp70-proven path.

### 6.2 Data flow for one node execution

```
1. LangGraph calls proxy node P(state_slice)
2. P picks a live worker W from the epoch table, reads E_W
3. P calls dispatch_work<run_node>(W, E_W, run_id, step, node, attempt_id, payload)
     3a. client-side admission check   -> may short-circuit with target_fenced
     3b. on W: authoritative check, then run_node executes the Python node fn
4. W returns (result_bytes, E_W, attempt_id, effect_intents[])
5. Commit fence on the coordinator:
     E_W still live in the coordinator's table, no terminal latched,
     and no result already accepted for (run, step, node)?
       yes -> apply outbox intents (keyed), return result to LangGraph
       no  -> discard, go to retry
6. Retry (target_fenced, any dispatch exception, or fence failure):
     mark W fenced if the cause is W, pick another live worker, redo from 2
7. LangGraph checkpoints at the superstep boundary as usual
```

### 6.3 Epoch handling

- `epoch_of[worker]` comes from `query_state` / `join_epoch` after join.
- A killed worker's replacement is a new locality with its own epoch; results tagged with the old locality and epoch are never committable **[exp70 confirms distinct incarnation]**.
- Always pass the real epoch.
- Epoch bumps are reserved for reincarnating the same target (later phase). Never used to clear a fence.

### 6.4 Failure detection path

Primary: HPX `failure_detection_loop()` latches `failed` on the coordinator's shadow **[exp70]**. The coordinator observes it via `await_terminal` / `check_admission` and marks the worker fenced in its own table.

Secondary signal: any dispatch exception also marks the worker suspect and moves the node to retry. This covers the window before detection fires.

After fencing: request `force_disconnect` for the dead locality (6.1 open point), then record membership before and after.

Fallback, only if spike A' shows a gap: coordinator-local blacklist plus commit fence, with or without a raw-id `failed` publication.

### 6.5 Commit fence

> A result is committable only if, at commit time on the coordinator, the producing worker's (locality, epoch) is live in the coordinator's table, no terminal event is latched for it, and no result has already been accepted for the same (run, step, node).

Closes the SIGSTOP window and the abandoned-dispatch window.

Atomicity: the fence check and the accept are done under one coordinator lock per (run, step, node). **[VERIFY]** against LangGraph's parallel branch execution (spike D).

### 6.6 Effects

- **Tier A (outbox):** `effect_intents: list[{key, kind, payload}]`, `key = sha256(run_id, step, node, index)`, append-only log with a unique constraint on `key`.
- **Tier B (direct):** node calls the sink itself; sink runs in `naive` and `keyed` modes.

### 6.7 Determinism aids

- Stub LLM replays recorded responses keyed by prompt hash.
- All randomness seeded; timestamps excluded from the state hash.
- State hash: canonical JSON, sorted keys, SHA-256.

---

## 7. Proposed Python surface

Names are proposals; align with the mentor and maintainers. Start from `connector_ext.cpp` (init, start, GIL release pattern) and add what is missing.

```python
# hpx_agent/runtime.py
def init(argv: list[str], discovery_timeout_s: float = 10.0) -> None: ...
def finalize(timeout_s: float = 10.0) -> bool: ...   # returns False if bounded wait expired
def is_initialized() -> bool: ...

def discover_and_join(timeout_s: float = 10.0) -> list["Peer"]: ...
def snapshot_peers() -> list["Peer"]: ...
def query_epoch(peer: "Peer") -> int: ...
def check_admission(peer: "Peer", epoch: int) -> bool: ...
def await_terminal(peer: "Peer", epoch: int, timeout_s: float) -> str: ...
def force_disconnect(peer: "Peer") -> None: ...       # spike F decides where this runs

async def dispatch_node(peer: "Peer", epoch: int, run_id: str, step: int,
                        node: str, attempt_id: str, payload: bytes) -> "NodeResult": ...
    # raises TargetFenced, DispatchFailed

class TargetFenced(Exception): ...
class DispatchFailed(Exception): ...   # every other dispatch exception, original text kept
```

C++ side:

- One generic action `run_node(run_id, step, node, attempt_id, payload)`. Acquires the GIL, calls the named node function, serializes the result, catches every Python exception into an error payload.
- Only this one action is dispatched; no Python callables are registered as actions.
- Blocking calls release the GIL (`py::gil_scoped_release`, as in `connector_ext.cpp`). HPX futures are bridged to asyncio.

---

## 8. Worker runtime design

### 8.1 Node resolution

Dotted path (`module:function`), imported at startup. No function pickling.

### 8.2 Serialization

JSON by default. MessagePack or pickle behind an explicit flag; pickle documented as a trusted-cluster-only risk.

### 8.3 GIL policy

- Python runs on a dedicated thread per worker, entered with `gil_scoped_acquire`.
- HPX threads never hold the GIL while blocked.
- CPU-bound nodes scale by localities.
- Free-threaded Python out of scope.

### 8.4 Worker lifecycle

1. Connect to the root, `init()`.
2. Wait to be joined (coordinator or root runs `discover_and_join`). **[VERIFY]** whether `running` must be published before admission (read `plain_worker.cpp`).
3. Serve `run_node`.
4. Graceful exit: publish `completed`, then `finalize()` with a bounded wait.

---

## 9. LangGraph integration

All LangGraph API statements are **[VERIFY]** against the pinned version.

### 9.1 Proxy nodes

```python
def remote(node_name):
    async def proxy(state):
        return await coordinator.execute(node_name, state)   # dispatch, fence, retry, commit
    return proxy

builder.add_node("search", remote("search"))
```

LangGraph keeps supersteps, conditional edges, fan-out and checkpointing.

### 9.2 Parallel branches

Use async graphs (`ainvoke`) and async proxies so HPX waits do not exhaust the sync thread pool. Spike D checks this under fan-out width 32.

### 9.3 Coordinator crash recovery

With the coordinator as a connector:

1. Coordinator is SIGKILLed. Workers' and root's shadows of it latch `failed`. In-flight dispatches finish on workers and their replies go nowhere.
2. Harness confirms the old coordinator PID is gone.
3. New coordinator process connects as a new locality, joins workers, and resumes from the checkpointer by `thread_id`.
4. Completed sibling writes are not re-run (`put_writes`); the interrupted node re-runs. Outbox keys stop re-application.

Run F7 with `sync` durability; report `async` separately. Contrast with stock LangGraph: stronger guarantees for outbox effects and live worker failure, not "stock LangGraph is broken".

### 9.4 Runtime internals (docs only, source not read)

- Pregel is bulk synchronous: nodes within a superstep are independent, safe to dispatch in parallel.
- `PregelRunner` takes `submit` and `put_writes`; executors live in private modules. Pin the version.
- The commit fence sits in the proxy, before LangGraph calls `put_writes`.

### 9.5 Retry policy

Disable LangGraph's node retry for proxy nodes; retry is owned by the coordinator **[VERIFY]**.

### 9.6 Fallback

If proxies cannot meet the claims, wrap the runner; if no extension point exists, document it and propose an upstream hook.

---

## 10. Test graph

Graph `research_agent`:

```
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

- Responses in a JSON fixture keyed by SHA-256 of the canonical prompt.
- A miss is an error. No real model fallback and no demo mode.

---

## 12. Effect log

| Column | Meaning |
| --- | --- |
| key | idempotency key (unique in keyed mode) |
| run_id, step, node, attempt_id | provenance |
| origin | `outbox` or `direct` |
| worker_locality, epoch | producer |
| committed_at | coordinator monotonic time |

Modes `naive` and `keyed`. Duplicates found by counting keys.

---

## 13. Failure injection matrix

Signals sent by the harness using recorded PIDs, timestamped on a monotonic clock in the harness and in each process.

| ID | Scenario | Injection | Expected outcome |
| --- | --- | --- | --- |
| F0 | No failure | none | Hash equals baseline; one commit per key |
| F1 | Kill before node starts | SIGKILL worker right after it is chosen | Dispatch fails or is fenced; retry on survivor; hash equals baseline |
| F2 | Kill mid-node | SIGKILL during simulated latency | Detected; retry; no lost step; Tier A exactly once |
| F3 | Kill after effect, before reply | SIGKILL after Tier B effect | Tier B naive duplicates (documented limit); keyed one; Tier A one |
| F4 | Zombie | SIGSTOP past detection window, then SIGCONT | Late result rejected by commit fence; zero stale commits |
| F5 | Multi-kill | Kill 2 of 4 workers in one superstep | Survivors finish; hash equals baseline |
| F6 | Kill and replace | SIGKILL, fencing observed, `force_disconnect`, start replacement, join it | Replacement (new locality) receives work; dead locality's results stay rejected; membership shrinks then grows |
| F7 | Coordinator crash | SIGKILL coordinator mid-run, confirm gone, start new coordinator, resume by `thread_id` | Run completes from checkpoint without root restart; no outbox re-application |
| F8 | Slow, not dead | SIGSTOP shorter than detection window | No false eviction, or if evicted, results rejected and run still correct; false positive rate measured |
| F9 | Abandoned dispatch | Dispatch timeout shorter than node latency | Late original result discarded; zero duplicate commits |

Baseline contrast: stock LangGraph, same graph, single process and with a separate unfenced worker process.

---

## 14. Metrics and acceptance criteria

### 14.1 Hard gates

- **G1:** final state hash equals baseline in all runs of F0, F1, F2, F4 to F9 (and F3 for Tier A).
- **G2:** zero duplicate Tier A keys in all scenarios.
- **G3:** zero accepted stale-epoch commits in F4, F6, F9.
- **G4a:** every agent run completes within a bounded multiple of the baseline.
- **G4b (observational, not gated):** process exit path recorded for every locality (clean, bounded-timeout, killed by harness). Gated only if the upstream shutdown hang is fixed.
- **G5:** all detection results come from HPX supervision state, not from the harness's knowledge of the kill (same rule as exp70: application `failed` publications count must be zero unless the fallback path is in use and declared).

### 14.2 Reported metrics

| Metric | Definition |
| --- | --- |
| Detection latency | kill signal (harness monotonic) to coordinator observing `failed` on its shadow |
| Recovery latency | kill signal to next committed step |
| Poll timeout used | value set through the testing hook, always reported next to detection latency |
| Dispatch overhead | HPX path latency minus in-process call latency, p50 and p99 |
| Overhead ratio | overhead divided by L |
| Throughput | committed nodes per second at width W |
| Scaling efficiency | throughput(N) / (N x throughput(1)) |
| False positive rate | evictions in F8 / trials |

Detection latency across processes requires one clock basis. Either keep every timestamp on one node (oversubscribed runs) or record per-node clock offsets; state which.

### 14.3 Statistics

- At least 30 trials per scenario and parameter point; median, p95, min/max.
- Seeds recorded; warmups discarded.
- Environment recorded: HPX commit plus pinned dependency commits (asio, stdexec), LangGraph version, Python, pybind11, compiler, nodes, parcelport, cores, kernel.
- Raw records as JSON lines; plots generated by script only.

---

## 15. Rostam setup

1. Development: one node, all localities oversubscribed.
2. Measurement: 2 to 6 nodes (root, coordinator, 1 to 4 workers). Root and coordinator may share a node.
3. HPX: master `cc2be553`, already built with SUPERVISION, FORCE_DISCONNECT, NETWORKING and PARCELPORT_TCP at `/work/rohan/apps/hpx-master-install`. Re-pin if a newer master is required, and record why.
4. Launch: HPX connect mode, as in exp70 Slice 3B. Root started first with `--hpx:expect-connecting-localities`; every other process connects with `--hpx:agas=<root>:<port>`, `--hpx:hpx=<own>:<port>`, `--hpx:ignore-batch-env`, explicit `--hpx:threads`, `--hpx:bind=none` unless pinning is being measured.
5. Transport: TCP parcelport over `10.42.5.x` (`--prefer-subnet 10.42.5.` parity with RayX).
6. Startup: wait until `snapshot_peers()` reports the expected count before the first dispatch; fail fast otherwise.
7. Never run two jobs that share one build directory concurrently (exp70 reproduction lesson); chain with `-d afterany`.
8. Job scripts parameterized, no hardcoded user paths.
9. Pin CPU affinity consistently across baseline and HPX runs.
10. Run the stock LangGraph baseline on the same nodes.

---

## 16. Week-by-week plan

LOC estimates include tests, exclude docs.

### Week 0: spikes and reading (about 2 days)

Deliverable: a private go/no-go note answering every [VERIFY].

- Read `dispatch_api.cpp`, `dispatch_work.hpp`, `testing.hpp`, `plain_worker.cpp` on `cc2be553`. Read `root_supervised.cpp`, `connector_ext.cpp`, `run_slice3b.py` in full.
- **Spike A' (detection latency):** copy the exp70 Slice 3B harness into a new spike directory; add monotonic timestamps at kill and at shadow `failed`; record what `snapshot_peers()` shows after detection. Run on one node.
- **Spike B (SIGSTOP):** same harness, add a SIGSTOP/SIGCONT arm: does the shadow latch, what does the resumed worker report, does a dispatch to it get fenced client-side.
- **Spike D (LangGraph):** async proxy nodes awaiting fake futures under width 32; retry policy off. Laptop only, no HPX.
- **Spike F (Python dispatch):** extend the connector module with a generic `run_node` action and call `dispatch_work` from a Python connector to another connector. Also test whether a connector can call `discover_and_join` and `force_disconnect` itself.

Gate 0: if SIGKILL is not detected within 30 s with the testing hook, or Python-side `dispatch_work` cannot be made to work, stop and bring the finding to the mentor and maintainers.

### Week 1: bindings core (about 800 LOC C++, 300 LOC Python)

Start from `connector_ext.cpp`. Add join, epoch query, admission, await, force_disconnect, `run_node`, exception mapping, asyncio bridge, bounded finalize.

Exit: a Python coordinator on 3 localities (root plus 2) discovers a worker, reads its epoch, dispatches a trivial node.

### Week 2: worker runtime (about 600 LOC)

`run_node` with GIL handling, dotted-path import, JSON, error payloads, lifecycle publishing.

Exit: coordinator dispatches a real node function and gets a result tagged with epoch and attempt id.

### Week 3: coordinator and proxies (about 800 LOC)

Proxy factory, worker selection, bounded retry, epoch table, commit fence, outbox and effect log, stub LLM and fixtures, `research_agent`.

Exit: F0 passes with hash equal to baseline on 2 workers.

### Week 4: failure harness (about 900 LOC)

Process launcher, PID recording, signal injection at defined points, F1 to F9 automated, JSON lines output.

Exit: G1 to G5 pass on one node.

### Week 5: Rostam measurement (about 300 LOC)

Slurm scripts, baselines, sweeps, plots. Second person or fresh checkout re-runs a subset.

Exit: results and plots from one command.

### Week 6: write-up

Two-page report (claim, method, results, limits). Terminal recording of F2 and F4 against stock LangGraph. Share with the mentor and maintainers.

Total: about 3,700 to 4,500 LOC, 6 weeks plus one buffer week.

---

## 17. Repository location and naming

Decide with the mentor:

- **Option 1: separate repository** that cites RayX exp70. Fits best: no Ray, adds LangGraph, which is outside RayX's stated scope.
- **Option 2: inside RayX as `experiments/71_...`**, fenced as experiment-only, not `rayx.runtime` API, no real model backends.

Proposed layout (option 1):

```
hpx-agent/
  bindings/            C++ pybind11 sources (derived from exp70 Slice 3B, credited)
  root/                work-free supervised root (derived from exp70 Slice 3B, credited)
  python/hpx_agent/
    runtime.py
    coordinator.py
    worker.py
    effects.py
    stub_llm.py
  experiments/durable_langgraph/
    graph.py
    fixtures/
    harness/
    analyze/
  scripts/slurm/
  tests/
  README.md
```

Naming checklist:

- No duplicate modules with overlapping responsibility.
- No `utils/`, `helpers/`, `misc/`, no empty placeholder directories.
- Python snake_case modules, PascalCase classes; C++ follows HPX style.
- Binding names match section 7.
- Slurm scripts named by purpose.
- Files derived from exp70 carry a header crediting the source file and author.

---

## 18. Engineering conventions

- Docstrings on every public function, class and module: PEP 257 (one style throughout) for Python, Doxygen for C++.
- No em dashes in code, comments, or documentation.
- No emoji in documents.
- Feature branches and pull requests; never push directly to main unless agreed.
- Commit messages: imperative, subject at most 50 characters, blank line, body wrapped at 72. One logical change per commit.
- No AI co-author trailers or attributions.
- This plan lives in `docs/plan.md` and is the single source of truth for scope. Handoff notes, one-off prompts and agent instruction files stay out of the repository.
- Tests accompany each module; failure-injection tests are marked and excluded from the quick run.

---

## 19. Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Python-side `dispatch_work` harder than expected | Medium | High | Spike F in week 0; start from exp70 C++ path |
| Connector cannot call `discover_and_join` / `force_disconnect` | Medium | Medium | Root membership action (6.1 option b) |
| Detection too slow without the testing hook | High | Medium | Report the hook value; ask maintainers for a production knob |
| Root or `supervision::finalize()` shutdown hang | High (seen in every 3B run) | Medium | G4 split; bounded finalize; harness kills after timeout and records it |
| Dispatch error shapes change between HPX commits | Medium (seen in Slice 4A) | Medium | Map every dispatch exception to retry; never match strings |
| LangGraph blocking starves fan-out | Medium | High | Async proxies; spike D |
| LangGraph retry or checkpoint semantics differ | Medium | Medium | Pin version; verify |
| GIL contention on workers | Medium | Medium | One Python thread per worker; scale by localities |
| False positives evict healthy workers | Low to medium | Medium | F8; commit fence keeps correctness |
| Results look like a synthetic toy | Medium | High | Realistic tool latency mix; honest framing |
| Overclaiming exactly-once | Medium | High | Section 4.2; Tier A and B reported separately |
| Attribution confusion with exp70 | Low | Medium | Provenance table; per-file credit headers |
| Scope creep | High | High | Section 5 |

---

## 20. Questions for the mentor and maintainers

For the mentor and Bita, before starting:

1. Is building on the exp70 Slice 3B code acceptable, and how should it be credited?
2. Separate repository or `experiments/71_...` inside RayX?
3. Should the pending exp70 reproduction findings (Slice 4A change, shutdown hang, unrun bisect) be resolved first?

For HPX maintainers:

4. What is the intended production knob for the failure-detection timeout? Is `set_failure_detection_poll_timeout_for_testing()` acceptable outside tests?
5. Should fencing a dead late-connecting locality trigger `force_disconnect` automatically, or is the explicit call the intended design?
6. Can a connector (not locality 0) call `discover_and_join` and `force_disconnect`?
7. Is the root shutdown hang after `force_disconnect` known? Is it in `supervision::finalize()`?
8. Is `target_fenced` meant to be the only fence signal, or are more `dispatch_outcome` values planned?
9. Would a linearizable "is epoch E still live for target T" helper be welcome in HPX, replacing part of the coordinator-side fence?
10. Preferred conventions if parts of the binding are upstreamed.

---

## 21. Funding case and follow-on phases

### 21.1 Pitch

"A real agent framework on HPX that cannot lose or double-commit a step when a worker dies or freezes, measured on Rostam against stock LangGraph."

### 21.2 Evidence package

- F0 to F9 results: hash equality, duplicate counts, stale commits rejected, detection and recovery latency (with the poll timeout used).
- Scaling plot: throughput versus workers and width.
- Side-by-side recording with stock LangGraph.
- Two-page report with limits (4.2, 5).
- Upstream findings list (detection knob, shutdown hang, connector-side membership calls).

### 21.3 Mapping to the PDF

| PDF claim | Status after this experiment |
| --- | --- |
| Fault recovery without losing partial work | Demonstrated at step level for worker and coordinator failure; root failure out of scope |
| Native checkpointing | Not yet; standard checkpointer |
| Hardware-level observability | Partial: per-step dispatch and recovery timings |
| Framework adapters | LangGraph via proxies |
| Deterministic execution and replay | Not claimed |
| Heterogeneous scheduling, accelerators | Not tested |
| Multi-node coordination | Demonstrated on Rostam without Ray |
| 10x faster than cloud GPU | Not tested, not claimed |

### 21.4 Follow-on phases

1. **Phase 2 (2 to 3 months):** executor-level LangGraph integration; HPX-backed checkpointer; step-level state migration.
2. **Phase 3 (about 3 months):** Ray layered on native supervision for placement and island restart; root-loss handling if upstream allows; PydanticAI and CrewAI adapters.
3. **Phase 4 (about 3 months):** observability (HPX counters, APEX traces per node); recorded replay; placement hints.

### 21.5 GSoC and extended work

- GSoC (350 h): weeks 0 to 4 are the core.
- Extended engagement (6 to 12 months, with the LSU group): phases 2 to 4.
- Before applying: land one small HPX pull request and share week 0 findings on HPX channels.

---

## 22. Week 0 checklist (private)

- [ ] Send the exp70 reproduction summary to Bita and the mentor; get answers to section 20 questions 1 to 3.
- [ ] Record HPX (`cc2be553`), RayX, LangGraph, pybind11, Python, compiler versions.
- [ ] Read the HPX and exp70 sources listed in week 0.
- [ ] Spike A': detection latency with monotonic timestamps; `snapshot_peers()` after detection.
- [ ] Spike B: SIGSTOP and SIGCONT behavior.
- [ ] Spike D: async LangGraph proxies under width 32.
- [ ] Spike F: Python `dispatch_work` through `run_node`; connector-side `discover_and_join` and `force_disconnect`.
- [ ] Decide the repository location.
- [ ] Answer every [VERIFY] in writing, privately.

---

## 23. Definition of done

1. G1 to G5 pass across F0 to F9 on Rostam, at least 30 trials per scenario.
2. Results, plots and the report come from documented commands on a clean checkout.
3. A second person has reproduced a subset.
4. Limits stated: Tier B duplicates, root out of scope, testing-hook detection timeout, shutdown hang, no scheduler determinism, no accelerators.
5. Code has docstrings, passes the naming checklist, credits exp70 where derived, and sits on a reviewed branch.
