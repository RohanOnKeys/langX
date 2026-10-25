# Dispatch overhead split on HPX master (cc2be553)

**Date:** 2026-10-25. **Cluster:** Rostam, partition `medusa`, one node per job. **HPX:** master
`cc2be553fac9b1f2a372f4277d7c708e70bf7eb4`. **Plan:** [`docs/plan.md`](../plan.md).

Code: [`bench/dispatch_overhead.py`](../../bench/dispatch_overhead.py),
[`bench/overhead.sbatch`](../../bench/overhead.sbatch), the `rtt` case of
[`bench/hpx_repro/supervision_gaps_repro.cpp`](../../bench/hpx_repro/supervision_gaps_repro.cpp),
and the `langx_echo` action in [`python/src/langx/_langx.cpp`](../../python/src/langx/_langx.cpp).
Earlier results: [`phase3.md`](phase3.md).

## Summary

* **HPX is about a third of a langx dispatch.** One `Coordinator.execute` round trip takes
  ~244 µs (median). The HPX part, a `dispatch_work` call from C++ to C++, takes ~81 µs.
* **Supervision adds ~9 µs to a plain HPX action.** `dispatch_work` takes ~81 µs where
  `hpx::async` of the same action takes ~72 µs.
* **The largest cost is on the worker's Python side, ~76 µs.** The node handler and JSON take
  ~11 µs when called in-process, so most of it is getting the call onto the worker's Python
  thread and back. That is an inference from the difference, not a separate measurement.
* langx already sends work as an HPX action (`run_node` under `dispatch_work`). It does not use
  a Python RPC layer, so swapping transports would not touch the Python costs measured here.

## Setup

| Item | Value |
|---|---|
| Topology | `langx_root` (locality 0), one worker, coordinator in the benchmark process, all on one node |
| Calls | sequential, 2000 per layer after 200 warm-up calls, 100-byte payload |
| Python stack | Python 3.12.3, venv `langx` |
| Detection hook | poll timeout 500 ms (Python layers), 2000 ms (C++ floor) |
| Node | `tests/integration/nodes.py:echo` |

| Job | Purpose |
|---|---|
| 219490 | build with the echo action; 42/42 unit and 9/9 integration tests |
| 219491 | `dispatch_overhead.py`, 3 runs, node medusa08 |
| 219492 | HPX-only floor (`rtt` case), 3 runs, node medusa11 |

## Layers

Each layer adds one cost to the one before it.

| Layer | What it runs |
|---|---|
| `plain_async` | C++ `hpx::async` of an echo action to the worker locality, no supervision |
| `dispatch_work` | the same action through `supervision::dispatch_work` |
| `echo` | the C++ echo action through `dispatch_work`, called from Python: GIL release, completion pipe, asyncio. The worker never enters Python |
| `node_nowatch` | `run_node` with the Python echo node, no fence watch: adds the worker's Python side (hand-off to its Python thread, GIL, JSON) |
| `node` | `Runtime.dispatch_node` as langx uses it, with the fence watch |
| `execute` | `Coordinator.execute`: adds the commit fence and bookkeeping |
| `local_python` | the worker's node handler plus the coordinator's JSON decode, in-process, no HPX |

## Results

Round trip p50 in µs, per run.

| Layer | Run 1 | Run 2 | Run 3 | Median | Added |
|---|---|---|---|---|---|
| `plain_async` | 72.9 | 62.9 | 71.9 | 71.9 | |
| `dispatch_work` | 80.6 | 71.1 | 80.5 | 80.5 | +8.6 |
| `echo` | 108.7 | 110.0 | 110.3 | 110.0 | +29.5 |
| `node_nowatch` | 188.6 | 183.4 | 186.3 | 186.3 | +76.3 |
| `node` | 204.4 | 197.3 | 199.1 | 199.1 | +12.8 |
| `execute` | 247.1 | 235.5 | 243.6 | 243.6 | +44.5 |
| `local_python` | 10.5 | 10.6 | 10.7 | 10.6 | |

In runs 1 and 3, p99 was within 1.25x of p50 for every dispatch layer. Run 2 of both jobs had a
longer tail, up to 1.6x.

Share of the `execute` median: HPX with supervision 33%, coordinator bridge 12%, worker Python
side 31%, fence watch 5%, commit fence 18%.

## Notes

* The C++ floor and the Python layers ran on different nodes (medusa11 and medusa08). Both are
  the same hardware class, but the `echo` minus `dispatch_work` difference crosses jobs.
* All localities share one host and talk over loopback, so these numbers are a lower bound for
  workers on other nodes.
* The fence watch polls `admitted()` every 10 ms on the coordinator's event loop. Its ~13 µs is
  the cost of starting and cancelling that task per call.
* The worker's Python thread is the clearest place to cut cost. The ~65 µs not explained by
  `local_python` is the hand-off between the HPX thread and the Python thread in both directions.
