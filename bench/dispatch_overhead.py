"""Split the langx dispatch round trip into HPX and Python costs.

Island: langx_root plus one worker, this process as coordinator, one host.
Each layer adds one cost to the one before; sequential calls, 100-byte
payload, after a warm-up:

  echo          C++ echo action through dispatch_work, Python on the
                coordinator side only (GIL release, completion pipe, asyncio)
  node_nowatch  run_node with an echo node, without the fence watch: adds the
                worker's Python side (hand-off to its Python thread, GIL, JSON)
  node          run_node as langx uses it, with the fence watch
  execute       Coordinator.execute: adds the commit fence and bookkeeping
  local_python  the worker's node handler and the coordinator's JSON decode,
                called in-process with no HPX at all

The HPX-only floor (C++ to C++, no Python) is the ``rtt`` case of
bench/hpx_repro. Prints one JSON line.

    python bench/dispatch_overhead.py --n 2000
"""

import argparse
import asyncio
import json
import pathlib
import statistics
import sys
import tempfile
import time

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / "python" / "src")]

from langx.coordinator import Coordinator  # noqa: E402
from langx.island import Island, free_port  # noqa: E402
from langx.runtime import Runtime  # noqa: E402
from langx.worker import NodeHost  # noqa: E402

NODE_DIR = REPO / "tests" / "integration"
PAYLOAD = "x" * 100


def stats(us):
    us = sorted(us)
    return {"n": len(us), "p50_us": round(us[len(us) // 2], 1),
            "p99_us": round(us[len(us) * 99 // 100], 1),
            "mean_us": round(statistics.fmean(us), 1)}


async def timed(n, call, warmup=200):
    for _ in range(warmup):
        await call()
    out = []
    for _ in range(n):
        t0 = time.perf_counter_ns()
        await call()
        out.append((time.perf_counter_ns() - t0) / 1e3)
    return stats(out)


async def measure(rt, peer, n):
    request = json.dumps({"attempt_id": "a", "input": PAYLOAD})
    coord = Coordinator(rt)
    coord.add_workers([peer])
    step = iter(range(10 ** 9))
    res = {}
    res["echo"] = await timed(n, lambda: rt.dispatch_echo(peer, PAYLOAD))
    res["node_nowatch"] = await timed(n, lambda: rt.dispatch_node(
        peer, "nodes:echo", request, abandon_on_fence=False))
    res["node"] = await timed(n, lambda: rt.dispatch_node(
        peer, "nodes:echo", request))
    res["execute"] = await timed(n, lambda: coord.execute(
        "bench", next(step), "echo", "nodes:echo", PAYLOAD))
    return res


def local_python(n):
    """The Python work of one call, with no HPX: worker handler + decode."""
    sys.path.insert(0, str(NODE_DIR))
    host = NodeHost({"locality": 1, "epoch": 1, "pid": 0})
    request = json.dumps({"attempt_id": "a", "input": PAYLOAD})
    for _ in range(200):
        json.loads(host("nodes:echo", request))
    out = []
    for _ in range(n):
        t0 = time.perf_counter_ns()
        json.loads(host("nodes:echo", request))
        out.append((time.perf_counter_ns() - t0) / 1e3)
    return stats(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--workdir", default="")
    a = ap.parse_args()
    isl = Island(a.workdir or tempfile.mkdtemp(), n_workers=1,
                 poll_timeout_ms=500, preload=["nodes:echo"],
                 pythonpath=[NODE_DIR]).start()
    rec = {"n": a.n, "payload_bytes": len(PAYLOAD)}
    try:
        rt = Runtime()
        rt.start(isl.agas, f"localhost:{free_port()}")
        rt.init(5.0, 0.5)
        w = next(iter(isl.workers().values()))
        peer = rt.wait_for_peers([w["locality"]])[w["locality"]]
        rec.update(asyncio.run(measure(rt, peer, a.n)))
        rec["local_python"] = local_python(a.n)
    finally:
        isl.cleanup()
    print(json.dumps(rec))
    sys.stdout.flush()


if __name__ == "__main__":
    main()
