"""Does killing one worker fence a surviving worker?

Island: root, w1, w2 (w2 starts after w1, so w2's init joins w1), and this
process as coordinator. Kills one worker, then dispatches to the survivor
every 100 ms, before and after the root force_disconnects the dead one.
Prints one JSON line per arm.

    python bench/diag_survivor_fence.py --kill w2   # survivor is the older w1
    python bench/diag_survivor_fence.py --kill w1   # survivor is the newer w2
"""

import argparse
import asyncio
import json
import pathlib
import sys
import tempfile
import time

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / "python" / "src"), str(REPO / "tests" / "integration")]

from island import Island, free_port  # noqa: E402

from langx.runtime import DispatchFailed, Runtime, TargetFenced  # noqa: E402


async def probe(rt, peer):
    try:
        await rt.dispatch_node(peer, "nodes:echo", json.dumps({"input": 1}),
                               timeout_s=5)
        return "ok"
    except TargetFenced:
        return "fenced"
    except DispatchFailed:
        return "failed"
    except TimeoutError:
        return "timeout"


async def series(rt, peer, seconds):
    out, t0 = [], time.monotonic()
    while time.monotonic() - t0 < seconds:
        out.append((round(time.monotonic() - t0, 1), await probe(rt, peer)))
        await asyncio.sleep(0.1)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kill", choices=["w1", "w2"], required=True)
    ap.add_argument("--worker-discovery-ms", type=int, default=0)
    a = ap.parse_args()
    survivor = "w1" if a.kill == "w2" else "w2"
    isl = Island(tempfile.mkdtemp(), n_workers=2, poll_timeout_ms=500,
                 worker_discovery_ms=a.worker_discovery_ms).start()
    rec = {"kill": a.kill, "survivor": survivor,
           "worker_discovery_ms": a.worker_discovery_ms,
           "joined_at_init": {n: w.get("joined_at_init")
                              for n, w in isl.workers().items()}}
    try:
        rt = Runtime()
        rt.start(isl.agas, f"localhost:{free_port()}")
        rt.init(5.0, 0.5)
        info = isl.workers()
        joined = rt.wait_for_peers([w["locality"] for w in info.values()])
        peers = {n: joined[w["locality"]] for n, w in info.items()}
        rec["before_kill"] = asyncio.run(probe(rt, peers[survivor]))
        isl.kill(a.kill)
        rec["after_kill"] = asyncio.run(series(rt, peers[survivor], 4.0))
        rec["dead_admitted_after_4s"] = rt.admitted(peers[a.kill])
        rec["force_disconnect"] = asyncio.run(rt.force_disconnect(peers[a.kill].locality))
        rec["after_force_disconnect"] = asyncio.run(series(rt, peers[survivor], 2.0))
        rec["survivor_admitted_on_coordinator"] = rt.admitted(peers[survivor])
    finally:
        isl.cleanup()
    print(json.dumps(rec))


if __name__ == "__main__":
    main()
