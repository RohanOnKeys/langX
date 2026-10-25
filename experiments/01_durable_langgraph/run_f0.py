"""F0, no failure (plan sections 13, 14): HPX runs against the baseline.

For each rep the same ``research_agent`` run is executed twice:

* baseline: stock LangGraph, every node in this process;
* HPX: every node dispatched to workers through the coordinator and its
  commit fence, on an island of ``langx_root`` plus ``--workers`` workers.

The final state hashes and the outbox effect keys must match, with no
duplicate key and one commit per task. Writes ``f0.json`` and exits non-zero
if any check fails.

    python experiments/01_durable_langgraph/run_f0.py --out _runs/f0
"""

import argparse
import asyncio
import json
import pathlib
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1] / "python" / "src")]

import graph  # noqa: E402
from langx.coordinator import Coordinator  # noqa: E402
from langx.effects import EffectLog, Outbox  # noqa: E402
from langx.island import Island, free_port  # noqa: E402
from langx.runtime import Runtime  # noqa: E402

PRELOAD = [f"agent_nodes:{n}" for n in graph.NODES]


async def run_one(g, run_id, args):
    t0 = time.monotonic()
    final = await graph.run(g, run_id, graph.initial_state(
        run_id, args.width, args.loops, args.latency_ms))
    return final, time.monotonic() - t0


async def runs(g, args):
    out = []
    for rep in range(args.reps):
        run_id = f"f0-rep{rep}"
        final, wall = await run_one(g, run_id, args)
        out.append({"run_id": run_id, "hash": graph.state_hash(final),
                    "wall_s": round(wall, 3), "answer": final.get("answer")})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--width", type=int, default=8)
    ap.add_argument("--loops", type=int, default=2)
    ap.add_argument("--latency-ms", type=int, default=50)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--poll-ms", type=int, default=500)
    args = ap.parse_args()
    # The stub LLM only knows the recorded parameter points (make_fixtures).
    import make_fixtures  # noqa: PLC0415
    if args.width not in make_fixtures.WIDTHS or args.loops not in make_fixtures.LOOPS:
        ap.error(f"--width must be one of {make_fixtures.WIDTHS} and --loops one of "
                 f"{make_fixtures.LOOPS}: only those prompts are recorded")
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    base_log = EffectLog(out / "baseline_effects.sqlite")
    base = asyncio.run(runs(graph.build(graph.local_nodes(Outbox(base_log))),
                            args))

    isl = Island(out / "island", n_workers=args.workers,
                 poll_timeout_ms=args.poll_ms, preload=PRELOAD,
                 pythonpath=[HERE])
    (out / "island").mkdir(exist_ok=True)
    isl.start()
    rec = {"args": vars(args), "baseline": base}
    try:
        rt = Runtime()
        rt.start(isl.agas, f"localhost:{free_port()}")
        rt.init(5.0, args.poll_ms / 1000.0)
        joined = rt.wait_for_peers([w["locality"] for w in isl.workers().values()])
        hpx_log = EffectLog(out / "hpx_effects.sqlite")
        coord = Coordinator(rt, Outbox(hpx_log))
        coord.add_workers(joined.values())
        rec["hpx"] = asyncio.run(runs(graph.build(graph.remote_nodes(coord)),
                                      args))

        kinds = {}
        for e in coord.events:
            kinds[e["event"]] = kinds.get(e["event"], 0) + 1
        per_worker = {}
        for e in coord.events:
            if e["event"] == "committed":
                per_worker[e["locality"]] = per_worker.get(e["locality"], 0) + 1
        rec["events"] = kinds
        rec["commits_per_worker"] = per_worker
        rec["effects"] = {
            "baseline_keys": len(base_log.keys("outbox")),
            "hpx_keys": len(hpx_log.keys("outbox")),
            "keys_equal": base_log.keys("outbox") == hpx_log.keys("outbox"),
            "hpx_duplicates": hpx_log.duplicates(),
        }
        rec["checks"] = {
            "hash_equal": [b["hash"] == h["hash"]
                           for b, h in zip(base, rec["hpx"])],
            "outbox_keys_equal": rec["effects"]["keys_equal"],
            "no_duplicate_keys": rec["effects"]["hpx_duplicates"] == 0,
            "one_commit_per_task": kinds.get("committed", 0) == len(coord.accepted),
            "no_retries_or_rejections": not (kinds.get("retry") or kinds.get("rejected")),
        }
        rec["exit"] = {"workers": isl.stop_workers()}
        finalized, rc = rt.shutdown()
        rec["exit"]["coordinator"] = {"finalized": finalized, "stop_rc": rc}
        rec["exit"]["root"] = isl.stop_root()
    finally:
        isl.cleanup()

    checks = rec["checks"]
    ok = all(checks["hash_equal"]) and all(v for k, v in checks.items()
                                           if k != "hash_equal")
    rec["ok"] = ok
    (out / "f0.json").write_text(json.dumps(rec, indent=1, sort_keys=True))
    print(json.dumps({"ok": ok, "checks": checks, "events": rec["events"],
                      "commits_per_worker": rec["commits_per_worker"],
                      "effects": rec["effects"],
                      "wall_s": {"baseline": [b["wall_s"] for b in base],
                                 "hpx": [h["wall_s"] for h in rec["hpx"]]},
                      "exit": {k: (v if k != "workers" else
                                   {n: (r or {}).get("exit_path") for n, r in v.items()})
                               for k, v in rec["exit"].items()}}, indent=1))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
