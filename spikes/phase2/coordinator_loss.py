"""What happens to the workers when the coordinator goes away? (plan 9.3)

Island: root, workers w1 and w2 (joining no peers at init), and coordinator
C1 in its own process. C1 joins the workers and dispatches to each. Then C1
is either SIGKILLed or shuts down cleanly. Each worker logs its own
supervision state every 100 ms. After 5 s a new coordinator C2 (this
process) joins the old workers and dispatches to each.

Then the restart path (plan 9.3): the old workers are SIGKILLed, the root
force_disconnects them, fresh workers w3 and w4 start as new localities, and
C2 joins them and dispatches to each. Prints one JSON line.

    python spikes/phase2/coordinator_loss.py --c1-exit kill
    python spikes/phase2/coordinator_loss.py --c1-exit clean
"""

import argparse
import asyncio
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time

REPO = pathlib.Path(__file__).resolve().parents[2]
sys.path[:0] = [str(REPO / "python" / "src"), str(REPO / "tests" / "integration")]

from island import Island, free_port, wait_json  # noqa: E402

from langx.runtime import DispatchFailed, Runtime, TargetFenced  # noqa: E402

POLL_S = 0.5


def request(value):
    return json.dumps({"attempt_id": "x", "input": value})


async def probe(rt, peer):
    """Dispatch one echo; return its outcome and the epoch the worker reports."""
    try:
        reply = (await rt.dispatch_node(peer, "nodes:echo", request(1),
                                        timeout_s=10)).json()
        return {"status": "ok", "worker_epoch": reply["worker"]["epoch"]}
    except TargetFenced as ex:
        # The message tells which side refused: "not sent" is this
        # coordinator's own view, "client-side" is dispatch_work's caller
        # check, "since the client-side admission check" is the target.
        return {"status": "fenced", "abandoned": ex.abandoned,
                "error": str(ex)[:160]}
    except DispatchFailed as ex:
        return {"status": "failed", "error": str(ex)[:120]}
    except TimeoutError:
        return {"status": "timeout"}


def run_c1(a):
    """Coordinator C1: join, dispatch once to each worker, then wait."""
    rt = Runtime()
    rt.start(a.agas, f"localhost:{free_port()}")
    rt.init(5.0, POLL_S)
    localities = [int(x) for x in a.localities.split(",")]
    peers = rt.wait_for_peers(localities)
    results = {loc: asyncio.run(probe(rt, p)) for loc, p in peers.items()}
    with open(a.out, "w") as f:
        json.dump({"pid": os.getpid(), "epochs": {k: p.epoch for k, p in peers.items()},
                   "dispatch": results}, f)
    while not os.path.exists(a.out + ".leave"):
        time.sleep(0.05)
    # The steps of Runtime.shutdown(), timed one by one (CLOCK_MONOTONIC,
    # shared with the harness on one node).
    t = {"leave_ns": time.monotonic_ns()}
    rt.finalize(pause_s=0)
    t["finalized_ns"] = time.monotonic_ns()
    time.sleep(a.c1_pause_s)
    t["disconnect_ns"] = time.monotonic_ns()
    t["stop_rc"] = rt.stop()
    t["stopped_ns"] = time.monotonic_ns()
    with open(a.out + ".left", "w") as f:
        json.dump(t, f)


def read_log(path):
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--c1-exit", choices=["kill", "clean"], default="kill")
    ap.add_argument("--role", default="harness")
    ap.add_argument("--agas", default="")
    ap.add_argument("--localities", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--workdir", default="", help="keep logs here instead of a temp dir")
    ap.add_argument("--c1-pause-s", type=float, default=1.0,
                    help="clean exit: pause between C1's finalize and disconnect")
    a = ap.parse_args()
    if a.role == "c1":
        run_c1(a)
        return

    wargs = ["--state-log", "{dir}/{name}.state.jsonl", "--state-poll-ms", "10"]
    isl = Island(a.workdir or tempfile.mkdtemp(), n_workers=2,
                 poll_timeout_ms=int(POLL_S * 1000), worker_args=wargs,
                 root_args=["--langx-state-log", "{dir}/root.state.jsonl"]).start()
    rec = {"c1_exit": a.c1_exit, "c1_pause_s": a.c1_pause_s, "dir": str(isl.dir)}
    try:
        workers = isl.workers()
        locs = ",".join(str(w["locality"]) for w in workers.values())
        c1_out = isl.dir / "c1.json"
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(
            [str(REPO / "python" / "src"), str(REPO / "tests" / "integration")])
        c1 = subprocess.Popen(
            [sys.executable, __file__, "--role", "c1", "--agas", isl.agas,
             "--localities", locs, "--out", str(c1_out),
             "--c1-pause-s", str(a.c1_pause_s)],
            stdout=open(isl.dir / "c1.log", "w"), stderr=subprocess.STDOUT, env=env)
        rec["c1"] = wait_json(c1_out, 90)

        t_exit = time.monotonic_ns()
        if a.c1_exit == "kill":
            os.kill(c1.pid, signal.SIGKILL)
        else:
            (isl.dir / "c1.json.leave").touch()
            left = wait_json(isl.dir / "c1.json.left", 30)
            rec["c1_left_clean"] = left is not None
            if left:
                rec["c1_steps_s"] = {k[:-3]: round((v - t_exit) / 1e9, 3)
                                     for k, v in left.items() if k.endswith("_ns")}
                rec["c1_stop_rc"] = left["stop_rc"]
        c1.wait(30)
        time.sleep(5.0)

        rec["workers"] = {}
        for name, info in workers.items():
            log = read_log(isl.dir / f"{name}.state.jsonl")
            first_failed = next((e for e in log if e["event"] == "failed"), None)
            rec["workers"][name] = {
                "initial_epoch": info["epoch"],
                "log": [(round((e["t_ns"] - t_exit) / 1e9, 2), e["event"], e["epoch"])
                        for e in log],
                "failed_after_s": round((first_failed["t_ns"] - t_exit) / 1e9, 3)
                if first_failed else None,
            }

        rt = Runtime()
        rt.start(isl.agas, f"localhost:{free_port()}")
        rt.init(5.0, POLL_S)
        joined = rt.wait_for_peers([w["locality"] for w in workers.values()])
        for name, info in workers.items():
            p = joined[info["locality"]]
            rec["workers"][name]["c2_join_epoch"] = p.epoch
            rec["workers"][name]["c2_admitted_before_dispatch"] = rt.admitted(p)
            rec["workers"][name]["c2_shadow"] = rt.peer_state(p.locality)
            rec["workers"][name]["c2_dispatch"] = asyncio.run(probe(rt, p))
        rec["c2_view_of_root"] = rt.peer_state(0)["last_event"]
        rec["root_own_state"] = [
            (round((e["t_ns"] - t_exit) / 1e9, 3), e["event"], e["epoch"])
            for e in read_log(isl.dir / "root.state.jsonl")]

        # Restart path: remove the fenced workers and start fresh ones.
        t_rec = time.monotonic_ns()
        rec["recovery"] = {"force_disconnect": {}, "new_workers": {}}
        for name, info in workers.items():
            isl.kill(name)
            report = asyncio.run(rt.force_disconnect(info["locality"]))
            rec["recovery"]["force_disconnect"][name] = report["rc"]
        fresh = {n: isl.start_worker(n) for n in ("w3", "w4")}
        joined = rt.wait_for_peers([w["locality"] for w in fresh.values()])
        for name, info in fresh.items():
            rec["recovery"]["new_workers"][name] = asyncio.run(
                probe(rt, joined[info["locality"]]))
        rec["recovery"]["seconds"] = round((time.monotonic_ns() - t_rec) / 1e9, 2)
        rec["recovery"]["membership_after"] = rt.membership()
    finally:
        isl.cleanup()
    print(json.dumps(rec))
    sys.stdout.flush()
    os._exit(0)   # C2's own shutdown is not what is measured here


if __name__ == "__main__":
    main()
