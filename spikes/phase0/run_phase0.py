"""Phase 0 orchestrator: launch islands, collect results, summarize.

One island = root (p0_root), workers w1..w3, coordinator, all on this node,
in HPX connect mode. Each rep is a fresh island on fresh ports. Every
process gets its own session so cleanup can kill whole groups; the exit
path of every locality is recorded (gate G4b, observational).

    python run_phase0.py --build-dir build --out _runs/<tag> \
        --fd-poll-ms 250,500,1000 --reps 5
"""

import argparse
import json
import os
import signal
import statistics
import subprocess
import sys
import time

from p0_common import (HPX_HOST, find_free_port, read_json, wait_for_file,
                       write_json)

HERE = os.path.dirname(os.path.abspath(__file__))
FINAL_ROOT_EXITS = ("clean", "shutdown_timeout", "hard_timeout")


def popen(cmd, log_path):
    log = open(log_path, "w")
    return subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                            start_new_session=True, cwd=HERE)


def kill_group(p):
    try:
        os.killpg(p.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def run_island(a, poll_ms, rep, out):
    boot = os.path.join(out, f"poll{poll_ms}_rep{rep}")
    os.makedirs(boot, exist_ok=True)
    root_port = find_free_port()
    names = [f"w{i}" for i in range(1, a.workers + 1)]
    procs = {}
    island = {"poll_ms": poll_ms, "rep": rep, "bootdir": boot}
    try:
        procs["root"] = popen(
            [os.path.join(a.build_dir, "p0_root"),
             f"--hpx:agas={HPX_HOST}:{root_port}",
             f"--hpx:hpx={HPX_HOST}:{root_port}",
             "--hpx:expect-connecting-localities", "--hpx:threads=2",
             "--hpx:bind=none", "--hpx:ignore-batch-env",
             "--p0-bootdir", boot, "--p0-fd-poll-ms", str(poll_ms)],
            os.path.join(boot, "root.log"))
        if not wait_for_file(os.path.join(boot, "root.started"), 30):
            raise RuntimeError("root did not start")
        island["root"] = read_json(os.path.join(boot, "root.started"))

        # One connector at a time: wait for each to be up before the next.
        for n in names:
            procs[n] = popen(
                [sys.executable, "p0_worker.py", "--name", n, "--bootdir", boot,
                 "--build-dir", a.build_dir, "--root-port", str(root_port),
                 "--port", str(find_free_port()), "--threads", str(a.threads),
                 "--fd-poll-ms", str(poll_ms)],
                os.path.join(boot, f"{n}.log"))
            if not wait_for_file(os.path.join(boot, f"{n}.started"), 60):
                raise RuntimeError(f"{n} did not start")

        procs["coord"] = popen(
            [sys.executable, "p0_coord.py", "--bootdir", boot,
             "--build-dir", a.build_dir, "--root-port", str(root_port),
             "--port", str(find_free_port()), "--threads", str(a.threads),
             "--fd-poll-ms", str(poll_ms), "--workers", ",".join(names),
             "--spikes", a.spikes, "--short-stop-s",
             str(a.short_stop_s if a.short_stop_s > 0 else poll_ms / 1000.0),
             "--echo-iters", str(a.echo_iters), "--fanout", str(a.fanout)],
            os.path.join(boot, "coord.log"))
        try:
            procs["coord"].wait(timeout=a.island_timeout_s)
        except subprocess.TimeoutExpired:
            island["coord_timeout"] = True
        island["coord"] = read_json(os.path.join(boot, "coord_result.json"))
    except Exception as ex:  # noqa: BLE001
        island["error"] = f"{type(ex).__name__}: {ex}"
    finally:
        # Shutdown is observational: ask nicely, then kill and record.
        # Connectors disconnect gracefully (10 s watchdog each) before the
        # root is asked to stop, so a root hang is not caused by peers that
        # simply vanished.
        island["disconnect_mode"] = os.environ.get("P0_DISCONNECT_MODE", "post")
        island["t_workers_stop_ns"] = time.monotonic_ns()
        for n in names:
            open(os.path.join(boot, f"{n}.stop"), "w").close()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and any(
                p.poll() is None for k, p in procs.items() if k != "root"):
            time.sleep(0.1)
        island["connector_exits"] = {
            k: read_json(os.path.join(boot, f"{k}.exit"))
            for k in names + ["coord"]}
        island["t_root_stop_ns"] = time.monotonic_ns()
        open(os.path.join(boot, "root.stop"), "w").close()
        # root.exit is rewritten at each shutdown step; wait for a final one.
        # The root's own watchdog ends it after --p0-shutdown-timeout-s (10 s).
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            path = (read_json(os.path.join(boot, "root.exit")) or {}).get(
                "exit_path")
            root = procs.get("root")
            if path in FINAL_ROOT_EXITS or root is None or root.poll() is not None:
                break
            time.sleep(0.1)
        time.sleep(0.5)
        exits = {}
        for k, p in procs.items():
            rc = p.poll()
            exits[k] = "exited_rc_%s" % rc if rc is not None else "killed_by_harness"
            kill_group(p)
        island["exit_paths"] = exits
        island["root_exit"] = read_json(os.path.join(boot, "root.exit"))
        write_json(os.path.join(boot, "island.json"), island)
    return island


def stats(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    return {"n": len(xs), "median": statistics.median(xs), "min": xs[0],
            "max": xs[-1], "p95": xs[int(0.95 * (len(xs) - 1))]}


def summarize(islands):
    by_poll = {}
    for i in islands:
        by_poll.setdefault(i["poll_ms"], []).append(i)
    out = {}
    for poll, isl in sorted(by_poll.items()):
        cs = [i.get("coord") or {} for i in isl]
        A = [c.get("A", {}) for c in cs]
        B = [c.get("B", {}) for c in cs]
        B8 = [c.get("B8", {}) for c in cs]
        F = [c.get("F", {}) for c in cs]
        out[poll] = {
            "islands": len(isl),
            "coord_ok": sum(1 for c in cs if c.get("ok")),
            "errors": [c.get("error") or i.get("error") for c, i in zip(cs, isl)
                       if c.get("error") or i.get("error")],
            "F_echo_ran_on_target_all": all(
                all(v["ran_on_target"] for v in f.get("echo", {}).values())
                for f in F if f) if any(F) else None,
            "F_echo_p50_us": stats([f.get("echo_latency_us", {}).get("p50")
                                    for f in F]),
            "F_node_raise_returned_as_value": [f.get("node_raise", {}).get(
                "returned_as_value") for f in F],
            "F_epoch_variants": [f.get("epoch_variants") for f in F][:1],
            "F_fanout_wall_ms": stats([f.get("fanout", {}).get("wall_ms")
                                       for f in F]),
            "A_detected": sum(1 for x in A if x.get("detection", {}).get("fenced")),
            "A_detect_poll_ms": stats([x.get("detection", {}).get("detect_poll_ms")
                                       for x in A]),
            "A_shadow_failed_ms": stats([x.get("detection", {}).get(
                "shadow_failed_ms") for x in A]),
            "A_inflight_status": [x.get("inflight", {}).get("status") for x in A],
            "A_inflight_returned_after_kill_ms": stats(
                [x.get("inflight", {}).get("returned_after_kill_ms") for x in A]),
            "A_snapshot_still_has_w1_after_detection": [
                c.get("workers", {}).get("w1", {}).get("locality")
                in x.get("snapshot_after_detection", []) for c, x in zip(cs, A)
                if x],
            "A_force_disconnect": [{"rc": x.get("force_disconnect", {}).get("rc"),
                                    "error": x.get("force_disconnect", {}).get("error")}
                                   for x in A if x],
            "A_dispatch_after_fd": [x.get("dispatch_after_force_disconnect", {})
                                    .get("status") for x in A],
            "A_dispatch_after_fd_skip_client": [
                x.get("dispatch_after_force_disconnect_skip_client_check", {})
                .get("status") for x in A],
            "B_detected": sum(1 for x in B if x.get("detection", {}).get("fenced")),
            "B_detect_poll_ms": stats([x.get("detection", {}).get("detect_poll_ms")
                                       for x in B]),
            "B_late_result_status": [x.get("inflight_late_result", {}).get("status")
                                     for x in B],
            "B_late_result_needs_commit_fence": [
                x.get("inflight_late_result", {}).get("would_need_commit_fence")
                for x in B],
            "B_still_fenced_after_resume": [x.get("after_resume", {}).get(
                "check_admission") for x in B],
            "B_dispatch_normal": [x.get("dispatch_normal", {}).get("status")
                                  for x in B],
            "B_dispatch_skip_client_check": [x.get("dispatch_skip_client_check", {})
                                             .get("status") for x in B],
            "B_zombie_executed_new_calls": [x.get("zombie_executed_new_calls")
                                            for x in B],
            "B_zombie_self_event": [((x.get("zombie_self_view") or {}).get(
                "own_state") or {}).get("last_event") for x in B],
            "B8_false_evictions": sum(1 for x in B8 if x.get("false_eviction")),
            "B8_trials": sum(1 for x in B8 if x),
            "exit_paths": [i.get("exit_paths") for i in isl],
            "root_exit": [(i.get("root_exit") or {}).get("exit_path") for i in isl],
        }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-dir", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--fd-poll-ms", default="500")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--spikes", default="F,A,B,B8")
    ap.add_argument("--short-stop-s", type=float, default=0.0,
                    help="B8 stop length; 0 means one poll window")
    ap.add_argument("--echo-iters", type=int, default=200)
    ap.add_argument("--fanout", type=int, default=32)
    ap.add_argument("--island-timeout-s", type=float, default=300)
    a = ap.parse_args()
    a.build_dir = os.path.abspath(a.build_dir)
    out = os.path.abspath(a.out)
    os.makedirs(out, exist_ok=True)

    islands = []
    for poll in [int(x) for x in a.fd_poll_ms.split(",")]:
        for rep in range(a.reps):
            t0 = time.monotonic()
            i = run_island(a, poll, rep, out)
            islands.append(i)
            c = i.get("coord") or {}
            print(f"poll={poll} rep={rep} ok={c.get('ok')} "
                  f"err={c.get('error') or i.get('error')} "
                  f"A={c.get('A', {}).get('detection', {}).get('detect_poll_ms')} "
                  f"B={c.get('B', {}).get('detection', {}).get('detect_poll_ms')} "
                  f"took={time.monotonic() - t0:.1f}s", flush=True)

    summary = summarize(islands)
    write_json(os.path.join(out, "summary.json"), summary)
    print(json.dumps(summary, indent=1, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
