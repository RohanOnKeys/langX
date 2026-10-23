"""Phase 0 coordinator: a late-connecting locality that runs spikes F, A', B.

Runs on the same node as the workers, so one CLOCK_MONOTONIC basis covers
every timestamp (plan section 14.2). The coordinator sends the signals
itself, but every detection result is read from its own HPX supervision
state (check_admission and the shadow's lifecycle_state), never inferred
from having sent the signal (gate G5).

Sequence per island, with workers w1, w2, w3:
  F   join, dispatch run_node to each worker, latency, errors, epochs,
      concurrency
  A'  SIGKILL w1 with a dispatch in flight, measure detection,
      force_disconnect from this connector
  B   SIGSTOP w2 with a dispatch in flight, hold past detection, SIGCONT,
      see what the late result and the target-side re-check do
  B8  SIGSTOP w3 below the detection window, check for false eviction
"""

import argparse
import json
import os
import signal
import statistics
import threading
import time

from p0_common import (connector_args, import_ext, leave, now_ns, read_json,
                       wait_for_file, write_json)


class Coordinator:
    """Holds the extension handle, the worker table and the record."""

    def __init__(self, ext, bootdir, workers, fd_poll_ms):
        self.ext = ext
        self.bootdir = bootdir
        self.fd_poll_ms = fd_poll_ms
        self.w = workers          # name -> {pid, locality, epoch}
        self.rec = {}

    def loc(self, name):
        return self.w[name]["locality"]

    def epoch(self, name):
        return self.w[name]["epoch"]

    def dispatch(self, name, node, payload=None, timeout_s=30.0,
                 epoch=None, skip_client_check=False):
        r = self.ext.dispatch_run_node(
            self.loc(name), self.epoch(name) if epoch is None else epoch,
            node, json.dumps(payload or {}), timeout_s, skip_client_check)
        r = dict(r)
        if r["status"] == "ok":
            r["result"] = json.loads(r["result"])
        return r

    def dispatch_in_background(self, name, node, payload, timeout_s=30.0):
        box = {}

        def run():
            box["r"] = self.dispatch(name, node, payload, timeout_s)
            box["returned_ns"] = now_ns()

        t = threading.Thread(target=run, daemon=True)
        t.start()
        return t, box

    def snapshot_localities(self):
        return sorted(p["locality"] for p in self.ext.snapshot_peers())

    def wait_fenced(self, name, t_ref_ns, timeout_s):
        """Poll this locality's own supervision view until name is fenced."""
        loc, ep = self.loc(name), self.epoch(name)
        deadline = time.monotonic() + timeout_s
        polls = 0
        while time.monotonic() < deadline:
            polls += 1
            if self.ext.check_admission(loc, ep) == "rejected_fenced":
                t = now_ns()
                st = dict(self.ext.peer_state(loc))
                return {"fenced": True, "polls": polls,
                        "detect_poll_ms": (t - t_ref_ns) / 1e6,
                        "shadow_failed_ms":
                            (st["timestamp_ns"] - t_ref_ns) / 1e6
                            if st["last_event"] == "failed" else None,
                        "shadow_state": st}
            time.sleep(0.005)
        return {"fenced": False, "polls": polls,
                "shadow_state": dict(self.ext.peer_state(loc))}

    # ---- spike F -----------------------------------------------------------

    def spike_f(self, echo_iters, fanout):
        f = {}
        f["echo"] = {}
        for name in self.w:
            r = self.dispatch(name, "echo", {"value": name})
            f["echo"][name] = {
                "status": r["status"],
                "ran_on_target": r["status"] == "ok"
                and r["result"]["locality"] == self.loc(name)
                and r["result"]["pid"] == self.w[name]["pid"],
                "error": r["error"]}

        lat = []
        for i in range(echo_iters):
            r = self.dispatch("w3", "echo", {"value": i})
            if r["status"] == "ok":
                lat.append((r["t1_ns"] - r["t0_ns"]) / 1e3)
        lat.sort()
        f["echo_latency_us"] = {
            "n": len(lat),
            "p50": statistics.median(lat) if lat else None,
            "p99": lat[int(0.99 * (len(lat) - 1))] if lat else None,
            "min": lat[0] if lat else None, "max": lat[-1] if lat else None}

        r = self.dispatch("w3", "raise")
        f["node_raise"] = {"status": r["status"],
                           "returned_as_value": r["status"] == "ok"
                           and r["result"]["ok"] is False,
                           "result": r.get("result")}

        f["epoch_variants"] = {}
        for label, ep in (("join_epoch", self.epoch("w3")), ("zero", 0),
                          ("join_plus_1", self.epoch("w3") + 1)):
            f["epoch_variants"][label] = {
                "epoch": ep,
                "check_admission": self.ext.check_admission(self.loc("w3"), ep),
                "dispatch_status": self.dispatch("w3", "echo", epoch=ep)["status"]}

        # fanout sleeps of 100 ms spread over all workers, from Python threads
        names = list(self.w)
        t0 = now_ns()
        boxes = [self.dispatch_in_background(names[i % len(names)], "sleep",
                                             {"ms": 100}) for i in range(fanout)]
        for t, _ in boxes:
            t.join(60)
        f["fanout"] = {
            "width": fanout, "node_ms": 100, "workers": len(names),
            "wall_ms": (now_ns() - t0) / 1e6,
            "ok": sum(1 for _, b in boxes if b.get("r", {}).get("status") == "ok")}
        self.rec["F"] = f

    # ---- spike A' ----------------------------------------------------------

    def spike_a(self, detect_timeout_s):
        a = {"target": "w1", "snapshot_before": self.snapshot_localities(),
             "membership_before": list(self.ext.membership())}
        thread, box = self.dispatch_in_background("w1", "sleep", {"ms": 3000})
        time.sleep(0.3)
        t_kill = now_ns()
        os.kill(self.w["w1"]["pid"], signal.SIGKILL)
        a["kill_ns"] = t_kill
        a["detection"] = self.wait_fenced("w1", t_kill, detect_timeout_s)
        a["snapshot_after_detection"] = self.snapshot_localities()
        thread.join(40)
        a["inflight"] = {
            "status": box.get("r", {}).get("status", "never_returned"),
            "error": box.get("r", {}).get("error"),
            "error_code": box.get("r", {}).get("error_code"),
            "returned_after_kill_ms":
                (box["returned_ns"] - t_kill) / 1e6 if "returned_ns" in box
                else None}
        a["dispatch_after_detection"] = self.dispatch("w1", "echo", timeout_s=10)
        a["force_disconnect"] = dict(self.ext.force_disconnect(self.loc("w1")))
        a["membership_after_force_disconnect"] = list(self.ext.membership())
        a["snapshot_after_force_disconnect"] = self.snapshot_localities()
        a["dispatch_after_force_disconnect"] = self.dispatch(
            "w1", "echo", timeout_s=10)
        a["dispatch_after_force_disconnect_skip_client_check"] = self.dispatch(
            "w1", "echo", timeout_s=10, skip_client_check=True)

        # A connector cannot force_disconnect, so ask the root to.
        rf = dict(self.ext.root_force_disconnect(self.loc("w1"), 60.0))
        if rf["status"] == "ok":
            rf["result"] = json.loads(rf["result"])
        a["root_force_disconnect"] = rf
        a["membership_after_root_force_disconnect"] = list(self.ext.membership())
        a["snapshot_after_root_force_disconnect"] = self.snapshot_localities()
        a["dispatch_after_root_force_disconnect"] = self.dispatch(
            "w1", "echo", timeout_s=10)
        a["dispatch_after_root_force_disconnect_skip_client_check"] = self.dispatch(
            "w1", "echo", timeout_s=10, skip_client_check=True)
        a["survivor_dispatch_after_root_force_disconnect"] = self.dispatch(
            "w3", "echo", timeout_s=10)["status"]
        self.rec["A"] = a

    # ---- spike B -----------------------------------------------------------

    def spike_b(self, detect_timeout_s, hold_after_s):
        b = {"target": "w2"}
        pid = self.w["w2"]["pid"]
        # 800 ms node, stopped 200 ms in, so the reply is produced only
        # after the worker resumes, well after it has been fenced.
        thread, box = self.dispatch_in_background("w2", "sleep", {"ms": 800},
                                                  timeout_s=120)
        time.sleep(0.2)
        t_stop = now_ns()
        os.kill(pid, signal.SIGSTOP)
        b["stop_ns"] = t_stop
        b["detection"] = self.wait_fenced("w2", t_stop, detect_timeout_s)
        time.sleep(hold_after_s)
        t_cont = now_ns()
        os.kill(pid, signal.SIGCONT)
        b["cont_ns"] = t_cont
        b["stopped_ms"] = (t_cont - t_stop) / 1e6
        thread.join(60)
        r = box.get("r", {})
        b["inflight_late_result"] = {
            "status": r.get("status", "never_returned"),
            "error": r.get("error"),
            "returned_after_cont_ms":
                (box["returned_ns"] - t_cont) / 1e6 if "returned_ns" in box
                else None,
            "returned_after_fence_ms":
                (box["returned_ns"] - t_stop) / 1e6
                - b["detection"].get("detect_poll_ms", 0)
                if "returned_ns" in box and b["detection"]["fenced"] else None,
            "would_need_commit_fence": r.get("status") == "ok"
            and b["detection"]["fenced"]}
        time.sleep(2.0)
        b["after_resume"] = {
            "check_admission": self.ext.check_admission(self.loc("w2"),
                                                        self.epoch("w2")),
            "shadow_state": dict(self.ext.peer_state(self.loc("w2"))),
            "snapshot": self.snapshot_localities()}
        n_calls_before = self.count_calls("w2")
        b["dispatch_normal"] = self.dispatch("w2", "echo", timeout_s=10)
        b["dispatch_skip_client_check"] = self.dispatch(
            "w2", "echo", timeout_s=10, skip_client_check=True)
        time.sleep(0.5)
        b["zombie_executed_new_calls"] = self.count_calls("w2") - n_calls_before
        b["zombie_self_view"] = read_json(
            os.path.join(self.bootdir, "w2.self.json"))
        b["zombie_gaps"] = self.read_lines("w2.gaps.jsonl")
        self.rec["B"] = b

    # ---- spike B8 (F8 preview) ---------------------------------------------

    def spike_b8(self, short_stop_s, observe_s):
        c = {"target": "w3", "short_stop_s": short_stop_s,
             "fd_poll_ms": self.fd_poll_ms}
        pid = self.w["w3"]["pid"]
        t_stop = now_ns()
        os.kill(pid, signal.SIGSTOP)
        time.sleep(short_stop_s)
        os.kill(pid, signal.SIGCONT)
        c["detection"] = self.wait_fenced("w3", t_stop, observe_s)
        c["false_eviction"] = c["detection"]["fenced"]
        c["dispatch_after"] = self.dispatch("w3", "echo", timeout_s=10)["status"]
        self.rec["B8"] = c

    # ---- helpers -----------------------------------------------------------

    def read_lines(self, fname):
        path = os.path.join(self.bootdir, fname)
        if not os.path.exists(path):
            return []
        with open(path) as f:
            return [json.loads(x) for x in f if x.strip()]

    def count_calls(self, name):
        return len(self.read_lines(f"{name}.calls.jsonl"))


def join_workers(ext, workers, timeout_s):
    """Run discover_and_join until every worker locality is joined."""
    want = {w["locality"] for w in workers.values()}
    deadline = time.monotonic() + timeout_s
    passes = 0
    joined = {}
    while time.monotonic() < deadline:
        passes += 1
        for p in ext.discover_and_join(2000):
            joined[p["locality"]] = p["join_epoch"]
        if want <= set(joined):
            break
    return joined, passes


def main():
    """Join the workers, run the requested spikes, write coord_result.json."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootdir", required=True)
    ap.add_argument("--build-dir", required=True)
    ap.add_argument("--root-port", type=int, required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--fd-poll-ms", type=int, default=500)
    ap.add_argument("--workers", default="w1,w2,w3")
    ap.add_argument("--spikes", default="F,A,B,B8")
    ap.add_argument("--echo-iters", type=int, default=200)
    ap.add_argument("--fanout", type=int, default=32)
    ap.add_argument("--detect-timeout-s", type=float, default=60.0)
    ap.add_argument("--hold-after-s", type=float, default=1.0)
    ap.add_argument("--short-stop-s", type=float, default=0.5)
    a = ap.parse_args()

    out_path = os.path.join(a.bootdir, "coord_result.json")
    rec = {"args": vars(a), "t_start_ns": now_ns()}
    ext = None
    try:
        ext = import_ext(a.build_dir)
        rec["version"] = dict(ext.version_info())
        ext.start_connect(a.threads, connector_args(a.root_port, a.port))
        rec["init"] = dict(ext.supervision_init(5000, a.fd_poll_ms))

        workers = {}
        for name in a.workers.split(","):
            path = os.path.join(a.bootdir, f"{name}.started")
            if not wait_for_file(path, 60):
                raise RuntimeError(f"{name} never started")
            m = read_json(path)
            workers[name] = {"pid": m["pid"], "locality": m["locality"],
                             "own_epoch": m["init"]["epoch"]}

        joined, passes = join_workers(ext, workers, 60)
        missing = [n for n, w in workers.items()
                   if w["locality"] not in joined]
        rec["join"] = {"passes": passes, "joined": joined, "missing": missing,
                       "snapshot": [dict(p) for p in ext.snapshot_peers()]}
        if missing:
            raise RuntimeError(f"workers not joined: {missing}")
        for w in workers.values():
            w["epoch"] = joined[w["locality"]]
        rec["workers"] = workers

        c = Coordinator(ext, a.bootdir, workers, a.fd_poll_ms)
        spikes = a.spikes.split(",")
        try:
            if "F" in spikes:
                c.spike_f(a.echo_iters, a.fanout)
            if "A" in spikes:
                c.spike_a(a.detect_timeout_s)
            if "B" in spikes:
                c.spike_b(a.detect_timeout_s, a.hold_after_s)
            if "B8" in spikes:
                c.spike_b8(a.short_stop_s, max(10.0, 6 * a.fd_poll_ms / 1000.0))
        finally:
            rec.update(c.rec)
        rec["ok"] = True
    except Exception as ex:  # noqa: BLE001  record and exit, never hang
        rec["ok"] = False
        rec["error"] = f"{type(ex).__name__}: {ex}"
    rec["t_end_ns"] = now_ns()
    write_json(out_path, rec)
    if ext is not None:
        leave(ext, os.path.join(a.bootdir, "coord.exit"))
    os._exit(0)


if __name__ == "__main__":
    main()
