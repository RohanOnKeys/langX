"""Failure matrix F1 to F9 for ``research_agent`` (plan sections 13, 14).

Each trial runs in its own process, since an HPX locality starts once per
process. A trial computes the stock LangGraph baseline for its run id, starts
a fresh island (``langx_root`` plus workers), runs the agent on HPX with one
injected fault, writes ``trial.json`` and then shuts the island down. This
script runs the trials one after another and appends each record to
``exp01.jsonl``; ``analyze_exp01.py`` computes the gates.

The target is the first search task, ``search:0``, attempt 1:

  F1  SIGKILL its worker right after the coordinator picked it
  F2  SIGKILL its worker halfway through the node's latency
  F3  SIGKILL its worker after its Tier B effect, before the reply
  F4  SIGSTOP its worker until the coordinator sees it fenced, hold one poll
      timeout more, then SIGCONT
  F5  SIGKILL the workers of search:0 and search:1 in one superstep (4 workers)
  F6  as F2, then root force_disconnect and a replacement worker that joins
  F7  SIGKILL the coordinator mid-superstep; the old workers are killed, fresh
      ones start, and a new coordinator resumes from the SQLite checkpoint
  F8  SIGSTOP its worker for less than the detection time
  F9  give its dispatch a timeout shorter than the node latency

Signals come from the harness, which knows the PIDs; detection comes only
from HPX: the coordinator sees a worker fenced when ``Runtime.admitted``
turns False. Every timestamp is CLOCK_MONOTONIC on one node.

    python experiments/01_durable_langgraph/run_exp01.py --scenario F2 \\
        --trials 10 --out experiments/01_durable_langgraph/_runs/exp01
"""

import argparse
import asyncio
import json
import os
import pathlib
import signal
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path[:0] = [str(HERE), str(REPO / "python" / "src")]

import graph  # noqa: E402
from langx.coordinator import Coordinator  # noqa: E402
from langx.effects import EffectLog, Outbox  # noqa: E402
from langx.island import Island, free_port, wait_json  # noqa: E402
from langx.runtime import Peer, Runtime  # noqa: E402

PRELOAD = [f"agent_nodes:{n}" for n in graph.NODES]
SCENARIOS = ("F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9")
DEFAULTS = {"workers": 2, "width": 8, "loops": 2, "latency_ms": 400}
PER_SCENARIO = {"F5": {"workers": 4},
                "F6": {"width": 16, "loops": 3, "latency_ms": 200}}
HPX_FENCE_REASONS = {"target_fenced", "dispatch_failed", "fenced_at_commit"}
POST_EFFECT_MS = 400    # F3: time between the Tier B effect and the reply


def params(a):
    """Scenario defaults, overridden by the flags that were given."""
    p = dict(DEFAULTS, **PER_SCENARIO.get(a.scenario, {}))
    for k in DEFAULTS:
        if getattr(a, k) is not None:
            p[k] = getattr(a, k)
    p["poll_ms"] = a.poll_ms
    p["stop_ms"] = a.stop_ms if a.stop_ms is not None else a.poll_ms // 2
    return p


def direct_cfg(path, mode):
    """Tier B sink settings for the initial state, or None."""
    if not mode:
        return None
    return {"path": str(path), "mode": mode, "post_ms": POST_EFFECT_MS}


def tier_b(path, mode):
    """Rows and duplicate keys in a Tier B sink."""
    if not mode or not pathlib.Path(path).exists():
        return None
    log = EffectLog(path, keyed=(mode == "keyed"))
    try:
        return {"rows": len(log.rows()), "keys": len(log.keys()),
                "duplicates": log.duplicates()}
    finally:
        log.close()


def baseline(run_id, p, tdir, mode):
    """Stock LangGraph, every node in this process."""
    log = EffectLog(tdir / "baseline_effects.sqlite")
    state = graph.initial_state(run_id, p["width"], p["loops"], p["latency_ms"],
                                direct=direct_cfg(tdir / "baseline_direct.sqlite",
                                                  mode))
    t0 = time.monotonic()
    final = asyncio.run(graph.run(graph.build(graph.local_nodes(Outbox(log))),
                                  run_id, state))
    return {"hash": graph.state_hash(final),
            "wall_s": round(time.monotonic() - t0, 3),
            "outbox_keys": sorted(log.keys("outbox")),
            "tier_b": tier_b(tdir / "baseline_direct.sqlite", mode)}


def write_json(path, obj):
    tmp = pathlib.Path(f"{path}.tmp")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True))
    os.replace(tmp, path)


# ---- fault injection, inside the coordinator process ----------------------

class Injector:
    """Turns the coordinator's dispatch hook into the scenario's fault."""

    def __init__(self, scenario, p, isl, rt):
        self.s = scenario
        self.p = p
        self.isl = isl
        self.rt = rt
        self.coord = None
        self.lat = p["latency_ms"] / 1000.0
        self.by_loc = {w["locality"]: (n, w) for n, w in isl.workers().items()}
        self.targets = {}
        self.signals = []
        self.fenced_seen = {}
        self.killed = set()
        self.stopped = set()
        self.notes = {}
        self.tasks = []
        self.watchers = []

    def start(self, peers):
        """Watch every worker for the whole run, as the coordinator sees it."""
        self.notes["membership_start"] = self.rt.membership()
        for peer in peers:
            self.watch(peer)

    def watch(self, peer):
        self.watchers.append(asyncio.ensure_future(self._watch(peer)))

    async def _watch(self, peer):
        while self.rt.admitted(peer):
            await asyncio.sleep(0.002)
        self.fenced_seen.setdefault(peer.locality, time.monotonic())

    def spawn(self, coro):
        self.tasks.append(asyncio.ensure_future(coro))

    def later(self, delay_s, fn, *args):
        async def run():
            await asyncio.sleep(delay_s)
            fn(*args)
        self.spawn(run())

    def send(self, peer, sig):
        name, info = self.by_loc[peer.locality]
        self.signals.append({"t": time.monotonic(), "signal": sig.name,
                             "locality": peer.locality, "worker": name})
        os.kill(info["pid"], sig)
        if sig == signal.SIGKILL:
            self.killed.add(name)
        elif sig == signal.SIGSTOP:
            self.stopped.add(name)
        elif sig == signal.SIGCONT:
            self.stopped.discard(name)

    def kill(self, peer):
        self.send(peer, signal.SIGKILL)

    def on_dispatch(self, key, attempt, peer):
        """Coordinator hook: arm the fault when the target is dispatched."""
        wanted = ("search:0", "search:1") if self.s == "F5" else ("search:0",)
        if attempt != 1 or key[2] not in wanted or key[2] in self.targets:
            return None
        self.targets[key[2]] = {"key": list(key), "locality": peer.locality,
                                "t": time.monotonic()}
        if self.s == "F1":
            self.kill(peer)
        elif self.s in ("F2", "F6"):
            self.later(self.lat / 2, self.kill, peer)
            if self.s == "F6":
                self.spawn(self.replace(peer))
        elif self.s == "F3":
            self.later(self.lat + POST_EFFECT_MS / 2000.0, self.kill, peer)
        elif self.s == "F4":
            self.spawn(self.zombie(peer))
        elif self.s == "F5" and len(self.targets) == 2:
            self.later(self.lat / 2, self.kill_many)
        elif self.s == "F8":
            self.spawn(self.pause(peer))
        elif self.s == "F9":
            return self.lat / 4
        return None

    def kill_many(self):
        """F5: the workers of both targets; another one if they coincide."""
        locs = []
        for t in self.targets.values():
            if t["locality"] not in locs:
                locs.append(t["locality"])
        for p in self.coord.epochs.live():
            if len(locs) >= 2:
                break
            if p.locality not in locs:
                locs.append(p.locality)
        for loc in locs:
            self.kill(Peer(loc, self.by_loc[loc][1]["epoch"]))

    async def wait_fenced(self, locality, timeout_s=20.0):
        deadline = time.monotonic() + timeout_s
        while locality not in self.fenced_seen and time.monotonic() < deadline:
            await asyncio.sleep(0.005)
        return locality in self.fenced_seen

    async def zombie(self, peer):
        await asyncio.sleep(self.lat / 2)
        self.send(peer, signal.SIGSTOP)
        self.notes["zombie_fenced_while_stopped"] = await self.wait_fenced(
            peer.locality)
        await asyncio.sleep(self.p["poll_ms"] / 1000.0)
        self.send(peer, signal.SIGCONT)

    async def pause(self, peer):
        await asyncio.sleep(self.lat / 4)
        self.send(peer, signal.SIGSTOP)
        await asyncio.sleep(self.p["stop_ms"] / 1000.0)
        self.send(peer, signal.SIGCONT)

    async def replace(self, peer):
        """F6: once the kill is seen, remove the locality and add a new one."""
        n = self.notes
        try:
            if not await self.wait_fenced(peer.locality):
                n["replace_error"] = "kill not detected"
                return
            n["membership_fenced"] = self.rt.membership()
            report = await self.rt.force_disconnect(peer.locality)
            n["force_disconnect"] = {k: report.get(k) for k in
                                     ("rc", "elapsed_ms", "membership_after")}
            info = await asyncio.to_thread(self.isl.start_worker, "r1")
            joined = await asyncio.to_thread(self.rt.wait_for_peers,
                                             [info["locality"]])
            new = joined[info["locality"]]
            self.by_loc[new.locality] = ("r1", info)
            self.coord.add_workers([new])
            self.watch(new)
            n["replacement"] = {"locality": new.locality, "epoch": new.epoch,
                                "joined_t": time.monotonic()}
            n["membership_after_join"] = self.rt.membership()
        except Exception as ex:  # noqa: BLE001  recorded, judged by analysis
            n["replace_error"] = f"{type(ex).__name__}: {ex}"[:300]

    async def settle(self):
        """Let pending injections finish and late replies arrive."""
        pending = [t for t in self.tasks if not t.done()]
        if pending:
            await asyncio.wait(pending, timeout=30)
        for name in list(self.stopped):   # never leave a worker stopped
            info = self.isl.workers()[name]
            self.send(Peer(info["locality"], info["epoch"]), signal.SIGCONT)
        await asyncio.sleep(self.lat + 1.0)
        for w in self.watchers:
            w.cancel()


async def run_hpx(g, run_id, state, inj, peers, timeout_s):
    inj.start(peers)
    t0 = time.monotonic()
    out = {"t0": t0}
    try:
        final = await asyncio.wait_for(graph.run(g, run_id, state), timeout_s)
        out.update(completed=True, hash=graph.state_hash(final))
    except Exception as ex:  # noqa: BLE001  a failed run is a result
        out.update(completed=False, error=f"{type(ex).__name__}: {ex}"[:300])
    out["wall_s"] = round(time.monotonic() - t0, 3)
    await inj.settle()
    return out


def summarize(rec, coord, rt, inj, log, direct_path, mode):
    """Gate inputs and metrics from the coordinator's own records."""
    ev = coord.events
    kinds = {}
    for e in ev:
        kinds[e["event"]] = kinds.get(e["event"], 0) + 1
    t_inject = min([s["t"] for s in inj.signals]
                   + ([t["t"] for t in inj.targets.values()] if inj.s == "F9"
                      else []), default=None)
    signaled = {}
    for s in inj.signals:
        signaled.setdefault(s["locality"], s["t"])
    detection = {str(loc): round((inj.fenced_seen[loc] - t) * 1e3, 1)
                 for loc, t in signaled.items() if loc in inj.fenced_seen}
    commits = {}
    for e in ev:
        if e["event"] == "committed":
            commits.setdefault((e["run_id"], e["step"], e["node"]), e)
    recovery = None
    if t_inject is not None:
        ts = [commits[tuple(t["key"])]["t"] for t in inj.targets.values()
              if tuple(t["key"]) in commits]
        if ts and len(ts) == len(inj.targets):
            recovery = round((max(ts) - t_inject) * 1e3, 1)
    stale = [e for e in ev if e["event"] == "committed"
             and e["locality"] in inj.fenced_seen
             and e["t"] >= inj.fenced_seen[e["locality"]]]
    late = [dict(x, after_run_s=round(x["t"] - rec["hpx"]["t0"], 3))
            for x in rt.late]
    first_attempts = {f"{'/'.join(map(str, t['key']))}/1" for t in inj.targets.values()}
    accepted = {"/".join(map(str, k)): v["attempt_id"]
                for k, v in coord.accepted.items()
                if k[2] in inj.targets}
    rec.update({
        "events": kinds,
        "signals": [dict(s, t_rel_ms=round((s["t"] - rec["hpx"]["t0"]) * 1e3, 1))
                    for s in inj.signals],
        "targets": {k: {"locality": v["locality"], "key": v["key"]}
                    for k, v in inj.targets.items()},
        "detection_ms": detection,
        "recovery_ms": recovery,
        "unsignaled_fenced": sorted(loc for loc in inj.fenced_seen
                                    if loc not in signaled),
        "fence_reasons": {str(k): v for k, v in coord.epochs.fenced().items()},
        "retries": [{"node": e["node"], "locality": e["locality"],
                     "detail": e["detail"]} for e in ev if e["event"] == "retry"],
        "rejected": [{"node": e["node"], "locality": e["locality"],
                      "detail": e["detail"]} for e in ev if e["event"] == "rejected"],
        "stale_commits": len(stale),
        "late_replies": late,
        "target_accepted_attempt": accepted,
        "first_attempt_committed": sorted(set(accepted.values()) & first_attempts),
        "outbox": {"keys_equal": sorted(log.keys("outbox"))
                   == rec["baseline"]["outbox_keys"],
                   "keys": len(log.keys("outbox")),
                   "duplicates": log.duplicates("outbox")},
        "tier_b": tier_b(direct_path, mode),
        "notes": {k: v for k, v in inj.notes.items()},
    })
    if inj.notes.get("replacement"):
        loc = inj.notes["replacement"]["locality"]
        rec["replacement_commits"] = sum(1 for e in ev if e["event"] == "committed"
                                         and e["locality"] == loc)
    rec["hpx"]["hash_equal"] = rec["hpx"].get("hash") == rec["baseline"]["hash"]


def shutdown(isl, rt, killed):
    """Stop everything that is still up; record how each locality left."""
    ex = {"workers": {}}
    reports = isl.stop_workers(timeout_s=20)
    for name in isl.workers():
        if name in killed:
            ex["workers"][name] = "killed_by_harness"
        else:
            r = reports.get(name)
            ex["workers"][name] = r["exit_path"] if r else "no_exit_report"
    finalized, rc = rt.shutdown()
    ex["coordinator"] = "clean" if finalized and rc == 0 else (
        f"finalize_timeout rc={rc}" if not finalized else f"stop_rc={rc}")
    r = isl.stop_root()
    ex["root"] = "clean" if r else "no_exit_report"
    return ex


def trial(a):
    """One trial of F1 to F6, F8 or F9, in this process."""
    p = params(a)
    tdir = pathlib.Path(a.trial_dir)
    tdir.mkdir(parents=True, exist_ok=True)
    run_id = f"{a.scenario}-{a.seed}"
    mode = a.tier_b if a.scenario == "F3" else None
    rec = {"scenario": a.scenario, "seed": a.seed, "run_id": run_id,
           "params": p, "tier_b_mode": mode}
    rec["baseline"] = baseline(run_id, p, tdir, mode)

    (tdir / "island").mkdir(exist_ok=True)
    isl = Island(tdir / "island", n_workers=p["workers"],
                 poll_timeout_ms=p["poll_ms"], preload=PRELOAD,
                 pythonpath=[HERE]).start()
    inj = None
    try:
        rt = Runtime()
        rt.start(isl.agas, f"localhost:{free_port()}")
        rt.init(5.0, p["poll_ms"] / 1000.0)
        joined = rt.wait_for_peers([w["locality"] for w in isl.workers().values()])
        log = EffectLog(tdir / "hpx_effects.sqlite")
        inj = Injector(a.scenario, p, isl, rt)
        coord = Coordinator(rt, Outbox(log), on_dispatch=inj.on_dispatch)
        inj.coord = coord
        coord.add_workers(joined.values())
        direct_path = tdir / "hpx_direct.sqlite"
        state = graph.initial_state(run_id, p["width"], p["loops"],
                                    p["latency_ms"],
                                    direct=direct_cfg(direct_path, mode))
        rec["hpx"] = asyncio.run(run_hpx(graph.build(graph.remote_nodes(coord)),
                                         run_id, state, inj,
                                         list(joined.values()), a.run_timeout_s))
        summarize(rec, coord, rt, inj, log, direct_path, mode)
        write_json(tdir / "trial.json", rec)
        rec["exit"] = shutdown(isl, rt, inj.killed)
        write_json(tdir / "trial.json", rec)
    finally:
        isl.cleanup()
    return 0


# ---- F7: coordinator crash ------------------------------------------------

def coordinator_process(a):
    """F7 coordinator: optionally removes old localities, then runs or resumes."""
    p = params(a)
    tdir = pathlib.Path(a.trial_dir)
    rt = Runtime()
    rt.start(a.agas, f"localhost:{free_port()}")
    me = rt.init(5.0, p["poll_ms"] / 1000.0)
    write_json(tdir / f"{a.name}.ready", {"locality": me.locality,
                                          "epoch": me.epoch, "pid": os.getpid()})
    rec = {"name": a.name, "locality": me.locality, "force_disconnect": {}}
    for loc in [int(x) for x in a.disconnect.split(",") if x]:
        try:
            r = asyncio.run(rt.force_disconnect(loc))
            rec["force_disconnect"][str(loc)] = {"rc": r["rc"],
                                                 "elapsed_ms": r.get("elapsed_ms")}
        except Exception as ex:  # noqa: BLE001  recorded
            rec["force_disconnect"][str(loc)] = {"error": str(ex)[:200]}
    rec["membership"] = rt.membership()
    joined = rt.wait_for_peers([int(x) for x in a.localities.split(",")])
    rec["joined_t"] = time.monotonic()
    log = EffectLog(tdir / "hpx_effects.sqlite")
    coord = Coordinator(rt, Outbox(log))
    coord.add_workers(joined.values())
    try:
        rec.update(asyncio.run(_coordinate(coord, a, p, tdir)))
    except Exception as ex:  # noqa: BLE001  e.g. no SQLite checkpointer
        rec.update(completed=False, error=f"{type(ex).__name__}: {ex}"[:300])
    rec["events"] = coord.events
    write_json(tdir / f"{a.name}.json", rec)
    finalized, rc = rt.shutdown()
    rec["exit"] = "clean" if finalized and rc == 0 else f"finalized={finalized} rc={rc}"
    write_json(tdir / f"{a.name}.json", rec)
    return 0


async def _coordinate(coord, a, p, tdir):
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver  # noqa: PLC0415

    events_path = tdir / f"{a.name}.events.jsonl"

    async def flush():
        done = 0
        with open(events_path, "a") as f:
            while True:
                for e in coord.events[done:]:
                    f.write(json.dumps(e) + "\n")
                done = len(coord.events)
                f.flush()
                await asyncio.sleep(0.01)

    async with AsyncSqliteSaver.from_conn_string(str(tdir / "checkpoint.sqlite")) as saver:
        g = graph.build(graph.remote_nodes(coord), checkpointer=saver)
        state = None if a.resume else graph.initial_state(
            a.run_id, p["width"], p["loops"], p["latency_ms"])
        flusher = asyncio.ensure_future(flush())
        t0 = time.monotonic()
        try:
            final = await asyncio.wait_for(
                graph.run(g, a.run_id, state, durability=a.durability),
                a.run_timeout_s)
            out = {"completed": True, "hash": graph.state_hash(final)}
        except Exception as ex:  # noqa: BLE001  a failed run is a result
            out = {"completed": False, "error": f"{type(ex).__name__}: {ex}"[:300]}
        out["wall_s"] = round(time.monotonic() - t0, 3)
        flusher.cancel()
    return out


def read_events(path):
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        try:
            out.append(json.loads(line))
        except ValueError:
            pass   # a line cut short by the SIGKILL
    return out


def trial_f7(a):
    """F7 driver: no HPX here; island and coordinators are child processes."""
    p = params(a)
    tdir = pathlib.Path(a.trial_dir)
    tdir.mkdir(parents=True, exist_ok=True)
    run_id = f"F7-{a.seed}"
    rec = {"scenario": "F7", "seed": a.seed, "run_id": run_id, "params": p,
           "durability": a.durability, "tier_b_mode": None}
    rec["baseline"] = baseline(run_id, p, tdir, None)
    (tdir / "island").mkdir(exist_ok=True)
    isl = Island(tdir / "island", n_workers=p["workers"],
                 poll_timeout_ms=p["poll_ms"], preload=PRELOAD,
                 pythonpath=[HERE]).start()
    procs = {}
    env = dict(os.environ)

    def coordinator(name, localities, resume, disconnect=()):
        cmd = [sys.executable, __file__, "--role", "coordinator", "--name", name,
               "--scenario", "F7", "--seed", str(a.seed), "--trial-dir", str(tdir),
               "--agas", isl.agas, "--run-id", run_id,
               "--localities", ",".join(map(str, localities)),
               "--disconnect", ",".join(map(str, disconnect)),
               "--durability", a.durability, "--poll-ms", str(p["poll_ms"]),
               "--run-timeout-s", str(a.run_timeout_s)]
        for k in DEFAULTS:
            cmd += [f"--{k.replace('_', '-')}", str(p[k])]
        if resume:
            cmd.append("--resume")
        procs[name] = subprocess.Popen(
            cmd, stdout=open(tdir / f"{name}.log", "w"), stderr=subprocess.STDOUT,
            env=env, start_new_session=True)
        return procs[name]

    try:
        old = isl.workers()
        c1 = coordinator("c1", [w["locality"] for w in old.values()], False)
        c1_ready = wait_json(tdir / "c1.ready", 60)
        kill_after = a.kill_after or 1 + p["width"] // 2
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and c1.poll() is None:
            if sum(e["event"] == "committed"
                   for e in read_events(tdir / "c1.events.jsonl")) >= kill_after:
                break
            time.sleep(0.005)
        t_kill = time.monotonic()
        os.kill(c1.pid, signal.SIGKILL)
        c1.wait(10)
        rec["c1_gone"] = c1.poll() is not None
        c1_events = read_events(tdir / "c1.events.jsonl")
        rec["c1_commits"] = sum(e["event"] == "committed" for e in c1_events)

        # Plan 9.3 step 4: the old workers are fenced by the loss; replace them.
        for name in old:
            isl.kill(name)
        fresh = {}
        for i in range(p["workers"]):
            name = f"w{p['workers'] + i + 1}"
            fresh[name] = isl.start_worker(name)
        # Plan 9.3 step 3: the new coordinator also removes the dead one.
        gone = [w["locality"] for w in old.values()]
        if c1_ready:
            gone.insert(0, c1_ready["locality"])
        c2 = coordinator("c2", [w["locality"] for w in fresh.values()], True, gone)
        try:
            c2.wait(a.run_timeout_s + 60)
        except subprocess.TimeoutExpired:
            os.killpg(c2.pid, signal.SIGKILL)
        r2 = wait_json(tdir / "c2.json", 5) or {}
        ev2 = r2.get("events", [])
        c1_done = {(e["run_id"], e["step"], e["node"]) for e in c1_events
                   if e["event"] == "committed"}
        c2_done = [(e["run_id"], e["step"], e["node"]) for e in ev2
                   if e["event"] == "committed"]
        first_commit = min((e["t"] for e in ev2 if e["event"] == "committed"),
                           default=None)
        log = EffectLog(tdir / "hpx_effects.sqlite")
        rec["hpx"] = {"completed": r2.get("completed", False),
                      "error": r2.get("error"), "hash": r2.get("hash"),
                      "wall_s": r2.get("wall_s")}
        rec["hpx"]["hash_equal"] = rec["hpx"]["hash"] == rec["baseline"]["hash"]
        rec.update({
            "c1_locality": (c1_ready or {}).get("locality"),
            "force_disconnect": r2.get("force_disconnect"),
            "membership_c2": r2.get("membership"),
            "recovery_ms": round((first_commit - t_kill) * 1e3, 1)
            if first_commit else None,
            "c2_ready_ms": round((r2["joined_t"] - t_kill) * 1e3, 1)
            if r2.get("joined_t") else None,
            "c2_commits": len(c2_done),
            "rerun_after_commit": len([k for k in c2_done if k in c1_done]),
            "effects_applied": sum(int(e["detail"].split("=")[1])
                                   for e in c1_events + ev2
                                   if e["event"] == "committed"),
            "outbox": {"keys_equal": sorted(log.keys("outbox"))
                       == rec["baseline"]["outbox_keys"],
                       "keys": len(log.keys("outbox")),
                       "duplicates": log.duplicates("outbox")},
            "stale_commits": 0,
            "fence_reasons": {},
            "root_alive": isl.procs["root"].poll() is None,
        })
        write_json(tdir / "trial.json", rec)
        reports = isl.stop_workers(timeout_s=20)
        rec["exit"] = {
            "workers": dict({n: "killed_by_harness" for n in old},
                            **{n: (r or {}).get("exit_path", "no_exit_report")
                               for n, r in reports.items()}),
            "coordinator": {"c1": "killed_by_harness",
                            "c2": r2.get("exit", "no_exit_report")},
            "root": "clean" if isl.stop_root() else "no_exit_report"}
        write_json(tdir / "trial.json", rec)
    finally:
        for pr in procs.values():
            if pr.poll() is None:
                try:
                    os.killpg(pr.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        isl.cleanup()
    return 0


# ---- driver ---------------------------------------------------------------

def run_trials(a):
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    tag = a.scenario + (f"-{a.tier_b}" if a.scenario == "F3" else "") + (
        f"-{a.durability}" if a.scenario == "F7" else "")
    failed = 0
    for i in range(a.first_seed, a.first_seed + a.trials):
        tdir = out / "trials" / f"{tag}-{i}"
        cmd = [sys.executable, __file__, "--role", "trial", "--scenario", a.scenario,
               "--seed", str(i), "--trial-dir", str(tdir), "--tier-b", a.tier_b,
               "--durability", a.durability, "--poll-ms", str(a.poll_ms),
               "--run-timeout-s", str(a.run_timeout_s)]
        for k in ("workers", "width", "loops", "latency_ms", "stop_ms", "kill_after"):
            if getattr(a, k) is not None:
                cmd += [f"--{k.replace('_', '-')}", str(getattr(a, k))]
        tdir.mkdir(parents=True, exist_ok=True)
        t0 = time.monotonic()
        proc = subprocess.Popen(cmd, stdout=open(tdir / "trial.log", "w"),
                                stderr=subprocess.STDOUT, start_new_session=True)
        try:
            rc = proc.wait(a.run_timeout_s + 180)
            timed_out = False
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            rc, timed_out = proc.wait(), True
        try:
            rec = json.loads((tdir / "trial.json").read_text())
        except (OSError, ValueError):
            rec = {"scenario": a.scenario, "seed": i, "params": None,
                   "hpx": {"completed": False, "error": "no trial record"}}
        rec.update(tag=tag, trial_rc=rc, trial_timed_out=timed_out,
                   trial_s=round(time.monotonic() - t0, 1))
        if "exit" not in rec:
            rec["exit"] = {"coordinator": f"no exit record, trial rc={rc}"}
        with open(out / "exp01.jsonl", "a") as f:
            f.write(json.dumps(rec, sort_keys=True) + "\n")
        hpx = rec.get("hpx", {})
        ok = hpx.get("completed") and hpx.get("hash_equal")
        failed += not ok
        print(json.dumps({"tag": tag, "seed": i, "completed": hpx.get("completed"),
                          "hash_equal": hpx.get("hash_equal"),
                          "detection_ms": rec.get("detection_ms"),
                          "recovery_ms": rec.get("recovery_ms"),
                          "stale_commits": rec.get("stale_commits"),
                          "trial_s": rec["trial_s"], "rc": rc}), flush=True)
    return 1 if failed else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", choices=SCENARIOS, required=True)
    ap.add_argument("--trials", type=int, default=10)
    ap.add_argument("--first-seed", type=int, default=0)
    ap.add_argument("--out", default="")
    ap.add_argument("--workers", type=int)
    ap.add_argument("--width", type=int)
    ap.add_argument("--loops", type=int)
    ap.add_argument("--latency-ms", type=int)
    ap.add_argument("--poll-ms", type=int, default=500)
    ap.add_argument("--stop-ms", type=int, help="F8 pause; default half the poll")
    ap.add_argument("--tier-b", choices=["naive", "keyed"], default="naive",
                    help="F3: Tier B sink mode")
    ap.add_argument("--durability", choices=["sync", "async"], default="sync",
                    help="F7: LangGraph checkpoint durability")
    ap.add_argument("--kill-after", type=int,
                    help="F7: kill the coordinator after this many commits")
    ap.add_argument("--run-timeout-s", type=float, default=120.0)
    # internal
    ap.add_argument("--role", choices=["driver", "trial", "coordinator"],
                    default="driver")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--trial-dir", default="")
    ap.add_argument("--agas", default="")
    ap.add_argument("--name", default="")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--localities", default="")
    ap.add_argument("--disconnect", default="")
    ap.add_argument("--resume", action="store_true")
    a = ap.parse_args()

    import make_fixtures  # noqa: PLC0415
    p = params(a)
    if p["width"] not in make_fixtures.WIDTHS or p["loops"] not in make_fixtures.LOOPS:
        ap.error(f"width must be one of {make_fixtures.WIDTHS} and loops one of "
                 f"{make_fixtures.LOOPS}: only those prompts are recorded")
    if a.role == "coordinator":
        code = coordinator_process(a)
    elif a.role == "trial":
        code = trial_f7(a) if a.scenario == "F7" else trial(a)
    else:
        if not a.out:
            ap.error("--out is required")
        code = run_trials(a)
    sys.stdout.flush()
    if a.role == "driver":
        sys.exit(code)
    # A trial's record is written before shutdown, and interpreter teardown
    # must not run on top of a stopped HPX runtime.
    os._exit(code)


if __name__ == "__main__":
    main()
