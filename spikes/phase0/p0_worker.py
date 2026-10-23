"""Phase 0 worker: one late-connecting HPX locality serving run_node.

Registers under supervision, installs the node handler, then idles. While
idle it writes its own supervision view to <name>.self.json, and records any
wall-clock gap in its loop (a SIGSTOP) to <name>.gaps.jsonl, so spike B can
see what a resumed zombie believes about itself.
"""

import argparse
import json
import os
import time

from p0_common import (append_jsonl, connector_args, import_ext, leave,
                       now_ns, write_json)


def make_handler(ext, name, bootdir, locality):
    """Build the Python callable that run_node invokes on this worker."""
    calls = os.path.join(bootdir, f"{name}.calls.jsonl")

    def handler(node, payload):
        t0 = now_ns()
        try:
            args = json.loads(payload) if payload else {}
            if node == "echo":
                out = {"ok": True, "value": args.get("value")}
            elif node == "sleep":
                time.sleep(args.get("ms", 0) / 1000.0)
                out = {"ok": True, "slept_ms": args.get("ms", 0)}
            elif node == "raise":
                raise ValueError("node raised on purpose")
            else:
                out = {"ok": False, "error_kind": "unknown_node", "error": node}
        except Exception as ex:  # noqa: BLE001  node errors are values
            out = {"ok": False, "error_kind": "node_exception",
                   "error": f"{type(ex).__name__}: {ex}"}
        out.update({"node": node, "worker": name, "locality": locality,
                    "pid": os.getpid(), "t_start_ns": t0, "t_end_ns": now_ns()})
        append_jsonl(calls, {"node": node, "t_start_ns": t0,
                             "t_end_ns": out["t_end_ns"], "ok": out["ok"]})
        return json.dumps(out)

    return handler


def main():
    """Start the locality, serve until <name>.stop appears."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--bootdir", required=True)
    ap.add_argument("--build-dir", required=True)
    ap.add_argument("--root-port", type=int, required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--fd-poll-ms", type=int, default=500)
    a = ap.parse_args()

    ext = import_ext(a.build_dir)
    ext.start_connect(a.threads, connector_args(a.root_port, a.port))
    init = ext.supervision_init(5000, a.fd_poll_ms)
    locality = ext.locality_id()
    ext.set_node_handler(make_handler(ext, a.name, a.bootdir, locality))
    write_json(os.path.join(a.bootdir, f"{a.name}.started"),
               {"name": a.name, "pid": os.getpid(), "locality": locality,
                "init": init, "version": ext.version_info(), "t_ns": now_ns()})

    stop = os.path.join(a.bootdir, f"{a.name}.stop")
    self_path = os.path.join(a.bootdir, f"{a.name}.self.json")
    gaps = os.path.join(a.bootdir, f"{a.name}.gaps.jsonl")
    last = now_ns()
    while not os.path.exists(stop):
        time.sleep(0.1)
        t = now_ns()
        if t - last > 1_000_000_000:
            append_jsonl(gaps, {"gap_ms": (t - last) / 1e6, "resumed_ns": t,
                                "own_state": ext.own_state()})
        last = t
        write_json(self_path, {"t_ns": t, "own_state": ext.own_state()})
    leave(ext, os.path.join(a.bootdir, f"{a.name}.exit"))


if __name__ == "__main__":
    main()
