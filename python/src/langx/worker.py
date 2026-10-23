"""Worker entry point: serves ``run_node`` calls on one HPX locality.

Responsibilities (plan section 8):
    * connect to the root and initialize supervision
    * resolve node functions by dotted path (``module:function``)
    * run them on the extension's dedicated Python thread, returning node
      errors as values
    * finalize with a bounded wait and the post-finalize pause, then
      disconnect

Request and reply payloads are JSON. A request is
``{"attempt_id": str, "input": any}``. The reply always carries the attempt
id and this worker's locality and epoch, so the coordinator's commit fence
can check where a result came from::

    {"ok": true, "value": ..., "attempt_id": ..., "worker": {...}}
    {"ok": false, "error_kind": ..., "error": ..., "attempt_id": ..., ...}

Run with ``python -m langx.worker --agas HOST:PORT --hpx HOST:PORT``.
"""

import argparse
import asyncio
import importlib
import inspect
import json
import os
import signal
import sys
import time

from langx.runtime import Runtime


def resolve(path):
    """Import ``module:function`` and return the function."""
    module_name, sep, attr = path.partition(":")
    if not sep or not module_name or not attr:
        raise ValueError(f"node path must be module:function, got {path!r}")
    return getattr(importlib.import_module(module_name), attr)


class NodeHost:
    """Turns run_node calls into node function calls and JSON replies."""

    def __init__(self, worker_info):
        self.worker_info = worker_info
        self._cache = {}

    def function(self, path):
        """Resolve ``path`` once and cache it."""
        if path not in self._cache:
            self._cache[path] = resolve(path)
        return self._cache[path]

    def preload(self, paths):
        """Resolve every path up front, so bad paths fail at startup."""
        for p in paths:
            self.function(p)

    def __call__(self, node, payload):
        """Handle one run_node call. Never raises."""
        attempt_id = None
        try:
            request = json.loads(payload) if payload else {}
            attempt_id = request.get("attempt_id")
        except ValueError as ex:
            return self._error(attempt_id, "bad_request", ex)
        try:
            fn = self.function(node)
        except Exception as ex:  # noqa: BLE001  node errors are values
            return self._error(attempt_id, "resolve_error", ex)
        try:
            value = fn(request.get("input"))
            if inspect.isawaitable(value):
                value = asyncio.run(_await(value))
            reply = {"ok": True, "value": value, "attempt_id": attempt_id,
                     "worker": self.worker_info}
            return json.dumps(reply)
        except Exception as ex:  # noqa: BLE001  node errors are values
            return self._error(attempt_id, "node_exception", ex)

    def _error(self, attempt_id, kind, ex):
        return json.dumps({"ok": False, "error_kind": kind,
                           "error": f"{type(ex).__name__}: {ex}",
                           "attempt_id": attempt_id,
                           "worker": self.worker_info})


async def _await(awaitable):
    return await awaitable


def _append_jsonl(path, obj):
    if not path:
        return
    with open(path, "a") as f:
        f.write(json.dumps(obj) + "\n")


def _write_json(path, obj):
    if not path:
        return
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def main(argv=None):
    """Serve run_node until SIGTERM, SIGINT or the stop file."""
    ap = argparse.ArgumentParser(prog="python -m langx.worker")
    ap.add_argument("--agas", required=True, help="root HOST:PORT")
    ap.add_argument("--hpx", required=True, help="own HOST:PORT")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--poll-timeout-ms", type=int, default=0,
                    help="failure-detection poll timeout; 0 keeps the HPX default")
    ap.add_argument("--discovery-timeout-ms", type=int, default=0,
                    help="discovery pass inside supervision init. 0 joins no "
                         "peers: on HPX cc2be553 a peer that joined this worker "
                         "and then died gets this worker marked failed")
    ap.add_argument("--preload", nargs="*", default=[],
                    help="module:function paths to resolve at startup")
    ap.add_argument("--ready-file", default="")
    ap.add_argument("--stop-file", default="")
    ap.add_argument("--finalize-pause-ms", type=int, default=-1,
                    help="pause after finalize; -1 means twice the poll timeout")
    ap.add_argument("--state-log", default="",
                    help="append this worker's own supervision state to this "
                         "JSON lines file whenever it changes")
    a = ap.parse_args(argv)

    stop = {"requested": False}

    def on_signal(signum, frame):  # noqa: ARG001
        stop["requested"] = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    discovery_s = a.discovery_timeout_ms / 1000.0
    poll_s = a.poll_timeout_ms / 1000.0 if a.poll_timeout_ms else None
    pause = None if a.finalize_pause_ms < 0 else a.finalize_pause_ms / 1000.0

    rt = Runtime()
    rt.start(a.agas, a.hpx, a.threads)
    me = rt.init(discovery_s, poll_s)
    info = {"locality": me.locality, "epoch": me.epoch, "pid": os.getpid()}
    host = NodeHost(info)
    host.preload(a.preload)
    rt.ext.set_node_handler(host)
    _write_json(a.ready_file, dict(
        info, joined_at_init=[p.locality for p in rt.snapshot_peers()]))

    # A worker whose own state turned failed cannot init supervision again
    # (HPX cc2be553: invalid lifecycle event transition), so recovery from
    # coordinator loss is a worker restart (plan 9.3), not an in-process
    # re-init. The state log records when this worker was fenced.
    last = None
    while not stop["requested"] and not (a.stop_file
                                         and os.path.exists(a.stop_file)):
        time.sleep(0.1)
        if not a.state_log:
            continue
        state = rt.ext.own_state()
        key = (state["last_event"], state["epoch"])
        if key != last:
            last = key
            _append_jsonl(a.state_log, {"t_ns": time.monotonic_ns(),
                                        "event": key[0], "epoch": key[1]})
    t0 = time.monotonic()
    finalized, rc = rt.shutdown(pause_s=pause)
    if a.ready_file:
        _write_json(a.ready_file + ".exit",
                    {"exit_path": "clean" if finalized else "finalize_timeout",
                     "stop_rc": rc, "shutdown_ms": (time.monotonic() - t0) * 1e3})
    return 0 if finalized and rc == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
