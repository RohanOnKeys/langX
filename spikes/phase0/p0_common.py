"""Shared helpers for the phase 0 spike processes (files, ports, HPX args)."""

import json
import os
import socket
import sys
import time

HPX_HOST = "localhost"  # RayX exp66/67: connect mode maps localhost to 127.0.0.1


def import_ext(build_dir):
    """Import the _p0 extension from the spike build directory."""
    sys.path.insert(0, os.path.abspath(build_dir))
    import _p0  # noqa: PLC0415

    return _p0


def now_ns():
    """CLOCK_MONOTONIC in ns; same clock as std::chrono::steady_clock on Linux."""
    return time.monotonic_ns()


def write_json(path, obj):
    """Write JSON atomically so readers never see a partial file."""
    tmp = f"{path}.tmp{os.getpid()}"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=1, sort_keys=True)
    os.replace(tmp, path)


def append_jsonl(path, obj):
    """Append one JSON record as a line."""
    with open(path, "a") as f:
        f.write(json.dumps(obj, sort_keys=True) + "\n")


def read_json(path, default=None):
    """Read a JSON file, returning default if it is missing or partial."""
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def wait_for_file(path, timeout_s, poll_s=0.05):
    """Return True once path exists, False after timeout_s."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if os.path.exists(path):
            return True
        time.sleep(poll_s)
    return os.path.exists(path)


def find_free_port():
    """Pick a free TCP port on the loopback interface."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def leave(ext, exit_path, timeout_s=10.0):
    """Disconnect this connector from HPX, then exit the process.

    Runs supervision::finalize, then hpx::disconnect. Records the exit path
    in exit_path: clean, or timeout_in_<step> written by a watchdog naming
    the step that hung. Never returns.
    """
    t0 = now_ns()
    state = {"step": "supervision_finalize", "finalize": None}

    mode = os.environ.get("P0_DISCONNECT_MODE", "post")
    # Pause between supervision::finalize and hpx::disconnect, to let
    # await_terminal calls left pending by the failure-detection loop
    # resolve while the runtime is still up.
    wait_ms = float(os.environ.get("P0_POST_FINALIZE_WAIT_MS", "0"))

    def mark(**kw):
        # t_ns lets the harness order connector and root exits.
        write_json(exit_path, dict(kw, t_ns=now_ns(), start_ns=t0, mode=mode,
                                   post_finalize_wait_ms=wait_ms,
                                   finalize=state["finalize"]))

    def watchdog():
        time.sleep(timeout_s)
        mark(exit_path=f"timeout_in_{state['step']}",
             waited_ms=(now_ns() - t0) / 1e6)
        os._exit(88)

    import threading  # noqa: PLC0415

    threading.Thread(target=watchdog, daemon=True).start()
    mark(exit_path="in_supervision_finalize")
    try:
        state["finalize"] = dict(ext.supervision_finalize())
        if wait_ms > 0:
            state["step"] = "post_finalize_wait"
            time.sleep(wait_ms / 1000.0)
        state["step"] = "disconnect"
        mark(exit_path="in_disconnect")
        rc = ext.stop_disconnect()
        mark(exit_path="clean", rc=rc, took_ms=(now_ns() - t0) / 1e6)
    except Exception as ex:  # noqa: BLE001
        mark(exit_path="disconnect_error", error=f"{type(ex).__name__}: {ex}")
    os._exit(0)


def connector_args(root_port, own_port):
    """HPX command-line arguments for a late-connecting locality."""
    return [
        f"--hpx:agas={HPX_HOST}:{root_port}",
        f"--hpx:hpx={HPX_HOST}:{own_port}",
        "--hpx:bind=none",
        "--hpx:ignore-batch-env",
    ]
