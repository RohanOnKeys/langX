"""Launch a test island: root, workers, all on this host in connect mode.

The test process itself is the coordinator. Root and workers write ready
files with their PIDs and epochs, and ``<ready>.exit`` files on shutdown.
"""

import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import time

REPO = pathlib.Path(__file__).resolve().parents[2]
HOST = "localhost"


def free_port():
    """Pick a free TCP port on the loopback interface."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_json(path, timeout_s):
    """Return the JSON in ``path`` once it exists, or None after timeout."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with open(path) as f:
                return json.load(f)
        except (OSError, ValueError):
            time.sleep(0.05)
    return None


class Island:
    """Root plus ``n_workers`` workers, started one at a time."""

    def __init__(self, workdir, n_workers=2, poll_timeout_ms=500, threads=4,
                 worker_discovery_ms=0, worker_args=()):
        self.dir = pathlib.Path(workdir)
        self.n_workers = n_workers
        self.worker_discovery_ms = worker_discovery_ms
        # Extra worker flags; "{dir}" and "{name}" are filled in per worker.
        self.worker_args = list(worker_args)
        self.poll_ms = poll_timeout_ms
        self.threads = threads
        self.root_port = free_port()
        self.agas = f"{HOST}:{self.root_port}"
        self.procs = {}
        self.ready = {}

    def _popen(self, name, cmd, env=None):
        log = open(self.dir / f"{name}.log", "w")
        self.procs[name] = subprocess.Popen(
            cmd, stdout=log, stderr=subprocess.STDOUT, env=env,
            start_new_session=True)

    def start(self):
        root_bin = os.environ.get("LANGX_ROOT_BIN",
                                  str(REPO / "python" / "build" / "langx_root"))
        self._popen("root", [
            root_bin, f"--hpx:agas={self.agas}", f"--hpx:hpx={self.agas}",
            "--hpx:expect-connecting-localities", "--hpx:threads=2",
            "--hpx:bind=none", "--hpx:ignore-batch-env",
            "--langx-ready-file", str(self.dir / "root.ready"),
            "--langx-stop-file", str(self.dir / "root.stop"),
            "--langx-poll-timeout-ms", str(self.poll_ms)])
        self.ready["root"] = wait_json(self.dir / "root.ready", 30)
        if self.ready["root"] is None:
            raise RuntimeError("root did not start")

        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(
            [str(REPO / "python" / "src"), str(REPO / "tests" / "integration"),
             env.get("PYTHONPATH", "")])
        for i in range(1, self.n_workers + 1):
            name = f"w{i}"
            self._popen(name, [
                sys.executable, "-m", "langx.worker", "--agas", self.agas,
                "--hpx", f"{HOST}:{free_port()}", "--threads", str(self.threads),
                "--poll-timeout-ms", str(self.poll_ms),
                "--discovery-timeout-ms", str(self.worker_discovery_ms),
                "--preload", "nodes:echo", "nodes:sleep_ms", "nodes:fail",
                "--ready-file", str(self.dir / f"{name}.ready"),
                "--stop-file", str(self.dir / f"{name}.stop"),
                *(x.format(dir=self.dir, name=name) for x in self.worker_args)],
                env=env)
            self.ready[name] = wait_json(self.dir / f"{name}.ready", 60)
            if self.ready[name] is None:
                raise RuntimeError(f"{name} did not start")
        return self

    def workers(self):
        """Worker name -> ready info (locality, epoch, pid)."""
        return {k: v for k, v in self.ready.items() if k != "root"}

    def kill(self, name):
        """SIGKILL one process."""
        os.kill(self.procs[name].pid, signal.SIGKILL)
        self.procs[name].wait(5)

    def stop_workers(self, timeout_s=20):
        """Ask live workers to leave; return their exit reports."""
        live = [n for n in self.workers() if self.procs[n].poll() is None]
        for n in live:
            (self.dir / f"{n}.stop").touch()
        return {n: wait_json(self.dir / f"{n}.ready.exit", timeout_s) for n in live}

    def stop_root(self, timeout_s=20):
        """Ask the root to leave; return its exit report."""
        (self.dir / "root.stop").touch()
        return wait_json(self.dir / "root.ready.exit", timeout_s)

    def cleanup(self):
        """Kill anything still running."""
        for p in self.procs.values():
            if p.poll() is None:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
