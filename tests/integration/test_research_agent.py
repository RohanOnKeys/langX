"""F0 for ``research_agent``: HPX run equals the stock LangGraph baseline.

Runs ``run_f0.py`` in a subprocess, because HPX can be started only once per
process and ``test_island.py`` already starts it in this one. Skipped when
LangGraph is not installed.
"""

import json
import pathlib
import subprocess
import sys

import pytest

pytest.importorskip("langgraph")

REPO = pathlib.Path(__file__).resolve().parents[2]
RUN_F0 = REPO / "experiments" / "01_durable_langgraph" / "run_f0.py"


def test_f0_hpx_run_matches_baseline(tmp_path):
    proc = subprocess.run(
        [sys.executable, str(RUN_F0), "--out", str(tmp_path), "--reps", "2",
         "--width", "3", "--loops", "2", "--latency-ms", "20"],
        capture_output=True, text=True, timeout=600)
    result = tmp_path / "f0.json"
    assert result.exists(), (f"run_f0.py exited {proc.returncode}\n"
                             f"stdout:\n{proc.stdout[-3000:]}\n"
                             f"stderr:\n{proc.stderr[-3000:]}")
    rec = json.loads(result.read_text())
    checks = rec["checks"]
    assert all(checks["hash_equal"]), proc.stdout[-2000:]
    assert checks["outbox_keys_equal"]
    assert checks["no_duplicate_keys"]
    assert checks["one_commit_per_task"]
    assert checks["no_retries_or_rejections"], rec["events"]
    assert proc.returncode == 0, proc.stderr[-2000:]
