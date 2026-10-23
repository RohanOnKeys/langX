"""Spike D: LangGraph async proxy nodes, no HPX (plan sections 9.2, 9.3, 16).

Questions answered, against the installed LangGraph version:
  D1  Do async proxy nodes in one superstep run concurrently up to width 32,
      with wall time near one node latency?
  D2  With node retry left off, does a branch failure run exactly once?
  D3  After one branch of a superstep fails, does resuming from the
      checkpointer re-run only the failed branch, not completed siblings
      (pending writes, the basis of F7)?

    python p0_spike_d.py --out spike_d.json
"""

import argparse
import asyncio
import importlib.metadata
import json
import operator
import platform
import time
from typing import Annotated, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send


class State(TypedDict):
    width: int
    results: Annotated[list, operator.add]


class Probe:
    """Counts live and total executions per branch; can fail a branch once."""

    def __init__(self, latency_s, fail_branch=None):
        self.latency_s = latency_s
        self.fail_branch = fail_branch
        self.live = 0
        self.max_live = 0
        self.calls = {}

    async def search(self, arg):
        i = arg["i"]
        self.calls[i] = self.calls.get(i, 0) + 1
        self.live += 1
        self.max_live = max(self.max_live, self.live)
        try:
            await asyncio.sleep(self.latency_s)   # stands in for an HPX future
            if i == self.fail_branch and self.calls[i] == 1:
                raise RuntimeError(f"branch {i} fails on first attempt")
            return {"results": [i]}
        finally:
            self.live -= 1


def build(probe, checkpointer=None):
    def fan_out(state):
        return [Send("search", {"i": i}) for i in range(state["width"])]

    def reduce(state):
        return {"results": []}

    g = StateGraph(State)
    g.add_node("search", probe.search)      # no retry_policy: retry off
    g.add_node("reduce", reduce)
    g.add_conditional_edges(START, fan_out, ["search"])
    g.add_edge("search", "reduce")
    g.add_edge("reduce", END)
    return g.compile(checkpointer=checkpointer)


async def d1(widths, latency_s):
    out = []
    for w in widths:
        p = Probe(latency_s)
        t0 = time.monotonic()
        res = await build(p).ainvoke({"width": w, "results": []})
        wall = time.monotonic() - t0
        out.append({"width": w, "latency_ms": latency_s * 1e3,
                    "wall_ms": wall * 1e3, "max_concurrent": p.max_live,
                    "all_results": sorted(res["results"]) == list(range(w)),
                    "concurrent": p.max_live == w})
    return out


async def d2_d3(width, latency_s):
    p = Probe(latency_s, fail_branch=width // 2)
    graph = build(p, InMemorySaver())
    cfg = {"configurable": {"thread_id": "spike-d"}}
    first = {}
    try:
        await graph.ainvoke({"width": width, "results": []}, cfg)
        first["raised"] = False
    except Exception as ex:  # noqa: BLE001
        first["raised"] = True
        first["error"] = f"{type(ex).__name__}: {ex}"
    calls_after_first = dict(p.calls)

    snap = await graph.aget_state(cfg)
    pending = [t.name for t in snap.tasks]
    first["pending_tasks"] = len(pending)
    first["next"] = list(snap.next)

    res = await graph.ainvoke(None, cfg)
    reruns = {i: p.calls[i] - calls_after_first.get(i, 0) for i in p.calls}
    return {
        "width": width, "fail_branch": p.fail_branch,
        "first_attempt": first,
        "d2_failed_branch_calls_in_first_attempt":
            calls_after_first.get(p.fail_branch),
        "d3_reran_branches": sorted(i for i, n in reruns.items() if n > 0),
        "d3_only_failed_branch_reran":
            sorted(i for i, n in reruns.items() if n > 0) == [p.fail_branch],
        "final_results_complete": sorted(res["results"]) == list(range(width)),
        "final_results_duplicates": len(res["results"]) - len(set(res["results"])),
    }


async def main_async(a):
    widths = [int(x) for x in a.widths.split(",")]
    rec = {
        "langgraph": importlib.metadata.version("langgraph"),
        "python": platform.python_version(),
        "D1": await d1(widths, a.latency_ms / 1e3),
        "D2_D3": await d2_d3(max(widths), a.latency_ms / 1e3),
    }
    with open(a.out, "w") as f:
        json.dump(rec, f, indent=1, sort_keys=True)
    print(json.dumps(rec, indent=1, sort_keys=True))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--widths", default="3,8,16,32")
    ap.add_argument("--latency-ms", type=float, default=200)
    ap.add_argument("--out", default="spike_d.json")
    asyncio.run(main_async(ap.parse_args()))


if __name__ == "__main__":
    main()
