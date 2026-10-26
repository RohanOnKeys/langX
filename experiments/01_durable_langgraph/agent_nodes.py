"""Node functions of ``research_agent`` (plan section 10).

Every node takes JSON-able input and returns ``{"update": ..., "effects":
[...]}``: the update goes to LangGraph's state, the effects are outbox
intents applied by the coordinator after the commit fence. The same functions
run in-process for the baseline and on HPX workers, so their results can be
compared by state hash.

The model is the replaying stub LLM; the search "tool" is a deterministic
hash with a simulated latency. No LangGraph import, so workers load this
module cheaply.
"""

import hashlib
import os
import pathlib
import time

from langx.effects import DirectSink, effect_key
from langx.stub_llm import StubLLM, canonical

FIXTURES = pathlib.Path(
    os.environ.get("LANGX_FIXTURES",
                   pathlib.Path(__file__).resolve().parent / "fixtures"
                   / "research_agent.json"))

_llm = None


def llm():
    """The stub LLM, loaded from the fixture file on first use."""
    global _llm
    if _llm is None:
        _llm = StubLLM.from_file(FIXTURES)
    return _llm


def use_llm(stub):
    """Swap in another stub, for example the recorder in make_fixtures."""
    global _llm
    _llm = stub


def digest(*parts):
    """Short deterministic text from its parts."""
    return hashlib.sha256(canonical(parts).encode()).hexdigest()[:16]


def plan(state):
    """Ask the model for ``width`` sub-queries for this loop."""
    prompt = canonical({"kind": "plan", "question": state["question"],
                        "width": state["width"], "loop": state.get("loop", 0),
                        "summary": state.get("summary", "")})
    queries = llm().complete(prompt).split("\n")
    return {"update": {"queries": queries, "loop": state.get("loop", 0)},
            "effects": []}


def search(task):
    """One fan-out branch: simulated tool latency, then a result.

    The result is also returned as one outbox intent (Tier A), so a retried
    search shows whether its effect is applied exactly once.

    With ``task["direct"]`` set to ``{"path", "mode"}`` it also writes one
    Tier B effect itself, then waits ``direct["post_ms"]`` (default 0) before
    returning, which leaves a window to kill the worker after the effect and
    before the reply (F3).
    """
    time.sleep(task.get("latency_ms", 0) / 1000.0)
    result = digest("search", task["query"])
    direct = task.get("direct")
    if direct:
        sink = DirectSink(direct["path"], direct["mode"])
        sink.emit(effect_key(task["run_id"], task["loop"], "search", task["i"]),
                  task["run_id"], task["loop"], f"search:{task['i']}", "",
                  -1, -1, "record_hit", {"query": task["query"]})
        sink.log.close()
        time.sleep(direct.get("post_ms", 0) / 1000.0)
    return {"update": {"results": {f"{task['loop']}:{task['i']}": result}},
            "effects": [{"kind": "record_hit",
                         "payload": {"query": task["query"], "result": result}}]}


def reduce(state):
    """Deterministic merge of this loop's results into a summary."""
    loop = state.get("loop", 0)
    mine = sorted((int(k.split(":")[1]), v) for k, v in state["results"].items()
                  if k.split(":")[0] == str(loop))
    return {"update": {"summary": digest("reduce", loop, mine)}, "effects": []}


def critique(state):
    """Ask the model whether to loop again; advance the loop counter."""
    prompt = canonical({"kind": "critique", "loop": state.get("loop", 0),
                        "max_loops": state["max_loops"],
                        "summary": state["summary"]})
    verdict = llm().complete(prompt)
    return {"update": {"verdict": verdict, "loop": state.get("loop", 0) + 1},
            "effects": []}


def finalize(state):
    """Final answer, plus one outbox effect that publishes it."""
    prompt = canonical({"kind": "finalize", "question": state["question"],
                        "summary": state["summary"]})
    answer = llm().complete(prompt)
    return {"update": {"answer": answer},
            "effects": [{"kind": "publish", "payload": {"answer": answer}}]}
