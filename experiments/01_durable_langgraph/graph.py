"""``research_agent`` graph (plan section 10) and its two node runners.

    START -> plan -> search x width (Send fan-out) -> reduce -> critique
          -> plan (loop again) or finalize -> END

``build(make_node)`` takes a factory that turns a node name into the async
callable LangGraph runs. ``local_nodes`` runs the node functions in this
process (the stock LangGraph baseline); ``Coordinator.remote`` runs them on
HPX workers. Both apply outbox effects through the same keyed ``Outbox``.
"""

import asyncio
import hashlib
from typing import Annotated, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

import agent_nodes
from langx.coordinator import task_identity
from langx.stub_llm import canonical


def merge(left, right):
    """State reducer for ``results``: union of dicts, order-independent."""
    out = dict(left or {})
    out.update(right or {})
    return out


class State(TypedDict, total=False):
    run_id: str
    question: str
    width: int
    latency_ms: int
    max_loops: int
    direct: dict
    loop: int
    queries: list
    results: Annotated[dict, merge]
    summary: str
    verdict: str
    answer: str


NODES = ("plan", "search", "reduce", "critique", "finalize")


def search_key(task):
    """Tells the parallel search tasks of one superstep apart."""
    return f"search:{task['i']}"


KEYS = {"search": search_key}


def fan_out(state):
    """One Send per sub-query."""
    return [Send("search", {"run_id": state["run_id"], "i": i, "query": q,
                            "loop": state["loop"],
                            "latency_ms": state.get("latency_ms", 0),
                            "direct": state.get("direct")})
            for i, q in enumerate(state["queries"])]


def route(state):
    """Loop back to plan, or finish."""
    return "plan" if state["verdict"] == "loop" else "finalize"


def build(make_node, checkpointer=None):
    """Compile the graph; ``make_node(name, key)`` returns each node."""
    g = StateGraph(State)
    for name in NODES:
        g.add_node(name, make_node(name, KEYS.get(name)))
    g.add_edge(START, "plan")
    g.add_conditional_edges("plan", fan_out, ["search"])
    g.add_edge("search", "reduce")
    g.add_edge("reduce", "critique")
    g.add_conditional_edges("critique", route, ["plan", "finalize"])
    g.add_edge("finalize", END)
    return g.compile(checkpointer=checkpointer or InMemorySaver())


def local_nodes(outbox=None):
    """Node factory for the in-process baseline."""
    def make_node(name, key):
        fn = getattr(agent_nodes, name)

        async def node(state, config):
            run_id, step, node_key = task_identity(config, state, key)
            value = await asyncio.to_thread(fn, state)
            if outbox is not None:
                outbox.apply(run_id, step, node_key, "local", -1, -1,
                             value.get("effects", []))
            return value["update"]
        return node
    return make_node


def remote_nodes(coordinator):
    """Node factory that runs every node on HPX workers."""
    def make_node(name, key):
        return coordinator.remote(f"agent_nodes:{name}", key)
    return make_node


def initial_state(run_id, width, loops, latency_ms, direct=None):
    """Input for one run."""
    state = {"run_id": run_id, "question": "how do HPX localities fail?",
             "width": width, "max_loops": loops, "latency_ms": latency_ms,
             "loop": 0, "results": {}}
    if direct:
        state["direct"] = direct
    return state


def state_hash(state):
    """SHA-256 of the canonical JSON of the final state (plan 6.7).

    ``direct`` only says where Tier B effects go, so it is left out: runs
    that write them to different files still compare equal.
    """
    kept = {k: v for k, v in state.items() if k != "direct"}
    return hashlib.sha256(canonical(kept).encode()).hexdigest()


async def run(graph, run_id, state, durability=None):
    """Run one agent to completion; returns the final state.

    ``state`` None resumes ``run_id`` from the graph's checkpointer.
    """
    kw = {"durability": durability} if durability else {}
    return await graph.ainvoke(state, {"configurable": {"thread_id": run_id},
                                       "recursion_limit": 100}, **kw)
