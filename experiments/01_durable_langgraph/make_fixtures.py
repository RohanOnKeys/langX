"""Record the stub LLM fixtures for ``research_agent``.

There is no real model: each prompt is answered by a deterministic
generator, run once over every parameter point of plan section 10, and the
answers are saved by prompt hash. Runs then replay the file, and a prompt
that was not recorded is an error.

    python experiments/01_durable_langgraph/make_fixtures.py
"""

import asyncio
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1] / "python" / "src")]

import agent_nodes  # noqa: E402
import graph  # noqa: E402
from langx.stub_llm import StubLLM  # noqa: E402

WIDTHS = (3, 8, 16, 32)
LOOPS = (1, 2, 3)


def answer(prompt):
    """The deterministic 'model'."""
    p = json.loads(prompt)
    if p["kind"] == "plan":
        return "\n".join(f"subquery {p['loop']}.{i} {agent_nodes.digest(p, i)}"
                         for i in range(p["width"]))
    if p["kind"] == "critique":
        return "loop" if p["loop"] + 1 < p["max_loops"] else "done"
    if p["kind"] == "finalize":
        return f"answer {agent_nodes.digest(p)}"
    raise ValueError(f"unknown prompt kind {p['kind']}")


async def main():
    recorder = StubLLM(generator=answer)
    agent_nodes.use_llm(recorder)
    g = graph.build(graph.local_nodes())
    for w in WIDTHS:
        for loops in LOOPS:
            run_id = f"fixture-w{w}-l{loops}"
            await graph.run(g, run_id, graph.initial_state(run_id, w, loops, 0))
    agent_nodes.FIXTURES.parent.mkdir(exist_ok=True)
    recorder.save(agent_nodes.FIXTURES)
    print(f"{len(recorder.responses)} responses -> {agent_nodes.FIXTURES}")


if __name__ == "__main__":
    asyncio.run(main())
