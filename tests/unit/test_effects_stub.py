"""Effect log, outbox, direct sink and stub LLM. No HPX, no LangGraph."""

import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "python" / "src"))

from langx.effects import DirectSink, EffectLog, Outbox, effect_key  # noqa: E402
from langx.stub_llm import StubLLM, StubMiss, canonical, prompt_key  # noqa: E402

INTENTS = [{"kind": "publish", "payload": {"a": 1}},
           {"kind": "notify", "payload": {"b": 2}}]


def test_effect_key_is_stable_and_distinct():
    assert effect_key("r", 1, "n", 0) == effect_key("r", 1, "n", 0)
    assert effect_key("r", 1, "n", 0) != effect_key("r", 1, "n", 1)
    assert effect_key("r", 1, "n", 0) != effect_key("r", 2, "n", 0)


def test_outbox_applies_each_intent_once(tmp_path):
    log = EffectLog(tmp_path / "e.sqlite")
    box = Outbox(log)
    assert box.apply("r", 3, "finalize", "a1", 2, 1, INTENTS) == 2
    assert box.apply("r", 3, "finalize", "a2", 3, 1, INTENTS) == 0   # retried task
    assert len(log.rows()) == 2
    assert log.duplicates() == 0


@pytest.mark.parametrize("mode,rows,dups", [("naive", 2, 1), ("keyed", 1, 0)])
def test_direct_sink_modes(tmp_path, mode, rows, dups):
    sink = DirectSink(tmp_path / "d.sqlite", mode)
    for attempt in ("a1", "a2"):
        sink.emit("k", "r", 1, "search:0", attempt, 2, 1, "hit", {})
    assert len(sink.log.rows()) == rows
    assert sink.log.duplicates() == dups


def test_direct_sink_rejects_unknown_mode(tmp_path):
    with pytest.raises(ValueError):
        DirectSink(tmp_path / "d.sqlite", "sometimes")


def test_stub_replays_and_misses():
    prompt = canonical({"kind": "plan", "loop": 0})
    stub = StubLLM({prompt_key(prompt): "answer"})
    assert stub.complete(prompt) == "answer"
    with pytest.raises(StubMiss):
        stub.complete(canonical({"kind": "plan", "loop": 1}))


def test_stub_records_and_saves(tmp_path):
    stub = StubLLM(generator=lambda p: p.upper())
    assert stub.complete("hi") == "HI"
    stub.save(tmp_path / "f.json")
    assert StubLLM.from_file(tmp_path / "f.json").complete("hi") == "HI"


def test_canonical_ignores_key_order():
    assert canonical({"b": 1, "a": 2}) == canonical({"a": 2, "b": 1})
