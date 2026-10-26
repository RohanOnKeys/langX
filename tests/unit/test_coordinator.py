"""Coordinator: worker selection, retry and the commit fence.

A fake runtime answers each dispatch from a script, so every path through
``Coordinator.execute`` is exercised without HPX.
"""

import asyncio
import json
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "python" / "src"))

from langx.coordinator import (Coordinator, EpochTable, NodeError,  # noqa: E402
                               RetriesExhausted, task_identity)
from langx.effects import EffectLog, Outbox  # noqa: E402
from langx.runtime import DispatchFailed, NodeResult, Peer, TargetFenced  # noqa: E402

W1, W2 = Peer(1, 1), Peer(2, 1)


class FakeRuntime:
    """Answers dispatches from ``script``: a list of callables or exceptions."""

    def __init__(self, script, fenced=()):
        self.script = list(script)
        self.calls = []
        self.timeouts = []
        self.fenced = set(fenced)

    async def dispatch_node(self, peer, node, payload, timeout_s=None):
        self.calls.append((peer.locality, json.loads(payload)["attempt_id"]))
        self.timeouts.append(timeout_s)
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step(peer, json.loads(payload))

    def admitted(self, peer):
        return peer.locality not in self.fenced


def ok(value=None, epoch=None, attempt=None):
    """A worker reply; overrides fake a stale or mismatched producer."""
    def reply(peer, req):
        body = {"ok": True, "attempt_id": attempt or req["attempt_id"],
                "value": value or {"update": {"x": 1}, "effects": []},
                "worker": {"locality": peer.locality,
                           "epoch": peer.epoch if epoch is None else epoch}}
        return NodeResult(peer, json.dumps(body).encode(), 0, 0)
    return reply


def node_error(peer, req):
    body = {"ok": False, "error_kind": "node_exception", "error": "boom",
            "attempt_id": req["attempt_id"],
            "worker": {"locality": peer.locality, "epoch": peer.epoch}}
    return NodeResult(peer, json.dumps(body).encode(), 0, 0)


def coordinator(script, fenced=(), outbox=None, **kw):
    c = Coordinator(FakeRuntime(script, fenced), outbox=outbox, **kw)
    c.add_workers([W1, W2])
    return c


def run(c):
    return asyncio.run(c.execute("r", 1, "plan", "m:plan", {"in": 1}))


def events(c, kind):
    return [e for e in c.events if e["event"] == kind]


def test_first_attempt_commits():
    c = coordinator([ok()])
    assert run(c) == {"update": {"x": 1}, "effects": []}
    assert len(events(c, "committed")) == 1


@pytest.mark.parametrize("error", [TargetFenced("x"), TargetFenced("x", abandoned=True),
                                   DispatchFailed("x")])
def test_infrastructure_failure_fences_and_retries_elsewhere(error):
    c = coordinator([error, ok()])
    run(c)
    first, second = c.rt.calls
    assert first[0] != second[0]
    assert c.epochs.fenced() and first[0] in c.epochs.fenced()
    assert first[1].endswith("/1") and second[1].endswith("/2")


def test_timeout_retries_without_fencing():
    c = coordinator([TimeoutError(), ok()])
    run(c)
    assert c.epochs.fenced() == {}
    assert len(events(c, "retry")) == 1


@pytest.mark.parametrize("reply,reason", [(ok(epoch=7), "producer_mismatch"),
                                          (ok(attempt="other"), "attempt_mismatch")])
def test_commit_fence_rejects_mismatched_results(reply, reason):
    c = coordinator([reply, ok()])
    run(c)
    assert events(c, "rejected")[0]["detail"] == reason
    assert len(events(c, "committed")) == 1


def test_commit_fence_rejects_worker_fenced_at_commit():
    c = coordinator([ok(), ok()], fenced=[1])
    run(c)
    assert events(c, "rejected")[0]["detail"] == "fenced_at_commit"
    assert 1 in c.epochs.fenced()
    assert events(c, "committed")[0]["locality"] == 2


def test_already_accepted_task_is_not_dispatched_again():
    c = coordinator([ok()])
    run(c)
    assert run(c) == {"update": {"x": 1}, "effects": []}
    assert len(c.rt.calls) == 1


def test_node_errors_are_raised_not_retried():
    c = coordinator([node_error])
    with pytest.raises(NodeError):
        run(c)
    assert len(c.rt.calls) == 1


def test_retries_are_bounded():
    c = coordinator([TimeoutError()] * 3, max_attempts=3)
    with pytest.raises(RetriesExhausted):
        run(c)


def test_no_live_workers():
    c = coordinator([TargetFenced("x"), TargetFenced("x")])
    with pytest.raises(RetriesExhausted):
        run(c)


def test_outbox_applied_only_on_commit(tmp_path):
    log = EffectLog(tmp_path / "e.sqlite")
    value = {"update": {}, "effects": [{"kind": "publish", "payload": {}}]}
    c = coordinator([ok(value, epoch=7), ok(value)], outbox=Outbox(log))
    run(c)
    rows = log.rows()
    assert len(rows) == 1
    assert rows[0]["attempt_id"].endswith("/2")


def test_on_dispatch_hook_sees_every_attempt_and_can_set_its_timeout():
    seen = []

    def hook(key, attempt, peer):
        seen.append((key, attempt, peer.locality))
        return 0.5 if attempt == 1 else None

    c = coordinator([TimeoutError(), ok()], dispatch_timeout_s=9.0,
                    on_dispatch=hook)
    run(c)
    assert seen == [(("r", 1, "plan"), 1, 1), (("r", 1, "plan"), 2, 2)]
    assert c.rt.timeouts == [0.5, 9.0]
    assert c.epochs.fenced() == {}


def test_events_are_timestamped_in_order():
    c = coordinator([TimeoutError(), ok()])
    run(c)
    assert [e["event"] for e in c.events] == ["dispatched", "retry",
                                              "dispatched", "committed"]
    ts = [e["t"] for e in c.events]
    assert ts == sorted(ts)


def test_epoch_table_never_unfences():
    t = EpochTable()
    t.add(W1)
    t.fence(1, "x")
    t.add(W1)
    assert not t.is_live(1, 1)
    assert not t.is_live(2, 1)


def test_task_identity_from_langgraph_config():
    config = {"configurable": {"thread_id": "t"},
              "metadata": {"langgraph_step": 4, "langgraph_node": "search"}}
    assert task_identity(config, {"i": 3}) == ("t", 4, "search")
    assert task_identity(config, {"i": 3}, key=lambda s: f"search:{s['i']}") == (
        "t", 4, "search:3")
