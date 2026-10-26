"""Runtime asyncio bridge and error mapping, against a fake extension.

No HPX: ``FakeExt`` stands in for ``_langx``, with a real pipe for the
completion queue so the asyncio reader path is the one used in production.
"""

import asyncio
import json
import os
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "python" / "src"))

from langx.runtime import (DispatchFailed, NodeResult, Peer, Runtime,  # noqa: E402
                           TargetFenced)


class FakeExt:
    """Minimal ``_langx`` stand-in. Completions are posted by the test."""

    def __init__(self):
        self.r, self.w = os.pipe()
        os.set_blocking(self.r, False)
        self.queue = []
        self.sent = []
        self.fenced = set()
        self.joined = []

    def completion_fd(self):
        return self.r

    def drain_completions(self):
        try:
            while os.read(self.r, 256):
                pass
        except BlockingIOError:
            pass
        out, self.queue = self.queue, []
        return out

    def post(self, request_id, status, result=b"", error="", code=0):
        self.queue.append({"id": request_id, "status": status, "result": result,
                           "error": error, "error_code": code,
                           "t0_ns": 1, "t1_ns": 2})
        os.write(self.w, b"x")

    def dispatch_node(self, request_id, locality, epoch, node, payload):
        self.sent.append((request_id, locality, epoch, node, payload))

    def request_force_disconnect(self, request_id, locality):
        self.sent.append((request_id, "force_disconnect", locality))

    def admitted(self, locality, epoch):
        return (locality, epoch) not in self.fenced

    def discover_and_join(self, timeout_ms):
        return self.joined.pop(0) if self.joined else []


PEER = Peer(locality=2, epoch=1)


def run(coro):
    return asyncio.run(coro)


def test_dispatch_ok_returns_node_result():
    ext = FakeExt()
    rt = Runtime(ext)

    async def go():
        task = asyncio.ensure_future(rt.dispatch_node(PEER, "m:f", "{}"))
        await asyncio.sleep(0.01)
        request_id, locality, epoch, node, payload = ext.sent[0]
        assert (locality, epoch, node, payload) == (2, 1, "m:f", "{}")
        ext.post(request_id, "ok", b'{"ok": true}')
        return await task

    res = run(go())
    assert isinstance(res, NodeResult)
    assert res.json() == {"ok": True}
    assert res.peer == PEER


@pytest.mark.parametrize("status,exc", [("fenced", TargetFenced),
                                        ("failed", DispatchFailed)])
def test_dispatch_errors_are_mapped(status, exc):
    ext = FakeExt()
    rt = Runtime(ext)

    async def go():
        task = asyncio.ensure_future(rt.dispatch_node(PEER, "m:f", "{}"))
        await asyncio.sleep(0.01)
        ext.post(ext.sent[0][0], status, error="HPX(x)", code=7)
        return await task

    with pytest.raises(exc) as info:
        run(go())
    if exc is TargetFenced:
        assert info.value.abandoned is False
    else:
        assert info.value.error_code == 7


def test_in_flight_dispatch_is_abandoned_when_target_is_fenced():
    ext = FakeExt()
    rt = Runtime(ext)

    async def go():
        task = asyncio.ensure_future(rt.dispatch_node(PEER, "m:f", "{}"))
        await asyncio.sleep(0.02)
        ext.fenced.add((PEER.locality, PEER.epoch))
        return await asyncio.wait_for(task, 1.0)

    with pytest.raises(TargetFenced) as info:
        run(go())
    assert info.value.abandoned is True


def test_late_completion_for_abandoned_request_is_dropped():
    ext = FakeExt()
    rt = Runtime(ext)

    async def go():
        with pytest.raises(TimeoutError):
            await rt.dispatch_node(PEER, "m:f", "{}", timeout_s=0.02,
                                   abandon_on_fence=False)
        ext.post(ext.sent[0][0], "ok", b'{"attempt_id": "r/1/plan/1"}')
        await asyncio.sleep(0.02)   # _drain runs; nothing is waiting
        return rt._pending

    assert run(go()) == {}
    assert [(x["status"], x["attempt_id"]) for x in rt.late] == [
        ("ok", "r/1/plan/1")]


def test_force_disconnect_reports_root_errors():
    ext = FakeExt()
    rt = Runtime(ext)

    async def go(report):
        task = asyncio.ensure_future(rt.force_disconnect(3))
        await asyncio.sleep(0.01)
        ext.post(ext.sent[-1][0], "ok", json.dumps(report).encode())
        return await task

    ok = {"rc": 0, "error": "", "elapsed_ms": 0, "membership_after": 4}
    assert run(go(ok)) == ok
    with pytest.raises(DispatchFailed):
        run(go({"rc": -1, "error": "invalid_status", "elapsed_ms": 0,
                "membership_after": 5}))


def test_wait_for_peers_repeats_discovery_until_all_joined():
    ext = FakeExt()
    ext.joined = [[(2, 1)], [], [(2, 1), (3, 1)]]
    rt = Runtime(ext)
    peers = rt.wait_for_peers([2, 3], timeout_s=5, pass_timeout_s=0)
    assert peers == {2: Peer(2, 1), 3: Peer(3, 1)}


def test_finalize_needs_a_pause_with_the_default_poll_timeout():
    rt = Runtime(FakeExt())
    with pytest.raises(ValueError):
        rt.finalize()


def test_wait_for_peers_times_out():
    rt = Runtime(FakeExt())
    with pytest.raises(TimeoutError):
        rt.wait_for_peers([9], timeout_s=0.05, pass_timeout_s=0)
