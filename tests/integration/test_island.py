"""Phase 1 exit check and phase 0 design changes on a live island.

Root, two workers and this test process as coordinator. Tests run in file
order on one island: the kill test removes w2, and the last test shuts the
island down and checks every exit path. Poll timeout 500 ms (testing hook).
"""

import asyncio
import json

import pytest

from island import Island, free_port

from langx.runtime import Runtime, TargetFenced

POLL_S = 0.5


@pytest.fixture(scope="module")
def island(tmp_path_factory):
    isl = Island(tmp_path_factory.mktemp("island"), n_workers=2,
                 poll_timeout_ms=int(POLL_S * 1000)).start()
    yield isl
    isl.cleanup()


@pytest.fixture(scope="module")
def rt(island):
    r = Runtime()
    r.start(island.agas, f"localhost:{free_port()}")
    r.init(discovery_timeout_s=5.0, poll_timeout_s=POLL_S)
    return r


@pytest.fixture(scope="module")
def peers(island, rt):
    localities = [w["locality"] for w in island.workers().values()]
    joined = rt.wait_for_peers(localities, timeout_s=60)
    return {name: joined[w["locality"]] for name, w in island.workers().items()}


def request(value, attempt_id="a"):
    return json.dumps({"attempt_id": attempt_id, "input": value})


def test_joins_workers_with_their_epochs(island, peers):
    for name, info in island.workers().items():
        assert peers[name].epoch == info["epoch"], name


def test_dispatch_runs_on_the_target(island, rt, peers):
    async def go():
        return {name: (await rt.dispatch_node(p, "nodes:echo",
                                              request(name, f"att-{name}"))).json()
                for name, p in peers.items()}

    for name, reply in asyncio.run(go()).items():
        info = island.workers()[name]
        assert reply["ok"] is True
        assert reply["value"] == {"value": name, "pid": info["pid"]}
        assert reply["attempt_id"] == f"att-{name}"
        assert reply["worker"]["locality"] == info["locality"]
        assert reply["worker"]["epoch"] == info["epoch"]


def test_node_error_is_a_value(rt, peers):
    reply = asyncio.run(rt.dispatch_node(peers["w1"], "nodes:fail",
                                         request(None))).json()
    assert reply["ok"] is False
    assert reply["error_kind"] == "node_exception"


def test_dispatches_run_concurrently_across_workers(rt, peers):
    async def go():
        ps = list(peers.values())
        t0 = asyncio.get_running_loop().time()
        await asyncio.gather(*(rt.dispatch_node(ps[i % 2], "nodes:sleep_ms",
                                                request(200)) for i in range(4)))
        return asyncio.get_running_loop().time() - t0

    # 4 nodes of 200 ms over 2 workers, one node at a time per worker.
    assert asyncio.run(go()) < 0.6


def test_killed_worker_in_flight_dispatch_is_abandoned(island, rt, peers):
    async def go():
        call = asyncio.ensure_future(
            rt.dispatch_node(peers["w2"], "nodes:sleep_ms", request(10_000)))
        await asyncio.sleep(0.3)
        t_kill = asyncio.get_running_loop().time()
        island.kill("w2")
        with pytest.raises(TargetFenced) as info:
            await asyncio.wait_for(call, 30)
        return info.value, asyncio.get_running_loop().time() - t_kill

    err, waited = asyncio.run(go())
    assert err.abandoned is True
    assert waited < 10 * POLL_S
    assert rt.admitted(peers["w2"]) is False


def test_dispatch_to_fenced_worker_is_refused(rt, peers):
    with pytest.raises(TargetFenced) as info:
        asyncio.run(rt.dispatch_node(peers["w2"], "nodes:echo", request(1)))
    assert info.value.abandoned is False


def test_root_force_disconnects_killed_worker(island, rt, peers):
    w2 = peers["w2"].locality
    report = asyncio.run(rt.force_disconnect(w2))
    assert report["rc"] == 0
    assert w2 not in rt.membership()
    reply = asyncio.run(rt.dispatch_node(peers["w1"], "nodes:echo",
                                         request("still here"))).json()
    assert reply["ok"] is True


def test_island_shuts_down_cleanly(island, rt):
    workers = island.stop_workers()
    assert all(r and r["exit_path"] == "clean" and r["stop_rc"] == 0
               for r in workers.values()), workers
    finalized, rc = rt.shutdown()
    assert finalized and rc == 0
    root = island.stop_root()
    assert root and root["exit_path"] == "clean", root
