"""NodeHost: dotted-path resolution and node errors returned as values."""

import json
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "python" / "src"))

from langx.worker import NodeHost, resolve  # noqa: E402

INFO = {"locality": 2, "epoch": 1, "pid": 123}


def double(x):
    return 2 * x


def boom(_):
    raise RuntimeError("node failed")


async def adouble(x):
    return 2 * x


def call(node, request):
    return json.loads(NodeHost(INFO)(node, json.dumps(request)))


def test_resolve_rejects_paths_without_colon():
    with pytest.raises(ValueError):
        resolve("test_worker.double")


def test_ok_reply_carries_attempt_and_worker():
    reply = call(f"{__name__}:double", {"attempt_id": "a1", "input": 21})
    assert reply == {"ok": True, "value": 42, "attempt_id": "a1", "worker": INFO}


def test_coroutine_node_is_awaited():
    assert call(f"{__name__}:adouble", {"input": 4})["value"] == 8


@pytest.mark.parametrize("node,kind", [(f"{__name__}:boom", "node_exception"),
                                       (f"{__name__}:missing", "resolve_error"),
                                       ("no_such_module:f", "resolve_error")])
def test_errors_are_values(node, kind):
    reply = call(node, {"attempt_id": "a2", "input": 1})
    assert reply["ok"] is False
    assert reply["error_kind"] == kind
    assert reply["attempt_id"] == "a2"


def test_bad_json_is_a_value():
    reply = json.loads(NodeHost(INFO)(f"{__name__}:double", "{not json"))
    assert reply["error_kind"] == "bad_request"


def test_unserializable_value_is_a_value():
    reply = call("builtins:set", {"input": [1]})
    assert reply["error_kind"] == "node_exception"
