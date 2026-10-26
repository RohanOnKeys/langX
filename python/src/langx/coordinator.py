"""Coordinator: proxy nodes, epoch table, commit fence, retry (plan 6.2 to 6.5, 9).

``Coordinator.execute`` runs one LangGraph task on a worker:

1. pick a live worker from the epoch table;
2. dispatch ``run_node`` with an attempt id (``Runtime.dispatch_node``
   refuses a fenced worker and abandons a call whose worker is fenced in
   flight);
3. commit fence: accept the result only if the producing (locality, epoch)
   is still live, the reply carries this attempt's id and that epoch, and
   nothing has been accepted for (run, step, node) yet;
4. apply the result's outbox intents, keyed, and return the result;
5. on any infrastructure failure, retry on another worker, up to
   ``max_attempts``.

Node errors come back from the worker as values and are raised as
``NodeError``; they are not retried. This module does not import LangGraph:
``remote`` builds the async node function LangGraph calls.
"""

import asyncio
import itertools
import json
import time

from langx.runtime import DispatchFailed, TargetFenced


class NodeError(Exception):
    """The node function itself failed on the worker."""


class RetriesExhausted(Exception):
    """No attempt produced a committable result."""


class EpochTable:
    """The coordinator's own record of which (locality, epoch) is live.

    HPX admission does not check epochs, so this table is authoritative. A
    fenced worker is never un-fenced; a replacement is a new locality.
    """

    def __init__(self):
        self._peers = {}
        self._fenced = {}

    def add(self, peer):
        """Start tracking ``peer`` at its join epoch."""
        self._peers[peer.locality] = peer

    def fence(self, locality, reason):
        """Mark ``locality`` fenced, permanently."""
        self._fenced.setdefault(locality, reason)

    def is_live(self, locality, epoch):
        """True if ``locality`` is tracked at ``epoch`` and not fenced."""
        peer = self._peers.get(locality)
        return (peer is not None and peer.epoch == epoch
                and locality not in self._fenced)

    def live(self):
        """Live peers, in locality order."""
        return [p for loc, p in sorted(self._peers.items())
                if loc not in self._fenced]

    def fenced(self):
        """Fenced localities and why."""
        return dict(self._fenced)


def task_identity(config, state, key=None):
    """(run_id, step, node_key) of the LangGraph task being executed.

    ``run_id`` is the ``thread_id``, ``step`` the superstep. ``key`` maps the
    node input to a key that tells apart parallel tasks of the same node in
    one superstep (for example Send fan-out); by default the node name.
    """
    md = config.get("metadata", {})
    run_id = config.get("configurable", {}).get("thread_id", "")
    node_key = key(state) if key else md.get("langgraph_node", "")
    return run_id, md.get("langgraph_step"), node_key


class Coordinator:
    """Dispatches LangGraph tasks to workers behind a commit fence."""

    def __init__(self, runtime, outbox=None, max_attempts=4,
                 dispatch_timeout_s=None, on_dispatch=None):
        self.rt = runtime
        self.outbox = outbox
        self.max_attempts = max_attempts
        self.dispatch_timeout_s = dispatch_timeout_s
        # Test hook: on_dispatch(key, attempt, peer) runs right before each
        # dispatch and may return a timeout for that attempt. The failure
        # harness uses it to inject faults; it never fences a worker.
        self.on_dispatch = on_dispatch
        self.epochs = EpochTable()
        self.accepted = {}
        self.events = []
        self._locks = {}
        self._next = itertools.count()

    def add_workers(self, peers):
        """Track joined workers at their join epochs."""
        for p in peers:
            self.epochs.add(p)

    def pick(self):
        """Next live worker, round robin."""
        live = self.epochs.live()
        if not live:
            raise RetriesExhausted("no live workers")
        return live[next(self._next) % len(live)]

    def _event(self, kind, key, attempt_id, peer, detail=""):
        self.events.append({"event": kind, "run_id": key[0], "step": key[1],
                            "node": key[2], "attempt_id": attempt_id,
                            "locality": peer.locality if peer else None,
                            "epoch": peer.epoch if peer else None,
                            "detail": detail, "t": time.monotonic()})

    async def execute(self, run_id, step, node_key, node_path, node_input):
        """Run one task and return the committed node value."""
        key = (run_id, step, node_key)
        for n in range(1, self.max_attempts + 1):
            if key in self.accepted:
                return self.accepted[key]["value"]
            peer = self.pick()
            attempt_id = f"{run_id}/{step}/{node_key}/{n}"
            payload = json.dumps({"attempt_id": attempt_id, "input": node_input},
                                 sort_keys=True)
            timeout_s = self.dispatch_timeout_s
            if self.on_dispatch is not None:
                override = self.on_dispatch(key, n, peer)
                if override is not None:
                    timeout_s = override
            self._event("dispatched", key, attempt_id, peer)
            try:
                res = await self.rt.dispatch_node(
                    peer, node_path, payload, timeout_s=timeout_s)
            except TargetFenced as ex:
                self.epochs.fence(peer.locality, "target_fenced")
                self._event("retry", key, attempt_id, peer,
                            "abandoned" if ex.abandoned else "fenced")
                continue
            except DispatchFailed as ex:
                self.epochs.fence(peer.locality, "dispatch_failed")
                self._event("retry", key, attempt_id, peer, f"failed: {ex}"[:200])
                continue
            except TimeoutError:
                self._event("retry", key, attempt_id, peer, "timeout")
                continue
            reply = res.json()
            if not reply.get("ok"):
                self._event("node_error", key, attempt_id, peer,
                            reply.get("error", ""))
                raise NodeError(f"{node_path} failed on locality "
                                f"{peer.locality}: {reply.get('error')}")
            if await self.commit(key, attempt_id, peer, reply):
                return reply["value"]
        raise RetriesExhausted(f"{node_path} {key}: {self.max_attempts} attempts")

    async def commit(self, key, attempt_id, peer, reply):
        """Commit fence (plan 6.5). Returns True if the result is accepted."""
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            worker = reply.get("worker", {})
            reason = None
            if key in self.accepted:
                reason = "already_accepted"
            elif reply.get("attempt_id") != attempt_id:
                reason = "attempt_mismatch"
            elif (worker.get("locality"), worker.get("epoch")) != (
                    peer.locality, peer.epoch):
                reason = "producer_mismatch"
            elif not self.epochs.is_live(peer.locality, peer.epoch):
                reason = "stale_epoch"
            elif not self.rt.admitted(peer):
                self.epochs.fence(peer.locality, "fenced_at_commit")
                reason = "fenced_at_commit"
            if reason:
                self._event("rejected", key, attempt_id, peer, reason)
                return False
            value = reply["value"]
            self.accepted[key] = {"attempt_id": attempt_id, "value": value,
                                  "locality": peer.locality, "epoch": peer.epoch}
            applied = 0
            if self.outbox is not None:
                applied = self.outbox.apply(
                    key[0], key[1], key[2], attempt_id, peer.locality,
                    peer.epoch, value.get("effects", []))
            self._event("committed", key, attempt_id, peer,
                        f"effects_applied={applied}")
            return True

    def remote(self, node_path, key=None):
        """Async LangGraph node that runs ``node_path`` on a worker.

        The node function returns ``{"update": ..., "effects": [...]}``; the
        proxy hands the update to LangGraph.
        """
        async def proxy(state, config):
            run_id, step, node_key = task_identity(config, state, key)
            value = await self.execute(run_id, step, node_key, node_path, state)
            return value["update"]
        return proxy
