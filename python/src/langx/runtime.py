"""Python facade over the ``_langx`` HPX extension.

Responsibilities (plan section 7):
    * start and stop the in-process HPX locality in connect mode
    * initialize and finalize supervision, with the post-finalize pause that
      phase 0 found necessary on HPX ``cc2be553``
    * discover and join peers, read their epochs, check admission
    * dispatch the generic ``run_node`` action from asyncio, mapping every
      dispatch failure to ``TargetFenced`` or ``DispatchFailed``
    * abandon a dispatch whose target is fenced while it is in flight, since
      HPX never fails such a dispatch itself (phase 0 results)
    * ask the root to ``force_disconnect`` a locality, since a connector
      cannot do it itself

The extension is imported lazily, so this module stays importable without
HPX. Results arrive from HPX through a completion pipe that the running
asyncio loop watches; no HPX thread ever takes the GIL.
"""

import asyncio
import itertools
import json
import threading
import time
from dataclasses import dataclass


class TargetFenced(Exception):
    """The target is fenced: the dispatch was refused, or abandoned in flight.

    ``abandoned`` is True when the dispatch had been sent and the target was
    fenced before a result arrived. The node may still have run.
    """

    def __init__(self, message, abandoned=False):
        super().__init__(message)
        self.abandoned = abandoned


class DispatchFailed(Exception):
    """Any other dispatch failure. The original HPX text is kept."""

    def __init__(self, message, error_code=0):
        super().__init__(message)
        self.error_code = error_code


@dataclass(frozen=True)
class Peer:
    """A joined locality and the epoch it was joined at."""

    locality: int
    epoch: int


@dataclass(frozen=True)
class NodeResult:
    """Raw run_node result from one worker, with dispatch timestamps."""

    peer: Peer
    value: bytes
    t0_ns: int
    t1_ns: int

    def json(self):
        """Decode the worker's JSON envelope."""
        return json.loads(self.value)


def load_extension():
    """Import and return the native ``_langx`` module."""
    from langx import _langx  # noqa: PLC0415

    return _langx


def connector_args(agas, hpx, extra=()):
    """HPX command-line arguments for a late-connecting locality."""
    return [f"--hpx:agas={agas}", f"--hpx:hpx={hpx}", "--hpx:bind=none",
            "--hpx:ignore-batch-env", *extra]


class Runtime:
    """One in-process HPX locality, used by the coordinator and by workers.

    ``ext`` is injectable so the asyncio logic can be tested without HPX.
    """

    def __init__(self, ext=None):
        self._ext = ext
        self._ids = itertools.count(1)
        self._pending = {}
        self._loop = None
        self.poll_timeout_s = None
        self.self_peer = None

    @property
    def ext(self):
        """The native extension, imported on first use."""
        if self._ext is None:
            self._ext = load_extension()
        return self._ext

    # ---- lifecycle ---------------------------------------------------------

    def start(self, agas, hpx, threads=4, extra_args=()):
        """Start HPX in connect mode against the root at ``agas``."""
        self.ext.start(threads, connector_args(agas, hpx, extra_args))

    def init(self, discovery_timeout_s=5.0, poll_timeout_s=None):
        """Initialize supervision and publish ``running``.

        ``poll_timeout_s`` sets the failure-detection poll timeout through
        HPX's testing hook; None keeps the HPX default (60 s). Returns this
        locality as a ``Peer``.
        """
        locality, epoch = self.ext.init(
            int(discovery_timeout_s * 1000),
            int(poll_timeout_s * 1000) if poll_timeout_s else 0)
        self.poll_timeout_s = poll_timeout_s
        self.self_peer = Peer(locality, epoch)
        return self.self_peer

    def finalize(self, timeout_s=10.0, pause_s=None):
        """Finalize supervision, then pause before any disconnect.

        On HPX ``cc2be553``, ``supervision::finalize()`` can return while
        failure-detection checks are still pending, and a locality that stops
        before they resolve aborts in ``hpx::stop`` (hpx#7825). The pause
        defaults to twice the poll timeout given to ``init``. With the HPX
        default poll timeout there is no safe default, so ``pause_s`` must be
        given.

        Returns False if finalize did not finish within ``timeout_s``.
        """
        if pause_s is None:
            if not self.poll_timeout_s:
                raise ValueError("pass pause_s: init() used the HPX default "
                                 "poll timeout, so no safe pause is known")
            pause_s = 2 * self.poll_timeout_s
        done = threading.Event()

        def run():
            self.ext.finalize()
            done.set()

        threading.Thread(target=run, daemon=True).start()
        if not done.wait(timeout_s):
            return False
        time.sleep(pause_s)
        return True

    def stop(self):
        """Disconnect from the root and stop HPX. Returns ``hpx::stop``'s rc."""
        self._detach()
        return self.ext.stop()

    def shutdown(self, timeout_s=10.0, pause_s=None):
        """Finalize, pause, then stop. Returns (finalized_in_time, stop_rc)."""
        finalized = self.finalize(timeout_s, pause_s)
        return finalized, self.stop()

    # ---- membership --------------------------------------------------------

    def discover_and_join(self, timeout_s=2.0):
        """Run one discovery and join pass. Returns the joined peers."""
        return [Peer(loc, ep)
                for loc, ep in self.ext.discover_and_join(int(timeout_s * 1000))]

    def wait_for_peers(self, localities, timeout_s=60.0, pass_timeout_s=2.0):
        """Join until every locality in ``localities`` is joined.

        Discovery is one-shot in HPX, so late arrivals need repeated passes.
        Returns a dict locality -> Peer. Raises TimeoutError otherwise.
        """
        want = set(localities)
        joined = {}
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            for p in self.discover_and_join(pass_timeout_s):
                joined[p.locality] = p
            if want <= set(joined):
                return {loc: joined[loc] for loc in want}
        missing = sorted(want - set(joined))
        raise TimeoutError(f"localities not joined: {missing}")

    def snapshot_peers(self):
        """Joined, non-evicted peers as seen by this locality's registry."""
        return [Peer(loc, ep) for loc, ep in self.ext.snapshot_peers()]

    def membership(self):
        """Locality ids currently known to AGAS on this locality."""
        return list(self.ext.membership())

    def peer_state(self, locality):
        """This locality's view of ``locality``: last event, epoch, seq."""
        return dict(self.ext.peer_state(locality))

    def query_epoch(self, locality):
        """Epoch of ``locality`` as recorded on this locality's shadow."""
        return self.peer_state(locality)["epoch"]

    def admitted(self, peer):
        """True unless a terminal event is latched for ``peer``'s epoch."""
        return bool(self.ext.admitted(peer.locality, peer.epoch))

    # ---- asyncio bridge ----------------------------------------------------

    def _attach(self):
        loop = asyncio.get_running_loop()
        if self._loop is loop:
            return loop
        self._detach()
        loop.add_reader(self.ext.completion_fd(), self._drain)
        self._loop = loop
        return loop

    def _detach(self):
        if self._loop is not None and not self._loop.is_closed():
            self._loop.remove_reader(self.ext.completion_fd())
        self._loop = None

    def _drain(self):
        for c in self.ext.drain_completions():
            fut = self._pending.pop(c["id"], None)
            if fut is not None and not fut.done():
                fut.set_result(c)

    async def _submit(self, send):
        loop = self._attach()
        request_id = next(self._ids)
        fut = loop.create_future()
        self._pending[request_id] = fut
        try:
            send(request_id)
            return await fut
        finally:
            # A completion for an abandoned request is dropped by _drain.
            self._pending.pop(request_id, None)

    async def wait_fenced(self, peer, poll_s=0.01):
        """Return once this locality's view shows ``peer`` fenced."""
        while self.admitted(peer):
            await asyncio.sleep(poll_s)

    async def dispatch_node(self, peer, node, payload, timeout_s=None,
                            abandon_on_fence=True, fence_poll_s=0.01):
        """Run ``node`` on ``peer`` and return its ``NodeResult``.

        Raises ``TargetFenced`` if the dispatch is refused, or, with
        ``abandon_on_fence``, if ``peer`` is fenced before the result
        arrives. Raises ``DispatchFailed`` for any other HPX failure and
        ``TimeoutError`` after ``timeout_s``. Neither a timeout nor an
        abandonment cancels the remote call, so a late result can still be
        produced; the coordinator's commit fence must reject it.
        """
        # Refuse up front when this locality already sees the target fenced,
        # as dispatch_work's own caller-side check would, so a refusal is
        # never reported as an abandonment.
        if not self.admitted(peer):
            raise TargetFenced("target already fenced; dispatch not sent")
        call = asyncio.ensure_future(self._submit(
            lambda i: self.ext.dispatch_node(i, peer.locality, peer.epoch,
                                             node, payload)))
        watch = (asyncio.ensure_future(self.wait_fenced(peer, fence_poll_s))
                 if abandon_on_fence else None)
        try:
            done, _ = await asyncio.wait({call} | ({watch} if watch else set()),
                                         timeout=timeout_s,
                                         return_when=asyncio.FIRST_COMPLETED)
        finally:
            if watch is not None:
                watch.cancel()
        if call in done:
            c = call.result()
            if c["status"] == "ok":
                return NodeResult(peer, c["result"], c["t0_ns"], c["t1_ns"])
            if c["status"] == "fenced":
                raise TargetFenced(c["error"])
            raise DispatchFailed(c["error"], c["error_code"])
        call.cancel()
        if watch is not None and watch in done:
            raise TargetFenced("target fenced while the dispatch was in flight",
                               abandoned=True)
        raise TimeoutError(f"no result from locality {peer.locality} "
                           f"within {timeout_s} s")

    async def force_disconnect(self, locality):
        """Ask the root to ``force_disconnect`` ``locality``.

        Returns the root's report (``rc``, ``elapsed_ms``,
        ``membership_after``). Raises ``DispatchFailed`` if the request fails
        or the root reports an error.
        """
        c = await self._submit(
            lambda i: self.ext.request_force_disconnect(i, locality))
        if c["status"] != "ok":
            raise DispatchFailed(c["error"], c["error_code"])
        report = json.loads(c["result"])
        if report["rc"] != 0 or report["error"]:
            raise DispatchFailed(
                f"root force_disconnect rc={report['rc']}: {report['error']}")
        return report
