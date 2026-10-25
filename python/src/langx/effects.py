"""Effect log, outbox applier and direct effect sink (plan sections 6.6, 12).

* Tier A (outbox): a node returns effect intents; the coordinator applies
  them after the commit fence, keyed by ``effect_key(run, step, node,
  index)``. The key is unique in the log, so an intent is applied at most
  once.
* Tier B (direct): the node writes the effect itself through
  ``DirectSink``. In ``naive`` mode every write lands, so a retried node
  duplicates it; in ``keyed`` mode the key is unique, like an external
  system that honors idempotency keys.

The log is an append-only SQLite table. Duplicates are found by counting
keys.
"""

import hashlib
import json
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS effects (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    run_id TEXT, step INTEGER, node TEXT, attempt_id TEXT,
    origin TEXT, worker_locality INTEGER, epoch INTEGER,
    kind TEXT, payload TEXT,
    committed_at REAL
)
"""


def effect_key(run_id, step, node, index):
    """Idempotency key of one effect intent."""
    text = "\x1f".join(str(x) for x in (run_id, step, node, index))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class EffectLog:
    """Append-only effect log in SQLite; ``keyed`` makes the key unique."""

    def __init__(self, path, keyed=True):
        self.keyed = keyed
        self.db = sqlite3.connect(str(path), isolation_level=None,
                                  check_same_thread=False, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(SCHEMA)
        if keyed:
            self.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS effects_key "
                            "ON effects(key)")

    def record(self, key, origin, run_id, step, node, attempt_id,
               worker_locality, epoch, kind, payload):
        """Append one effect. Returns False if keyed and already present."""
        verb = "INSERT OR IGNORE" if self.keyed else "INSERT"
        cur = self.db.execute(
            f"{verb} INTO effects (key, run_id, step, node, attempt_id, origin,"
            " worker_locality, epoch, kind, payload, committed_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (key, run_id, step, node, attempt_id, origin, worker_locality,
             epoch, kind, json.dumps(payload, sort_keys=True), time.monotonic()))
        return cur.rowcount == 1

    def rows(self):
        """Every effect, as dicts, in append order."""
        cur = self.db.execute("SELECT key, run_id, step, node, attempt_id, origin,"
                              " worker_locality, epoch, kind, payload FROM effects"
                              " ORDER BY seq")
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]

    def keys(self, origin=None):
        """Distinct keys, optionally for one origin."""
        sql, args = "SELECT DISTINCT key FROM effects", ()
        if origin:
            sql, args = sql + " WHERE origin = ?", (origin,)
        return {r[0] for r in self.db.execute(sql, args)}

    def duplicates(self, origin=None):
        """Number of keys that were written more than once."""
        sql = "SELECT key FROM effects"
        args = ()
        if origin:
            sql, args = sql + " WHERE origin = ?", (origin,)
        sql += " GROUP BY key HAVING COUNT(*) > 1"
        return len(self.db.execute(sql, args).fetchall())

    def close(self):
        """Close the database."""
        self.db.close()


class Outbox:
    """Applies a committed node's effect intents to the log, keyed."""

    def __init__(self, log):
        self.log = log

    def apply(self, run_id, step, node, attempt_id, worker_locality, epoch,
              intents):
        """Apply ``intents`` (dicts with ``kind`` and ``payload``).

        Returns the number newly applied; an intent whose key is already in
        the log is skipped.
        """
        applied = 0
        for index, intent in enumerate(intents):
            if self.log.record(effect_key(run_id, step, node, index), "outbox",
                               run_id, step, node, attempt_id, worker_locality,
                               epoch, intent["kind"], intent.get("payload")):
                applied += 1
        return applied


class DirectSink:
    """Tier B: a node writes its effect directly, ``naive`` or ``keyed``."""

    def __init__(self, path, mode):
        if mode not in ("naive", "keyed"):
            raise ValueError(f"mode must be naive or keyed, got {mode!r}")
        self.log = EffectLog(path, keyed=(mode == "keyed"))

    def emit(self, key, run_id, step, node, attempt_id, worker_locality, epoch,
             kind, payload):
        """Write one direct effect. Returns False if keyed and present."""
        return self.log.record(key, "direct", run_id, step, node, attempt_id,
                               worker_locality, epoch, kind, payload)
