"""Effect log, outbox applier and direct effect sink.

Responsibilities (plan sections 6.6 and 12):
    * Tier A: apply outbox intents at commit time, keyed by
      ``sha256(run_id, step, node, index)``, exactly once
    * Tier B: direct sink in ``naive`` and ``keyed`` modes
    * append-only log with provenance (run, step, node, attempt, worker,
      epoch, origin) for duplicate counting

Not implemented yet.
"""
