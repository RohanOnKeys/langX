"""Coordinator: LangGraph proxy nodes, epoch table and commit fence.

Responsibilities (plan sections 6.2 to 6.5 and 9):
    * proxy node factory that dispatches a node to a live worker
    * coordinator-owned table of (worker locality, epoch) -> live or fenced
    * commit fence: accept at most one result per (run, step, node), only
      from a live, non-latched epoch
    * bounded retry on another worker after any dispatch failure

Not implemented yet.
"""
