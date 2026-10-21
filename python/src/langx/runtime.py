"""Python facade over the ``_langx`` HPX extension.

Responsibilities (plan section 7):
    * start and stop the in-process HPX locality in connect mode
    * discover and join peers, read their epochs, check admission
    * wait for terminal lifecycle events on a peer's shadow
    * request ``force_disconnect`` for a fenced locality
    * dispatch the generic ``run_node`` action and map every dispatch failure
      to ``TargetFenced`` or ``DispatchFailed``

Not implemented yet.
"""
