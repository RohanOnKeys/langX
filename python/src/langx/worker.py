"""Worker entry point: serves ``run_node`` calls on one HPX locality.

Responsibilities (plan section 8):
    * connect to the root and initialize supervision
    * resolve node functions by dotted path (``module:function``)
    * run them on a dedicated Python thread, returning node errors as values
    * publish lifecycle events and finalize with a bounded wait

Not implemented yet.
"""
