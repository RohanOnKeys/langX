# hpx_impl

Native C++ for the parts of langX that run outside Python.

Planned contents (plan sections 6.1 and 17):

- **Work-free supervised root (HPX locality 0).** Anchors AGAS, expects connecting localities, runs no application work. It also serves membership requests (`discover_and_join`, `force_disconnect`) if spike F shows a connector cannot make these calls itself.
- **Shared headers** for the generic `run_node` action, included by `python/src/langx/_langx.cpp`.

The root is expected to derive from RayX exp70 Slice 3B `native/root_supervised.cpp`. That file is not copied here until its author has agreed to the reuse (see `rayx/README.md`).
