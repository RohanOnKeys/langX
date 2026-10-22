# hpx_impl

Native C++ for the parts of langX that run outside Python.

| Path | Purpose |
| --- | --- |
| `root/langx_root.cpp` | Work-free supervised root, HPX locality 0. Anchors AGAS, expects connecting localities, runs no application work, serves the membership action. Built by `python/CMakeLists.txt` as `python/build/langx_root`. |
| `langx/membership.hpp` | Membership action: the root calls `hpx::force_disconnect` on a connector's request, because a connector cannot call it itself (phase 0 results). Included in exactly one translation unit per binary. |

The root's startup sequence follows RayX exp70 Slice 3B `native/root_supervised.cpp` (Bita).

## Root options

| Option | Meaning |
| --- | --- |
| `--langx-ready-file PATH` | JSON written once the root is up; `PATH.exit` is written on shutdown |
| `--langx-stop-file PATH` | exit when this file appears (SIGTERM and SIGINT also work) |
| `--langx-poll-timeout-ms N` | failure-detection poll timeout through the HPX testing hook; 0 keeps the HPX default |
| `--langx-shutdown-timeout-s N` | bound on shutdown, default 10 |

Every other option goes to HPX, for example `--hpx:agas=HOST:PORT --hpx:hpx=HOST:PORT --hpx:expect-connecting-localities`.
