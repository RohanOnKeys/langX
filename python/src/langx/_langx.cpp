//  Copyright (c) 2026 Rohan Pattanayak
//
//  _langx: pybind11 bridge between the langx Python package and an HPX
//  locality hosted in-process. Planned surface: plan section 7 (init,
//  finalize, discovery and join, epoch query, admission, await_terminal,
//  force_disconnect, and dispatch of the single generic run_node action).
//
//  Not implemented yet. This translation unit only defines the module so the
//  build wiring can be checked.

#include <pybind11/pybind11.h>

namespace py = pybind11;

PYBIND11_MODULE(_langx, m)
{
    m.doc() = "langx native bridge (HPX supervision dispatch); not implemented yet";
}
