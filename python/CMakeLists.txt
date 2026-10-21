cmake_minimum_required(VERSION 3.19)
project(langx_ext CXX)

# Build against an installed HPX prefix and the pybind11 shipped in the venv.
# Configure with, e.g.:
#   cmake -S python -B python/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
#     -DPYBIND11_FINDPYTHON=ON \
#     -DCMAKE_PREFIX_PATH="<hpx-install>;$(.venv/bin/python -m pybind11 --cmakedir)" \
#     -DPython_EXECUTABLE="<repo>/.venv/bin/python"
# -DPYBIND11_FINDPYTHON=ON is required, otherwise pybind11 may pick up a
# system Python instead of the venv.
#
# HPX must be built with supervision_dispatch, force_disconnect, networking
# and the TCP parcelport enabled (tested target: master cc2be553).
find_package(HPX REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

# Python-facing module is `_langx`. Link HPX::hpx only, not HPX::wrap_main:
# a Python extension has no main(), so HPX is started as a library.
pybind11_add_module(_langx src/langx/_langx.cpp)
target_include_directories(_langx PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../hpx_impl)
target_link_libraries(_langx PRIVATE HPX::hpx)

# Drop the compiled module next to the Python package so
# `PYTHONPATH=python/src` can import `langx._langx`.
set_target_properties(_langx PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/langx)
