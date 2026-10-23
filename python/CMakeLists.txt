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

if(NOT HPX_WITH_SUPERVISION OR NOT TARGET HPX::supervision_dispatch_component)
  message(FATAL_ERROR
    "The HPX install at CMAKE_PREFIX_PATH lacks HPX_WITH_SUPERVISION or "
    "HPX::supervision_dispatch_component; langx needs both.")
endif()

set(LANGX_HPX_IMPL ${CMAKE_CURRENT_SOURCE_DIR}/../hpx_impl)

# Python-facing module is `_langx`. Link HPX::hpx only, not HPX::wrap_main:
# a Python extension has no main(), so HPX is started as a library.
pybind11_add_module(_langx src/langx/_langx.cpp)
target_include_directories(_langx PRIVATE ${LANGX_HPX_IMPL})
target_link_libraries(_langx PRIVATE HPX::hpx HPX::supervision_dispatch_component)
target_compile_features(_langx PRIVATE cxx_std_20)

# Drop the compiled module next to the Python package so
# `PYTHONPATH=python/src` can import `langx._langx`.
set_target_properties(_langx PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/langx)

# Work-free supervised root (locality 0), left in the build directory.
add_executable(langx_root ${LANGX_HPX_IMPL}/root/langx_root.cpp)
target_include_directories(langx_root PRIVATE ${LANGX_HPX_IMPL})
target_link_libraries(langx_root PRIVATE HPX::hpx HPX::supervision_dispatch_component)
target_compile_features(langx_root PRIVATE cxx_std_20)
