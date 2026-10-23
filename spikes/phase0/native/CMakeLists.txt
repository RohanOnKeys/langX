# Phase 0 spike build: work-free root executable and the _p0 pybind11 module.
# Configure against a supervision-enabled HPX install, e.g.:
#   cmake -S native -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
#     -DPYBIND11_FINDPYTHON=ON \
#     -DCMAKE_PREFIX_PATH="<hpx-install>;$(python -m pybind11 --cmakedir)" \
#     -DPython_EXECUTABLE="$(which python)"
cmake_minimum_required(VERSION 3.19)
project(langx_phase0 CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
endif()

find_package(HPX REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

if(NOT HPX_WITH_SUPERVISION OR NOT TARGET HPX::supervision_dispatch_component)
  message(FATAL_ERROR
    "HPX at CMAKE_PREFIX_PATH lacks HPX_WITH_SUPERVISION or "
    "HPX::supervision_dispatch_component; phase 0 spikes need both.")
endif()

add_executable(p0_root p0_root.cpp)
target_link_libraries(p0_root PRIVATE HPX::hpx HPX::supervision_dispatch_component)
target_compile_features(p0_root PRIVATE cxx_std_20)

add_executable(repro_leave repro_leave.cpp)
target_link_libraries(repro_leave PRIVATE HPX::hpx HPX::supervision_dispatch_component)
target_compile_features(repro_leave PRIVATE cxx_std_20)

pybind11_add_module(_p0 p0_ext.cpp)
target_link_libraries(_p0 PRIVATE HPX::hpx HPX::supervision_dispatch_component)
target_compile_features(_p0 PRIVATE cxx_std_20)
