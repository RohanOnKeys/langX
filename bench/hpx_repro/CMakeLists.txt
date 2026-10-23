# Standalone build of the supervision reproducers. Needs only HPX:
#   cmake -S bench/hpx_repro -B bench/hpx_repro/build -G Ninja \
#     -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=<hpx-install>
cmake_minimum_required(VERSION 3.19)
project(hpx_supervision_repro CXX)

find_package(HPX REQUIRED)
if(NOT HPX_WITH_SUPERVISION OR NOT TARGET HPX::supervision_dispatch_component)
  message(FATAL_ERROR "HPX must be built with HPX_WITH_SUPERVISION=ON")
endif()

add_executable(supervision_gaps_repro supervision_gaps_repro.cpp)
target_link_libraries(supervision_gaps_repro
  PRIVATE HPX::hpx HPX::supervision_dispatch_component)
target_compile_features(supervision_gaps_repro PRIVATE cxx_std_20)
