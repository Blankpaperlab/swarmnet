if(NOT DEFINED SWARMNET_SOURCE_DIR)
    message(FATAL_ERROR "SWARMNET_SOURCE_DIR is required")
endif()
if(NOT DEFINED SWARMNET_BINARY_DIR)
    message(FATAL_ERROR "SWARMNET_BINARY_DIR is required")
endif()

if(NOT DEFINED PYTHON_EXECUTABLE)
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    set(PYTHON_EXECUTABLE "${Python3_EXECUTABLE}")
endif()

set(_source_dir "${SWARMNET_SOURCE_DIR}")
set(_binary_dir "${SWARMNET_BINARY_DIR}")

execute_process(
    COMMAND "${PYTHON_EXECUTABLE}" "${_source_dir}/tools/amalgamate.py" --output dist
    WORKING_DIRECTORY "${_source_dir}"
    RESULT_VARIABLE _amalgamate_rv
)
if(NOT _amalgamate_rv EQUAL 0)
    message(FATAL_ERROR "amalgamation generation failed")
endif()

set(_smoke_dir "${_binary_dir}/amalgamated-smoke")
set(_smoke_build_dir "${_smoke_dir}/build")
file(REMOVE_RECURSE "${_smoke_dir}")
file(MAKE_DIRECTORY "${_smoke_dir}")

file(COPY "${_source_dir}/dist/swarmnet.hpp" DESTINATION "${_smoke_dir}")
file(COPY "${_source_dir}/dist/swarmnet.cpp" DESTINATION "${_smoke_dir}")

file(WRITE "${_smoke_dir}/main.cpp" [=[
#include "swarmnet.hpp"

int main() {
    swarmnet::Config cfg{};
    cfg.swarm_id = "amalgamated-smoke";
    cfg.tick_us = 1;

    swarmnet::SwarmNet swarm(cfg);
    swarm.spawn([](swarmnet::Agent& agent) {
        agent.set_shared_state("k", "v");
        (void)agent.get_shared_state_view("k");
    });
    swarm.run_ticks(8);
    return swarm.latest_state_root() == 0 ? 1 : 0;
}
]=])

file(WRITE "${_smoke_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.24)
project(SwarmNetAmalgamatedSmoke LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
add_library(swarmnet STATIC swarmnet.cpp)
if(WIN32)
    target_link_libraries(swarmnet PUBLIC ws2_32)
endif()
add_executable(amalgamated_smoke main.cpp)
target_link_libraries(amalgamated_smoke PRIVATE swarmnet)
]=])

set(_configure_cmd "${CMAKE_COMMAND}" -S "${_smoke_dir}" -B "${_smoke_build_dir}")
if(DEFINED CMAKE_GENERATOR AND NOT CMAKE_GENERATOR STREQUAL "")
    list(APPEND _configure_cmd -G "${CMAKE_GENERATOR}")
endif()
if(DEFINED CMAKE_BUILD_TYPE AND NOT CMAKE_BUILD_TYPE STREQUAL "")
    list(APPEND _configure_cmd "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")
endif()
if(DEFINED CMAKE_C_COMPILER AND NOT CMAKE_C_COMPILER STREQUAL "")
    list(APPEND _configure_cmd "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
endif()
if(DEFINED CMAKE_CXX_COMPILER AND NOT CMAKE_CXX_COMPILER STREQUAL "")
    list(APPEND _configure_cmd "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}")
endif()

execute_process(COMMAND ${_configure_cmd} RESULT_VARIABLE _cfg_rv)
if(NOT _cfg_rv EQUAL 0)
    message(FATAL_ERROR "amalgamated smoke configure failed")
endif()

set(_build_cmd "${CMAKE_COMMAND}" --build "${_smoke_build_dir}")
if(DEFINED CMAKE_BUILD_TYPE AND NOT CMAKE_BUILD_TYPE STREQUAL "")
    list(APPEND _build_cmd --config "${CMAKE_BUILD_TYPE}")
endif()
execute_process(COMMAND ${_build_cmd} RESULT_VARIABLE _build_rv)
if(NOT _build_rv EQUAL 0)
    message(FATAL_ERROR "amalgamated smoke build failed")
endif()

if(WIN32)
    set(_exe_path "${_smoke_build_dir}/${CMAKE_BUILD_TYPE}/amalgamated_smoke.exe")
    if(NOT EXISTS "${_exe_path}")
        set(_exe_path "${_smoke_build_dir}/amalgamated_smoke.exe")
    endif()
else()
    set(_exe_path "${_smoke_build_dir}/amalgamated_smoke")
endif()

execute_process(COMMAND "${_exe_path}" RESULT_VARIABLE _run_rv)
if(NOT _run_rv EQUAL 0)
    message(FATAL_ERROR "amalgamated smoke run failed")
endif()
