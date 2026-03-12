cmake_minimum_required(VERSION 3.24)

project(SwarmNet LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(SWARMNET_BUILD_TESTS "Build SwarmNet tests." ON)
option(SWARMNET_BUILD_EXAMPLES "Build SwarmNet examples." ON)
option(SWARMNET_ENABLE_ASAN "Enable AddressSanitizer (non-Windows)." OFF)
option(SWARMNET_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer (non-Windows)." OFF)
option(SWARMNET_ENABLE_TSAN "Enable ThreadSanitizer (non-Windows)." OFF)
option(SWARMNET_ENABLE_CLANG_TIDY "Enable clang-tidy checks." OFF)

if(SWARMNET_ENABLE_TSAN AND SWARMNET_ENABLE_ASAN)
    message(FATAL_ERROR "SWARMNET_ENABLE_TSAN and SWARMNET_ENABLE_ASAN cannot both be ON.")
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(Warnings)
include(Sanitizers)
find_package(Python3 COMPONENTS Interpreter QUIET)

if(SWARMNET_ENABLE_CLANG_TIDY)
    find_program(CLANG_TIDY_EXE NAMES clang-tidy REQUIRED)
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_EXE};--config-file=${CMAKE_SOURCE_DIR}/.clang-tidy")
endif()

add_library(swarmnet STATIC
    src/core.cpp
    src/membership.cpp
    src/replay.cpp
    src/transport.cpp
    src/swarmnet.cpp
)
add_library(swarmnet::swarmnet ALIAS swarmnet)
target_include_directories(swarmnet PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_compile_features(swarmnet PUBLIC cxx_std_23)
if(WIN32)
    target_link_libraries(swarmnet PUBLIC ws2_32)
endif()
swarmnet_enable_warnings(swarmnet)
swarmnet_enable_sanitizers(swarmnet)

add_custom_target(benchmark-reference
    COMMAND ${CMAKE_COMMAND} -P "${CMAKE_CURRENT_SOURCE_DIR}/tools/benchmark_reference.cmake"
    COMMENT "Print benchmark reference machine baseline for Week 1 targets."
)

if(Python3_FOUND)
    add_custom_target(amalgamate
        COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/tools/amalgamate.py" --output dist
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        COMMENT "Generate dist/swarmnet.hpp and dist/swarmnet.cpp"
    )
endif()

if(SWARMNET_BUILD_EXAMPLES)
    add_executable(example_minimal examples/minimal/main.cpp)
    target_link_libraries(example_minimal PRIVATE swarmnet::swarmnet)
    swarmnet_enable_warnings(example_minimal)
    swarmnet_enable_sanitizers(example_minimal)

    add_executable(swarmnet_test tools/swarmnet_test.cpp)
    target_link_libraries(swarmnet_test PRIVATE swarmnet::swarmnet)
    if(WIN32)
        target_link_libraries(swarmnet_test PRIVATE psapi)
    endif()
    swarmnet_enable_warnings(swarmnet_test)
    swarmnet_enable_sanitizers(swarmnet_test)

    add_custom_target(benchmark-week7
        COMMAND $<TARGET_FILE:swarmnet_test>
                --mode=bench
                --agents=1000
                --ticks=300
                --payload-bytes=48
                --target-cpu=40
                --target-mem-mb=3072
                --arena-max-mb=1024
        DEPENDS swarmnet_test
        COMMENT "Run Week 7 performance benchmark and target gate."
    )
endif()

include(CTest)
if(BUILD_TESTING AND SWARMNET_BUILD_TESTS)
    add_executable(test_determinism_harness tests/unit/test_determinism_harness.cpp)
    target_link_libraries(test_determinism_harness PRIVATE swarmnet::swarmnet)
    target_include_directories(test_determinism_harness PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    swarmnet_enable_warnings(test_determinism_harness)
    swarmnet_enable_sanitizers(test_determinism_harness)

    add_test(NAME determinism_harness COMMAND test_determinism_harness)

    add_executable(test_core_kernel tests/unit/test_core_kernel.cpp)
    target_link_libraries(test_core_kernel PRIVATE swarmnet::swarmnet)
    target_include_directories(test_core_kernel PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    swarmnet_enable_warnings(test_core_kernel)
    swarmnet_enable_sanitizers(test_core_kernel)
    add_test(NAME core_kernel COMMAND test_core_kernel)

    add_executable(test_replay_engine tests/unit/test_replay_engine.cpp)
    target_link_libraries(test_replay_engine PRIVATE swarmnet::swarmnet)
    target_include_directories(test_replay_engine PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    swarmnet_enable_warnings(test_replay_engine)
    swarmnet_enable_sanitizers(test_replay_engine)
    add_test(NAME replay_engine COMMAND test_replay_engine)

    add_executable(test_membership_cluster tests/unit/test_membership_cluster.cpp)
    target_link_libraries(test_membership_cluster PRIVATE swarmnet::swarmnet)
    target_include_directories(test_membership_cluster PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    swarmnet_enable_warnings(test_membership_cluster)
    swarmnet_enable_sanitizers(test_membership_cluster)
    add_test(NAME membership_cluster COMMAND test_membership_cluster)

    if(TARGET example_minimal)
        add_test(NAME example_minimal_smoke COMMAND example_minimal)
    endif()
    if(TARGET swarmnet_test)
        add_test(NAME multiprocess_mesh_smoke
                 COMMAND swarmnet_test --mode=multi --agents=60 --ticks=40 --processes=6 --quiet)
        add_test(NAME membership_cluster_smoke
                 COMMAND swarmnet_test --mode=cluster --agents=100 --bootstrap=3 --fanout=5 --ticks=220 --quiet)
        add_test(NAME benchmark_smoke
                 COMMAND swarmnet_test --mode=bench --agents=120 --ticks=40 --target-cpu=100 --target-mem-mb=4096 --quiet)
    endif()
    if(Python3_FOUND)
        add_test(NAME amalgamation_smoke
                 COMMAND ${CMAKE_COMMAND}
                         -DSWARMNET_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}
                         -DSWARMNET_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
                         -DPYTHON_EXECUTABLE=${Python3_EXECUTABLE}
                         -P ${CMAKE_CURRENT_SOURCE_DIR}/tools/verify_amalgamated.cmake)
    endif()
endif()
