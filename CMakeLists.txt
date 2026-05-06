cmake_minimum_required(VERSION 3.22)

# Heartbeat handler — Apache 2.0 (real implementation, no upstream
# analogue per LICENSE convention).
#
# OBJECT library carries the C++ class for in-tree tests; the
# `goodnet_handler_heartbeat.so` plugin entry registers the handler
# + the `gn.heartbeat` extension through the host_api. Standalone:
# `find_package(GoodNet REQUIRED)` brings the SDK + `add_plugin()`
# helper from the installed kernel package.

if(NOT TARGET GoodNet::sdk)
    project(goodnet_handler_heartbeat
        VERSION   0.1.0
        LANGUAGES CXX)
    find_package(GoodNet REQUIRED)
    goodnet_standalone_plugin_includes()
    include(CTest)
    if(BUILD_TESTING)
        find_package(GTest      REQUIRED)
        find_package(rapidcheck QUIET)
        goodnet_patch_rapidcheck_targets()
        set(GOODNET_BUILD_TESTS ON)
    endif()
endif()

add_library(goodnet_heartbeat_objects OBJECT
    heartbeat.cpp
)
target_link_libraries(goodnet_heartbeat_objects PUBLIC GoodNet::sdk)
target_compile_features(goodnet_heartbeat_objects PUBLIC cxx_std_23)
target_compile_options(goodnet_heartbeat_objects PRIVATE
    -Wall -Wextra -Wpedantic -Wno-unused-parameter)
target_include_directories(goodnet_heartbeat_objects PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(goodnet_heartbeat_objects PROPERTIES
    POSITION_INDEPENDENT_CODE ON)

add_plugin(goodnet_handler_heartbeat
    plugin_entry.cpp
)
target_link_libraries(goodnet_handler_heartbeat
    PRIVATE goodnet_heartbeat_objects)

if(GOODNET_BUILD_TESTS)
    add_subdirectory(tests)
endif()
