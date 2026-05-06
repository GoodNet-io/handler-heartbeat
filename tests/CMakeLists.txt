# Heartbeat handler unit tests — Apache 2.0.
include(GoogleTest)

add_executable(test_heartbeat test_heartbeat.cpp)
target_link_libraries(test_heartbeat PRIVATE
    GoodNet::sdk
    goodnet_heartbeat_objects
    GTest::gtest
    GTest::gtest_main)
target_compile_features(test_heartbeat PRIVATE cxx_std_23)
if(COMMAND goodnet_apply_pch)
    goodnet_apply_pch(test_heartbeat)
endif()
gtest_discover_tests(test_heartbeat
    DISCOVERY_TIMEOUT 30
    PROPERTIES TIMEOUT 60)
