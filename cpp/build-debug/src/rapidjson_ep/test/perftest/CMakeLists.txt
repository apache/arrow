set(PERFTEST_SOURCES
    misctest.cpp
    perftest.cpp
    platformtest.cpp
    rapidjsontest.cpp
    schematest.cpp)

add_executable(perftest ${PERFTEST_SOURCES})
target_link_libraries(perftest ${TEST_LIBRARIES})

add_dependencies(tests perftest)

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments -fcolor-diagnostics")
    endif()
endif(CCACHE_FOUND)

set_property(DIRECTORY PROPERTY COMPILE_OPTIONS ${EXTRA_CXX_FLAGS})

IF(NOT (CMAKE_BUILD_TYPE STREQUAL "Debug"))
add_test(NAME perftest
    COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/perftest
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)
ENDIF()
