include(CheckCXXCompilerFlag)

set(UNITTEST_SOURCES
	allocatorstest.cpp
    bigintegertest.cpp
    clzlltest.cpp
	cursorstreamwrappertest.cpp
    documenttest.cpp
    dtoatest.cpp
    encodedstreamtest.cpp
    encodingstest.cpp
    fwdtest.cpp
    filestreamtest.cpp
    itoatest.cpp
    istreamwrappertest.cpp
    jsoncheckertest.cpp
    namespacetest.cpp
    pointertest.cpp
    platformtest.cpp
    prettywritertest.cpp
    ostreamwrappertest.cpp
    readertest.cpp
    regextest.cpp
	schematest.cpp
	simdtest.cpp
    strfunctest.cpp
    stringbuffertest.cpp
    strtodtest.cpp
    unittest.cpp
    uritest.cpp
    valuetest.cpp
    writertest.cpp)

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments -fcolor-diagnostics")
		endif()
endif(CCACHE_FOUND)

set_property(DIRECTORY PROPERTY COMPILE_OPTIONS ${EXTRA_CXX_FLAGS})

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # If the user is running a newer version of Clang that includes the
    # -Wdouble-promotion, we will ignore that warning.
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 3.7)
        CHECK_CXX_COMPILER_FLAG("-Wno-double-promotion" HAS_NO_DOUBLE_PROMOTION)
        if (HAS_NO_DOUBLE_PROMOTION)
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-double-promotion")
        endif()
    endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    # Force to always compile with /W4
    if(CMAKE_CXX_FLAGS MATCHES "/W[0-4]")
        string(REGEX REPLACE "/W[0-4]" "/W4" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W4")
    endif()

    # Force to always compile with /WX
    if(CMAKE_CXX_FLAGS MATCHES "/WX-")
        string(REGEX REPLACE "/WX-" "/WX" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /WX")
    endif()
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DRAPIDJSON_HAS_STDSTRING=1")

add_library(namespacetest STATIC namespacetest.cpp)

add_executable(unittest ${UNITTEST_SOURCES})
target_link_libraries(unittest ${TEST_LIBRARIES} namespacetest)

add_dependencies(tests unittest)

add_test(NAME unittest
    COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

if(NOT MSVC)
    # Not running SIMD.* unit test cases for Valgrind
    add_test(NAME valgrind_unittest
        COMMAND valgrind --suppressions=${CMAKE_SOURCE_DIR}/test/valgrind.supp --leak-check=full --error-exitcode=1 ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest --gtest_filter=-SIMD.*
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        add_test(NAME symbol_check
        COMMAND sh -c "objdump -t -C libnamespacetest.a | grep rapidjson ; test $? -ne 0"
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    endif(CMAKE_BUILD_TYPE STREQUAL "Debug")

endif(NOT MSVC)
