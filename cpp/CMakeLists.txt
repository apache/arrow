# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 2.7)
project(arrow)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

include(CMakeParseArguments)

set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")
set(THIRDPARTY_DIR "${CMAKE_SOURCE_DIR}/thirdparty")

find_package(ClangTools)
if ("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1" OR CLANG_TIDY_FOUND)
  # Generate a Clang compile_commands.json "compilation database" file for use
  # with various development tools, such as Vim's YouCompleteMe plugin.
  # See http://clang.llvm.org/docs/JSONCompilationDatabase.html
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

# ----------------------------------------------------------------------
# cmake options

# Top level cmake dir
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
  option(ARROW_BUILD_STATIC
    "Build the libarrow static libraries"
    ON)

  option(ARROW_BUILD_SHARED
    "Build the libarrow shared libraries"
    ON)

  option(ARROW_PARQUET
    "Build the Parquet adapter and link to libparquet"
    OFF)

  option(ARROW_TEST_MEMCHECK
    "Run the test suite using valgrind --tool=memcheck"
    OFF)

  option(ARROW_BUILD_TESTS
    "Build the Arrow googletest unit tests"
    ON)

  option(ARROW_BUILD_BENCHMARKS
    "Build the Arrow micro benchmarks"
    OFF)

  option(ARROW_IPC
    "Build the Arrow IPC extensions"
    ON)

  option(ARROW_HDFS
    "Build the Arrow IO extensions for the Hadoop file system"
    OFF)

  option(ARROW_BOOST_USE_SHARED
    "Rely on boost shared libraries where relevant"
    ON)

  option(ARROW_SSE3
    "Build Arrow with SSE3"
    ON)

  option(ARROW_ALTIVEC
    "Build Arrow with Altivec"
    ON)

endif()

if(NOT ARROW_BUILD_TESTS)
  set(NO_TESTS 1)
endif()

if(NOT ARROW_BUILD_BENCHMARKS)
  set(NO_BENCHMARKS 1)
endif()


############################################################
# Compiler flags
############################################################

# Check if the target architecture and compiler supports some special
# instruction sets that would boost performance.
include(CheckCXXCompilerFlag)
# x86/amd64 compiler flags
CHECK_CXX_COMPILER_FLAG("-msse3" CXX_SUPPORTS_SSE3)
# power compiler flags
CHECK_CXX_COMPILER_FLAG("-maltivec" CXX_SUPPORTS_ALTIVEC)

# compiler flags that are common across debug/release builds
#  - Wall: Enable all warnings.
set(CXX_COMMON_FLAGS "-std=c++11 -Wall")

# Only enable additional instruction sets if they are supported
if (CXX_SUPPORTS_SSE3 AND ARROW_SSE3)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -msse3")
endif()
if (CXX_SUPPORTS_ALTIVEC AND ARROW_ALTIVEC)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -maltivec")
endif()

if (APPLE)
  # Depending on the default OSX_DEPLOYMENT_TARGET (< 10.9), libstdc++ may be
  # the default standard library which does not support C++11. libc++ is the
  # default from 10.9 onward.
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -stdlib=libc++")
endif()

# compiler flags for different build types (run 'cmake -DCMAKE_BUILD_TYPE=<type> .')
# For all builds:
# For CMAKE_BUILD_TYPE=Debug
#   -ggdb: Enable gdb debugging
# For CMAKE_BUILD_TYPE=FastDebug
#   Same as DEBUG, except with some optimizations on.
# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -g: Enable symbols for profiler tools (TODO: remove for shipping)
set(CXX_FLAGS_DEBUG "-ggdb -O0")
set(CXX_FLAGS_FASTDEBUG "-ggdb -O1")
set(CXX_FLAGS_RELEASE "-O3 -g -DNDEBUG")

set(CXX_FLAGS_PROFILE_GEN "${CXX_FLAGS_RELEASE} -fprofile-generate")
set(CXX_FLAGS_PROFILE_BUILD "${CXX_FLAGS_RELEASE} -fprofile-use")

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif(NOT CMAKE_BUILD_TYPE)

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)


# Set compile flags based on the build type.
message("Configured for ${CMAKE_BUILD_TYPE} build (set with cmake -DCMAKE_BUILD_TYPE={release,debug,...})")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_DEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_FASTDEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_RELEASE}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "PROFILE_GEN")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_PROFILE_GEN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "PROFILE_BUILD")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_PROFILE_BUILD}")
else()
  message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()

# Add common flags
set(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")

# Determine compiler version
include(CompilerInfo)

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  # Using Clang with ccache causes a bunch of spurious warnings that are
  # purportedly fixed in the next version of ccache. See the following for details:
  #
  #   http://petereisentraut.blogspot.com/2011/05/ccache-and-clang.html
  #   http://petereisentraut.blogspot.com/2011/09/ccache-and-clang-part-2.html
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CLANG_OPTIONS}")
endif()

# ASAN / TSAN / UBSAN
include(san-config)

# For any C code, use the same flags.
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS}")

# Code coverage
if ("${ARROW_GENERATE_COVERAGE}")
  if("${CMAKE_CXX_COMPILER}" MATCHES ".*clang.*")
    # There appears to be some bugs in clang 3.3 which cause code coverage
    # to have link errors, not locating the llvm_gcda_* symbols.
    # This should be fixed in llvm 3.4 with http://llvm.org/viewvc/llvm-project?view=revision&revision=184666
    message(SEND_ERROR "Cannot currently generate coverage with clang")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -DCOVERAGE_BUILD")

  # For coverage to work properly, we need to use static linkage. Otherwise,
  # __gcov_flush() doesn't properly flush coverage from every module.
  # See http://stackoverflow.com/questions/28164543/using-gcov-flush-within-a-library-doesnt-force-the-other-modules-to-yield-gc
  if(NOT ARROW_BUILD_STATIC)
    message(SEND_ERROR "Coverage requires the static lib to be built")
  endif()
endif()

# set compile output directory
string (TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)

# If build in-source, create the latest symlink. If build out-of-source, which is
# preferred, simply output the binaries in the build folder
if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/build/${BUILD_SUBDIR_NAME}/")
  # Link build/latest to the current build directory, to avoid developers
  # accidentally running the latest debug build when in fact they're building
  # release builds.
  FILE(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
  if (NOT APPLE)
    set(MORE_ARGS "-T")
  endif()
EXECUTE_PROCESS(COMMAND ln ${MORE_ARGS} -sf ${BUILD_OUTPUT_ROOT_DIRECTORY}
  ${CMAKE_CURRENT_BINARY_DIR}/build/latest)
else()
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}/")
endif()

# where to put generated archives (.a files)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated libraries (.so files)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")
include_directories(src)

############################################################
# Visibility
############################################################
# For generate_export_header() and add_compiler_export_flags().
include(GenerateExportHeader)

# Sets -fvisibility=hidden for gcc
add_compiler_export_flags()

############################################################
# Benchmarking
############################################################
# Add a new micro benchmark, with or without an executable that should be built.
# If benchmarks are enabled then they will be run along side unit tests with ctest.
# 'make runbenchmark' and 'make unittest' to build/run only benchmark or unittests,
# respectively.
#
# REL_BENCHMARK_NAME is the name of the benchmark app. It may be a single component
# (e.g. monotime-benchmark) or contain additional components (e.g.
# net/net_util-benchmark). Either way, the last component must be a globally
# unique name.

# The benchmark will registered as unit test with ctest with a label
# of 'benchmark'.
#
# Arguments after the test name will be passed to set_tests_properties().
function(ADD_ARROW_BENCHMARK REL_BENCHMARK_NAME)
  if(NO_BENCHMARKS)
    return()
  endif()
  get_filename_component(BENCHMARK_NAME ${REL_BENCHMARK_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_BENCHMARK_NAME}.cc)
    # This benchmark has a corresponding .cc file, set it up as an executable.
    set(BENCHMARK_PATH "${EXECUTABLE_OUTPUT_PATH}/${BENCHMARK_NAME}")
    add_executable(${BENCHMARK_NAME} "${REL_BENCHMARK_NAME}.cc")
    target_link_libraries(${BENCHMARK_NAME} ${ARROW_BENCHMARK_LINK_LIBS})
    add_dependencies(runbenchmark ${BENCHMARK_NAME})
    set(NO_COLOR "--color_print=false")
  else()
    # No executable, just invoke the benchmark (probably a script) directly.
    set(BENCHMARK_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${REL_BENCHMARK_NAME})
    set(NO_COLOR "")
  endif()

  add_test(${BENCHMARK_NAME}
    ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} benchmark ${BENCHMARK_PATH} ${NO_COLOR})
  set_tests_properties(${BENCHMARK_NAME} PROPERTIES LABELS "benchmark")
  if(ARGN)
    set_tests_properties(${BENCHMARK_NAME} PROPERTIES ${ARGN})
  endif()
endfunction()

# A wrapper for add_dependencies() that is compatible with NO_BENCHMARKS.
function(ADD_ARROW_BENCHMARK_DEPENDENCIES REL_BENCHMARK_NAME)
  if(NO_BENCHMARKS)
    return()
  endif()
  get_filename_component(BENCMARK_NAME ${REL_BENCHMARK_NAME} NAME_WE)

  add_dependencies(${BENCHMARK_NAME} ${ARGN})
endfunction()


############################################################
# Testing
############################################################
# Add a new test case, with or without an executable that should be built.
#
# REL_TEST_NAME is the name of the test. It may be a single component
# (e.g. monotime-test) or contain additional components (e.g.
# net/net_util-test). Either way, the last component must be a globally
# unique name.
#
# The unit test is added with a label of "unittest" to support filtering with
# ctest.
#
# Arguments after the test name will be passed to set_tests_properties().
function(ADD_ARROW_TEST REL_TEST_NAME)
  if(NO_TESTS OR NOT ARROW_BUILD_STATIC)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME}.cc)
    # This test has a corresponding .cc file, set it up as an executable.
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
    add_executable(${TEST_NAME} "${REL_TEST_NAME}.cc")
    target_link_libraries(${TEST_NAME} ${ARROW_TEST_LINK_LIBS})
    add_dependencies(unittest ${TEST_NAME})
  else()
    # No executable, just invoke the test (probably a script) directly.
    set(TEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME})
  endif()

  if (ARROW_TEST_MEMCHECK)
    SET_PROPERTY(TARGET ${TEST_NAME}
      APPEND_STRING PROPERTY
      COMPILE_FLAGS " -DARROW_VALGRIND")
    add_test(${TEST_NAME}
      valgrind --tool=memcheck --leak-check=full --error-exitcode=1 ${TEST_PATH})
  else()
    add_test(${TEST_NAME}
      ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} test ${TEST_PATH})
  endif()
  set_tests_properties(${TEST_NAME} PROPERTIES LABELS "unittest")

  if(ARGN)
    set_tests_properties(${TEST_NAME} PROPERTIES ${ARGN})
  endif()
endfunction()

# A wrapper for add_dependencies() that is compatible with NO_TESTS.
function(ADD_ARROW_TEST_DEPENDENCIES REL_TEST_NAME)
  if(NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_dependencies(${TEST_NAME} ${ARGN})
endfunction()

# A wrapper for target_link_libraries() that is compatible with NO_TESTS.
function(ARROW_TEST_LINK_LIBRARIES REL_TEST_NAME)
  if(NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  target_link_libraries(${TEST_NAME} ${ARGN})
endfunction()

enable_testing()

############################################################
# Dependencies
############################################################
function(ADD_THIRDPARTY_LIB LIB_NAME)
  set(options)
  set(one_value_args SHARED_LIB STATIC_LIB)
  set(multi_value_args DEPS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(ARG_STATIC_LIB AND ARG_SHARED_LIB)
    if(NOT ARG_STATIC_LIB)
      message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
    endif()

    SET(AUG_LIB_NAME "${LIB_NAME}_static")
    add_library(${AUG_LIB_NAME} STATIC IMPORTED)
    set_target_properties(${AUG_LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    message("Added static library dependency ${LIB_NAME}: ${ARG_STATIC_LIB}")

    SET(AUG_LIB_NAME "${LIB_NAME}_shared")
    add_library(${AUG_LIB_NAME} SHARED IMPORTED)
    set_target_properties(${AUG_LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    message("Added shared library dependency ${LIB_NAME}: ${ARG_SHARED_LIB}")
  elseif(ARG_STATIC_LIB)
    add_library(${LIB_NAME} STATIC IMPORTED)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    message("Added static library dependency ${LIB_NAME}: ${ARG_STATIC_LIB}")
  elseif(ARG_SHARED_LIB)
    add_library(${LIB_NAME} SHARED IMPORTED)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    message("Added shared library dependency ${LIB_NAME}: ${ARG_SHARED_LIB}")
  else()
    message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
  endif()

  if(ARG_DEPS)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${ARG_DEPS}")
  endif()
endfunction()

## GTest
if ("$ENV{GTEST_HOME}" STREQUAL "")
  set(GTest_HOME ${THIRDPARTY_DIR}/googletest-release-1.7.0)
endif()

## Google Benchmark
if ("$ENV{GBENCHMARK_HOME}" STREQUAL "")
  set(GBENCHMARK_HOME ${THIRDPARTY_DIR}/installed)
endif()

# ----------------------------------------------------------------------
# Add Boost dependencies (code adapted from Apache Kudu (incubating))

# find boost headers and libs
set(Boost_DEBUG TRUE)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost COMPONENTS system filesystem REQUIRED)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
set(BOOST_STATIC_LIBS ${Boost_LIBRARIES})
list(LENGTH BOOST_STATIC_LIBS BOOST_STATIC_LIBS_LEN)

# Find Boost shared libraries.
set(Boost_USE_STATIC_LIBS OFF)
find_package(Boost COMPONENTS system filesystem REQUIRED)
set(BOOST_SHARED_LIBS ${Boost_LIBRARIES})
list(LENGTH BOOST_SHARED_LIBS BOOST_SHARED_LIBS_LEN)
list(SORT BOOST_SHARED_LIBS)

message(STATUS "Boost include dir: " ${Boost_INCLUDE_DIRS})
message(STATUS "Boost libraries: " ${Boost_LIBRARIES})

math(EXPR LAST_IDX "${BOOST_STATIC_LIBS_LEN} - 1")
foreach(IDX RANGE ${LAST_IDX})
  list(GET BOOST_STATIC_LIBS ${IDX} BOOST_STATIC_LIB)
  list(GET BOOST_SHARED_LIBS ${IDX} BOOST_SHARED_LIB)

  # Remove the prefix/suffix from the library name.
  #
  # e.g. libboost_system-mt --> boost_system
  get_filename_component(LIB_NAME ${BOOST_STATIC_LIB} NAME_WE)
  string(REGEX REPLACE "lib([^-]*)(-mt)?" "\\1" LIB_NAME_NO_PREFIX_SUFFIX ${LIB_NAME})
  ADD_THIRDPARTY_LIB(${LIB_NAME_NO_PREFIX_SUFFIX}
    STATIC_LIB "${BOOST_STATIC_LIB}"
    SHARED_LIB "${BOOST_SHARED_LIB}")
  list(APPEND ARROW_BOOST_LIBS ${LIB_NAME_NO_PREFIX_SUFFIX})
endforeach()
include_directories(SYSTEM ${Boost_INCLUDE_DIR})

# ----------------------------------------------------------------------
# Enable / disable tests and benchmarks

if(ARROW_BUILD_TESTS)
  add_custom_target(unittest ctest -L unittest)
  find_package(GTest REQUIRED)
  include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(gtest
    STATIC_LIB ${GTEST_STATIC_LIB})
endif()

if(ARROW_BUILD_BENCHMARKS)
  add_custom_target(runbenchmark ctest -L benchmark)
  find_package(GBenchmark REQUIRED)
  include_directories(SYSTEM ${GBENCHMARK_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(benchmark
    STATIC_LIB ${GBENCHMARK_STATIC_LIB})
endif()

## Google PerfTools
##
## Disabled with TSAN/ASAN as well as with gold+dynamic linking (see comment
## near definition of ARROW_USING_GOLD).
# find_package(GPerf REQUIRED)
# if (NOT "${ARROW_USE_ASAN}" AND
#     NOT "${ARROW_USE_TSAN}" AND
#     NOT ("${ARROW_USING_GOLD}" AND "${ARROW_LINK}" STREQUAL "d"))
#   ADD_THIRDPARTY_LIB(tcmalloc
#     STATIC_LIB "${TCMALLOC_STATIC_LIB}"
#     SHARED_LIB "${TCMALLOC_SHARED_LIB}")
#   ADD_THIRDPARTY_LIB(profiler
#     STATIC_LIB "${PROFILER_STATIC_LIB}"
#     SHARED_LIB "${PROFILER_SHARED_LIB}")
#   list(APPEND ARROW_BASE_LIBS tcmalloc profiler)
#   add_definitions("-DTCMALLOC_ENABLED")
#   set(ARROW_TCMALLOC_AVAILABLE 1)
# endif()

############################################################
# Linker setup
############################################################
set(ARROW_MIN_TEST_LIBS
  arrow_static
  arrow_test_main
  ${ARROW_BASE_LIBS})

set(ARROW_TEST_LINK_LIBS ${ARROW_MIN_TEST_LIBS})

set(ARROW_BENCHMARK_LINK_LIBS
  arrow_static
  arrow_benchmark_main
  ${ARROW_BASE_LIBS})

############################################################
# "make ctags" target
############################################################
if (UNIX)
  add_custom_target(ctags ctags -R --languages=c++,c)
endif (UNIX)

############################################################
# "make etags" target
############################################################
if (UNIX)
  add_custom_target(tags etags --members --declarations
  `find ${CMAKE_CURRENT_SOURCE_DIR}/src
   -name \\*.cc -or -name \\*.hh -or -name \\*.cpp -or -name \\*.h -or -name \\*.c -or
   -name \\*.f`)
  add_custom_target(etags DEPENDS tags)
endif (UNIX)

############################################################
# "make cscope" target
############################################################
if (UNIX)
  add_custom_target(cscope find ${CMAKE_CURRENT_SOURCE_DIR}
  ( -name \\*.cc -or -name \\*.hh -or -name \\*.cpp -or
    -name \\*.h -or -name \\*.c -or -name \\*.f )
  -exec echo \"{}\" \; > cscope.files && cscope -q -b VERBATIM)
endif (UNIX)

############################################################
# "make lint" target
############################################################
if (UNIX)

  file(GLOB_RECURSE LINT_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    )

  FOREACH(item ${LINT_FILES})
    IF(NOT (item MATCHES "_generated.h"))
      LIST(APPEND FILTERED_LINT_FILES ${item})
    ENDIF()
  ENDFOREACH(item ${LINT_FILES})

  # Full lint
  add_custom_target(lint ${BUILD_SUPPORT_DIR}/cpplint.py
  --verbose=2
  --linelength=90
  --filter=-whitespace/comments,-readability/todo,-build/header_guard,-build/c++11,-runtime/references
  ${FILTERED_LINT_FILES})
endif (UNIX)


############################################################
# "make format" and "make check-format" targets
############################################################
if (${CLANG_FORMAT_FOUND})
  # runs clang format and updates files in place.
  add_custom_target(format ${BUILD_SUPPORT_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 1
  `find ${CMAKE_CURRENT_SOURCE_DIR}/src -name \\*.cc -or -name \\*.h | sed -e '/_generated/g'`)

  # runs clang format and exits with a non-zero exit code if any files need to be reformatted
  add_custom_target(check-format ${BUILD_SUPPORT_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 0
  `find ${CMAKE_CURRENT_SOURCE_DIR}/src -name \\*.cc -or -name \\*.h | sed -e '/_generated/g'`)
endif()


############################################################
# "make clang-tidy" and "make check-clang-tidy" targets
############################################################
if (${CLANG_TIDY_FOUND})
  # runs clang-tidy and attempts to fix any warning automatically
  add_custom_target(clang-tidy ${BUILD_SUPPORT_DIR}/run-clang-tidy.sh ${CLANG_TIDY_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json 1
  `find ${CMAKE_CURRENT_SOURCE_DIR}/src -name \\*.cc | sed -e '/_generated/g'`)
  # runs clang-tidy and exits with a non-zero exit code if any errors are found.
  add_custom_target(check-clang-tidy ${BUILD_SUPPORT_DIR}/run-clang-tidy.sh ${CLANG_TIDY_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json
  0 `find ${CMAKE_CURRENT_SOURCE_DIR}/src -name \\*.cc |grep -v -F -f ${CMAKE_CURRENT_SOURCE_DIR}/src/.clang-tidy-ignore | sed -e '/_generated/g'`)

endif()



############################################################
# Subdirectories
############################################################

set(ARROW_LINK_LIBS
)

set(ARROW_PRIVATE_LINK_LIBS
)

set(ARROW_SRCS
  src/arrow/array.cc
  src/arrow/builder.cc
  src/arrow/column.cc
  src/arrow/schema.cc
  src/arrow/table.cc
  src/arrow/type.cc

  src/arrow/types/construct.cc
  src/arrow/types/decimal.cc
  src/arrow/types/json.cc
  src/arrow/types/list.cc
  src/arrow/types/primitive.cc
  src/arrow/types/string.cc
  src/arrow/types/struct.cc
  src/arrow/types/union.cc

  src/arrow/util/bit-util.cc
  src/arrow/util/buffer.cc
  src/arrow/util/memory-pool.cc
  src/arrow/util/status.cc
)

add_library(arrow_objlib OBJECT
  ${ARROW_SRCS}
)

# Necessary to make static linking into other shared libraries work properly
set_property(TARGET arrow_objlib PROPERTY POSITION_INDEPENDENT_CODE 1)

if(NOT APPLE)
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(SHARED_LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/src/arrow/symbols.map")
endif()

if (ARROW_BUILD_SHARED)
  add_library(arrow_shared SHARED $<TARGET_OBJECTS:arrow_objlib>)
  if(APPLE)
    set_target_properties(arrow_shared PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
  endif()
  set_target_properties(arrow_shared
    PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
    LINK_FLAGS "${SHARED_LINK_FLAGS}"
    OUTPUT_NAME "arrow")
  target_link_libraries(arrow_shared
    LINK_PUBLIC ${ARROW_LINK_LIBS}
    LINK_PRIVATE ${ARROW_PRIVATE_LINK_LIBS})

  install(TARGETS arrow_shared
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib)
endif()

if (ARROW_BUILD_STATIC)
  add_library(arrow_static STATIC $<TARGET_OBJECTS:arrow_objlib>)
  set_target_properties(arrow_static
    PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
    OUTPUT_NAME "arrow")

  target_link_libraries(arrow_static
    LINK_PUBLIC ${ARROW_LINK_LIBS}
    LINK_PRIVATE ${ARROW_PRIVATE_LINK_LIBS})

  install(TARGETS arrow_static
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib)
endif()

if (APPLE)
  set_target_properties(arrow_shared
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH ON
    INSTALL_NAME_DIR "@rpath")
endif()

add_subdirectory(src/arrow)
add_subdirectory(src/arrow/io)
add_subdirectory(src/arrow/util)
add_subdirectory(src/arrow/types)

#----------------------------------------------------------------------
# Parquet adapter library

if(ARROW_PARQUET)
  find_package(Parquet REQUIRED)
  include_directories(SYSTEM ${PARQUET_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(parquet
    STATIC_LIB ${PARQUET_STATIC_LIB}
    SHARED_LIB ${PARQUET_SHARED_LIB})

  add_subdirectory(src/arrow/parquet)
  list(APPEND LINK_LIBS arrow_parquet parquet)
endif()

#----------------------------------------------------------------------
# IPC library

## Flatbuffers
if(ARROW_IPC)
  find_package(Flatbuffers REQUIRED)
  message(STATUS "Flatbuffers include dir: ${FLATBUFFERS_INCLUDE_DIR}")
  message(STATUS "Flatbuffers static library: ${FLATBUFFERS_STATIC_LIB}")
  message(STATUS "Flatbuffers compiler: ${FLATBUFFERS_COMPILER}")
  include_directories(SYSTEM ${FLATBUFFERS_INCLUDE_DIR})
  add_library(flatbuffers STATIC IMPORTED)
  set_target_properties(flatbuffers PROPERTIES
    IMPORTED_LOCATION ${FLATBUFFERS_STATIC_LIB})

  add_subdirectory(src/arrow/ipc)
endif()
