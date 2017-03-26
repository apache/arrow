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

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/../java/pom.xml" POM_XML)
string(REGEX MATCHALL
  "\n  <version>[^<]+</version>" ARROW_VERSION_TAG "${POM_XML}")
string(REGEX REPLACE
  "(\n  <version>|</version>)" "" ARROW_VERSION "${ARROW_VERSION_TAG}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

include(CMakeParseArguments)
include(ExternalProject)

if(CMAKE_MAJOR_VERSION LESS 3)
  set(CMAKE_INSTALL_INCLUDEDIR "include")
  set(CMAKE_INSTALL_LIBDIR "lib")
else()
  include(GNUInstallDirs)
endif()

set(ARROW_SO_VERSION "0")
set(ARROW_ABI_VERSION "${ARROW_SO_VERSION}.0.0")

set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")
set(THIRDPARTY_DIR "${CMAKE_SOURCE_DIR}/thirdparty")

set(GFLAGS_VERSION "2.1.2")
set(GTEST_VERSION "1.7.0")
set(GBENCHMARK_VERSION "1.1.0")
set(FLATBUFFERS_VERSION "1.6.0")
set(JEMALLOC_VERSION "4.4.0")

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
  set(ARROW_CXXFLAGS "" CACHE STRING
    "Compiler flags to append when compiling Arrow")

  option(ARROW_BUILD_STATIC
    "Build the libarrow static libraries"
    ON)

  option(ARROW_BUILD_SHARED
    "Build the libarrow shared libraries"
    ON)

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

  option(ARROW_JEMALLOC
    "Build the Arrow jemalloc-based allocator"
    ON)

  option(ARROW_JEMALLOC_USE_SHARED
    "Rely on jemalloc shared libraries where relevant"
    ON)

  option(ARROW_BOOST_USE_SHARED
    "Rely on boost shared libraries where relevant"
    ON)

  option(ARROW_PYTHON
    "Build the Arrow CPython extensions"
    OFF)

  option(ARROW_SSE3
    "Build Arrow with SSE3"
    ON)

  option(ARROW_ALTIVEC
    "Build Arrow with Altivec"
    ON)

  option(ARROW_BUILD_UTILITIES
    "Build Arrow commandline utilities"
    ON)

  option(ARROW_RPATH_ORIGIN
    "Build Arrow libraries with RATH set to \$ORIGIN"
    OFF)
endif()

if(ARROW_BUILD_TESTS)
  set(ARROW_BUILD_STATIC ON)
else()
  set(NO_TESTS 1)
endif()

if(NOT ARROW_BUILD_BENCHMARKS)
  set(NO_BENCHMARKS 1)
endif()

include(BuildUtils)

############################################################
# Compiler flags
############################################################

include(SetupCxxFlags)

# Add common flags
set(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")
set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${ARROW_CXXFLAGS} ${CMAKE_CXX_FLAGS}")

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

# A wrapper for target_link_libraries() that is compatible with NO_BENCHMARKS.
function(ARROW_BENCHMARK_LINK_LIBRARIES REL_BENCHMARK_NAME)
    if(NO_BENCHMARKS)
    return()
  endif()
  get_filename_component(BENCHMARK_NAME ${REL_BENCHMARK_NAME} NAME_WE)

  target_link_libraries(${BENCHMARK_NAME} ${ARGN})
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
  set(options)
  set(single_value_args)
  set(multi_value_args STATIC_LINK_LIBS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(NO_TESTS OR NOT ARROW_BUILD_STATIC)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME}.cc)
    # This test has a corresponding .cc file, set it up as an executable.
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
    add_executable(${TEST_NAME} "${REL_TEST_NAME}.cc")

    if (ARG_STATIC_LINK_LIBS)
      # Customize link libraries
      target_link_libraries(${TEST_NAME} ${ARG_STATIC_LINK_LIBS})
    else()
      target_link_libraries(${TEST_NAME} ${ARROW_TEST_LINK_LIBS})
    endif()
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
  elseif(MSVC)
    add_test(${TEST_NAME} ${TEST_PATH})
  else()
    add_test(${TEST_NAME}
      ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} test ${TEST_PATH})
  endif()
  set_tests_properties(${TEST_NAME} PROPERTIES LABELS "unittest")
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

# ----------------------------------------------------------------------
# Add Boost dependencies (code adapted from Apache Kudu (incubating))

set(Boost_DEBUG TRUE)
set(Boost_USE_MULTITHREADED ON)
if (ARROW_BOOST_USE_SHARED)
  # Find shared Boost libraries.
  set(Boost_USE_STATIC_LIBS OFF)

  if(MSVC)
    # disable autolinking in boost
    add_definitions(-DBOOST_ALL_NO_LIB)

    # force all boost libraries to dynamic link
    add_definitions(-DBOOST_ALL_DYN_LINK)
  endif()

  find_package(Boost COMPONENTS system filesystem REQUIRED)
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(BOOST_SHARED_SYSTEM_LIBRARY ${Boost_SYSTEM_LIBRARY_DEBUG})
    set(BOOST_SHARED_FILESYSTEM_LIBRARY ${Boost_FILESYSTEM_LIBRARY_DEBUG})
  else()
    set(BOOST_SHARED_SYSTEM_LIBRARY ${Boost_SYSTEM_LIBRARY_RELEASE})
    set(BOOST_SHARED_FILESYSTEM_LIBRARY ${Boost_FILESYSTEM_LIBRARY_RELEASE})
  endif()
  set(BOOST_SYSTEM_LIBRARY boost_system_shared)
  set(BOOST_FILESYSTEM_LIBRARY boost_filesystem_shared)
else()
  # Find static boost headers and libs
  # TODO Differentiate here between release and debug builds
  set(Boost_USE_STATIC_LIBS ON)
  find_package(Boost COMPONENTS system filesystem REQUIRED)
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(BOOST_STATIC_SYSTEM_LIBRARY ${Boost_SYSTEM_LIBRARY_DEBUG})
    set(BOOST_STATIC_FILESYSTEM_LIBRARY ${Boost_FILESYSTEM_LIBRARY_DEBUG})
  else()
    set(BOOST_STATIC_SYSTEM_LIBRARY ${Boost_SYSTEM_LIBRARY_RELEASE})
    set(BOOST_STATIC_FILESYSTEM_LIBRARY ${Boost_FILESYSTEM_LIBRARY_RELEASE})
  endif()
  set(BOOST_SYSTEM_LIBRARY boost_system_static)
  set(BOOST_FILESYSTEM_LIBRARY boost_filesystem_static)
endif()

message(STATUS "Boost include dir: " ${Boost_INCLUDE_DIRS})
message(STATUS "Boost libraries: " ${Boost_LIBRARIES})

ADD_THIRDPARTY_LIB(boost_system
    STATIC_LIB "${BOOST_STATIC_SYSTEM_LIBRARY}"
    SHARED_LIB "${BOOST_SHARED_SYSTEM_LIBRARY}")

ADD_THIRDPARTY_LIB(boost_filesystem
    STATIC_LIB "${BOOST_STATIC_FILESYSTEM_LIBRARY}"
    SHARED_LIB "${BOOST_SHARED_FILESYSTEM_LIBRARY}")

SET(ARROW_BOOST_LIBS boost_system boost_filesystem)

include_directories(SYSTEM ${Boost_INCLUDE_DIR})

# ----------------------------------------------------------------------
# Enable / disable tests and benchmarks

if(ARROW_BUILD_TESTS)
  add_custom_target(unittest ctest -L unittest)

  if("$ENV{GTEST_HOME}" STREQUAL "")
    if(APPLE)
      set(GTEST_CMAKE_CXX_FLAGS "-fPIC -DGTEST_USE_OWN_TR1_TUPLE=1 -Wno-unused-value -Wno-ignored-attributes")
    else()
      set(GTEST_CMAKE_CXX_FLAGS "-fPIC")
    endif()
    string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)
    set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}} ${GTEST_CMAKE_CXX_FLAGS}")

    set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
    set(GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")
    set(GTEST_STATIC_LIB "${GTEST_PREFIX}/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GTEST_VENDORED 1)

    if (CMAKE_VERSION VERSION_GREATER "3.2")
      # BUILD_BYPRODUCTS is a 3.2+ feature
      ExternalProject_Add(googletest_ep
        URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
        CMAKE_ARGS -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS} -Dgtest_force_shared_crt=ON
        # googletest doesn't define install rules, so just build in the
        # source dir and don't try to install.  See its README for
        # details.
        BUILD_IN_SOURCE 1
        BUILD_BYPRODUCTS "${GTEST_STATIC_LIB}"
        INSTALL_COMMAND "")
    else()
      ExternalProject_Add(googletest_ep
        URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
        CMAKE_ARGS -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS} -Dgtest_force_shared_crt=ON
        # googletest doesn't define install rules, so just build in the
        # source dir and don't try to install.  See its README for
        # details.
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND "")
    endif()
  else()
    find_package(GTest REQUIRED)
    set(GTEST_VENDORED 0)
  endif()

  message(STATUS "GTest include dir: ${GTEST_INCLUDE_DIR}")
  message(STATUS "GTest static library: ${GTEST_STATIC_LIB}")
  include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(gtest
    STATIC_LIB ${GTEST_STATIC_LIB})

  if(GTEST_VENDORED)
    add_dependencies(gtest googletest_ep)
  endif()

  # gflags (formerly Googleflags) command line parsing
  if("$ENV{GFLAGS_HOME}" STREQUAL "")
    set(GFLAGS_CMAKE_CXX_FLAGS ${EP_CXX_FLAGS})

    set(GFLAGS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gflags_ep-prefix/src/gflags_ep")
    set(GFLAGS_HOME "${GFLAGS_PREFIX}")
    set(GFLAGS_INCLUDE_DIR "${GFLAGS_PREFIX}/include")
    set(GFLAGS_STATIC_LIB "${GFLAGS_PREFIX}/lib/libgflags.a")
    set(GFLAGS_VENDORED 1)
    set(GFLAGS_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                          -DCMAKE_INSTALL_PREFIX=${GFLAGS_PREFIX}
                          -DBUILD_SHARED_LIBS=OFF
                          -DBUILD_STATIC_LIBS=ON
                          -DBUILD_PACKAGING=OFF
                          -DBUILD_TESTING=OFF
                          -BUILD_CONFIG_TESTS=OFF
                          -DINSTALL_HEADERS=ON
                          -DCMAKE_CXX_FLAGS=${GFLAGS_CMAKE_CXX_FLAGS})
    if (CMAKE_VERSION VERSION_GREATER "3.2")
      # BUILD_BYPRODUCTS is a 3.2+ feature
      ExternalProject_Add(gflags_ep
        GIT_REPOSITORY https://github.com/gflags/gflags.git
        GIT_TAG cce68f0c9c5d054017425e6e6fd54f696d36e8ee
        BUILD_IN_SOURCE 1
        BUILD_BYPRODUCTS "${GFLAGS_STATIC_LIB}"
        CMAKE_ARGS ${GFLAGS_CMAKE_ARGS})
    else()
      ExternalProject_Add(gflags_ep
        GIT_REPOSITORY https://github.com/gflags/gflags.git
        GIT_TAG cce68f0c9c5d054017425e6e6fd54f696d36e8ee
        BUILD_IN_SOURCE 1
        CMAKE_ARGS ${GFLAGS_CMAKE_ARGS})
    endif()
  else()
    set(GFLAGS_VENDORED 0)
    find_package(GFlags REQUIRED)
  endif()

  message(STATUS "GFlags include dir: ${GFLAGS_INCLUDE_DIR}")
  message(STATUS "GFlags static library: ${GFLAGS_STATIC_LIB}")
  include_directories(SYSTEM ${GFLAGS_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(gflags
    STATIC_LIB ${GFLAGS_STATIC_LIB})

  if(GFLAGS_VENDORED)
    add_dependencies(gflags gflags_ep)
  endif()
endif()

if(ARROW_BUILD_BENCHMARKS)
  add_custom_target(runbenchmark ctest -L benchmark)

  if("$ENV{GBENCHMARK_HOME}" STREQUAL "")
    if(APPLE)
      set(GBENCHMARK_CMAKE_CXX_FLAGS "-std=c++11 -stdlib=libc++")
    else()
      set(GBENCHMARK_CMAKE_CXX_FLAGS "--std=c++11")
    endif()

    set(GBENCHMARK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gbenchmark_ep/src/gbenchmark_ep-install")
    set(GBENCHMARK_INCLUDE_DIR "${GBENCHMARK_PREFIX}/include")
    set(GBENCHMARK_STATIC_LIB "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GBENCHMARK_VENDORED 1)
    set(GBENCHMARK_CMAKE_ARGS
          "-DCMAKE_BUILD_TYPE=Release"
          "-DCMAKE_INSTALL_PREFIX:PATH=${GBENCHMARK_PREFIX}"
          "-DBENCHMARK_ENABLE_TESTING=OFF"
          "-DCMAKE_CXX_FLAGS=-fPIC ${GBENCHMARK_CMAKE_CXX_FLAGS}")
    if (APPLE)
      set(GBENCHMARK_CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS} "-DBENCHMARK_USE_LIBCXX=ON")
    endif()
    if (CMAKE_VERSION VERSION_GREATER "3.2")
      # BUILD_BYPRODUCTS is a 3.2+ feature
      ExternalProject_Add(gbenchmark_ep
        URL "https://github.com/google/benchmark/archive/v${GBENCHMARK_VERSION}.tar.gz"
        BUILD_BYPRODUCTS "${GBENCHMARK_STATIC_LIB}"
        CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS})
    else()
      ExternalProject_Add(gbenchmark_ep
        URL "https://github.com/google/benchmark/archive/v${GBENCHMARK_VERSION}.tar.gz"
        CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS})
    endif()
  else()
    find_package(GBenchmark REQUIRED)
    set(GBENCHMARK_VENDORED 0)
  endif()

  message(STATUS "GBenchmark include dir: ${GBENCHMARK_INCLUDE_DIR}")
  message(STATUS "GBenchmark static library: ${GBENCHMARK_STATIC_LIB}")
  include_directories(SYSTEM ${GBENCHMARK_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(benchmark
    STATIC_LIB ${GBENCHMARK_STATIC_LIB})

  if(GBENCHMARK_VENDORED)
    add_dependencies(benchmark gbenchmark_ep)
  endif()
endif()

# RapidJSON, header only dependency
if("$ENV{RAPIDJSON_HOME}" STREQUAL "")
  ExternalProject_Add(rapidjson_ep
    PREFIX "${CMAKE_BINARY_DIR}"
    URL "https://github.com/miloyip/rapidjson/archive/v1.1.0.tar.gz"
    URL_MD5 "badd12c511e081fec6c89c43a7027bce"
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND "")

  ExternalProject_Get_Property(rapidjson_ep SOURCE_DIR)
  set(RAPIDJSON_INCLUDE_DIR "${SOURCE_DIR}/include")
  set(RAPIDJSON_VENDORED 1)
else()
  set(RAPIDJSON_INCLUDE_DIR "$ENV{RAPIDJSON_HOME}/include")
  set(RAPIDJSON_VENDORED 0)
endif()
message(STATUS "RapidJSON include dir: ${RAPIDJSON_INCLUDE_DIR}")
include_directories(SYSTEM ${RAPIDJSON_INCLUDE_DIR})

if (ARROW_JEMALLOC)
  find_package(jemalloc)

  if(NOT JEMALLOC_FOUND)
    set(JEMALLOC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/jemalloc_ep-prefix/src/jemalloc_ep/dist/")
    set(JEMALLOC_HOME "${JEMALLOC_PREFIX}")
    set(JEMALLOC_INCLUDE_DIR "${JEMALLOC_PREFIX}/include")
    set(JEMALLOC_SHARED_LIB "${JEMALLOC_PREFIX}/lib/libjemalloc${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(JEMALLOC_STATIC_LIB "${JEMALLOC_PREFIX}/lib/libjemalloc_pic${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(JEMALLOC_VENDORED 1)
    if (CMAKE_VERSION VERSION_GREATER "3.2")
      # BUILD_BYPRODUCTS is a 3.2+ feature
      ExternalProject_Add(jemalloc_ep
        URL https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_VERSION}/jemalloc-${JEMALLOC_VERSION}.tar.bz2
        CONFIGURE_COMMAND ./configure "--prefix=${JEMALLOC_PREFIX}" "--with-jemalloc-prefix="
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ${MAKE}
        BUILD_BYPRODUCTS "${JEMALLOC_STATIC_LIB}" "${JEMALLOC_SHARED_LIB}")
    else()
      ExternalProject_Add(jemalloc_ep
        URL https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_VERSION}/jemalloc-${JEMALLOC_VERSION}.tar.bz2
        CONFIGURE_COMMAND ./configure "--prefix=${JEMALLOC_PREFIX}" "--with-jemalloc-prefix="
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ${MAKE})
    endif()
  else()
    set(JEMALLOC_VENDORED 0)
  endif()

  include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(jemalloc
      STATIC_LIB ${JEMALLOC_STATIC_LIB}
      SHARED_LIB ${JEMALLOC_SHARED_LIB})

  if (JEMALLOC_VENDORED)
    add_dependencies(jemalloc_shared jemalloc_ep)
    add_dependencies(jemalloc_static jemalloc_ep)
  endif()
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
  --filter=-whitespace/comments,-readability/todo,-build/header_guard,-build/c++11,-runtime/references,-build/include_order
  ${FILTERED_LINT_FILES})
endif (UNIX)


############################################################
# "make format" and "make check-format" targets
############################################################
if (${CLANG_FORMAT_FOUND})
  # runs clang format and updates files in place.
  add_custom_target(format ${BUILD_SUPPORT_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 1
    `find ${CMAKE_CURRENT_SOURCE_DIR}/src -name \\*.cc -or -name \\*.h |
    sed -e '/_generated/g' |
    sed -e '/windows_compatibility.h/g'`)

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
  src/arrow/buffer.cc
  src/arrow/builder.cc
  src/arrow/column.cc
  src/arrow/compare.cc
  src/arrow/loader.cc
  src/arrow/memory_pool.cc
  src/arrow/pretty_print.cc
  src/arrow/schema.cc
  src/arrow/status.cc
  src/arrow/table.cc
  src/arrow/type.cc
  src/arrow/visitor.cc

  src/arrow/util/bit-util.cc
)

if(NOT APPLE)
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(ARROW_SHARED_LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/src/arrow/symbols.map")
endif()

ADD_ARROW_LIB(arrow
  SOURCES ${ARROW_SRCS}
  SHARED_LINK_FLAGS ${ARROW_SHARED_LINK_FLAGS}
)

add_subdirectory(src/arrow)
add_subdirectory(src/arrow/io)
add_subdirectory(src/arrow/util)

if(ARROW_JEMALLOC)
  add_subdirectory(src/arrow/jemalloc)
endif()

#----------------------------------------------------------------------
# IPC library

if(ARROW_PYTHON)
  set(ARROW_IPC on)
endif()

## Flatbuffers
if(ARROW_IPC)
  if("$ENV{FLATBUFFERS_HOME}" STREQUAL "")
    set(FLATBUFFERS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/flatbuffers_ep-prefix/src/flatbuffers_ep-install")
    ExternalProject_Add(flatbuffers_ep
      URL "https://github.com/google/flatbuffers/archive/v${FLATBUFFERS_VERSION}.tar.gz"
      CMAKE_ARGS
        "-DCMAKE_CXX_FLAGS=-fPIC"
        "-DCMAKE_INSTALL_PREFIX:PATH=${FLATBUFFERS_PREFIX}"
        "-DFLATBUFFERS_BUILD_TESTS=OFF")

    set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_PREFIX}/include")
    set(FLATBUFFERS_STATIC_LIB "${FLATBUFFERS_PREFIX}/libflatbuffers.a")
    set(FLATBUFFERS_COMPILER "${FLATBUFFERS_PREFIX}/bin/flatc")
    set(FLATBUFFERS_VENDORED 1)
  else()
    find_package(Flatbuffers REQUIRED)
    set(FLATBUFFERS_VENDORED 0)
  endif()

  message(STATUS "Flatbuffers include dir: ${FLATBUFFERS_INCLUDE_DIR}")
  message(STATUS "Flatbuffers static library: ${FLATBUFFERS_STATIC_LIB}")
  message(STATUS "Flatbuffers compiler: ${FLATBUFFERS_COMPILER}")
  include_directories(SYSTEM ${FLATBUFFERS_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(flatbuffers
    STATIC_LIB ${FLATBUFFERS_STATIC_LIB})

  if(FLATBUFFERS_VENDORED)
    add_dependencies(flatbuffers flatbuffers_ep)
  endif()

  add_subdirectory(src/arrow/ipc)
endif()

if(ARROW_PYTHON)
  find_package(PythonLibsNew REQUIRED)
  find_package(NumPy REQUIRED)

  include_directories(SYSTEM
    ${NUMPY_INCLUDE_DIRS}
    ${PYTHON_INCLUDE_DIRS})

  add_subdirectory(src/arrow/python)
endif()
