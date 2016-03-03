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

# Allow "make install" to not depend on all targets.
#
# Must be declared in the top-level CMakeLists.txt.
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

# Generate a Clang compile_commands.json "compilation database" file for use
# with various development tools, such as Vim's YouCompleteMe plugin.
# See http://clang.llvm.org/docs/JSONCompilationDatabase.html
if ("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1")
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

# Enable using a custom GCC toolchain to build Arrow
if (NOT "$ENV{ARROW_GCC_ROOT}" STREQUAL "")
  set(GCC_ROOT $ENV{ARROW_GCC_ROOT})
  set(CMAKE_C_COMPILER ${GCC_ROOT}/bin/gcc)
  set(CMAKE_CXX_COMPILER ${GCC_ROOT}/bin/g++)
endif()

if(APPLE)
  # In newer versions of CMake, this is the default setting
  set(CMAKE_MACOSX_RPATH 1)
endif()

# ----------------------------------------------------------------------
# cmake options

# Top level cmake dir
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
  option(ARROW_WITH_PARQUET
    "Build the Parquet adapter and link to libparquet"
    OFF)

  option(ARROW_BUILD_TESTS
    "Build the Arrow googletest unit tests"
    ON)
endif()

if(NOT ARROW_BUILD_TESTS)
  set(NO_TESTS 1)
endif()


############################################################
# Compiler flags
############################################################

# compiler flags that are common across debug/release builds
#  - Wall: Enable all warnings.
set(CXX_COMMON_FLAGS "-std=c++11 -msse3 -Wall")

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
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_DEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_FASTDEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_RELEASE})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "PROFILE_GEN")
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_PROFILE_GEN})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "PROFILE_BUILD")
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_PROFILE_BUILD})
else()
  message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()

# Add common flags
set(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")

# Required to avoid static linking errors with dependencies
add_definitions(-fPIC)

# Determine compiler version
include(CompilerInfo)

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  # Clang helpfully provides a few extensions from C++11 such as the 'override'
  # keyword on methods. This doesn't change behavior, and we selectively enable
  # it in src/gutil/port.h only on clang. So, we can safely use it, and don't want
  # to trigger warnings when we do so.
  # set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-c++11-extensions")

  # Using Clang with ccache causes a bunch of spurious warnings that are
  # purportedly fixed in the next version of ccache. See the following for details:
  #
  #   http://petereisentraut.blogspot.com/2011/05/ccache-and-clang.html
  #   http://petereisentraut.blogspot.com/2011/09/ccache-and-clang-part-2.html
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments")

  # Only hardcode -fcolor-diagnostics if stderr is opened on a terminal. Otherwise
  # the color codes show up as noisy artifacts.
  #
  # This test is imperfect because 'cmake' and 'make' can be run independently
  # (with different terminal options), and we're testing during the former.
  execute_process(COMMAND test -t 2 RESULT_VARIABLE ARROW_IS_TTY)
  if ((${ARROW_IS_TTY} EQUAL 0) AND (NOT ("$ENV{TERM}" STREQUAL "dumb")))
    message("Running in a controlling terminal")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcolor-diagnostics")
  else()
    message("Running without a controlling terminal or in a dumb terminal")
  endif()
endif()

# Sanity check linking option.
if (NOT ARROW_LINK)
  set(ARROW_LINK "d")
elseif(NOT ("auto" MATCHES "^${ARROW_LINK}" OR
            "dynamic" MATCHES "^${ARROW_LINK}" OR
            "static" MATCHES "^${ARROW_LINK}"))
  message(FATAL_ERROR "Unknown value for ARROW_LINK, must be auto|dynamic|static")
else()
  # Remove all but the first letter.
  string(SUBSTRING "${ARROW_LINK}" 0 1 ARROW_LINK)
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
  if("${ARROW_LINK}" STREQUAL "a")
    message("Using static linking for coverage build")
    set(ARROW_LINK "s")
  elseif("${ARROW_LINK}" STREQUAL "d")
    message(SEND_ERROR "Cannot use coverage with dynamic linking")
  endif()
endif()

# If we still don't know what kind of linking to perform, choose based on
# build type (developers like fast builds).
if ("${ARROW_LINK}" STREQUAL "a")
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR
      "${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
    message("Using dynamic linking for ${CMAKE_BUILD_TYPE} builds")
    set(ARROW_LINK "d")
  else()
    message("Using static linking for ${CMAKE_BUILD_TYPE} builds")
    set(ARROW_LINK "s")
  endif()
endif()

# Are we using the gold linker? It doesn't work with dynamic linking as
# weak symbols aren't properly overridden, causing tcmalloc to be omitted.
# Let's flag this as an error in RELEASE builds (we shouldn't release a
# product like this).
#
# See https://sourceware.org/bugzilla/show_bug.cgi?id=16979 for details.
#
# The gold linker is only for ELF binaries, which OSX doesn't use. We can
# just skip.
if (NOT APPLE)
  execute_process(COMMAND ${CMAKE_CXX_COMPILER} -Wl,--version OUTPUT_VARIABLE LINKER_OUTPUT)
endif ()
if (LINKER_OUTPUT MATCHES "gold")
  if ("${ARROW_LINK}" STREQUAL "d" AND
      "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    message(SEND_ERROR "Cannot use gold with dynamic linking in a RELEASE build "
      "as it would cause tcmalloc symbols to get dropped")
  else()
    message("Using gold linker")
  endif()
  set(ARROW_USING_GOLD 1)
else()
  message("Using ld linker")
endif()

# Having set ARROW_LINK due to build type and/or sanitizer, it's now safe to
# act on its value.
if ("${ARROW_LINK}" STREQUAL "d")
  set(BUILD_SHARED_LIBS ON)

  # Position independent code is only necessary when producing shared objects.
  add_definitions(-fPIC)
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
# Arguments after the test name will be passed to set_tests_properties().
function(ADD_ARROW_TEST REL_TEST_NAME)
  if(NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME}.cc)
    # This test has a corresponding .cc file, set it up as an executable.
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
    add_executable(${TEST_NAME} "${REL_TEST_NAME}.cc")
    target_link_libraries(${TEST_NAME} ${ARROW_TEST_LINK_LIBS})
  else()
    # No executable, just invoke the test (probably a script) directly.
    set(TEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME})
  endif()

  add_test(${TEST_NAME}
    ${BUILD_SUPPORT_DIR}/run-test.sh ${TEST_PATH})
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

  if(("${ARROW_LINK}" STREQUAL "s" AND ARG_STATIC_LIB) OR (NOT ARG_SHARED_LIB))
    if(NOT ARG_STATIC_LIB)
      message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
    endif()
    add_library(${LIB_NAME} STATIC IMPORTED)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    message("Added static library dependency ${LIB_NAME}: ${ARG_STATIC_LIB}")
  else()
    add_library(${LIB_NAME} SHARED IMPORTED)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    message("Added shared library dependency ${LIB_NAME}: ${ARG_SHARED_LIB}")
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
find_package(GTest REQUIRED)
include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(gtest
  STATIC_LIB ${GTEST_STATIC_LIB})

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
set(ARROW_MIN_TEST_LIBS arrow arrow_test_main arrow_test_util ${ARROW_BASE_LIBS})
set(ARROW_TEST_LINK_LIBS ${ARROW_MIN_TEST_LIBS})

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
  # Full lint
  add_custom_target(lint ${BUILD_SUPPORT_DIR}/cpplint.py
  --verbose=2
  --linelength=90
  --filter=-whitespace/comments,-readability/todo,-build/header_guard,-build/c++11
    `find ${CMAKE_CURRENT_SOURCE_DIR}/src -name \\*.cc -or -name \\*.h`)
endif (UNIX)

#----------------------------------------------------------------------
# Parquet adapter

if(ARROW_WITH_PARQUET)
  find_package(Parquet REQUIRED)
  include_directories(SYSTEM ${PARQUET_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(parquet
    STATIC_LIB ${PARQUET_STATIC_LIB}
    SHARED_LIB ${PARQUET_SHARED_LIB})

  add_subdirectory(src/arrow/parquet)
  list(APPEND LINK_LIBS arrow_parquet parquet)
endif()

############################################################
# Subdirectories
############################################################

add_subdirectory(src/arrow)
add_subdirectory(src/arrow/util)
add_subdirectory(src/arrow/types)

set(LINK_LIBS
  arrow_util
  arrow_types)

set(ARROW_SRCS
  src/arrow/array.cc
  src/arrow/builder.cc
  src/arrow/field.cc
  src/arrow/schema.cc
  src/arrow/type.cc
)

set(LIBARROW_LINKAGE "SHARED")

add_library(arrow
  ${LIBARROW_LINKAGE}
  ${ARROW_SRCS}
)
target_link_libraries(arrow ${LINK_LIBS})
set_target_properties(arrow PROPERTIES LINKER_LANGUAGE CXX)

install(TARGETS arrow
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib)
