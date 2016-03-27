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
#
# Includes code assembled from BSD/MIT/Apache-licensed code from some 3rd-party
# projects, including Kudu, Impala, and libdynd. See python/LICENSE.txt

cmake_minimum_required(VERSION 2.7)
project(pyarrow)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

# Use common cmake modules from Arrow C++ if available
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/../cpp/cmake_modules")

include(CMakeParseArguments)

set(BUILD_SUPPORT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../cpp/build-support)

# Allow "make install" to not depend on all targets.
#
# Must be declared in the top-level CMakeLists.txt.
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

set(CMAKE_MACOSX_RPATH 1)
set(CMAKE_OSX_DEPLOYMENT_TARGET 10.9)

# Generate a Clang compile_commands.json "compilation database" file for use
# with various development tools, such as Vim's YouCompleteMe plugin.
# See http://clang.llvm.org/docs/JSONCompilationDatabase.html
if ("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1")
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

# Top level cmake dir
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
  option(PYARROW_BUILD_TESTS
    "Build the PyArrow C++ googletest unit tests"
    OFF)
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

############################################################
# Compiler flags
############################################################

# compiler flags that are common across debug/release builds
set(CXX_COMMON_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")

# compiler flags for different build types (run 'cmake -DCMAKE_BUILD_TYPE=<type> .')
# For all builds:
# For CMAKE_BUILD_TYPE=Debug
#   -ggdb: Enable gdb debugging
# For CMAKE_BUILD_TYPE=FastDebug
#   Same as DEBUG, except with some optimizations on.
# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -g: Enable symbols for profiler tools (TODO: remove for shipping)
#   -DNDEBUG: Turn off dchecks/asserts/debug only code.
set(CXX_FLAGS_DEBUG "-ggdb -O0")
set(CXX_FLAGS_FASTDEBUG "-ggdb -O1")
set(CXX_FLAGS_RELEASE "-O3 -g -DNDEBUG")

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
endif()

set(PYARROW_LINK "a")

# For any C code, use the same flags.
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS}")

# Code coverage
if ("${PYARROW_GENERATE_COVERAGE}")
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
  if("${PYARROW_LINK}" STREQUAL "a")
    message("Using static linking for coverage build")
    set(PYARROW_LINK "s")
  elseif("${PYARROW_LINK}" STREQUAL "d")
    message(SEND_ERROR "Cannot use coverage with static linking")
  endif()
endif()

# If we still don't know what kind of linking to perform, choose based on
# build type (developers like fast builds).
if ("${PYARROW_LINK}" STREQUAL "a")
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR
      "${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
    message("Using dynamic linking for ${CMAKE_BUILD_TYPE} builds")
    set(PYARROW_LINK "d")
  else()
    message("Using static linking for ${CMAKE_BUILD_TYPE} builds")
    set(PYARROW_LINK "s")
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
  if ("${PYARROW_LINK}" STREQUAL "d" AND
      "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    message(SEND_ERROR "Cannot use gold with dynamic linking in a RELEASE build "
      "as it would cause tcmalloc symbols to get dropped")
  else()
    message("Using gold linker")
  endif()
  set(PYARROW_USING_GOLD 1)
else()
  message("Using ld linker")
endif()

# Having set PYARROW_LINK due to build type and/or sanitizer, it's now safe to
# act on its value.
if ("${PYARROW_LINK}" STREQUAL "d")
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
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
  # set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}/")
endif()

# where to put generated archives (.a files)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated libraries (.so files)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")

## Python and libraries
find_package(PythonLibsNew REQUIRED)
find_package(NumPy REQUIRED)
include(UseCython)

include_directories(SYSTEM
  ${NUMPY_INCLUDE_DIRS}
  ${PYTHON_INCLUDE_DIRS}
  src)

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
function(ADD_PYARROW_TEST REL_TEST_NAME)
  if(NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME}.cc)
    # This test has a corresponding .cc file, set it up as an executable.
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
    add_executable(${TEST_NAME} "${REL_TEST_NAME}.cc")
    target_link_libraries(${TEST_NAME} ${PYARROW_TEST_LINK_LIBS})
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
function(ADD_PYARROW_TEST_DEPENDENCIES REL_TEST_NAME)
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

  if(("${PYARROW_LINK}" STREQUAL "s" AND ARG_STATIC_LIB) OR (NOT ARG_SHARED_LIB))
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

  # Set up an "exported variant" for this thirdparty library (see "Visibility"
  # above). It's the same as the real target, just with an "_exported" suffix.
  # We prefer the static archive if it exists (as it's akin to an "internal"
  # library), but we'll settle for the shared object if we must.
  #
  # A shared object exported variant will force any "leaf" library that
  # transitively depends on it to also depend on it at runtime; this is
  # desirable for some libraries (e.g. cyrus_sasl).
  set(LIB_NAME_EXPORTED ${LIB_NAME}_exported)
  if(ARG_STATIC_LIB)
    add_library(${LIB_NAME_EXPORTED} STATIC IMPORTED)
    set_target_properties(${LIB_NAME_EXPORTED}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
  else()
    add_library(${LIB_NAME_EXPORTED} SHARED IMPORTED)
    set_target_properties(${LIB_NAME_EXPORTED}
      PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
  endif()
  if(ARG_DEPS)
    set_target_properties(${LIB_NAME_EXPORTED}
      PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${ARG_DEPS}")
  endif()
endfunction()

## GMock
if (PYARROW_BUILD_TESTS)
  find_package(GTest REQUIRED)
  include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(gtest
    STATIC_LIB ${GTEST_STATIC_LIB})
endif()

## Arrow
find_package(Arrow REQUIRED)
include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(arrow
  SHARED_LIB ${ARROW_SHARED_LIB})

############################################################
# Linker setup
############################################################

set(PYARROW_MIN_TEST_LIBS
  pyarrow_test_main
  pyarrow)

set(PYARROW_MIN_TEST_LIBS
  pyarrow_test_main
  pyarrow
  ${PYARROW_BASE_LIBS})

set(PYARROW_TEST_LINK_LIBS ${PYARROW_MIN_TEST_LIBS})

############################################################
# "make ctags" target
############################################################
if (UNIX)
  add_custom_target(ctags ctags -R --languages=c++,c --exclude=thirdparty/installed)
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
  --filter=-whitespace/comments,-readability/todo,-build/header_guard
    `find ${CMAKE_CURRENT_SOURCE_DIR}/src -name \\*.cc -or -name \\*.h`)
endif (UNIX)

############################################################
# Subdirectories
############################################################

if (UNIX)
  set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
endif()

add_subdirectory(src/pyarrow)
add_subdirectory(src/pyarrow/util)

set(PYARROW_SRCS
  src/pyarrow/common.cc
  src/pyarrow/config.cc
  src/pyarrow/helpers.cc
  src/pyarrow/status.cc

  src/pyarrow/adapters/builtin.cc
  src/pyarrow/adapters/pandas.cc
)

set(LINK_LIBS
  arrow
)

SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

add_library(pyarrow SHARED
  ${PYARROW_SRCS})
target_link_libraries(pyarrow ${LINK_LIBS})

if(APPLE)
  set_target_properties(pyarrow PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif()

############################################################
# Setup and build Cython modules
############################################################

set(CYTHON_EXTENSIONS
  array
  config
  error
  scalar
  schema
)

foreach(module ${CYTHON_EXTENSIONS})
    string(REPLACE "." ";" directories ${module})
    list(GET directories -1 module_name)
    list(REMOVE_AT directories -1)

    string(REPLACE "." "/" module_root "${module}")
    set(module_SRC pyarrow/${module_root}.pyx)
    set_source_files_properties(${module_SRC} PROPERTIES CYTHON_IS_CXX 1)

    cython_add_module(${module_name}
      ${module_name}_pyx
      ${module_name}_output
      ${module_SRC})

    if (directories)
        string(REPLACE ";" "/" module_output_directory ${directories})
        set_target_properties(${module_name} PROPERTIES
          LIBRARY_OUTPUT_DIRECTORY ${module_output_directory})
    endif()

    if(APPLE)
        set(module_install_rpath "@loader_path")
    else()
        set(module_install_rpath "$ORIGIN")
    endif()
    list(LENGTH directories i)
    while(${i} GREATER 0)
        set(module_install_rpath "${module_install_rpath}/..")
        math(EXPR i "${i} - 1" )
    endwhile(${i} GREATER 0)

    # for inplace development for now
    #set(module_install_rpath "${CMAKE_SOURCE_DIR}/pyarrow/")

    set_target_properties(${module_name} PROPERTIES
      INSTALL_RPATH ${module_install_rpath})
    target_link_libraries(${module_name} pyarrow)
endforeach(module)
