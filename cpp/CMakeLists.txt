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

cmake_minimum_required(VERSION 3.2)
project(arrow)

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/../java/pom.xml" POM_XML)
string(REGEX MATCHALL
  "\n  <version>[^<]+</version>" ARROW_VERSION_TAG "${POM_XML}")
string(REGEX REPLACE
  "(\n  <version>|</version>)" "" ARROW_VERSION "${ARROW_VERSION_TAG}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

include(CMakeParseArguments)
include(ExternalProject)

# This ensures that things like gnu++11 get passed correctly
set(CMAKE_CXX_STANDARD 11)

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(GNUInstallDirs)

# Compatibility with CMake 3.1
if(POLICY CMP0054)
  # http://www.cmake.org/cmake/help/v3.1/policy/CMP0054.html
  cmake_policy(SET CMP0054 NEW)
endif()

set(ARROW_SO_VERSION "0")
set(ARROW_ABI_VERSION "${ARROW_SO_VERSION}.0.0")

set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")

set(CLANG_FORMAT_VERSION "4.0")
find_package(ClangTools)
if ("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1" OR CLANG_TIDY_FOUND)
  # Generate a Clang compile_commands.json "compilation database" file for use
  # with various development tools, such as Vim's YouCompleteMe plugin.
  # See http://clang.llvm.org/docs/JSONCompilationDatabase.html
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

find_package(InferTools)
if ("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1" OR INFER_FOUND)
  # Generate a Clang compile_commands.json "compilation database" file for use
  # with various development tools, such as Vim's YouCompleteMe plugin.
  # See http://clang.llvm.org/docs/JSONCompilationDatabase.html
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${CCACHE_FOUND})
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ${CCACHE_FOUND})
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

  option(ARROW_NO_DEPRECATED_API
    "Exclude deprecated APIs from build"
    OFF)

  option(ARROW_COMPUTE
    "Build the Arrow Compute Modules"
    ON)

  option(ARROW_EXTRA_ERROR_CONTEXT
    "Compile with extra error context (line numbers, code)"
    OFF)

  option(ARROW_IPC
    "Build the Arrow IPC extensions"
    ON)

  option(ARROW_GPU
    "Build the Arrow GPU extensions (requires CUDA installation)"
    OFF)

  option(ARROW_ORC
    "Build the Arrow ORC adapter"
    OFF)

  option(ARROW_JEMALLOC
    "Build the Arrow jemalloc-based allocator"
    OFF)

  option(ARROW_JEMALLOC_USE_SHARED
    "Rely on jemalloc shared libraries where relevant"
    ON)

  option(ARROW_HDFS
    "Build the Arrow HDFS bridge"
    ON)

  option(ARROW_BOOST_USE_SHARED
    "Rely on boost shared libraries where relevant"
    ON)

  option(ARROW_BOOST_VENDORED
    "Use vendored Boost instead of existing Boost"
    OFF)

  option(ARROW_PYTHON
    "Build the Arrow CPython extensions"
    OFF)

  option(ARROW_FUZZING
    "Build Arrow Fuzzing executables"
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

  option(ARROW_INSTALL_NAME_RPATH
    "Build Arrow libraries with install_name set to @rpath"
    ON)

  option(ARROW_PLASMA
    "Build the plasma object store along with Arrow"
    OFF)

  option(ARROW_USE_SSE
    "Build with SSE4 optimizations"
    OFF)

  option(ARROW_WITH_BROTLI
    "Build with Brotli compression"
    ON)

  option(ARROW_WITH_LZ4
    "Build with lz4 compression"
    ON)

  option(ARROW_WITH_SNAPPY
    "Build with Snappy compression"
    ON)

  option(ARROW_WITH_ZLIB
    "Build with zlib compression"
    ON)

  option(ARROW_WITH_ZSTD
    "Build with zstd compression"
    ON)

  option(ARROW_WITH_GRPC
    "Build with GRPC"
    OFF)

  option(ARROW_VERBOSE_THIRDPARTY_BUILD
    "If off, output from ExternalProjects will be logged to files rather than shown"
    OFF)

  if (MSVC)
    set(BROTLI_MSVC_STATIC_LIB_SUFFIX "-static" CACHE STRING
      "Brotli static lib suffix used on Windows with MSVC (default -static)")
    set(SNAPPY_MSVC_STATIC_LIB_SUFFIX "_static" CACHE STRING
      "Snappy static lib suffix used on Windows with MSVC (default is empty string)")
    set(ZLIB_MSVC_STATIC_LIB_SUFFIX "libstatic" CACHE STRING
      "Zlib static lib suffix used on Windows with MSVC (default libstatic)")
    set(LZ4_MSVC_STATIC_LIB_SUFFIX "_static" CACHE STRING
      "Lz4 static lib suffix used on Windows with MSVC (default _static)")
    set(ZSTD_MSVC_STATIC_LIB_SUFFIX "_static" CACHE STRING
      "ZStd static lib suffix used on Windows with MSVC (default _static)")

    option(ARROW_USE_STATIC_CRT
      "Build Arrow with statically linked CRT"
      OFF)
  endif()
endif()

if(ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS)
  set(ARROW_BUILD_STATIC ON)
  set(ARROW_WITH_BROTLI ON)
  set(ARROW_WITH_LZ4 ON)
  set(ARROW_WITH_SNAPPY ON)
  set(ARROW_WITH_ZLIB ON)
  set(ARROW_WITH_ZSTD ON)
endif()

if (MSVC)
  # ORC doesn't build on windows
  set(ARROW_ORC OFF)
endif()

if(ARROW_ORC)
  set(ARROW_WITH_LZ4 ON)
  set(ARROW_WITH_SNAPPY ON)
  set(ARROW_WITH_ZLIB ON)
endif()

if(NOT ARROW_BUILD_TESTS)
  set(NO_TESTS 1)
endif()

if(NOT ARROW_BUILD_BENCHMARKS)
  set(NO_BENCHMARKS 1)
endif()

if (NOT ARROW_FUZZING)
  set(NO_FUZZING 1)
endif()

if(ARROW_HDFS)
  set(ARROW_BOOST_HEADER_ONLY 0)
else()
  set(ARROW_BOOST_HEADER_ONLY 1)
endif()

############################################################
# Compiler flags
############################################################

# Determine compiler version
include(CompilerInfo)

if (ARROW_NO_DEPRECATED_API)
  add_definitions(-DARROW_NO_DEPRECATED_API)
endif()

if (ARROW_EXTRA_ERROR_CONTEXT)
  add_definitions(-DARROW_EXTRA_ERROR_CONTEXT)
endif()

include(SetupCxxFlags)

############################################################
# Dependencies
############################################################

add_custom_target(arrow_dependencies)

include(BuildUtils)
enable_testing()

include(ThirdpartyToolchain)

# Add common flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_COMMON_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ARROW_CXXFLAGS}")

message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

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
if(ARROW_FUZZING)
    set(ARROW_USE_COVERAGE ON)
endif()
include(san-config)

# For any C code, use the same flags.
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS}")

# Remove --std=c++11 to avoid errors from C compilers
string(REPLACE "-std=c++11" "" CMAKE_C_FLAGS ${CMAKE_C_FLAGS})

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

include_directories(${CMAKE_CURRENT_BINARY_DIR}/src)
include_directories(src)

############################################################
# Visibility
############################################################
# For generate_export_header() and add_compiler_export_flags().
include(GenerateExportHeader)

# Adapted from Apache Kudu: https://github.com/apache/kudu/commit/bd549e13743a51013585
# Honor visibility properties for all target types. See
# "cmake --help-policy CMP0063" for details.
#
# This policy was only added to cmake in version 3.3, so until the cmake in
# thirdparty is updated, we must check if the policy exists before setting it.
if(POLICY CMP0063)
  cmake_policy(SET CMP0063 NEW)
endif()

if (PARQUET_BUILD_SHARED)
  if (POLICY CMP0063)
    set_target_properties(arrow_shared
      PROPERTIES
      C_VISIBILITY_PRESET hidden
      CXX_VISIBILITY_PRESET hidden
      VISIBILITY_INLINES_HIDDEN 1)
  else()
    # Sets -fvisibility=hidden for gcc
    add_compiler_export_flags()
  endif()
endif()

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
    IF(NOT ((item MATCHES "_generated.h") OR
            (item MATCHES "pyarrow_api.h") OR
            (item MATCHES "xxhash.h") OR
            (item MATCHES "xxhash.cc") OR
            (item MATCHES "config.h") OR
            (item MATCHES "util/variant") OR
            (item MATCHES "zmalloc.h") OR
            (item MATCHES "ae.h")))
      LIST(APPEND FILTERED_LINT_FILES ${item})
    ENDIF()
  ENDFOREACH(item ${LINT_FILES})

  find_program(CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR})
  message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")

  # Full lint
  add_custom_target(lint ${CPPLINT_BIN}
  --verbose=2
  --linelength=90
  --filter=-whitespace/comments,-readability/todo,-build/header_guard,-build/c++11,-runtime/references,-build/include_order
  ${FILTERED_LINT_FILES})
endif (UNIX)


############################################################
# "make format" and "make check-format" targets
############################################################

# runs clang format and updates files in place.
add_custom_target(format ${BUILD_SUPPORT_DIR}/run_clang_format.py
  ${CLANG_FORMAT_BIN}
  ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
  ${CMAKE_CURRENT_SOURCE_DIR}/src)

# runs clang format and exits with a non-zero exit code if any files need to be reformatted

# TODO(wesm): Make this work in run_clang_format.py
add_custom_target(check-format ${BUILD_SUPPORT_DIR}/run_clang_format.py
   ${CLANG_FORMAT_BIN}
   ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
   ${CMAKE_CURRENT_SOURCE_DIR}/src 1)

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
# "make infer" target
############################################################

if (${INFER_FOUND})
  # runs infer capture
  add_custom_target(infer ${BUILD_SUPPORT_DIR}/run-infer.sh ${INFER_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json 1)
  # runs infer analyze
  add_custom_target(infer-analyze ${BUILD_SUPPORT_DIR}/run-infer.sh ${INFER_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json 2)
  # runs infer report
  add_custom_target(infer-report ${BUILD_SUPPORT_DIR}/run-infer.sh ${INFER_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json 3)
endif()

############################################################
# "make iwyu" target
############################################################
if(UNIX)
  add_custom_target(iwyu ${BUILD_SUPPORT_DIR}/iwyu/iwyu.sh)
endif(UNIX)

############################################################
# Linker and Dependencies
############################################################

set(ARROW_STATIC_LINK_LIBS)

if (ARROW_WITH_BROTLI)
  SET(ARROW_STATIC_LINK_LIBS
    brotli_dec
    brotli_enc
    brotli_common
    ${ARROW_STATIC_LINK_LIBS})
endif()

if (ARROW_WITH_LZ4)
  SET(ARROW_STATIC_LINK_LIBS lz4_static ${ARROW_STATIC_LINK_LIBS})
endif()

if (ARROW_WITH_SNAPPY)
  SET(ARROW_STATIC_LINK_LIBS snappy ${ARROW_STATIC_LINK_LIBS})
endif()

if (ARROW_WITH_ZLIB)
  SET(ARROW_STATIC_LINK_LIBS zlib ${ARROW_STATIC_LINK_LIBS})
endif()

if (ARROW_WITH_ZSTD)
  SET(ARROW_STATIC_LINK_LIBS zstd_static ${ARROW_STATIC_LINK_LIBS})
endif()

if (ARROW_WITH_GRPC)
  SET(ARROW_STATIC_LINK_LIBS
    grpc_grp
    grpc_grpc
    grpc_grpcpp
    ${ARROW_STATIC_LINK_LIBS})
endif()

if (ARROW_ORC)
  SET(ARROW_STATIC_LINK_LIBS
    orc
    protobuf
    ${ARROW_STATIC_LINK_LIBS})
endif()

if (ARROW_STATIC_LINK_LIBS)
  add_dependencies(arrow_dependencies ${ARROW_STATIC_LINK_LIBS})
endif()

set(ARROW_BENCHMARK_LINK_LIBS
  arrow_static
  arrow_benchmark_main
  gtest
  ${ARROW_STATIC_LINK_LIBS})

set(ARROW_LINK_LIBS
  ${ARROW_STATIC_LINK_LIBS})

set(ARROW_SHARED_PRIVATE_LINK_LIBS
  ${BOOST_SYSTEM_LIBRARY}
  ${BOOST_FILESYSTEM_LIBRARY})

set(ARROW_STATIC_PRIVATE_LINK_LIBS
  ${BOOST_SYSTEM_LIBRARY}
  ${BOOST_FILESYSTEM_LIBRARY})

if (NOT MSVC)
  set(ARROW_LINK_LIBS
    ${ARROW_LINK_LIBS}
    ${CMAKE_DL_LIBS})
endif()

set(ARROW_MIN_TEST_LIBS
  arrow_static
  ${ARROW_STATIC_LINK_LIBS}
  gtest
  gtest_main)

if(NOT MSVC)
  set(ARROW_MIN_TEST_LIBS
    ${ARROW_MIN_TEST_LIBS}
    ${CMAKE_DL_LIBS})
endif()

set(ARROW_TEST_LINK_LIBS ${ARROW_MIN_TEST_LIBS})

if (ARROW_JEMALLOC)
  add_definitions(-DARROW_JEMALLOC)
  # In the case that jemalloc is only available as a shared library also use it to
  # link it in the static requirements. In contrast to other libraries we try in
  # most cases to use the system provided version of jemalloc to better align with
  # other potential users of jemalloc.
  if (JEMALLOC_STATIC_LIB AND NOT ARROW_JEMALLOC_USE_SHARED)
    set(ARROW_JEMALLOC_STATIC_LINK_LIBS jemalloc_static)
  else()
    set(ARROW_JEMALLOC_STATIC_LINK_LIBS jemalloc_shared)
  endif()

  if (NOT APPLE)
      set(ARROW_JEMALLOC_STATIC_LINK_LIBS ${ARROW_JEMALLOC_STATIC_LINK_LIBS} ${PTHREAD_LIBRARY} rt)
  endif()

  if (ARROW_JEMALLOC_USE_SHARED)
    set(ARROW_JEMALLOC_SHARED_LINK_LIBS jemalloc_shared)
  else()
    if (CMAKE_COMPILER_IS_GNUCXX)
      set(ARROW_JEMALLOC_SHARED_LINK_LIBS
        jemalloc_static
        # For glibc <2.17 we need to link to librt.
        # As we compile with --as-needed by default, the linker will omit this
        # dependency if not required.
        ${PTHREAD_LIBRARY}
        rt
        )
    else()
      set(ARROW_JEMALLOC_SHARED_LINK_LIBS
        jemalloc_static
        )
    endif()
  endif()
  set(ARROW_SHARED_PRIVATE_LINK_LIBS
    ${ARROW_SHARED_PRIVATE_LINK_LIBS}
    ${ARROW_JEMALLOC_SHARED_LINK_LIBS})
  set(ARROW_STATIC_LINK_LIBS
    ${ARROW_STATIC_LINK_LIBS}
    ${ARROW_JEMALLOC_STATIC_LINK_LIBS})
elseif (NOT MSVC)
  # We need to separate this as otherwise CMake would mess with the library
  # linking order.
  set(ARROW_LINK_LIBS
    ${ARROW_LINK_LIBS}
    ${PTHREAD_LIBRARY})
  set(ARROW_STATIC_LINK_LIBS
    ${ARROW_STATIC_LINK_LIBS}
    ${PTHREAD_LIBRARY})
endif()

############################################################
# Subdirectories
############################################################

if(NOT WIN32 AND ARROW_PLASMA)
  add_subdirectory(src/plasma)
endif()

add_subdirectory(src/arrow)

if(ARROW_PYTHON)
  find_package(PythonLibsNew REQUIRED)
  find_package(NumPy REQUIRED)

  include_directories(SYSTEM
    ${NUMPY_INCLUDE_DIRS}
    ${PYTHON_INCLUDE_DIRS})

  add_subdirectory(src/arrow/python)
endif()
