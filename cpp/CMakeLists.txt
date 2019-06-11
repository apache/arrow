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
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

set(ARROW_VERSION "0.14.0-SNAPSHOT")

string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" ARROW_BASE_VERSION "${ARROW_VERSION}")

project(arrow VERSION "${ARROW_BASE_VERSION}")

# if no build build type is specified, default to release builds
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif(NOT CMAKE_BUILD_TYPE)

set(ARROW_VERSION_MAJOR "${arrow_VERSION_MAJOR}")
set(ARROW_VERSION_MINOR "${arrow_VERSION_MINOR}")
set(ARROW_VERSION_PATCH "${arrow_VERSION_PATCH}")
if(ARROW_VERSION_MAJOR STREQUAL ""
   OR ARROW_VERSION_MINOR STREQUAL ""
   OR ARROW_VERSION_PATCH STREQUAL "")
  message(FATAL_ERROR "Failed to determine Arrow version from '${ARROW_VERSION}'")
endif()

# The SO version is also the ABI version
if(arrow_VERSION_MAJOR STREQUAL "0")
  # Arrow 0.x.y => SO version is "x", full SO version is "x.y.0"
  set(ARROW_SO_VERSION "${ARROW_VERSION_MINOR}")
  set(ARROW_FULL_SO_VERSION "${ARROW_SO_VERSION}.${ARROW_VERSION_PATCH}.0")
else()
  message(FATAL_ERROR "Need to implement SO version generation for Arrow 1.0+")
endif()

message(STATUS "Arrow version: "
               "${ARROW_VERSION_MAJOR}.${ARROW_VERSION_MINOR}.${ARROW_VERSION_PATCH} "
               "(full: '${ARROW_VERSION}')")

set(ARROW_SOURCE_DIR ${PROJECT_SOURCE_DIR})
set(ARROW_BINARY_DIR ${PROJECT_BINARY_DIR})

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules")

include(CMakeParseArguments)
include(ExternalProject)
include(FindPkgConfig)

include(GNUInstallDirs)

cmake_policy(SET CMP0025 NEW)

# Compatibility with CMake 3.1
if(POLICY CMP0054)
  # http://www.cmake.org/cmake/help/v3.1/policy/CMP0054.html
  cmake_policy(SET CMP0054 NEW)
endif()

# don't ignore <PackageName>_ROOT variables in find_package
if(POLICY CMP0074)
  # https://cmake.org/cmake/help/v3.12/policy/CMP0074.html
  cmake_policy(SET CMP0074 NEW)
endif()

set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")

set(ARROW_LLVM_VERSION "7.0")
# TODO: Remove this when we drop LLVM 7 support.
set(ARROW_LLVM_VERSION_FALLBACK "7.1")
string(
  REGEX
  REPLACE "^([0-9]+)\\.[0-9]+" "\\1" ARROW_LLVM_MAJOR_VERSION "${ARROW_LLVM_VERSION}")
string(
  REGEX
  REPLACE "^[0-9]+\\.([0-9]+)" "\\1" ARROW_LLVM_MINOR_VERSION "${ARROW_LLVM_VERSION}")
find_package(ClangTools)
if("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1" OR CLANG_TIDY_FOUND)
  # Generate a Clang compile_commands.json "compilation database" file for use
  # with various development tools, such as Vim's YouCompleteMe plugin.
  # See http://clang.llvm.org/docs/JSONCompilationDatabase.html
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

find_package(InferTools)
if("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1" OR INFER_FOUND)
  # Generate a Clang compile_commands.json "compilation database" file for use
  # with various development tools, such as Vim's YouCompleteMe plugin.
  # See http://clang.llvm.org/docs/JSONCompilationDatabase.html
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

# ----------------------------------------------------------------------
# cmake options
include(DefineOptions)

# Needed for linting targets, etc.
find_package(PythonInterp)

if(ARROW_USE_CCACHE)
  find_program(CCACHE_FOUND ccache)
  if(CCACHE_FOUND)
    message(STATUS "Using ccache: ${CCACHE_FOUND}")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${CCACHE_FOUND})
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ${CCACHE_FOUND})
    # ARROW-3985: let ccache preserve C++ comments, because some of them may be
    # meaningful to the compiler
    set(ENV{CCACHE_COMMENTS} "1")
  endif(CCACHE_FOUND)
endif()

if(ARROW_OPTIONAL_INSTALL)
  # Don't make the "install" target depend on the "all" target
  set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

  set(INSTALL_IS_OPTIONAL OPTIONAL)
endif()

#
# "make lint" target
#
if(NOT ARROW_VERBOSE_LINT)
  set(ARROW_LINT_QUIET "--quiet")
endif()

if(NOT LINT_EXCLUSIONS_FILE)
  # source files matching a glob from a line in this file
  # will be excluded from linting (cpplint, clang-tidy, clang-format)
  set(LINT_EXCLUSIONS_FILE ${BUILD_SUPPORT_DIR}/lint_exclusions.txt)
endif()

find_program(CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR})
message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")

add_custom_target(lint
                  ${PYTHON_EXECUTABLE}
                  ${BUILD_SUPPORT_DIR}/run_cpplint.py
                  --cpplint_binary
                  ${CPPLINT_BIN}
                  --exclude_globs
                  ${LINT_EXCLUSIONS_FILE}
                  --source_dir
                  ${CMAKE_CURRENT_SOURCE_DIR}/src
                  ${ARROW_LINT_QUIET})

#
# "make format" and "make check-format" targets
#
if(${CLANG_FORMAT_FOUND})
  # runs clang format and updates files in place.
  add_custom_target(format
                    ${PYTHON_EXECUTABLE}
                    ${BUILD_SUPPORT_DIR}/run_clang_format.py
                    --clang_format_binary
                    ${CLANG_FORMAT_BIN}
                    --exclude_globs
                    ${LINT_EXCLUSIONS_FILE}
                    --source_dir
                    ${CMAKE_CURRENT_SOURCE_DIR}/src
                    --fix
                    ${ARROW_LINT_QUIET})

  # runs clang format and exits with a non-zero exit code if any files need to be reformatted
  add_custom_target(check-format
                    ${PYTHON_EXECUTABLE}
                    ${BUILD_SUPPORT_DIR}/run_clang_format.py
                    --clang_format_binary
                    ${CLANG_FORMAT_BIN}
                    --exclude_globs
                    ${LINT_EXCLUSIONS_FILE}
                    --source_dir
                    ${CMAKE_CURRENT_SOURCE_DIR}/src
                    ${ARROW_LINT_QUIET})
endif()

#
# "make clang-tidy" and "make check-clang-tidy" targets
#
if(${CLANG_TIDY_FOUND})
  # TODO check to make sure .clang-tidy is being respected

  # runs clang-tidy and attempts to fix any warning automatically
  add_custom_target(clang-tidy
                    ${PYTHON_EXECUTABLE}
                    ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
                    --clang_tidy_binary
                    ${CLANG_TIDY_BIN}
                    --exclude_globs
                    ${LINT_EXCLUSIONS_FILE}
                    --compile_commands
                    ${CMAKE_BINARY_DIR}/compile_commands.json
                    --source_dir
                    ${CMAKE_CURRENT_SOURCE_DIR}/src
                    --fix
                    ${ARROW_LINT_QUIET})

  # runs clang-tidy and exits with a non-zero exit code if any errors are found.
  add_custom_target(check-clang-tidy
                    ${PYTHON_EXECUTABLE}
                    ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
                    --clang_tidy_binary
                    ${CLANG_TIDY_BIN}
                    --exclude_globs
                    ${LINT_EXCLUSIONS_FILE}
                    --compile_commands
                    ${CMAKE_BINARY_DIR}/compile_commands.json
                    --source_dir
                    ${CMAKE_CURRENT_SOURCE_DIR}/src
                    ${ARROW_LINT_QUIET})
endif()

if(ARROW_ONLY_LINT)
  return()
endif()

#
# Set up various options
#

if(ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS)
  # Currently the compression tests require at least these libraries; bz2 and
  # zstd are optional. See ARROW-3984
  set(ARROW_WITH_BROTLI ON)
  set(ARROW_WITH_LZ4 ON)
  set(ARROW_WITH_SNAPPY ON)
  set(ARROW_WITH_ZLIB ON)
endif()

if(ARROW_BUILD_TESTS)
  # JSON parsing of arrays is required for Arrow unit tests
  set(ARROW_IPC ON)
endif()

if(PARQUET_BUILD_EXAMPLES OR PARQUET_BUILD_EXECUTABLES)
  set(ARROW_PARQUET ON)
  set(ARROW_BUILD_STATIC ON)
endif()

if(MSVC)
  # ORC doesn't build on windows
  set(ARROW_ORC OFF)
  # Plasma using glog is not fully tested on windows.
  set(ARROW_USE_GLOG OFF)
endif()

if(ARROW_JNI)
  set(ARROW_BUILD_STATIC ON)
endif()

if(ARROW_ORC)
  set(ARROW_WITH_LZ4 ON)
  set(ARROW_WITH_SNAPPY ON)
  set(ARROW_WITH_ZLIB ON)
endif()

if(NOT ARROW_BUILD_TESTS)
  set(NO_TESTS 1)
else()
  add_custom_target(all-tests)
  add_custom_target(unittest ctest -L unittest)
  add_dependencies(unittest all-tests)
endif()

if(NOT ARROW_BUILD_BENCHMARKS)
  set(NO_BENCHMARKS 1)
else()
  add_custom_target(all-benchmarks)
  add_custom_target(benchmark ctest -L benchmark)
  add_dependencies(benchmark all-benchmarks)
  if(ARROW_BUILD_BENCHMARKS_REFERENCE)
    add_definitions(-DARROW_WITH_BENCHMARKS_REFERENCE)
  endif()
endif()

if(NOT ARROW_BUILD_EXAMPLES)
  set(NO_EXAMPLES 1)
endif()

if(NOT ARROW_FUZZING)
  set(NO_FUZZING 1)
endif()

if(MSVC
   AND ARROW_USE_CLCACHE
   AND (("${CMAKE_GENERATOR}" STREQUAL "NMake Makefiles")
        OR ("${CMAKE_GENERATOR}" STREQUAL "Ninja")))
  find_program(CLCACHE_FOUND clcache)
  if(CLCACHE_FOUND)
    set(CMAKE_CXX_COMPILER ${CLCACHE_FOUND})
  endif(CLCACHE_FOUND)
endif()

#
# Compiler flags
#

# Determine compiler version
include(CompilerInfo)

if(ARROW_NO_DEPRECATED_API)
  add_definitions(-DARROW_NO_DEPRECATED_API)
endif()

if(ARROW_EXTRA_ERROR_CONTEXT)
  add_definitions(-DARROW_EXTRA_ERROR_CONTEXT)
endif()

include(SetupCxxFlags)

#
# Build output directory
#

# set compile output directory
string(TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)

# If build in-source, create the latest symlink. If build out-of-source, which is
# preferred, simply output the binaries in the build folder
if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
  set(BUILD_OUTPUT_ROOT_DIRECTORY
      "${CMAKE_CURRENT_BINARY_DIR}/build/${BUILD_SUBDIR_NAME}/")
  # Link build/latest to the current build directory, to avoid developers
  # accidentally running the latest debug build when in fact they're building
  # release builds.
  file(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
  if(NOT APPLE)
    set(MORE_ARGS "-T")
  endif()
  execute_process(COMMAND ln
                          ${MORE_ARGS}
                          -sf
                          ${BUILD_OUTPUT_ROOT_DIRECTORY}
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

#
# Dependencies
#

include(BuildUtils)
enable_testing()

include(ThirdpartyToolchain)

# Add common flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_COMMON_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ARROW_CXXFLAGS}")

# For any C code, use the same flags. These flags don't contain
# C++ specific flags.
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${ARROW_CXX_FLAGS} ${CXX_COMMON_FLAGS}")

# Remove --std=c++11 to avoid errors from C compilers
string(REPLACE "-std=c++11" "" CMAKE_C_FLAGS ${CMAKE_C_FLAGS})

# Add C++-only flags, like -std=c++11
set(CMAKE_CXX_FLAGS "${CXX_ONLY_FLAGS} ${CMAKE_CXX_FLAGS}")

# ASAN / TSAN / UBSAN
if(ARROW_FUZZING)
  set(ARROW_USE_COVERAGE ON)
endif()
include(san-config)

# Code coverage
if("${ARROW_GENERATE_COVERAGE}")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage -DCOVERAGE_BUILD")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -DCOVERAGE_BUILD")
endif()

# CMAKE_CXX_FLAGS now fully assembled
message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/src)
include_directories(src)

#
# Visibility
#
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

if(PARQUET_BUILD_SHARED)
  if(POLICY CMP0063)
    set_target_properties(arrow_shared
                          PROPERTIES C_VISIBILITY_PRESET
                                     hidden
                                     CXX_VISIBILITY_PRESET
                                     hidden
                                     VISIBILITY_INLINES_HIDDEN
                                     1)
  else()
    # Sets -fvisibility=hidden for gcc
    add_compiler_export_flags()
  endif()
endif()

#
# "make ctags" target
#
if(UNIX)
  add_custom_target(ctags ctags -R --languages=c++,c)
endif(UNIX)

#
# "make etags" target
#
if(UNIX)
  add_custom_target(tags
                    etags
                    --members
                    --declarations
                    `find
                    ${CMAKE_CURRENT_SOURCE_DIR}/src
                    -name
                    \\*.cc
                    -or
                    -name
                    \\*.hh
                    -or
                    -name
                    \\*.cpp
                    -or
                    -name
                    \\*.h
                    -or
                    -name
                    \\*.c
                    -or
                    -name
                    \\*.f`)
  add_custom_target(etags DEPENDS tags)
endif(UNIX)

#
# "make cscope" target
#
if(UNIX)
  add_custom_target(cscope find ${CMAKE_CURRENT_SOURCE_DIR}
                    (-name
                     \\*.cc
                     -or
                     -name
                     \\*.hh
                     -or
                     -name
                     \\*.cpp
                     -or
                     -name
                     \\*.h
                     -or
                     -name
                     \\*.c
                     -or
                     -name
                     \\*.f)
                    -exec
                    echo
                    \"{}\"
                    \;
                    >
                    cscope.files
                    &&
                    cscope
                    -q
                    -b
                    VERBATIM)
endif(UNIX)

#
# "make infer" target
#

if(${INFER_FOUND})
  # runs infer capture
  add_custom_target(infer
                    ${BUILD_SUPPORT_DIR}/run-infer.sh
                    ${INFER_BIN}
                    ${CMAKE_BINARY_DIR}/compile_commands.json
                    1)
  # runs infer analyze
  add_custom_target(infer-analyze
                    ${BUILD_SUPPORT_DIR}/run-infer.sh
                    ${INFER_BIN}
                    ${CMAKE_BINARY_DIR}/compile_commands.json
                    2)
  # runs infer report
  add_custom_target(infer-report
                    ${BUILD_SUPPORT_DIR}/run-infer.sh
                    ${INFER_BIN}
                    ${CMAKE_BINARY_DIR}/compile_commands.json
                    3)
endif()

#
# "make iwyu" target
#
if(UNIX)
  add_custom_target(iwyu ${BUILD_SUPPORT_DIR}/iwyu/iwyu.sh)
endif(UNIX)

#
# Linker and Dependencies
#

# TODO: Also rework how these libs work
set(ARROW_LINK_LIBS)
set(ARROW_SHARED_INSTALL_INTERFACE_LIBS)
set(ARROW_STATIC_INSTALL_INTERFACE_LIBS)

# Libraries to link statically with libarrow.so
set(ARROW_LINK_LIBS ${double-conversion_LIBRARIES})
set(ARROW_STATIC_LINK_LIBS ${double-conversion_LIBRARIES})
set(ARROW_STATIC_INSTALL_INTERFACE_LIBS ${double-conversion_LIBRARIES})

if(ARROW_WITH_URIPARSER)
  list(APPEND ARROW_STATIC_LINK_LIBS uriparser::uriparser)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS uriparser::uriparser)
endif()

if(PARQUET_BUILD_ENCRYPTION)
  list(APPEND ARROW_LINK_LIBS OpenSSL::Crypto)
  list(APPEND ARROW_STATIC_LINK_LIBS OpenSSL::Crypto)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS OpenSSL::Crypto)
endif()

if(ARROW_WITH_BROTLI)
  # Order is important for static linking
  list(APPEND ARROW_LINK_LIBS Brotli::brotlienc Brotli::brotlidec Brotli::brotlicommon)
  list(APPEND ARROW_STATIC_LINK_LIBS Brotli::brotlienc Brotli::brotlidec
              Brotli::brotlicommon)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS Brotli::brotlienc Brotli::brotlidec
              Brotli::brotlicommon)
endif()

if(ARROW_WITH_BZ2)
  list(APPEND ARROW_STATIC_LINK_LIBS BZip2::BZip2)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS BZip2::BZip2)
endif()

if(ARROW_WITH_LZ4)
  list(APPEND ARROW_STATIC_LINK_LIBS LZ4::lz4)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS LZ4::lz4)
endif()

if(ARROW_WITH_SNAPPY)
  list(APPEND ARROW_STATIC_LINK_LIBS Snappy::snappy)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS Snappy::snappy)
endif()

if(ARROW_WITH_ZLIB)
  list(APPEND ARROW_STATIC_LINK_LIBS ZLIB::ZLIB)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ZLIB::ZLIB)
endif()

if(ARROW_WITH_ZSTD)
  list(APPEND ARROW_STATIC_LINK_LIBS ZSTD::zstd)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ZSTD::zstd)
endif()

if(ARROW_ORC)
  list(APPEND ARROW_LINK_LIBS protobuf::libprotobuf orc_static)
  list(APPEND ARROW_STATIC_LINK_LIBS protobuf::libprotobuf orc_static)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS protobuf::libprotobuf orc)
endif()

if(ARROW_USE_GLOG)
  list(APPEND ARROW_LINK_LIBS GLOG::glog)
  list(APPEND ARROW_STATIC_LINK_LIBS GLOG::glog)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS GLOG::glog)
  add_definitions("-DARROW_USE_GLOG")
endif()

add_custom_target(arrow_dependencies)
add_custom_target(arrow_benchmark_dependencies)
add_custom_target(arrow_test_dependencies)

# ARROW-4581: CMake can be finicky about invoking the ExternalProject builds
# for some of the library dependencies, so we "nuke it from orbit" by making
# the toolchain dependency explicit using these "dependencies" targets
add_dependencies(arrow_dependencies toolchain)
add_dependencies(arrow_test_dependencies toolchain-tests)

if(ARROW_STATIC_LINK_LIBS)
  add_dependencies(arrow_dependencies ${ARROW_STATIC_LINK_LIBS})
endif()

set(ARROW_SHARED_PRIVATE_LINK_LIBS ${ARROW_STATIC_LINK_LIBS} ${BOOST_SYSTEM_LIBRARY}
                                   ${BOOST_FILESYSTEM_LIBRARY} ${BOOST_REGEX_LIBRARY})

list(APPEND ARROW_STATIC_LINK_LIBS ${BOOST_SYSTEM_LIBRARY} ${BOOST_FILESYSTEM_LIBRARY}
            ${BOOST_REGEX_LIBRARY})

list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS boost_system boost_filesystem boost_regex)

if(NOT MSVC)
  list(APPEND ARROW_LINK_LIBS ${CMAKE_DL_LIBS})
  list(APPEND ARROW_SHARED_INSTALL_INTERFACE_LIBS ${CMAKE_DL_LIBS})
endif()

set(ARROW_TEST_LINK_TOOLCHAIN GTest::Main GTest::GTest GTest::GMock)

if(ARROW_BUILD_TESTS)
  add_dependencies(arrow_test_dependencies ${ARROW_TEST_LINK_TOOLCHAIN})
endif()

if(ARROW_BUILD_BENCHMARKS)
  # Some benchmarks use gtest
  add_dependencies(arrow_benchmark_dependencies arrow_test_dependencies
                   toolchain-benchmarks)
endif()

set(ARROW_TEST_STATIC_LINK_LIBS arrow_testing_static arrow_static ${ARROW_LINK_LIBS}
                                ${ARROW_TEST_LINK_TOOLCHAIN})

set(ARROW_TEST_SHARED_LINK_LIBS
    arrow_testing_shared
    arrow_shared
    ${ARROW_LINK_LIBS}
    ${double-conversion_LIBRARIES}
    ${BOOST_SYSTEM_LIBRARY}
    ${BOOST_FILESYSTEM_LIBRARY}
    ${BOOST_REGEX_LIBRARY}
    ${ARROW_TEST_LINK_TOOLCHAIN})

if(NOT MSVC)
  set(ARROW_TEST_SHARED_LINK_LIBS ${ARROW_TEST_SHARED_LINK_LIBS} ${CMAKE_DL_LIBS})
endif()

if("${ARROW_TEST_LINKAGE}" STREQUAL "shared")
  if(ARROW_BUILD_TESTS AND NOT ARROW_BUILD_SHARED)
    message(FATAL_ERROR "If using shared linkage for unit tests, must also \
pass ARROW_BUILD_SHARED=on")
  endif()
  # Use shared linking for unit tests if it's available
  set(ARROW_TEST_LINK_LIBS ${ARROW_TEST_SHARED_LINK_LIBS})
  set(ARROW_EXAMPLE_LINK_LIBS arrow_shared)
else()
  if(ARROW_BUILD_TESTS AND NOT ARROW_BUILD_STATIC)
    message(FATAL_ERROR "If using static linkage for unit tests, must also \
pass ARROW_BUILD_STATIC=on")
  endif()
  set(ARROW_TEST_LINK_LIBS ${ARROW_TEST_STATIC_LINK_LIBS})
  set(ARROW_EXAMPLE_LINK_LIBS arrow_static)
endif()

if(ARROW_BUILD_BENCHMARKS)
  # In the case that benchmark::benchmark_main is not available,
  # we need to provide our own version. This only happens for older versions
  # of benchmark.
  if(NOT TARGET benchmark::benchmark_main)
    add_library(arrow_benchmark_main STATIC src/arrow/util/benchmark_main.cc)
    add_library(benchmark::benchmark_main ALIAS arrow_benchmark_main)
  endif()

  set(ARROW_BENCHMARK_LINK_LIBS benchmark::benchmark_main benchmark::benchmark
                                ${ARROW_TEST_LINK_LIBS})
endif()

set(ARROW_SYSTEM_LINK_LIBS)

if(ARROW_JEMALLOC)
  add_definitions(-DARROW_JEMALLOC)
  add_definitions(-DARROW_JEMALLOC_INCLUDE_DIR=${JEMALLOC_INCLUDE_DIR})
  list(APPEND ARROW_SYSTEM_LINK_LIBS jemalloc::jemalloc)
endif(ARROW_JEMALLOC)

if(THREADS_FOUND)
  list(APPEND ARROW_SYSTEM_LINK_LIBS Threads::Threads)
endif()

if(NOT WIN32 AND NOT APPLE)
  # Pass -lrt on Linux only
  list(APPEND ARROW_SYSTEM_LINK_LIBS rt)
endif()

list(APPEND ARROW_LINK_LIBS ${ARROW_SYSTEM_LINK_LIBS})
list(APPEND ARROW_STATIC_LINK_LIBS ${ARROW_SYSTEM_LINK_LIBS})
list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_SYSTEM_LINK_LIBS})

#
# Subdirectories
#

if(NOT WIN32 AND ARROW_PLASMA)
  add_subdirectory(src/plasma)
endif()

add_subdirectory(src/arrow)

if(ARROW_PARQUET)
  add_subdirectory(src/parquet)
  add_subdirectory(tools/parquet)
  if(PARQUET_BUILD_EXAMPLES)
    add_subdirectory(examples/parquet)
  endif()
endif()

if(ARROW_JNI)
  add_subdirectory(src/jni)
endif()

if(ARROW_GANDIVA)
  add_subdirectory(src/gandiva)
endif()

if(ARROW_BUILD_EXAMPLES)
  add_custom_target(runexample ctest -L example)
  add_subdirectory(examples/arrow)
endif()

include(CMakePackageConfigHelpers)

# Makes the project importable from the build directory
export(EXPORT ${PROJECT_NAME}-targets
       FILE "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Targets.cmake")
configure_package_config_file(${PROJECT_NAME}Config.cmake.in
                              "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
                              INSTALL_DESTINATION
                              "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
  VERSION ${${PROJECT_NAME}_VERSION}
  COMPATIBILITY AnyNewerVersion)

install(EXPORT ${PROJECT_NAME}-targets
        FILE ${PROJECT_NAME}Targets.cmake
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
              ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")

config_summary()
