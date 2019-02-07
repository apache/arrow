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

set(ARROW_VERSION "0.13.0-SNAPSHOT")

string(REGEX MATCH
  "^[0-9]+\\.[0-9]+\\.[0-9]+" ARROW_BASE_VERSION "${ARROW_VERSION}")

project(arrow VERSION "${ARROW_BASE_VERSION}")

set(ARROW_VERSION_MAJOR "${arrow_VERSION_MAJOR}")
set(ARROW_VERSION_MINOR "${arrow_VERSION_MINOR}")
set(ARROW_VERSION_PATCH "${arrow_VERSION_PATCH}")
if (ARROW_VERSION_MAJOR STREQUAL "" OR
    ARROW_VERSION_MINOR STREQUAL "" OR
    ARROW_VERSION_PATCH STREQUAL "")
  MESSAGE(FATAL_ERROR "Failed to determine Arrow version from '${ARROW_VERSION}'")
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
STRING(REGEX REPLACE "^([0-9]+)\\.[0-9]+" "\\1" ARROW_LLVM_MAJOR_VERSION "${ARROW_LLVM_VERSION}")
STRING(REGEX REPLACE "^[0-9]+\\.([0-9]+)" "\\1" ARROW_LLVM_MINOR_VERSION "${ARROW_LLVM_VERSION}")
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

# ----------------------------------------------------------------------
# cmake options

# Top level cmake dir
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
  set(ARROW_CXXFLAGS "" CACHE STRING
    "Compiler flags to append when compiling Arrow")

  option(ARROW_ONLY_LINT
    "Only define the lint and check-format targets"
    OFF)

  option(ARROW_BUILD_STATIC
    "Build static libraries"
    ON)

  option(ARROW_BUILD_SHARED
    "Build shared libraries"
    ON)

  option(ARROW_TEST_MEMCHECK
    "Run the test suite using valgrind --tool=memcheck"
    OFF)

  option(ARROW_USE_ASAN
    "Enable Address Sanitizer checks"
    OFF)

  option(ARROW_USE_CCACHE
    "Use ccache when compiling (if available)"
    ON)

  option(ARROW_USE_LD_GOLD
    "Use ld.gold for linking on Linux (if available)"
    OFF)

  option(ARROW_USE_TSAN
    "Enable Thread Sanitizer checks"
    OFF)

  option(ARROW_BUILD_TESTS
    "Build the Arrow googletest unit tests, default OFF"
    OFF)

  option(ARROW_BUILD_BENCHMARKS
    "Build the Arrow micro benchmarks, default OFF"
    OFF)

  option(ARROW_BUILD_EXAMPLES
    "Build the Arrow examples, default OFF"
    OFF)

  set(ARROW_TEST_LINKAGE "shared" CACHE STRING
    "Linkage of Arrow libraries with unit tests executables. \
static|shared (default shared)")

  option(ARROW_NO_DEPRECATED_API
    "Exclude deprecated APIs from build"
    OFF)

  option(ARROW_FUZZING
    "Build Arrow Fuzzing executables"
    OFF)

  # Disable this option to exercise non-SIMD fallbacks
  option(ARROW_USE_SIMD
    "Build with SIMD optimizations"
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

  option(ARROW_GENERATE_COVERAGE
    "Build with C++ code coverage enabled"
    OFF)

  option(ARROW_VERBOSE_LINT
    "If off, 'quiet' flags will be passed to linting tools"
    OFF)

  option(ARROW_GGDB_DEBUG
    "Pass -ggdb flag to debug builds"
    ON)

  #----------------------------------------------------------------------
  # Project components to enable / disable building

  option(ARROW_COMPUTE
    "Build the Arrow Compute Modules"
    ON)

  option(ARROW_FLIGHT
    "Build the Arrow Flight RPC System (requires GRPC, Protocol Buffers)"
    OFF)

  option(ARROW_GANDIVA
    "Build the Gandiva libraries"
    OFF)

  option(ARROW_PARQUET
    "Build the Parquet libraries"
    OFF)

  option(ARROW_IPC
    "Build the Arrow IPC extensions"
    ON)

  option(ARROW_CUDA
    "Build the Arrow CUDA extensions (requires CUDA toolkit)"
    OFF)

  option(ARROW_ORC
    "Build the Arrow ORC adapter"
    OFF)

  option(ARROW_TENSORFLOW
    "Build Arrow with TensorFlow support enabled"
    OFF)

  option(ARROW_JEMALLOC
    "Build the Arrow jemalloc-based allocator"
    ON)

  option(ARROW_HDFS
    "Build the Arrow HDFS bridge"
    ON)

  option(ARROW_PYTHON
    "Build the Arrow CPython extensions"
    OFF)

  option(ARROW_HIVESERVER2
    "Build the HiveServer2 client and Arrow adapter"
    OFF)

  option(ARROW_PLASMA
    "Build the plasma object store along with Arrow"
    OFF)

  option(ARROW_PLASMA_JAVA_CLIENT
    "Build the plasma object store java client"
    OFF)

  #----------------------------------------------------------------------
  # Thirdparty toolchain options

  option(ARROW_VERBOSE_THIRDPARTY_BUILD
    "If off, output from ExternalProjects will be logged to files rather than shown"
    OFF)

  option(ARROW_BOOST_USE_SHARED
    "Rely on boost shared libraries where relevant"
    ON)

  option(ARROW_BOOST_VENDORED
    "Use vendored Boost instead of existing Boost. \
Note that this requires linking Boost statically"
    OFF)

  option(ARROW_PROTOBUF_USE_SHARED
    "Rely on Protocol Buffers shared libraries where relevant"
    OFF)

  option(ARROW_WITH_BACKTRACE
    "Build with backtrace support"
    ON)

  option(ARROW_USE_GLOG
    "Build libraries with glog support for pluggable logging"
    ON)

  option(ARROW_WITH_BROTLI
    "Build with Brotli compression"
    ON)

  option(ARROW_WITH_BZ2
    "Build with BZ2 compression"
    OFF)

  option(ARROW_WITH_LZ4
    "Build with lz4 compression"
    ON)

  option(ARROW_WITH_SNAPPY
    "Build with Snappy compression"
    ON)

  option(ARROW_WITH_ZLIB
    "Build with zlib compression"
    ON)

  if(CMAKE_VERSION VERSION_LESS 3.7)
    set(ARROW_WITH_ZSTD_DEFAULT OFF)
  else()
    # ExternalProject_Add(SOURCE_SUBDIR) is available since CMake 3.7.
    set(ARROW_WITH_ZSTD_DEFAULT ON)
  endif()
  option(ARROW_WITH_ZSTD
    "Build with zstd compression"
    ${ARROW_WITH_ZSTD_DEFAULT})

  #----------------------------------------------------------------------
  # Windows options

  if (MSVC)
    option(MSVC_LINK_VERBOSE
      "Pass verbose linking options when linking libraries and executables"
      OFF)

    option(ARROW_USE_CLCACHE
      "Use clcache if available"
      ON)

    set(BROTLI_MSVC_STATIC_LIB_SUFFIX "-static" CACHE STRING
      "Brotli static lib suffix used on Windows with MSVC (default -static)")
    set(PROTOBUF_MSVC_STATIC_LIB_SUFFIX "" CACHE STRING
      "Protobuf static lib suffix used on Windows with MSVC (default is empty string)")
    set(RE2_MSVC_STATIC_LIB_SUFFIX "_static" CACHE STRING
      "re2 static lib suffix used on Windows with MSVC (default is _static)")
    set(SNAPPY_MSVC_STATIC_LIB_SUFFIX "_static" CACHE STRING
      "Snappy static lib suffix used on Windows with MSVC (default is _static)")
    set(LZ4_MSVC_STATIC_LIB_SUFFIX "_static" CACHE STRING
      "Lz4 static lib suffix used on Windows with MSVC (default _static)")
    set(ZSTD_MSVC_STATIC_LIB_SUFFIX "_static" CACHE STRING
      "ZStd static lib suffix used on Windows with MSVC (default _static)")

    option(ARROW_USE_STATIC_CRT
      "Build Arrow with statically linked CRT"
      OFF)
  endif()

  #----------------------------------------------------------------------
  # Parquet build options

  option(PARQUET_MINIMAL_DEPENDENCY
    "Depend only on Thirdparty headers to build libparquet. \
Always OFF if building binaries"
    OFF)

  option(PARQUET_BUILD_EXECUTABLES
    "Build the Parquet executable CLI tools. Requires static libraries to be built."
    OFF)

  option(PARQUET_BUILD_EXAMPLES
    "Build the Parquet examples. Requires static libraries to be built."
    OFF)

  #----------------------------------------------------------------------
  # Gandiva build options

  option(ARROW_GANDIVA_JAVA
    "Build the Gandiva JNI wrappers"
    OFF)

  # ARROW-3860: Temporary workaround
  option(ARROW_GANDIVA_STATIC_LIBSTDCPP
    "Include -static-libstdc++ -static-libgcc when linking with Gandiva static libraries"
    OFF)

  set(ARROW_GANDIVA_PC_CXX_FLAGS "" CACHE STRING
    "Compiler flags to append when pre-compiling Gandiva operations")

  #----------------------------------------------------------------------
  # Advanced developer options

  option(ARROW_EXTRA_ERROR_CONTEXT
    "Compile with extra error context (line numbers, code)"
    OFF)

  option(ARROW_OPTIONAL_INSTALL
    "If enabled install ONLY targets that have already been built. Please be \
advised that if this is enabled 'install' will fail silently on components \
that have not been built"
    OFF)
endif()

# Needed for linting targets, etc.
find_package(PythonInterp)

if (ARROW_USE_CCACHE)
  find_program(CCACHE_FOUND ccache)
  if(CCACHE_FOUND)
    message(STATUS "Using ccache: ${CCACHE_FOUND}")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${CCACHE_FOUND})
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ${CCACHE_FOUND})
  endif(CCACHE_FOUND)
endif()

if (ARROW_OPTIONAL_INSTALL)
  # Don't make the "install" target depend on the "all" target
  set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

  set(INSTALL_IS_OPTIONAL OPTIONAL)
endif()

############################################################
# "make lint" target
############################################################
if (NOT ARROW_VERBOSE_LINT)
  set(ARROW_LINT_QUIET "--quiet")
endif()

if (NOT LINT_EXCLUSIONS_FILE)
  # source files matching a glob from a line in this file
  # will be excluded from linting (cpplint, clang-tidy, clang-format)
  set(LINT_EXCLUSIONS_FILE ${BUILD_SUPPORT_DIR}/lint_exclusions.txt)
endif()

find_program(CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR})
message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")

add_custom_target(lint
  ${PYTHON_EXECUTABLE} ${BUILD_SUPPORT_DIR}/run_cpplint.py
  --cpplint_binary ${CPPLINT_BIN}
  --exclude_globs ${LINT_EXCLUSIONS_FILE}
  --source_dir ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${ARROW_LINT_QUIET})

############################################################
# "make format" and "make check-format" targets
############################################################
if (${CLANG_FORMAT_FOUND})
  # runs clang format and updates files in place.
  add_custom_target(format
    ${PYTHON_EXECUTABLE} ${BUILD_SUPPORT_DIR}/run_clang_format.py
    --clang_format_binary ${CLANG_FORMAT_BIN}
    --exclude_globs ${LINT_EXCLUSIONS_FILE}
    --source_dir ${CMAKE_CURRENT_SOURCE_DIR}/src
    --fix
    ${ARROW_LINT_QUIET})

  # runs clang format and exits with a non-zero exit code if any files need to be reformatted
  add_custom_target(check-format
    ${PYTHON_EXECUTABLE} ${BUILD_SUPPORT_DIR}/run_clang_format.py
    --clang_format_binary ${CLANG_FORMAT_BIN}
    --exclude_globs ${LINT_EXCLUSIONS_FILE}
    --source_dir ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${ARROW_LINT_QUIET})
endif()

############################################################
# "make clang-tidy" and "make check-clang-tidy" targets
############################################################
if (${CLANG_TIDY_FOUND})
  # TODO check to make sure .clang-tidy is being respected

  # runs clang-tidy and attempts to fix any warning automatically
  add_custom_target(clang-tidy
    ${PYTHON_EXECUTABLE} ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
    --clang_tidy_binary ${CLANG_TIDY_BIN}
    --exclude_globs ${LINT_EXCLUSIONS_FILE}
    --compile_commands ${CMAKE_BINARY_DIR}/compile_commands.json
    --source_dir ${CMAKE_CURRENT_SOURCE_DIR}/src
    --fix
    ${ARROW_LINT_QUIET})

  # runs clang-tidy and exits with a non-zero exit code if any errors are found.
  add_custom_target(check-clang-tidy
    ${PYTHON_EXECUTABLE} ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
    --clang_tidy_binary ${CLANG_TIDY_BIN}
    --exclude_globs ${LINT_EXCLUSIONS_FILE}
    --compile_commands ${CMAKE_BINARY_DIR}/compile_commands.json
    --source_dir ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${ARROW_LINT_QUIET})
endif()

if (ARROW_ONLY_LINT)
  return()
endif()


############################################################
# Set up various options
############################################################

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

if (MSVC)
  # ORC doesn't build on windows
  set(ARROW_ORC OFF)
  # Plasma using glog is not fully tested on windows.
  set(ARROW_USE_GLOG OFF)
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
endif()

if(NOT ARROW_BUILD_EXAMPLES)
  set(NO_EXAMPLES 1)
endif()

if (NOT ARROW_FUZZING)
  set(NO_FUZZING 1)
endif()

if (MSVC AND ARROW_USE_CLCACHE AND
     (("${CMAKE_GENERATOR}" STREQUAL "NMake Makefiles") OR
      ("${CMAKE_GENERATOR}" STREQUAL "Ninja")))
  find_program(CLCACHE_FOUND clcache)
  if(CLCACHE_FOUND)
    set(CMAKE_CXX_COMPILER ${CLCACHE_FOUND})
  endif(CLCACHE_FOUND)
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
if ("${ARROW_GENERATE_COVERAGE}")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage -DCOVERAGE_BUILD")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -DCOVERAGE_BUILD")
endif()

# CMAKE_CXX_FLAGS now fully assembled
message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

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

set(ARROW_LINK_LIBS)
set(ARROW_SHARED_INSTALL_INTERFACE_LIBS)
set(ARROW_STATIC_INSTALL_INTERFACE_LIBS)

# Libraries to link statically with libarrow.so
set(ARROW_STATIC_LINK_LIBS double-conversion_static)
set(ARROW_STATIC_INSTALL_INTERFACE_LIBS double-conversion)

if (ARROW_WITH_BROTLI)
  list(APPEND
    ARROW_STATIC_LINK_LIBS
    brotli_dec_static
    brotli_enc_static
    brotli_common_static)
  list(APPEND
    ARROW_STATIC_INSTALL_INTERFACE_LIBS
    brotlidec
    brotlienc
    brotlicommon)
endif()

if (ARROW_WITH_BZ2)
  list(APPEND ARROW_STATIC_LINK_LIBS bz2_static)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS bz2)
endif()

if (ARROW_WITH_LZ4)
  list(APPEND ARROW_STATIC_LINK_LIBS lz4_static)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS lz4)
endif()

if (ARROW_WITH_SNAPPY)
  list(APPEND ARROW_STATIC_LINK_LIBS snappy_static)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS snappy)
endif()

if (ARROW_WITH_ZLIB)
  list(APPEND ARROW_STATIC_LINK_LIBS ${ZLIB_LIBRARY})
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS z)
endif()

if (ARROW_WITH_ZSTD)
  list(APPEND ARROW_STATIC_LINK_LIBS zstd_static)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS zstd)
endif()

if (ARROW_ORC)
  list(APPEND ARROW_STATIC_LINK_LIBS orc_static)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS orc)
endif()

if (ARROW_USE_GLOG)
  list(APPEND ARROW_STATIC_LINK_LIBS glog_static)
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS glog)
  add_definitions("-DARROW_USE_GLOG")
endif()

add_custom_target(arrow_dependencies)
add_dependencies(arrow_dependencies toolchain)

if (ARROW_STATIC_LINK_LIBS)
  add_dependencies(arrow_dependencies ${ARROW_STATIC_LINK_LIBS})
endif()

set(ARROW_SHARED_PRIVATE_LINK_LIBS
  ${ARROW_STATIC_LINK_LIBS}
  ${BOOST_SYSTEM_LIBRARY}
  ${BOOST_FILESYSTEM_LIBRARY}
  ${BOOST_REGEX_LIBRARY})

list(APPEND
  ARROW_STATIC_LINK_LIBS
  ${BOOST_SYSTEM_LIBRARY}
  ${BOOST_FILESYSTEM_LIBRARY}
  ${BOOST_REGEX_LIBRARY})

list(APPEND
  ARROW_STATIC_INSTALL_INTERFACE_LIBS
  boost_system
  boost_filesystem
  boost_regex)

if (NOT MSVC)
  list(APPEND
    ARROW_LINK_LIBS
    ${CMAKE_DL_LIBS})
  list(APPEND
    ARROW_SHARED_INSTALL_INTERFACE_LIBS
    ${CMAKE_DL_LIBS})
endif()

set(ARROW_TEST_STATIC_LINK_LIBS
  arrow_testing_static
  arrow_static
  ${ARROW_LINK_LIBS}
  ${GTEST_LIBRARY}
  ${GMOCK_MAIN_LIBRARY}
  ${GMOCK_LIBRARY})

set(ARROW_TEST_SHARED_LINK_LIBS
  arrow_testing_shared
  arrow_shared
  ${ARROW_LINK_LIBS}
  double-conversion_static
  ${BOOST_SYSTEM_LIBRARY}
  ${BOOST_FILESYSTEM_LIBRARY}
  ${BOOST_REGEX_LIBRARY}
  ${GTEST_LIBRARY}
  ${GMOCK_MAIN_LIBRARY}
  ${GMOCK_LIBRARY})

if(NOT MSVC)
  set(ARROW_TEST_SHARED_LINK_LIBS
    ${ARROW_TEST_SHARED_LINK_LIBS}
    ${CMAKE_DL_LIBS})
endif()

if (ARROW_BUILD_TESTS AND "${ARROW_TEST_LINKAGE}" STREQUAL "shared")
  if (NOT ARROW_BUILD_SHARED)
    message(FATAL_ERROR "If using shared linkage for unit tests, must also \
pass ARROW_BUILD_SHARED=on")
  endif()
  # Use shared linking for unit tests if it's available
  set(ARROW_TEST_LINK_LIBS ${ARROW_TEST_SHARED_LINK_LIBS})
  set(ARROW_EXAMPLE_LINK_LIBS arrow_shared)
else()
  if (NOT ARROW_BUILD_STATIC)
    message(FATAL_ERROR "If using static linkage for unit tests, must also \
pass ARROW_BUILD_STATIC=on")
  endif()
  set(ARROW_TEST_LINK_LIBS ${ARROW_TEST_STATIC_LINK_LIBS})
  set(ARROW_EXAMPLE_LINK_LIBS arrow_static)
endif()

if (ARROW_BUILD_BENCHMARKS)
  set(ARROW_BENCHMARK_LINK_LIBS
    arrow_benchmark_main
    ${ARROW_TEST_LINK_LIBS})
endif()

set(ARROW_SYSTEM_LINK_LIBS)

if (ARROW_JEMALLOC)
  add_definitions(-DARROW_JEMALLOC)
  add_definitions(-DARROW_JEMALLOC_INCLUDE_DIR=${JEMALLOC_INCLUDE_DIR})
  list(APPEND ARROW_SYSTEM_LINK_LIBS jemalloc_static)
endif(ARROW_JEMALLOC)

if (THREADS_FOUND)
  list(APPEND ARROW_SYSTEM_LINK_LIBS Threads::Threads)
endif()

if (NOT WIN32 AND NOT APPLE)
  # Pass -lrt on Linux only
  list(APPEND ARROW_SYSTEM_LINK_LIBS rt)
endif()

list(APPEND ARROW_LINK_LIBS ${ARROW_SYSTEM_LINK_LIBS})
list(APPEND ARROW_STATIC_LINK_LIBS ${ARROW_SYSTEM_LINK_LIBS})
list(APPEND ARROW_SHARED_INSTALL_INTERFACE_LIBS ${ARROW_SYSTEM_LINK_LIBS})
list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_SYSTEM_LINK_LIBS})

############################################################
# Subdirectories
############################################################

if(NOT WIN32 AND ARROW_PLASMA)
  add_subdirectory(src/plasma)
endif()

add_subdirectory(src/arrow)

if(ARROW_PARQUET)
  add_subdirectory(src/parquet)
  add_subdirectory(tools/parquet)
  if (PARQUET_BUILD_EXAMPLES)
    add_subdirectory(examples/parquet)
  endif()
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
                              INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
                                 VERSION ${${PROJECT_NAME}_VERSION}
                                 COMPATIBILITY AnyNewerVersion)

install(EXPORT ${PROJECT_NAME}-targets
        FILE ${PROJECT_NAME}Targets.cmake
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
