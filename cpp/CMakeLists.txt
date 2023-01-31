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

cmake_minimum_required(VERSION 3.5)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

# Compiler id for Apple Clang is now AppleClang.
# https://www.cmake.org/cmake/help/latest/policy/CMP0025.html
cmake_policy(SET CMP0025 NEW)

# Only interpret if() arguments as variables or keywords when unquoted.
# https://www.cmake.org/cmake/help/latest/policy/CMP0054.html
cmake_policy(SET CMP0054 NEW)

# Support new if() IN_LIST operator.
# https://www.cmake.org/cmake/help/latest/policy/CMP0057.html
cmake_policy(SET CMP0057 NEW)

# Adapted from Apache Kudu: https://github.com/apache/kudu/commit/bd549e13743a51013585
# Honor visibility properties for all target types.
# https://www.cmake.org/cmake/help/latest/policy/CMP0063.html
cmake_policy(SET CMP0063 NEW)

# RPATH settings on macOS do not affect install_name.
# https://cmake.org/cmake/help/latest/policy/CMP0068.html
if(POLICY CMP0068)
  cmake_policy(SET CMP0068 NEW)
endif()

# find_package() uses <PackageName>_ROOT variables.
# https://cmake.org/cmake/help/latest/policy/CMP0074.html
if(POLICY CMP0074)
  cmake_policy(SET CMP0074 NEW)
endif()

set(ARROW_VERSION "12.0.0-SNAPSHOT")

string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" ARROW_BASE_VERSION "${ARROW_VERSION}")

# if no build build type is specified, default to release builds
if(NOT DEFINED CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE
      Release
      CACHE STRING "Choose the type of build.")
endif()
string(TOLOWER ${CMAKE_BUILD_TYPE} LOWERCASE_BUILD_TYPE)
string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules")

# this must be included before the project() command, because of the way
# vcpkg (ab)uses CMAKE_TOOLCHAIN_FILE to inject its logic into CMake
if(ARROW_DEPENDENCY_SOURCE STREQUAL "VCPKG")
  include(Usevcpkg)
endif()

project(arrow VERSION "${ARROW_BASE_VERSION}")

set(ARROW_VERSION_MAJOR "${arrow_VERSION_MAJOR}")
set(ARROW_VERSION_MINOR "${arrow_VERSION_MINOR}")
set(ARROW_VERSION_PATCH "${arrow_VERSION_PATCH}")
if(ARROW_VERSION_MAJOR STREQUAL ""
   OR ARROW_VERSION_MINOR STREQUAL ""
   OR ARROW_VERSION_PATCH STREQUAL "")
  message(FATAL_ERROR "Failed to determine Arrow version from '${ARROW_VERSION}'")
endif()

# The SO version is also the ABI version
if(ARROW_VERSION_MAJOR STREQUAL "0")
  # Arrow 0.x.y => SO version is "x", full SO version is "x.y.0"
  set(ARROW_SO_VERSION "${ARROW_VERSION_MINOR}")
  set(ARROW_FULL_SO_VERSION "${ARROW_SO_VERSION}.${ARROW_VERSION_PATCH}.0")
else()
  # Arrow 1.x.y => SO version is "10x", full SO version is "10x.y.0"
  math(EXPR ARROW_SO_VERSION "${ARROW_VERSION_MAJOR} * 100 + ${ARROW_VERSION_MINOR}")
  set(ARROW_FULL_SO_VERSION "${ARROW_SO_VERSION}.${ARROW_VERSION_PATCH}.0")
endif()

message(STATUS "Arrow version: "
               "${ARROW_VERSION_MAJOR}.${ARROW_VERSION_MINOR}.${ARROW_VERSION_PATCH} "
               "(full: '${ARROW_VERSION}')")
message(STATUS "Arrow SO version: ${ARROW_SO_VERSION} (full: ${ARROW_FULL_SO_VERSION})")

set(ARROW_SOURCE_DIR ${PROJECT_SOURCE_DIR})
set(ARROW_BINARY_DIR ${PROJECT_BINARY_DIR})

include(CMakePackageConfigHelpers)
include(CMakeParseArguments)
include(ExternalProject)
include(FindPackageHandleStandardArgs)

include(GNUInstallDirs)
if(IS_ABSOLUTE "${CMAKE_INSTALL_BINDIR}")
  set(ARROW_PKG_CONFIG_BINDIR "${CMAKE_INSTALL_BINDIR}")
else()
  set(ARROW_PKG_CONFIG_BINDIR "\${prefix}/${CMAKE_INSTALL_BINDIR}")
endif()
if(IS_ABSOLUTE "${CMAKE_INSTALL_INCLUDEDIR}")
  set(ARROW_PKG_CONFIG_INCLUDEDIR "${CMAKE_INSTALL_INCLUDEDIR}")
else()
  set(ARROW_PKG_CONFIG_INCLUDEDIR "\${prefix}/${CMAKE_INSTALL_INCLUDEDIR}")
endif()
if(IS_ABSOLUTE "${CMAKE_INSTALL_LIBDIR}")
  set(ARROW_PKG_CONFIG_LIBDIR "${CMAKE_INSTALL_LIBDIR}")
else()
  set(ARROW_PKG_CONFIG_LIBDIR "\${prefix}/${CMAKE_INSTALL_LIBDIR}")
endif()
set(ARROW_GDB_DIR "${CMAKE_INSTALL_DATADIR}/${PROJECT_NAME}/gdb")
set(ARROW_FULL_GDB_DIR "${CMAKE_INSTALL_FULL_DATADIR}/${PROJECT_NAME}/gdb")
set(ARROW_GDB_AUTO_LOAD_DIR "${CMAKE_INSTALL_DATADIR}/gdb/auto-load")
set(ARROW_CMAKE_DIR "${CMAKE_INSTALL_LIBDIR}/cmake")
set(ARROW_DOC_DIR "share/doc/${PROJECT_NAME}")

set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")

set(ARROW_LLVM_VERSIONS
    "15.0"
    "14.0"
    "13.0"
    "12.0"
    "11.1"
    "11.0"
    "10"
    "9"
    "8"
    "7")

file(READ ${CMAKE_CURRENT_SOURCE_DIR}/../.env ARROW_ENV)
string(REGEX MATCH "CLANG_TOOLS=[^\n]+" ARROW_ENV_CLANG_TOOLS_VERSION "${ARROW_ENV}")
string(REGEX REPLACE "^CLANG_TOOLS=" "" ARROW_CLANG_TOOLS_VERSION
                     "${ARROW_ENV_CLANG_TOOLS_VERSION}")
string(REGEX REPLACE "^([0-9]+)(\\..+)?" "\\1" ARROW_CLANG_TOOLS_VERSION_MAJOR
                     "${ARROW_CLANG_TOOLS_VERSION}")

if(WIN32 AND NOT MINGW)
  # This is used to handle builds using e.g. clang in an MSVC setting.
  set(MSVC_TOOLCHAIN TRUE)
else()
  set(MSVC_TOOLCHAIN FALSE)
endif()

find_package(ClangTools)
find_package(InferTools)
if("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1"
   OR CLANG_TIDY_FOUND
   OR INFER_FOUND)
  # Generate a Clang compile_commands.json "compilation database" file for use
  # with various development tools, such as Vim's YouCompleteMe plugin.
  # See http://clang.llvm.org/docs/JSONCompilationDatabase.html
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

# Needed for linting targets, etc.
if(${CMAKE_VERSION} VERSION_LESS "3.12.0")
  find_package(PythonInterp)
else()
  # Use the first Python installation on PATH, not the newest one
  set(Python3_FIND_STRATEGY "LOCATION")
  # On Windows, use registry last, not first
  set(Python3_FIND_REGISTRY "LAST")
  # On macOS, use framework last, not first
  set(Python3_FIND_FRAMEWORK "LAST")

  find_package(Python3)
  set(PYTHON_EXECUTABLE ${Python3_EXECUTABLE})
endif()

# ----------------------------------------------------------------------
# cmake options
include(DefineOptions)

if(ARROW_BUILD_SHARED AND NOT ARROW_POSITION_INDEPENDENT_CODE)
  message(WARNING "Can't disable position-independent code to build shared libraries, enabling"
  )
  set(ARROW_POSITION_INDEPENDENT_CODE ON)
endif()

if(ARROW_USE_SCCACHE
   AND NOT CMAKE_C_COMPILER_LAUNCHER
   AND NOT CMAKE_CXX_COMPILER_LAUNCHER)

  find_program(SCCACHE_FOUND sccache)

  if(NOT SCCACHE_FOUND AND DEFINED ENV{SCCACHE_PATH})
    # cmake has problems finding sccache from within mingw
    message(STATUS "Did not find sccache, using envvar fallback.")
    set(SCCACHE_FOUND $ENV{SCCACHE_PATH})
  endif()

  # Only use sccache if a storage backend is configured
  if(SCCACHE_FOUND
     AND (DEFINED ENV{SCCACHE_AZURE_BLOB_CONTAINER}
          OR DEFINED ENV{SCCACHE_BUCKET}
          OR DEFINED ENV{SCCACHE_DIR}
          OR DEFINED ENV{SCCACHE_GCS_BUCKET}
          OR DEFINED ENV{SCCACHE_MEMCACHED}
          OR DEFINED ENV{SCCACHE_REDIS}
         ))
    message(STATUS "Using sccache: ${SCCACHE_FOUND}")
    set(CMAKE_C_COMPILER_LAUNCHER ${SCCACHE_FOUND})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${SCCACHE_FOUND})
  endif()
endif()

if(ARROW_USE_CCACHE
   AND NOT CMAKE_C_COMPILER_LAUNCHER
   AND NOT CMAKE_CXX_COMPILER_LAUNCHER)

  find_program(CCACHE_FOUND ccache)

  if(CCACHE_FOUND)
    message(STATUS "Using ccache: ${CCACHE_FOUND}")
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_FOUND})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_FOUND})
    # ARROW-3985: let ccache preserve C++ comments, because some of them may be
    # meaningful to the compiler
    set(ENV{CCACHE_COMMENTS} "1")
  endif()
endif()

if(ARROW_USE_PRECOMPILED_HEADERS AND ${CMAKE_VERSION} VERSION_LESS "3.16.0")
  message(WARNING "Precompiled headers need CMake 3.16.0 or later, disabling")
  set(ARROW_USE_PRECOMPILED_HEADERS OFF)
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

find_program(CPPLINT_BIN
             NAMES cpplint cpplint.py
             HINTS ${BUILD_SUPPORT_DIR})
message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")

set(COMMON_LINT_OPTIONS
    --exclude_globs
    ${LINT_EXCLUSIONS_FILE}
    --source_dir
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    --source_dir
    ${CMAKE_CURRENT_SOURCE_DIR}/examples
    --source_dir
    ${CMAKE_CURRENT_SOURCE_DIR}/tools)

add_custom_target(lint
                  ${PYTHON_EXECUTABLE}
                  ${BUILD_SUPPORT_DIR}/run_cpplint.py
                  --cpplint_binary
                  ${CPPLINT_BIN}
                  ${COMMON_LINT_OPTIONS}
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
                    ${COMMON_LINT_OPTIONS}
                    --fix
                    ${ARROW_LINT_QUIET})

  # runs clang format and exits with a non-zero exit code if any files need to be reformatted
  add_custom_target(check-format
                    ${PYTHON_EXECUTABLE}
                    ${BUILD_SUPPORT_DIR}/run_clang_format.py
                    --clang_format_binary
                    ${CLANG_FORMAT_BIN}
                    ${COMMON_LINT_OPTIONS}
                    ${ARROW_LINT_QUIET})
endif()

add_custom_target(lint_cpp_cli ${PYTHON_EXECUTABLE} ${BUILD_SUPPORT_DIR}/lint_cpp_cli.py
                               ${CMAKE_CURRENT_SOURCE_DIR}/src)

if(ARROW_LINT_ONLY)
  message("ARROW_LINT_ONLY was specified, this is only a partial build directory")
  return()
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
                    --compile_commands
                    ${CMAKE_BINARY_DIR}/compile_commands.json
                    ${COMMON_LINT_OPTIONS}
                    --fix
                    ${ARROW_LINT_QUIET})

  # runs clang-tidy and exits with a non-zero exit code if any errors are found.
  add_custom_target(check-clang-tidy
                    ${PYTHON_EXECUTABLE}
                    ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
                    --clang_tidy_binary
                    ${CLANG_TIDY_BIN}
                    --compile_commands
                    ${CMAKE_BINARY_DIR}/compile_commands.json
                    ${COMMON_LINT_OPTIONS}
                    ${ARROW_LINT_QUIET})
endif()

if(UNIX)
  add_custom_target(iwyu ${BUILD_SUPPORT_DIR}/iwyu/iwyu.sh)
  add_custom_target(iwyu-all ${BUILD_SUPPORT_DIR}/iwyu/iwyu.sh all)
endif(UNIX)

# datetime code used by iOS requires zlib support
if(IOS)
  set(ARROW_WITH_ZLIB ON)
endif()

if(NOT ARROW_BUILD_TESTS)
  set(NO_TESTS 1)
else()
  add_custom_target(all-tests)
  add_custom_target(unittest
                    ctest
                    -j4
                    -L
                    unittest
                    --output-on-failure)
  add_dependencies(unittest all-tests)
endif()

if(ARROW_ENABLE_TIMING_TESTS)
  add_definitions(-DARROW_WITH_TIMING_TESTS)
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

if(ARROW_FUZZING)
  # Fuzzing builds enable ASAN without setting our home-grown option for it.
  add_definitions(-DADDRESS_SANITIZER)
endif()

if(ARROW_LARGE_MEMORY_TESTS)
  add_definitions(-DARROW_LARGE_MEMORY_TESTS)
endif()

if(ARROW_TEST_MEMCHECK)
  add_definitions(-DARROW_VALGRIND)
endif()

if(ARROW_USE_UBSAN)
  add_definitions(-DARROW_UBSAN)
endif()

#
# Compiler flags
#

if(ARROW_NO_DEPRECATED_API)
  add_definitions(-DARROW_NO_DEPRECATED_API)
endif()

if(ARROW_EXTRA_ERROR_CONTEXT)
  add_definitions(-DARROW_EXTRA_ERROR_CONTEXT)
endif()

include(SetupCxxFlags)

#
# Linker flags
#

# Localize thirdparty symbols using a linker version script. This hides them
# from the client application. The OS X linker does not support the
# version-script option.
if(CMAKE_VERSION VERSION_LESS 3.18)
  if(APPLE OR WIN32)
    set(CXX_LINKER_SUPPORTS_VERSION_SCRIPT FALSE)
  else()
    set(CXX_LINKER_SUPPORTS_VERSION_SCRIPT TRUE)
  endif()
else()
  include(CheckLinkerFlag)
  check_linker_flag(CXX
                    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/src/arrow/symbols.map"
                    CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
endif()

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
  execute_process(COMMAND ln ${MORE_ARGS} -sf ${BUILD_OUTPUT_ROOT_DIRECTORY}
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

if(CMAKE_GENERATOR STREQUAL Xcode)
  # Xcode projects support multi-configuration builds.  This forces a single output directory
  # when building with Xcode that is consistent with single-configuration Makefile driven build.
  set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${UPPERCASE_BUILD_TYPE}
      "${BUILD_OUTPUT_ROOT_DIRECTORY}")
  set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${UPPERCASE_BUILD_TYPE}
      "${BUILD_OUTPUT_ROOT_DIRECTORY}")
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${UPPERCASE_BUILD_TYPE}
      "${BUILD_OUTPUT_ROOT_DIRECTORY}")
endif()

#
# Dependencies
#

include(BuildUtils)
enable_testing()

# For arrow.pc. Cflags.private, Libs.private and Requires.private are
# used when "pkg-config --cflags --libs --static arrow" is used.
set(ARROW_PC_CFLAGS_PRIVATE " -DARROW_STATIC")
set(ARROW_PC_LIBS_PRIVATE "")
set(ARROW_PC_REQUIRES_PRIVATE "")

# For arrow-testing.pc.
set(ARROW_TESTING_PC_CFLAGS "")
set(ARROW_TESTING_PC_CFLAGS_PRIVATE " -DARROW_TESTING_STATIC")
set(ARROW_TESTING_PC_LIBS "")
set(ARROW_TESTING_PC_REQUIRES "")

include(ThirdpartyToolchain)

# Add common flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_COMMON_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ARROW_CXXFLAGS}")

# For any C code, use the same flags. These flags don't contain
# C++ specific flags.
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CXX_COMMON_FLAGS} ${ARROW_CXXFLAGS}")

# Remove --std=c++17 to avoid errors from C compilers
string(REPLACE "-std=c++17" "" CMAKE_C_FLAGS ${CMAKE_C_FLAGS})

# Add C++-only flags, like -std=c++17
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
message(STATUS "CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}: ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}"
)
message(STATUS "CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}: ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}"
)

include_directories(${CMAKE_CURRENT_BINARY_DIR}/src)
include_directories(src)

# Compiled flatbuffers files
include_directories(src/generated)

#
# Visibility
#
if(PARQUET_BUILD_SHARED)
  set_target_properties(arrow_shared
                        PROPERTIES C_VISIBILITY_PRESET hidden
                                   CXX_VISIBILITY_PRESET hidden
                                   VISIBILITY_INLINES_HIDDEN 1)
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
  add_custom_target(cscope
                    find
                    ${CMAKE_CURRENT_SOURCE_DIR}
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
  add_custom_target(infer ${BUILD_SUPPORT_DIR}/run-infer.sh ${INFER_BIN}
                          ${CMAKE_BINARY_DIR}/compile_commands.json 1)
  # runs infer analyze
  add_custom_target(infer-analyze ${BUILD_SUPPORT_DIR}/run-infer.sh ${INFER_BIN}
                                  ${CMAKE_BINARY_DIR}/compile_commands.json 2)
  # runs infer report
  add_custom_target(infer-report ${BUILD_SUPPORT_DIR}/run-infer.sh ${INFER_BIN}
                                 ${CMAKE_BINARY_DIR}/compile_commands.json 3)
endif()

#
# Linker and Dependencies
#

# Libraries to link statically with libarrow.so.
#
# Note that arrow::hadoop is a header only target that refers
# cpp/thirdparty/hadoop/include/. See
# cpp/cmake_modules/ThirdpartyToolchain.cmake for details.
set(ARROW_SHARED_LINK_LIBS arrow::flatbuffers arrow::hadoop)
set(ARROW_SHARED_INSTALL_INTERFACE_LIBS)
set(ARROW_STATIC_LINK_LIBS arrow::flatbuffers arrow::hadoop)
set(ARROW_STATIC_INSTALL_INTERFACE_LIBS)

if(ARROW_USE_BOOST)
  list(APPEND ARROW_SHARED_LINK_LIBS Boost::headers)
  list(APPEND ARROW_STATIC_LINK_LIBS Boost::headers)
endif()

if(ARROW_USE_OPENSSL)
  set(ARROW_OPENSSL_LIBS OpenSSL::Crypto OpenSSL::SSL)
  list(APPEND ARROW_SHARED_LINK_LIBS ${ARROW_OPENSSL_LIBS})
  list(APPEND ARROW_STATIC_LINK_LIBS ${ARROW_OPENSSL_LIBS})
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_OPENSSL_LIBS})
endif()

if(ARROW_WITH_BROTLI)
  # Order is important for static linking
  set(ARROW_BROTLI_LIBS Brotli::brotlienc Brotli::brotlidec Brotli::brotlicommon)
  list(APPEND ARROW_SHARED_LINK_LIBS ${ARROW_BROTLI_LIBS})
  list(APPEND ARROW_STATIC_LINK_LIBS ${ARROW_BROTLI_LIBS})
  if(Brotli_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_BROTLI_LIBS})
  endif()
endif()

if(ARROW_WITH_BZ2)
  list(APPEND ARROW_STATIC_LINK_LIBS BZip2::BZip2)
  if(BZip2_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS BZip2::BZip2)
  endif()
endif()

if(ARROW_WITH_LZ4)
  list(APPEND ARROW_STATIC_LINK_LIBS LZ4::lz4)
  if(lz4_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS LZ4::lz4)
  endif()
endif()

if(ARROW_WITH_SNAPPY)
  list(APPEND ARROW_STATIC_LINK_LIBS ${Snappy_TARGET})
  if(Snappy_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${Snappy_TARGET})
  endif()
endif()

if(ARROW_WITH_ZLIB)
  list(APPEND ARROW_STATIC_LINK_LIBS ZLIB::ZLIB)
  if(ZLIB_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ZLIB::ZLIB)
  endif()
endif()

if(ARROW_WITH_ZSTD)
  list(APPEND ARROW_STATIC_LINK_LIBS ${ARROW_ZSTD_LIBZSTD})
  if(zstd_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_ZSTD_LIBZSTD})
  endif()
endif()

if(ARROW_ORC)
  list(APPEND ARROW_SHARED_LINK_LIBS orc::liborc ${ARROW_PROTOBUF_LIBPROTOBUF})
  list(APPEND ARROW_STATIC_LINK_LIBS orc::liborc ${ARROW_PROTOBUF_LIBPROTOBUF})
  if(ORC_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS orc::liborc
         ${ARROW_PROTOBUF_LIBPROTOBUF})
  endif()
endif()

if(ARROW_GCS)
  list(APPEND ARROW_SHARED_LINK_LIBS google-cloud-cpp::storage)
  list(APPEND ARROW_STATIC_LINK_LIBS google-cloud-cpp::storage)
  if(google_cloud_cpp_storage_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS google-cloud-cpp::storage)
  endif()
endif()

if(ARROW_USE_GLOG)
  list(APPEND ARROW_SHARED_LINK_LIBS glog::glog)
  list(APPEND ARROW_STATIC_LINK_LIBS glog::glog)
  if(GLOG_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS glog::glog)
  endif()
  add_definitions("-DARROW_USE_GLOG")
endif()

if(ARROW_S3)
  list(APPEND ARROW_SHARED_LINK_LIBS ${AWSSDK_LINK_LIBRARIES})
  list(APPEND ARROW_STATIC_LINK_LIBS ${AWSSDK_LINK_LIBRARIES})
  if(AWSSDK_SOURCE STREQUAL "SYSTEM")
    list(APPEND
         ARROW_STATIC_INSTALL_INTERFACE_LIBS
         aws-cpp-sdk-identity-management
         aws-cpp-sdk-sts
         aws-cpp-sdk-cognito-identity
         aws-cpp-sdk-s3
         aws-cpp-sdk-core)
  elseif(AWSSDK_SOURCE STREQUAL "BUNDLED")
    if(UNIX)
      list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS CURL::libcurl)
    endif()
  endif()
endif()

if(ARROW_WITH_OPENTELEMETRY)
  list(APPEND
       ARROW_SHARED_LINK_LIBS
       opentelemetry-cpp::trace
       opentelemetry-cpp::ostream_span_exporter
       opentelemetry-cpp::otlp_http_exporter)
  list(APPEND
       ARROW_STATIC_LINK_LIBS
       opentelemetry-cpp::trace
       opentelemetry-cpp::ostream_span_exporter
       opentelemetry-cpp::otlp_http_exporter)
  if(opentelemetry_SOURCE STREQUAL "SYSTEM")
    list(APPEND
         ARROW_STATIC_INSTALL_INTERFACE_LIBS
         opentelemetry-cpp::trace
         opentelemetry-cpp::ostream_span_exporter
         opentelemetry-cpp::otlp_http_exporter)
  endif()
  if(Protobuf_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_PROTOBUF_LIBPROTOBUF})
  endif()
  list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS CURL::libcurl)
endif()

if(ARROW_WITH_UTF8PROC)
  list(APPEND ARROW_SHARED_LINK_LIBS utf8proc::utf8proc)
  list(APPEND ARROW_STATIC_LINK_LIBS utf8proc::utf8proc)
  if(utf8proc_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS utf8proc::utf8proc)
  endif()
endif()

if(ARROW_WITH_RE2)
  list(APPEND ARROW_SHARED_LINK_LIBS re2::re2)
  list(APPEND ARROW_STATIC_LINK_LIBS re2::re2)
  if(re2_SOURCE STREQUAL "SYSTEM")
    list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS re2::re2)
  endif()
endif()

if(ARROW_WITH_RAPIDJSON)
  list(APPEND ARROW_SHARED_LINK_LIBS rapidjson::rapidjson)
  list(APPEND ARROW_STATIC_LINK_LIBS rapidjson::rapidjson)
endif()

if(ARROW_USE_XSIMD)
  list(APPEND ARROW_SHARED_LINK_LIBS ${ARROW_XSIMD})
  list(APPEND ARROW_STATIC_LINK_LIBS ${ARROW_XSIMD})
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
  if(ARROW_HDFS OR ARROW_ORC)
    if(Protobuf_SOURCE STREQUAL "SYSTEM")
      list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${ARROW_PROTOBUF_LIBPROTOBUF})
    endif()
    if(NOT MSVC_TOOLCHAIN)
      list(APPEND ARROW_STATIC_LINK_LIBS ${CMAKE_DL_LIBS})
      list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS ${CMAKE_DL_LIBS})
    endif()
  endif()
endif()

set(ARROW_SHARED_PRIVATE_LINK_LIBS ${ARROW_STATIC_LINK_LIBS})

if(NOT MSVC_TOOLCHAIN)
  list(APPEND ARROW_SHARED_LINK_LIBS ${CMAKE_DL_LIBS})
endif()

set(ARROW_TEST_LINK_TOOLCHAIN arrow::flatbuffers GTest::gtest_main GTest::gtest
                              GTest::gmock)

if(ARROW_BUILD_TESTS)
  add_dependencies(arrow_test_dependencies ${ARROW_TEST_LINK_TOOLCHAIN})
endif()

if(ARROW_BUILD_BENCHMARKS)
  # Some benchmarks use gtest
  add_dependencies(arrow_benchmark_dependencies arrow_test_dependencies
                   toolchain-benchmarks)
endif()

set(ARROW_TEST_STATIC_LINK_LIBS arrow_testing_static arrow_static
                                ${ARROW_SHARED_LINK_LIBS} ${ARROW_TEST_LINK_TOOLCHAIN})

set(ARROW_TEST_SHARED_LINK_LIBS arrow_testing_shared arrow_shared
                                ${ARROW_SHARED_LINK_LIBS} ${ARROW_TEST_LINK_TOOLCHAIN})

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
  if(WIN32)
    set(ARROW_BENCHMARK_LINK_LIBS Shlwapi.dll ${ARROW_BENCHMARK_LINK_LIBS})
  endif()
endif()

if(ARROW_JEMALLOC)
  list(APPEND ARROW_SHARED_LINK_LIBS jemalloc::jemalloc)
  list(APPEND ARROW_STATIC_LINK_LIBS jemalloc::jemalloc)
endif()

if(ARROW_MIMALLOC)
  add_definitions(-DARROW_MIMALLOC)
  list(APPEND ARROW_SHARED_LINK_LIBS mimalloc::mimalloc)
  list(APPEND ARROW_STATIC_LINK_LIBS mimalloc::mimalloc)
endif()

# ----------------------------------------------------------------------
# Handle platform-related libraries like -pthread

set(ARROW_SYSTEM_LINK_LIBS)

list(APPEND ARROW_SYSTEM_LINK_LIBS Threads::Threads)
if(CMAKE_THREAD_LIBS_INIT)
  string(APPEND ARROW_PC_LIBS_PRIVATE " ${CMAKE_THREAD_LIBS_INIT}")
endif()

if(WIN32)
  # Winsock
  list(APPEND ARROW_SYSTEM_LINK_LIBS "ws2_32.dll")
endif()

if(NOT WIN32 AND NOT APPLE)
  # Pass -lrt on Linux only
  list(APPEND ARROW_SYSTEM_LINK_LIBS rt)
endif()

list(APPEND ARROW_SHARED_LINK_LIBS ${ARROW_SYSTEM_LINK_LIBS})
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

if(ARROW_GANDIVA)
  add_subdirectory(src/gandiva)
endif()

if(ARROW_SKYHOOK)
  add_subdirectory(src/skyhook)
endif()

if(ARROW_BUILD_EXAMPLES)
  add_custom_target(runexample ctest -L example)
  add_subdirectory(examples/arrow)
endif()

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/../LICENSE.txt
              ${CMAKE_CURRENT_SOURCE_DIR}/../NOTICE.txt
              ${CMAKE_CURRENT_SOURCE_DIR}/README.md DESTINATION "${ARROW_DOC_DIR}")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gdb_arrow.py DESTINATION "${ARROW_GDB_DIR}")

#
# Validate and print out Arrow configuration options
#

validate_config()
config_summary_message()
if(${ARROW_BUILD_CONFIG_SUMMARY_JSON})
  config_summary_json()
endif()
