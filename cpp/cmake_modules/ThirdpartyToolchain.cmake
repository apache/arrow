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

add_custom_target(rapidjson)
add_custom_target(toolchain)
add_custom_target(toolchain-benchmarks)
add_custom_target(toolchain-tests)

# ----------------------------------------------------------------------
# Toolchain linkage options

set(ARROW_RE2_LINKAGE
    "static"
    CACHE STRING "How to link the re2 library. static|shared (default static)")

if(ARROW_PROTOBUF_USE_SHARED)
  set(Protobuf_USE_STATIC_LIBS OFF)
else()
  set(Protobuf_USE_STATIC_LIBS ON)
endif()

# ----------------------------------------------------------------------
# Resolve the dependencies

# TODO: add uriparser here when it gets a conda package
set(ARROW_THIRDPARTY_DEPENDENCIES
    benchmark
    BOOST
    Brotli
    BZip2
    c-ares
    double-conversion
    Flatbuffers
    gflags
    GLOG
    gRPC
    GTest
    LLVM
    Lz4
    RE2
    Protobuf
    RapidJSON
    Snappy
    Thrift
    ZLIB
    ZSTD)

# TODO(wesm): External GTest shared libraries are not currently
# supported when building with MSVC because of the way that
# conda-forge packages have 4 variants of the libraries packaged
# together
if(MSVC AND "${GTest_SOURCE}" STREQUAL "")
  set(GTest_SOURCE "BUNDLED")
endif()

message(STATUS "Using ${ARROW_DEPENDENCY_SOURCE} approach to find dependencies")

# TODO: double-conversion check fails for conda, it should not
if(ARROW_DEPENDENCY_SOURCE STREQUAL "CONDA")
  if(MSVC)
    set(ARROW_PACKAGE_PREFIX "$ENV{CONDA_PREFIX}/Library")
  else()
    set(ARROW_PACKAGE_PREFIX $ENV{CONDA_PREFIX})
  endif()
  set(ARROW_ACTUAL_DEPENDENCY_SOURCE "SYSTEM")
  message(STATUS "Using CONDA_PREFIX for ARROW_PACKAGE_PREFIX: ${ARROW_PACKAGE_PREFIX}")
else()
  set(ARROW_ACTUAL_DEPENDENCY_SOURCE "${ARROW_DEPENDENCY_SOURCE}")
endif()

if(ARROW_PACKAGE_PREFIX)
  message(STATUS "Setting (unset) dependency *_ROOT variables: ${ARROW_PACKAGE_PREFIX}")
  set(ENV{PKG_CONFIG_PATH} "${ARROW_PACKAGE_PREFIX}/lib/pkgconfig/")

  if(NOT ENV{BOOST_ROOT})
    set(ENV{BOOST_ROOT} ${ARROW_PACKAGE_PREFIX})
  endif()
  if(NOT ENV{Boost_ROOT})
    set(ENV{Boost_ROOT} ${ARROW_PACKAGE_PREFIX})
  endif()
endif()

# For each dependency, set dependency source to global default, if unset
foreach(DEPENDENCY ${ARROW_THIRDPARTY_DEPENDENCIES})
  if("${${DEPENDENCY}_SOURCE}" STREQUAL "")
    set(${DEPENDENCY}_SOURCE ${ARROW_ACTUAL_DEPENDENCY_SOURCE})
    # If no ROOT was supplied and we have a global prefix, use it
    if(NOT ${DEPENDENCY}_ROOT AND ARROW_PACKAGE_PREFIX)
      set(${DEPENDENCY}_ROOT ${ARROW_PACKAGE_PREFIX})
    endif()
  endif()
endforeach()

macro(build_dependency DEPENDENCY_NAME)
  if("${DEPENDENCY_NAME}" STREQUAL "benchmark")
    build_benchmark()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Brotli")
    build_brotli()
  elseif("${DEPENDENCY_NAME}" STREQUAL "BZip2")
    build_bzip2()
  elseif("${DEPENDENCY_NAME}" STREQUAL "c-ares")
    build_cares()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Flatbuffers")
    build_flatbuffers()
  elseif("${DEPENDENCY_NAME}" STREQUAL "gflags")
    build_gflags()
  elseif("${DEPENDENCY_NAME}" STREQUAL "GLOG")
    build_glog()
  elseif("${DEPENDENCY_NAME}" STREQUAL "gRPC")
    build_grpc()
  elseif("${DEPENDENCY_NAME}" STREQUAL "GTest")
    build_gtest()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Lz4")
    build_lz4()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Protobuf")
    build_protobuf()
  elseif("${DEPENDENCY_NAME}" STREQUAL "RE2")
    build_re2()
  elseif("${DEPENDENCY_NAME}" STREQUAL "Thrift")
    build_thrift()
  elseif("${DEPENDENCY_NAME}" STREQUAL "uriparser")
    build_uriparser()
  elseif("${DEPENDENCY_NAME}" STREQUAL "ZLIB")
    build_zlib()
  elseif("${DEPENDENCY_NAME}" STREQUAL "ZSTD")
    build_zstd()
  else()
    message(FATAL_ERROR "Unknown thirdparty dependency to build: ${DEPENDENCY_NAME}")
  endif()
endmacro()

macro(resolve_dependency DEPENDENCY_NAME)
  if(${DEPENDENCY_NAME}_SOURCE STREQUAL "AUTO")
    find_package(${DEPENDENCY_NAME} MODULE)
    if(NOT ${${DEPENDENCY_NAME}_FOUND})
      build_dependency(${DEPENDENCY_NAME})
    endif()
  elseif(${DEPENDENCY_NAME}_SOURCE STREQUAL "BUNDLED")
    build_dependency(${DEPENDENCY_NAME})
  elseif(${DEPENDENCY_NAME}_SOURCE STREQUAL "SYSTEM")
    find_package(${DEPENDENCY_NAME} REQUIRED)
  endif()
endmacro()

# ----------------------------------------------------------------------
# Thirdparty versions, environment variables, source URLs

set(THIRDPARTY_DIR "${arrow_SOURCE_DIR}/thirdparty")

if(DEFINED ENV{ORC_HOME})
  set(ORC_HOME "$ENV{ORC_HOME}")
endif()

# ----------------------------------------------------------------------
# Some EP's require other EP's

if(ARROW_THRIFT OR ARROW_WITH_ZLIB)
  set(ARROW_WITH_ZLIB ON)
endif()

if(ARROW_HIVESERVER2 OR ARROW_PARQUET)
  set(ARROW_WITH_THRIFT ON)
else()
  set(ARROW_WITH_THRIFT OFF)
endif()

if(ARROW_FLIGHT)
  set(ARROW_WITH_GRPC ON)
  set(ARROW_WITH_URIPARSER ON)
endif()

if(ARROW_FLIGHT OR ARROW_IPC)
  set(ARROW_WITH_FLATBUFFERS ON)
  set(ARROW_WITH_RAPIDJSON ON)
endif()

if(ARROW_ORC OR ARROW_FLIGHT OR ARROW_GANDIVA)
  set(ARROW_WITH_PROTOBUF ON)
endif()

# ----------------------------------------------------------------------
# Versions and URLs for toolchain builds, which also can be used to configure
# offline builds

# Read toolchain versions from cpp/thirdparty/versions.txt
file(STRINGS "${THIRDPARTY_DIR}/versions.txt" TOOLCHAIN_VERSIONS_TXT)
foreach(_VERSION_ENTRY ${TOOLCHAIN_VERSIONS_TXT})
  # Exclude comments
  if(NOT _VERSION_ENTRY MATCHES "^[^#][A-Za-z0-9-_]+_VERSION=")
    continue()
  endif()

  string(REGEX MATCH "^[^=]*" _LIB_NAME ${_VERSION_ENTRY})
  string(REPLACE "${_LIB_NAME}=" "" _LIB_VERSION ${_VERSION_ENTRY})

  # Skip blank or malformed lines
  if(${_LIB_VERSION} STREQUAL "")
    continue()
  endif()

  # For debugging
  message(STATUS "${_LIB_NAME}: ${_LIB_VERSION}")

  set(${_LIB_NAME} "${_LIB_VERSION}")
endforeach()

if(DEFINED ENV{ARROW_BOOST_URL})
  set(BOOST_SOURCE_URL "$ENV{ARROW_BOOST_URL}")
else()
  string(REPLACE "." "_" BOOST_VERSION_UNDERSCORES ${BOOST_VERSION})
  set(
    BOOST_SOURCE_URL
    "https://dl.bintray.com/boostorg/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_UNDERSCORES}.tar.gz"
    )
endif()

if(DEFINED ENV{ARROW_BROTLI_URL})
  set(BROTLI_SOURCE_URL "$ENV{ARROW_BROTLI_URL}")
else()
  set(BROTLI_SOURCE_URL
      "https://github.com/google/brotli/archive/${BROTLI_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_CARES_URL})
  set(CARES_SOURCE_URL "$ENV{ARROW_CARES_URL}")
else()
  set(CARES_SOURCE_URL "https://c-ares.haxx.se/download/c-ares-${CARES_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_DOUBLE_CONVERSION_URL})
  set(DOUBLE_CONVERSION_SOURCE_URL "$ENV{ARROW_DOUBLE_CONVERSION_URL}")
else()
  set(
    DOUBLE_CONVERSION_SOURCE_URL
    "https://github.com/google/double-conversion/archive/${DOUBLE_CONVERSION_VERSION}.tar.gz"
    )
endif()

if(DEFINED ENV{ARROW_FLATBUFFERS_URL})
  set(FLATBUFFERS_SOURCE_URL "$ENV{ARROW_FLATBUFFERS_URL}")
else()
  set(FLATBUFFERS_SOURCE_URL
      "https://github.com/google/flatbuffers/archive/${FLATBUFFERS_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_GBENCHMARK_URL})
  set(GBENCHMARK_SOURCE_URL "$ENV{ARROW_GBENCHMARK_URL}")
else()
  set(GBENCHMARK_SOURCE_URL
      "https://github.com/google/benchmark/archive/${GBENCHMARK_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_GFLAGS_URL})
  set(GFLAGS_SOURCE_URL "$ENV{ARROW_GFLAGS_URL}")
else()
  set(GFLAGS_SOURCE_URL
      "https://github.com/gflags/gflags/archive/${GFLAGS_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_GLOG_URL})
  set(GLOG_SOURCE_URL "$ENV{ARROW_GLOG_URL}")
else()
  set(GLOG_SOURCE_URL "https://github.com/google/glog/archive/${GLOG_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_GRPC_URL})
  set(GRPC_SOURCE_URL "$ENV{ARROW_GRPC_URL}")
else()
  set(GRPC_SOURCE_URL "https://github.com/grpc/grpc/archive/${GRPC_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_GTEST_URL})
  set(GTEST_SOURCE_URL "$ENV{ARROW_GTEST_URL}")
else()
  set(GTEST_SOURCE_URL
      "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_JEMALLOC_URL})
  set(JEMALLOC_SOURCE_URL "$ENV{ARROW_JEMALLOC_URL}")
else()
  set(JEMALLOC_SOURCE_URL
      "https://github.com/jemalloc/jemalloc/archive/${JEMALLOC_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_LZ4_URL})
  set(LZ4_SOURCE_URL "$ENV{ARROW_LZ4_URL}")
else()
  set(LZ4_SOURCE_URL "https://github.com/lz4/lz4/archive/${LZ4_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_ORC_URL})
  set(ORC_SOURCE_URL "$ENV{ARROW_ORC_URL}")
else()
  set(ORC_SOURCE_URL
      "https://github.com/apache/orc/archive/rel/release-${ORC_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_PROTOBUF_URL})
  set(PROTOBUF_SOURCE_URL "$ENV{ARROW_PROTOBUF_URL}")
else()
  string(SUBSTRING ${PROTOBUF_VERSION} 1 -1 STRIPPED_PROTOBUF_VERSION)
  # strip the leading `v`
  set(
    PROTOBUF_SOURCE_URL
    "https://github.com/protocolbuffers/protobuf/releases/download/${PROTOBUF_VERSION}/protobuf-all-${STRIPPED_PROTOBUF_VERSION}.tar.gz"
    )
endif()

if(DEFINED ENV{ARROW_RE2_URL})
  set(RE2_SOURCE_URL "$ENV{ARROW_RE2_URL}")
else()
  set(RE2_SOURCE_URL "https://github.com/google/re2/archive/${RE2_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_RAPIDJSON_URL})
  set(RAPIDJSON_SOURCE_URL "$ENV{ARROW_RAPIDJSON_URL}")
else()
  set(RAPIDJSON_SOURCE_URL
      "https://github.com/miloyip/rapidjson/archive/${RAPIDJSON_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_SNAPPY_URL})
  set(SNAPPY_SOURCE_URL "$ENV{ARROW_SNAPPY_URL}")
else()
  set(SNAPPY_SOURCE_URL
      "https://github.com/google/snappy/archive/${SNAPPY_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_THRIFT_URL})
  set(THRIFT_SOURCE_URL "$ENV{ARROW_THRIFT_URL}")
else()
  set(
    THRIFT_SOURCE_URL
    "http://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz"
    )
endif()

if(DEFINED ENV{ARROW_URIPARSER_URL})
  set(URIPARSER_SOURCE_URL "$ENV{ARROW_URIPARSER_URL}")
else()
  set(
    URIPARSER_SOURCE_URL

    "https://github.com/uriparser/uriparser/archive/uriparser-${URIPARSER_VERSION}.tar.gz"
    )
endif()

if(DEFINED ENV{ARROW_ZLIB_URL})
  set(ZLIB_SOURCE_URL "$ENV{ARROW_ZLIB_URL}")
else()
  set(ZLIB_SOURCE_URL "http://zlib.net/fossils/zlib-${ZLIB_VERSION}.tar.gz")
endif()

if(DEFINED ENV{ARROW_ZSTD_URL})
  set(ZSTD_SOURCE_URL "$ENV{ARROW_ZSTD_URL}")
else()
  set(ZSTD_SOURCE_URL "https://github.com/facebook/zstd/archive/${ZSTD_VERSION}.tar.gz")
endif()

if(DEFINED ENV{BZIP2_SOURCE_URL})
  set(BZIP2_SOURCE_URL "$ENV{BZIP2_SOURCE_URL}")
else()
  set(BZIP2_SOURCE_URL "https://fossies.org/linux/misc/bzip2-${BZIP2_VERSION}.tar.gz")
endif()

# ----------------------------------------------------------------------
# ExternalProject options

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

if(NOT MSVC)
  # Set -fPIC on all external projects
  set(EP_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")
  set(EP_C_FLAGS "${EP_C_FLAGS} -fPIC")
endif()

# CC/CXX environment variables are captured on the first invocation of the
# builder (e.g make or ninja) instead of when CMake is invoked into to build
# directory. This leads to issues if the variables are exported in a subshell
# and the invocation of make/ninja is in distinct subshell without the same
# environment (CC/CXX).
set(EP_COMMON_TOOLCHAIN -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
                        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})

if(CMAKE_AR)
  set(EP_COMMON_TOOLCHAIN ${EP_COMMON_TOOLCHAIN} -DCMAKE_AR=${CMAKE_AR})
endif()

if(CMAKE_RANLIB)
  set(EP_COMMON_TOOLCHAIN ${EP_COMMON_TOOLCHAIN} -DCMAKE_RANLIB=${CMAKE_RANLIB})
endif()

# External projects are still able to override the following declarations.
# cmake command line will favor the last defined variable when a duplicate is
# encountered. This requires that `EP_COMMON_CMAKE_ARGS` is always the first
# argument.
set(EP_COMMON_CMAKE_ARGS
    ${EP_COMMON_TOOLCHAIN}
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_C_FLAGS=${EP_C_FLAGS}
    -DCMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_C_FLAGS}
    -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}
    -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_CXX_FLAGS})

if(NOT ARROW_VERBOSE_THIRDPARTY_BUILD)
  set(EP_LOG_OPTIONS
      LOG_CONFIGURE
      1
      LOG_BUILD
      1
      LOG_INSTALL
      1
      LOG_DOWNLOAD
      1)
  set(Boost_DEBUG FALSE)
else()
  set(EP_LOG_OPTIONS)
  set(Boost_DEBUG TRUE)
endif()

# Ensure that a default make is set
if("${MAKE}" STREQUAL "")
  if(NOT MSVC)
    find_program(MAKE make)
  endif()
endif()

# Using make -j in sub-make is fragile
# see discussion https://github.com/apache/arrow/pull/2779
if(${CMAKE_GENERATOR} MATCHES "Makefiles")
  set(MAKE_BUILD_ARGS "")
else()
  # limit the maximum number of jobs for ninja
  set(MAKE_BUILD_ARGS "-j4")
endif()

# ----------------------------------------------------------------------
# Find pthreads

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# ----------------------------------------------------------------------
# Google double-conversion

macro(build_double_conversion)
  message(STATUS "Building double-conversion from source")
  set(DOUBLE_CONVERSION_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/double-conversion_ep/src/double-conversion_ep")
  set(double-conversion_INCLUDE_DIRS "${DOUBLE_CONVERSION_PREFIX}/include")
  set(
    DOUBLE_CONVERSION_STATIC_LIB
    "${DOUBLE_CONVERSION_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}double-conversion${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )

  set(DOUBLE_CONVERSION_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS}
                                   "-DCMAKE_INSTALL_PREFIX=${DOUBLE_CONVERSION_PREFIX}")

  externalproject_add(double-conversion_ep
                      ${EP_LOG_OPTIONS}
                      INSTALL_DIR ${DOUBLE_CONVERSION_PREFIX}
                      URL ${DOUBLE_CONVERSION_SOURCE_URL}
                      CMAKE_ARGS ${DOUBLE_CONVERSION_CMAKE_ARGS}
                      BUILD_BYPRODUCTS "${DOUBLE_CONVERSION_STATIC_LIB}")

  add_library(double-conversion STATIC IMPORTED)
  set_target_properties(double-conversion
                        PROPERTIES IMPORTED_LOCATION "${DOUBLE_CONVERSION_STATIC_LIB}")
  add_dependencies(toolchain double-conversion_ep)
  add_dependencies(double-conversion double-conversion_ep)
  set(double-conversion_LIBRARIES double-conversion)
endmacro()

macro(double_conversion_config)
  # Map the newer target to the old, simpler setting
  if(TARGET double-conversion::double-conversion)
    set(double-conversion_LIBRARIES double-conversion::double-conversion)
    get_target_property(double-conversion_INCLUDE_DIRS
                        double-conversion::double-conversion
                        INTERFACE_INCLUDE_DIRECTORIES)
  endif()
endmacro()

macro(double_conversion_compability)
  check_cxx_source_compiles("
#include <double-conversion/double-conversion.h>
int main() {
const int flags_ = double_conversion::StringToDoubleConverter::ALLOW_CASE_INSENSIBILITY;
      }" DOUBLE_CONVERSION_HAS_CASE_INSENSIBILITY)
endmacro()

if(double-conversion_SOURCE STREQUAL "AUTO")
  # Debian does not ship cmake configs for double-conversion
  # TODO: Make upstream bug
  find_package(double-conversion QUIET)
  if(NOT double-conversion_FOUND)
    find_package(DoubleConversion)
  endif()
  if(double-conversion_FOUND OR DoubleConversion_FOUND)
    double_conversion_config()
  else()
    build_double_conversion()
  endif()
elseif(double-conversion_SOURCE STREQUAL "BUNDLED")
  build_double_conversion()
elseif(double-conversion_SOURCE STREQUAL "SYSTEM")
  # Debian does not ship cmake configs for double-conversion
  # TODO: Make upstream bug
  find_package(double-conversion)
  if(NOT double-conversion_FOUND)
    find_package(DoubleConversion REQUIRED)
  endif()

  double_conversion_config()
endif()
# TODO: Don't use global includes but rather target_include_directories
include_directories(SYSTEM ${double-conversion_INCLUDE_DIRS})

double_conversion_compability()

# ----------------------------------------------------------------------
# uriparser library

macro(build_uriparser)
  message(STATUS "Building uriparser from source")
  set(URIPARSER_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/uriparser_ep-install")
  set(
    URIPARSER_STATIC_LIB
    "${URIPARSER_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}uriparser${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(URIPARSER_INCLUDE_DIRS "${URIPARSER_PREFIX}/include")

  set(URIPARSER_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      "-DURIPARSER_BUILD_DOCS=off"
      "-DURIPARSER_BUILD_TESTS=off"
      "-DURIPARSER_BUILD_TOOLS=off"
      "-DURIPARSER_BUILD_WCHAR_T=off"
      "-DBUILD_SHARED_LIBS=off"
      "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
      "-DCMAKE_INSTALL_LIBDIR=lib"
      "-DCMAKE_POSITION_INDEPENDENT_CODE=on"
      "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>")

  if(MSVC AND ARROW_USE_STATIC_CRT)
    if("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
      list(APPEND URIPARSER_CMAKE_ARGS "-DURIPARSER_MSVC_RUNTIME=/MTd")
    else()
      list(APPEND URIPARSER_CMAKE_ARGS "-DURIPARSER_MSVC_RUNTIME=/MT")
    endif()
  endif()

  externalproject_add(uriparser_ep
                      URL ${URIPARSER_SOURCE_URL}
                      CMAKE_ARGS ${URIPARSER_CMAKE_ARGS}
                      BUILD_BYPRODUCTS ${URIPARSER_STATIC_LIB}
                      INSTALL_DIR ${URIPARSER_PREFIX}
                      ${EP_LOG_OPTIONS})

  add_library(uriparser::uriparser STATIC IMPORTED)
  # Work around https://gitlab.kitware.com/cmake/cmake/issues/15052
  file(MAKE_DIRECTORY ${URIPARSER_INCLUDE_DIRS})
  set_target_properties(uriparser::uriparser
                        PROPERTIES IMPORTED_LOCATION ${URIPARSER_STATIC_LIB}
                                   INTERFACE_INCLUDE_DIRECTORIES ${URIPARSER_INCLUDE_DIRS}
                                   # URI_STATIC_BUILD required on Windows
                                   INTERFACE_COMPILE_DEFINITIONS
                                   "URI_STATIC_BUILD;URI_NO_UNICODE")

  add_dependencies(toolchain uriparser_ep)
  add_dependencies(uriparser::uriparser uriparser_ep)
endmacro()

if(ARROW_WITH_URIPARSER)
  # Unless the user overrides uriparser_SOURCE, build uriparser ourselves
  if("${uriparser_SOURCE}" STREQUAL "")
    set(uriparser_SOURCE "BUNDLED")
  endif()

  resolve_dependency(uriparser)

  get_target_property(URIPARSER_INCLUDE_DIRS uriparser::uriparser
                      INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${URIPARSER_INCLUDE_DIRS})
endif()

# ----------------------------------------------------------------------
# Snappy

macro(build_snappy)
  message(STATUS "Building snappy from source")
  set(SNAPPY_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/snappy_ep/src/snappy_ep-install")
  set(SNAPPY_STATIC_LIB_NAME snappy)
  set(
    SNAPPY_STATIC_LIB
    "${SNAPPY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${SNAPPY_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )

  set(SNAPPY_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} -DCMAKE_INSTALL_LIBDIR=lib
                        -DSNAPPY_BUILD_TESTS=OFF
                        "-DCMAKE_INSTALL_PREFIX=${SNAPPY_PREFIX}")

  externalproject_add(snappy_ep
                      ${EP_LOG_OPTIONS}
                      BUILD_IN_SOURCE 1
                      INSTALL_DIR ${SNAPPY_PREFIX}
                      URL ${SNAPPY_SOURCE_URL}
                      CMAKE_ARGS ${SNAPPY_CMAKE_ARGS}
                      BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}")

  file(MAKE_DIRECTORY "${SNAPPY_PREFIX}/include")

  add_library(Snappy::snappy STATIC IMPORTED)
  set_target_properties(Snappy::snappy
                        PROPERTIES IMPORTED_LOCATION "${SNAPPY_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${SNAPPY_PREFIX}/include")
  add_dependencies(toolchain snappy_ep)
  add_dependencies(Snappy::snappy snappy_ep)
endmacro()

if(ARROW_WITH_SNAPPY)
  if(Snappy_SOURCE STREQUAL "AUTO")
    # Normally *Config.cmake files reside in /usr/lib/cmake but Snappy
    # errornously places them in ${CMAKE_ROOT}/Modules/
    # This is fixed in 1.1.7 but fedora (30) still installs into the wrong
    # location.
    # https://bugzilla.redhat.com/show_bug.cgi?id=1679727
    # https://src.fedoraproject.org/rpms/snappy/pull-request/1
    find_package(Snappy QUIET HINTS "${CMAKE_ROOT}/Modules/")
    if(NOT Snappy_FOUND)
      find_package(SnappyAlt)
    endif()
    if(NOT Snappy_FOUND AND NOT SnappyAlt_FOUND)
      build_snappy()
    endif()
  elseif(Snappy_SOURCE STREQUAL "BUNDLED")
    build_snappy()
  elseif(Snappy_SOURCE STREQUAL "SYSTEM")
    # SnappyConfig.cmake is not installed on Ubuntu/Debian
    # TODO: Make a bug report upstream
    find_package(Snappy HINTS "${CMAKE_ROOT}/Modules/")
    if(NOT Snappy_FOUND)
      find_package(SnappyAlt REQUIRED)
    endif()
  endif()

  # TODO: Don't use global includes but rather target_include_directories
  get_target_property(SNAPPY_INCLUDE_DIRS Snappy::snappy INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${SNAPPY_INCLUDE_DIRS})
endif()

# ----------------------------------------------------------------------
# Brotli

macro(build_brotli)
  message(STATUS "Building brotli from source")
  set(BROTLI_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/brotli_ep/src/brotli_ep-install")
  set(BROTLI_INCLUDE_DIR "${BROTLI_PREFIX}/include")
  set(BROTLI_LIB_DIR lib)
  set(
    BROTLI_STATIC_LIBRARY_ENC
    "${BROTLI_PREFIX}/${BROTLI_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc-static${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    BROTLI_STATIC_LIBRARY_DEC
    "${BROTLI_PREFIX}/${BROTLI_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec-static${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    BROTLI_STATIC_LIBRARY_COMMON
    "${BROTLI_PREFIX}/${BROTLI_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon-static${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(BROTLI_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=${BROTLI_PREFIX}"
                        -DCMAKE_INSTALL_LIBDIR=lib -DBUILD_SHARED_LIBS=OFF)

  externalproject_add(brotli_ep
                      URL ${BROTLI_SOURCE_URL}
                      BUILD_BYPRODUCTS "${BROTLI_STATIC_LIBRARY_ENC}"
                                       "${BROTLI_STATIC_LIBRARY_DEC}"
                                       "${BROTLI_STATIC_LIBRARY_COMMON}"
                                       ${BROTLI_BUILD_BYPRODUCTS}
                                       ${EP_LOG_OPTIONS}
                      CMAKE_ARGS ${BROTLI_CMAKE_ARGS}
                      STEP_TARGETS headers_copy)

  add_dependencies(toolchain brotli_ep)
  file(MAKE_DIRECTORY "${BROTLI_INCLUDE_DIR}")

  add_library(Brotli::brotlicommon STATIC IMPORTED)
  set_target_properties(Brotli::brotlicommon
                        PROPERTIES IMPORTED_LOCATION "${BROTLI_STATIC_LIBRARY_COMMON}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")
  add_dependencies(Brotli::brotlicommon brotli_ep)

  add_library(Brotli::brotlienc STATIC IMPORTED)
  set_target_properties(Brotli::brotlienc
                        PROPERTIES IMPORTED_LOCATION "${BROTLI_STATIC_LIBRARY_ENC}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")
  add_dependencies(Brotli::brotlienc brotli_ep)

  add_library(Brotli::brotlidec STATIC IMPORTED)
  set_target_properties(Brotli::brotlidec
                        PROPERTIES IMPORTED_LOCATION "${BROTLI_STATIC_LIBRARY_DEC}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")
  add_dependencies(Brotli::brotlidec brotli_ep)
endmacro()

if(ARROW_WITH_BROTLI)
  resolve_dependency(Brotli)
  # TODO: Don't use global includes but rather target_include_directories
  get_target_property(BROTLI_INCLUDE_DIR Brotli::brotlicommon
                      INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${BROTLI_INCLUDE_DIR})
endif()

if(PARQUET_BUILD_ENCRYPTION OR ARROW_WITH_GRPC)
  find_package(OpenSSL REQUIRED)
  # OpenSSL::SSL and OpenSSL::Crypto
  # are not available in older CMake versions (CMake < v3.2).
  if(NOT TARGET OpenSSL::SSL)
    add_library(OpenSSL::SSL UNKNOWN IMPORTED)
    set_target_properties(OpenSSL::SSL
                          PROPERTIES IMPORTED_LOCATION "${OPENSSL_LIBRARIES}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${OPENSSL_INCLUDE_DIR}")

    add_library(OpenSSL::Crypto UNKNOWN IMPORTED)
    set_target_properties(OpenSSL::Crypto
                          PROPERTIES IMPORTED_LOCATION "${OPENSSL_LIBRARIES}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${OPENSSL_INCLUDE_DIR}")
  endif()

  include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# GLOG

macro(build_glog)
  message(STATUS "Building glog from source")
  set(GLOG_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/glog_ep-prefix/src/glog_ep")
  set(GLOG_INCLUDE_DIR "${GLOG_BUILD_DIR}/include")
  set(
    GLOG_STATIC_LIB
    "${GLOG_BUILD_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}glog${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(GLOG_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
  set(GLOG_CMAKE_C_FLAGS "${EP_C_FLAGS} -fPIC")
  if(Threads::Threads)
    set(GLOG_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -pthread")
    set(GLOG_CMAKE_C_FLAGS "${EP_C_FLAGS} -fPIC -pthread")
  endif()

  if(APPLE)
    # If we don't set this flag, the binary built with 10.13 cannot be used in 10.12.
    set(GLOG_CMAKE_CXX_FLAGS "${GLOG_CMAKE_CXX_FLAGS} -mmacosx-version-min=10.9")
  endif()

  set(GLOG_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      "-DCMAKE_INSTALL_PREFIX=${GLOG_BUILD_DIR}"
      -DBUILD_SHARED_LIBS=OFF
      -DBUILD_TESTING=OFF
      -DWITH_GFLAGS=OFF
      -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${GLOG_CMAKE_CXX_FLAGS}
      -DCMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${GLOG_CMAKE_C_FLAGS}
      -DCMAKE_CXX_FLAGS=${GLOG_CMAKE_CXX_FLAGS})
  externalproject_add(glog_ep
                      URL ${GLOG_SOURCE_URL}
                      BUILD_IN_SOURCE 1
                      BUILD_BYPRODUCTS "${GLOG_STATIC_LIB}"
                      CMAKE_ARGS ${GLOG_CMAKE_ARGS} ${EP_LOG_OPTIONS})

  add_dependencies(toolchain glog_ep)
  file(MAKE_DIRECTORY "${GLOG_INCLUDE_DIR}")

  add_library(GLOG::glog STATIC IMPORTED)
  set_target_properties(GLOG::glog
                        PROPERTIES IMPORTED_LOCATION "${GLOG_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GLOG_INCLUDE_DIR}")
  add_dependencies(GLOG::glog glog_ep)
endmacro()

if(ARROW_USE_GLOG)
  resolve_dependency(GLOG)
  # TODO: Don't use global includes but rather target_include_directories
  get_target_property(GLOG_INCLUDE_DIR GLOG::glog INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${GLOG_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# gflags

if(ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS OR ARROW_USE_GLOG OR ARROW_WITH_GRPC)
  set(ARROW_NEED_GFLAGS 1)
else()
  set(ARROW_NEED_GFLAGS 0)
endif()

macro(build_gflags)
  message(STATUS "Building gflags from source")

  set(GFLAGS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gflags_ep-prefix/src/gflags_ep")
  set(GFLAGS_INCLUDE_DIR "${GFLAGS_PREFIX}/include")
  if(MSVC)
    set(GFLAGS_STATIC_LIB "${GFLAGS_PREFIX}/lib/gflags_static.lib")
  else()
    set(GFLAGS_STATIC_LIB "${GFLAGS_PREFIX}/lib/libgflags.a")
  endif()
  set(GFLAGS_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      "-DCMAKE_INSTALL_PREFIX=${GFLAGS_PREFIX}"
      -DBUILD_SHARED_LIBS=OFF
      -DBUILD_STATIC_LIBS=ON
      -DBUILD_PACKAGING=OFF
      -DBUILD_TESTING=OFF
      -DBUILD_CONFIG_TESTS=OFF
      -DINSTALL_HEADERS=ON)

  file(MAKE_DIRECTORY "${GFLAGS_INCLUDE_DIR}")
  externalproject_add(gflags_ep
                      URL ${GFLAGS_SOURCE_URL} ${EP_LOG_OPTIONS}
                      BUILD_IN_SOURCE 1
                      BUILD_BYPRODUCTS "${GFLAGS_STATIC_LIB}"
                      CMAKE_ARGS ${GFLAGS_CMAKE_ARGS})

  add_dependencies(toolchain gflags_ep)

  add_thirdparty_lib(gflags STATIC_LIB ${GFLAGS_STATIC_LIB})
  set(GFLAGS_LIBRARY gflags_static)
  set_target_properties(${GFLAGS_LIBRARY}
                        PROPERTIES INTERFACE_COMPILE_DEFINITIONS "GFLAGS_IS_A_DLL=0"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GFLAGS_INCLUDE_DIR}")
  if(MSVC)
    set_target_properties(${GFLAGS_LIBRARY}
                          PROPERTIES INTERFACE_LINK_LIBRARIES "shlwapi.lib")
  endif()
  set(GFLAGS_LIBRARIES ${GFLAGS_LIBRARY})
endmacro()

if(ARROW_NEED_GFLAGS)
  if(gflags_SOURCE STREQUAL "AUTO")
    find_package(gflags QUIET)
    if(NOT gflags_FOUND)
      find_package(gflagsAlt)
    endif()
    if(NOT gflags_FOUND AND NOT gflagsAlt_FOUND)
      build_gflags()
    endif()
  elseif(gflags_SOURCE STREQUAL "BUNDLED")
    build_gflags()
  elseif(gflags_SOURCE STREQUAL "SYSTEM")
    # gflagsConfig.cmake is not installed on Ubuntu/Debian
    # TODO: Make a bug report upstream
    find_package(gflags)
    if(NOT gflags_FOUND)
      find_package(gflagsAlt REQUIRED)
    endif()
  endif()
  # TODO: Don't use global includes but rather target_include_directories
  include_directories(SYSTEM ${GFLAGS_INCLUDE_DIR})

  if(NOT TARGET ${GFLAGS_LIBRARIES})
    if(TARGET gflags-shared)
      set(GFLAGS_LIBRARIES gflags-shared)
    elseif(TARGET gflags_shared)
      set(GFLAGS_LIBRARIES gflags_shared)
    endif()
  endif()
endif()

# ----------------------------------------------------------------------
# Thrift

macro(build_thrift)
  message("Building Apache Thrift from source")
  set(THRIFT_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/thrift_ep/src/thrift_ep-install")
  set(THRIFT_INCLUDE_DIR "${THRIFT_PREFIX}/include")
  set(THRIFT_COMPILER "${THRIFT_PREFIX}/bin/thrift")
  set(THRIFT_CMAKE_ARGS
      ${EP_COMMON_CMAKE_ARGS}
      "-DCMAKE_INSTALL_PREFIX=${THRIFT_PREFIX}"
      "-DCMAKE_INSTALL_RPATH=${THRIFT_PREFIX}/lib"
      -DBUILD_SHARED_LIBS=OFF
      -DBUILD_TESTING=OFF
      -DBUILD_EXAMPLES=OFF
      -DBUILD_TUTORIALS=OFF
      -DWITH_QT4=OFF
      -DWITH_C_GLIB=OFF
      -DWITH_JAVA=OFF
      -DWITH_PYTHON=OFF
      -DWITH_HASKELL=OFF
      -DWITH_CPP=ON
      -DWITH_STATIC_LIB=ON
      -DWITH_LIBEVENT=OFF)

  # Thrift also uses boost. Forward important boost settings if there were ones passed.
  if(DEFINED BOOST_ROOT)
    set(THRIFT_CMAKE_ARGS ${THRIFT_CMAKE_ARGS} "-DBOOST_ROOT=${BOOST_ROOT}")
  endif()
  if(DEFINED Boost_NAMESPACE)
    set(THRIFT_CMAKE_ARGS ${THRIFT_CMAKE_ARGS} "-DBoost_NAMESPACE=${Boost_NAMESPACE}")
  endif()

  set(THRIFT_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}thrift")
  if(MSVC)
    if(ARROW_USE_STATIC_CRT)
      set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}mt")
      set(THRIFT_CMAKE_ARGS ${THRIFT_CMAKE_ARGS} "-DWITH_MT=ON")
    else()
      set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}md")
      set(THRIFT_CMAKE_ARGS ${THRIFT_CMAKE_ARGS} "-DWITH_MT=OFF")
    endif()
  endif()
  if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
    set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}d")
  endif()
  set(THRIFT_STATIC_LIB
      "${THRIFT_PREFIX}/lib/${THRIFT_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")

  if(ZLIB_SHARED_LIB)
    set(THRIFT_CMAKE_ARGS "-DZLIB_LIBRARY=${ZLIB_SHARED_LIB}" ${THRIFT_CMAKE_ARGS})
  else()
    set(THRIFT_CMAKE_ARGS "-DZLIB_LIBRARY=${ZLIB_STATIC_LIB}" ${THRIFT_CMAKE_ARGS})
  endif()
  set(THRIFT_DEPENDENCIES ${THRIFT_DEPENDENCIES} ${ZLIB_LIBRARY})

  if(MSVC)
    set(WINFLEXBISON_VERSION 2.4.9)
    set(WINFLEXBISON_PREFIX
        "${CMAKE_CURRENT_BINARY_DIR}/winflexbison_ep/src/winflexbison_ep-install")
    externalproject_add(
      winflexbison_ep
      URL
        https://github.com/lexxmark/winflexbison/releases/download/v.${WINFLEXBISON_VERSION}/win_flex_bison-${WINFLEXBISON_VERSION}.zip
      URL_HASH MD5=a2e979ea9928fbf8567e995e9c0df765
      SOURCE_DIR ${WINFLEXBISON_PREFIX}
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ""
      INSTALL_COMMAND "" ${EP_LOG_OPTIONS})
    set(THRIFT_DEPENDENCIES ${THRIFT_DEPENDENCIES} winflexbison_ep)

    set(THRIFT_CMAKE_ARGS
        "-DFLEX_EXECUTABLE=${WINFLEXBISON_PREFIX}/win_flex.exe"
        "-DBISON_EXECUTABLE=${WINFLEXBISON_PREFIX}/win_bison.exe"
        "-DZLIB_INCLUDE_DIR=${ZLIB_INCLUDE_DIR}"
        "-DWITH_SHARED_LIB=OFF"
        "-DWITH_PLUGIN=OFF"
        ${THRIFT_CMAKE_ARGS})
  elseif(APPLE)
    # Some other process always resets BISON_EXECUTABLE to the system default,
    # thus we use our own variable here.
    if(NOT DEFINED THRIFT_BISON_EXECUTABLE)
      find_package(BISON 2.5.1)

      # In the case where we cannot find a system-wide installation, look for
      # homebrew and ask for its bison installation.
      if(NOT BISON_FOUND)
        find_program(BREW_BIN brew)
        if(BREW_BIN)
          execute_process(COMMAND ${BREW_BIN} --prefix bison
                          OUTPUT_VARIABLE BISON_PREFIX
                          OUTPUT_STRIP_TRAILING_WHITESPACE)
          set(BISON_EXECUTABLE "${BISON_PREFIX}/bin/bison")
          find_package(BISON 2.5.1)
          set(THRIFT_BISON_EXECUTABLE "${BISON_EXECUTABLE}")
        endif()
      else()
        set(THRIFT_BISON_EXECUTABLE "${BISON_EXECUTABLE}")
      endif()
    endif()
    set(THRIFT_CMAKE_ARGS "-DBISON_EXECUTABLE=${THRIFT_BISON_EXECUTABLE}"
                          ${THRIFT_CMAKE_ARGS})
  endif()

  externalproject_add(thrift_ep
                      URL ${THRIFT_SOURCE_URL}
                      BUILD_BYPRODUCTS "${THRIFT_STATIC_LIB}" "${THRIFT_COMPILER}"
                      CMAKE_ARGS ${THRIFT_CMAKE_ARGS}
                      DEPENDS ${THRIFT_DEPENDENCIES} ${EP_LOG_OPTIONS})

  add_library(Thrift::thrift STATIC IMPORTED)
  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${THRIFT_INCLUDE_DIR}")
  set_target_properties(Thrift::thrift
                        PROPERTIES IMPORTED_LOCATION "${THRIFT_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${THRIFT_INCLUDE_DIR}")
  add_dependencies(toolchain thrift_ep)
  add_dependencies(Thrift::thrift thrift_ep)
endmacro()

if(ARROW_WITH_THRIFT)
  resolve_dependency(Thrift)
  # TODO: Don't use global includes but rather target_include_directories
  include_directories(SYSTEM ${THRIFT_INCLUDE_DIR})

  if(THRIFT_VERSION VERSION_LESS "0.11.0")
    add_definitions(-DPARQUET_THRIFT_USE_BOOST)
  endif()
endif()

# ----------------------------------------------------------------------
# Protocol Buffers (required for ORC and Flight and Gandiva libraries)

macro(build_protobuf)
  message("Building Protocol Buffers from source")
  set(PROTOBUF_PREFIX "${THIRDPARTY_DIR}/protobuf_ep-install")
  set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_PREFIX}/include")
  set(
    PROTOBUF_STATIC_LIB
    "${PROTOBUF_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    PROTOC_STATIC_LIB
    "${PROTOBUF_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protoc${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(PROTOBUF_COMPILER "${PROTOBUF_PREFIX}/bin/protoc")
  set(PROTOBUF_CONFIGURE_ARGS
      "AR=${CMAKE_AR}"
      "RANLIB=${CMAKE_RANLIB}"
      "CC=${CMAKE_C_COMPILER}"
      "CXX=${CMAKE_CXX_COMPILER}"
      "--disable-shared"
      "--prefix=${PROTOBUF_PREFIX}"
      "CFLAGS=${EP_C_FLAGS}"
      "CXXFLAGS=${EP_CXX_FLAGS}")

  externalproject_add(protobuf_ep
                      CONFIGURE_COMMAND "./configure" ${PROTOBUF_CONFIGURE_ARGS}
                      BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS}
                      BUILD_IN_SOURCE 1
                      URL ${PROTOBUF_SOURCE_URL}
                      BUILD_BYPRODUCTS "${PROTOBUF_STATIC_LIB}" "${PROTOBUF_COMPILER}"
                                       ${EP_LOG_OPTIONS})

  file(MAKE_DIRECTORY "${PROTOBUF_INCLUDE_DIR}")

  add_library(protobuf::libprotobuf STATIC IMPORTED)
  set_target_properties(
    protobuf::libprotobuf
    PROPERTIES IMPORTED_LOCATION "${PROTOBUF_STATIC_LIB}" INTERFACE_INCLUDE_DIRECTORIES
               "${PROTOBUF_INCLUDE_DIR}")
  add_library(protobuf::libprotoc STATIC IMPORTED)
  set_target_properties(
    protobuf::libprotoc
    PROPERTIES IMPORTED_LOCATION "${PROTOC_STATIC_LIB}" INTERFACE_INCLUDE_DIRECTORIES
               "${PROTOBUF_INCLUDE_DIR}")
  add_executable(protobuf::protoc IMPORTED)
  set_target_properties(protobuf::protoc
                        PROPERTIES IMPORTED_LOCATION "${PROTOBUF_COMPILER}")

  add_dependencies(toolchain protobuf_ep)
  add_dependencies(protobuf::libprotobuf protobuf_ep)
endmacro()

if(ARROW_WITH_PROTOBUF)
  resolve_dependency(Protobuf)

  # TODO: Don't use global includes but rather target_include_directories
  include_directories(SYSTEM ${PROTOBUF_INCLUDE_DIR})

  # Old CMake versions don't define the targets
  if(NOT TARGET protobuf::libprotobuf)
    add_library(protobuf::libprotobuf UNKNOWN IMPORTED)
    set_target_properties(protobuf::libprotobuf
                          PROPERTIES IMPORTED_LOCATION "${PROTOBUF_LIBRARY}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${PROTOBUF_INCLUDE_DIR}")
  endif()
  if(NOT TARGET protobuf::libprotoc)
    if(PROTOBUF_PROTOC_LIBRARY AND NOT Protobuf_PROTOC_LIBRARY)
      # Old CMake versions have a different casing.
      set(Protobuf_PROTOC_LIBRARY ${PROTOBUF_PROTOC_LIBRARY})
    endif()
    if(NOT Protobuf_PROTOC_LIBRARY)
      message(FATAL_ERROR "libprotoc was set to ${Protobuf_PROTOC_LIBRARY}")
    endif()
    add_library(protobuf::libprotoc UNKNOWN IMPORTED)
    set_target_properties(protobuf::libprotoc
                          PROPERTIES IMPORTED_LOCATION "${Protobuf_PROTOC_LIBRARY}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${PROTOBUF_INCLUDE_DIR}")
  endif()
  if(NOT TARGET protobuf::protoc)
    add_executable(protobuf::protoc IMPORTED)
    set_target_properties(protobuf::protoc
                          PROPERTIES IMPORTED_LOCATION "${PROTOBUF_PROTOC_EXECUTABLE}")
  endif()

  # Log protobuf paths as we often see issues with mixed sources for
  # the libraries and protoc.
  get_target_property(PROTOBUF_PROTOC_EXECUTABLE protobuf::protoc IMPORTED_LOCATION)
  message(STATUS "Found protoc: ${PROTOBUF_PROTOC_EXECUTABLE}")
  # Protobuf_PROTOC_LIBRARY is set by all versions of FindProtobuf.cmake
  message(STATUS "Found libprotoc: ${Protobuf_PROTOC_LIBRARY}")
  get_target_property(PROTOBUF_LIBRARY protobuf::libprotobuf IMPORTED_LOCATION)
  message(STATUS "Found libprotobuf: ${PROTOBUF_LIBRARY}")
  message(STATUS "Found protobuf headers: ${PROTOBUF_INCLUDE_DIR}")
endif()

if(WIN32)
  # jemalloc is not supported on Windows
  set(ARROW_JEMALLOC off)
endif()

if(ARROW_JEMALLOC)
  message(STATUS "Building (vendored) jemalloc from source")
  # We only use a vendored jemalloc as we want to control its version.
  # Also our build of jemalloc is specially prefixed so that it will not
  # conflict with the default allocator as well as other jemalloc
  # installations.
  # find_package(jemalloc)

  set(ARROW_JEMALLOC_USE_SHARED OFF)
  set(JEMALLOC_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/jemalloc_ep-prefix/src/jemalloc_ep/dist/")
  set(JEMALLOC_STATIC_LIB
      "${JEMALLOC_PREFIX}/lib/libjemalloc_pic${CMAKE_STATIC_LIBRARY_SUFFIX}")
  externalproject_add(
    jemalloc_ep
    URL ${JEMALLOC_SOURCE_URL}
    PATCH_COMMAND touch doc/jemalloc.3 doc/jemalloc.html
    CONFIGURE_COMMAND ./autogen.sh
                      "AR=${CMAKE_AR}"
                      "CC=${CMAKE_C_COMPILER}"
                      "--prefix=${JEMALLOC_PREFIX}"
                      "--with-jemalloc-prefix=je_arrow_"
                      "--with-private-namespace=je_arrow_private_"
                      "--without-export"
                      # Don't override operator new()
                      "--disable-cxx" "--disable-libdl"
                      # See https://github.com/jemalloc/jemalloc/issues/1237
                      "--disable-initial-exec-tls" ${EP_LOG_OPTIONS}
    BUILD_IN_SOURCE 1
    BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS}
    BUILD_BYPRODUCTS "${JEMALLOC_STATIC_LIB}"
    INSTALL_COMMAND ${MAKE} install)

  # Don't use the include directory directly so that we can point to a path
  # that is unique to our codebase.
  include_directories(SYSTEM "${CMAKE_CURRENT_BINARY_DIR}/jemalloc_ep-prefix/src/")
  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/jemalloc_ep-prefix/src/")
  add_library(jemalloc::jemalloc STATIC IMPORTED)
  set_target_properties(jemalloc::jemalloc
                        PROPERTIES INTERFACE_LINK_LIBRARIES
                                   Threads::Threads
                                   IMPORTED_LOCATION
                                   "${JEMALLOC_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${CMAKE_CURRENT_BINARY_DIR}/jemalloc_ep-prefix/src")
  add_dependencies(jemalloc::jemalloc jemalloc_ep)
  add_dependencies(toolchain jemalloc_ep)
endif()

# ----------------------------------------------------------------------
# Google gtest

macro(build_gtest)
  message(STATUS "Building gtest from source")
  set(GTEST_VENDORED TRUE)
  set(GTEST_CMAKE_CXX_FLAGS ${EP_CXX_FLAGS})

  if(CMAKE_BUILD_TYPE MATCHES DEBUG)
    set(CMAKE_GTEST_DEBUG_EXTENSION "d")
  else()
    set(CMAKE_GTEST_DEBUG_EXTENSION "")
  endif()

  if(APPLE)
    set(GTEST_CMAKE_CXX_FLAGS ${GTEST_CMAKE_CXX_FLAGS} -DGTEST_USE_OWN_TR1_TUPLE=1
                              -Wno-unused-value -Wno-ignored-attributes)
  endif()

  if(MSVC)
    set(GTEST_CMAKE_CXX_FLAGS "${GTEST_CMAKE_CXX_FLAGS} -DGTEST_CREATE_SHARED_LIBRARY=1")
  endif()

  set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
  set(GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")

  if(MSVC)
    set(_GTEST_IMPORTED_TYPE IMPORTED_IMPLIB)
    set(_GTEST_LIBRARY_SUFFIX
        "${CMAKE_GTEST_DEBUG_EXTENSION}${CMAKE_IMPORT_LIBRARY_SUFFIX}")
  else()
    set(_GTEST_IMPORTED_TYPE IMPORTED_LOCATION)
    set(_GTEST_LIBRARY_SUFFIX
        "${CMAKE_GTEST_DEBUG_EXTENSION}${CMAKE_SHARED_LIBRARY_SUFFIX}")
  endif()

  set(GTEST_SHARED_LIB
      "${GTEST_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}gtest${_GTEST_LIBRARY_SUFFIX}")
  set(GMOCK_SHARED_LIB
      "${GTEST_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}gmock${_GTEST_LIBRARY_SUFFIX}")
  set(
    GTEST_MAIN_SHARED_LIB

    "${GTEST_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}gtest_main${_GTEST_LIBRARY_SUFFIX}"
    )
  set(GTEST_CMAKE_ARGS
      ${EP_COMMON_TOOLCHAIN}
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
      "-DCMAKE_INSTALL_PREFIX=${GTEST_PREFIX}"
      "-DCMAKE_INSTALL_LIBDIR=lib"
      -DBUILD_SHARED_LIBS=ON
      -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS}
      -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${GTEST_CMAKE_CXX_FLAGS})
  set(GMOCK_INCLUDE_DIR "${GTEST_PREFIX}/include")

  if(MSVC)
    if("${CMAKE_GENERATOR}" STREQUAL "Ninja")
      set(_GTEST_LIBRARY_DIR ${BUILD_OUTPUT_ROOT_DIRECTORY})
    else()
      set(_GTEST_LIBRARY_DIR ${BUILD_OUTPUT_ROOT_DIRECTORY}/${CMAKE_BUILD_TYPE})
    endif()

    set(GTEST_CMAKE_ARGS
        ${GTEST_CMAKE_ARGS} "-DCMAKE_RUNTIME_OUTPUT_DIRECTORY=${_GTEST_LIBRARY_DIR}"
        "-DCMAKE_RUNTIME_OUTPUT_DIRECTORY_${CMAKE_BUILD_TYPE}=${_GTEST_LIBRARY_DIR}")
  endif()

  add_definitions(-DGTEST_LINKED_AS_SHARED_LIBRARY=1)

  if(MSVC AND NOT ARROW_USE_STATIC_CRT)
    set(GTEST_CMAKE_ARGS ${GTEST_CMAKE_ARGS} -Dgtest_force_shared_crt=ON)
  endif()

  externalproject_add(googletest_ep
                      URL ${GTEST_SOURCE_URL}
                      BUILD_BYPRODUCTS ${GTEST_SHARED_LIB} ${GTEST_MAIN_SHARED_LIB}
                                       ${GMOCK_SHARED_LIB}
                      CMAKE_ARGS ${GTEST_CMAKE_ARGS} ${EP_LOG_OPTIONS})

  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${GTEST_INCLUDE_DIR}")

  add_library(GTest::GTest SHARED IMPORTED)
  set_target_properties(GTest::GTest
                        PROPERTIES ${_GTEST_IMPORTED_TYPE} "${GTEST_SHARED_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIR}")

  add_library(GTest::Main SHARED IMPORTED)
  set_target_properties(GTest::Main
                        PROPERTIES ${_GTEST_IMPORTED_TYPE} "${GTEST_MAIN_SHARED_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIR}")

  add_library(GTest::GMock SHARED IMPORTED)
  set_target_properties(GTest::GMock
                        PROPERTIES ${_GTEST_IMPORTED_TYPE} "${GMOCK_SHARED_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIR}")
  add_dependencies(toolchain-tests googletest_ep)
  add_dependencies(GTest::GTest googletest_ep)
  add_dependencies(GTest::Main googletest_ep)
  add_dependencies(GTest::GMock googletest_ep)
endmacro()

if(ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS)
  resolve_dependency(GTest)

  if(NOT GTEST_VENDORED)
    # TODO(wesm): This logic does not work correctly with the MSVC static libraries
    # built for the shared crt

    #     set(CMAKE_REQUIRED_LIBRARIES GTest::GTest GTest::Main GTest::GMock)
    #     CHECK_CXX_SOURCE_COMPILES("
    # #include <gmock/gmock.h>
    # #include <gtest/gtest.h>

    # class A {
    #   public:
    #     int run() const { return 1; }
    # };

    # class B : public A {
    #   public:
    #     MOCK_CONST_METHOD0(run, int());
    # };

    # TEST(Base, Test) {
    #   B b;
    # }" GTEST_COMPILES_WITHOUT_MACRO)
    #     if (NOT GTEST_COMPILES_WITHOUT_MACRO)
    #       message(STATUS "Setting GTEST_LINKED_AS_SHARED_LIBRARY=1 on GTest::GTest")
    #       add_compile_definitions("GTEST_LINKED_AS_SHARED_LIBRARY=1")
    #     endif()
    #     set(CMAKE_REQUIRED_LIBRARIES)
  endif()

  # TODO: Don't use global includes but rather target_include_directories
  get_target_property(GTEST_INCLUDE_DIR GTest::GTest INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
endif()

macro(build_benchmark)
  message(STATUS "Building benchmark from source")
  if(CMAKE_VERSION VERSION_LESS 3.6)
    message(FATAL_ERROR "Building gbenchmark from source requires at least CMake 3.6")
  endif()

  if(NOT MSVC)
    set(GBENCHMARK_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -std=c++11")
  endif()

  if(APPLE)
    set(GBENCHMARK_CMAKE_CXX_FLAGS "${GBENCHMARK_CMAKE_CXX_FLAGS} -stdlib=libc++")
  endif()

  set(GBENCHMARK_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/gbenchmark_ep/src/gbenchmark_ep-install")
  set(GBENCHMARK_INCLUDE_DIR "${GBENCHMARK_PREFIX}/include")
  set(
    GBENCHMARK_STATIC_LIB
    "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    GBENCHMARK_MAIN_STATIC_LIB
    "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark_main${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(GBENCHMARK_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS}
                            "-DCMAKE_INSTALL_PREFIX=${GBENCHMARK_PREFIX}"
                            -DBENCHMARK_ENABLE_TESTING=OFF
                            -DCMAKE_CXX_FLAGS=${GBENCHMARK_CMAKE_CXX_FLAGS})
  if(APPLE)
    set(GBENCHMARK_CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS} "-DBENCHMARK_USE_LIBCXX=ON")
  endif()

  externalproject_add(gbenchmark_ep
                      URL ${GBENCHMARK_SOURCE_URL}
                      BUILD_BYPRODUCTS "${GBENCHMARK_STATIC_LIB}"
                                       "${GBENCHMARK_MAIN_STATIC_LIB}"
                      CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS} ${EP_LOG_OPTIONS})

  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${GBENCHMARK_INCLUDE_DIR}")

  add_library(benchmark::benchmark STATIC IMPORTED)
  set_target_properties(benchmark::benchmark
                        PROPERTIES IMPORTED_LOCATION "${GBENCHMARK_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${GBENCHMARK_INCLUDE_DIR}")

  add_library(benchmark::benchmark_main STATIC IMPORTED)
  set_target_properties(benchmark::benchmark_main
                        PROPERTIES IMPORTED_LOCATION "${GBENCHMARK_MAIN_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${GBENCHMARK_INCLUDE_DIR}")

  add_dependencies(toolchain-benchmarks gbenchmark_ep)
  add_dependencies(benchmark::benchmark gbenchmark_ep)
  add_dependencies(benchmark::benchmark_main gbenchmark_ep)
endmacro()

if(ARROW_BUILD_BENCHMARKS)
  resolve_dependency(benchmark)
  # TODO: Don't use global includes but rather target_include_directories
  get_target_property(BENCHMARK_INCLUDE_DIR benchmark::benchmark
                      INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${BENCHMARK_INCLUDE_DIR})
endif()

macro(build_rapidjson)
  message(STATUS "Building rapidjson from source")
  set(RAPIDJSON_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/rapidjson_ep/src/rapidjson_ep-install")
  set(RAPIDJSON_CMAKE_ARGS -DRAPIDJSON_BUILD_DOC=OFF -DRAPIDJSON_BUILD_EXAMPLES=OFF
                           -DRAPIDJSON_BUILD_TESTS=OFF
                           "-DCMAKE_INSTALL_PREFIX=${RAPIDJSON_PREFIX}")

  externalproject_add(rapidjson_ep
                      ${EP_LOG_OPTIONS}
                      PREFIX "${CMAKE_BINARY_DIR}"
                      URL ${RAPIDJSON_SOURCE_URL}
                      CMAKE_ARGS ${RAPIDJSON_CMAKE_ARGS})

  set(RAPIDJSON_INCLUDE_DIR "${RAPIDJSON_PREFIX}/include")

  add_dependencies(toolchain rapidjson_ep)
  add_dependencies(rapidjson rapidjson_ep)
endmacro()

# TODO: Check for 1.1.0+
if(ARROW_WITH_RAPIDJSON)
  if(RapidJSON_SOURCE STREQUAL "AUTO")
    # Fedora packages place the package information at the wrong location.
    # https://bugzilla.redhat.com/show_bug.cgi?id=1680400
    find_package(RapidJSON QUIET HINTS "${CMAKE_ROOT}")
    if(NOT RapidJSON_FOUND)
      # Ubuntu / Debian don't package the CMake config
      find_package(RapidJSONAlt)
    endif()
    if(NOT RapidJSON_FOUND AND NOT RapidJSONAlt_FOUND)
      build_rapidjson()
    endif()
  elseif(RapidJSON_SOURCE STREQUAL "BUNDLED")
    build_rapidjson()
  elseif(RapidJSON_SOURCE STREQUAL "SYSTEM")
    # Fedora packages place the package information at the wrong location.
    # https://bugzilla.redhat.com/show_bug.cgi?id=1680400
    find_package(RapidJSON HINTS "${CMAKE_ROOT}")
    if(NOT RapidJSON_FOUND)
      # Ubuntu / Debian don't package the CMake config
      find_package(RapidJSONAlt REQUIRED)
    endif()
  endif()

  if(RapidJSON_INCLUDE_DIR)
    set(RAPIDJSON_INCLUDE_DIR "${RapidJSON_INCLUDE_DIR}")
  endif()

  # TODO: Don't use global includes but rather target_include_directories
  include_directories(SYSTEM ${RAPIDJSON_INCLUDE_DIR})
endif()

macro(build_flatbuffers)
  message(STATUS "Building flatbuffers from source")
  set(FLATBUFFERS_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/flatbuffers_ep-prefix/src/flatbuffers_ep-install")
  if(MSVC)
    set(FLATBUFFERS_CMAKE_CXX_FLAGS /EHsc)
  else()
    set(FLATBUFFERS_CMAKE_CXX_FLAGS -fPIC)
  endif()
  set(FLATBUFFERS_COMPILER "${FLATBUFFERS_PREFIX}/bin/flatc")
  set(
    FLATBUFFERS_STATIC_LIB
    "${FLATBUFFERS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}flatbuffers${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  # We always need to do release builds, otherwise flatc will not be installed.
  externalproject_add(flatbuffers_ep
                      URL ${FLATBUFFERS_SOURCE_URL}
                      BUILD_BYPRODUCTS ${FLATBUFFERS_COMPILER} ${FLATBUFFERS_STATIC_LIB}
                      CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS}
                                 "-DCMAKE_BUILD_TYPE=RELEASE"
                                 "-DCMAKE_CXX_FLAGS=${FLATBUFFERS_CMAKE_CXX_FLAGS}"
                                 "-DCMAKE_INSTALL_PREFIX:PATH=${FLATBUFFERS_PREFIX}"
                                 "-DFLATBUFFERS_BUILD_TESTS=OFF"
                                 ${EP_LOG_OPTIONS})

  file(MAKE_DIRECTORY "${FLATBUFFERS_PREFIX}/include")

  add_library(flatbuffers::flatbuffers STATIC IMPORTED)
  set_target_properties(flatbuffers::flatbuffers
                        PROPERTIES IMPORTED_LOCATION "${FLATBUFFERS_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${FLATBUFFERS_PREFIX}/include")
  add_executable(flatbuffers::flatc IMPORTED)
  set_target_properties(flatbuffers::flatc
                        PROPERTIES IMPORTED_LOCATION "${FLATBUFFERS_COMPILER}")

  add_dependencies(toolchain flatbuffers_ep)
  add_dependencies(flatbuffers::flatbuffers flatbuffers_ep)
  add_dependencies(flatbuffers::flatc flatbuffers_ep)
endmacro()

if(ARROW_WITH_FLATBUFFERS)
  if(Flatbuffers_SOURCE STREQUAL "AUTO")
    find_package(Flatbuffers QUIET)
    # Older versions of Flatbuffers (that are not built using CMake)
    # don't install a FlatbuffersConfig.cmake
    # This is only supported from 1.10+ on, we support at least 1.7
    if(NOT Flatbuffers_FOUND)
      find_package(FlatbuffersAlt)
    endif()
    if(NOT Flatbuffers_FOUND AND NOT FlatbuffersAlt_FOUND)
      build_flatbuffers()
    endif()
  elseif(Flatbuffers_SOURCE STREQUAL "BUNDLED")
    build_flatbuffers()
  elseif(Flatbuffers_SOURCE STREQUAL "SYSTEM")
    find_package(Flatbuffers QUIET)
    if(NOT Flatbuffers_FOUND)
      find_package(FlatbuffersAlt REQUIRED)
    endif()
  endif()

  if(TARGET flatbuffers::flatbuffers_shared AND NOT TARGET flatbuffers::flatbuffers)
    get_target_property(FLATBUFFERS_INCLUDE_DIR flatbuffers::flatbuffers_shared
                        INTERFACE_INCLUDE_DIRECTORIES)
    get_target_property(FLATBUFFERS_SHARED_LIB flatbuffers::flatbuffers_shared
                        IMPORTED_LOCATION)
    add_library(flatbuffers::flatbuffers SHARED IMPORTED)
    set_target_properties(flatbuffers::flatbuffers
                          PROPERTIES IMPORTED_LOCATION "${FLATBUFFERS_SHARED_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${FLATBUFFERS_INCLUDE_DIR}")
  endif()

  # mingw-w64-flatbuffers doesn't set the flatc target
  if(NOT TARGET flatbuffers::flatc)
    get_target_property(FLATBUFFERS_INCLUDE_DIR flatbuffers::flatbuffers
                        INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(FB_ROOT "${FLATBUFFERS_INCLUDE_DIR}" DIRECTORY)
    find_program(FLATBUFFERS_COMPILER
                 NAMES flatc flatc.exe PATH ${FB_ROOT}
                 PATH_SUFFIXES "bin")
    add_executable(flatbuffers::flatc IMPORTED)
    set_target_properties(flatbuffers::flatc
                          PROPERTIES IMPORTED_LOCATION "${FLATBUFFERS_COMPILER}")
  endif()

  # TODO: Don't use global includes but rather target_include_directories
  get_target_property(FLATBUFFERS_INCLUDE_DIR flatbuffers::flatbuffers
                      INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${FLATBUFFERS_INCLUDE_DIR})
endif()

macro(build_zlib)
  message(STATUS "Building ZLIB from source")
  set(ZLIB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zlib_ep/src/zlib_ep-install")
  if(MSVC)
    if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
      set(ZLIB_STATIC_LIB_NAME zlibstaticd.lib)
    else()
      set(ZLIB_STATIC_LIB_NAME zlibstatic.lib)
    endif()
  else()
    set(ZLIB_STATIC_LIB_NAME libz.a)
  endif()
  set(ZLIB_STATIC_LIB "${ZLIB_PREFIX}/lib/${ZLIB_STATIC_LIB_NAME}")
  set(ZLIB_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}"
                      -DBUILD_SHARED_LIBS=OFF)

  externalproject_add(zlib_ep
                      URL ${ZLIB_SOURCE_URL} ${EP_LOG_OPTIONS}
                      BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}"
                      CMAKE_ARGS ${ZLIB_CMAKE_ARGS})

  file(MAKE_DIRECTORY "${ZLIB_PREFIX}/include")

  add_library(ZLIB::ZLIB STATIC IMPORTED)
  set_target_properties(ZLIB::ZLIB
                        PROPERTIES IMPORTED_LOCATION "${ZLIB_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${ZLIB_PREFIX}/include")

  add_dependencies(toolchain zlib_ep)
  add_dependencies(ZLIB::ZLIB zlib_ep)
endmacro()

if(ARROW_WITH_ZLIB)
  resolve_dependency(ZLIB)

  # TODO: Don't use global includes but rather target_include_directories
  get_target_property(ZLIB_INCLUDE_DIR ZLIB::ZLIB INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${ZLIB_INCLUDE_DIR})
endif()

macro(build_lz4)
  message(STATUS "Building lz4 from source")
  set(LZ4_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-prefix/src/lz4_ep")
  set(LZ4_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-prefix")

  if(MSVC)
    if(ARROW_USE_STATIC_CRT)
      if(${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
        set(LZ4_RUNTIME_LIBRARY_LINKAGE "/p:RuntimeLibrary=MultiThreadedDebug")
      else()
        set(LZ4_RUNTIME_LIBRARY_LINKAGE "/p:RuntimeLibrary=MultiThreaded")
      endif()
    endif()
    set(LZ4_STATIC_LIB
        "${LZ4_BUILD_DIR}/visual/VS2010/bin/x64_${CMAKE_BUILD_TYPE}/liblz4_static.lib")
    set(LZ4_BUILD_COMMAND
        BUILD_COMMAND
        msbuild.exe
        /m
        /p:Configuration=${CMAKE_BUILD_TYPE}
        /p:Platform=x64
        /p:PlatformToolset=v140
        ${LZ4_RUNTIME_LIBRARY_LINKAGE}
        /t:Build
        ${LZ4_BUILD_DIR}/visual/VS2010/lz4.sln)
  else()
    set(LZ4_STATIC_LIB "${LZ4_BUILD_DIR}/lib/liblz4.a")
    set(LZ4_BUILD_COMMAND BUILD_COMMAND ${CMAKE_SOURCE_DIR}/build-support/build-lz4-lib.sh
                          "AR=${CMAKE_AR}")
  endif()

  # We need to copy the header in lib to directory outside of the build
  externalproject_add(lz4_ep
                      URL ${LZ4_SOURCE_URL} ${EP_LOG_OPTIONS}
                      UPDATE_COMMAND ${CMAKE_COMMAND} -E copy_directory
                                                         "${LZ4_BUILD_DIR}/lib"
                                                         "${LZ4_PREFIX}/include"
                                                         ${LZ4_PATCH_COMMAND}
                      CONFIGURE_COMMAND ""
                      INSTALL_COMMAND ""
                      BINARY_DIR ${LZ4_BUILD_DIR}
                      BUILD_BYPRODUCTS ${LZ4_STATIC_LIB} ${LZ4_BUILD_COMMAND})

  file(MAKE_DIRECTORY "${LZ4_PREFIX}/include")
  add_library(LZ4::lz4 STATIC IMPORTED)
  set_target_properties(LZ4::lz4
                        PROPERTIES IMPORTED_LOCATION "${LZ4_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${LZ4_PREFIX}/include")
  add_dependencies(toolchain lz4_ep)
  add_dependencies(LZ4::lz4 lz4_ep)
endmacro()

if(ARROW_WITH_LZ4)
  resolve_dependency(Lz4)

  # TODO: Don't use global includes but rather target_include_directories
  get_target_property(LZ4_INCLUDE_DIR LZ4::lz4 INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${LZ4_INCLUDE_DIR})
endif()

macro(build_zstd)
  message(STATUS "Building zstd from source")
  set(ZSTD_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zstd_ep-install")

  set(ZSTD_CMAKE_ARGS
      ${EP_COMMON_TOOLCHAIN}
      "-DCMAKE_INSTALL_PREFIX=${ZSTD_PREFIX}"
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
      -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
      -DZSTD_BUILD_PROGRAMS=off
      -DZSTD_BUILD_SHARED=off
      -DZSTD_BUILD_STATIC=on
      -DZSTD_MULTITHREAD_SUPPORT=off)

  if(MSVC)
    set(ZSTD_STATIC_LIB "${ZSTD_PREFIX}/${CMAKE_INSTALL_LIBDIR}/zstd_static.lib")
    if(ARROW_USE_STATIC_CRT)
      set(ZSTD_CMAKE_ARGS ${ZSTD_CMAKE_ARGS} "-DZSTD_USE_STATIC_RUNTIME=on")
    endif()
  else()
    set(ZSTD_STATIC_LIB "${ZSTD_PREFIX}/${CMAKE_INSTALL_LIBDIR}/libzstd.a")
    # Only pass our C flags on Unix as on MSVC it leads to a
    # "incompatible command-line options" error
    set(ZSTD_CMAKE_ARGS
        ${ZSTD_CMAKE_ARGS}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_FLAGS=${EP_C_FLAGS}
        -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS})
  endif()

  if(CMAKE_VERSION VERSION_LESS 3.7)
    message(FATAL_ERROR "Building zstd using ExternalProject requires at least CMake 3.7")
  endif()

  externalproject_add(zstd_ep
                      ${EP_LOG_OPTIONS}
                      CMAKE_ARGS ${ZSTD_CMAKE_ARGS}
                      SOURCE_SUBDIR "build/cmake"
                      INSTALL_DIR ${ZSTD_PREFIX}
                      URL ${ZSTD_SOURCE_URL}
                      BUILD_BYPRODUCTS "${ZSTD_STATIC_LIB}")

  file(MAKE_DIRECTORY "${ZSTD_PREFIX}/include")

  add_library(ZSTD::zstd STATIC IMPORTED)
  set_target_properties(ZSTD::zstd
                        PROPERTIES IMPORTED_LOCATION "${ZSTD_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${ZSTD_PREFIX}/include")

  add_dependencies(toolchain zstd_ep)
  add_dependencies(ZSTD::zstd zstd_ep)
endmacro()

if(ARROW_WITH_ZSTD)
  resolve_dependency(ZSTD)

  # TODO: Don't use global includes but rather target_include_directories
  get_target_property(ZSTD_INCLUDE_DIR ZSTD::zstd INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${ZSTD_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# RE2 (required for Gandiva)

macro(build_re2)
  message(STATUS "Building re2 from source")
  set(RE2_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/re2_ep-install")
  set(RE2_STATIC_LIB
      "${RE2_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}re2${CMAKE_STATIC_LIBRARY_SUFFIX}")

  set(RE2_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=${RE2_PREFIX}")

  externalproject_add(re2_ep
                      ${EP_LOG_OPTIONS}
                      INSTALL_DIR ${RE2_PREFIX}
                      URL ${RE2_SOURCE_URL}
                      CMAKE_ARGS ${RE2_CMAKE_ARGS}
                      BUILD_BYPRODUCTS "${RE2_STATIC_LIB}")

  file(MAKE_DIRECTORY "${RE2_PREFIX}/include")
  add_library(RE2::re2 STATIC IMPORTED)
  set_target_properties(RE2::re2
                        PROPERTIES IMPORTED_LOCATION "${RE2_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${RE2_PREFIX}/include")

  add_dependencies(toolchain re2_ep)
  add_dependencies(RE2::re2 re2_ep)
endmacro()

if(ARROW_GANDIVA)
  resolve_dependency(RE2)

  # TODO: Don't use global includes but rather target_include_directories
  get_target_property(RE2_INCLUDE_DIR RE2::re2 INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${RE2_INCLUDE_DIR})
endif()

macro(build_bzip2)
  message(STATUS "Building BZip2 from source")
  set(BZIP2_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/bzip2_ep-install")
  set(
    BZIP2_STATIC_LIB
    "${BZIP2_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}bz2${CMAKE_STATIC_LIBRARY_SUFFIX}")

  externalproject_add(bzip2_ep
                      ${EP_LOG_OPTIONS}
                      CONFIGURE_COMMAND ""
                      BUILD_IN_SOURCE 1
                      BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS} CFLAGS=${EP_C_FLAGS}
                      INSTALL_COMMAND ${MAKE} install PREFIX=${BZIP2_PREFIX}
                                      CFLAGS=${EP_C_FLAGS}
                      INSTALL_DIR ${BZIP2_PREFIX}
                      URL ${BZIP2_SOURCE_URL}
                      BUILD_BYPRODUCTS "${BZIP2_STATIC_LIB}")

  file(MAKE_DIRECTORY "${BZIP2_PREFIX}/include")
  add_library(BZip2::BZip2 STATIC IMPORTED)
  set_target_properties(
    BZip2::BZip2
    PROPERTIES IMPORTED_LOCATION "${BZIP2_STATIC_LIB}" INTERFACE_INCLUDE_DIRECTORIES
               "${BZIP2_PREFIX}/include")
  set(BZIP2_INCLUDE_DIR "${BZIP2_PREFIX}/include")

  add_dependencies(toolchain bzip2_ep)
  add_dependencies(BZip2::BZip2 bzip2_ep)
endmacro()

if(ARROW_WITH_BZ2)
  resolve_dependency(BZip2)

  if(NOT TARGET BZip2::BZip2)
    add_library(BZip2::BZip2 UNKNOWN IMPORTED)
    set_target_properties(BZip2::BZip2
                          PROPERTIES IMPORTED_LOCATION "${BZIP2_LIBRARIES}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${BZIP2_INCLUDE_DIR}")
  endif()
  include_directories(SYSTEM "${BZIP2_INCLUDE_DIR}")
endif()

macro(build_cares)
  message(STATUS "Building c-ares from source")
  set(CARES_PREFIX "${THIRDPARTY_DIR}/cares_ep-install")
  set(CARES_INCLUDE_DIR "${CARES_PREFIX}/include")

  # If you set -DCARES_SHARED=ON then the build system names the library
  # libcares_static.a
  set(
    CARES_STATIC_LIB
    "${CARES_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}cares${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )

  set(CARES_CMAKE_ARGS
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
      -DCARES_STATIC=ON
      -DCARES_SHARED=OFF
      "-DCMAKE_C_FLAGS=${EP_C_FLAGS}"
      "-DCMAKE_INSTALL_PREFIX=${CARES_PREFIX}")

  externalproject_add(cares_ep
                      ${EP_LOG_OPTIONS}
                      URL ${CARES_SOURCE_URL}
                      CMAKE_ARGS ${CARES_CMAKE_ARGS}
                      BUILD_BYPRODUCTS "${CARES_STATIC_LIB}")

  add_dependencies(toolchain cares_ep)
  add_library(c-ares::cares STATIC IMPORTED)
  set_target_properties(c-ares::cares
                        PROPERTIES IMPORTED_LOCATION "${CARES_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${CARES_INCLUDE_DIR}")
endmacro()

if(ARROW_WITH_GRPC)
  if(c-ares_SOURCE STREQUAL "AUTO")
    find_package(c-ares QUIET)
    if(NOT c-ares_FOUND)
      # Fedora doesn't package the CMake config
      find_package(c-aresAlt)
    endif()
    if(NOT c-ares_FOUND AND NOT c-aresAlt_FOUND)
      build_cares()
    endif()
  elseif(c-ares_SOURCE STREQUAL "BUNDLED")
    build_cares()
  elseif(c-ares_SOURCE STREQUAL "SYSTEM")
    find_package(c-ares QUIET)
    if(NOT c-ares_FOUND)
      # Fedora doesn't package the CMake config
      find_package(c-aresAlt REQUIRED)
    endif()
  endif()

  # TODO: Don't use global includes but rather target_include_directories
  get_target_property(CARES_INCLUDE_DIR c-ares::cares INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${CARES_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# Dependencies for Arrow Flight RPC

macro(build_grpc)
  message(STATUS "Building gRPC from source")
  set(GRPC_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/grpc_ep-prefix/src/grpc_ep-build")
  set(GRPC_PREFIX "${THIRDPARTY_DIR}/grpc_ep-install")
  set(GRPC_HOME "${GRPC_PREFIX}")
  set(GRPC_INCLUDE_DIR "${GRPC_PREFIX}/include")
  set(GRPC_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=${GRPC_PREFIX}"
                      -DBUILD_SHARED_LIBS=OFF)

  set(
    GRPC_STATIC_LIBRARY_GPR
    "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gpr${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(
    GRPC_STATIC_LIBRARY_GRPC
    "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}grpc${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(
    GRPC_STATIC_LIBRARY_GRPCPP
    "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}grpc++${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    GRPC_STATIC_LIBRARY_ADDRESS_SORTING
    "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}address_sorting${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(GRPC_CPP_PLUGIN "${GRPC_PREFIX}/bin/grpc_cpp_plugin")

  set(GRPC_CMAKE_PREFIX)

  add_custom_target(grpc_dependencies)

  if(CARES_VENDORED)
    add_dependencies(grpc_dependencies cares_ep)
  endif()

  if(GFLAGS_VENDORED)
    add_dependencies(grpc_dependencies gflags_ep)
  endif()

  add_dependencies(grpc_dependencies protobuf::libprotobuf c-ares::cares)

  get_target_property(GRPC_PROTOBUF_INCLUDE_DIR protobuf::libprotobuf
                      INTERFACE_INCLUDE_DIRECTORIES)
  get_filename_component(GRPC_PB_ROOT "${GRPC_PROTOBUF_INCLUDE_DIR}" DIRECTORY)
  get_target_property(GRPC_Protobuf_PROTOC_LIBRARY protobuf::libprotoc IMPORTED_LOCATION)
  get_target_property(GRPC_CARES_INCLUDE_DIR c-ares::cares INTERFACE_INCLUDE_DIRECTORIES)
  get_filename_component(GRPC_CARES_ROOT "${GRPC_CARES_INCLUDE_DIR}" DIRECTORY)
  get_target_property(GRPC_GFLAGS_INCLUDE_DIR ${GFLAGS_LIBRARIES}
                      INTERFACE_INCLUDE_DIRECTORIES)
  get_filename_component(GRPC_GFLAGS_ROOT "${GRPC_GFLAGS_INCLUDE_DIR}" DIRECTORY)

  set(GRPC_CMAKE_PREFIX "${GRPC_CMAKE_PREFIX};${GRPC_PB_ROOT}")
  set(GRPC_CMAKE_PREFIX "${GRPC_CMAKE_PREFIX};${GRPC_GFLAGS_ROOT}")
  set(GRPC_CMAKE_PREFIX "${GRPC_CMAKE_PREFIX};${GRPC_CARES_ROOT}")

  # ZLIB is never vendored
  set(GRPC_CMAKE_PREFIX "${GRPC_CMAKE_PREFIX};${ZLIB_ROOT}")

  if(RAPIDJSON_VENDORED)
    add_dependencies(grpc_dependencies rapidjson_ep)
  endif()

  # Yuck, see https://stackoverflow.com/a/45433229/776560
  string(REPLACE ";" "|" GRPC_PREFIX_PATH_ALT_SEP "${GRPC_CMAKE_PREFIX}")

  set(GRPC_CMAKE_ARGS
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
      -DCMAKE_PREFIX_PATH='${GRPC_PREFIX_PATH_ALT_SEP}'
      -DgRPC_CARES_PROVIDER=package
      -DgRPC_GFLAGS_PROVIDER=package
      -DgRPC_PROTOBUF_PROVIDER=package
      -DgRPC_SSL_PROVIDER=package
      -DgRPC_ZLIB_PROVIDER=package
      -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}
      -DCMAKE_C_FLAGS=${EP_C_FLAGS}
      -DCMAKE_INSTALL_PREFIX=${GRPC_PREFIX}
      -DCMAKE_INSTALL_LIBDIR=lib
      "-DProtobuf_PROTOC_LIBRARY=${GRPC_Protobuf_PROTOC_LIBRARY}"
      -DBUILD_SHARED_LIBS=OFF)

  # XXX the gRPC git checkout is huge and takes a long time
  # Ideally, we should be able to use the tarballs, but they don't contain
  # vendored dependencies such as c-ares...
  externalproject_add(grpc_ep
                      URL ${GRPC_SOURCE_URL}
                      LIST_SEPARATOR |
                      BUILD_BYPRODUCTS ${GRPC_STATIC_LIBRARY_GPR}
                                       ${GRPC_STATIC_LIBRARY_GRPC}
                                       ${GRPC_STATIC_LIBRARY_GRPCPP}
                                       ${GRPC_STATIC_LIBRARY_ADDRESS_SORTING}
                                       ${GRPC_CPP_PLUGIN}
                      CMAKE_ARGS ${GRPC_CMAKE_ARGS} ${EP_LOG_OPTIONS}
                      DEPENDS ${grpc_dependencies})

  add_library(gRPC::gpr STATIC IMPORTED)
  set_target_properties(gRPC::gpr
                        PROPERTIES IMPORTED_LOCATION "${GRPC_STATIC_LIBRARY_GPR}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}")

  add_library(gRPC::grpc STATIC IMPORTED)
  set_target_properties(gRPC::grpc
                        PROPERTIES IMPORTED_LOCATION "${GRPC_STATIC_LIBRARY_GRPC}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}")

  add_library(gRPC::grpc++ STATIC IMPORTED)
  set_target_properties(gRPC::grpc++
                        PROPERTIES IMPORTED_LOCATION "${GRPC_STATIC_LIBRARY_GRPCPP}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}")

  add_library(gRPC::address_sorting STATIC IMPORTED)
  set_target_properties(gRPC::address_sorting
                        PROPERTIES IMPORTED_LOCATION
                                   "${GRPC_STATIC_LIBRARY_ADDRESS_SORTING}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}")

  add_executable(gRPC::grpc_cpp_plugin IMPORTED)
  set_target_properties(gRPC::grpc_cpp_plugin
                        PROPERTIES IMPORTED_LOCATION ${GRPC_CPP_PLUGIN})

  add_dependencies(grpc_ep grpc_dependencies)
  add_dependencies(toolchain grpc_ep)
  add_dependencies(gRPC::gpr grpc_ep)
  add_dependencies(gRPC::grpc grpc_ep)
  add_dependencies(gRPC::grpc++ grpc_ep)
  add_dependencies(gRPC::address_sorting grpc_ep)
  set(GRPC_VENDORED TRUE)
endmacro()

if(ARROW_WITH_GRPC)
  if(gRPC_SOURCE STREQUAL "AUTO")
    find_package(gRPC QUIET)
    if(NOT gRPC_FOUND)
      # Ubuntu doesn't package the CMake config
      find_package(gRPCAlt)
    endif()
    if(NOT gRPC_FOUND AND NOT gRPCAlt_FOUND)
      build_grpc()
    endif()
  elseif(gRPC_SOURCE STREQUAL "BUNDLED")
    build_grpc()
  elseif(gRPC_SOURCE STREQUAL "SYSTEM")
    find_package(gRPC QUIET)
    if(NOT gRPC_FOUND)
      # Ubuntu doesn't package the CMake config
      find_package(gRPCAlt REQUIRED)
    endif()
  endif()

  get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin IMPORTED_LOCATION)
  if(NOT GRPC_CPP_PLUGIN)
    get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin IMPORTED_LOCATION_RELEASE)
  endif()

  if(TARGET gRPC::address_sorting)
    set(GRPC_HAS_ADDRESS_SORTING TRUE)
  else()
    set(GRPC_HAS_ADDRESS_SORTING FALSE)
  endif()

  # TODO: Don't use global includes but rather target_include_directories
  get_target_property(GRPC_INCLUDE_DIR gRPC::grpc INTERFACE_INCLUDE_DIRECTORIES)
  include_directories(SYSTEM ${GRPC_INCLUDE_DIR})

  if(GRPC_VENDORED)
    set(GRPCPP_PP_INCLUDE TRUE)
  else()
    # grpc++ headers may reside in ${GRPC_INCLUDE_DIR}/grpc++ or ${GRPC_INCLUDE_DIR}/grpcpp
    # depending on the gRPC version.
    if(EXISTS "${GRPC_INCLUDE_DIR}/grpcpp/impl/codegen/config_protobuf.h")
      set(GRPCPP_PP_INCLUDE TRUE)
    elseif(EXISTS "${GRPC_INCLUDE_DIR}/grpc++/impl/codegen/config_protobuf.h")
      set(GRPCPP_PP_INCLUDE FALSE)
    else()
      message(FATAL_ERROR "Cannot find grpc++ headers in ${GRPC_INCLUDE_DIR}")
    endif()
  endif()
endif()

# ----------------------------------------------------------------------
# Add Boost dependencies (code adapted from Apache Kudu (incubating))

set(Boost_USE_MULTITHREADED ON)
if(MSVC AND ARROW_USE_STATIC_CRT)
  set(Boost_USE_STATIC_RUNTIME ON)
endif()
set(Boost_ADDITIONAL_VERSIONS
    "1.70.0"
    "1.70"
    "1.69.0"
    "1.69"
    "1.68.0"
    "1.68"
    "1.67.0"
    "1.67"
    "1.66.0"
    "1.66"
    "1.65.0"
    "1.65"
    "1.64.0"
    "1.64"
    "1.63.0"
    "1.63"
    "1.62.0"
    "1.61"
    "1.61.0"
    "1.62"
    "1.60.0"
    "1.60")

if(ARROW_BOOST_VENDORED)
  set(BOOST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/boost_ep-prefix/src/boost_ep")
  set(BOOST_LIB_DIR "${BOOST_PREFIX}/stage/lib")
  set(BOOST_BUILD_LINK "static")
  set(
    BOOST_STATIC_SYSTEM_LIBRARY
    "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_system${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    BOOST_STATIC_FILESYSTEM_LIBRARY
    "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_filesystem${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    BOOST_STATIC_REGEX_LIBRARY
    "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_regex${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(BOOST_SYSTEM_LIBRARY boost_system_static)
  set(BOOST_FILESYSTEM_LIBRARY boost_filesystem_static)
  set(BOOST_REGEX_LIBRARY boost_regex_static)

  if(ARROW_BOOST_HEADER_ONLY)
    set(BOOST_BUILD_PRODUCTS)
    set(BOOST_CONFIGURE_COMMAND "")
    set(BOOST_BUILD_COMMAND "")
  else()
    set(BOOST_BUILD_PRODUCTS ${BOOST_STATIC_SYSTEM_LIBRARY}
                             ${BOOST_STATIC_FILESYSTEM_LIBRARY}
                             ${BOOST_STATIC_REGEX_LIBRARY})
    set(BOOST_CONFIGURE_COMMAND "./bootstrap.sh" "--prefix=${BOOST_PREFIX}"
                                "--with-libraries=filesystem,regex,system")
    if("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
      set(BOOST_BUILD_VARIANT "debug")
    else()
      set(BOOST_BUILD_VARIANT "release")
    endif()
    set(BOOST_BUILD_COMMAND "./b2" "link=${BOOST_BUILD_LINK}"
                            "variant=${BOOST_BUILD_VARIANT}" "cxxflags=-fPIC")

    add_thirdparty_lib(boost_system STATIC_LIB "${BOOST_STATIC_SYSTEM_LIBRARY}")

    add_thirdparty_lib(boost_filesystem STATIC_LIB "${BOOST_STATIC_FILESYSTEM_LIBRARY}")

    add_thirdparty_lib(boost_regex STATIC_LIB "${BOOST_STATIC_REGEX_LIBRARY}")

    set(ARROW_BOOST_LIBS ${BOOST_SYSTEM_LIBRARY} ${BOOST_FILESYSTEM_LIBRARY})
  endif()
  externalproject_add(boost_ep
                      URL ${BOOST_SOURCE_URL}
                      BUILD_BYPRODUCTS ${BOOST_BUILD_PRODUCTS}
                      BUILD_IN_SOURCE 1
                      CONFIGURE_COMMAND ${BOOST_CONFIGURE_COMMAND}
                      BUILD_COMMAND ${BOOST_BUILD_COMMAND}
                      INSTALL_COMMAND "" ${EP_LOG_OPTIONS})
  set(Boost_INCLUDE_DIR "${BOOST_PREFIX}")
  set(Boost_INCLUDE_DIRS "${BOOST_INCLUDE_DIR}")
  add_dependencies(toolchain boost_ep)
else()
  if(MSVC)
    # disable autolinking in boost
    add_definitions(-DBOOST_ALL_NO_LIB)
  endif()

  if(DEFINED ENV{BOOST_ROOT} OR DEFINED BOOST_ROOT)
    # In older versions of CMake (such as 3.2), the system paths for Boost will
    # be looked in first even if we set $BOOST_ROOT or pass -DBOOST_ROOT
    set(Boost_NO_SYSTEM_PATHS ON)
  endif()

  if(ARROW_BOOST_USE_SHARED)
    # Find shared Boost libraries.
    set(Boost_USE_STATIC_LIBS OFF)
    set(BUILD_SHARED_LIBS_KEEP ${BUILD_SHARED_LIBS})
    set(BUILD_SHARED_LIBS ON)

    if(MSVC)
      # force all boost libraries to dynamic link
      add_definitions(-DBOOST_ALL_DYN_LINK)
    endif()

    if(ARROW_BOOST_HEADER_ONLY)
      find_package(Boost REQUIRED)
    else()
      find_package(Boost COMPONENTS regex system filesystem REQUIRED)
      set(BOOST_SYSTEM_LIBRARY Boost::system)
      set(BOOST_FILESYSTEM_LIBRARY Boost::filesystem)
      set(BOOST_REGEX_LIBRARY Boost::regex)
      set(ARROW_BOOST_LIBS ${BOOST_SYSTEM_LIBRARY} ${BOOST_FILESYSTEM_LIBRARY})
    endif()
    set(BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS_KEEP})
    unset(BUILD_SHARED_LIBS_KEEP)
  else()
    # Find static boost headers and libs
    # TODO Differentiate here between release and debug builds
    set(Boost_USE_STATIC_LIBS ON)
    if(ARROW_BOOST_HEADER_ONLY)
      find_package(Boost REQUIRED)
    else()
      find_package(Boost COMPONENTS regex system filesystem REQUIRED)
      set(BOOST_SYSTEM_LIBRARY Boost::system)
      set(BOOST_FILESYSTEM_LIBRARY Boost::filesystem)
      set(BOOST_REGEX_LIBRARY Boost::regex)
      set(ARROW_BOOST_LIBS ${BOOST_SYSTEM_LIBRARY} ${BOOST_FILESYSTEM_LIBRARY})
    endif()
  endif()
endif()

message(STATUS "Boost include dir: " ${Boost_INCLUDE_DIR})
message(STATUS "Boost libraries: " ${Boost_LIBRARIES})

include_directories(SYSTEM ${Boost_INCLUDE_DIR})

#
# HDFS thirdparty setup

if(DEFINED ENV{HADOOP_HOME})
  set(HADOOP_HOME $ENV{HADOOP_HOME})
  if(NOT EXISTS "${HADOOP_HOME}/include/hdfs.h")
    message(STATUS "Did not find hdfs.h in expected location, using vendored one")
    set(HADOOP_HOME "${THIRDPARTY_DIR}/hadoop")
  endif()
else()
  set(HADOOP_HOME "${THIRDPARTY_DIR}/hadoop")
endif()

set(HDFS_H_PATH "${HADOOP_HOME}/include/hdfs.h")
if(NOT EXISTS ${HDFS_H_PATH})
  message(FATAL_ERROR "Did not find hdfs.h at ${HDFS_H_PATH}")
endif()
message(STATUS "Found hdfs.h at: " ${HDFS_H_PATH})

include_directories(SYSTEM "${HADOOP_HOME}/include")

# ----------------------------------------------------------------------
# Apache ORC

if(ARROW_ORC)
  # orc
  if("${ORC_HOME}" STREQUAL "")
    set(ORC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/orc_ep-install")
    set(ORC_HOME "${ORC_PREFIX}")
    set(ORC_INCLUDE_DIR "${ORC_PREFIX}/include")
    set(
      ORC_STATIC_LIB
      "${ORC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}orc${CMAKE_STATIC_LIBRARY_SUFFIX}")

    if("${COMPILER_FAMILY}" STREQUAL "clang")
      if("${COMPILER_VERSION}" VERSION_EQUAL "4.0")
        # conda OSX builds uses clang 4.0.1 and orc_ep fails to build unless
        # disabling the following errors
        set(ORC_CMAKE_CXX_FLAGS " -Wno-error=weak-vtables -Wno-error=undef ")
      endif()
      if("${COMPILER_VERSION}" VERSION_GREATER "4.0")
        set(ORC_CMAKE_CXX_FLAGS " -Wno-zero-as-null-pointer-constant \
  -Wno-inconsistent-missing-destructor-override ")
      endif()
    endif()

    set(ORC_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} ${ORC_CMAKE_CXX_FLAGS}")

    get_target_property(ORC_PROTOBUF_INCLUDE_DIR protobuf::libprotobuf
                        INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ORC_PB_ROOT "${ORC_PROTOBUF_INCLUDE_DIR}" DIRECTORY)
    get_target_property(ORC_PROTOBUF_LIBRARY protobuf::libprotobuf IMPORTED_LOCATION)

    get_target_property(ORC_SNAPPY_INCLUDE_DIR Snappy::snappy
                        INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ORC_SNAPPY_ROOT "${ORC_SNAPPY_INCLUDE_DIR}" DIRECTORY)

    get_target_property(ORC_LZ4_ROOT LZ4::lz4 INTERFACE_INCLUDE_DIRECTORIES)
    get_filename_component(ORC_LZ4_ROOT "${ORC_LZ4_ROOT}" DIRECTORY)

    # Weirdly passing in PROTOBUF_LIBRARY for PROTOC_LIBRARY still results in ORC finding
    # the protoc library.
    set(ORC_CMAKE_ARGS
        ${EP_COMMON_CMAKE_ARGS}
        "-DCMAKE_INSTALL_PREFIX=${ORC_PREFIX}"
        -DCMAKE_CXX_FLAGS=${ORC_CMAKE_CXX_FLAGS}
        -DBUILD_LIBHDFSPP=OFF
        -DBUILD_JAVA=OFF
        -DBUILD_TOOLS=OFF
        -DBUILD_CPP_TESTS=OFF
        -DINSTALL_VENDORED_LIBS=OFF
        "-DSNAPPY_HOME=${ORC_SNAPPY_ROOT}"
        "-DSNAPPY_INCLUDE_DIR=${ORC_SNAPPY_INCLUDE_DIR}"
        "-DPROTOBUF_HOME=${ORC_PB_ROOT}"
        "-DPROTOBUF_INCLUDE_DIR=${ORC_PROTOBUF_INCLUDE_DIR}"
        "-DPROTOBUF_LIBRARY=${ORC_PROTOBUF_LIBRARY}"
        "-DPROTOC_LIBRARY=${ORC_PROTOBUF_LIBRARY}"
        "-DLZ4_HOME=${LZ4_HOME}")
    if(ZLIB_ROOT)
      set(ORC_CMAKE_ARGS ${ORC_CMAKE_ARGS} "-DZLIB_HOME=${ZLIB_ROOT}")
    endif()

    externalproject_add(orc_ep
                        URL ${ORC_SOURCE_URL}
                        BUILD_BYPRODUCTS ${ORC_STATIC_LIB}
                        CMAKE_ARGS ${ORC_CMAKE_ARGS} ${EP_LOG_OPTIONS})

    add_dependencies(toolchain orc_ep)

    set(ORC_VENDORED 1)
    add_dependencies(orc_ep ZLIB::ZLIB)
    add_dependencies(orc_ep LZ4::lz4)
    add_dependencies(orc_ep Snappy::snappy)
    add_dependencies(orc_ep protobuf::libprotobuf)
  else()
    set(ORC_INCLUDE_DIR "${ORC_HOME}/include")
    set(ORC_STATIC_LIB
        "${ORC_HOME}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}orc${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(ORC_VENDORED 0)
  endif()

  include_directories(SYSTEM ${ORC_INCLUDE_DIR})
  add_thirdparty_lib(orc
                     STATIC_LIB
                     ${ORC_STATIC_LIB}
                     DEPS
                     protobuf::libprotobuf)

  if(ORC_VENDORED)
    add_dependencies(orc_static orc_ep)
  endif()
endif()

# Write out the package configurations.

configure_file("src/arrow/util/config.h.cmake" "src/arrow/util/config.h")
install(FILES "${ARROW_BINARY_DIR}/src/arrow/util/config.h"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/util")
