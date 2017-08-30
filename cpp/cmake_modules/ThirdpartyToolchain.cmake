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


# ----------------------------------------------------------------------
# Thirdparty toolchain

set(THIRDPARTY_DIR "${CMAKE_SOURCE_DIR}/thirdparty")
set(GFLAGS_VERSION "2.2.0")
set(GTEST_VERSION "1.8.0")
set(GBENCHMARK_VERSION "1.1.0")
set(FLATBUFFERS_VERSION "1.7.1")
set(JEMALLOC_VERSION "4.4.0")
set(SNAPPY_VERSION "1.1.3")
set(BROTLI_VERSION "v0.6.0")
set(LZ4_VERSION "1.7.5")
set(ZSTD_VERSION "1.2.0")

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

if (NOT ARROW_VERBOSE_THIRDPARTY_BUILD)
  set(EP_LOG_OPTIONS
    LOG_CONFIGURE 1
    LOG_BUILD 1
    LOG_INSTALL 1
    LOG_DOWNLOAD 1)
else()
  set(EP_LOG_OPTIONS)
endif()

if (NOT MSVC)
  # Set -fPIC on all external projects
  set(EP_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")
  set(EP_C_FLAGS "${EP_C_FLAGS} -fPIC")
endif()

if (NOT "$ENV{ARROW_BUILD_TOOLCHAIN}" STREQUAL "")
  set(FLATBUFFERS_HOME "$ENV{ARROW_BUILD_TOOLCHAIN}")
  set(RAPIDJSON_HOME "$ENV{ARROW_BUILD_TOOLCHAIN}")
  set(JEMALLOC_HOME "$ENV{ARROW_BUILD_TOOLCHAIN}")
  set(GFLAGS_HOME "$ENV{ARROW_BUILD_TOOLCHAIN}")
  set(SNAPPY_HOME "$ENV{ARROW_BUILD_TOOLCHAIN}")
  set(ZLIB_HOME "$ENV{ARROW_BUILD_TOOLCHAIN}")
  set(BROTLI_HOME "$ENV{ARROW_BUILD_TOOLCHAIN}")
  set(LZ4_HOME "$ENV{ARROW_BUILD_TOOLCHAIN}")
  set(ZSTD_HOME "$ENV{ARROW_BUILD_TOOLCHAIN}")

  if (NOT DEFINED ENV{BOOST_ROOT})
    # Since we have to set this in the environment, we check whether
    # $BOOST_ROOT is defined inside here
    set(ENV{BOOST_ROOT} "$ENV{ARROW_BUILD_TOOLCHAIN}")
  endif()
endif()

if (DEFINED ENV{FLATBUFFERS_HOME})
  set(FLATBUFFERS_HOME "$ENV{FLATBUFFERS_HOME}")
endif()

if (DEFINED ENV{RAPIDJSON_HOME})
  set(RAPIDJSON_HOME "$ENV{RAPIDJSON_HOME}")
endif()

if (DEFINED ENV{JEMALLOC_HOME})
  set(JEMALLOC_HOME "$ENV{JEMALLOC_HOME}")
endif()

if (DEFINED ENV{GFLAGS_HOME})
  set(GFLAGS_HOME "$ENV{GFLAGS_HOME}")
endif()

if (DEFINED ENV{SNAPPY_HOME})
  set(SNAPPY_HOME "$ENV{SNAPPY_HOME}")
endif()

if (DEFINED ENV{ZLIB_HOME})
  set(ZLIB_HOME "$ENV{ZLIB_HOME}")
endif()

if (DEFINED ENV{BROTLI_HOME})
  set(BROTLI_HOME "$ENV{BROTLI_HOME}")
endif()

if (DEFINED ENV{LZ4_HOME})
  set(LZ4_HOME "$ENV{LZ4_HOME}")
endif()

if (DEFINED ENV{ZSTD_HOME})
  set(ZSTD_HOME "$ENV{ZSTD_HOME}")
endif()

# Ensure that a default make is set
if ("${MAKE}" STREQUAL "")
    if (NOT MSVC)
        find_program(MAKE make)
    endif()
endif()

# ----------------------------------------------------------------------
# Find pthreads

if (NOT MSVC)
  find_library(PTHREAD_LIBRARY pthread)
  message(STATUS "Found pthread: ${PTHREAD_LIBRARY}")
endif()

# ----------------------------------------------------------------------
# Add Boost dependencies (code adapted from Apache Kudu (incubating))

set(Boost_DEBUG TRUE)
set(Boost_USE_MULTITHREADED ON)
set(Boost_ADDITIONAL_VERSIONS
  "1.64.0" "1.64"
  "1.63.0" "1.63"
  "1.62.0" "1.61"
  "1.61.0" "1.62"
  "1.60.0" "1.60")
list(GET Boost_ADDITIONAL_VERSIONS 0 BOOST_LATEST_VERSION)
string(REPLACE "." "_" BOOST_LATEST_VERSION_IN_PATH ${BOOST_LATEST_VERSION})
set(BOOST_LATEST_URL
  "https://dl.bintray.com/boostorg/release/${BOOST_LATEST_VERSION}/source/boost_${BOOST_LATEST_VERSION_IN_PATH}.tar.gz")

if (ARROW_BOOST_VENDORED)
  set(BOOST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/boost_ep-prefix/src/boost_ep")
  set(BOOST_LIB_DIR "${BOOST_PREFIX}/stage/lib")
  set(BOOST_BUILD_LINK "static")
  set(BOOST_STATIC_SYSTEM_LIBRARY
    "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_system${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(BOOST_STATIC_FILESYSTEM_LIBRARY
    "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_filesystem${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(BOOST_SYSTEM_LIBRARY "${BOOST_STATIC_SYSTEM_LIBRARY}")
  set(BOOST_FILESYSTEM_LIBRARY "${BOOST_STATIC_FILESYSTEM_LIBRARY}")
  if (ARROW_BOOST_HEADER_ONLY)
    set(BOOST_BUILD_PRODUCTS)
    set(BOOST_CONFIGURE_COMMAND "")
    set(BOOST_BUILD_COMMAND "")
  else()
    set(BOOST_BUILD_PRODUCTS
      ${BOOST_SYSTEM_LIBRARY}
      ${BOOST_FILESYSTEM_LIBRARY})
    set(BOOST_CONFIGURE_COMMAND
      "./bootstrap.sh"
      "--prefix=${BOOST_PREFIX}"
      "--with-libraries=filesystem,system")
    if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
      set(BOOST_BUILD_VARIANT "debug")
    else()
      set(BOOST_BUILD_VARIANT "release")
    endif()
    set(BOOST_BUILD_COMMAND
      "./b2"
      "link=${BOOST_BUILD_LINK}"
      "variant=${BOOST_BUILD_VARIANT}"
      "cxxflags=-fPIC")
  endif()
  ExternalProject_Add(boost_ep
    URL ${BOOST_LATEST_URL}
    BUILD_BYPRODUCTS ${BOOST_BUILD_PRODUCTS}
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND ${BOOST_CONFIGURE_COMMAND}
    BUILD_COMMAND ${BOOST_BUILD_COMMAND}
    INSTALL_COMMAND ""
    ${EP_LOG_OPTIONS})
  set(Boost_INCLUDE_DIR "${BOOST_PREFIX}")
  set(Boost_INCLUDE_DIRS "${BOOST_INCLUDE_DIR}")
  add_dependencies(arrow_dependencies boost_ep)
else()
  if (ARROW_BOOST_USE_SHARED)
    # Find shared Boost libraries.
    set(Boost_USE_STATIC_LIBS OFF)

    if(MSVC)
      # disable autolinking in boost
      add_definitions(-DBOOST_ALL_NO_LIB)

      # force all boost libraries to dynamic link
      add_definitions(-DBOOST_ALL_DYN_LINK)
    endif()

    if (ARROW_BOOST_HEADER_ONLY)
      find_package(Boost REQUIRED)
    else()
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
    endif()
  else()
    # Find static boost headers and libs
    # TODO Differentiate here between release and debug builds
    set(Boost_USE_STATIC_LIBS ON)
    if (ARROW_BOOST_HEADER_ONLY)
      find_package(Boost REQUIRED)
    else()
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
  endif()
endif()

message(STATUS "Boost include dir: " ${Boost_INCLUDE_DIRS})
message(STATUS "Boost libraries: " ${Boost_LIBRARIES})

if (NOT ARROW_BOOST_HEADER_ONLY)
  ADD_THIRDPARTY_LIB(boost_system
      STATIC_LIB "${BOOST_STATIC_SYSTEM_LIBRARY}"
      SHARED_LIB "${BOOST_SHARED_SYSTEM_LIBRARY}")

  ADD_THIRDPARTY_LIB(boost_filesystem
      STATIC_LIB "${BOOST_STATIC_FILESYSTEM_LIBRARY}"
      SHARED_LIB "${BOOST_SHARED_FILESYSTEM_LIBRARY}")

  SET(ARROW_BOOST_LIBS boost_system boost_filesystem)
endif()

include_directories(SYSTEM ${Boost_INCLUDE_DIR})

if(ARROW_BUILD_TESTS OR ARROW_BUILD_BENCHMARKS)
  add_custom_target(unittest ctest -L unittest)

  if("$ENV{GTEST_HOME}" STREQUAL "")
    if(APPLE)
      set(GTEST_CMAKE_CXX_FLAGS "-fPIC -DGTEST_USE_OWN_TR1_TUPLE=1 -Wno-unused-value -Wno-ignored-attributes")
    elseif(NOT MSVC)
      set(GTEST_CMAKE_CXX_FLAGS "-fPIC")
    endif()
    string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)
    set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}} ${GTEST_CMAKE_CXX_FLAGS}")

    set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
    set(GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")
    set(GTEST_STATIC_LIB
      "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GTEST_MAIN_STATIC_LIB
      "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest_main${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GTEST_VENDORED 1)
    set(GTEST_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                         -DCMAKE_INSTALL_PREFIX=${GTEST_PREFIX}
                         -Dgtest_force_shared_crt=ON
                         -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS})

    ExternalProject_Add(googletest_ep
      URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
      BUILD_BYPRODUCTS ${GTEST_STATIC_LIB} ${GTEST_MAIN_STATIC_LIB}
      CMAKE_ARGS ${GTEST_CMAKE_ARGS}
      ${EP_LOG_OPTIONS})
  else()
    find_package(GTest REQUIRED)
    set(GTEST_VENDORED 0)
  endif()

  message(STATUS "GTest include dir: ${GTEST_INCLUDE_DIR}")
  message(STATUS "GTest static library: ${GTEST_STATIC_LIB}")
  include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(gtest
    STATIC_LIB ${GTEST_STATIC_LIB})
  ADD_THIRDPARTY_LIB(gtest_main
    STATIC_LIB ${GTEST_MAIN_STATIC_LIB})

  if(GTEST_VENDORED)
    add_dependencies(gtest googletest_ep)
    add_dependencies(gtest_main googletest_ep)
  endif()

  # gflags (formerly Googleflags) command line parsing
  if("${GFLAGS_HOME}" STREQUAL "")
    set(GFLAGS_CMAKE_CXX_FLAGS ${EP_CXX_FLAGS})

    set(GFLAGS_URL "https://github.com/gflags/gflags/archive/v${GFLAGS_VERSION}.tar.gz")
    set(GFLAGS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gflags_ep-prefix/src/gflags_ep")
    set(GFLAGS_HOME "${GFLAGS_PREFIX}")
    set(GFLAGS_INCLUDE_DIR "${GFLAGS_PREFIX}/include")
    if(MSVC)
      set(GFLAGS_STATIC_LIB "${GFLAGS_PREFIX}/lib/gflags_static.lib")
    else()
      set(GFLAGS_STATIC_LIB "${GFLAGS_PREFIX}/lib/libgflags.a")
    endif()
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

    ExternalProject_Add(gflags_ep
      URL ${GFLAGS_URL}
      ${EP_LOG_OPTIONS}
      BUILD_IN_SOURCE 1
      BUILD_BYPRODUCTS "${GFLAGS_STATIC_LIB}"
      CMAKE_ARGS ${GFLAGS_CMAKE_ARGS})
  else()
    set(GFLAGS_VENDORED 0)
    find_package(GFlags REQUIRED)
  endif()

  message(STATUS "GFlags include dir: ${GFLAGS_INCLUDE_DIR}")
  message(STATUS "GFlags static library: ${GFLAGS_STATIC_LIB}")
  include_directories(SYSTEM ${GFLAGS_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(gflags
    STATIC_LIB ${GFLAGS_STATIC_LIB})
  if(MSVC)
    set_target_properties(gflags
      PROPERTIES
      IMPORTED_LINK_INTERFACE_LIBRARIES "shlwapi.lib")
  endif()

  if(GFLAGS_VENDORED)
    add_dependencies(gflags gflags_ep)
  endif()
endif()

if(ARROW_BUILD_BENCHMARKS)
  add_custom_target(runbenchmark ctest -L benchmark)

  if("$ENV{GBENCHMARK_HOME}" STREQUAL "")
    if(APPLE)
      set(GBENCHMARK_CMAKE_CXX_FLAGS "-fPIC -std=c++11 -stdlib=libc++")
    elseif(NOT MSVC)
      set(GBENCHMARK_CMAKE_CXX_FLAGS "-fPIC --std=c++11")
    endif()

    set(GBENCHMARK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gbenchmark_ep/src/gbenchmark_ep-install")
    set(GBENCHMARK_INCLUDE_DIR "${GBENCHMARK_PREFIX}/include")
    set(GBENCHMARK_STATIC_LIB "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GBENCHMARK_VENDORED 1)
    set(GBENCHMARK_CMAKE_ARGS
          "-DCMAKE_BUILD_TYPE=Release"
          "-DCMAKE_INSTALL_PREFIX:PATH=${GBENCHMARK_PREFIX}"
          "-DBENCHMARK_ENABLE_TESTING=OFF"
          "-DCMAKE_CXX_FLAGS=${GBENCHMARK_CMAKE_CXX_FLAGS}")
    if (APPLE)
      set(GBENCHMARK_CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS} "-DBENCHMARK_USE_LIBCXX=ON")
    endif()

    ExternalProject_Add(gbenchmark_ep
      URL "https://github.com/google/benchmark/archive/v${GBENCHMARK_VERSION}.tar.gz"
      BUILD_BYPRODUCTS "${GBENCHMARK_STATIC_LIB}"
      CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS}
      ${EP_LOG_OPTIONS})
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

if (ARROW_IPC)
  # RapidJSON, header only dependency
  if("${RAPIDJSON_HOME}" STREQUAL "")
    ExternalProject_Add(rapidjson_ep
      PREFIX "${CMAKE_BINARY_DIR}"
      URL "https://github.com/miloyip/rapidjson/archive/v1.1.0.tar.gz"
      URL_MD5 "badd12c511e081fec6c89c43a7027bce"
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ""
      BUILD_IN_SOURCE 1
      ${EP_LOG_OPTIONS}
      INSTALL_COMMAND "")

    ExternalProject_Get_Property(rapidjson_ep SOURCE_DIR)
    set(RAPIDJSON_INCLUDE_DIR "${SOURCE_DIR}/include")
    set(RAPIDJSON_VENDORED 1)
  else()
    set(RAPIDJSON_INCLUDE_DIR "${RAPIDJSON_HOME}/include")
    set(RAPIDJSON_VENDORED 0)
  endif()
  message(STATUS "RapidJSON include dir: ${RAPIDJSON_INCLUDE_DIR}")
  include_directories(SYSTEM ${RAPIDJSON_INCLUDE_DIR})

  if(RAPIDJSON_VENDORED)
    add_dependencies(arrow_dependencies rapidjson_ep)
  endif()

  ## Flatbuffers
  if("${FLATBUFFERS_HOME}" STREQUAL "")
    set(FLATBUFFERS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/flatbuffers_ep-prefix/src/flatbuffers_ep-install")
    if (MSVC)
      set(FLATBUFFERS_CMAKE_CXX_FLAGS /EHsc)
    else()
      set(FLATBUFFERS_CMAKE_CXX_FLAGS -fPIC)
    endif()
    ExternalProject_Add(flatbuffers_ep
      URL "https://github.com/google/flatbuffers/archive/v${FLATBUFFERS_VERSION}.tar.gz"
      CMAKE_ARGS
      "-DCMAKE_CXX_FLAGS=${FLATBUFFERS_CMAKE_CXX_FLAGS}"
      "-DCMAKE_INSTALL_PREFIX:PATH=${FLATBUFFERS_PREFIX}"
      "-DFLATBUFFERS_BUILD_TESTS=OFF"
      ${EP_LOG_OPTIONS})

    set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_PREFIX}/include")
    set(FLATBUFFERS_COMPILER "${FLATBUFFERS_PREFIX}/bin/flatc")
    set(FLATBUFFERS_VENDORED 1)
  else()
    find_package(Flatbuffers REQUIRED)
    set(FLATBUFFERS_VENDORED 0)
  endif()

  if(FLATBUFFERS_VENDORED)
    add_dependencies(arrow_dependencies flatbuffers_ep)
  endif()

  message(STATUS "Flatbuffers include dir: ${FLATBUFFERS_INCLUDE_DIR}")
  message(STATUS "Flatbuffers compiler: ${FLATBUFFERS_COMPILER}")
  include_directories(SYSTEM ${FLATBUFFERS_INCLUDE_DIR})
endif()
#----------------------------------------------------------------------

if (MSVC)
  # jemalloc is not supported on Windows
  set(ARROW_JEMALLOC off)
endif()

if (ARROW_JEMALLOC)
  find_package(jemalloc)

  if(NOT JEMALLOC_FOUND)
    set(ARROW_JEMALLOC_USE_SHARED OFF)
    set(JEMALLOC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/jemalloc_ep-prefix/src/jemalloc_ep/dist/")
    set(JEMALLOC_HOME "${JEMALLOC_PREFIX}")
    set(JEMALLOC_INCLUDE_DIR "${JEMALLOC_PREFIX}/include")
    set(JEMALLOC_SHARED_LIB "${JEMALLOC_PREFIX}/lib/libjemalloc${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(JEMALLOC_STATIC_LIB "${JEMALLOC_PREFIX}/lib/libjemalloc_pic${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(JEMALLOC_VENDORED 1)
    ExternalProject_Add(jemalloc_ep
      URL https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_VERSION}/jemalloc-${JEMALLOC_VERSION}.tar.bz2
      CONFIGURE_COMMAND ./configure "--prefix=${JEMALLOC_PREFIX}" "--with-jemalloc-prefix=je_arrow_" "--with-private-namespace=je_arrow_private_"
      ${EP_LOG_OPTIONS}
      BUILD_IN_SOURCE 1
      BUILD_COMMAND ${MAKE}
      BUILD_BYPRODUCTS "${JEMALLOC_STATIC_LIB}" "${JEMALLOC_SHARED_LIB}"
      INSTALL_COMMAND ${MAKE} -j1 install)
  else()
    set(JEMALLOC_VENDORED 0)
  endif()

  include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(jemalloc
    STATIC_LIB ${JEMALLOC_STATIC_LIB}
    SHARED_LIB ${JEMALLOC_SHARED_LIB}
    DEPS ${PTHREAD_LIBRARY})
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

########################################################################
# HDFS thirdparty setup

if (DEFINED ENV{HADOOP_HOME})
  set(HADOOP_HOME $ENV{HADOOP_HOME})
  if (NOT EXISTS "${HADOOP_HOME}/include/hdfs.h")
    message(STATUS "Did not find hdfs.h in expected location, using vendored one")
    set(HADOOP_HOME "${THIRDPARTY_DIR}/hadoop")
  endif()
else()
  set(HADOOP_HOME "${THIRDPARTY_DIR}/hadoop")
endif()

set(HDFS_H_PATH "${HADOOP_HOME}/include/hdfs.h")
if (NOT EXISTS ${HDFS_H_PATH})
  message(FATAL_ERROR "Did not find hdfs.h at ${HDFS_H_PATH}")
endif()
message(STATUS "Found hdfs.h at: " ${HDFS_H_PATH})

include_directories(SYSTEM "${HADOOP_HOME}/include")

if (ARROW_WITH_ZLIB)
# ----------------------------------------------------------------------
# ZLIB

  if("${ZLIB_HOME}" STREQUAL "")
    set(ZLIB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zlib_ep/src/zlib_ep-install")
    set(ZLIB_HOME "${ZLIB_PREFIX}")
    set(ZLIB_INCLUDE_DIR "${ZLIB_PREFIX}/include")
    if (MSVC)
      if (${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
        set(ZLIB_STATIC_LIB_NAME zlibstaticd.lib)
      else()
        set(ZLIB_STATIC_LIB_NAME zlibstatic.lib)
      endif()
    else()
      set(ZLIB_STATIC_LIB_NAME libz.a)
    endif()
    set(ZLIB_STATIC_LIB "${ZLIB_PREFIX}/lib/${ZLIB_STATIC_LIB_NAME}")
    set(ZLIB_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                        -DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}
                        -DCMAKE_C_FLAGS=${EP_C_FLAGS}
                        -DBUILD_SHARED_LIBS=OFF)

    ExternalProject_Add(zlib_ep
      URL "http://zlib.net/fossils/zlib-1.2.8.tar.gz"
      ${EP_LOG_OPTIONS}
      BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}"
      CMAKE_ARGS ${ZLIB_CMAKE_ARGS})
    set(ZLIB_VENDORED 1)
  else()
    find_package(ZLIB REQUIRED)
    set(ZLIB_VENDORED 0)
  endif()

  include_directories(SYSTEM ${ZLIB_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(zlib
    STATIC_LIB ${ZLIB_STATIC_LIB})

  if (ZLIB_VENDORED)
    add_dependencies(zlib zlib_ep)
  endif()
endif()

if (ARROW_WITH_SNAPPY)
# ----------------------------------------------------------------------
# Snappy

  if("${SNAPPY_HOME}" STREQUAL "")
    set(SNAPPY_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/snappy_ep/src/snappy_ep-install")
    set(SNAPPY_HOME "${SNAPPY_PREFIX}")
    set(SNAPPY_INCLUDE_DIR "${SNAPPY_PREFIX}/include")
    if (MSVC)
      set(SNAPPY_STATIC_LIB_NAME snappy_static)
    else()
      set(SNAPPY_STATIC_LIB_NAME snappy)
    endif()
    set(SNAPPY_STATIC_LIB "${SNAPPY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${SNAPPY_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(SNAPPY_SRC_URL "https://github.com/google/snappy/releases/download/${SNAPPY_VERSION}/snappy-${SNAPPY_VERSION}.tar.gz")

    if (${UPPERCASE_BUILD_TYPE} EQUAL "RELEASE")
      if (APPLE)
        set(SNAPPY_CXXFLAGS "CXXFLAGS='-DNDEBUG -O1'")
      else()
        set(SNAPPY_CXXFLAGS "CXXFLAGS='-DNDEBUG -O2'")
      endif()
    endif()

    if (MSVC)
      set(SNAPPY_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                            "-DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}"
                            "-DCMAKE_C_FLAGS=${EX_C_FLAGS}"
                            "-DCMAKE_INSTALL_PREFIX=${SNAPPY_PREFIX}")
      set(SNAPPY_UPDATE_COMMAND ${CMAKE_COMMAND} -E copy
                        ${CMAKE_SOURCE_DIR}/cmake_modules/SnappyCMakeLists.txt
                        ./CMakeLists.txt &&
                        ${CMAKE_COMMAND} -E copy
                        ${CMAKE_SOURCE_DIR}/cmake_modules/SnappyConfig.h
                        ./config.h)
      ExternalProject_Add(snappy_ep
        UPDATE_COMMAND ${SNAPPY_UPDATE_COMMAND}
        ${EP_LOG_OPTIONS}
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ${MAKE}
        INSTALL_DIR ${SNAPPY_PREFIX}
        URL ${SNAPPY_SRC_URL}
        CMAKE_ARGS ${SNAPPY_CMAKE_ARGS}
        BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}")
    else()
      ExternalProject_Add(snappy_ep
        CONFIGURE_COMMAND ./configure --with-pic "--prefix=${SNAPPY_PREFIX}" ${SNAPPY_CXXFLAGS}
        ${EP_LOG_OPTIONS}
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ${MAKE}
        INSTALL_DIR ${SNAPPY_PREFIX}
        URL ${SNAPPY_SRC_URL}
        BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}")
    endif()
    set(SNAPPY_VENDORED 1)
  else()
    find_package(Snappy REQUIRED)
    set(SNAPPY_VENDORED 0)
  endif()

  include_directories(SYSTEM ${SNAPPY_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(snappy
    STATIC_LIB ${SNAPPY_STATIC_LIB})

  if (SNAPPY_VENDORED)
    add_dependencies(snappy snappy_ep)
  endif()
endif()

if (ARROW_WITH_BROTLI)
# ----------------------------------------------------------------------
# Brotli

  if("${BROTLI_HOME}" STREQUAL "")
    set(BROTLI_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/brotli_ep/src/brotli_ep-install")
    set(BROTLI_HOME "${BROTLI_PREFIX}")
    set(BROTLI_INCLUDE_DIR "${BROTLI_PREFIX}/include")
    if (MSVC)
      set(BROTLI_LIB_DIR bin)
    else()
      set(BROTLI_LIB_DIR lib)
    endif()
    set(BROTLI_STATIC_LIBRARY_ENC "${BROTLI_PREFIX}/${BROTLI_LIB_DIR}/${CMAKE_LIBRARY_ARCHITECTURE}/${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(BROTLI_STATIC_LIBRARY_DEC "${BROTLI_PREFIX}/${BROTLI_LIB_DIR}/${CMAKE_LIBRARY_ARCHITECTURE}/${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(BROTLI_STATIC_LIBRARY_COMMON "${BROTLI_PREFIX}/${BROTLI_LIB_DIR}/${CMAKE_LIBRARY_ARCHITECTURE}/${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(BROTLI_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                          "-DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}"
                          "-DCMAKE_C_FLAGS=${EX_C_FLAGS}"
                          -DCMAKE_INSTALL_PREFIX=${BROTLI_PREFIX}
                          -DCMAKE_INSTALL_LIBDIR=lib/${CMAKE_LIBRARY_ARCHITECTURE}
                          -DBUILD_SHARED_LIBS=OFF)

    ExternalProject_Add(brotli_ep
      URL "https://github.com/google/brotli/archive/${BROTLI_VERSION}.tar.gz"
      BUILD_BYPRODUCTS "${BROTLI_STATIC_LIBRARY_ENC}" "${BROTLI_STATIC_LIBRARY_DEC}" "${BROTLI_STATIC_LIBRARY_COMMON}"
      ${BROTLI_BUILD_BYPRODUCTS}
      ${EP_LOG_OPTIONS}
      CMAKE_ARGS ${BROTLI_CMAKE_ARGS}
      STEP_TARGETS headers_copy)
    if (MSVC)
      ExternalProject_Get_Property(brotli_ep SOURCE_DIR)

      ExternalProject_Add_Step(brotli_ep headers_copy
        COMMAND xcopy /E /I include ..\\..\\..\\brotli_ep\\src\\brotli_ep-install\\include /Y
        DEPENDEES build
        WORKING_DIRECTORY ${SOURCE_DIR})
    endif()
    set(BROTLI_VENDORED 1)
  else()
    find_package(Brotli REQUIRED)
    set(BROTLI_VENDORED 0)
  endif()

  include_directories(SYSTEM ${BROTLI_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(brotli_enc
    STATIC_LIB ${BROTLI_STATIC_LIBRARY_ENC})
  ADD_THIRDPARTY_LIB(brotli_dec
    STATIC_LIB ${BROTLI_STATIC_LIBRARY_DEC})
  ADD_THIRDPARTY_LIB(brotli_common
    STATIC_LIB ${BROTLI_STATIC_LIBRARY_COMMON})

  if (BROTLI_VENDORED)
    add_dependencies(brotli_enc brotli_ep)
    add_dependencies(brotli_dec brotli_ep)
    add_dependencies(brotli_common brotli_ep)
  endif()
endif()

if (ARROW_WITH_LZ4)
# ----------------------------------------------------------------------
# Lz4

  if("${LZ4_HOME}" STREQUAL "")
    set(LZ4_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-prefix/src/lz4_ep")
    set(LZ4_INCLUDE_DIR "${LZ4_BUILD_DIR}/lib")

    if (MSVC)
      set(LZ4_STATIC_LIB "${LZ4_BUILD_DIR}/visual/VS2010/bin/x64_${CMAKE_BUILD_TYPE}/liblz4_static.lib")
      set(LZ4_BUILD_COMMAND BUILD_COMMAND msbuild.exe /m /p:Configuration=${CMAKE_BUILD_TYPE} /p:Platform=x64 /p:PlatformToolset=v140 /t:Build ${LZ4_BUILD_DIR}/visual/VS2010/lz4.sln)
      set(LZ4_PATCH_COMMAND PATCH_COMMAND git --git-dir=. apply --verbose --whitespace=fix ${CMAKE_SOURCE_DIR}/build-support/lz4_msbuild_wholeprogramoptimization_param.patch)
    else()
      set(LZ4_STATIC_LIB "${LZ4_BUILD_DIR}/lib/liblz4.a")
      set(LZ4_BUILD_COMMAND BUILD_COMMAND ${CMAKE_SOURCE_DIR}/build-support/build-lz4-lib.sh)
    endif()

    ExternalProject_Add(lz4_ep
        URL "https://github.com/lz4/lz4/archive/v${LZ4_VERSION}.tar.gz"
        ${EP_LOG_OPTIONS}
        UPDATE_COMMAND ""
        ${LZ4_PATCH_COMMAND}
        CONFIGURE_COMMAND ""
        INSTALL_COMMAND ""
        BINARY_DIR ${LZ4_BUILD_DIR}
        BUILD_BYPRODUCTS ${LZ4_STATIC_LIB}
        ${LZ4_BUILD_COMMAND}
        )

    set(LZ4_VENDORED 1)
  else()
    find_package(Lz4 REQUIRED)
    set(LZ4_VENDORED 0)
  endif()

  include_directories(SYSTEM ${LZ4_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(lz4_static
    STATIC_LIB ${LZ4_STATIC_LIB})

  if (LZ4_VENDORED)
    add_dependencies(lz4_static lz4_ep)
  endif()
endif()

if (ARROW_WITH_ZSTD)
# ----------------------------------------------------------------------
# ZSTD

  if("${ZSTD_HOME}" STREQUAL "")
    set(ZSTD_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/zstd_ep-prefix/src/zstd_ep")
    set(ZSTD_INCLUDE_DIR "${ZSTD_BUILD_DIR}/lib")

    if (MSVC)
      set(ZSTD_STATIC_LIB "${ZSTD_BUILD_DIR}/build/VS2010/bin/x64_${CMAKE_BUILD_TYPE}/libzstd_static.lib")
      set(ZSTD_BUILD_COMMAND BUILD_COMMAND msbuild ${ZSTD_BUILD_DIR}/build/VS2010/zstd.sln /t:Build /v:minimal /p:Configuration=${CMAKE_BUILD_TYPE} /p:Platform=x64 /p:PlatformToolset=v140 /p:OutDir=${ZSTD_BUILD_DIR}/build/VS2010/bin/x64_${CMAKE_BUILD_TYPE}/ /p:SolutionDir=${ZSTD_BUILD_DIR}/build/VS2010/ )
      set(ZSTD_PATCH_COMMAND PATCH_COMMAND git --git-dir=. apply --verbose --whitespace=fix ${CMAKE_SOURCE_DIR}/build-support/zstd_msbuild_wholeprogramoptimization_param.patch)
    else()
      set(ZSTD_STATIC_LIB "${ZSTD_BUILD_DIR}/lib/libzstd.a")
      set(ZSTD_BUILD_COMMAND BUILD_COMMAND ${CMAKE_SOURCE_DIR}/build-support/build-zstd-lib.sh)
    endif()

    ExternalProject_Add(zstd_ep
        URL "https://github.com/facebook/zstd/archive/v${ZSTD_VERSION}.tar.gz"
        ${EP_LOG_OPTIONS}
        UPDATE_COMMAND ""
        ${ZSTD_PATCH_COMMAND}
        CONFIGURE_COMMAND ""
        INSTALL_COMMAND ""
        BINARY_DIR ${ZSTD_BUILD_DIR}
        BUILD_BYPRODUCTS ${ZSTD_STATIC_LIB}
        ${ZSTD_BUILD_COMMAND}
        )

    set(ZSTD_VENDORED 1)
  else()
    find_package(ZSTD REQUIRED)
    set(ZSTD_VENDORED 0)
  endif()

  include_directories(SYSTEM ${ZSTD_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(zstd_static
    STATIC_LIB ${ZSTD_STATIC_LIB})

  if (ZSTD_VENDORED)
    add_dependencies(zstd_static zstd_ep)
  endif()
endif()
