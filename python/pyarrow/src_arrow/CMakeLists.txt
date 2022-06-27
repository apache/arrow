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
# arrow_python
#

cmake_minimum_required(VERSION 3.5)

# RPATH settings on macOS do not affect install_name.
# https://cmake.org/cmake/help/latest/policy/CMP0068.html
if(POLICY CMP0068)
  cmake_policy(SET CMP0068 NEW)
endif()

# Define
# ARROW_SOURCE_DIR: location of arrow/cpp
# CMAKE_MODULE_PATH: location of cmake_modules in python
get_filename_component(PYARROW_SOURCE_DIR ${CMAKE_SOURCE_DIR} DIRECTORY)
get_filename_component(PYTHON_SOURCE_DIR ${PYARROW_SOURCE_DIR} DIRECTORY)
get_filename_component(ARROW_SOURCE ${PYTHON_SOURCE_DIR} DIRECTORY)
set(ARROW_SOURCE_DIR "${ARROW_SOURCE}/cpp")
# Add cmake_modules and lib/cmake/arrow folder to CMAKE_MODULE_PATH
# TODO: remove lib/cmake/arrow folder as it should not be necessary as find_package(Arrow)
# should load all the cmake variables (ex ARROW_DATASET)
set(CMAKE_MODULE_PATH "${PYTHON_SOURCE_DIR}/cmake_modules" "${ARROW_SOURCE}/dist/lib/cmake/arrow")
message(STATUS "CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}")

set(ARROW_PYTHON_VERSION "9.0.0-SNAPSHOT")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" ARROW_PYTHON_BASE_VERSION "${ARROW_PYTHON_VERSION}")
# Need to set to ARRROW_VERSION before finding Arrow package!
project(arrow_python VERSION "${ARROW_PYTHON_BASE_VERSION}")

if(NOT DEFINED CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Arrow REQUIRED)
# Loading ArrowOptions, they should be loaded by default with find_package(Arrow REQUIRED)
include(ArrowOptions)
find_package(Python3Alt 3.7 REQUIRED)

# Needed due to
# CMake Error: INSTALL(EXPORT) given unknown export "arrow_python_targets"
option(ARROW_BUILD_SHARED "Link to the Arrow shared library" ON)

add_custom_target(arrow_python-all)
add_custom_target(arrow_python)
add_custom_target(arrow_python-tests)
add_dependencies(arrow_python-all arrow_python arrow_python-tests)

set(ARROW_PYTHON_SRCS
    arrow_to_pandas.cc
    benchmark.cc
    common.cc
    datetime.cc
    decimal.cc
    deserialize.cc
    extension_type.cc
    gdb.cc
    helpers.cc
    inference.cc
    init.cc
    io.cc
    ipc.cc
    numpy_convert.cc
    numpy_to_arrow.cc
    python_to_arrow.cc
    pyarrow.cc
    serialize.cc
    udf.cc)

set_source_files_properties(init.cc PROPERTIES SKIP_PRECOMPILE_HEADERS ON
                                               SKIP_UNITY_BUILD_INCLUSION ON)

#
# Arrow vs C PyArrow options
#

# Check all the options from Arrow and C PyArrow to be in line
if(PYARROW_WITH_DATASET)
  find_package(ArrowDataset REQUIRED)
endif()

if(ARROW_CSV)
  list(APPEND ARROW_PYTHON_SRCS csv.cc)
endif()

if(ARROW_FILESYSTEM)
  list(APPEND ARROW_PYTHON_SRCS filesystem.cc)
endif()

if(PYARROW_WITH_PARQUET_ENCRYPTION)
  if(PARQUET_REQUIRE_ENCRYPTION)
    list(APPEND ARROW_PYTHON_SRCS parquet_encryption.cc)
  else()
    message(FATAL_ERROR "You must build Arrow C++ with PARQUET_REQUIRE_ENCRYPTION=ON")
  endif()
endif()

if(PYARROW_WITH_HDFS)
  if(NOT ARROW_HDFS)
    message(FATAL_ERROR "You must build Arrow C++ with ARROW_HDFS=ON")
  endif()
endif()

# Link to arrow dependecies
if(ARROW_BUILD_SHARED)
  set(ARROW_PYTHON_DEPENDENCIES arrow_shared)
else()
  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
  set(ARROW_PYTHON_DEPENDENCIES arrow_static Threads::Threads)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set_property(SOURCE pyarrow.cc
               APPEND_STRING
               PROPERTY COMPILE_FLAGS " -Wno-cast-qual ")
endif()

#
# Compiler stuff
#

include(GNUInstallDirs)

# This ensures that things like gnu++11 get passed correctly
if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 11)
endif()

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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
                    "-Wl,--version-script=${ARROW_SOURCE_DIR}/src/arrow/symbols.map"
                    CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
endif()

# Need to set ARROW_VERSION_SCRIPT_FLAGS for add_arrow_lib
if(CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
  set(ARROW_VERSION_SCRIPT_FLAGS
      "-Wl,--version-script=${ARROW_SOURCE_DIR}/src/arrow/symbols.map")
endif()

#
# shred/static link libs
#

set(ARROW_PYTHON_SHARED_LINK_LIBS arrow_shared)
set(ARROW_PYTHON_SHARED_PRIVATE_LINK_LIBS)
set(ARROW_PYTHON_STATIC_LINK_LIBS ${PYTHON_OTHER_LIBS})

if(WIN32)
  list(APPEND ARROW_PYTHON_SHARED_LINK_LIBS ${PYTHON_LIBRARIES} ${PYTHON_OTHER_LIBS})
endif()
if(PARQUET_REQUIRE_ENCRYPTION)
  list(APPEND ARROW_PYTHON_SHARED_LINK_LIBS parquet_shared)
endif()
if(ARROW_USE_XSIMD)
  list(APPEND ARROW_PYTHON_SHARED_PRIVATE_LINK_LIBS xsimd)
  list(APPEND ARROW_PYTHON_STATIC_LINK_LIBS xsimd)
endif()

set(ARROW_PYTHON_INCLUDES ${NUMPY_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS})

# Inlude macros needed to find and use add_arrow_lib function
include(BuildUtils)
include(CMakePackageConfigHelpers)

# Set the output directory for cmake module
# (CMAKE_INSTALL_PREFIX = python/build/dist! should be set in setup.py!)
set(ARROW_CMAKE_INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")

add_arrow_lib(arrow_python
              CMAKE_PACKAGE_NAME
              ArrowPython
              PKG_CONFIG_NAME
              arrow-python
              SOURCES
              ${ARROW_PYTHON_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:pch.h>"
              OUTPUTS
              ARROW_PYTHON_LIBRARIES
              DEPENDENCIES
              ${ARROW_PYTHON_DEPENDENCIES}
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in line 95
              SHARED_LINK_LIBS
              ${ARROW_PYTHON_SHARED_LINK_LIBS}
              SHARED_PRIVATE_LINK_LIBS
              ${ARROW_PYTHON_SHARED_PRIVATE_LINK_LIBS}
              STATIC_LINK_LIBS
              ${ARROW_PYTHON_STATIC_LINK_LIBS}
              EXTRA_INCLUDES
              "${ARROW_PYTHON_INCLUDES}")

add_dependencies(arrow_python ${ARROW_PYTHON_LIBRARIES})

foreach(LIB_TARGET ${ARROW_PYTHON_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_PYTHON_EXPORTING)
endforeach()

if(ARROW_BUILD_STATIC AND MSVC)
  target_compile_definitions(arrow_python_static PUBLIC ARROW_STATIC)
endif()

if(ARROW_FLIGHT AND ARROW_BUILD_SHARED)

  find_package(ArrowFlight REQUIRED)
  find_package(gRPC CONFIG REQUIRED)
  include_directories("${ARROW_SOURCE_DIR}/src" "${ARROW_SOURCE_DIR}/build/src")

  # Using arrow_flight_shared, see example
  # https://github.com/apache/arrow/blob/7a0f00c16e084d194ae53d209b33b809cfc8f2d5/cpp/examples/arrow/CMakeLists.txt
  set(ARROW_GRPC_USE_SHARED ON)
  set(GRPC_REFLECTION_LINK_LIBS -Wl,--no-as-needed gRPC::grpc++_reflection
                                  -Wl,--as-needed)

  set(FLIGHT_PROTO_PATH "${ARROW_SOURCE}/format")
  set(FLIGHT_PROTO "${ARROW_SOURCE}/format/Flight.proto")

  set(FLIGHT_GENERATED_PROTO_FILES
      "${ARROW_SOURCE_DIR}/build/src/arrow/flight/Flight.pb.cc"
      "${ARROW_SOURCE_DIR}/build/src/arrow/flight/Flight.pb.h"
      "${ARROW_SOURCE_DIR}/build/src/arrow/flight/Flight.grpc.pb.cc"
      "${ARROW_SOURCE_DIR}/build/src/arrow/flight/Flight.grpc.pb.h")

  set(PROTO_DEPENDS ${FLIGHT_PROTO} gRPC::grpc_cpp_plugin)

  add_custom_command(OUTPUT ${FLIGHT_GENERATED_PROTO_FILES}
                    COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${FLIGHT_PROTO_PATH}"
                            "--cpp_out=${${ARROW_SOURCE_DIR}/build/src/arrow/flight/}" "${FLIGHT_PROTO}"
                    DEPENDS ${PROTO_DEPENDS} ARGS
                    COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${FLIGHT_PROTO_PATH}"
                            "--grpc_out=${${ARROW_SOURCE_DIR}/build/src/arrow/flight/}"
                            "--plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
                            "${FLIGHT_PROTO}")

  set_source_files_properties(${FLIGHT_GENERATED_PROTO_FILES} PROPERTIES GENERATED TRUE)
  add_custom_target(flight_grpc_gen ALL DEPENDS ${FLIGHT_GENERATED_PROTO_FILES})

  add_arrow_lib(arrow_python_flight
                CMAKE_PACKAGE_NAME
                ArrowPythonFlight
                PKG_CONFIG_NAME
                arrow-python-flight
                SOURCES
                flight.cc
                OUTPUTS
                ARROW_PYFLIGHT_LIBRARIES
                DEPENDENCIES
                flight_grpc_gen
                SHARED_LINK_FLAGS
                ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in line 95
                SHARED_LINK_LIBS
                arrow_python_shared
                arrow_flight_shared
                STATIC_LINK_LIBS
                ${PYTHON_OTHER_LIBS}
                EXTRA_INCLUDES
                "${ARROW_PYTHON_INCLUDES}"
                PRIVATE_INCLUDES
                "${Protobuf_INCLUDE_DIRS}")

  add_dependencies(arrow_python ${ARROW_PYFLIGHT_LIBRARIES})

  foreach(LIB_TARGET ${ARROW_PYFLIGHT_LIBRARIES})
    target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_PYFLIGHT_EXPORTING)
  endforeach()

  if(ARROW_BUILD_STATIC AND MSVC)
    target_compile_definitions(arrow_python_flight_static PUBLIC ARROW_STATIC)
  endif()
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  # Clang, be quiet. Python C API has lots of macros
  set_property(SOURCE ${ARROW_PYTHON_SRCS}
               APPEND_STRING
               PROPERTY COMPILE_FLAGS -Wno-parentheses-equality)
endif()

arrow_install_all_headers("arrow/python")

# ----------------------------------------------------------------------

set(ARROW_BUILD_TESTS off)
if(ARROW_BUILD_TESTS)
  add_library(arrow_python_test_main STATIC util/test_main.cc)

  target_link_libraries(arrow_python_test_main GTest::gtest)
  target_include_directories(arrow_python_test_main SYSTEM
                             PUBLIC ${ARROW_PYTHON_INCLUDES})

  if(APPLE)
    target_link_libraries(arrow_python_test_main ${CMAKE_DL_LIBS})
    set_target_properties(arrow_python_test_main PROPERTIES LINK_FLAGS
                                                            "-undefined dynamic_lookup")
  elseif(NOT MSVC)
    target_link_libraries(arrow_python_test_main pthread ${CMAKE_DL_LIBS})
  endif()

  if(ARROW_TEST_LINKAGE STREQUAL shared)
    set(ARROW_PYTHON_TEST_LINK_LIBS arrow_python_test_main arrow_python_shared
                                    arrow_testing_shared arrow_shared)
  else()
    set(ARROW_PYTHON_TEST_LINK_LIBS arrow_python_test_main arrow_python_static
                                    arrow_testing_static arrow_static)
  endif()

  add_arrow_test(python_test
                 STATIC_LINK_LIBS
                 "${ARROW_PYTHON_TEST_LINK_LIBS}"
                 EXTRA_LINK_LIBS
                 ${PYTHON_LIBRARIES}
                 EXTRA_INCLUDES
                 "${ARROW_PYTHON_INCLUDES}"
                 LABELS
                 "arrow_python-tests"
                 NO_VALGRIND)
endif()
