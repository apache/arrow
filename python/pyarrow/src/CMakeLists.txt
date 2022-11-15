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

#
# Define
# CMAKE_MODULE_PATH: location of cmake_modules in python
#

get_filename_component(PYARROW_SOURCE_DIR ${CMAKE_SOURCE_DIR} DIRECTORY)
get_filename_component(PYTHON_SOURCE_DIR ${PYARROW_SOURCE_DIR} DIRECTORY)
get_filename_component(ARROW_SOURCE_DIR ${PYTHON_SOURCE_DIR} DIRECTORY)
set(ARROW_CPP_SOURCE_DIR "${ARROW_SOURCE_DIR}/cpp")

set(CMAKE_MODULE_PATH "${PYTHON_SOURCE_DIR}/cmake_modules")

# normalize ARROW_HOME path
file(TO_CMAKE_PATH "$ENV{ARROW_HOME}" ARROW_HOME)
list(INSERT CMAKE_PREFIX_PATH 0 "${ARROW_HOME}")

#
# Arrow version
#

set(ARROW_PYTHON_VERSION "10.0.1")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" ARROW_PYTHON_BASE_VERSION "${ARROW_PYTHON_VERSION}")
# Need to set to ARRROW_VERSION before finding Arrow package!
project(arrow_python VERSION "${ARROW_PYTHON_BASE_VERSION}")

if(NOT DEFINED CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

#
# Arrow
#

find_package(Arrow REQUIRED)

#
# Python
#
# Use the first Python installation on PATH, not the newest one
set(Python3_FIND_STRATEGY "LOCATION")
# On Windows, use registry last, not first
set(Python3_FIND_REGISTRY "LAST")
# On macOS, use framework last, not first
set(Python3_FIND_FRAMEWORK "LAST")

find_package(Python3Alt 3.7 REQUIRED)
include_directories(SYSTEM ${NUMPY_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS} ${ARROW_INCLUDE_DIR} ${CMAKE_SOURCE_DIR} src)

add_custom_target(arrow_python)

set(ARROW_PYTHON_SRCS
    arrow/python/arrow_to_pandas.cc
    arrow/python/benchmark.cc
    arrow/python/common.cc
    arrow/python/datetime.cc
    arrow/python/decimal.cc
    arrow/python/deserialize.cc
    arrow/python/extension_type.cc
    arrow/python/gdb.cc
    arrow/python/helpers.cc
    arrow/python/inference.cc
    arrow/python/init.cc
    arrow/python/io.cc
    arrow/python/ipc.cc
    arrow/python/numpy_convert.cc
    arrow/python/numpy_to_arrow.cc
    arrow/python/python_test.cc
    arrow/python/python_to_arrow.cc
    arrow/python/pyarrow.cc
    arrow/python/serialize.cc
    arrow/python/udf.cc)

set_source_files_properties(init.cc PROPERTIES SKIP_PRECOMPILE_HEADERS ON
                                               SKIP_UNITY_BUILD_INCLUSION ON)

#
# Shared/static link libs
#

set(ARROW_PYTHON_SHARED_LINK_LIBS)
set(ARROW_PYTHON_SHARED_PRIVATE_LINK_LIBS)
set(ARROW_PYTHON_SHARED_INSTALL_INTERFACE_LIBS)
set(ARROW_PYTHON_STATIC_LINK_LIBS)
set(ARROW_PYTHON_STATIC_INSTALL_INTERFACE_LIBS)

#
# Arrow vs PyArrow C++ options
#

# Check all the options from Arrow and PyArrow C++ to be in line
if(PYARROW_WITH_DATASET)
  find_package(ArrowDataset REQUIRED)
  list(APPEND ARROW_PYTHON_SHARED_LINK_LIBS ArrowDataset::arrow_dataset_shared)
  list(APPEND ARROW_PYTHON_SHARED_INSTALL_INTERFACE_LIBS ArrowDataset::arrow_dataset_shared)
  list(APPEND ARROW_PYTHON_STATIC_LINK_LIBS ArrowDataset::arrow_dataset_static)
  list(APPEND ARROW_PYTHON_STATIC_INSTALL_INTERFACE_LIBS ArrowDataset::arrow_dataset_static)
endif()

if(PYARROW_WITH_PARQUET_ENCRYPTION)
  if(PARQUET_REQUIRE_ENCRYPTION)
    list(APPEND ARROW_PYTHON_SRCS arrow/python/parquet_encryption.cc)
    find_package(Parquet REQUIRED)
    list(APPEND ARROW_PYTHON_SHARED_LINK_LIBS Parquet::parquet_shared)
    list(APPEND ARROW_PYTHON_SHARED_INSTALL_INTERFACE_LIBS Parquet::parquet_shared)
    list(APPEND ARROW_PYTHON_STATIC_LINK_LIBS Parquet::parquet_static)
    list(APPEND ARROW_PYTHON_STATIC_INSTALL_INTERFACE_LIBS Parquet::parquet_static)
  else()
    message(FATAL_ERROR "You must build Arrow C++ with PARQUET_REQUIRE_ENCRYPTION=ON")
  endif()
endif()

if(PYARROW_WITH_HDFS)
  if(NOT ARROW_HDFS)
    message(FATAL_ERROR "You must build Arrow C++ with ARROW_HDFS=ON")
  endif()
endif()

# Check for only Arrow C++ options
if(ARROW_CSV)
  list(APPEND ARROW_PYTHON_SRCS arrow/python/csv.cc)
endif()

if(ARROW_FILESYSTEM)
  list(APPEND ARROW_PYTHON_SRCS arrow/python/filesystem.cc)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set_property(SOURCE pyarrow.cc
               APPEND_STRING
               PROPERTY COMPILE_FLAGS " -Wno-cast-qual ")
endif()

if(NOT ARROW_PYTHON_SHARED_LINK_LIBS)
  list(APPEND ARROW_PYTHON_SHARED_LINK_LIBS Arrow::arrow_shared)
endif()
if(NOT ARROW_PYTHON_SHARED_INSTALL_INTERFACE_LIBS)
  list(APPEND ARROW_PYTHON_SHARED_INSTALL_INTERFACE_LIBS Arrow::arrow_shared)
endif()
if(NOT ARROW_PYTHON_STATIC_LINK_LIBS)
  list(APPEND ARROW_PYTHON_STATIC_LINK_LIBS Arrow::arrow_static)
endif()
list(APPEND ARROW_PYTHON_STATIC_LINK_LIBS ${PYTHON_OTHER_LIBS})
if(NOT ARROW_PYTHON_STATIC_INSTALL_INTERFACE_LIBS)
  list(APPEND ARROW_PYTHON_STATIC_INSTALL_INTERFACE_LIBS Arrow::arrow_static)
endif()

if(WIN32)
  list(APPEND ARROW_PYTHON_SHARED_LINK_LIBS ${PYTHON_LIBRARIES} ${PYTHON_OTHER_LIBS})
endif()

#
# Compiler stuff
#

include(GNUInstallDirs)

# This ensures that things like -std=gnu++... get passed correctly
if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()

# We require a C++17 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Needed compiler flags
include(SetupCxxFlags)

# Add common flags
set(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${PYARROW_CXXFLAGS}")

message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

set(ARROW_PYTHON_INCLUDES ${NUMPY_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS})

# Inlude macros needed to find and use add_arrow_lib function
include(BuildUtils)
include(CMakePackageConfigHelpers)

# Set the output directory for CMake files
# (CMAKE_INSTALL_PREFIX = python/build/dist! should be set in setup.py!)
set(ARROW_CMAKE_DIR "${CMAKE_INSTALL_LIBDIR}/cmake")

# Changing ARROW_SOURCE_DIR for sdist build
# In this case cpp/cmake_modules doesn't exist
if(NOT EXISTS "${ARROW_SOURCE_DIR}/cpp/cmake_modules")
  set(ARROW_SOURCE_DIR ${PYTHON_SOURCE_DIR})
endif()

add_arrow_lib(arrow_python
              CMAKE_PACKAGE_NAME
              ArrowPython
              PKG_CONFIG_NAME
              arrow-python
              SOURCES
              ${ARROW_PYTHON_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:arrow/python/pch.h>"
              OUTPUTS
              ARROW_PYTHON_LIBRARIES
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS}
              SHARED_LINK_LIBS
              ${ARROW_PYTHON_SHARED_LINK_LIBS}
              SHARED_PRIVATE_LINK_LIBS
              ${ARROW_PYTHON_SHARED_PRIVATE_LINK_LIBS}
              SHARED_INSTALL_INTERFACE_LIBS
              ${ARROW_PYTHON_SHARED_INSTALL_INTERFACE_LIBS}
              STATIC_LINK_LIBS
              ${ARROW_PYTHON_STATIC_LINK_LIBS}
              STATIC_INSTALL_INTERFACE_LIBS
              ${ARROW_PYTHON_STATIC_INSTALL_INTERFACE_LIBS}
              EXTRA_INCLUDES
              "${ARROW_PYTHON_INCLUDES}")

add_dependencies(arrow_python ${ARROW_PYTHON_LIBRARIES})

foreach(LIB_TARGET ${ARROW_PYTHON_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_PYTHON_EXPORTING)
endforeach()

if(ARROW_BUILD_STATIC AND MSVC)
  target_compile_definitions(arrow_python_static PUBLIC ARROW_STATIC)
endif()

if(PYARROW_WITH_FLIGHT AND ARROW_FLIGHT AND ARROW_BUILD_SHARED)
   # Must link to shared libarrow_flight: we don't want to link more than one
   # copy of gRPC into the eventual Cython shared object, otherwise gRPC calls
   # fail with weird errors due to multiple copies of global static state (The
   # other solution is to link gRPC shared everywhere instead of statically only
   # in Flight)
  find_package(ArrowFlight REQUIRED)

  set(FLIGHT_LINK_LIBS ArrowFlight::arrow_flight_shared)

  add_arrow_lib(arrow_python_flight
                CMAKE_PACKAGE_NAME
                ArrowPythonFlight
                PKG_CONFIG_NAME
                arrow-python-flight
                SOURCES
                arrow/python/flight.cc
                OUTPUTS
                ARROW_PYFLIGHT_LIBRARIES
                SHARED_LINK_FLAGS
                ${ARROW_VERSION_SCRIPT_FLAGS}
                SHARED_LINK_LIBS
                arrow_python_shared
                ArrowFlight::arrow_flight_shared
                SHARED_INSTALL_INTERFACE_LIBS
                ArrowPython::arrow_python_shared
                ArrowFlight::arrow_flight_shared
                STATIC_LINK_LIBS
                arrow_python_static
                ArrowFlight::arrow_flight_static
                STATIC_INSTALL_INTERFACE_LIBS
                ArrowPython::arrow_python_static
                ArrowFlight::arrow_flight_static
                EXTRA_INCLUDES
                "${ARROW_PYTHON_INCLUDES}"
                PRIVATE_INCLUDES
                "${Protobuf_INCLUDE_DIRS}")

  add_dependencies(arrow_python ${ARROW_PYFLIGHT_LIBRARIES})

  foreach(LIB_TARGET ${ARROW_PYFLIGHT_LIBRARIES})
    target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_PYFLIGHT_EXPORTING)
  endforeach()

  if(ARROW_BUILD_STATIC AND MSVC)
    target_compile_definitions(arrow_python_flight_static PUBLIC ARROW_PYTHON_STATIC)
  endif()
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  # Clang, be quiet. Python C API has lots of macros
  set_property(SOURCE ${ARROW_PYTHON_SRCS}
               APPEND_STRING
               PROPERTY COMPILE_FLAGS -Wno-parentheses-equality)
endif()

add_subdirectory(arrow/python)
