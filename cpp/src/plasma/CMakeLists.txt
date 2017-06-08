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

cmake_minimum_required(VERSION 2.8)

project(plasma)

find_package(PythonLibsNew REQUIRED)
find_package(Threads)


if(APPLE)
  SET(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
endif(APPLE)

include_directories(SYSTEM ${PYTHON_INCLUDE_DIRS})
include_directories(thirdparty ..)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++11 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -march=native -mtune=native -O3")

# Compile flatbuffers

set(PLASMA_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/plasma.fbs" "${CMAKE_CURRENT_LIST_DIR}/format/common.fbs")
set(OUTPUT_DIR ${CMAKE_CURRENT_LIST_DIR}/format/)

set(PLASMA_FBS_OUTPUT_FILES
  "${OUTPUT_DIR}/common_generated.h"
  "${OUTPUT_DIR}/plasma_generated.h")

add_custom_command(
  OUTPUT ${PLASMA_FBS_OUTPUT_FILES}
  # The --gen-object-api flag generates a C++ class MessageT for each
  # flatbuffers message Message, which can be used to store deserialized
  # messages in data structures. This is currently used for ObjectInfo for
  # example.
  COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${PLASMA_FBS_SRC} --gen-object-api
  DEPENDS ${PLASMA_FBS_SRC}
  COMMENT "Running flatc compiler on ${PLASMA_FBS_SRC}"
  VERBATIM)

add_custom_target(gen_plasma_fbs DEPENDS ${PLASMA_FBS_OUTPUT_FILES})

add_dependencies(gen_plasma_fbs flatbuffers_ep)

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

include_directories("${CMAKE_CURRENT_LIST_DIR}/")
include_directories("${CMAKE_CURRENT_LIST_DIR}/../")

add_library(plasma SHARED
  plasma.cc
  extension.cc
  protocol.cc
  client.cc
  thirdparty/xxhash.cc
  fling.cc)

add_dependencies(plasma gen_plasma_fbs)

if(APPLE)
  target_link_libraries(plasma plasma_lib "-undefined dynamic_lookup" ${FLATBUFFERS_STATIC_LIB} ${FLATBUFFERS_STATIC_LIB} ${CMAKE_THREAD_LIBS_INIT})
else(APPLE)
  target_link_libraries(plasma plasma_lib -Wl,--whole-archive ${FLATBUFFERS_STATIC_LIB} -Wl,--no-whole-archive ${FLATBUFFERS_STATIC_LIB} ${CMAKE_THREAD_LIBS_INIT})
endif(APPLE)

include_directories("${FLATBUFFERS_INCLUDE_DIR}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

set_source_files_properties(thirdparty/dlmalloc.c PROPERTIES COMPILE_FLAGS -Wno-all)
set_source_files_properties(extension.cc PROPERTIES COMPILE_FLAGS -Wno-strict-aliasing)

add_library(plasma_lib STATIC
  client.cc
  plasma.cc
  common.cc
  io.cc
  protocol.cc
  ../arrow/status.cc
  fling.cc
  thirdparty/xxhash.cc)

target_link_libraries(plasma_lib ${FLATBUFFERS_STATIC_LIB} ${CMAKE_THREAD_LIBS_INIT})
add_dependencies(plasma_lib gen_plasma_fbs)

set_source_files_properties(malloc.cc PROPERTIES COMPILE_FLAGS "-Wno-error=conversion")

add_executable(plasma_store
  store.cc
  thirdparty/ae/ae.c
  plasma.cc
  events.cc
  protocol.cc
  eviction_policy.cc
  fling.cc
  malloc.cc)

add_dependencies(plasma_store gen_plasma_fbs)

target_link_libraries(plasma_store plasma_lib ${FLATBUFFERS_STATIC_LIB})

add_library(plasma_client SHARED client.cc)
target_link_libraries(plasma_client ${FLATBUFFERS_STATIC_LIB})

target_link_libraries(plasma_client plasma_lib ${FLATBUFFERS_STATIC_LIB})

# define_test(client_tests plasma_lib)
# define_test(manager_tests plasma_lib plasma_manager.cc)
# define_test(serialization_tests plasma_lib)
