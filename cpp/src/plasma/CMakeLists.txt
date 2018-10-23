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

# For the moment, Plasma is versioned like Arrow
project(plasma VERSION "${ARROW_BASE_VERSION}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/../python/cmake_modules")

find_package(PythonLibsNew REQUIRED)
find_package(Threads)

# The SO version is also the ABI version
set(PLASMA_SO_VERSION "${ARROW_SO_VERSION}")
set(PLASMA_FULL_SO_VERSION "${ARROW_FULL_SO_VERSION}")

include_directories(SYSTEM ${PYTHON_INCLUDE_DIRS})
include_directories("${FLATBUFFERS_INCLUDE_DIR}" "${CMAKE_CURRENT_LIST_DIR}/" "${CMAKE_CURRENT_LIST_DIR}/thirdparty/" "${CMAKE_CURRENT_LIST_DIR}/../")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-conversion")

# Compile flatbuffers

set(PLASMA_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/plasma.fbs" "${CMAKE_CURRENT_LIST_DIR}/format/common.fbs")
set(OUTPUT_DIR ${CMAKE_BINARY_DIR}/src/plasma)

set(PLASMA_FBS_OUTPUT_FILES
  "${OUTPUT_DIR}/common_generated.h"
  "${OUTPUT_DIR}/plasma_generated.h")

add_custom_target(gen_plasma_fbs DEPENDS ${PLASMA_FBS_OUTPUT_FILES})

if(FLATBUFFERS_VENDORED)
  add_dependencies(gen_plasma_fbs flatbuffers_ep)
endif()

add_custom_command(
  OUTPUT ${PLASMA_FBS_OUTPUT_FILES}
  # The --gen-object-api flag generates a C++ class MessageT for each
  # flatbuffers message Message, which can be used to store deserialized
  # messages in data structures. This is currently used for ObjectInfo for
  # example.
  COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${PLASMA_FBS_SRC} --gen-object-api --scoped-enums
  DEPENDS ${PLASMA_FBS_SRC}
  COMMENT "Running flatc compiler on ${PLASMA_FBS_SRC}"
  VERBATIM)

if(UNIX AND NOT APPLE)
  link_libraries(rt)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

set(PLASMA_SRCS
  client.cc
  common.cc
  eviction_policy.cc
  events.cc
  fling.cc
  io.cc
  malloc.cc
  plasma.cc
  protocol.cc
  thirdparty/ae/ae.c
  thirdparty/xxhash.cc)

set(PLASMA_LINK_LIBS arrow_shared)
set(PLASMA_STATIC_LINK_LIBS arrow_static)

if (ARROW_GPU)
  set(PLASMA_LINK_LIBS ${PLASMA_LINK_LIBS} arrow_gpu_shared)
  set(PLASMA_STATIC_LINK_LIBS ${PLASMA_STATIC_LINK_LIBS} arrow_gpu_static)
  add_definitions(-DPLASMA_GPU)
endif()

ADD_ARROW_LIB(plasma
  SOURCES ${PLASMA_SRCS}
  OUTPUTS PLASMA_LIBRARIES
  DEPENDENCIES gen_plasma_fbs
  SHARED_LINK_LIBS ${FLATBUFFERS_STATIC_LIB} ${CMAKE_THREAD_LIBS_INIT} ${PLASMA_LINK_LIBS}
  STATIC_LINK_LIBS ${FLATBUFFERS_STATIC_LIB} ${CMAKE_THREAD_LIBS_INIT} ${PLASMA_STATIC_LINK_LIBS})

foreach(LIB_TARGET ${PLASMA_LIBRARIES})
  target_compile_definitions(${LIB_TARGET}
    PRIVATE ARROW_EXPORTING)
endforeach()

# The optimization flag -O3 is suggested by dlmalloc.c, which is #included in
# malloc.cc; we set it here regardless of whether we do a debug or release build.
set_source_files_properties(malloc.cc PROPERTIES
  COMPILE_FLAGS "-O3")

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  set_property(SOURCE malloc.cc
    APPEND_STRING
    PROPERTY COMPILE_FLAGS
    " -Wno-parentheses-equality \
-Wno-null-pointer-arithmetic \
-Wno-shorten-64-to-32 \
-Wno-unused-macros")

  set_property(SOURCE thirdparty/xxhash.cc
    APPEND_STRING
    PROPERTY COMPILE_FLAGS
    "-Wno-unused-macros \
-Wno-unreachable-code")
endif()

if ("${COMPILER_FAMILY}" STREQUAL "gcc")
  set_property(SOURCE malloc.cc
    APPEND_STRING
    PROPERTY COMPILE_FLAGS
    " -Wno-conversion")
endif()

# We use static libraries for the plasma_store_server executable so that it can
# be copied around and used in different locations.
add_executable(plasma_store_server store.cc)
target_link_libraries(plasma_store_server plasma_static ${PLASMA_STATIC_LINK_LIBS})

if (ARROW_RPATH_ORIGIN)
  if (APPLE)
    set(_lib_install_rpath "@loader_path")
  else()
    set(_lib_install_rpath "\$ORIGIN")
  endif()
  set_target_properties(plasma_store_server PROPERTIES
      INSTALL_RPATH ${_lib_install_rpath})
endif()

# Headers: top level
install(FILES
  common.h
  compat.h
  client.h
  events.h
  test-util.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/plasma")

# Plasma store
set_target_properties(plasma_store_server PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS plasma_store_server DESTINATION ${CMAKE_INSTALL_BINDIR})

# pkg-config support
configure_file(plasma.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/plasma.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/plasma.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

if(ARROW_PLASMA_JAVA_CLIENT)
  # Plasma java client support
  find_package(JNI REQUIRED)
  # add jni support
  include_directories(${JAVA_INCLUDE_PATH})
  include_directories(${JAVA_INCLUDE_PATH2})
  if (JNI_FOUND)
    message (STATUS "JNI_INCLUDE_DIRS = ${JNI_INCLUDE_DIRS}")
    message (STATUS "JNI_LIBRARIES = ${JNI_LIBRARIES}")
  else()
    message (WARNING "Could not find JNI")
  endif()

  add_compile_options("-I$ENV{JAVA_HOME}/include/")
  if(WIN32)
    add_compile_options("-I$ENV{JAVA_HOME}/include/win32")
  elseif(APPLE)
    add_compile_options("-I$ENV{JAVA_HOME}/include/darwin")
  else() # linux
    add_compile_options("-I$ENV{JAVA_HOME}/include/linux")
  endif()

  include_directories("${CMAKE_CURRENT_LIST_DIR}/lib/java")

  file(GLOB PLASMA_LIBRARY_EXT_java_SRC
    lib/java/*.cc lib/*.cc)
  add_library(plasma_java SHARED
    ${PLASMA_LIBRARY_EXT_java_SRC})

  if(APPLE)
    target_link_libraries(plasma_java plasma_shared ${PLASMA_LINK_LIBS} "-undefined dynamic_lookup" -Wl,-force_load,${FLATBUFFERS_STATIC_LIB} ${FLATBUFFERS_STATIC_LIB} ${PTHREAD_LIBRARY})
  else(APPLE)
    target_link_libraries(plasma_java plasma_shared ${PLASMA_LINK_LIBS} -Wl,--whole-archive ${FLATBUFFERS_STATIC_LIB} -Wl,--no-whole-archive ${FLATBUFFERS_STATIC_LIB} ${PTHREAD_LIBRARY})
  endif(APPLE)
endif()
#######################################
# Unit tests
#######################################

ADD_ARROW_TEST(test/serialization_tests
  EXTRA_LINK_LIBS plasma_shared ${PLASMA_LINK_LIBS})
ADD_ARROW_TEST(test/client_tests
  EXTRA_LINK_LIBS plasma_shared ${PLASMA_LINK_LIBS}
  EXTRA_DEPENDENCIES plasma_store_server)
