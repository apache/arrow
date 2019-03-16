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

add_custom_target(plasma-all)
add_custom_target(plasma)
add_custom_target(plasma-benchmarks)
add_custom_target(plasma-tests)
add_dependencies(plasma-all plasma plasma-tests plasma-benchmarks)

# For the moment, Plasma is versioned like Arrow
project(plasma VERSION "${ARROW_BASE_VERSION}")
set(PLASMA_VERSION "${ARROW_VERSION}")

find_package(Threads)

# The SO version is also the ABI version
set(PLASMA_SO_VERSION "${ARROW_SO_VERSION}")
set(PLASMA_FULL_SO_VERSION "${ARROW_FULL_SO_VERSION}")

include_directories("${CMAKE_CURRENT_LIST_DIR}/" "${CMAKE_CURRENT_LIST_DIR}/thirdparty/"
                    "${CMAKE_CURRENT_LIST_DIR}/../")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-conversion")

# Compile flatbuffers

set(PLASMA_FBS_SRC "${CMAKE_CURRENT_LIST_DIR}/format/plasma.fbs"
    "${CMAKE_CURRENT_LIST_DIR}/format/common.fbs")
set(OUTPUT_DIR ${ARROW_BINARY_DIR}/src/plasma)

set(PLASMA_FBS_OUTPUT_FILES "${OUTPUT_DIR}/common_generated.h"
    "${OUTPUT_DIR}/plasma_generated.h")

add_custom_target(gen_plasma_fbs DEPENDS ${PLASMA_FBS_OUTPUT_FILES})

add_dependencies(gen_plasma_fbs flatbuffers::flatc)

add_custom_command(
  OUTPUT ${PLASMA_FBS_OUTPUT_FILES}
         # The --gen-object-api flag generates a C++ class MessageT for each
         # flatbuffers message Message, which can be used to store deserialized
         # messages in data structures. This is currently used for ObjectInfo for
         # example.
  COMMAND flatbuffers::flatc -c -o ${OUTPUT_DIR} ${PLASMA_FBS_SRC}
          --gen-object-api
          --scoped-enums
  DEPENDS ${PLASMA_FBS_SRC}
  COMMENT "Running flatc compiler on ${PLASMA_FBS_SRC}"
  VERBATIM)

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
    plasma_allocator.cc
    protocol.cc
    thirdparty/ae/ae.c)

set(PLASMA_LINK_LIBS arrow_shared)
set(PLASMA_STATIC_LINK_LIBS arrow_static)

if(ARROW_CUDA)
  set(PLASMA_LINK_LIBS ${PLASMA_LINK_LIBS} arrow_cuda_shared)
  set(PLASMA_STATIC_LINK_LIBS arrow_cuda_static ${PLASMA_STATIC_LINK_LIBS})
  add_definitions(-DPLASMA_CUDA)
endif()

add_arrow_lib(plasma
              SOURCES
              ${PLASMA_SRCS}
              OUTPUTS
              PLASMA_LIBRARIES
              DEPENDENCIES
              gen_plasma_fbs
              SHARED_LINK_LIBS
              ${PLASMA_LINK_LIBS}
              STATIC_LINK_LIBS
              ${PLASMA_STATIC_LINK_LIBS})

add_dependencies(plasma ${PLASMA_LIBRARIES})

foreach(LIB_TARGET ${PLASMA_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_EXPORTING)
endforeach()

# The optimization flag -O3 is suggested by dlmalloc.c, which is #included in
# malloc.cc; we set it here regardless of whether we do a debug or release build.
set_source_files_properties(malloc.cc PROPERTIES COMPILE_FLAGS "-O3")

if("${COMPILER_FAMILY}" STREQUAL "clang")
  set_property(SOURCE malloc.cc
               APPEND_STRING
               PROPERTY COMPILE_FLAGS " -Wno-parentheses-equality \
-Wno-null-pointer-arithmetic \
-Wno-shorten-64-to-32 \
-Wno-unused-macros")
endif()

if("${COMPILER_FAMILY}" STREQUAL "gcc")
  set_property(SOURCE malloc.cc APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-conversion")
endif()

list(APPEND PLASMA_EXTERNAL_STORE_SOURCES "external_store.cc" "hash_table_store.cc")

# We use static libraries for the plasma_store_server executable so that it can
# be copied around and used in different locations.
add_executable(plasma_store_server ${PLASMA_EXTERNAL_STORE_SOURCES} store.cc)
if(ARROW_BUILD_STATIC)
  target_link_libraries(plasma_store_server plasma_static ${PLASMA_STATIC_LINK_LIBS})
else()
  # Fallback to shared libs in the case that static libraries are not build.
  target_link_libraries(plasma_store_server plasma_shared ${PLASMA_LINK_LIBS})
endif()
add_dependencies(plasma plasma_store_server)

if(ARROW_RPATH_ORIGIN)
  if(APPLE)
    set(_lib_install_rpath "@loader_path")
  else()
    set(_lib_install_rpath "\$ORIGIN")
  endif()
  set_target_properties(plasma_store_server
                        PROPERTIES INSTALL_RPATH ${_lib_install_rpath})
elseif(APPLE)
  # With OSX and conda, we need to set the correct RPATH so that dependencies
  # are found. The installed libraries with conda have an RPATH that matches
  # for executables and libraries lying in $ENV{CONDA_PREFIX}/bin or
  # $ENV{CONDA_PREFIX}/lib but our test libraries and executables are not
  # installed there.
  if(NOT "$ENV{CONDA_PREFIX}" STREQUAL "" AND APPLE)
    set_target_properties(plasma_store_server
                          PROPERTIES BUILD_WITH_INSTALL_RPATH
                                     TRUE
                                     INSTALL_RPATH_USE_LINK_PATH
                                     TRUE
                                     INSTALL_RPATH
                                     "$ENV{CONDA_PREFIX}/lib")
  endif()
endif()

install(FILES common.h
              compat.h
              client.h
              events.h
              test-util.h
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/plasma")

# Plasma store
set_target_properties(plasma_store_server PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS plasma_store_server ${INSTALL_IS_OPTIONAL}
        DESTINATION ${CMAKE_INSTALL_BINDIR})

# pkg-config support
arrow_add_pkg_config("plasma")

if(ARROW_PLASMA_JAVA_CLIENT)
  # Plasma java client support
  find_package(JNI REQUIRED)
  # add jni support
  include_directories(${JAVA_INCLUDE_PATH})
  include_directories(${JAVA_INCLUDE_PATH2})
  if(JNI_FOUND)
    message(STATUS "JNI_INCLUDE_DIRS = ${JNI_INCLUDE_DIRS}")
    message(STATUS "JNI_LIBRARIES = ${JNI_LIBRARIES}")
  else()
    message(WARNING "Could not find JNI")
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

  file(GLOB PLASMA_LIBRARY_EXT_java_SRC lib/java/*.cc lib/*.cc)
  add_library(plasma_java SHARED ${PLASMA_LIBRARY_EXT_java_SRC})

  if(APPLE)
    target_link_libraries(plasma_java
                          plasma_shared
                          ${PLASMA_LINK_LIBS}
                          "-undefined dynamic_lookup"
                          -Wl,-force_load,flatbuffers::flatbuffers
                          flatbuffers::flatbuffers
                          ${PTHREAD_LIBRARY})
  else(APPLE)
    target_link_libraries(plasma_java
                          plasma_shared
                          ${PLASMA_LINK_LIBS}
                          -Wl,--whole-archive
                          flatbuffers::flatbuffers
                          -Wl,--no-whole-archive
                          flatbuffers::flatbuffers
                          ${PTHREAD_LIBRARY})
  endif(APPLE)
endif()
#
# Unit tests
#

# Adding unit tests part of the "arrow" portion of the test suite
function(ADD_PLASMA_TEST REL_TEST_NAME)
  set(options)
  set(one_value_args)
  set(multi_value_args)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  add_test_case(${REL_TEST_NAME}
                PREFIX
                "plasma"
                LABELS
                "plasma-tests"
                ${ARG_UNPARSED_ARGUMENTS})
endfunction()

if(ARROW_BUILD_SHARED)
  set(PLASMA_TEST_LIBS plasma_shared ${PLASMA_LINK_LIBS})
else()
  set(PLASMA_TEST_LIBS plasma_static ${PLASMA_STATIC_LINK_LIBS})
endif()

add_plasma_test(test/serialization_tests EXTRA_LINK_LIBS ${PLASMA_TEST_LIBS})
add_plasma_test(test/client_tests
                EXTRA_LINK_LIBS
                ${PLASMA_TEST_LIBS}
                EXTRA_DEPENDENCIES
                plasma_store_server)
add_plasma_test(test/external_store_tests
                EXTRA_LINK_LIBS
                ${PLASMA_TEST_LIBS}
                EXTRA_DEPENDENCIES
                plasma_store_server)
