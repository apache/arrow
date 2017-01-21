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

#######################################
# arrow_ipc
#######################################

set(ARROW_IPC_SHARED_LINK_LIBS
  arrow_io_shared
  arrow_shared
)

set(ARROW_IPC_STATIC_LINK_LIBS
  arrow_static
  arrow_io_static
)

if (ARROW_BUILD_STATIC)
  set(ARROW_IPC_TEST_LINK_LIBS
    arrow_io_static
    arrow_ipc_static)
else()
  set(ARROW_IPC_TEST_LINK_LIBS
    arrow_io_shared
    arrow_ipc_shared)
endif()

set(ARROW_IPC_SRCS
  adapter.cc
  file.cc
  json.cc
  json-internal.cc
  metadata.cc
  metadata-internal.cc
  stream.cc
)

if(NOT APPLE)
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(ARROW_IPC_LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")
endif()

ADD_ARROW_LIB(arrow_ipc
    SOURCES ${ARROW_IPC_SRCS}
    SHARED_LINK_FLAGS ${ARROW_IPC_LINK_FLAGS}
    SHARED_LINK_LIBS ${ARROW_IPC_SHARED_LINK_LIBS}
    STATIC_LINK_LIBS ${ARROW_IO_SHARED_PRIVATE_LINK_LIBS}
)

if(RAPIDJSON_VENDORED)
  add_dependencies(arrow_ipc_objlib rapidjson_ep)
endif()
if(FLATBUFFERS_VENDORED)
  add_dependencies(arrow_ipc_objlib flatbuffers_ep)
endif()

ADD_ARROW_TEST(ipc-adapter-test)
ARROW_TEST_LINK_LIBRARIES(ipc-adapter-test
  ${ARROW_IPC_TEST_LINK_LIBS})

ADD_ARROW_TEST(ipc-file-test)
ARROW_TEST_LINK_LIBRARIES(ipc-file-test
  ${ARROW_IPC_TEST_LINK_LIBS})

ADD_ARROW_TEST(ipc-metadata-test)
ARROW_TEST_LINK_LIBRARIES(ipc-metadata-test
  ${ARROW_IPC_TEST_LINK_LIBS})

ADD_ARROW_TEST(ipc-json-test)
ARROW_TEST_LINK_LIBRARIES(ipc-json-test
  ${ARROW_IPC_TEST_LINK_LIBS})

ADD_ARROW_TEST(json-integration-test)

if (ARROW_BUILD_TESTS)
  if (APPLE)
    target_link_libraries(json-integration-test
      arrow_ipc_static
      arrow_io_static
      arrow_static
      gflags
      gtest
      boost_filesystem_static
      boost_system_static
      dl)
    set_target_properties(json-integration-test
      PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
  else()
    target_link_libraries(json-integration-test
      arrow_ipc_static
      arrow_io_static
      arrow_static
      gflags
      gtest
      pthread
      boost_filesystem_static
      boost_system_static
      dl)
  endif()
endif()

# make clean will delete the generated file
set_source_files_properties(Metadata_generated.h PROPERTIES GENERATED TRUE)

set(OUTPUT_DIR ${CMAKE_SOURCE_DIR}/src/arrow/ipc)
set(FBS_OUTPUT_FILES "${OUTPUT_DIR}/Message_generated.h")

set(FBS_SRC
  ${CMAKE_SOURCE_DIR}/../format/Message.fbs
  ${CMAKE_SOURCE_DIR}/../format/File.fbs)

foreach(FIL ${FBS_SRC})
  get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
  list(APPEND ABS_FBS_SRC ${ABS_FIL})
endforeach()

if(FLATBUFFERS_VENDORED)
  set(FBS_DEPENDS ${ABS_FBS_SRC} flatbuffers_ep)
else()
  set(FBS_DEPENDS ${ABS_FBS_SRC})
endif()
add_custom_command(
  OUTPUT ${FBS_OUTPUT_FILES}
  COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${ABS_FBS_SRC}
  DEPENDS ${FBS_DEPENDS}
  COMMENT "Running flatc compiler on ${ABS_FBS_SRC}"
  VERBATIM
)

add_custom_target(metadata_fbs DEPENDS ${FBS_OUTPUT_FILES})
add_dependencies(arrow_ipc_objlib metadata_fbs)

# Headers: top level
install(FILES
  adapter.h
  file.h
  json.h
  metadata.h
  stream.h
  DESTINATION include/arrow/ipc)

# pkg-config support
configure_file(arrow-ipc.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/arrow-ipc.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/arrow-ipc.pc"
  DESTINATION "lib/pkgconfig/")
