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

set(ARROW_IPC_LINK_LIBS
  arrow_io
  arrow_shared
)

set(ARROW_IPC_PRIVATE_LINK_LIBS
  )

set(ARROW_IPC_TEST_LINK_LIBS
  arrow_ipc
  ${ARROW_IPC_PRIVATE_LINK_LIBS})

set(ARROW_IPC_SRCS
  adapter.cc
  metadata.cc
  metadata-internal.cc
)

# TODO(wesm): SHARED and STATIC targets
add_library(arrow_ipc SHARED
  ${ARROW_IPC_SRCS}
)
target_link_libraries(arrow_ipc
  LINK_PUBLIC ${ARROW_IPC_LINK_LIBS}
  LINK_PRIVATE ${ARROW_IPC_PRIVATE_LINK_LIBS})

if(NOT APPLE)
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(ARROW_IPC_LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")
endif()

SET_TARGET_PROPERTIES(arrow_ipc PROPERTIES
  LINKER_LANGUAGE CXX
  LINK_FLAGS "${ARROW_IPC_LINK_FLAGS}")

ADD_ARROW_TEST(ipc-adapter-test)
ARROW_TEST_LINK_LIBRARIES(ipc-adapter-test
  ${ARROW_IPC_TEST_LINK_LIBS})

ADD_ARROW_TEST(ipc-metadata-test)
ARROW_TEST_LINK_LIBRARIES(ipc-metadata-test
  ${ARROW_IPC_TEST_LINK_LIBS})

# make clean will delete the generated file
set_source_files_properties(Metadata_generated.h PROPERTIES GENERATED TRUE)

set(OUTPUT_DIR ${CMAKE_SOURCE_DIR}/src/arrow/ipc)
set(FBS_OUTPUT_FILES "${OUTPUT_DIR}/Message_generated.h")

set(FBS_SRC ${CMAKE_SOURCE_DIR}/../format/Message.fbs)
get_filename_component(ABS_FBS_SRC ${FBS_SRC} ABSOLUTE)

add_custom_command(
  OUTPUT ${FBS_OUTPUT_FILES}
  COMMAND ${FLATBUFFERS_COMPILER} -c -o ${OUTPUT_DIR} ${ABS_FBS_SRC}
  DEPENDS ${ABS_FBS_SRC}
  COMMENT "Running flatc compiler on ${FBS_SRC}"
  VERBATIM
)

add_custom_target(metadata_fbs DEPENDS ${FBS_OUTPUT_FILES})
add_dependencies(arrow_objlib metadata_fbs)

# Headers: top level
install(FILES
  adapter.h
  metadata.h
  DESTINATION include/arrow/ipc)

install(TARGETS arrow_ipc
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib)
