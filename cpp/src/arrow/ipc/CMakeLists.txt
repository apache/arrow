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

# Headers: top level
install(FILES
  adapter.h
  metadata.h
  memory.h
  DESTINATION include/arrow/ipc)

ADD_ARROW_TEST(ipc-adapter-test)
ADD_ARROW_TEST(ipc-memory-test)
ADD_ARROW_TEST(ipc-metadata-test)

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
