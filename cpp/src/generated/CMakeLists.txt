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

externalproject_get_property(FlatBuffers BINARY_DIR)
set(FLATC_EXECUTABLE ${BINARY_DIR}/flatc)

set(ARROW_GENERATED_OUTPUTS)

set(ARROW_FORMAT_FILES_TO_GENERATE File;Message;Schema;SparseTensor;Tensor)
foreach(ARROW_FILE_TO_GENERATE ${ARROW_FORMAT_FILES_TO_GENERATE})
  set(OUTPUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/${ARROW_FILE_TO_GENERATE}_generated.h)
  add_custom_command(OUTPUT ${OUTPUT_FILE}
                     COMMAND ${FLATC_EXECUTABLE} --cpp
                             ${CMAKE_CURRENT_SOURCE_DIR}/../../../format/${ARROW_FILE_TO_GENERATE}.fbs
                     VERBATIM)

  list(APPEND ARROW_GENERATED_OUTPUTS ${OUTPUT_FILE})
endforeach()

set(OUTPUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/feather_generated.h)
add_custom_command(OUTPUT ${OUTPUT_FILE}
                   COMMAND ${FLATC_EXECUTABLE} --cpp
                           ${CMAKE_CURRENT_SOURCE_DIR}/../arrow/ipc/feather.fbs
                   VERBATIM)
list(APPEND ARROW_GENERATED_OUTPUTS ${OUTPUT_FILE})

add_custom_target(arrow_generated_files ALL DEPENDS ${ARROW_GENERATED_OUTPUTS}
                                                    FlatBuffers)

include_directories(${CMAKE_CURRENT_BINARY_DIR})
