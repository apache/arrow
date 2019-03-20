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

# Targets required for protocol integration testing
add_custom_target(integration)
add_dependencies(arrow-tests integration)

#
# Messaging and interprocess communication

add_arrow_test(feather-test)
add_arrow_test(read-write-test PREFIX "arrow-ipc")
add_arrow_test(json-simple-test PREFIX "arrow-ipc")
add_arrow_test(json-test PREFIX "arrow-ipc")

if(NOT ARROW_BOOST_HEADER_ONLY)
  add_arrow_test(json-integration-test EXTRA_LINK_LIBS ${GFLAGS_LIBRARIES})

  # Test is being built
  if(TARGET arrow-json-integration-test)
    add_dependencies(integration arrow-json-integration-test)
    if(UNIX)
      if(APPLE)
        set_target_properties(arrow-json-integration-test
                              PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
      else()
        target_link_libraries(arrow-json-integration-test PRIVATE pthread)
      endif()
    endif()
  endif()
endif()

# make clean will delete the generated file
set_source_files_properties(Message_generated.h PROPERTIES GENERATED TRUE)
set_source_files_properties(feather_generated.h PROPERTIES GENERATED TRUE)
set_source_files_properties(File_generated.h PROPERTIES GENERATED TRUE)

set(OUTPUT_DIR ${ARROW_BINARY_DIR}/src/arrow/ipc)
set(FBS_OUTPUT_FILES "${OUTPUT_DIR}/File_generated.h" "${OUTPUT_DIR}/Message_generated.h"
    "${OUTPUT_DIR}/feather_generated.h")

set(FBS_SRC
    ${ARROW_SOURCE_DIR}/../format/Message.fbs
    ${ARROW_SOURCE_DIR}/../format/File.fbs
    ${ARROW_SOURCE_DIR}/../format/Schema.fbs
    ${ARROW_SOURCE_DIR}/../format/Tensor.fbs
    ${ARROW_SOURCE_DIR}/../format/SparseTensor.fbs
    ${CMAKE_CURRENT_SOURCE_DIR}/feather.fbs)

foreach(FIL ${FBS_SRC})
  get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
  list(APPEND ABS_FBS_SRC ${ABS_FIL})
endforeach()

get_target_property(FLATC_EXECUTABLE flatbuffers::flatc IMPORTED_LOCATION)
if(NOT FLATC_EXECUTABLE)
  get_target_property(FLATC_EXECUTABLE flatbuffers::flatc IMPORTED_LOCATION_RELEASE)
endif()
message(STATUS "flatc: ${FLATC_EXECUTABLE}")
add_custom_command(OUTPUT ${FBS_OUTPUT_FILES}
                   COMMAND ${FLATC_EXECUTABLE} -c -o ${OUTPUT_DIR} ${ABS_FBS_SRC}
                   DEPENDS flatbuffers::flatc
                   COMMENT "Running flatc compiler on ${ABS_FBS_SRC}"
                   VERBATIM)

add_custom_target(metadata_fbs DEPENDS ${FBS_OUTPUT_FILES})

# Headers: top level
arrow_install_all_headers("arrow/ipc")

if(ARROW_BUILD_STATIC)
  set(ARROW_UTIL_LIB arrow_static)
else()
  set(ARROW_UTIL_LIB arrow_shared)
endif()

if(ARROW_BUILD_UTILITIES)
  add_executable(arrow-file-to-stream file-to-stream.cc)
  target_link_libraries(arrow-file-to-stream ${ARROW_UTIL_LIB})
  add_executable(arrow-stream-to-file stream-to-file.cc)
  target_link_libraries(arrow-stream-to-file ${ARROW_UTIL_LIB})

  add_dependencies(integration arrow-file-to-stream)
  add_dependencies(integration arrow-stream-to-file)
endif()

add_arrow_benchmark(read-write-benchmark PREFIX "arrow-ipc")
add_arrow_fuzzing(ipc-fuzzing-test)
