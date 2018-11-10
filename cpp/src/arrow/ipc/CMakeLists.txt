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
# Messaging and interprocess communication

ADD_ARROW_TEST(feather-test)
ADD_ARROW_TEST(ipc-read-write-test)
ADD_ARROW_TEST(ipc-json-test)

if (NOT ARROW_BOOST_HEADER_ONLY)
  ADD_ARROW_TEST(json-integration-test
    EXTRA_LINK_LIBS gflags_static)

  # Test is being built
  if (TARGET json-integration-test)
    if (UNIX)
      if (APPLE)
        set_target_properties(json-integration-test
          PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
      else()
        target_link_libraries(json-integration-test
          pthread)
      endif()
    endif()
  endif()
endif()

# make clean will delete the generated file
set_source_files_properties(Message_generated.h PROPERTIES GENERATED TRUE)
set_source_files_properties(feather_generated.h PROPERTIES GENERATED TRUE)
set_source_files_properties(File_generated.h PROPERTIES GENERATED TRUE)

set(OUTPUT_DIR ${CMAKE_BINARY_DIR}/src/arrow/ipc)
set(FBS_OUTPUT_FILES
  "${OUTPUT_DIR}/File_generated.h"
  "${OUTPUT_DIR}/Message_generated.h"
  "${OUTPUT_DIR}/feather_generated.h")

set(FBS_SRC
  ${CMAKE_SOURCE_DIR}/../format/Message.fbs
  ${CMAKE_SOURCE_DIR}/../format/File.fbs
  ${CMAKE_SOURCE_DIR}/../format/Schema.fbs
  ${CMAKE_SOURCE_DIR}/../format/Tensor.fbs
  ${CMAKE_CURRENT_SOURCE_DIR}/feather.fbs)

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

# Headers: top level
install(FILES
  api.h
  dictionary.h
  feather.h
  json.h
  message.h
  reader.h
  writer.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/ipc")

if (ARROW_BUILD_STATIC)
  set(ARROW_UTIL_LIB arrow_static)
else()
  set(ARROW_UTIL_LIB arrow_shared)
endif()

set(UTIL_LINK_LIBS
  ${ARROW_UTIL_LIB}
  ${BOOST_FILESYSTEM_LIBRARY}
  ${BOOST_SYSTEM_LIBRARY})

if(NOT WIN32)
  set(UTIL_LINK_LIBS
    ${UTIL_LINK_LIBS}
    pthread
    ${CMAKE_DL_LIBS})
endif()

if (ARROW_BUILD_UTILITIES)
  add_executable(file-to-stream file-to-stream.cc)
  target_link_libraries(file-to-stream ${UTIL_LINK_LIBS})
  add_executable(stream-to-file stream-to-file.cc)
  target_link_libraries(stream-to-file ${UTIL_LINK_LIBS})
endif()

ADD_ARROW_BENCHMARK(ipc-read-write-benchmark)

ADD_ARROW_FUZZING(ipc-fuzzing-test)
