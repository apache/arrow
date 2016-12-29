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

set(THRIFT_SRCS
  parquet_constants.cpp
  parquet_types.cpp)

set_source_files_properties(parquet_types.cpp PROPERTIES
  COMPILE_FLAGS -Wno-unused-variable)

add_library(parquet_thrift STATIC
  ${THRIFT_SRCS}
)

set_target_properties(parquet_thrift
  PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set_source_files_properties(${THRIFT_SRCS} PROPERTIES GENERATED TRUE)

# List of thrift output targets
set(OUTPUT_DIR ${CMAKE_SOURCE_DIR}/src/parquet/thrift)
set(THRIFT_OUTPUT_FILES "${OUTPUT_DIR}/parquet_types.cpp")
set(THRIFT_OUTPUT_FILES ${THRIFT_OUTPUT_FILES} "${OUTPUT_DIR}/parquet_types.h")
set(THRIFT_OUTPUT_FILES ${THRIFT_OUTPUT_FILES} "${OUTPUT_DIR}/parquet_constants.cpp")
set(THRIFT_OUTPUT_FILES ${THRIFT_OUTPUT_FILES} "${OUTPUT_DIR}/parquet_constants.h")

get_filename_component(ABS_PARQUET_THRIFT parquet.thrift ABSOLUTE)

add_custom_command(
  OUTPUT ${THRIFT_OUTPUT_FILES}
  COMMAND ${THRIFT_COMPILER} --gen cpp -out ${OUTPUT_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/parquet.thrift
  DEPENDS ${ABS_PARQUET_THRIFT} thriftstatic
  COMMENT "Running thrift compiler on parquet.thrift"
  VERBATIM
)
