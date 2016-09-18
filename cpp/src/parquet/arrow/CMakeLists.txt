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

# ----------------------------------------------------------------------
# parquet_arrow : Arrow <-> Parquet adapter

set(PARQUET_ARROW_SRCS
  io.cc
  reader.cc
  schema.cc
  writer.cc
)

add_library(parquet_arrow_objlib OBJECT
  ${PARQUET_ARROW_SRCS}
)

# SET_TARGET_PROPERTIES(parquet_arrow PROPERTIES LINKER_LANGUAGE CXX)

if (PARQUET_BUILD_SHARED)
    add_library(parquet_arrow_shared SHARED $<TARGET_OBJECTS:parquet_arrow_objlib>)
    set_target_properties(parquet_arrow_shared
      PROPERTIES
      LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      LINK_FLAGS "${SHARED_LINK_FLAGS}"
      OUTPUT_NAME "parquet_arrow")
    target_link_libraries(parquet_arrow_shared
      arrow
      arrow_io
      parquet_shared)
    if (APPLE)
      set_target_properties(parquet_arrow_shared
        PROPERTIES
        BUILD_WITH_INSTALL_RPATH ON
        INSTALL_NAME_DIR "@rpath")
    endif()
endif()

if (PARQUET_BUILD_STATIC)
    add_library(parquet_arrow_static STATIC $<TARGET_OBJECTS:parquet_arrow_objlib>)
    set_target_properties(parquet_arrow_static
      PROPERTIES
      LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      OUTPUT_NAME "parquet_arrow")
  target_link_libraries(parquet_arrow_static
      arrow_static
      arrow_static
      parquet_static)
  install(TARGETS parquet_arrow_static
      ARCHIVE DESTINATION lib
      LIBRARY DESTINATION lib)
endif()

ADD_PARQUET_TEST(arrow-schema-test)
ADD_PARQUET_TEST(arrow-io-test)
ADD_PARQUET_TEST(arrow-reader-writer-test)

if (PARQUET_BUILD_STATIC)
  ADD_PARQUET_LINK_LIBRARIES(arrow-schema-test parquet_arrow_static)
  ADD_PARQUET_LINK_LIBRARIES(arrow-io-test parquet_arrow_static)
  ADD_PARQUET_LINK_LIBRARIES(arrow-reader-writer-test parquet_arrow_static)
else()
  ADD_PARQUET_LINK_LIBRARIES(arrow-schema-test parquet_arrow_shared)
  ADD_PARQUET_LINK_LIBRARIES(arrow-io-test parquet_arrow_shared)
  ADD_PARQUET_LINK_LIBRARIES(arrow-reader-writer-test parquet_arrow_shared)
endif()

# Headers: top level
install(FILES
  io.h
  reader.h
  schema.h
  utils.h
  writer.h
  DESTINATION include/parquet/arrow)

