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
# arrow_parquet : Arrow <-> Parquet adapter

set(PARQUET_SRCS
  reader.cc
  schema.cc
)

set(PARQUET_LIBS
  arrow
  ${PARQUET_SHARED_LIB}
)

add_library(arrow_parquet SHARED
  ${PARQUET_SRCS}
)
target_link_libraries(arrow_parquet ${PARQUET_LIBS})
SET_TARGET_PROPERTIES(arrow_parquet PROPERTIES LINKER_LANGUAGE CXX)

ADD_ARROW_TEST(parquet-schema-test)
ARROW_TEST_LINK_LIBRARIES(parquet-schema-test arrow_parquet)

ADD_ARROW_TEST(parquet-reader-test)
ARROW_TEST_LINK_LIBRARIES(parquet-reader-test arrow_parquet)

# Headers: top level
install(FILES
  reader.h
  schema.h
  utils.h
  DESTINATION include/arrow/parquet)

install(TARGETS arrow_parquet
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib)
