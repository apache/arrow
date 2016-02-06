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

# Headers: util
install(FILES
  bit-stream-utils.h
  bit-stream-utils.inline.h
  bit-util.h
  cpu-info.h
  sse-info.h
  compiler-util.h
  logging.h
  macros.h
  rle-encoding.h
  stopwatch.h
  input_stream.h
  DESTINATION include/parquet/util)

add_library(parquet_util STATIC
  input_stream.cc
  cpu-info.cc
)

if(PARQUET_BUILD_TESTS)
  add_library(parquet_test_main
	test_main.cc)
  if (APPLE)
	target_link_libraries(parquet_test_main
      gtest
      dl)
	set_target_properties(parquet_test_main
      PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
  else()
	target_link_libraries(parquet_test_main
      dl
      gtest
      pthread
	  )
  endif()
endif()

ADD_PARQUET_TEST(bit-util-test)
ADD_PARQUET_TEST(rle-test)
