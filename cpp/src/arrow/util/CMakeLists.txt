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
# arrow_util
#######################################

# Headers: top level
install(FILES
  bit-util.h
  buffer.h
  macros.h
  memory-pool.h
  status.h
  DESTINATION include/arrow/util)

#######################################
# arrow_test_util
#######################################

add_library(arrow_test_util)
target_link_libraries(arrow_test_util
)

SET_TARGET_PROPERTIES(arrow_test_util PROPERTIES LINKER_LANGUAGE CXX)

#######################################
# arrow_test_main
#######################################

add_library(arrow_test_main
  test_main.cc)

if (APPLE)
  target_link_libraries(arrow_test_main
    gtest
	arrow_test_util
    dl)
  set_target_properties(arrow_test_main
        PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
else()
  target_link_libraries(arrow_test_main
    gtest
	arrow_test_util
    pthread
    dl
  )
endif()

ADD_ARROW_TEST(bit-util-test)
ADD_ARROW_TEST(buffer-test)
ADD_ARROW_TEST(memory-pool-test)
