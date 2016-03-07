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
# pyarrow_util
#######################################

set(UTIL_SRCS
)

set(UTIL_LIBS
)

add_library(pyarrow_util STATIC
  ${UTIL_SRCS}
)
target_link_libraries(pyarrow_util ${UTIL_LIBS})
SET_TARGET_PROPERTIES(pyarrow_util PROPERTIES LINKER_LANGUAGE CXX)

#######################################
# pyarrow_test_main
#######################################

add_library(pyarrow_test_main
  test_main.cc)

if (APPLE)
  target_link_libraries(pyarrow_test_main
    gmock
    dl)
  set_target_properties(pyarrow_test_main
        PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
else()
  target_link_libraries(pyarrow_test_main
    gtest
    pthread
    dl
  )
endif()
