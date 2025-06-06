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

# TODO(GH-37221): Remove compute dependency for REE requirements on bridge_test
set(ARROW_TEST_LINK_LIBS "")

if(ARROW_TEST_LINKAGE STREQUAL "static")
  list(APPEND ARROW_TEST_LINK_LIBS ${ARROW_TEST_STATIC_LINK_LIBS})
else()
  list(APPEND ARROW_TEST_LINK_LIBS ${ARROW_TEST_SHARED_LINK_LIBS})
endif()

if(ARROW_COMPUTE)
  if(ARROW_TEST_LINKAGE STREQUAL "static")
    list(APPEND ARROW_TEST_LINK_LIBS arrow_compute_static arrow_compute_testing)
  else()
    list(APPEND ARROW_TEST_LINK_LIBS arrow_compute_shared arrow_compute_testing)
  endif()
endif()

add_arrow_test(bridge_test
               PREFIX
               "arrow-c"
               STATIC_LINK_LIBS
               ${ARROW_TEST_LINK_LIBS})
add_arrow_test(dlpack_test)

add_arrow_benchmark(bridge_benchmark)

arrow_install_all_headers("arrow/c")
