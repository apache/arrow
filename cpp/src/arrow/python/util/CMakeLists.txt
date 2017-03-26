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
# arrow/python_test_main
#######################################

if (PYARROW_BUILD_TESTS)
  add_library(arrow/python_test_main STATIC
	test_main.cc)

  if (APPLE)
	target_link_libraries(arrow/python_test_main
      gtest
      dl)
	set_target_properties(arrow/python_test_main
      PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
  else()
	target_link_libraries(arrow/python_test_main
      gtest
      pthread
      dl
	  )
  endif()
endif()
