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
  logging.h
  macros.h
  random.h
  visibility.h
  DESTINATION include/arrow/util)

#######################################
# arrow_test_main
#######################################

if (ARROW_BUILD_TESTS)
  add_library(arrow_test_main
	test_main.cc)

  if (APPLE)
	  target_link_libraries(arrow_test_main
        gtest
        dl)
	  set_target_properties(arrow_test_main
        PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
  elseif(MSVC)
	  target_link_libraries(arrow_test_main
        gtest)
  else()
	  target_link_libraries(arrow_test_main
        gtest
        pthread
        dl
	    )
  endif()
endif()

if (ARROW_BUILD_BENCHMARKS)
  add_library(arrow_benchmark_main benchmark_main.cc)
  if (APPLE)
    target_link_libraries(arrow_benchmark_main
      benchmark
    )
  else()
	  target_link_libraries(arrow_benchmark_main
      benchmark
      pthread
	  )
  endif()
endif()

if (ARROW_IPC AND ARROW_BUILD_UTILITIES)
  set(UTIL_LINK_LIBS
    arrow_ipc_static
    arrow_io_static
    arrow_static
    boost_filesystem_static
    boost_system_static
    dl)

  if (NOT APPLE)
    set(UTIL_LINK_LIBS
      ${UTIL_LINK_LIBS}
      boost_filesystem_static
      boost_system_static)
  endif()

  add_executable(file-to-stream file-to-stream.cc)
  target_link_libraries(file-to-stream ${UTIL_LINK_LIBS})
  add_executable(stream-to-file stream-to-file.cc)
  target_link_libraries(stream-to-file ${UTIL_LINK_LIBS})
endif()

ADD_ARROW_TEST(bit-util-test)
