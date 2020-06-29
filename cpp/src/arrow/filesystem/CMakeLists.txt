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

# Headers: top level
arrow_install_all_headers("arrow/filesystem")

# pkg-config support
arrow_add_pkg_config("arrow-filesystem")

add_arrow_test(filesystem-test
               SOURCES
               filesystem_test.cc
               localfs_test.cc
               path_forest_test.cc
               EXTRA_LABELS
               filesystem)

if(ARROW_S3)
  add_arrow_test(s3fs_test EXTRA_LABELS filesystem)

  if(ARROW_BUILD_TESTS)
    add_executable(arrow-s3fs-narrative-test s3fs_narrative_test.cc)
    target_link_libraries(arrow-s3fs-narrative-test ${ARROW_TEST_LINK_LIBS}
                          ${GFLAGS_LIBRARIES} GTest::gtest)
    add_dependencies(arrow-tests arrow-s3fs-narrative-test)
  endif()

  if(ARROW_BUILD_BENCHMARKS AND ARROW_PARQUET)
    add_arrow_benchmark(s3fs_benchmark PREFIX "arrow-filesystem")
    if(ARROW_TEST_LINKAGE STREQUAL "static")
      target_link_libraries(arrow-filesystem-s3fs-benchmark PRIVATE parquet_static)
    else()
      target_link_libraries(arrow-filesystem-s3fs-benchmark PRIVATE parquet_shared)
    endif()
  endif()
endif()

if(ARROW_HDFS)
  add_arrow_test(hdfs_test EXTRA_LABELS filesystem)
endif()
