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
               EXTRA_LABELS
               filesystem)

if(ARROW_BUILD_BENCHMARKS)
  add_arrow_benchmark(localfs_benchmark
                      PREFIX
                      "arrow-filesystem"
                      SOURCES
                      localfs_benchmark.cc
                      STATIC_LINK_LIBS
                      ${ARROW_BENCHMARK_LINK_LIBS})
endif()

if(ARROW_GCS)
  add_arrow_test(gcsfs_test
                 EXTRA_LABELS
                 filesystem
                 EXTRA_LINK_LIBS
                 Boost::filesystem
                 Boost::system)
endif()

if(ARROW_S3)
  set(ARROW_S3_BUILD_TESTS ${ARROW_BUILD_TESTS})
  get_target_property(AWS_CPP_SDK_S3_TYPE aws-cpp-sdk-s3 TYPE)
  # We disable S3 filesystem tests with bundled (static linked) AWS
  # C++ SDK because it's difficult to initialize underlying S3 library
  # properly.
  if(AWS_CPP_SDK_S3_TYPE STREQUAL "STATIC_LIBRARY")
    set(ARROW_S3_BUILD_TESTS OFF)
  endif()
else()
  set(ARROW_S3_BUILD_TESTS OFF)
endif()

if(ARROW_S3_BUILD_TESTS)
  add_arrow_test(s3fs_test
                 SOURCES
                 s3fs_test.cc
                 s3_test_util.cc
                 EXTRA_LABELS
                 filesystem
                 EXTRA_LINK_LIBS
                 Boost::filesystem
                 Boost::system)
  add_executable(arrow-s3fs-narrative-test s3fs_narrative_test.cc)
  target_link_libraries(arrow-s3fs-narrative-test ${ARROW_TEST_LINK_LIBS}
                        ${GFLAGS_LIBRARIES} GTest::gtest)
  add_dependencies(arrow-tests arrow-s3fs-narrative-test)

  if(ARROW_BUILD_BENCHMARKS AND ARROW_PARQUET)
    add_arrow_benchmark(s3fs_benchmark
                        PREFIX
                        "arrow-filesystem"
                        SOURCES
                        s3fs_benchmark.cc
                        s3_test_util.cc
                        STATIC_LINK_LIBS
                        ${ARROW_BENCHMARK_LINK_LIBS}
                        Boost::filesystem
                        Boost::system)
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
