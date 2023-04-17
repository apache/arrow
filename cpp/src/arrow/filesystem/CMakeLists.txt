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
  add_arrow_test(s3fs_test
                 SOURCES
                 s3fs_test.cc
                 s3_test_util.cc
                 EXTRA_LABELS
                 filesystem
                 EXTRA_LINK_LIBS
                 Boost::filesystem
                 Boost::system)
  if(TARGET arrow-s3fs-test)
    set(ARROW_S3FS_TEST_COMPILE_DEFINITIONS)
    get_target_property(AWS_CPP_SDK_S3_TYPE aws-cpp-sdk-s3 TYPE)
    # We need to initialize AWS C++ SDK for direct use (not via
    # arrow::fs::S3FileSystem) in arrow-s3fs-test if we use static AWS
    # C++ SDK. Because AWS C++ SDK has internal static variables that
    # aren't shared in libarrow and arrow-s3fs-test. It means that
    # arrow::fs::InitializeS3() doesn't initialize AWS C++ SDK that is
    # directly used in arrow-s3fs-test.
    #
    # But it seems that internal static variables in AWS C++ SDK are
    # shared on macOS even if we link static AWS C++ SDK to both
    # libarrow and arrow-s3fs-test. So we don't need to initialize AWS
    # C++ SDK in arrow-s3fs-test on macOS.
    if(AWS_CPP_SDK_S3_TYPE STREQUAL "STATIC_LIBRARY" AND NOT APPLE)
      list(APPEND ARROW_S3FS_TEST_COMPILE_DEFINITIONS "AWS_CPP_SDK_S3_NOT_SHARED")
    endif()
    target_compile_definitions(arrow-s3fs-test
                               PRIVATE ${ARROW_S3FS_TEST_COMPILE_DEFINITIONS})
  endif()

  if(ARROW_BUILD_TESTS)
    add_executable(arrow-s3fs-narrative-test s3fs_narrative_test.cc)
    target_link_libraries(arrow-s3fs-narrative-test ${ARROW_TEST_LINK_LIBS}
                          ${GFLAGS_LIBRARIES} ${ARROW_GTEST_GTEST})
    add_dependencies(arrow-tests arrow-s3fs-narrative-test)
  endif()

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
