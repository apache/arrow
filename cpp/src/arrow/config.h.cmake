// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Generated from cpp/src/arrow/config.h.cmake

#pragma once

#define ARROW_VERSION_MAJOR @ARROW_VERSION_MAJOR@
#define ARROW_VERSION_MINOR @ARROW_VERSION_MINOR@
#define ARROW_VERSION_PATCH @ARROW_VERSION_PATCH@
#define ARROW_VERSION \
  ((ARROW_VERSION_MAJOR * 1000) + ARROW_VERSION_MINOR) * 1000 + ARROW_VERSION_PATCH

#define ARROW_SO_VERSION "@ARROW_SO_VERSION@"
#define ARROW_FULL_SO_VERSION "@ARROW_FULL_SO_VERSION@"

//////////////////////////////////////////////////////////////////////////////
// Compile and link
#cmakedefine ARROW_NO_DEPRECATED_API
#cmakedefine ARROW_USE_SIMD
#cmakedefine ARROW_SSE42
#cmakedefine ARROW_ALTIVEC

// Test and benchmark
#cmakedefine ARROW_BUILD_EXAMPLES
#cmakedefine ARROW_BUILD_TESTS
#cmakedefine ARROW_WITH_TIMING_TESTS
#cmakedefine ARROW_BUILD_INTEGRATION
#cmakedefine ARROW_BUILD_BENCHMARKS
#cmakedefine ARROW_WITH_BENCHMARKS_REFERENCE
#cmakedefine ARROW_FUZZING
#cmakedefine ARROW_LARGE_MEMORY_TESTS

// Lint
#cmakedefine ARROW_ONLY_LINT
#cmakedefine ARROW_GENERATE_COVERAGE

// Checks
#cmakedefine ARROW_TEST_MEMCHECK
#cmakedefine ARROw_USE_ASAN
#cmakedefine ARROw_USE_TSAN
#cmakedefine ARROw_USE_UBSAN

// Project component
#cmakedefine ARROW_BUILD_UTILITIES
#cmakedefine ARROW_COMPUTE
#cmakedefine ARROW_CSV
#cmakedefine ARROW_CUDA
#cmakedefine ARROW_DATASET
#cmakedefine ARROW_FILESYSTEM
#cmakedefine ARROW_FLIGHT
#cmakedefine ARROW_HDFS
#cmakedefine ARROW_HIVESERVER2
#cmakedefine ARROW_IPC
#cmakedefine ARROW_JEMALLOC
#cmakedefine ARROW_JNI
#cmakedefine ARROW_JSON
#cmakedefine ARROW_MIMALLOC
#cmakedefine ARROW_PARQUET
#cmakedefine ARROW_ORC
#cmakedefine ARROW_PLASMA
#cmakedefine ARROW_PLASMA_JAVA_CLIENT
#cmakedefine ARROW_PYTHON
#cmakedefine ARROW_S3
#cmakedefine ARROW_TENSORFLOW

// Thirdparty toolchain
#cmakedefine ARROW_WITH_BACKTRACE
#cmakedefine ARROW_USE_GLOG
#cmakedefine ARROW_WITH_BROTLI
#cmakedefine ARROW_WITH_BZ2
#cmakedefine ARROW_WITH_LZ4
#cmakedefine ARROW_WITH_SNAPPY
#cmakedefine ARROW_WITH_ZLIB
#cmakedefine ARROW_WITH_ZSTD

// Parquet

// Gandiva
#cmakedefine ARROW_GANDIVA_JAVA

// Advanced developer
#cmakedefine ARROW_EXTRA_ERROR_CONTEXT

//////////////////////////////////////////////////////////////////////////////
#ifdef ARROW_JEMALLOC
#define ARROW_JEMALLOC_INCLUDE_DIR "@JEMALLOC_INCLUDE_DIR@"
#endif  // ARROW_JEMALLOC

#ifdef ARROW_TEST_MEMCHECK
#define ARROW_VALGRIND
#endif // ARROW_TEST_MEMCHECK

// Disable DLL exports in vendored uriparser library
#define URI_STATIC_BUILD

#cmakedefine GRPCPP_PP_INCLUDE
