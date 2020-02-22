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

#define ARROW_VERSION_MAJOR @ARROW_VERSION_MAJOR @
#define ARROW_VERSION_MINOR @ARROW_VERSION_MINOR @
#define ARROW_VERSION_PATCH @ARROW_VERSION_PATCH @
#define ARROW_VERSION \
  ((ARROW_VERSION_MAJOR * 1000) + ARROW_VERSION_MINOR) * 1000 + ARROW_VERSION_PATCH

#define ARROW_SO_VERSION "100"
#define ARROW_FULL_SO_VERSION "100.0.0"

#define ARROW_WITH_TIMING_TESTS
/* #undef ARROW_WITH_BENCHMARKS_REFERENCE */
/* #undef ARROW_LARGE_MEMORY_TESTS */
/* #undef ARROW_VALGRIND */
/* #undef ARROW_NO_DEPRECATED_API */
/* #undef ARROW_EXTRA_ERROR_CONTEXT */
/* #undef ARROW_USE_GLOG */
#define ARROW_JEMALLOC
/* #undef ARROW_MIMALLOC */
/* #undef ARROW_WITH_BROTLI */
/* #undef ARROW_WITH_BZ2 */
/* #undef ARROW_WITH_LZ4 */
/* #undef ARROW_WITH_SNAPPY */
/* #undef ARROW_WITH_ZLIB */
/* #undef ARROW_WITH_ZSTD */
/* #undef ARROW_HDFS */
/* #undef ARROW_S3 */
/* #undef ARROW_WITH_BROTLI */
/* #undef ARROW_WITH_BZ2 */
/* #undef ARROW_WITH_LZ4 */
/* #undef ARROW_WITH_SNAPPY */
/* #undef ARROW_WITH_ZLIB */
/* #undef ARROW_WITH_ZSTD */

#ifdef ARROW_JEMALLOC
#define ARROW_JEMALLOC_INCLUDE_DIR ""
#endif  // ARROW_JEMALLOC

// Disable DLL exports in vendored uriparser library
#define URI_STATIC_BUILD

/* #undef GRPCPP_PP_INCLUDE */
