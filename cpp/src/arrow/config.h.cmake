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

#cmakedefine GRPCPP_PP_INCLUDE
#cmakedefine ARROW_WITH_TIMING_TESTS
#cmakedefine ARROW_WITH_BENCHMARKS_REFERENCE
#cmakedefine ARROW_LARGE_MEMORY_TESTS
#cmakedefine ARROW_VALGRIND
#cmakedefine ARROW_NO_DEPRECATED_API
#cmakedefine ARROW_EXTRA_ERROR_CONTEXT
#cmakedefine ARROW_USE_GLOG
#cmakedefine ARROW_JEMALLOC
#cmakedefine ARROW_MIMALLOC
#cmakedefine ARROW_WITH_BROTLI
#cmakedefine ARROW_WITH_BZ2
#cmakedefine ARROW_WITH_LZ4
#cmakedefine ARROW_WITH_SNAPPY
#cmakedefine ARROW_WITH_ZLIB
#cmakedefine ARROW_WITH_ZSTD
#cmakedefine ARROW_HDFS
#cmakedefine ARROW_S3
#cmakedefine ARROW_WITH_BROTLI
#cmakedefine ARROW_WITH_BZ2
#cmakedefine ARROW_WITH_LZ4
#cmakedefine ARROW_WITH_SNAPPY
#cmakedefine ARROW_WITH_ZLIB
#cmakedefine ARROW_WITH_ZSTD
