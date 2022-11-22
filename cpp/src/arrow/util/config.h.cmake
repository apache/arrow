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

#define ARROW_VERSION_MAJOR @ARROW_VERSION_MAJOR@
#define ARROW_VERSION_MINOR @ARROW_VERSION_MINOR@
#define ARROW_VERSION_PATCH @ARROW_VERSION_PATCH@
#define ARROW_VERSION ((ARROW_VERSION_MAJOR * 1000) + ARROW_VERSION_MINOR) * 1000 + ARROW_VERSION_PATCH

#define ARROW_VERSION_STRING "@ARROW_VERSION@"

#define ARROW_SO_VERSION "@ARROW_SO_VERSION@"
#define ARROW_FULL_SO_VERSION "@ARROW_FULL_SO_VERSION@"

#define ARROW_CXX_COMPILER_ID "@CMAKE_CXX_COMPILER_ID@"
#define ARROW_CXX_COMPILER_VERSION "@CMAKE_CXX_COMPILER_VERSION@"
#define ARROW_CXX_COMPILER_FLAGS "@CMAKE_CXX_FLAGS@"

#define ARROW_BUILD_TYPE "@UPPERCASE_BUILD_TYPE@"

#define ARROW_GIT_ID "@ARROW_GIT_ID@"
#define ARROW_GIT_DESCRIPTION "@ARROW_GIT_DESCRIPTION@"

#define ARROW_PACKAGE_KIND "@ARROW_PACKAGE_KIND@"

#cmakedefine ARROW_COMPUTE
#cmakedefine ARROW_CSV
#cmakedefine ARROW_CUDA
#cmakedefine ARROW_DATASET
#cmakedefine ARROW_FILESYSTEM
#cmakedefine ARROW_FLIGHT
#cmakedefine ARROW_FLIGHT_SQL
#cmakedefine ARROW_IPC
#cmakedefine ARROW_JEMALLOC
#cmakedefine ARROW_JEMALLOC_VENDORED
#cmakedefine ARROW_JSON
#cmakedefine ARROW_ORC
#cmakedefine ARROW_PARQUET
#cmakedefine ARROW_SUBSTRAIT
#cmakedefine ARROW_WITH_QPL

#cmakedefine ARROW_GCS
#cmakedefine ARROW_S3
#cmakedefine ARROW_USE_NATIVE_INT128
#cmakedefine ARROW_WITH_MUSL
#cmakedefine ARROW_WITH_OPENTELEMETRY
#cmakedefine ARROW_WITH_UCX

#cmakedefine GRPCPP_PP_INCLUDE
