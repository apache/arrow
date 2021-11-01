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

#include <sstream>
#include <string>

#include "arrow/config.h"

// Include various "api.h" entrypoints and check they don't leak internal symbols

#include "arrow/api.h"      // IWYU pragma: keep
#include "arrow/io/api.h"   // IWYU pragma: keep
#include "arrow/ipc/api.h"  // IWYU pragma: keep

#ifdef ARROW_COMPUTE
#include "arrow/compute/api.h"  // IWYU pragma: keep
#endif

#ifdef ARROW_CSV
#include "arrow/csv/api.h"  // IWYU pragma: keep
#endif

#ifdef ARROW_DATASET
#include "arrow/dataset/api.h"  // IWYU pragma: keep
#endif

#ifdef ARROW_FILESYSTEM
#include "arrow/filesystem/api.h"  // IWYU pragma: keep
#endif

#ifdef ARROW_FLIGHT
#include "arrow/flight/api.h"  // IWYU pragma: keep
#endif

#ifdef ARROW_JSON
#include "arrow/json/api.h"  // IWYU pragma: keep
#endif

#ifdef ARROW_PYTHON
#include "arrow/python/api.h"  // IWYU pragma: keep
#endif

#ifdef DCHECK
#error "DCHECK should not be visible from Arrow public headers."
#endif

#ifdef ASSIGN_OR_RAISE
#error "ASSIGN_OR_RAISE should not be visible from Arrow public headers."
#endif

#ifdef XSIMD_VERSION_MAJOR
#error "xsimd should not be visible from Arrow public headers."
#endif

#ifdef HAS_CHRONO_ROUNDING
#error "arrow::vendored::date should not be visible from Arrow public headers."
#endif

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace arrow {

TEST(Misc, BuildInfo) {
  const auto& info = GetBuildInfo();
  // The runtime version (GetBuildInfo) should have the same major number as the
  // build-time version (ARROW_VERSION), but may have a greater minor / patch number.
  ASSERT_EQ(info.version_major, ARROW_VERSION_MAJOR);
  ASSERT_GE(info.version_minor, ARROW_VERSION_MINOR);
  ASSERT_GE(info.version_patch, ARROW_VERSION_PATCH);
  ASSERT_GE(info.version, ARROW_VERSION);
  ASSERT_LT(info.version, ARROW_VERSION + 1000 * 1000);  // Same major version
  std::stringstream ss;
  ss << info.version_major << "." << info.version_minor << "." << info.version_patch;
  ASSERT_THAT(info.version_string, ::testing::HasSubstr(ss.str()));
  ASSERT_THAT(info.full_so_version, ::testing::HasSubstr(info.so_version));
}

}  // namespace arrow
