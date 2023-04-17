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

#include "arrow/api.h"          // IWYU pragma: keep
#include "arrow/compute/api.h"  // IWYU pragma: keep
#include "arrow/io/api.h"       // IWYU pragma: keep
#include "arrow/ipc/api.h"      // IWYU pragma: keep

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

#ifdef ARROW_FLIGHT_SQL
#include "arrow/flight/sql/api.h"  // IWYU pragma: keep
#endif

#ifdef ARROW_JSON
#include "arrow/json/api.h"  // IWYU pragma: keep
#endif

#ifdef ARROW_SUBSTRAIT
#include "arrow/engine/api.h"            // IWYU pragma: keep
#include "arrow/engine/substrait/api.h"  // IWYU pragma: keep
#endif

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

namespace arrow {

TEST(InternalHeaders, DCheckExposed) {
#ifdef DCHECK
  FAIL() << "DCHECK should not be visible from Arrow public headers.";
#endif
}

TEST(InternalHeaders, AssignOrRaiseExposed) {
#ifdef ASSIGN_OR_RAISE
  FAIL() << "ASSIGN_OR_RAISE should not be visible from Arrow public headers.";
#endif
}

TEST(InternalDependencies, OpenTelemetryExposed) {
#ifdef OPENTELEMETRY_VERSION
  FAIL() << "OpenTelemetry should not be visible from Arrow public headers.";
#endif
}

TEST(InternalDependencies, XSimdExposed) {
#ifdef XSIMD_VERSION_MAJOR
  FAIL() << "xsimd should not be visible from Arrow public headers.";
#endif
}

TEST(InternalDependencies, DateLibraryExposed) {
#ifdef HAS_CHRONO_ROUNDING
  FAIL() << "arrow::vendored::date should not be visible from Arrow public headers.";
#endif
}

TEST(InternalDependencies, ProtobufExposed) {
#ifdef PROTOBUF_EXPORT
  FAIL() << "Protocol Buffers should not be visible from Arrow public headers.";
#endif
}

TEST(TransitiveDependencies, WindowsHeadersExposed) {
#if defined(SendMessage) || defined(GetObject) || defined(ERROR_INVALID_HANDLE) || \
    defined(FILE_SHARE_READ) || defined(WAIT_TIMEOUT)
  FAIL() << "Windows.h should not be included by Arrow public headers";
#endif
}

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

TEST(Misc, SetTimezoneConfig) {
#ifndef _WIN32
  GTEST_SKIP() << "Can only set the Timezone database on Windows";
#elif !defined(ARROW_FILESYSTEM)
  GTEST_SKIP() << "Need filesystem support to test timezone config.";
#else
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

  std::optional<std::string> tzdata_result = GetTestTimezoneDatabaseRoot();
  std::string tzdata_dir;
  if (tzdata_result.has_value()) {
    tzdata_dir = tzdata_result.value();
  } else {
    auto home_raw = std::getenv("USERPROFILE");
    std::string home = home_raw == nullptr ? "~" : std::string(home_raw);
    ASSERT_OK_AND_ASSIGN(tzdata_dir, fs->NormalizePath(home + "\\Downloads\\tzdata"));
  }
  ASSERT_OK_AND_ASSIGN(tzdata_dir, fs->NormalizePath(tzdata_dir));
  ASSERT_OK_AND_ASSIGN(auto tzdata_path,
                       arrow::internal::PlatformFilename::FromString(tzdata_dir));

  if (!arrow::internal::FileExists(tzdata_path).ValueOr(false)) {
    GTEST_SKIP() << "Couldn't find timezone database in expected dir: " << tzdata_dir;
  }
  // Create a tmp directory
  ASSERT_OK_AND_ASSIGN(auto tempdir, arrow::internal::TemporaryDir::Make("tzdata"));

  // Validate that setting tzdb to that dir fails
  arrow::GlobalOptions options = {std::make_optional(tempdir->path().ToString())};
  ASSERT_NOT_OK(arrow::Initialize(options));

  // Copy tzdb data from ~/Downloads
  auto selector = arrow::fs::FileSelector();
  selector.base_dir = tzdata_dir;
  selector.recursive = true;
  ASSERT_OK(arrow::fs::CopyFiles(fs, selector, fs, tempdir->path().ToString()));

  // Validate that tzdb is working
  ASSERT_OK(arrow::Initialize(options));
#endif
}

}  // namespace arrow
