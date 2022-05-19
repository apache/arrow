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

#include "arrow/config.h"

#include <cstdint>

#include "arrow/util/config.h"
#include "arrow/util/cpu_info.h"
#include "arrow/vendored/datetime.h"

namespace arrow {

using internal::CpuInfo;

namespace {

const BuildInfo kBuildInfo = {
    // clang-format off
    ARROW_VERSION,
    ARROW_VERSION_MAJOR,
    ARROW_VERSION_MINOR,
    ARROW_VERSION_PATCH,
    ARROW_VERSION_STRING,
    ARROW_SO_VERSION,
    ARROW_FULL_SO_VERSION,
    ARROW_CXX_COMPILER_ID,
    ARROW_CXX_COMPILER_VERSION,
    ARROW_CXX_COMPILER_FLAGS,
    ARROW_GIT_ID,
    ARROW_GIT_DESCRIPTION,
    ARROW_PACKAGE_KIND,
    ARROW_BUILD_TYPE,
    // clang-format on
};

template <typename QueryFlagFunction>
std::string MakeSimdLevelString(QueryFlagFunction&& query_flag) {
  if (query_flag(CpuInfo::AVX512)) {
    return "avx512";
  } else if (query_flag(CpuInfo::AVX2)) {
    return "avx2";
  } else if (query_flag(CpuInfo::AVX)) {
    return "avx";
  } else if (query_flag(CpuInfo::SSE4_2)) {
    return "sse4_2";
  } else {
    return "none";
  }
}

util::optional<std::string> timezone_db_path;

};  // namespace

const BuildInfo& GetBuildInfo() { return kBuildInfo; }

RuntimeInfo GetRuntimeInfo() {
  RuntimeInfo info;
  auto cpu_info = CpuInfo::GetInstance();
  info.simd_level =
      MakeSimdLevelString([&](int64_t flags) { return cpu_info->IsSupported(flags); });
  info.detected_simd_level =
      MakeSimdLevelString([&](int64_t flags) { return cpu_info->IsDetected(flags); });
  info.using_os_timezone_db = USE_OS_TZDB;
#if !USE_OS_TZDB
  info.timezone_db_path = timezone_db_path;
#else
  info.timezone_db_path = util::optional<std::string>();
#endif
  return info;
}

Status Initialize(const GlobalOptions& options) noexcept {
  if (options.timezone_db_path.has_value()) {
#if !USE_OS_TZDB
    try {
      arrow_vendored::date::set_install(options.timezone_db_path.value());
      arrow_vendored::date::reload_tzdb();
    } catch (const std::runtime_error& e) {
      return Status::IOError(e.what());
    }
    timezone_db_path = options.timezone_db_path.value();
#else
    return Status::Invalid(
        "Arrow was set to use OS timezone database at compile time, "
        "so a downloaded database cannot be provided at runtime.");
#endif  // !USE_OS_TZDB
  }
  return Status::OK();
}

}  // namespace arrow
