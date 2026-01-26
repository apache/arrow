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

#pragma once

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>

#include "arrow/util/logging.h"

namespace parquet::encryption {

// Configure Arrow logging threshold from environment (once per procall site).
//
// There is currently no mechanism to configure Arrow's logger.
// Given that most of the logging changes that we have added are related to
// external/dbpa encryption, we decided to keep most of the Arrow base code as-is,
// and made the config belong to external/dbpa.
//
// Env var: PARQUET_DBPA_LOG_LEVEL (case-insensitive: TRACE, DEBUG, INFO, WARN[ING],
// ERROR, FATAL, or -2..3)
inline void ConfigureArrowLogLevel() {
  const char* env_val = std::getenv("PARQUET_DBPA_LOG_LEVEL");
  if (env_val == nullptr || *env_val == '\0') {
    return;
  }
  std::string s(env_val);
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::toupper(c)); });

  using ::arrow::util::ArrowLogLevel;
  auto to_level = [&](const std::string& v) -> std::optional<ArrowLogLevel> {
    if (v == "TRACE") return ArrowLogLevel::ARROW_TRACE;
    if (v == "DEBUG") return ArrowLogLevel::ARROW_DEBUG;
    if (v == "INFO") return ArrowLogLevel::ARROW_INFO;
    if (v == "WARN" || v == "WARNING") return ArrowLogLevel::ARROW_WARNING;
    if (v == "ERROR") return ArrowLogLevel::ARROW_ERROR;
    if (v == "FATAL") return ArrowLogLevel::ARROW_FATAL;
    try {
      int n = std::stoi(v);
      switch (n) {
        case -2:
          return ArrowLogLevel::ARROW_TRACE;
        case -1:
          return ArrowLogLevel::ARROW_DEBUG;
        case 0:
          return ArrowLogLevel::ARROW_INFO;
        case 1:
          return ArrowLogLevel::ARROW_WARNING;
        case 2:
          return ArrowLogLevel::ARROW_ERROR;
        case 3:
          return ArrowLogLevel::ARROW_FATAL;
        default:
          return std::nullopt;
      }
    } catch (...) {
      return std::nullopt;
    }
  };

  if (auto lvl = to_level(s)) {
    ::arrow::util::ArrowLog::StartArrowLog("parquet-dbpa", *lvl, "");
  }
}

inline void EnsureDbpaLoggingConfigured() {
  static std::once_flag once;
  std::call_once(once, [] { ConfigureArrowLogLevel(); });
}

}  // namespace parquet::encryption
