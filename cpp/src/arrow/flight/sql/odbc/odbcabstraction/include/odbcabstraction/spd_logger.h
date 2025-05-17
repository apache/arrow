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

#include "odbcabstraction/logger.h"

#include <cstdint>
#include <string>

#include <spdlog/spdlog.h>

namespace driver {
namespace odbcabstraction {

class SPDLogger : public Logger {
 protected:
  std::shared_ptr<spdlog::logger> logger_;

 public:
  static constexpr std::string_view LOG_LEVEL = "LogLevel";
  static constexpr std::string_view LOG_PATH = "LogPath";
  static constexpr std::string_view MAXIMUM_FILE_SIZE = "MaximumFileSize";
  static constexpr std::string_view FILE_QUANTITY = "FileQuantity";
  static constexpr std::string_view LOG_ENABLED = "LogEnabled";

  SPDLogger() = default;
  ~SPDLogger() = default;
  SPDLogger(SPDLogger& other) = delete;

  void operator=(const SPDLogger&) = delete;
  void init(int64_t fileQuantity, int64_t maxFileSize, const std::string& fileNamePrefix,
            LogLevel level);

  void log(LogLevel level,
           const std::function<std::string(void)>& build_message) override;
};

}  // namespace odbcabstraction
}  // namespace driver
