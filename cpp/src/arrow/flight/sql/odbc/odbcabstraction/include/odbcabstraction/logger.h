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

#include <functional>
#include <string>

#include <spdlog/fmt/bundled/format.h>

#define __LAZY_LOG(LEVEL, ...)                                         \
  do {                                                                 \
    driver::odbcabstraction::Logger* logger =                          \
        driver::odbcabstraction::Logger::GetInstance();                \
    if (logger) {                                                      \
      logger->log(driver::odbcabstraction::LogLevel::LogLevel_##LEVEL, \
                  [&]() { return fmt::format(__VA_ARGS__); });         \
    }                                                                  \
  } while (0)
#define LOG_DEBUG(...) __LAZY_LOG(DEBUG, __VA_ARGS__)
#define LOG_INFO(...) __LAZY_LOG(INFO, __VA_ARGS__)
#define LOG_ERROR(...) __LAZY_LOG(ERROR, __VA_ARGS__)
#define LOG_TRACE(...) __LAZY_LOG(TRACE, __VA_ARGS__)
#define LOG_WARN(...) __LAZY_LOG(WARN, __VA_ARGS__)

namespace driver {
namespace odbcabstraction {

enum LogLevel {
  LogLevel_TRACE,
  LogLevel_DEBUG,
  LogLevel_INFO,
  LogLevel_WARN,
  LogLevel_ERROR,
  LogLevel_OFF
};

class Logger {
 protected:
  Logger() = default;

 public:
  static Logger* GetInstance();
  static void SetInstance(std::unique_ptr<Logger> logger);

  virtual ~Logger() = default;

  virtual void log(LogLevel level,
                   const std::function<std::string(void)>& build_message) = 0;
};

}  // namespace odbcabstraction
}  // namespace driver
