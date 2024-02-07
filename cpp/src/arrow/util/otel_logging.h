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

#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "arrow/util/config.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {
namespace logging {

using LogLevel = ArrowLogLevel;

/// \brief Attributes to be set in an OpenTelemetry resource
///
/// The OTEL_RESOURCE_ATTRIBUTES envvar can be used to set additional attributes
struct ServiceAttributes {
  static constexpr char kDefaultName[] = "arrow.unknown_service";
  static constexpr char kDefaultNamespace[] = "org.apache";
  static constexpr char kDefaultInstanceId[] = "arrow.unknown_id";
  static constexpr char kDefaultVersion[] = ARROW_VERSION_STRING;

  std::optional<std::string> name = kDefaultName;
  std::optional<std::string> name_space = kDefaultNamespace;
  std::optional<std::string> instance_id = kDefaultInstanceId;
  std::optional<std::string> version = kDefaultVersion;

  static ServiceAttributes Defaults() { return ServiceAttributes{}; }
};

struct LoggingOptions {
  /// \brief Attributes to set for the LoggerProvider's Resource
  ServiceAttributes service_attributes = ServiceAttributes::Defaults();

  /// \brief Default stream to use for the ostream/arrow_otlp_ostream log record exporters
  ///
  /// If null, stderr will be used
  std::ostream* default_export_stream = NULLPTR;

  /// \brief Minimum severity required to emit an OpenTelemetry log record
  LogLevel severity_threshold = LogLevel::ARROW_INFO;

  static LoggingOptions Defaults() { return LoggingOptions{}; }
};

class ARROW_EXPORT Logger {
 public:
  virtual ~Logger() = default;

  virtual void Log(LogLevel severity, std::string_view body) = 0;

  void Log(std::string_view message) { this->Log(LogLevel::ARROW_INFO, message); }
};

ARROW_EXPORT std::unique_ptr<Logger> MakeNoopLogger();

class ARROW_EXPORT LoggingEnvironment {
 public:
  static Status Initialize(const LoggingOptions& options = LoggingOptions::Defaults());

  static void Reset() { logger_.reset(); }

  static Logger* GetLogger() {
    if (!logger_) {
      logger_ = MakeNoopLogger();
    }
    return logger_.get();
  }

  static void SetLogger(std::unique_ptr<Logger> logger) {
    if (logger) {
      logger_ = std::move(logger);
    } else {
      logger_ = MakeNoopLogger();
    }
  }

 private:
  static inline std::unique_ptr<Logger> logger_{};
};

}  // namespace logging
}  // namespace util
}  // namespace arrow
