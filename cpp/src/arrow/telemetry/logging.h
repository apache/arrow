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

#include <chrono>
#include <iosfwd>
#include <memory>
#include <string>
#include <string_view>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logger.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace telemetry {

using LogLevel = util::ArrowLogLevel;

struct OtelLoggingOptions {
  /// \brief Minimum severity required to emit an OpenTelemetry log record
  LogLevel severity_threshold = LogLevel::ARROW_INFO;

  /// \brief Minimum severity required to immediately attempt to flush pending log records
  LogLevel flush_severity = LogLevel::ARROW_ERROR;

  static OtelLoggingOptions Defaults() { return OtelLoggingOptions{}; }
};

class ARROW_EXPORT OtelLogger : public util::Logger {
 public:
  virtual ~OtelLogger() = default;

  virtual std::string_view name() const = 0;
};

/// \brief A wrapper interface for `opentelemetry::logs::Provider`
/// \details Application authors will typically want to set the global OpenTelemetry
/// logger provider themselves after configuring an exporter, processor, resource etc.
/// This API will then defer to the provider returned by
/// `opentelemetry::logs::Provider::GetLoggerProvider`
class ARROW_EXPORT OtelLoggerProvider {
 public:
  /// \brief Attempt to flush the log record processor associated with the provider
  /// \return `true` if the flush occured
  static bool Flush(std::chrono::microseconds timeout = std::chrono::microseconds::max());

  static Result<std::shared_ptr<OtelLogger>> MakeLogger(
      std::string_view name,
      const OtelLoggingOptions& options = OtelLoggingOptions::Defaults());
};

namespace internal {

// These utilities are primarily intended for Arrow developers

struct OtelLogExporterOptions {
  /// \brief Default stream to use for the ostream/arrow_otlp_ostream log record exporters
  /// \details If null, stderr will be used
  std::ostream* default_stream = NULLPTR;

  static OtelLogExporterOptions Defaults() { return OtelLogExporterOptions{}; }
};

/// \brief Initialize the global OpenTelemetry logger provider with a default exporter
/// (based on the ARROW_LOGGING_BACKEND envvar) and batch processor
ARROW_EXPORT Status InitializeOtelLoggerProvider(
    const OtelLogExporterOptions& exporter_options = OtelLogExporterOptions::Defaults());

/// \brief Attempt to shut down the global OpenTelemetry logger provider
/// \return `true` if shutdown was successful
ARROW_EXPORT bool ShutdownOtelLoggerProvider();

}  // namespace internal

}  // namespace telemetry
}  // namespace arrow
