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
#include "arrow/util/logging.h"
#include "arrow/util/logging_v2.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace telemetry {

class AttributeHolder;

using LogLevel = util::ArrowLogLevel;

struct LoggingOptions {
  /// \brief Minimum severity required to emit an OpenTelemetry log record
  LogLevel severity_threshold = LogLevel::ARROW_INFO;

  /// \brief Minimum severity required to immediately attempt to flush pending log records
  LogLevel flush_severity = LogLevel::ARROW_ERROR;

  static LoggingOptions Defaults() { return LoggingOptions{}; }
};

/// \brief Represents an event as a name/integer id pair
struct EventId {
  constexpr EventId() = default;
  constexpr EventId(int64_t id, std::string_view name) : name(name), id(id) {}

  constexpr bool is_valid() const { return id >= 0; }
  constexpr operator bool() const { return is_valid(); }

  static constexpr EventId Invalid() { return EventId{}; }

  std::string_view name;
  int64_t id = -1;
};

struct LogDescriptor {
  LogLevel severity = LogLevel::ARROW_INFO;

  std::chrono::system_clock::time_point timestamp = std::chrono::system_clock::now();

  std::string_view body = "";

  EventId event_id = EventId::Invalid();

  const AttributeHolder* attributes = NULLPTR;
};

class ARROW_EXPORT Logger : public util::Logger {
 public:
  virtual ~Logger() = default;

  virtual void Log(const LogDescriptor&) = 0;

  void Log(const util::LogDetails& details) override {
    LogDescriptor desc;
    desc.body = details.message;
    desc.severity = details.severity;
    desc.timestamp = details.timestamp;
    this->Log(desc);
  }

  void Log(LogLevel severity, std::string_view body, const AttributeHolder& attributes,
           EventId event_id = EventId::Invalid()) {
    LogDescriptor desc;
    desc.severity = severity;
    desc.body = body;
    desc.attributes = &attributes;
    desc.event_id = event_id;
    this->Log(desc);
  }

  void Log(LogLevel severity, EventId event_id = EventId::Invalid()) {
    LogDescriptor desc;
    desc.severity = severity;
    desc.event_id = event_id;
    this->Log(desc);
  }

  void Log(LogLevel severity, const AttributeHolder& attributes,
           EventId event_id = EventId::Invalid()) {
    LogDescriptor desc;
    desc.severity = severity;
    desc.attributes = &attributes;
    desc.event_id = event_id;
    this->Log(desc);
  }

  void Log(LogLevel severity, std::string_view body,
           EventId event_id = EventId::Invalid()) {
    LogDescriptor desc;
    desc.severity = severity;
    desc.body = body;
    desc.event_id = event_id;
    this->Log(desc);
  }

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

  static Result<std::shared_ptr<Logger>> MakeLogger(
      std::string_view name, const LoggingOptions& options = LoggingOptions::Defaults());
  static Result<std::shared_ptr<Logger>> MakeLogger(std::string_view name,
                                                    const LoggingOptions& options,
                                                    const AttributeHolder& attributes);
};

namespace internal {

// These utilities are primarily intended for Arrow developers

struct LogExporterOptions {
  /// \brief Default stream to use for the ostream/arrow_otlp_ostream log record exporters
  /// \details If null, stderr will be used
  std::ostream* default_stream = NULLPTR;

  static LogExporterOptions Defaults() { return LogExporterOptions{}; }
};

/// \brief Initialize the global OpenTelemetry logger provider with a default exporter
/// (based on the ARROW_LOGGING_BACKEND envvar) and batch processor
ARROW_EXPORT Status InitializeOtelLoggerProvider(
    const LogExporterOptions& exporter_options = LogExporterOptions::Defaults());

/// \brief Attempt to shut down the global OpenTelemetry logger provider
/// \return `true` if shutdown was successful
ARROW_EXPORT bool ShutdownOtelLoggerProvider();

}  // namespace internal

}  // namespace telemetry
}  // namespace arrow
