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
#include <optional>
#include <string>
#include <string_view>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/config.h"
#include "arrow/util/logging.h"
#include "arrow/util/logging_v2.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace telemetry {

class AttributeHolder;

using LogLevel = util::ArrowLogLevel;

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

struct LoggerProviderOptions {
  /// \brief Attributes to set for the LoggerProvider's Resource
  ServiceAttributes service_attributes = ServiceAttributes::Defaults();

  /// \brief Default stream to use for the ostream/arrow_otlp_ostream log record exporters
  ///
  /// If null, stderr will be used
  std::ostream* default_export_stream = NULLPTR;

  static LoggerProviderOptions Defaults() { return LoggerProviderOptions{}; }
};

constexpr LogLevel kDefaultSeverityThreshold = LogLevel::ARROW_WARNING;
constexpr LogLevel kDefaultSeverity = LogLevel::ARROW_INFO;

struct LoggingOptions {
  /// \brief Minimum severity required to emit an OpenTelemetry log record
  LogLevel severity_threshold = kDefaultSeverityThreshold;

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
  LogLevel severity = kDefaultSeverity;

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

class ARROW_EXPORT GlobalLoggerProvider {
 public:
  static Status Initialize(
      const LoggerProviderOptions& = LoggerProviderOptions::Defaults());

  static bool ShutDown();

  static bool Flush(std::chrono::microseconds timeout = std::chrono::microseconds::max());

  static Result<std::shared_ptr<Logger>> MakeLogger(
      std::string_view name, const LoggingOptions& options = LoggingOptions::Defaults());
  static Result<std::shared_ptr<Logger>> MakeLogger(std::string_view name,
                                                    const LoggingOptions& options,
                                                    const AttributeHolder& attributes);
};

}  // namespace telemetry
}  // namespace arrow