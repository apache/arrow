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

// Pick up ARROW_WITH_OPENTELEMETRY first
#include "arrow/util/config.h"

#include <chrono>

#include "arrow/result.h"
#include "arrow/telemetry/logging.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

#ifdef ARROW_WITH_OPENTELEMETRY
#include <opentelemetry/exporters/ostream/log_record_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter_options.h>
#include <opentelemetry/logs/logger.h>
#include <opentelemetry/logs/noop.h>
#include <opentelemetry/logs/provider.h>
#include <opentelemetry/sdk/logs/batch_log_record_processor.h>
#include <opentelemetry/sdk/logs/batch_log_record_processor_options.h>
#include <opentelemetry/sdk/logs/exporter.h>
#include <opentelemetry/sdk/logs/logger_provider.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/resource/resource_detector.h>
#include <opentelemetry/sdk/resource/semantic_conventions.h>
#include <opentelemetry/trace/tracer.h>
#endif

namespace arrow {
namespace telemetry {
#ifdef ARROW_WITH_OPENTELEMETRY

namespace otel = ::opentelemetry;
namespace SemanticConventions = otel::sdk::resource::SemanticConventions;

namespace {

template <typename T>
using otel_shared_ptr = otel::nostd::shared_ptr<T>;
using otel_string_view = otel::nostd::string_view;

constexpr const char kLoggingBackendEnvVar[] = "ARROW_LOGGING_BACKEND";

otel::logs::Severity ToOtelSeverity(LogLevel level) {
  switch (level) {
    case LogLevel::ARROW_TRACE:
      return opentelemetry::logs::Severity::kTrace;
    case LogLevel::ARROW_DEBUG:
      return opentelemetry::logs::Severity::kDebug;
    case LogLevel::ARROW_INFO:
      return opentelemetry::logs::Severity::kInfo;
    case LogLevel::ARROW_WARNING:
      return opentelemetry::logs::Severity::kWarn;
    case LogLevel::ARROW_ERROR:
      return opentelemetry::logs::Severity::kError;
    case LogLevel::ARROW_FATAL:
      return opentelemetry::logs::Severity::kFatal;
  }
  return otel::logs::Severity::kInvalid;
}

enum class ExporterKind {
  OSTREAM,
  OTLP_HTTP,
  OTLP_OSTREAM,
};

std::unique_ptr<otel::sdk::logs::LogRecordExporter> MakeExporter(
    ExporterKind exporter_kind, std::ostream* ostream = nullptr) {
  switch (exporter_kind) {
    case ExporterKind::OSTREAM: {
      return std::make_unique<otel::exporter::logs::OStreamLogRecordExporter>(*ostream);
    } break;
    case ExporterKind::OTLP_HTTP: {
      namespace otlp = otel::exporter::otlp;
      // TODO: Allow user configuration here?
      otlp::OtlpHttpLogRecordExporterOptions options{};
      return std::make_unique<otlp::OtlpHttpLogRecordExporter>(options);
    } break;
    case ExporterKind::OTLP_OSTREAM: {
      // TODO: These require custom (subclassed) exporters
    } break;
    default:
      break;
  }
  return nullptr;
}

std::unique_ptr<otel::sdk::logs::LogRecordExporter> MakeExporterFromEnv(
    const LoggingOptions& options) {
  auto maybe_env_var = arrow::internal::GetEnvVar(kLoggingBackendEnvVar);
  if (maybe_env_var.ok()) {
    auto env_var = maybe_env_var.ValueOrDie();
    auto* default_ostream =
        options.default_export_stream ? options.default_export_stream : &std::cerr;
    if (env_var == "ostream") {
      return MakeExporter(ExporterKind::OSTREAM, default_ostream);
    } else if (env_var == "otlp_http") {
      return MakeExporter(ExporterKind::OTLP_HTTP);
    } else if (env_var == "arrow_otlp_stdout") {
      return MakeExporter(ExporterKind::OTLP_OSTREAM, &std::cout);
    } else if (env_var == "arrow_otlp_stderr") {
      return MakeExporter(ExporterKind::OTLP_OSTREAM, &std::cerr);
    } else if (env_var == "arrow_otlp_ostream") {
      return MakeExporter(ExporterKind::OTLP_OSTREAM, default_ostream);
    } else if (!env_var.empty()) {
      ARROW_LOG(WARNING) << "Requested unknown backend " << kLoggingBackendEnvVar << "="
                         << env_var;
    }
  }
  return nullptr;
}

std::unique_ptr<otel::sdk::logs::LogRecordProcessor> MakeLogRecordProcessor(
    std::unique_ptr<otel::sdk::logs::LogRecordExporter> exporter) {
  otel::sdk::logs::BatchLogRecordProcessorOptions options{};
  return std::make_unique<otel::sdk::logs::BatchLogRecordProcessor>(std::move(exporter),
                                                                    options);
}

otel::sdk::resource::Resource MakeResource(const ServiceAttributes& service_attributes) {
  // TODO: We could also include process info...
  //  - SemanticConventions::kProcessPid
  //  - SemanticConventions::kProcessExecutableName
  //  - SemanticConventions::kProcessExecutablePath
  //  - SemanticConventions::kProcessOwner
  //  - SemanticConventions::kProcessCommandArgs
  otel::sdk::resource::ResourceAttributes resource_attributes{};

  auto set_attr = [&](const char* key, const std::optional<std::string>& val) {
    if (val.has_value()) resource_attributes.SetAttribute(key, val.value());
  };
  set_attr(SemanticConventions::kServiceName, service_attributes.name);
  set_attr(SemanticConventions::kServiceNamespace, service_attributes.name_space);
  set_attr(SemanticConventions::kServiceInstanceId, service_attributes.instance_id);
  set_attr(SemanticConventions::kServiceVersion, service_attributes.version);

  auto resource = otel::sdk::resource::Resource::Create(resource_attributes);
  auto env_resource = otel::sdk::resource::OTELResourceDetector().Detect();
  return resource.Merge(env_resource);
}

otel_shared_ptr<otel::logs::LoggerProvider> MakeLoggerProvider(
    const LoggingOptions& options) {
  auto exporter = MakeExporterFromEnv(options);
  if (exporter) {
    auto processor = MakeLogRecordProcessor(std::move(exporter));
    auto resource = MakeResource(options.service_attributes);
    return otel_shared_ptr<otel::sdk::logs::LoggerProvider>(
        new otel::sdk::logs::LoggerProvider(std::move(processor), resource));
  }
  return otel_shared_ptr<otel::logs::LoggerProvider>(
      new otel::logs::NoopLoggerProvider{});
}

class NoopLogger : public Logger {
 public:
  void Log(LogLevel, std::string_view) override {}
};

class OtelLogger : public Logger {
 public:
  OtelLogger(LoggingOptions options, otel_shared_ptr<otel::logs::Logger> ot_logger)
      : logger_(ot_logger), options_(std::move(options)) {}

  void Log(LogLevel severity, std::string_view body) override {
    if (severity < options_.severity_threshold) {
      return;
    }

    auto log = logger_->CreateLogRecord();
    if (log == nullptr) {
      return;
    }

    log->SetTimestamp(otel::common::SystemTimestamp(std::chrono::system_clock::now()));
    log->SetSeverity(ToOtelSeverity(severity));
    log->SetBody(otel_string_view(body.data(), body.length()));

    auto span_ctx = otel::trace::Tracer::GetCurrentSpan()->GetContext();
    log->SetSpanId(span_ctx.span_id());
    log->SetTraceId(span_ctx.trace_id());
    log->SetTraceFlags(span_ctx.trace_flags());

    logger_->EmitLogRecord(std::move(log));
  }

 private:
  otel_shared_ptr<otel::logs::Logger> logger_;
  LoggingOptions options_;
};

std::unique_ptr<Logger> MakeOTelLogger(const LoggingOptions& options) {
  otel::logs::Provider::SetLoggerProvider(MakeLoggerProvider(options));
  return std::make_unique<OtelLogger>(
      options, otel::logs::Provider::GetLoggerProvider()->GetLogger("arrow"));
}

}  // namespace

Status LoggingEnvironment::Initialize(const LoggingOptions& options) {
  logger_ = MakeOTelLogger(options);
  return Status::OK();
}

#else

Status LoggingEnvironment::Initialize(const LoggingOptions&) { return Status::OK(); }

#endif

std::unique_ptr<Logger> MakeNoopLogger() { return std::make_unique<NoopLogger>(); }

}  // namespace telemetry
}  // namespace arrow
