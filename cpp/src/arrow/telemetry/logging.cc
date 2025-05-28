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

#include "arrow/telemetry/logging.h"
#include "arrow/telemetry/util_internal.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4522)
#endif

#include <google/protobuf/util/json_util.h>

#include <opentelemetry/exporters/ostream/log_record_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_recordable_utils.h>
#include <opentelemetry/logs/logger.h>
#include <opentelemetry/logs/noop.h>
#include <opentelemetry/logs/provider.h>
#include <opentelemetry/sdk/logs/batch_log_record_processor.h>
#include <opentelemetry/sdk/logs/batch_log_record_processor_options.h>
#include <opentelemetry/sdk/logs/exporter.h>
#include <opentelemetry/sdk/logs/logger_provider.h>
#include <opentelemetry/trace/tracer.h>

#include <opentelemetry/exporters/otlp/protobuf_include_prefix.h>

#include <opentelemetry/proto/collector/logs/v1/logs_service.pb.h>

#include <opentelemetry/exporters/otlp/protobuf_include_suffix.h>
#ifdef _MSC_VER
#  pragma warning(pop)
#endif

namespace arrow {
namespace telemetry {

namespace {

using internal::OtelLogExporterOptions;

constexpr const char kLoggingBackendEnvVar[] = "ARROW_LOGGING_BACKEND";

class OtlpOStreamLogRecordExporter final : public otel::sdk::logs::LogRecordExporter {
 public:
  explicit OtlpOStreamLogRecordExporter(std::ostream* sink) : sink_(sink) {
    pb_json_options_.add_whitespace = false;
  }

  otel::sdk::common::ExportResult Export(
      const otel_span<std::unique_ptr<otel::sdk::logs::Recordable>>& records) noexcept
      override {
    otel::proto::collector::logs::v1::ExportLogsServiceRequest request;
    otel::exporter::otlp::OtlpRecordableUtils::PopulateRequest(records, &request);

    for (const auto& logs : request.resource_logs()) {
      std::string out;
      auto status =
          google::protobuf::util::MessageToJsonString(logs, &out, pb_json_options_);
      if (ARROW_PREDICT_FALSE(!status.ok())) {
        return otel::sdk::common::ExportResult::kFailure;
      }
      (*sink_) << out << std::endl;
    }

    return otel::sdk::common::ExportResult::kSuccess;
  }

  bool ForceFlush(std::chrono::microseconds timeout) noexcept override {
    return exporter_.ForceFlush(timeout);
  }

  bool Shutdown(std::chrono::microseconds timeout) noexcept override {
    return exporter_.Shutdown(timeout);
  }

  std::unique_ptr<otel::sdk::logs::Recordable> MakeRecordable() noexcept override {
    return exporter_.MakeRecordable();
  }

 private:
  std::ostream* sink_;
  otel::exporter::otlp::OtlpHttpLogRecordExporter exporter_;
  google::protobuf::util::JsonPrintOptions pb_json_options_;
};

otel::logs::Severity ToOtelSeverity(LogLevel level) {
  switch (level) {
    case LogLevel::ARROW_TRACE:
      return otel::logs::Severity::kTrace;
    case LogLevel::ARROW_DEBUG:
      return otel::logs::Severity::kDebug;
    case LogLevel::ARROW_INFO:
      return otel::logs::Severity::kInfo;
    case LogLevel::ARROW_WARNING:
      return otel::logs::Severity::kWarn;
    case LogLevel::ARROW_ERROR:
      return otel::logs::Severity::kError;
    case LogLevel::ARROW_FATAL:
      return otel::logs::Severity::kFatal;
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
      otlp::OtlpHttpLogRecordExporterOptions options{};
      return std::make_unique<otlp::OtlpHttpLogRecordExporter>(options);
    } break;
    case ExporterKind::OTLP_OSTREAM: {
      return std::make_unique<OtlpOStreamLogRecordExporter>(ostream);
    } break;
    default:
      break;
  }
  return nullptr;
}

std::unique_ptr<otel::sdk::logs::LogRecordExporter> MakeExporterFromEnv(
    const OtelLogExporterOptions& exporter_options) {
  auto maybe_env_var = arrow::internal::GetEnvVar(kLoggingBackendEnvVar);
  if (maybe_env_var.ok()) {
    auto env_var = maybe_env_var.ValueOrDie();
    auto* default_ostream =
        exporter_options.default_stream ? exporter_options.default_stream : &std::cerr;
    if (env_var == "ostream") {
      // TODO: Currently disabled as the log records returned by otel's ostream exporter
      // don't maintain copies of their attributes, leading to lifetime issues. If/when
      // this is addressed, we can enable it. See:
      // https://github.com/open-telemetry/opentelemetry-cpp/issues/2402
#if 0
      return MakeExporter(ExporterKind::OSTREAM, default_ostream);
#else
      ARROW_LOG(WARNING) << "Requested unimplemented backend " << kLoggingBackendEnvVar
                         << "=" << env_var << ". Falling back to arrow_otlp_ostream";
      return MakeExporter(ExporterKind::OTLP_OSTREAM, default_ostream);
#endif
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

otel_shared_ptr<otel::logs::LoggerProvider> MakeLoggerProvider(
    const OtelLogExporterOptions& exporter_options) {
  auto exporter = MakeExporterFromEnv(exporter_options);
  if (exporter) {
    auto processor = MakeLogRecordProcessor(std::move(exporter));
    return otel_shared_ptr<otel::sdk::logs::LoggerProvider>(
        new otel::sdk::logs::LoggerProvider(std::move(processor)));
  }
  return otel_shared_ptr<otel::logs::LoggerProvider>(
      new otel::logs::NoopLoggerProvider{});
}

class OtelLoggerImpl : public OtelLogger {
 public:
  OtelLoggerImpl(OtelLoggingOptions options,
                 otel_shared_ptr<otel::logs::Logger> ot_logger)
      : logger_(ot_logger), options_(std::move(options)) {}

  void Log(const util::LogDetails& details) override {
    if (details.severity < options_.severity_threshold) {
      return;
    }

    auto log = logger_->CreateLogRecord();
    if (log == nullptr) {
      return;
    }

    // We set the remaining attributes AFTER the custom attributes in AttributeHolder
    // because, in the event of key collisions, these should take precedence.
    log->SetTimestamp(otel::common::SystemTimestamp(details.timestamp));
    log->SetSeverity(ToOtelSeverity(details.severity));

    auto span_ctx = otel::trace::Tracer::GetCurrentSpan()->GetContext();
    log->SetSpanId(span_ctx.span_id());
    log->SetTraceId(span_ctx.trace_id());
    log->SetTraceFlags(span_ctx.trace_flags());

    log->SetBody(ToOtel(details.message));

    logger_->EmitLogRecord(std::move(log));

    if (details.severity >= options_.flush_severity) {
      util::Logger::Flush();
    }
  }

  bool Flush(std::chrono::microseconds timeout) override {
    return OtelLoggerProvider::Flush(timeout);
  }

  bool is_enabled() const override { return true; }

  util::ArrowLogLevel severity_threshold() const override {
    return options_.severity_threshold;
  }

  std::string_view name() const override {
    otel_string_view s = logger_->GetName();
    return std::string_view(s.data(), s.length());
  }

 private:
  otel_shared_ptr<otel::logs::Logger> logger_;
  OtelLoggingOptions options_;
};

}  // namespace

bool OtelLoggerProvider::Flush(std::chrono::microseconds timeout) {
  auto provider = otel::logs::Provider::GetLoggerProvider();
  if (auto sdk_provider =
          dynamic_cast<otel::sdk::logs::LoggerProvider*>(provider.get())) {
    return sdk_provider->ForceFlush(timeout);
  }
  return false;
}

Result<std::shared_ptr<OtelLogger>> OtelLoggerProvider::MakeLogger(
    std::string_view name, const OtelLoggingOptions& options) {
  auto ot_logger = otel::logs::Provider::GetLoggerProvider()->GetLogger(ToOtel(name));
  return std::make_shared<OtelLoggerImpl>(options, std::move(ot_logger));
}

namespace internal {

Status InitializeOtelLoggerProvider(const OtelLogExporterOptions& exporter_options) {
  otel::logs::Provider::SetLoggerProvider(MakeLoggerProvider(exporter_options));
  return Status::OK();
}

bool ShutdownOtelLoggerProvider() {
  auto provider = otel::logs::Provider::GetLoggerProvider();
  if (auto sdk_provider =
          dynamic_cast<otel::sdk::logs::LoggerProvider*>(provider.get())) {
    return sdk_provider->Shutdown();
  }
  return false;
}

}  // namespace internal

}  // namespace telemetry
}  // namespace arrow
