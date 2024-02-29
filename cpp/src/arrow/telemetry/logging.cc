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
#include "arrow/telemetry/util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

#ifdef ARROW_WITH_OPENTELEMETRY
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4522)
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
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/resource/resource_detector.h>
#include <opentelemetry/sdk/resource/semantic_conventions.h>
#include <opentelemetry/trace/tracer.h>

#include <opentelemetry/exporters/otlp/protobuf_include_prefix.h>

#include <opentelemetry/proto/collector/logs/v1/logs_service.pb.h>

#include <opentelemetry/exporters/otlp/protobuf_include_suffix.h>
#ifdef _MSC_VER
#pragma warning(pop)
#endif
#endif

namespace arrow {
namespace telemetry {

namespace {

class NoopLogger : public Logger {
 public:
  void Log(const LogDescriptor&) override {}
};

}  // namespace

std::unique_ptr<Logger> MakeNoopLogger() { return std::make_unique<NoopLogger>(); }

#ifdef ARROW_WITH_OPENTELEMETRY

namespace otel = ::opentelemetry;
namespace SemanticConventions = otel::sdk::resource::SemanticConventions;

namespace {

template <typename T>
using otel_shared_ptr = otel::nostd::shared_ptr<T>;
template <typename T>
using otel_span = otel::nostd::span<T>;
using otel_string_view = otel::nostd::string_view;

using util::span;

constexpr const char kLoggingBackendEnvVar[] = "ARROW_LOGGING_BACKEND";

struct AttributeConverter {
  using OtelValue = otel::common::AttributeValue;

  OtelValue operator()(bool v) { return OtelValue(v); }
  OtelValue operator()(int32_t v) { return OtelValue(v); }
  OtelValue operator()(uint32_t v) { return OtelValue(v); }
  OtelValue operator()(int64_t v) { return OtelValue(v); }
  OtelValue operator()(double v) { return OtelValue(v); }
  OtelValue operator()(const char* v) { return OtelValue(otel_string_view(v)); }
  OtelValue operator()(std::string_view v) {
    return OtelValue(otel_string_view(v.data(), v.length()));
  }
  OtelValue operator()(span<const uint8_t> v) { return ToOtelSpan<uint8_t>(v); }
  OtelValue operator()(span<const int32_t> v) { return ToOtelSpan<int32_t>(v); }
  OtelValue operator()(span<const uint32_t> v) { return ToOtelSpan<uint32_t>(v); }
  OtelValue operator()(span<const int64_t> v) { return ToOtelSpan<int64_t>(v); }
  OtelValue operator()(span<const uint64_t> v) { return ToOtelSpan<uint64_t>(v); }
  OtelValue operator()(span<const double> v) { return ToOtelSpan<double>(v); }
  OtelValue operator()(span<const char* const> v) {
    return ToOtelStringSpan<const char*>(v);
  }
  OtelValue operator()(span<const std::string> v) {
    return ToOtelStringSpan<std::string>(v);
  }
  OtelValue operator()(span<const std::string_view> v) {
    return ToOtelStringSpan<std::string_view>(v);
  }

 private:
  template <typename T, typename U = T>
  OtelValue ToOtelSpan(span<const U> vals) const {
    return otel_span<const T>(vals.begin(), vals.end());
  }

  template <typename T>
  OtelValue ToOtelStringSpan(span<const T> vals) {
    const size_t length = vals.size();
    output_views_.resize(length);
    for (size_t i = 0; i < length; ++i) {
      const std::string_view s{vals[i]};
      output_views_[i] = otel_string_view(s.data(), s.length());
    }
    return otel_span<const otel_string_view>(output_views_.data(), length);
  }

  std::vector<otel_string_view> output_views_;
};

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
      return std::make_unique<OtlpOStreamLogRecordExporter>(ostream);
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

class OtelLogger : public Logger {
 public:
  OtelLogger(LoggingOptions options, otel_shared_ptr<otel::logs::Logger> ot_logger)
      : logger_(ot_logger), options_(std::move(options)) {}

  void Log(const LogDescriptor& desc) override {
    if (desc.severity < options_.severity_threshold) {
      return;
    }

    const auto timestamp =
        otel::common::SystemTimestamp(std::chrono::system_clock::now());

    auto log = logger_->CreateLogRecord();
    if (log == nullptr) {
      return;
    }

    if (desc.attributes && desc.attributes->num_attributes() > 0) {
      auto callback = [&log](std::string_view k, const AttributeValue& v) -> bool {
        AttributeConverter converter{};
        log->SetAttribute(otel_string_view(k.data(), k.length()),
                          std::visit(converter, v));
        return true;
      };
      desc.attributes->ForEach(std::move(callback));
    }

    // We set the remaining attributes AFTER the custom attributes in AttributeHolder
    // because, in the event of key collisions, these should take precedence.
    log->SetTimestamp(timestamp);
    log->SetSeverity(ToOtelSeverity(desc.severity));

    auto span_ctx = otel::trace::Tracer::GetCurrentSpan()->GetContext();
    log->SetSpanId(span_ctx.span_id());
    log->SetTraceId(span_ctx.trace_id());
    log->SetTraceFlags(span_ctx.trace_flags());

    if (desc.body) {
      auto body = *desc.body;
      log->SetBody(otel_string_view(body.data(), body.length()));
    }

    if (const auto& event = desc.event_id; event.is_valid()) {
      log->SetEventId(event.id, otel_string_view(event.name.data(), event.name.length()));
    }

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

}  // namespace telemetry
}  // namespace arrow
