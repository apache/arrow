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

#include "arrow/result.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/otel_logging.h"

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
#endif

namespace arrow {
namespace util {
namespace logging {
#ifdef ARROW_WITH_OPENTELEMETRY

namespace otel = ::opentelemetry;
namespace SemanticConventions = otel::sdk::resource::SemanticConventions;

namespace {

template <typename T>
using otel_shared_ptr = otel::nostd::shared_ptr<T>;

constexpr const char kLoggingBackendEnvVar[] = "ARROW_LOGGING_BACKEND";

std::unique_ptr<otel::sdk::logs::LogRecordExporter> MakeExporter(
    ExporterType exporter_type, std::ostream* ostream = nullptr) {
  switch (exporter_type) {
    case ExporterType::OSTREAM: {
      return std::make_unique<otel::exporter::logs::OStreamLogRecordExporter>(*ostream);
    } break;
    case ExporterType::OTLP_HTTP: {
      namespace otlp = otel::exporter::otlp;
      // TODO: Allow user configuration here?
      otlp::OtlpHttpLogRecordExporterOptions options{};
      return std::make_unique<otlp::OtlpHttpLogRecordExporter>(options);
    } break;
    case ExporterType::OTLP_STDOUT:
    case ExporterType::OTLP_STDERR: {
      // TODO: These require custom (subclassed) exporters
    } break;
    default:
      break;
  }
  return nullptr;
}

std::unique_ptr<otel::sdk::logs::LogRecordExporter> GetExporter() {
  auto maybe_env_var = arrow::internal::GetEnvVar(kLoggingBackendEnvVar);
  if (maybe_env_var.ok()) {
    auto env_var = maybe_env_var.ValueOrDie();
    if (env_var == "ostream") {
      return MakeExporter(ExporterType::OSTREAM, &std::cerr);
    } else if (env_var == "otlp_http") {
      return MakeExporter(ExporterType::OTLP_HTTP);
    } else if (env_var == "arrow_otlp_stdout") {
      return MakeExporter(ExporterType::OTLP_STDOUT, &std::cout);
    } else if (env_var == "arrow_otlp_stderr") {
      return MakeExporter(ExporterType::OTLP_STDERR, &std::cerr);
    } else if (!env_var.empty()) {
      ARROW_LOG(WARNING) << "Requested unknown backend " << kLoggingBackendEnvVar << "="
                         << env_var;
    }
  }
  return nullptr;
}

std::unique_ptr<otel::sdk::logs::LogRecordProcessor> GetProcessor(
    std::unique_ptr<otel::sdk::logs::LogRecordExporter> exporter) {
  otel::sdk::logs::BatchLogRecordProcessorOptions options{};
  return std::make_unique<otel::sdk::logs::BatchLogRecordProcessor>(std::move(exporter),
                                                                    options);
}

otel::sdk::resource::Resource GetResource() {
  // TODO: We'll want to set custom key/value attributes here (with keys that respect
  // OTel's semantic conventions). Some of them could probably be user-configurable, e.g:
  //  - SemanticConventions::kServiceName
  //  - SemanticConventions::kServiceInstanceId
  //  - SemanticConventions::kServiceNamespace
  //  - SemanticConventions::kServiceVersion
  // We could also include process info...
  //  - SemanticConventions::kProcessPid
  //  - SemanticConventions::kProcessExecutableName
  //  - SemanticConventions::kProcessExecutablePath
  //  - SemanticConventions::kProcessOwner
  //  - SemanticConventions::kProcessCommandArgs
  otel::sdk::resource::ResourceAttributes attributes{};
  auto resource = otel::sdk::resource::Resource::Create(attributes);
  auto env_resource = otel::sdk::resource::OTELResourceDetector().Detect();
  return resource.Merge(env_resource);
}

otel_shared_ptr<otel::logs::LoggerProvider> GetLoggerProvider() {
  auto exporter = GetExporter();
  if (exporter) {
    auto processor = GetProcessor(std::move(exporter));
    auto resource = GetResource();
    return otel_shared_ptr<otel::sdk::logs::LoggerProvider>(
        new otel::sdk::logs::LoggerProvider(std::move(processor), resource));
  }
  return otel_shared_ptr<otel::logs::LoggerProvider>(
      new otel::logs::NoopLoggerProvider{});
}

}  // namespace

void InitializeLoggerProvider() {
  auto provider = GetLoggerProvider();
  // The LoggerProvider can be accessed globally, serving as a factory for loggers
  otel::logs::Provider::SetLoggerProvider(std::move(provider));
}

#else

void InitializeLoggerProvider() {}

#endif
}  // namespace logging
}  // namespace util
}  // namespace arrow
