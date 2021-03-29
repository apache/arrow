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

#include "arrow/util/tracing_internal.h"

#include <iostream>
#include <sstream>
#include <thread>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4522)
#endif
#ifdef ARROW_WITH_OPENTELEMETRY
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/recordable.h>
#include <opentelemetry/sdk/trace/span_data.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/noop.h>
#include <opentelemetry/trace/provider.h>
#endif
#ifdef _MSC_VER
#pragma warning(pop)
#endif

#include "arrow/util/config.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#ifdef ARROW_JSON
#include "arrow/json/rapidjson_defs.h"
#include "rapidjson/ostreamwrapper.h"
#include "rapidjson/writer.h"
#endif

namespace arrow {
namespace internal {
namespace tracing {

namespace nostd = opentelemetry::nostd;
namespace otel = opentelemetry;

constexpr char kTracingBackendEnvVar[] = "ARROW_TRACING_BACKEND";

namespace {

#ifdef ARROW_WITH_OPENTELEMETRY
namespace sdktrace = opentelemetry::sdk::trace;
#ifdef ARROW_JSON
struct OwnedAttributeValueVisitor {
  OwnedAttributeValueVisitor(
      arrow::rapidjson::Writer<arrow::rapidjson::OStreamWrapper>& writer_)
      : writer(writer_) {}

  void operator()(const std::string& arg) { writer.String(arg); }

  void operator()(const int32_t& arg) { writer.Int(arg); }

  void operator()(const uint32_t& arg) { writer.Uint(arg); }

  void operator()(const int64_t& arg) { writer.Int64(arg); }

  void operator()(const uint64_t& arg) { writer.Uint64(arg); }

  template <typename T>
  void operator()(T&& arg) {
    writer.Null();
  }

  arrow::rapidjson::Writer<arrow::rapidjson::OStreamWrapper>& writer;
};

/// Export spans as newline-delimited JSON.
class OStreamJsonSpanExporter : public sdktrace::SpanExporter {
 public:
  explicit OStreamJsonSpanExporter(std::ostream& sout = std::cerr) noexcept
      : sout_(sout), shutdown_(false) {}
  std::unique_ptr<sdktrace::Recordable> MakeRecordable() noexcept override {
    return std::unique_ptr<sdktrace::Recordable>(new sdktrace::SpanData);
  }
  otel::sdk::common::ExportResult Export(
      const nostd::span<std::unique_ptr<sdktrace::Recordable>>& spans) noexcept override {
    if (shutdown_) return otel::sdk::common::ExportResult::kFailure;

    for (auto& recordable : spans) {
      arrow::rapidjson::Writer<arrow::rapidjson::OStreamWrapper> writer(sout_);
      auto span = std::unique_ptr<sdktrace::SpanData>(
          static_cast<sdktrace::SpanData*>(recordable.release()));
      if (!span) continue;
      char trace_id[32] = {0};
      char span_id[16] = {0};
      char parent_span_id[16] = {0};
      span->GetTraceId().ToLowerBase16(trace_id);
      span->GetSpanId().ToLowerBase16(span_id);
      span->GetParentSpanId().ToLowerBase16(parent_span_id);

      writer.StartObject();
      writer.Key("name");
      writer.String(span->GetName().data(), span->GetName().length());
      writer.Key("trace_id");
      writer.String(trace_id, 32);
      writer.Key("span_id");
      writer.String(span_id, 16);
      writer.Key("parent_span_id");
      writer.String(parent_span_id, 16);
      writer.Key("start");
      writer.Int64(span->GetStartTime().time_since_epoch().count());
      writer.Key("duration");
      writer.Int64(span->GetDuration().count());
      writer.Key("description");
      writer.String(span->GetDescription().data(), span->GetDescription().length());
      writer.Key("kind");
      writer.Int(static_cast<int>(span->GetSpanKind()));
      writer.Key("status");
      // TODO: this is expensive
      writer.String(statuses_[static_cast<int>(span->GetStatus())]);
      writer.Key("args");
      writer.StartObject();
      OwnedAttributeValueVisitor visitor(writer);
      for (const auto& pair : span->GetAttributes()) {
        writer.Key(pair.first.data(), pair.first.length());
        nostd::visit(visitor, pair.second);
      }
      writer.EndObject();
      writer.EndObject();
      sout_.Put('\n');
    }
    sout_.Flush();
    return otel::sdk::common::ExportResult::kSuccess;
  }
  bool Shutdown(std::chrono::microseconds) noexcept override {
    shutdown_ = true;
    return true;
  }

 private:
  arrow::rapidjson::OStreamWrapper sout_;
  bool shutdown_;
  std::map<int, std::string> statuses_{{0, "Unset"}, {1, "Ok"}, {2, "Error"}};
};
#endif

class ThreadIdSpanProcessor : public sdktrace::BatchSpanProcessor {
 public:
  using sdktrace::BatchSpanProcessor::BatchSpanProcessor;
  void OnEnd(std::unique_ptr<sdktrace::Recordable>&& span) noexcept override {
    std::stringstream thread_id;
    thread_id << std::this_thread::get_id();
    span->SetAttribute("thread_id", thread_id.str());
    sdktrace::BatchSpanProcessor::OnEnd(std::move(span));
  }
};

std::unique_ptr<sdktrace::SpanExporter> InitializeExporter() {
  auto maybe_env_var = arrow::internal::GetEnvVar(kTracingBackendEnvVar);
  if (maybe_env_var.ok()) {
    auto env_var = maybe_env_var.ValueOrDie();
    if (env_var == "json") {
#ifdef ARROW_JSON
      return std::unique_ptr<sdktrace::SpanExporter>(
          new OStreamJsonSpanExporter(std::cerr));
#else
      ARROW_LOG(WARNING) << "Requested " << kTracingBackendEnvVar
                         << "=json but Arrow was built without ARROW_JSON";
#endif
    } else if (!env_var.empty()) {
      ARROW_LOG(WARNING) << "Requested unknown backend " << kTracingBackendEnvVar << "="
                         << env_var;
    }
  }
  return std::unique_ptr<sdktrace::SpanExporter>();
}

nostd::shared_ptr<sdktrace::TracerProvider> InitializeSdkTracerProvider() {
  auto exporter = InitializeExporter();
  if (exporter) {
    sdktrace::BatchSpanProcessorOptions options;
    options.max_queue_size = 16384;
    options.schedule_delay_millis = std::chrono::milliseconds(500);
    options.max_export_batch_size = 16384;
    auto processor = std::unique_ptr<sdktrace::SpanProcessor>(
        new ThreadIdSpanProcessor(std::move(exporter), options));
    return nostd::shared_ptr<sdktrace::TracerProvider>(
        new sdktrace::TracerProvider(std::move(processor)));
  }
  return nostd::shared_ptr<sdktrace::TracerProvider>();
}

class FlushLog {
 public:
  explicit FlushLog(nostd::shared_ptr<sdktrace::TracerProvider> provider)
      : provider_(std::move(provider)) {}
  ~FlushLog() {
    if (provider_) {
      provider_->ForceFlush(std::chrono::microseconds(1000000));
    }
  }
  nostd::shared_ptr<sdktrace::TracerProvider> provider_;
};

nostd::shared_ptr<sdktrace::TracerProvider> GetSdkTracerProvider() {
  static FlushLog flush_log = FlushLog(InitializeSdkTracerProvider());
  return flush_log.provider_;
}
#else
nostd::shared_ptr<otel::trace::TracerProvider> GetSdkTracerProvider() {
  auto maybe_env_var = arrow::internal::GetEnvVar(kTracingBackendEnvVar);
  if (maybe_env_var.ok()) {
    auto env_var = maybe_env_var.ValueOrDie();
    ARROW_LOG(WARNING)
        << "Requested " << kTracingBackendEnvVar << "=" << env_var
        << " but Arrow was built without ARROW_WITH_OPENTELEMETRY, using no-op tracer";
  }
  return nostd::shared_ptr<otel::trace::TracerProvider>();
}
#endif

nostd::shared_ptr<otel::trace::TracerProvider> InitializeTracing() {
  nostd::shared_ptr<otel::trace::TracerProvider> provider = GetSdkTracerProvider();
  if (provider) otel::trace::Provider::SetTracerProvider(provider);
  return otel::trace::Provider::GetTracerProvider();
}

otel::trace::TracerProvider* GetTracerProvider() {
  static nostd::shared_ptr<otel::trace::TracerProvider> provider = InitializeTracing();
  return provider.get();
}
}  // namespace

opentelemetry::trace::Tracer* GetTracer() {
  static nostd::shared_ptr<opentelemetry::trace::Tracer> tracer =
      GetTracerProvider()->GetTracer("arrow");
  return tracer.get();
}

}  // namespace tracing
}  // namespace internal
}  // namespace arrow
