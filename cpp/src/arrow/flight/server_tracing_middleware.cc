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

#include "arrow/flight/server_tracing_middleware.h"
#include "arrow/flight/server.h"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/flight/transport/grpc/util_internal.h"
#include "arrow/util/tracing_internal.h"

#ifdef ARROW_WITH_OPENTELEMETRY
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/trace/context.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#include <opentelemetry/trace/semantic_conventions.h>
#endif

namespace arrow {
namespace flight {

#ifdef ARROW_WITH_OPENTELEMETRY
namespace otel = opentelemetry;

// TODO: Update this once opentelemetry-cpp exposes minor version as a macro
// https://github.com/open-telemetry/opentelemetry-cpp/issues/2012
// TODO: Remove once we drop support for opentelemetry-cpp < 1.8.0
// They switched from ALL_CAPS to kConstantFormat in 1.8.0. But we can't check
// the minor version until they expose that. So, for now, we vendor these constants.
namespace SemanticConventions {
static constexpr const char* kRpcGrpcStatusCode = "rpc.grpc.status_code";
static constexpr const char* kRpcSystem = "rpc.system";
static constexpr const char* kRpcService = "rpc.service";
static constexpr const char* kRpcMethod = "rpc.method";
namespace RpcSystemValues {
static constexpr const char* kGrpc = "grpc";
}
}  // namespace SemanticConventions

namespace {
class FlightServerCarrier : public otel::context::propagation::TextMapCarrier {
 public:
  explicit FlightServerCarrier(const CallHeaders& incoming_headers)
      : incoming_headers_(incoming_headers) {}

  otel::nostd::string_view Get(otel::nostd::string_view key) const noexcept override {
    std::string_view arrow_key(key.data(), key.size());
    auto it = incoming_headers_.find(arrow_key);
    if (it == incoming_headers_.end()) return "";
    std::string_view result = it->second;
    return {result.data(), result.size()};
  }

  void Set(otel::nostd::string_view, otel::nostd::string_view) noexcept override {}

  const CallHeaders& incoming_headers_;
};
class KeyValueCarrier : public otel::context::propagation::TextMapCarrier {
 public:
  explicit KeyValueCarrier(std::vector<TracingServerMiddleware::TraceKey>* items)
      : items_(items) {}
  otel::nostd::string_view Get(otel::nostd::string_view key) const noexcept override {
    return {};
  }
  void Set(otel::nostd::string_view key,
           otel::nostd::string_view value) noexcept override {
    items_->push_back({std::string(key), std::string(value)});
  }

 private:
  std::vector<TracingServerMiddleware::TraceKey>* items_;
};
}  // namespace

class TracingServerMiddleware::Impl {
 public:
  Impl(otel::trace::Scope scope, otel::nostd::shared_ptr<otel::trace::Span> span)
      : scope_(std::move(scope)), span_(std::move(span)) {}
  void CallCompleted(const Status& status) {
    if (!status.ok()) {
      auto grpc_status = transport::grpc::ToGrpcStatus(status, /*ctx=*/nullptr);
      span_->SetStatus(otel::trace::StatusCode::kError, status.ToString());
      span_->SetAttribute(SemanticConventions::kRpcGrpcStatusCode,
                          static_cast<int32_t>(grpc_status.error_code()));
    } else {
      span_->SetStatus(otel::trace::StatusCode::kOk, "");
      span_->SetAttribute(SemanticConventions::kRpcGrpcStatusCode, int32_t(0));
    }
    span_->End();
  }
  std::vector<TraceKey> GetTraceContext() const {
    std::vector<TraceKey> result;
    KeyValueCarrier carrier(&result);
    auto context = otel::context::RuntimeContext::GetCurrent();
    otel::trace::propagation::HttpTraceContext propagator;
    propagator.Inject(carrier, context);
    return result;
  }

 private:
  otel::trace::Scope scope_;
  otel::nostd::shared_ptr<otel::trace::Span> span_;
};

class TracingServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  virtual ~TracingServerMiddlewareFactory() = default;
  Status StartCall(const CallInfo& info, const ServerCallContext& context,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    constexpr char kServiceName[] = "arrow.flight.protocol.FlightService";

    FlightServerCarrier carrier(context.incoming_headers());
    auto otel_context = otel::context::RuntimeContext::GetCurrent();
    auto propagator =
        otel::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    auto new_otel_context = propagator->Extract(carrier, otel_context);

    otel::trace::StartSpanOptions options;
    options.kind = otel::trace::SpanKind::kServer;
    options.parent = otel::trace::GetSpan(new_otel_context)->GetContext();

    auto* tracer = arrow::internal::tracing::GetTracer();
    auto method_name = ToString(info.method);
    auto span = tracer->StartSpan(
        method_name,
        {
            // Attributes from trace semantic conventions spec
            // https://github.com/open-telemetry/opentelemetry-specification/blob/main/semantic_conventions/trace/rpc.yaml
            {SemanticConventions::kRpcSystem,
             SemanticConventions::RpcSystemValues::kGrpc},
            {SemanticConventions::kRpcService, kServiceName},
            {SemanticConventions::kRpcMethod, method_name},
        },
        options);
    auto scope = tracer->WithActiveSpan(span);

    std::unique_ptr<TracingServerMiddleware::Impl> impl(
        new TracingServerMiddleware::Impl(std::move(scope), std::move(span)));
    *middleware = std::shared_ptr<TracingServerMiddleware>(
        new TracingServerMiddleware(std::move(impl)));
    return Status::OK();
  }
};
#else
class TracingServerMiddleware::Impl {
 public:
  void CallCompleted(const Status&) {}
  std::vector<TraceKey> GetTraceContext() const { return {}; }
};
class TracingServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  virtual ~TracingServerMiddlewareFactory() = default;
  Status StartCall(const CallInfo&, const ServerCallContext&,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    std::unique_ptr<TracingServerMiddleware::Impl> impl(
        new TracingServerMiddleware::Impl());
    *middleware = std::shared_ptr<TracingServerMiddleware>(
        new TracingServerMiddleware(std::move(impl)));
    return Status::OK();
  }
};
#endif

TracingServerMiddleware::TracingServerMiddleware(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
TracingServerMiddleware::~TracingServerMiddleware() = default;
void TracingServerMiddleware::SendingHeaders(AddCallHeaders*) {}
void TracingServerMiddleware::CallCompleted(const Status& status) {
  impl_->CallCompleted(status);
}
std::vector<TracingServerMiddleware::TraceKey> TracingServerMiddleware::GetTraceContext()
    const {
  return impl_->GetTraceContext();
}
constexpr char const TracingServerMiddleware::kMiddlewareName[];

std::shared_ptr<ServerMiddlewareFactory> MakeTracingServerMiddlewareFactory() {
  return std::make_shared<TracingServerMiddlewareFactory>();
}

}  // namespace flight
}  // namespace arrow
