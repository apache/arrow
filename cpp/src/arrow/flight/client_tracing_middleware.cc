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

#include "arrow/flight/client_tracing_middleware.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/util/tracing_internal.h"

#ifdef ARROW_WITH_OPENTELEMETRY
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#endif

namespace arrow {
namespace flight {

namespace {
#ifdef ARROW_WITH_OPENTELEMETRY
namespace otel = opentelemetry;
class FlightClientCarrier : public otel::context::propagation::TextMapCarrier {
 public:
  FlightClientCarrier() = default;

  otel::nostd::string_view Get(otel::nostd::string_view key) const noexcept override {
    return "";
  }

  void Set(otel::nostd::string_view key,
           otel::nostd::string_view value) noexcept override {
    context_.emplace_back(key, value);
  }

  std::vector<std::pair<std::string, std::string>> context_;
};

class TracingClientMiddleware : public ClientMiddleware {
 public:
  explicit TracingClientMiddleware(FlightClientCarrier carrier)
      : carrier_(std::move(carrier)) {}
  virtual ~TracingClientMiddleware() = default;

  void SendingHeaders(AddCallHeaders* outgoing_headers) override {
    // The exact headers added are not arbitrary and are defined in
    // the OpenTelemetry specification (see
    // open-telemetry/opentelemetry-specification api-propagators.md)
    for (const auto& pair : carrier_.context_) {
      outgoing_headers->AddHeader(pair.first, pair.second);
    }
  }
  void ReceivedHeaders(const CallHeaders&) override {}
  void CallCompleted(const Status&) override {}

 private:
  FlightClientCarrier carrier_;
};

class TracingClientMiddlewareFactory : public ClientMiddlewareFactory {
 public:
  virtual ~TracingClientMiddlewareFactory() = default;
  void StartCall(const CallInfo& info,
                 std::unique_ptr<ClientMiddleware>* middleware) override {
    FlightClientCarrier carrier;
    auto context = otel::context::RuntimeContext::GetCurrent();
    auto propagator =
        otel::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    propagator->Inject(carrier, context);
    *middleware = std::make_unique<TracingClientMiddleware>(std::move(carrier));
  }
};
#else
class TracingClientMiddlewareFactory : public ClientMiddlewareFactory {
 public:
  virtual ~TracingClientMiddlewareFactory() = default;
  void StartCall(const CallInfo&, std::unique_ptr<ClientMiddleware>*) override {}
};
#endif
}  // namespace

std::shared_ptr<ClientMiddlewareFactory> MakeTracingClientMiddlewareFactory() {
  return std::make_shared<TracingClientMiddlewareFactory>();
}

}  // namespace flight
}  // namespace arrow
