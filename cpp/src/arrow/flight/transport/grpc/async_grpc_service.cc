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

#include "arrow/flight/transport/grpc/async_grpc_service.h"

#include <string_view>
#include <unordered_map>
#include <utility>

#include "arrow/flight/transport/grpc/async_grpc_service_internal.h"

namespace arrow::flight::transport::grpc {

namespace {

namespace detail = arrow::flight::transport::grpc::detail;
namespace pb = arrow::flight::protocol;

constexpr std::string_view kPrefix = "/arrow.flight.protocol.FlightService/";
constexpr std::string_view kHandshakeMethod = "Handshake";
constexpr std::string_view kDoGetMethod = "DoGet";
constexpr std::string_view kDoPutMethod = "DoPut";
constexpr std::string_view kDoActionMethod = "DoAction";
constexpr std::string_view kListActionsMethod = "ListActions";
constexpr std::string_view kDoExchangeMethod = "DoExchange";
constexpr std::string_view kPollFlightInfoMethod = "PollFlightInfo";
constexpr std::string_view kListFlightsMethod = "ListFlights";
constexpr std::string_view kGetFlightInfoMethod = "GetFlightInfo";
constexpr std::string_view kGetSchemaMethod = "GetSchema";

static const std::unordered_map<std::string_view, FlightMethod> kMethodMap = {
    {kHandshakeMethod, FlightMethod::Handshake},
    {kListFlightsMethod, FlightMethod::ListFlights},
    {kGetFlightInfoMethod, FlightMethod::GetFlightInfo},
    {kGetSchemaMethod, FlightMethod::GetSchema},
    {kDoGetMethod, FlightMethod::DoGet},
    {kDoPutMethod, FlightMethod::DoPut},
    {kDoActionMethod, FlightMethod::DoAction},
    {kListActionsMethod, FlightMethod::ListActions},
    {kDoExchangeMethod, FlightMethod::DoExchange},
    {kPollFlightInfoMethod, FlightMethod::PollFlightInfo},
};

FlightMethod MethodFromName(std::string_view method) {
  if (!method.starts_with(kPrefix)) {
    return FlightMethod::Invalid;
  }
  method.remove_prefix(kPrefix.size());
  const auto it = kMethodMap.find(method);
  return it != kMethodMap.end() ? it->second : FlightMethod::Invalid;
}

class Unimplemented : public ::grpc::ServerGenericBidiReactor {
 public:
  Unimplemented(detail::AsyncCallContext& flight_context, ::grpc::Status status) {
    Finish(flight_context.FinishRequest(status));
  }

  explicit Unimplemented(::grpc::Status status) { Finish(status); }

  void OnDone() override { delete this; }
};

}  // namespace

AsyncGenericFlightService::AsyncGenericFlightService(
    AsyncGenericFlightServerBase* async_base,
    std::shared_ptr<GrpcServerCallContextHelper<::grpc::CallbackServerContext>> helper)
    : base_(async_base), helper_(std::move(helper)) {}

::grpc::ServerGenericBidiReactor* AsyncGenericFlightService::CreateReactor(
    ::grpc::GenericCallbackServerContext* context) {
  const FlightMethod flight_method = MethodFromName(context->method());
  detail::AsyncCallContext flight_context(context);

  const auto prepare_status =
      flight_method == FlightMethod::Handshake
          ? helper_->MakeCallContext(flight_method, context, &flight_context)
          : helper_->CheckAuth(flight_method, context, &flight_context);
  if (!prepare_status.ok()) {
    return new Unimplemented(prepare_status);
  }

  switch (flight_method) {
    case FlightMethod::Handshake:
      return detail::MakeHandshakeReactor(std::move(flight_context), base_);
    case FlightMethod::GetFlightInfo:
    case FlightMethod::GetSchema:
    case FlightMethod::PollFlightInfo:
      return detail::MakeUnaryReactor(flight_method, std::move(flight_context), base_);
    case FlightMethod::ListActions:
      return detail::MakeListActionsReactor(std::move(flight_context), base_);
    case FlightMethod::DoAction:
      return detail::MakeDoActionReactor(std::move(flight_context), base_);
    case FlightMethod::ListFlights:
      return detail::MakeListFlightsReactor(std::move(flight_context), base_);
    case FlightMethod::DoExchange:
      return detail::MakeExchangeReactor(std::move(flight_context), base_);
    case FlightMethod::DoGet:
      return detail::MakeDoGetReactor(std::move(flight_context), base_);
    case FlightMethod::DoPut: {
      std::shared_ptr<AsyncFlightDataListener> listener =
          base_->CreateDoPutListener(flight_context);
      if (!listener) {
        return new Unimplemented(
            flight_context,
            ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED,
                           "DoPut is not implemented: no listener available"));
      }
      return detail::MakeDoPutReactor(std::move(flight_context), std::move(listener));
    }
    case FlightMethod::Invalid:
      break;
  }
  return new Unimplemented(
      flight_context,
      ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Unknown method"));
}

namespace detail {

::grpc::ServerGenericBidiReactor* MakeUnaryReactor(FlightMethod method,
                                                   AsyncCallContext flight_context,
                                                   AsyncGenericFlightServerBase* base) {
  // The response type decides the reactor's protobuf type, so the mapping stays
  // here, next to the reactor types themselves: instantiating them in a test
  // would name protobuf types the DLL does not export.
  switch (method) {
    case FlightMethod::GetFlightInfo:
      return new UnaryReactor<FlightInfo, pb::FlightInfo>(
          std::move(flight_context), base,
          &AsyncGenericFlightServerBase::GetFlightInfoAsync);
    case FlightMethod::GetSchema:
      return new UnaryReactor<SchemaResult, pb::SchemaResult>(
          std::move(flight_context), base, &AsyncGenericFlightServerBase::GetSchemaAsync);
    case FlightMethod::PollFlightInfo:
      return new UnaryReactor<PollInfo, pb::PollInfo>(
          std::move(flight_context), base,
          &AsyncGenericFlightServerBase::PollFlightInfoAsync);
    default:
      break;
  }
  // Only the three cases above reach this factory.
  return new Unimplemented(
      flight_context,
      ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "Not a unary method"));
}

}  // namespace detail

}  // namespace arrow::flight::transport::grpc
