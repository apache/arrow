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

// Standalone async Flight server implementation on top of the transport
// interface.

#include "arrow/flight/server_async.h"

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/flight/transport.h"
#include "arrow/flight/transport/grpc/async_grpc_compat.h"
#include "arrow/flight/transport/grpc/grpc_server.h"
#if ARROW_FLIGHT_HAS_ASYNC_SERVER
#  include "arrow/flight/transport/grpc/async_grpc_service_internal.h"
#endif
#include "arrow/flight/transport_server.h"
#include "arrow/flight/transport_server_internal.h"
#include "arrow/flight/types.h"

namespace arrow::flight {

AsyncFlightDataStream::~AsyncFlightDataStream() = default;
AsyncFlightListing::~AsyncFlightListing() = default;
AsyncResultStream::~AsyncResultStream() = default;
AsyncFlightMessageReader::~AsyncFlightMessageReader() = default;
AsyncFlightMessageWriter::~AsyncFlightMessageWriter() = default;

struct AsyncGenericFlightServerBase::Impl {
  internal::ServerLifecycle lifecycle{"AsyncGenericFlightServerBase"};
};

AsyncGenericFlightServerBase::AsyncGenericFlightServerBase() : impl_(new Impl) {}

AsyncGenericFlightServerBase::~AsyncGenericFlightServerBase() = default;

Status AsyncGenericFlightServerBase::Init(const FlightServerOptions& options) {
  flight::transport::grpc::InitializeFlightGrpcServer();

  const auto scheme = options.location.scheme();
  ARROW_ASSIGN_OR_RAISE(auto transport,
                        internal::GetDefaultTransportRegistry()->MakeServerAsync(
                            scheme, this, options.memory_manager));
  return impl_->lifecycle.Init(options, std::move(transport));
}

int AsyncGenericFlightServerBase::port() const { return impl_->lifecycle.port(); }

Location AsyncGenericFlightServerBase::location() const {
  return impl_->lifecycle.location();
}

Status AsyncGenericFlightServerBase::SetShutdownOnSignals(
    const std::vector<int> signals) {
  return impl_->lifecycle.SetShutdownOnSignals(signals);
}

Status AsyncGenericFlightServerBase::Serve() { return impl_->lifecycle.Serve(); }

int AsyncGenericFlightServerBase::GotSignal() const {
  return impl_->lifecycle.GotSignal();
}

Status AsyncGenericFlightServerBase::Shutdown(
    const std::chrono::system_clock::time_point* deadline) {
  return impl_->lifecycle.Shutdown(deadline);
}

Status AsyncGenericFlightServerBase::Wait() { return impl_->lifecycle.Wait(); }

Status AsyncGenericFlightServerBase::AddCancelCallback(const ServerCallContext& context,
                                                       std::function<void()> callback) {
#if ARROW_FLIGHT_HAS_ASYNC_SERVER
  // Only the contexts the async transport hands its handlers carry a registry.
  if (const auto* async_context =
          dynamic_cast<const transport::grpc::detail::AsyncCallContext*>(&context)) {
    return async_context->AddCancelCallback(std::move(callback));
  }
#endif
  return Status::Invalid(
      "AddCancelCallback() needs the context an AsyncGenericFlightServerBase handed "
      "to a handler");
}

// --- handler defaults: the same answers FlightServerBase's defaults give ---

arrow::Future<std::shared_ptr<AsyncFlightListing>>
AsyncGenericFlightServerBase::ListFlightsAsync(const ServerCallContext&,
                                               const Criteria*) {
  return arrow::Future<std::shared_ptr<AsyncFlightListing>>::MakeFinished(
      Status::NotImplemented("NYI"));
}

arrow::Future<std::shared_ptr<FlightInfo>>
AsyncGenericFlightServerBase::GetFlightInfoAsync(const ServerCallContext&,
                                                 const FlightDescriptor&) {
  return arrow::Future<std::shared_ptr<FlightInfo>>::MakeFinished(
      Status::NotImplemented("NYI"));
}

arrow::Future<std::shared_ptr<PollInfo>>
AsyncGenericFlightServerBase::PollFlightInfoAsync(const ServerCallContext&,
                                                  const FlightDescriptor&) {
  return arrow::Future<std::shared_ptr<PollInfo>>::MakeFinished(
      Status::NotImplemented("NYI"));
}

arrow::Future<std::shared_ptr<SchemaResult>> AsyncGenericFlightServerBase::GetSchemaAsync(
    const ServerCallContext&, const FlightDescriptor&) {
  return arrow::Future<std::shared_ptr<SchemaResult>>::MakeFinished(
      Status::NotImplemented("NYI"));
}

arrow::Future<std::shared_ptr<AsyncResultStream>>
AsyncGenericFlightServerBase::DoActionAsync(const ServerCallContext&, const Action&) {
  return arrow::Future<std::shared_ptr<AsyncResultStream>>::MakeFinished(
      Status::NotImplemented("NYI"));
}

arrow::Future<std::vector<ActionType>> AsyncGenericFlightServerBase::ListActionsAsync(
    const ServerCallContext&) {
  return arrow::Future<std::vector<ActionType>>::MakeFinished(
      Status::NotImplemented("NYI"));
}

arrow::Future<> AsyncGenericFlightServerBase::DoExchangeAsync(
    const ServerCallContext&, std::shared_ptr<AsyncFlightMessageReader>,
    std::shared_ptr<AsyncFlightMessageWriter>) {
  return arrow::Future<>::MakeFinished(Status::NotImplemented("NYI"));
}

arrow::Future<std::shared_ptr<AsyncFlightDataStream>>
AsyncGenericFlightServerBase::DoGetAsync(const ServerCallContext&, const Ticket&) {
  return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(
      Status::NotImplemented("DoGetAsync is not implemented"));
}

std::shared_ptr<AsyncFlightDataListener>
AsyncGenericFlightServerBase::CreateDoPutListener(const ServerCallContext&) {
  // The default refuses uploads, a server that accepts them overrides this.
  return nullptr;
}

Status AsyncGenericFlightServerBase::Handshake(const ServerCallContext&,
                                               const std::string&, std::string*) {
  return Status::NotImplemented(
      "This service does not have an authentication mechanism enabled.");
}

Status AsyncGenericFlightServerBase::ValidateToken(const ServerCallContext&,
                                                   const std::string&, std::string*) {
  // Keep the transport-level (TLS) identity, ignore the token.
  return Status::OK();
}

}  // namespace arrow::flight
