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

#include <memory>

#include "arrow/flight/server_async.h"
#include "arrow/flight/transport/grpc/async_grpc_compat.h"
#include "arrow/flight/transport/grpc/grpc_server_internal.h"

namespace arrow::flight::transport::grpc {

#if ARROW_FLIGHT_HAS_ASYNC_SERVER

using HandshakeFn =
    GrpcServerCallContextHelper<::grpc::CallbackServerContext>::HandshakeFn;

/// \brief Generic callback service dispatching Flight methods to reactors.
///
/// Constructed with the AsyncGenericFlightServerBase whose handlers are served.
/// The server's middleware runs for every call, through the shared context
/// helper, which also authenticates every method except Handshake. Handshake,
/// DoGet, DoPut and the synchronous Flight methods (GetFlightInfo, GetSchema,
/// PollFlightInfo, ListActions, DoAction, ListFlights, DoExchange) have a
/// reactor. Every other method is answered UNIMPLEMENTED.
class ARROW_FLIGHT_EXPORT AsyncGenericFlightService
    : public ::grpc::CallbackGenericService {
 public:
  /// \param async_base is the async server whose handlers are served, it must
  /// outlive the service.
  /// \param helper runs middleware and auth and builds the call context.
  /// \param handshake_handler is the server's Handshake hook, an empty hook
  /// means the server has no authentication mechanism.
  AsyncGenericFlightService(
      AsyncGenericFlightServerBase* async_base,
      std::shared_ptr<GrpcServerCallContextHelper<::grpc::CallbackServerContext>> helper,
      HandshakeFn handshake_handler = {});

  /// \brief Creates a reactor for the incoming RPC.
  /// \param context is the gRPC callback server context for the RPC.
  /// \return a new reactor handling the RPC, or an Unimplemented reactor if the method is
  /// unknown.
  ::grpc::ServerGenericBidiReactor* CreateReactor(
      ::grpc::GenericCallbackServerContext* context) override;

 private:
  AsyncGenericFlightServerBase* base_;
  std::shared_ptr<GrpcServerCallContextHelper<::grpc::CallbackServerContext>> helper_;
  /// The server class's Handshake hook. Empty when the server has none.
  HandshakeFn handshake_handler_;
};

#endif  // ARROW_FLIGHT_HAS_ASYNC_SERVER

}  // namespace arrow::flight::transport::grpc
