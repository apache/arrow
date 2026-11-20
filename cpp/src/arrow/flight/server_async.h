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

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/flight/server.h"
#include "arrow/flight/types.h"
#include "arrow/util/future.h"
#include "arrow/util/macros.h"

namespace arrow::flight {

/// \brief a Flight server served by the async generic gRPC
/// callback service.
///
/// Derive from this class alone and implement the handlers you serve:
///
/// \code
/// class MyServer : public arrow::flight::AsyncGenericFlightServerBase {
///   arrow::Future<std::shared_ptr<arrow::flight::FlightInfo>> GetFlightInfoAsync(
///       const arrow::flight::ServerCallContext&,
///       const arrow::flight::FlightDescriptor&) override;
/// };
/// \endcode
///
/// Every handler returns a future: complete it from your own thread or pool
/// when the answer is ready, and no gRPC callback thread is ever held waiting
/// for one. Handlers that do run inline should still return promptly: blocking
/// a callback thread starves new connections.
///
/// All handlers must be non-blocking and return promptly.
/// Non overriding handlers will return `Status::NotImplemented` by default.
///
/// Threading: the transport has no thread or executor of its own. It chains
/// its continuation on every future you return (a handler's result) with the
/// default CallbackOptions, so the continuation runs on whichever thread
/// completes your future, or inline on the gRPC callback thread that called
/// you if the future is already complete when you return it. That
/// continuation starts the RPC's next operation: the next StartWrite() (a
/// unary response), the next StartRead(), or Finish(). Complete your futures
/// on the executor you want the RPC's I/O issued from.
///
/// Every request argument a handler receives (`FlightDescriptor`) is only
/// valid for the duration of that handler call: copy what you need before
/// completing the returned future from another thread. The
/// `ServerCallContext` handed to a handler is likewise the call's
/// own context and must not outlive it.
///
/// Authentication goes through Handshake()/ValidateToken(): a blocking
/// ServerAuthHandler in FlightServerOptions is not supported, and Init()
/// rejects it.
class ARROW_FLIGHT_EXPORT AsyncGenericFlightServerBase {
 public:
  AsyncGenericFlightServerBase();
  virtual ~AsyncGenericFlightServerBase();

  /// \brief Initialize the server listening at the given location.
  /// Must be called before any other method.
  /// \param options The server options to use for initialization.
  /// \return Status indicating success or failure of the initialization.
  Status Init(const FlightServerOptions& options);

  /// \brief Get the port that the Flight server is listening on.
  ///
  /// This method must only be called after Init(). Will return a non-positive
  /// value if no port exists (e.g. when listening on a domain socket).
  /// \return The port number the server is listening on, or a non-positive value.
  int port() const;

  /// \return The address the server is listening on.
  /// \warning Only valid after Init() has been called.
  Location location() const;

  /// \brief Set the server to stop on any of the given signal numbers.
  /// \warning Must be called before Serve().
  /// \param signals The signal numbers on which the server should shut down.
  /// \return Status indicating success or failure of setting the shutdown signals.
  Status SetShutdownOnSignals(const std::vector<int> signals);

  /// \brief Start serving. Blocks until the server shuts down, which happens
  /// when Shutdown() is called or a registered signal arrives.
  /// \return Status indicating success or failure of starting the server.
  Status Serve();

  /// \return The signal number that interrupted Serve(), or 0.
  /// \note Only valid after Serve() has returned.
  int GotSignal() const;

  /// \brief Shut down the server, blocking until in-flight RPCs finish.
  /// May be called from another thread or a signal handler while Serve()
  /// blocks, with an optional deadline.
  ///
  /// An RPC whose handler never completes its future is not finished, so this
  /// blocks until that future resolves: a handler that abandons its future
  /// (see the per-handler notes on the transport not waiting for it) needs a
  /// deadline here, or the future must be given up on before shutdown.
  /// \param[in] deadline is an optional time point by which the server should complete
  /// shutdown.
  /// \return Status indicating success or failure of the shutdown operation.
  /// \warning Call at most once.
  Status Shutdown(const std::chrono::system_clock::time_point* deadline = NULLPTR);

  /// \brief Block until the server shuts down. Does not respond to signals.
  ///
  /// Like Shutdown(), this does not return while a handler future on a live RPC
  /// is still unresolved.
  Status Wait();

  /// \brief Run `callback` once if the RPC behind `context` is cancelled: the
  /// push counterpart of ServerCallContext::is_cancelled(), to stop work a
  /// handler started instead of polling.
  ///
  /// The callback runs when the RPC is cancelled (by the client, a deadline, a
  /// network error, etc.), on the transport thread that observes it and after
  /// the transport has settled the RPC's status; or before this call returns,
  /// on the calling thread, if the RPC is already cancelled. It never runs if
  /// the RPC ends without being cancelled. It must not block.
  ///
  /// Call this while the RPC is live: during a handler call, or while a future
  /// the transport awaits from that handler is still pending.
  /// \param[in] context The context an AsyncGenericFlightServerBase handed to the
  ///   handler.
  /// \param[in] callback The function to run on cancellation.
  /// \return OK once the callback is registered (or has run); Invalid when
  ///   `context` comes from anywhere else.
  static Status AddCancelCallback(const ServerCallContext& context,
                                  std::function<void()> callback);

  /// \brief Asynchronously retrieve the schema and an access plan for the
  /// indicated descriptor.
  /// \see FlightServerBase::GetFlightInfo
  /// \param[in] context is the server call context.
  /// \param[in] request is the flight descriptor for which to retrieve the info.
  ///
  /// \return A future completed with the info, or with a non-OK status to
  /// fail the RPC. A `nullptr` info answers `KeyError("Flight not found")`, as
  /// a synchronous server that returns OK without setting the response does.
  virtual arrow::Future<std::shared_ptr<FlightInfo>> GetFlightInfoAsync(
      const ServerCallContext& context, const FlightDescriptor& request);

  /// \brief Asynchronously retrieve the current status of the target query.
  /// \param[in] context is the server call context.
  /// \param[in] request is the flight descriptor for which to retrieve the status.
  /// \see FlightServerBase::PollFlightInfo.
  /// \return A future completed with the info, or with a non-OK status to
  /// fail the RPC. A nullptr info answers `KeyError("Flight not found")`.
  virtual arrow::Future<std::shared_ptr<PollInfo>> PollFlightInfoAsync(
      const ServerCallContext& context, const FlightDescriptor& request);

  /// \brief Asynchronously retrieve the schema for the indicated descriptor.
  /// \param[in] context is the server call context.
  /// \param[in] request is the flight descriptor for which to retrieve the schema.
  /// \see FlightServerBase::GetSchema.
  /// \return A future completed with the schema, or with a non-OK status to
  /// fail the RPC.
  /// A `nullptr` schema answers `KeyError("Flight not found")`.
  virtual arrow::Future<std::shared_ptr<SchemaResult>> GetSchemaAsync(
      const ServerCallContext& context, const FlightDescriptor& request);

  /// \brief Handle the handshake protocol with the client.
  /// \param[in] context is the server call context.
  /// \param[in] request is the client's handshake message.
  /// \param[out] response is where the server writes its handshake response.
  /// \return Status indicating success or failure of the handshake.
  virtual Status Handshake(const ServerCallContext& context, const std::string& request,
                           std::string* response);

  /// \brief Validate the token sent in the `auth-token-bin` header of RPCs
  /// issued after a successful Handshake().
  /// \param[in] context is the server call context.
  /// \param[in] token is the authentication token sent by the client.
  /// \param[out] peer_identity is set to the authenticated identity on success.
  /// \return Status indicating success or failure of the token validation.
  virtual Status ValidateToken(const ServerCallContext& context, const std::string& token,
                               std::string* peer_identity);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace arrow::flight
