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

/// \file
/// Internal (but not private) interface for implementing
/// alternate network transports in Flight.
///
/// \warning EXPERIMENTAL. Subject to change.
///
/// To implement a transport, implement ServerTransportImpl and
/// ClientTransportImpl, and register the desired URI schemes with
/// TransportImplRegistry. Flight takes care of most of the per-RPC
/// details; transports only handle connections and providing a I/O
/// stream implementation (TransportDataStream).
///
/// On the server side:
///
/// 1. Applications subclass FlightServerBase and override RPC handlers.
/// 2. FlightServerBase::Init will look up and create a ServerTransportImpl
///    based on the scheme of the Location given to it.
/// 3. The ServerTransportImpl will start the actual server. (For instance,
///    for gRPC, it creates a gRPC server and registers a gRPC service.)
///    That server will handle connections.
/// 4. Incoming calls to the underlying server get forwarded to
///    FlightServiceImpl, which implements the actual RPC handler using the
///    interfaces here. Transports implement TransportDataStream to handle
///    any I/O the RPC handler needs to do.
/// 5. FlightServiceImpl calls FlightServerBase for the actual application
///    logic.
///
/// On the client side:
///
/// 1. Applications create a FlightClient with a Location.
/// 2. FlightClient will look up and create a ClientTransportImpl based on
///    the scheme of the Location given to it.
/// 3. When calling a method on FlightClient, FlightClient will delegate to
///    the ClientTransportImpl. There is some indirection, e.g. for DoGet,
///    FlightClient only requests that the ClientTransportImpl start the
///    call and provide it with an I/O stream. The "Flight implementation"
///    itself still lives in FlightClient.

#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/flight/type_fwd.h"
#include "arrow/flight/visibility.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace ipc {
class Message;
}
namespace flight {
namespace internal {

/// Internal, not user-visible type used for memory-efficient reads
struct FlightData {
  /// Used only for puts, may be null
  std::unique_ptr<FlightDescriptor> descriptor;

  /// Non-length-prefixed Message header as described in format/Message.fbs
  std::shared_ptr<Buffer> metadata;

  /// Application-defined metadata
  std::shared_ptr<Buffer> app_metadata;

  /// Message body
  std::shared_ptr<Buffer> body;

  /// Open IPC message from the metadata and body
  ::arrow::Result<std::unique_ptr<ipc::Message>> OpenMessage();
};

/// \brief A transport-specific interface for reading/writing Arrow data.
///
/// New transports will implement this to read/write IPC payloads to
/// the underlying stream.
class ARROW_FLIGHT_EXPORT TransportDataStream {
 public:
  virtual ~TransportDataStream() = default;
  /// \brief Attempt to read the next FlightData message.
  ///
  /// \return success true if data was populated, false if there was
  ///   an error. For clients, the error can be retrieved from
  ///   Finish(Status).
  virtual bool ReadData(FlightData* data);
  /// \brief Attempt to write a FlightPayload.
  virtual Status WriteData(const FlightPayload& payload);
  /// \brief Indicate that there are no more writes on this stream.
  ///
  /// This is only a hint for the underlying transport and may not
  /// actually do anything.
  virtual Status WritesDone();
};

/// \brief A transport-specific interface for reading/writing Arrow
///   data for a server.
class ARROW_FLIGHT_EXPORT ServerDataStream : public TransportDataStream {
 public:
  /// \brief Attempt to write a non-data message.
  ///
  /// Only implemented for DoPut; mutually exclusive with
  /// WriteData(const FlightPayload&).
  virtual Status WritePutMetadata(const Buffer& payload);
};

/// \brief A transport-specific interface for reading/writing Arrow
///   data for a client.
class ARROW_FLIGHT_EXPORT ClientDataStream : public TransportDataStream {
 public:
  /// \brief Attempt to read a non-data message.
  ///
  /// Only implemented for DoPut; mutually exclusive with
  /// ReadData(FlightData*).
  virtual bool ReadPutMetadata(std::shared_ptr<Buffer>* out);
  /// \brief Attempt to cancel the call.
  ///
  /// This is only a hint and may not take effect immediately. The
  /// client should still finish the call with Finish() as usual.
  virtual void TryCancel() {}
  /// \brief Finish the call, reporting the server-sent status and/or
  ///   any client-side errors as appropriate.
  ///
  /// Implies WritesDone().
  ///
  /// \param[in] st A client-side status to combine with the
  ///   server-side error. That is, if an error occurs on the
  ///   client-side, call Finish(Status) to finish the server-side
  ///   call, get the server-side status, and merge the statuses
  ///   together so context is not lost.
  Status Finish(Status st);

 protected:
  /// \brief Finish the call, returning the final server status.
  ///
  /// For implementors: should imply WritesDone() (even if it does not
  /// directly call it).
  ///
  /// Implies WritesDone().
  virtual Status Finish() = 0;
};

/// An implementation of a Flight client for a particular transport.
///
/// Transports should override the methods they are capable of
/// supporting. The default method implementations return an error.
class ARROW_FLIGHT_EXPORT ClientTransportImpl {
 public:
  virtual ~ClientTransportImpl() = default;

  /// Initialize the client.
  virtual Status Init(const FlightClientOptions& options, const Location& location,
                      const arrow::internal::Uri& uri) = 0;
  /// Close the client. Once this returns, the client is no longer usable.
  virtual Status Close() = 0;

  virtual Status Authenticate(const FlightCallOptions& options,
                              std::unique_ptr<ClientAuthHandler> auth_handler);
  virtual arrow::Result<std::pair<std::string, std::string>> AuthenticateBasicToken(
      const FlightCallOptions& options, const std::string& username,
      const std::string& password);
  virtual Status DoAction(const FlightCallOptions& options, const Action& action,
                          std::unique_ptr<ResultStream>* results);
  virtual Status ListActions(const FlightCallOptions& options,
                             std::vector<ActionType>* actions);
  virtual Status GetFlightInfo(const FlightCallOptions& options,
                               const FlightDescriptor& descriptor,
                               std::unique_ptr<FlightInfo>* info);
  virtual Status GetSchema(const FlightCallOptions& options,
                           const FlightDescriptor& descriptor,
                           std::unique_ptr<SchemaResult>* schema_result);
  virtual Status ListFlights(const FlightCallOptions& options, const Criteria& criteria,
                             std::unique_ptr<FlightListing>* listing);
  virtual Status DoGet(const FlightCallOptions& options, const Ticket& ticket,
                       std::unique_ptr<ClientDataStream>* stream);
  virtual Status DoPut(const FlightCallOptions& options,
                       std::unique_ptr<ClientDataStream>* stream);
  virtual Status DoExchange(const FlightCallOptions& options,
                            std::unique_ptr<ClientDataStream>* stream);
};

/// The implementation of the Flight service.
///
/// Transports implement the server and handle connections. Calls are
/// forwarded to this server, which implements method handlers that
/// work in terms of the generic interfaces above. This server
/// forwards calls to the underlying FlightServerBase instance which
/// contains the application RPC method handlers.
///
/// Transport implementations should implement the necessary
/// interfaces and call methods of this service.
class ARROW_FLIGHT_EXPORT FlightServiceImpl {
 public:
  explicit FlightServiceImpl(FlightServerBase* base,
                             std::shared_ptr<MemoryManager> memory_manager)
      : base_(base), memory_manager_(std::move(memory_manager)) {}
  /// \brief Implement DoGet in terms of a transport-level stream.
  ///
  /// \param[in] context The server context.
  /// \param[in] request The request payload.
  /// \param[in] stream The transport-specific data stream
  ///   implementation. Must implement WriteData(const
  ///   FlightPayload&).
  Status DoGet(const ServerCallContext& context, const Ticket& request,
               ServerDataStream* stream);
  /// \brief Implement DoPut in terms of a transport-level stream.
  ///
  /// \param[in] context The server context.
  /// \param[in] stream The transport-specific data stream
  ///   implementation. Must implement ReadData(FlightData*)
  ///   and WritePutMetadata(const Buffer&).
  Status DoPut(const ServerCallContext& context, ServerDataStream* stream);
  /// \brief Implement DoExchange in terms of a transport-level stream.
  ///
  /// \param[in] context The server context.
  /// \param[in] stream The transport-specific data stream
  ///   implementation. Must implement ReadData(FlightData*)
  ///   and WriteData(const FlightPayload&).
  Status DoExchange(const ServerCallContext& context, ServerDataStream* stream);
  FlightServerBase* base() const { return base_; }

 private:
  FlightServerBase* base_;
  std::shared_ptr<MemoryManager> memory_manager_;
};

/// \brief An implementation of a Flight server for a particular
/// transport.
///
/// Used by FlightServerBase to manage the server lifecycle.
class ARROW_FLIGHT_EXPORT ServerTransportImpl {
 public:
  virtual ~ServerTransportImpl() = default;

  /// Initialize the server.
  virtual Status Init(const FlightServerOptions& options, const arrow::internal::Uri& uri,
                      FlightServiceImpl* service) = 0;
  /// Shutdown the server. Once this returns, the server is no longer listening.
  virtual Status Shutdown() = 0;
  /// Shutdown the server. Once this returns, the server is no longer listening.
  virtual Status Shutdown(const std::chrono::system_clock::time_point& deadline) = 0;
  /// Wait for the server to shutdown. Once this returns, the server is no longer
  /// listening.
  virtual Status Wait() = 0;
  /// Get the address the server is listening on, else an empty Location.
  virtual Location location() const = 0;
};

/// A registry of transport implementations.
class ARROW_FLIGHT_EXPORT TransportImplRegistry {
 public:
  using ClientFactory =
      std::function<arrow::Result<std::unique_ptr<ClientTransportImpl>>()>;
  using ServerFactory =
      std::function<arrow::Result<std::unique_ptr<ServerTransportImpl>>()>;

  TransportImplRegistry();
  ~TransportImplRegistry();

  arrow::Result<std::unique_ptr<ClientTransportImpl>> MakeClientImpl(
      const std::string& scheme);
  arrow::Result<std::unique_ptr<ServerTransportImpl>> MakeServerImpl(
      const std::string& scheme);

  Status RegisterClient(const std::string& scheme, ClientFactory factory);
  Status RegisterServer(const std::string& scheme, ServerFactory factory);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

/// \brief Get the registry of transport implementations.
ARROW_FLIGHT_EXPORT
TransportImplRegistry* GetDefaultTransportImplRegistry();

}  // namespace internal
}  // namespace flight
}  // namespace arrow
