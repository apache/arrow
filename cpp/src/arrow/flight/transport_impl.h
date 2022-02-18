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

// Internal (but not private) interface for implementing alternate
// transports in Flight.
//
// EXPERIMENTAL. Subject to change.

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

/// Internal, not user-visible type used for memory-efficient reads from gRPC
/// stream
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

/// \brief An transport-specific interface for reading/writing Arrow data.
///
/// New transports will implement this to read/write IPC payloads to
/// the underlying stream.
class ARROW_FLIGHT_EXPORT TransportDataStream {
 public:
  virtual ~TransportDataStream() = default;
  /// \brief Attemnpt to read the next FlightData message.
  ///
  /// \return success true if data was populated, false if there was
  ///   an error. For clients, the error can be retrieved from Finish.
  virtual bool ReadData(FlightData* data);
  /// \brief Attempt to write a FlightPayload.
  virtual Status WriteData(const FlightPayload& payload);
  /// \brief Attempt to write a non-data message.
  ///
  /// Only implemented for DoPut; mutually exclusive with Write(const
  /// FlightPayload&).
  virtual Status WritePutMetadata(const Buffer& payload);
  /// \brief Indicate that there are no more writes on this stream.
  ///
  /// This is only a hint for the underlying transport and may not
  /// actually do anything.
  virtual Status WritesDone();
};

/// \brief A transport-specific interface for reading/writing Arrow
///   data for a client.
class ARROW_FLIGHT_EXPORT ClientDataStream : public TransportDataStream {
 public:
  /// \brief Attempt to read a non-data message.
  ///
  /// Only implemented for DoPut; mutually exclusive with Read(FlightData*).
  virtual bool ReadPutMetadata(std::shared_ptr<Buffer>* out);
  /// \brief Finish the call, returning the final server status.
  ///
  /// Implies WritesDone().
  virtual Status Finish() = 0;
  /// \brief Attempt to cancel the call.
  ///
  /// This is only a hint and may not take effect immediately. The
  /// client should still finish the call with Finish() as usual.
  virtual void TryCancel() {}
  /// \brief Finish the call, combining client-side status with server status.
  Status Finish(Status st);
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

/// The implementation of the Flight service, which implements RPC
/// method handlers that work in terms of the generic interfaces
/// here.
///
/// Transport implementations should implement the necessary
/// interfaces and call methods of this service.
class ARROW_FLIGHT_EXPORT FlightServiceImpl {
 public:
  explicit FlightServiceImpl(FlightServerBase* base,
                             std::shared_ptr<MemoryManager> memory_manager)
      : service_(base), memory_manager_(std::move(memory_manager)) {}
  Status DoGet(const ServerCallContext& context, const Ticket& request,
               TransportDataStream* stream);
  Status DoPut(const ServerCallContext& context, TransportDataStream* stream);
  Status DoExchange(const ServerCallContext& context, TransportDataStream* stream);
  FlightServerBase* base() const { return service_; }

 private:
  FlightServerBase* service_;
  std::shared_ptr<MemoryManager> memory_manager_;
};

/// An implementation of a Flight server for a particular transport.
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
