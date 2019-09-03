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

// Interfaces to use for defining Flight RPC servers. API should be considered
// experimental for now

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/flight/server_auth.h"
#include "arrow/flight/types.h"       // IWYU pragma: keep
#include "arrow/flight/visibility.h"  // IWYU pragma: keep
#include "arrow/ipc/dictionary.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"

namespace arrow {

class MemoryPool;
class Schema;
class Status;

namespace flight {

/// \brief Interface that produces a sequence of IPC payloads to be sent in
/// FlightData protobuf messages
class ARROW_FLIGHT_EXPORT FlightDataStream {
 public:
  virtual ~FlightDataStream();

  virtual std::shared_ptr<Schema> schema() = 0;

  /// \brief Compute FlightPayload containing serialized RecordBatch schema
  virtual Status GetSchemaPayload(FlightPayload* payload) = 0;

  // When the stream is completed, the last payload written will have null
  // metadata
  virtual Status Next(FlightPayload* payload) = 0;
};

/// \brief A basic implementation of FlightDataStream that will provide
/// a sequence of FlightData messages to be written to a gRPC stream
class ARROW_FLIGHT_EXPORT RecordBatchStream : public FlightDataStream {
 public:
  /// \param[in] reader produces a sequence of record batches
  /// \param[in,out] pool a MemoryPool to use for allocations
  explicit RecordBatchStream(const std::shared_ptr<RecordBatchReader>& reader,
                             MemoryPool* pool = default_memory_pool());
  ~RecordBatchStream() override;

  std::shared_ptr<Schema> schema() override;
  Status GetSchemaPayload(FlightPayload* payload) override;
  Status Next(FlightPayload* payload) override;

 private:
  class RecordBatchStreamImpl;
  std::unique_ptr<RecordBatchStreamImpl> impl_;
};

/// \brief A reader for IPC payloads uploaded by a client. Also allows
/// reading application-defined metadata via the Flight protocol.
class ARROW_FLIGHT_EXPORT FlightMessageReader : public MetadataRecordBatchReader {
 public:
  /// \brief Get the descriptor for this upload.
  virtual const FlightDescriptor& descriptor() const = 0;
};

/// \brief A writer for application-specific metadata sent back to the
/// client during an upload.
class ARROW_FLIGHT_EXPORT FlightMetadataWriter {
 public:
  virtual ~FlightMetadataWriter();
  /// \brief Send a message to the client.
  virtual Status WriteMetadata(const Buffer& app_metadata) = 0;
};

/// \brief Call state/contextual data.
class ARROW_FLIGHT_EXPORT ServerCallContext {
 public:
  virtual ~ServerCallContext() = default;
  /// \brief The name of the authenticated peer (may be the empty string)
  virtual const std::string& peer_identity() const = 0;
};

class ARROW_FLIGHT_EXPORT FlightServerOptions {
 public:
  explicit FlightServerOptions(const Location& location_);

  Location location;
  std::unique_ptr<ServerAuthHandler> auth_handler;
  std::vector<CertKeyPair> tls_certificates;
  /// \brief A Flight implementation-specific callback to customize
  /// transport-specific options.
  /// Not guaranteed to be called. The type of the parameter is
  /// specific to the Flight implementation. Users should take care to
  /// link to the same transport implementation as Flight to avoid
  /// runtime problems.
  std::function<void(void*)> builder_hook;
};

/// \brief Skeleton RPC server implementation which can be used to create
/// custom servers by implementing its abstract methods
class ARROW_FLIGHT_EXPORT FlightServerBase {
 public:
  FlightServerBase();
  virtual ~FlightServerBase();

  // Lifecycle methods.

  /// \brief Initialize a Flight server listening at the given location.
  /// This method must be called before any other method.
  /// \param[in] options The configuration for this server.
  Status Init(FlightServerOptions& options);

  /// \brief Get the port that the Flight server is listening on.
  /// This method must only be called after Init().  Will return a
  /// non-positive value if no port exists (e.g. when listening on a
  /// domain socket).
  int port() const;

  /// \brief Set the server to stop when receiving any of the given signal
  /// numbers.
  /// This method must be called before Serve().
  Status SetShutdownOnSignals(const std::vector<int> sigs);

  /// \brief Start serving.
  /// This method blocks until either Shutdown() is called or one of the signals
  /// registered in SetShutdownOnSignals() is received.
  Status Serve();

  /// \brief Query whether Serve() was interrupted by a signal.
  /// This method must be called after Serve() has returned.
  ///
  /// \return int the signal number that interrupted Serve(), if any, otherwise 0
  int GotSignal() const;

  /// \brief Shut down the server. Can be called from signal handler or another
  /// thread while Serve() blocks.
  ///
  /// TODO(wesm): Shutdown with deadline
  Status Shutdown();

  // Implement these methods to create your own server. The default
  // implementations will return a not-implemented result to the client

  /// \brief Retrieve a list of available fields given an optional opaque
  /// criteria
  /// \param[in] context The call context.
  /// \param[in] criteria may be null
  /// \param[out] listings the returned listings iterator
  /// \return Status
  virtual Status ListFlights(const ServerCallContext& context, const Criteria* criteria,
                             std::unique_ptr<FlightListing>* listings);

  /// \brief Retrieve the schema and an access plan for the indicated
  /// descriptor
  /// \param[in] context The call context.
  /// \param[in] request may be null
  /// \param[out] info the returned flight info provider
  /// \return Status
  virtual Status GetFlightInfo(const ServerCallContext& context,
                               const FlightDescriptor& request,
                               std::unique_ptr<FlightInfo>* info);

  /// \brief Retrieve the schema for the indicated descriptor
  /// \param[in] context The call context.
  /// \param[in] request may be null
  /// \param[out] schema the returned flight schema provider
  /// \return Status
  virtual Status GetSchema(const ServerCallContext& context,
                           const FlightDescriptor& request,
                           std::unique_ptr<SchemaResult>* schema);

  /// \brief Get a stream of IPC payloads to put on the wire
  /// \param[in] context The call context.
  /// \param[in] request an opaque ticket
  /// \param[out] stream the returned stream provider
  /// \return Status
  virtual Status DoGet(const ServerCallContext& context, const Ticket& request,
                       std::unique_ptr<FlightDataStream>* stream);

  /// \brief Process a stream of IPC payloads sent from a client
  /// \param[in] context The call context.
  /// \param[in] reader a sequence of uploaded record batches
  /// \param[in] writer send metadata back to the client
  /// \return Status
  virtual Status DoPut(const ServerCallContext& context,
                       std::unique_ptr<FlightMessageReader> reader,
                       std::unique_ptr<FlightMetadataWriter> writer);

  /// \brief Execute an action, return stream of zero or more results
  /// \param[in] context The call context.
  /// \param[in] action the action to execute, with type and body
  /// \param[out] result the result iterator
  /// \return Status
  virtual Status DoAction(const ServerCallContext& context, const Action& action,
                          std::unique_ptr<ResultStream>* result);

  /// \brief Retrieve the list of available actions
  /// \param[in] context The call context.
  /// \param[out] actions a vector of available action types
  /// \return Status
  virtual Status ListActions(const ServerCallContext& context,
                             std::vector<ActionType>* actions);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace flight
}  // namespace arrow
