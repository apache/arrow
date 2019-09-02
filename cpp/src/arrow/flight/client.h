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

/// \brief Implementation of Flight RPC client using gRPC. API should be
// considered experimental for now

#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/status.h"

#include "arrow/flight/types.h"  // IWYU pragma: keep
#include "arrow/flight/visibility.h"

namespace arrow {

class MemoryPool;
class RecordBatch;
class Schema;

namespace flight {

class ClientAuthHandler;

/// \brief A duration type for Flight call timeouts.
typedef std::chrono::duration<double, std::chrono::seconds::period> TimeoutDuration;

/// \brief Hints to the underlying RPC layer for Arrow Flight calls.
class ARROW_FLIGHT_EXPORT FlightCallOptions {
 public:
  /// Create a default set of call options.
  FlightCallOptions();

  /// \brief An optional timeout for this call. Negative durations
  /// mean an implementation-defined default behavior will be used
  /// instead. This is the default value.
  TimeoutDuration timeout;
};

class ARROW_FLIGHT_EXPORT FlightClientOptions {
 public:
  /// \brief Root certificates to use for validating server
  /// certificates.
  std::string tls_root_certs;
  /// \brief Override the hostname checked by TLS. Use with caution.
  std::string override_hostname;
};

/// \brief A RecordBatchReader exposing Flight metadata and cancel
/// operations.
class ARROW_FLIGHT_EXPORT FlightStreamReader : public MetadataRecordBatchReader {
 public:
  /// \brief Try to cancel the call.
  virtual void Cancel() = 0;
};

// Silence warning
// "non dll-interface class RecordBatchReader used as base for dll-interface class"
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4275)
#endif

/// \brief A RecordBatchWriter that also allows sending
/// application-defined metadata via the Flight protocol.
class ARROW_FLIGHT_EXPORT FlightStreamWriter : public ipc::RecordBatchWriter {
 public:
  virtual Status WriteWithMetadata(const RecordBatch& batch,
                                   std::shared_ptr<Buffer> app_metadata) = 0;
  /// \brief Indicate that the application is done writing to this stream.
  ///
  /// The application may not write to this stream after calling
  /// this. This differs from closing the stream because this writer
  /// may represent only one half of a readable and writable stream.
  virtual Status DoneWriting() = 0;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

/// \brief A reader for application-specific metadata sent back to the
/// client during an upload.
class ARROW_FLIGHT_EXPORT FlightMetadataReader {
 public:
  virtual ~FlightMetadataReader();
  /// \brief Read a message from the server.
  virtual Status ReadMetadata(std::shared_ptr<Buffer>* out) = 0;
};

/// \brief Client class for Arrow Flight RPC services (gRPC-based).
/// API experimental for now
class ARROW_FLIGHT_EXPORT FlightClient {
 public:
  ~FlightClient();

  /// \brief Connect to an unauthenticated flight service
  /// \param[in] location the URI
  /// \param[out] client the created FlightClient
  /// \return Status OK status may not indicate that the connection was
  /// successful
  static Status Connect(const Location& location, std::unique_ptr<FlightClient>* client);

  /// \brief Connect to an unauthenticated flight service
  /// \param[in] location the URI
  /// \param[in] options Other options for setting up the client
  /// \param[out] client the created FlightClient
  /// \return Status OK status may not indicate that the connection was
  /// successful
  static Status Connect(const Location& location, const FlightClientOptions& options,
                        std::unique_ptr<FlightClient>* client);

  /// \brief Authenticate to the server using the given handler.
  /// \param[in] options Per-RPC options
  /// \param[in] auth_handler The authentication mechanism to use
  /// \return Status OK if the client authenticated successfully
  Status Authenticate(const FlightCallOptions& options,
                      std::unique_ptr<ClientAuthHandler> auth_handler);

  /// \brief Perform the indicated action, returning an iterator to the stream
  /// of results, if any
  /// \param[in] options Per-RPC options
  /// \param[in] action the action to be performed
  /// \param[out] results an iterator object for reading the returned results
  /// \return Status
  Status DoAction(const FlightCallOptions& options, const Action& action,
                  std::unique_ptr<ResultStream>* results);
  Status DoAction(const Action& action, std::unique_ptr<ResultStream>* results) {
    return DoAction({}, action, results);
  }

  /// \brief Retrieve a list of available Action types
  /// \param[in] options Per-RPC options
  /// \param[out] actions the available actions
  /// \return Status
  Status ListActions(const FlightCallOptions& options, std::vector<ActionType>* actions);
  Status ListActions(std::vector<ActionType>* actions) {
    return ListActions({}, actions);
  }

  /// \brief Request access plan for a single flight, which may be an existing
  /// dataset or a command to be executed
  /// \param[in] options Per-RPC options
  /// \param[in] descriptor the dataset request, whether a named dataset or
  /// command
  /// \param[out] info the FlightInfo describing where to access the dataset
  /// \return Status
  Status GetFlightInfo(const FlightCallOptions& options,
                       const FlightDescriptor& descriptor,
                       std::unique_ptr<FlightInfo>* info);
  Status GetFlightInfo(const FlightDescriptor& descriptor,
                       std::unique_ptr<FlightInfo>* info) {
    return GetFlightInfo({}, descriptor, info);
  }

  /// \brief Request schema for a single flight, which may be an existing
  /// dataset or a command to be executed
  /// \param[in] options Per-RPC options
  /// \param[in] descriptor the dataset request, whether a named dataset or
  /// command
  /// \param[out] schema_result the SchemaResult describing the dataset schema
  /// \return Status
  Status GetSchema(const FlightCallOptions& options, const FlightDescriptor& descriptor,
                   std::unique_ptr<SchemaResult>* schema_result);
  Status GetSchema(const FlightDescriptor& descriptor,
                   std::unique_ptr<SchemaResult>* schema_result) {
    return GetSchema({}, descriptor, schema_result);
  }

  /// \brief List all available flights known to the server
  /// \param[out] listing an iterator that returns a FlightInfo for each flight
  /// \return Status
  Status ListFlights(std::unique_ptr<FlightListing>* listing);

  /// \brief List available flights given indicated filter criteria
  /// \param[in] options Per-RPC options
  /// \param[in] criteria the filter criteria (opaque)
  /// \param[out] listing an iterator that returns a FlightInfo for each flight
  /// \return Status
  Status ListFlights(const FlightCallOptions& options, const Criteria& criteria,
                     std::unique_ptr<FlightListing>* listing);

  /// \brief Given a flight ticket and schema, request to be sent the
  /// stream. Returns record batch stream reader
  /// \param[in] options Per-RPC options
  /// \param[in] ticket The flight ticket to use
  /// \param[out] stream the returned RecordBatchReader
  /// \return Status
  Status DoGet(const FlightCallOptions& options, const Ticket& ticket,
               std::unique_ptr<FlightStreamReader>* stream);
  Status DoGet(const Ticket& ticket, std::unique_ptr<FlightStreamReader>* stream) {
    return DoGet({}, ticket, stream);
  }

  /// \brief Upload data to a Flight described by the given
  /// descriptor. The caller must call Close() on the returned stream
  /// once they are done writing.
  ///
  /// The reader and writer are linked; closing the writer will also
  /// close the reader. Use \a DoneWriting to only close the write
  /// side of the channel.
  ///
  /// \param[in] options Per-RPC options
  /// \param[in] descriptor the descriptor of the stream
  /// \param[in] schema the schema for the data to upload
  /// \param[out] stream a writer to write record batches to
  /// \param[out] reader a reader for application metadata from the server
  /// \return Status
  Status DoPut(const FlightCallOptions& options, const FlightDescriptor& descriptor,
               const std::shared_ptr<Schema>& schema,
               std::unique_ptr<FlightStreamWriter>* stream,
               std::unique_ptr<FlightMetadataReader>* reader);
  Status DoPut(const FlightDescriptor& descriptor, const std::shared_ptr<Schema>& schema,
               std::unique_ptr<FlightStreamWriter>* stream,
               std::unique_ptr<FlightMetadataReader>* reader) {
    return DoPut({}, descriptor, schema, stream, reader);
  }

 private:
  FlightClient();
  class FlightClientImpl;
  std::unique_ptr<FlightClientImpl> impl_;
};

}  // namespace flight
}  // namespace arrow
