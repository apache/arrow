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

#include "arrow/ipc/writer.h"
#include "arrow/status.h"

#include "arrow/flight/types.h"  // IWYU pragma: keep
#include "arrow/flight/visibility.h"

namespace arrow {

class MemoryPool;
class RecordBatch;
class RecordBatchReader;
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
               std::unique_ptr<RecordBatchReader>* stream);
  Status DoGet(const Ticket& ticket, std::unique_ptr<RecordBatchReader>* stream) {
    return DoGet({}, ticket, stream);
  }

  /// \brief Upload data to a Flight described by the given
  /// descriptor. The caller must call Close() on the returned stream
  /// once they are done writing.
  /// \param[in] options Per-RPC options
  /// \param[in] descriptor the descriptor of the stream
  /// \param[in] schema the schema for the data to upload
  /// \param[out] stream a writer to write record batches to
  /// \return Status
  Status DoPut(const FlightCallOptions& options, const FlightDescriptor& descriptor,
               const std::shared_ptr<Schema>& schema,
               std::unique_ptr<ipc::RecordBatchWriter>* stream);
  Status DoPut(const FlightDescriptor& descriptor, const std::shared_ptr<Schema>& schema,
               std::unique_ptr<ipc::RecordBatchWriter>* stream) {
    return DoPut({}, descriptor, schema, stream);
  }

 private:
  FlightClient();
  class FlightClientImpl;
  std::unique_ptr<FlightClientImpl> impl_;
};

}  // namespace flight
}  // namespace arrow
