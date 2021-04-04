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
#include <utility>
#include <vector>

#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/variant.h"

#include "arrow/flight/types.h"  // IWYU pragma: keep
#include "arrow/flight/visibility.h"

namespace arrow {

class RecordBatch;
class Schema;

namespace flight {

class ClientAuthHandler;
class ClientMiddleware;
class ClientMiddlewareFactory;

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

  /// \brief IPC reader options, if applicable for the call.
  ipc::IpcReadOptions read_options;

  /// \brief IPC writer options, if applicable for the call.
  ipc::IpcWriteOptions write_options;

  /// \brief Headers for client to add to context.
  std::vector<std::pair<std::string, std::string>> headers;
};

/// \brief Indicate that the client attempted to write a message
///     larger than the soft limit set via write_size_limit_bytes.
class ARROW_FLIGHT_EXPORT FlightWriteSizeStatusDetail : public arrow::StatusDetail {
 public:
  explicit FlightWriteSizeStatusDetail(int64_t limit, int64_t actual)
      : limit_(limit), actual_(actual) {}
  const char* type_id() const override;
  std::string ToString() const override;
  int64_t limit() const { return limit_; }
  int64_t actual() const { return actual_; }

  /// \brief Extract this status detail from a status, or return
  ///     nullptr if the status doesn't contain this status detail.
  static std::shared_ptr<FlightWriteSizeStatusDetail> UnwrapStatus(
      const arrow::Status& status);

 private:
  int64_t limit_;
  int64_t actual_;
};

struct ARROW_FLIGHT_EXPORT FlightClientOptions {
  /// \brief Root certificates to use for validating server
  /// certificates.
  std::string tls_root_certs;
  /// \brief Override the hostname checked by TLS. Use with caution.
  std::string override_hostname;
  /// \brief The client certificate to use if using Mutual TLS
  std::string cert_chain;
  /// \brief The private key associated with the client certificate for Mutual TLS
  std::string private_key;
  /// \brief A list of client middleware to apply.
  std::vector<std::shared_ptr<ClientMiddlewareFactory>> middleware;
  /// \brief A soft limit on the number of bytes to write in a single
  ///     batch when sending Arrow data to a server.
  ///
  /// Used to help limit server memory consumption. Only enabled if
  /// positive. When enabled, FlightStreamWriter.Write* may yield a
  /// IOError with error detail FlightWriteSizeStatusDetail.
  int64_t write_size_limit_bytes = 0;

  /// \brief Generic connection options, passed to the underlying
  ///     transport; interpretation is implementation-dependent.
  std::vector<std::pair<std::string, util::Variant<int, std::string>>> generic_options;

  /// \brief Use TLS without validating the server certificate. Use with caution.
  bool disable_server_verification = false;

  /// \brief Get default options.
  static FlightClientOptions Defaults();
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
class ARROW_FLIGHT_EXPORT FlightStreamWriter : public MetadataRecordBatchWriter {
 public:
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

  /// \brief Authenticate to the server using basic HTTP style authentication.
  /// \param[in] options Per-RPC options
  /// \param[in] username Username to use
  /// \param[in] password Password to use
  /// \return Arrow result with bearer token and status OK if client authenticated
  /// sucessfully
  arrow::Result<std::pair<std::string, std::string>> AuthenticateBasicToken(
      const FlightCallOptions& options, const std::string& username,
      const std::string& password);

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

  Status DoExchange(const FlightCallOptions& options, const FlightDescriptor& descriptor,
                    std::unique_ptr<FlightStreamWriter>* writer,
                    std::unique_ptr<FlightStreamReader>* reader);
  Status DoExchange(const FlightDescriptor& descriptor,
                    std::unique_ptr<FlightStreamWriter>* writer,
                    std::unique_ptr<FlightStreamReader>* reader) {
    return DoExchange({}, descriptor, writer, reader);
  }

 private:
  FlightClient();
  class FlightClientImpl;
  std::unique_ptr<FlightClientImpl> impl_;
};

}  // namespace flight
}  // namespace arrow
