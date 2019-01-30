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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/util/visibility.h"

#include "arrow/flight/types.h"
#include "arrow/ipc/dictionary.h"

namespace arrow {

class MemoryPool;
class RecordBatchReader;
class Status;

namespace ipc {
namespace internal {

struct IpcPayload;

}  // namespace internal
}  // namespace ipc

namespace io {

class OutputStream;

}  // namespace io

namespace flight {

/// \brief Interface that produces a sequence of IPC payloads to be sent in
/// FlightData protobuf messages
class ARROW_EXPORT FlightDataStream {
 public:
  virtual ~FlightDataStream() = default;

  // When the stream starts, send the schema.
  virtual std::shared_ptr<Schema> schema() = 0;

  // When the stream is completed, the last payload written will have null
  // metadata
  virtual Status Next(ipc::internal::IpcPayload* payload) = 0;
};

/// \brief A basic implementation of FlightDataStream that will provide
/// a sequence of FlightData messages to be written to a gRPC stream
/// \param[in] reader produces a sequence of record batches
class ARROW_EXPORT RecordBatchStream : public FlightDataStream {
 public:
  explicit RecordBatchStream(const std::shared_ptr<RecordBatchReader>& reader);

  std::shared_ptr<Schema> schema() override;
  Status Next(ipc::internal::IpcPayload* payload) override;

 private:
  MemoryPool* pool_;
  std::shared_ptr<RecordBatchReader> reader_;
};

/// \brief Skeleton RPC server implementation which can be used to create
/// custom servers by implementing its abstract methods
class ARROW_EXPORT FlightServerBase {
 public:
  FlightServerBase();
  virtual ~FlightServerBase();

  /// \brief Run an insecure server on localhost at the indicated port. Block
  /// until server is shut down or otherwise terminates
  /// \param[in] port
  /// \return Status
  void Run(int port);

  /// \brief Shut down the server. Can be called from signal handler or another
  /// thread while Run blocks
  ///
  /// TODO(wesm): Shutdown with deadline
  void Shutdown();

  // Implement these methods to create your own server. The default
  // implementations will return a not-implemented result to the client

  /// \brief Retrieve a list of available fields given an optional opaque
  /// criteria
  /// \param[in] criteria may be null
  /// \param[out] listings the returned listings iterator
  /// \return Status
  virtual Status ListFlights(const Criteria* criteria,
                             std::unique_ptr<FlightListing>* listings);

  /// \brief Retrieve the schema and an access plan for the indicated
  /// descriptor
  /// \param[in] request may be null
  /// \param[out] info the returned flight info provider
  /// \return Status
  virtual Status GetFlightInfo(const FlightDescriptor& request,
                               std::unique_ptr<FlightInfo>* info);

  /// \brief Get a stream of IPC payloads to put on the wire
  /// \param[in] request an opaque ticket
  /// \param[out] stream the returned stream provider
  /// \return Status
  virtual Status DoGet(const Ticket& request, std::unique_ptr<FlightDataStream>* stream);

  // virtual Status DoPut(std::unique_ptr<FlightMessageReader>* reader) = 0;

  /// \brief Execute an action, return stream of zero or more results
  /// \param[in] action the action to execute, with type and body
  /// \param[out] result the result iterator
  /// \return Status
  virtual Status DoAction(const Action& action, std::unique_ptr<ResultStream>* result);

  /// \brief Retrieve the list of available actions
  /// \param[out] actions a vector of available action types
  /// \return Status
  virtual Status ListActions(std::vector<ActionType>* actions);

 private:
  struct FlightServerBaseImpl;
  std::unique_ptr<FlightServerBaseImpl> impl_;
};

}  // namespace flight
}  // namespace arrow
