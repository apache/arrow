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

// (De)serialization utilities that hook into gRPC, efficiently
// handling Arrow-encoded data in a gRPC call.

#pragma once

#include <memory>

#include "arrow/flight/internal.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/message.h"
#include "arrow/result.h"

namespace arrow {

class Buffer;

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

/// Write Flight message on gRPC stream with zero-copy optimizations.
// Returns Invalid if the payload is ill-formed
// Returns IOError if gRPC did not write the message (note this is not
// necessarily an error - the client may simply have gone away)
Status WritePayload(const FlightPayload& payload,
                    grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>* writer);
Status WritePayload(const FlightPayload& payload,
                    grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>* writer);
Status WritePayload(const FlightPayload& payload,
                    grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* writer);
Status WritePayload(const FlightPayload& payload,
                    grpc::ServerWriter<pb::FlightData>* writer);

/// Read Flight message from gRPC stream with zero-copy optimizations.
/// True is returned on success, false if stream ended.
bool ReadPayload(grpc::ClientReader<pb::FlightData>* reader, FlightData* data);
bool ReadPayload(grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>* reader,
                 FlightData* data);
bool ReadPayload(grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader,
                 FlightData* data);
bool ReadPayload(grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* reader,
                 FlightData* data);
// Overload to make genericity easier in DoPutPayloadWriter
bool ReadPayload(grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>* reader,
                 pb::PutResult* data);

// We want to reuse RecordBatchStreamReader's implementation while
// (1) Adapting it to the Flight message format
// (2) Allowing pure-metadata messages before data is sent
// (3) Reusing the reader implementation between DoGet and DoExchange.
// To do this, we wrap the gRPC reader in a peekable iterator.
// The Flight reader can then peek at the message to determine whether
// it has application metadata or not, and pass the message to
// RecordBatchStreamReader as appropriate.
template <typename ReaderPtr>
class PeekableFlightDataReader {
 public:
  explicit PeekableFlightDataReader(ReaderPtr stream)
      : stream_(stream), peek_(), finished_(false), valid_(false) {}

  void Peek(internal::FlightData** out) {
    *out = nullptr;
    if (finished_) {
      return;
    }
    if (EnsurePeek()) {
      *out = &peek_;
    }
  }

  void Next(internal::FlightData** out) {
    Peek(out);
    valid_ = false;
  }

  /// \brief Peek() until the first data message.
  ///
  /// After this is called, either this will return \a false, or the
  /// next result of \a Peek and \a Next will contain Arrow data.
  bool SkipToData() {
    FlightData* data;
    while (true) {
      Peek(&data);
      if (!data) {
        return false;
      }
      if (data->metadata) {
        return true;
      }
      Next(&data);
    }
  }

 private:
  bool EnsurePeek() {
    if (finished_ || valid_) {
      return valid_;
    }

    if (!internal::ReadPayload(&*stream_, &peek_)) {
      finished_ = true;
      valid_ = false;
    } else {
      valid_ = true;
    }
    return valid_;
  }

  ReaderPtr stream_;
  internal::FlightData peek_;
  bool finished_;
  bool valid_;
};

}  // namespace internal
}  // namespace flight
}  // namespace arrow
