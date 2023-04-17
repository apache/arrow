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

// Generic Flight I/O utilities.

#pragma once

#include "arrow/flight/protocol_internal.h"  // IWYU pragma: keep
#include "arrow/flight/transport.h"
#include "arrow/flight/types.h"
#include "arrow/util/macros.h"

namespace arrow {

class Schema;
class Status;

namespace ipc {
class Message;
}  // namespace ipc

namespace flight {
namespace pb = arrow::flight::protocol;
namespace internal {

/// \brief The header used for transmitting authentication/authorization data.
static constexpr char kAuthHeader[] = "authorization";

ARROW_FLIGHT_EXPORT
Status SchemaToString(const Schema& schema, std::string* out);

// These functions depend on protobuf types which are not exported in the Flight DLL.

Status FromProto(const pb::ActionType& pb_type, ActionType* type);
Status FromProto(const pb::Action& pb_action, Action* action);
Status FromProto(const pb::Result& pb_result, Result* result);
Status FromProto(const pb::Criteria& pb_criteria, Criteria* criteria);
Status FromProto(const pb::Location& pb_location, Location* location);
Status FromProto(const pb::Ticket& pb_ticket, Ticket* ticket);
Status FromProto(const pb::FlightData& pb_data, FlightDescriptor* descriptor,
                 std::unique_ptr<ipc::Message>* message);
Status FromProto(const pb::FlightDescriptor& pb_descr, FlightDescriptor* descr);
Status FromProto(const pb::FlightEndpoint& pb_endpoint, FlightEndpoint* endpoint);
Status FromProto(const pb::FlightInfo& pb_info, FlightInfo::Data* info);
Status FromProto(const pb::SchemaResult& pb_result, std::string* result);
Status FromProto(const pb::BasicAuth& pb_basic_auth, BasicAuth* info);

Status ToProto(const FlightDescriptor& descr, pb::FlightDescriptor* pb_descr);
Status ToProto(const FlightEndpoint& endpoint, pb::FlightEndpoint* pb_endpoint);
Status ToProto(const FlightInfo& info, pb::FlightInfo* pb_info);
Status ToProto(const ActionType& type, pb::ActionType* pb_type);
Status ToProto(const Action& action, pb::Action* pb_action);
Status ToProto(const Result& result, pb::Result* pb_result);
Status ToProto(const Criteria& criteria, pb::Criteria* pb_criteria);
Status ToProto(const SchemaResult& result, pb::SchemaResult* pb_result);
Status ToProto(const Ticket& ticket, pb::Ticket* pb_ticket);
Status ToProto(const BasicAuth& basic_auth, pb::BasicAuth* pb_basic_auth);

Status ToPayload(const FlightDescriptor& descr, std::shared_ptr<Buffer>* out);

// We want to reuse RecordBatchStreamReader's implementation while
// (1) Adapting it to the Flight message format
// (2) Allowing pure-metadata messages before data is sent
// (3) Reusing the reader implementation between DoGet and DoExchange.
// To do this, we wrap the transport-level reader in a peekable
// iterator.  The Flight reader can then peek at the message to
// determine whether it has application metadata or not, and pass the
// message to RecordBatchStreamReader as appropriate.
class PeekableFlightDataReader {
 public:
  explicit PeekableFlightDataReader(TransportDataStream* stream)
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

    if (!stream_->ReadData(&peek_)) {
      finished_ = true;
      valid_ = false;
    } else {
      valid_ = true;
    }
    return valid_;
  }

  internal::TransportDataStream* stream_;
  internal::FlightData peek_;
  bool finished_;
  bool valid_;
};

}  // namespace internal
}  // namespace flight
}  // namespace arrow
