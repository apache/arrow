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

#include <memory>
#include <string>

#include "arrow/flight/protocol-internal.h"  // IWYU pragma: keep
#include "arrow/flight/types.h"
#include "arrow/util/macros.h"

namespace grpc {

class Status;

}  // namespace grpc

namespace arrow {

class Schema;
class Status;

namespace pb = arrow::flight::protocol;

namespace ipc {

class Message;

}  // namespace ipc

namespace flight {

#define GRPC_RETURN_NOT_OK(expr)                          \
  do {                                                    \
    ::arrow::Status _s = (expr);                          \
    if (ARROW_PREDICT_FALSE(!_s.ok())) {                  \
      return ::arrow::flight::internal::ToGrpcStatus(_s); \
    }                                                     \
  } while (0)

#define GRPC_RETURN_NOT_GRPC_OK(expr)    \
  do {                                   \
    ::grpc::Status _s = (expr);          \
    if (ARROW_PREDICT_FALSE(!_s.ok())) { \
      return _s;                         \
    }                                    \
  } while (0)

namespace internal {

static const char* AUTH_HEADER = "auth-token-bin";

ARROW_FLIGHT_EXPORT
Status SchemaToString(const Schema& schema, std::string* out);

ARROW_FLIGHT_EXPORT
Status FromGrpcStatus(const grpc::Status& grpc_status);

ARROW_FLIGHT_EXPORT
grpc::Status ToGrpcStatus(const Status& arrow_status);

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

Status ToProto(const FlightDescriptor& descr, pb::FlightDescriptor* pb_descr);
Status ToProto(const FlightInfo& info, pb::FlightInfo* pb_info);
Status ToProto(const ActionType& type, pb::ActionType* pb_type);
Status ToProto(const Action& action, pb::Action* pb_action);
Status ToProto(const Result& result, pb::Result* pb_result);
void ToProto(const Ticket& ticket, pb::Ticket* pb_ticket);

}  // namespace internal
}  // namespace flight
}  // namespace arrow
