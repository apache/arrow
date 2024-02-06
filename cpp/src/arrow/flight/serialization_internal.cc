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

#include "arrow/flight/serialization_internal.h"

#include <memory>
#include <string>

#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace flight {
namespace internal {

// Timestamp

Status FromProto(const google::protobuf::Timestamp& pb_timestamp, Timestamp* timestamp) {
  const auto seconds = std::chrono::seconds{pb_timestamp.seconds()};
  const auto nanoseconds = std::chrono::nanoseconds{pb_timestamp.nanos()};
  const auto duration =
      std::chrono::duration_cast<Timestamp::duration>(seconds + nanoseconds);
  *timestamp = Timestamp(duration);
  return Status::OK();
}

Status ToProto(const Timestamp& timestamp, google::protobuf::Timestamp* pb_timestamp) {
  const auto since_epoch = timestamp.time_since_epoch();
  const auto since_epoch_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(since_epoch).count();
  pb_timestamp->set_seconds(since_epoch_ns / std::nano::den);
  pb_timestamp->set_nanos(since_epoch_ns % std::nano::den);
  return Status::OK();
}

// ActionType

Status FromProto(const pb::ActionType& pb_type, ActionType* type) {
  type->type = pb_type.type();
  type->description = pb_type.description();
  return Status::OK();
}

Status ToProto(const ActionType& type, pb::ActionType* pb_type) {
  pb_type->set_type(type.type);
  pb_type->set_description(type.description);
  return Status::OK();
}

// Action

Status FromProto(const pb::Action& pb_action, Action* action) {
  action->type = pb_action.type();
  action->body = Buffer::FromString(pb_action.body());
  return Status::OK();
}

Status ToProto(const Action& action, pb::Action* pb_action) {
  pb_action->set_type(action.type);
  if (action.body) {
    pb_action->set_body(action.body->ToString());
  }
  return Status::OK();
}

// Result (of an Action)

Status FromProto(const pb::Result& pb_result, Result* result) {
  // ARROW-3250; can avoid copy. Can also write custom deserializer if it
  // becomes an issue
  result->body = Buffer::FromString(pb_result.body());
  return Status::OK();
}

Status ToProto(const Result& result, pb::Result* pb_result) {
  pb_result->set_body(result.body->ToString());
  return Status::OK();
}

// CancelFlightInfoResult

Status FromProto(const pb::CancelFlightInfoResult& pb_result,
                 CancelFlightInfoResult* result) {
  result->status = static_cast<CancelStatus>(pb_result.status());
  return Status::OK();
}

Status ToProto(const CancelFlightInfoResult& result,
               pb::CancelFlightInfoResult* pb_result) {
  pb_result->set_status(static_cast<protocol::CancelStatus>(result.status));
  return Status::OK();
}

// Criteria

Status FromProto(const pb::Criteria& pb_criteria, Criteria* criteria) {
  criteria->expression = pb_criteria.expression();
  return Status::OK();
}
Status ToProto(const Criteria& criteria, pb::Criteria* pb_criteria) {
  pb_criteria->set_expression(criteria.expression);
  return Status::OK();
}

// Location

Status FromProto(const pb::Location& pb_location, Location* location) {
  return Location::Parse(pb_location.uri()).Value(location);
}

Status ToProto(const Location& location, pb::Location* pb_location) {
  pb_location->set_uri(location.ToString());
  return Status::OK();
}

Status ToProto(const BasicAuth& basic_auth, pb::BasicAuth* pb_basic_auth) {
  pb_basic_auth->set_username(basic_auth.username);
  pb_basic_auth->set_password(basic_auth.password);
  return Status::OK();
}

// Ticket

Status FromProto(const pb::Ticket& pb_ticket, Ticket* ticket) {
  ticket->ticket = pb_ticket.ticket();
  return Status::OK();
}

Status ToProto(const Ticket& ticket, pb::Ticket* pb_ticket) {
  pb_ticket->set_ticket(ticket.ticket);
  return Status::OK();
}

// FlightData

Status FromProto(const pb::FlightData& pb_data, FlightDescriptor* descriptor,
                 std::unique_ptr<ipc::Message>* message) {
  RETURN_NOT_OK(internal::FromProto(pb_data.flight_descriptor(), descriptor));
  const std::string& header = pb_data.data_header();
  const std::string& body = pb_data.data_body();
  std::shared_ptr<Buffer> header_buf = Buffer::Wrap(header.data(), header.size());
  std::shared_ptr<Buffer> body_buf = Buffer::Wrap(body.data(), body.size());
  if (header_buf == nullptr || body_buf == nullptr) {
    return Status::UnknownError("Could not create buffers from protobuf");
  }
  return ipc::Message::Open(header_buf, body_buf).Value(message);
}

// FlightEndpoint

Status FromProto(const pb::FlightEndpoint& pb_endpoint, FlightEndpoint* endpoint) {
  RETURN_NOT_OK(FromProto(pb_endpoint.ticket(), &endpoint->ticket));
  endpoint->locations.resize(pb_endpoint.location_size());
  for (int i = 0; i < pb_endpoint.location_size(); ++i) {
    RETURN_NOT_OK(FromProto(pb_endpoint.location(i), &endpoint->locations[i]));
  }
  if (pb_endpoint.has_expiration_time()) {
    Timestamp expiration_time;
    RETURN_NOT_OK(FromProto(pb_endpoint.expiration_time(), &expiration_time));
    endpoint->expiration_time = std::move(expiration_time);
  }
  endpoint->app_metadata = pb_endpoint.app_metadata();
  return Status::OK();
}

Status ToProto(const FlightEndpoint& endpoint, pb::FlightEndpoint* pb_endpoint) {
  RETURN_NOT_OK(ToProto(endpoint.ticket, pb_endpoint->mutable_ticket()));
  pb_endpoint->clear_location();
  for (const Location& location : endpoint.locations) {
    RETURN_NOT_OK(ToProto(location, pb_endpoint->add_location()));
  }
  if (endpoint.expiration_time) {
    RETURN_NOT_OK(ToProto(endpoint.expiration_time.value(),
                          pb_endpoint->mutable_expiration_time()));
  }
  pb_endpoint->set_app_metadata(endpoint.app_metadata);
  return Status::OK();
}

// RenewFlightEndpointRequest

Status FromProto(const pb::RenewFlightEndpointRequest& pb_request,
                 RenewFlightEndpointRequest* request) {
  RETURN_NOT_OK(FromProto(pb_request.endpoint(), &request->endpoint));
  return Status::OK();
}

Status ToProto(const RenewFlightEndpointRequest& request,
               pb::RenewFlightEndpointRequest* pb_request) {
  RETURN_NOT_OK(ToProto(request.endpoint, pb_request->mutable_endpoint()));
  return Status::OK();
}

// FlightDescriptor

Status FromProto(const pb::FlightDescriptor& pb_descriptor,
                 FlightDescriptor* descriptor) {
  if (pb_descriptor.type() == pb::FlightDescriptor::PATH) {
    descriptor->type = FlightDescriptor::PATH;
    descriptor->path.reserve(pb_descriptor.path_size());
    for (int i = 0; i < pb_descriptor.path_size(); ++i) {
      descriptor->path.emplace_back(pb_descriptor.path(i));
    }
  } else if (pb_descriptor.type() == pb::FlightDescriptor::CMD) {
    descriptor->type = FlightDescriptor::CMD;
    descriptor->cmd = pb_descriptor.cmd();
  } else {
    return Status::Invalid("Client sent UNKNOWN descriptor type");
  }
  return Status::OK();
}

Status ToProto(const FlightDescriptor& descriptor, pb::FlightDescriptor* pb_descriptor) {
  if (descriptor.type == FlightDescriptor::PATH) {
    pb_descriptor->set_type(pb::FlightDescriptor::PATH);
    for (const std::string& path : descriptor.path) {
      pb_descriptor->add_path(path);
    }
  } else {
    pb_descriptor->set_type(pb::FlightDescriptor::CMD);
    pb_descriptor->set_cmd(descriptor.cmd);
  }
  return Status::OK();
}

// FlightInfo

arrow::Result<FlightInfo> FromProto(const pb::FlightInfo& pb_info) {
  FlightInfo::Data info;
  RETURN_NOT_OK(FromProto(pb_info.flight_descriptor(), &info.descriptor));

  info.schema = pb_info.schema();

  info.endpoints.resize(pb_info.endpoint_size());
  for (int i = 0; i < pb_info.endpoint_size(); ++i) {
    RETURN_NOT_OK(FromProto(pb_info.endpoint(i), &info.endpoints[i]));
  }

  info.total_records = pb_info.total_records();
  info.total_bytes = pb_info.total_bytes();
  info.ordered = pb_info.ordered();
  info.app_metadata = pb_info.app_metadata();
  return FlightInfo(std::move(info));
}

Status FromProto(const pb::BasicAuth& pb_basic_auth, BasicAuth* basic_auth) {
  basic_auth->password = pb_basic_auth.password();
  basic_auth->username = pb_basic_auth.username();

  return Status::OK();
}

Status FromProto(const pb::SchemaResult& pb_result, std::string* result) {
  *result = pb_result.schema();
  return Status::OK();
}

Status SchemaToString(const Schema& schema, std::string* out) {
  ipc::DictionaryMemo unused_dict_memo;
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> serialized_schema,
                        ipc::SerializeSchema(schema));
  *out = std::string(reinterpret_cast<const char*>(serialized_schema->data()),
                     static_cast<size_t>(serialized_schema->size()));
  return Status::OK();
}

Status ToProto(const FlightInfo& info, pb::FlightInfo* pb_info) {
  // clear any repeated fields
  pb_info->clear_endpoint();

  pb_info->set_schema(info.serialized_schema());

  // descriptor
  RETURN_NOT_OK(ToProto(info.descriptor(), pb_info->mutable_flight_descriptor()));

  // endpoints
  for (const FlightEndpoint& endpoint : info.endpoints()) {
    RETURN_NOT_OK(ToProto(endpoint, pb_info->add_endpoint()));
  }

  pb_info->set_total_records(info.total_records());
  pb_info->set_total_bytes(info.total_bytes());
  pb_info->set_ordered(info.ordered());
  pb_info->set_app_metadata(info.app_metadata());
  return Status::OK();
}

// PollInfo

Status FromProto(const pb::PollInfo& pb_info, PollInfo* info) {
  if (pb_info.has_info()) {
    ARROW_ASSIGN_OR_RAISE(auto flight_info, FromProto(pb_info.info()));
    info->info = std::make_unique<FlightInfo>(std::move(flight_info));
  }
  if (pb_info.has_flight_descriptor()) {
    FlightDescriptor descriptor;
    RETURN_NOT_OK(FromProto(pb_info.flight_descriptor(), &descriptor));
    info->descriptor = std::move(descriptor);
  } else {
    info->descriptor = std::nullopt;
  }
  if (pb_info.has_progress()) {
    info->progress = pb_info.progress();
  } else {
    info->progress = std::nullopt;
  }
  if (pb_info.has_expiration_time()) {
    Timestamp expiration_time;
    RETURN_NOT_OK(FromProto(pb_info.expiration_time(), &expiration_time));
    info->expiration_time = std::move(expiration_time);
  } else {
    info->expiration_time = std::nullopt;
  }
  return Status::OK();
}

Status ToProto(const PollInfo& info, pb::PollInfo* pb_info) {
  if (info.info) {
    RETURN_NOT_OK(ToProto(*info.info, pb_info->mutable_info()));
  }
  if (info.descriptor) {
    RETURN_NOT_OK(ToProto(*info.descriptor, pb_info->mutable_flight_descriptor()));
  }
  if (info.progress) {
    pb_info->set_progress(info.progress.value());
  }
  if (info.expiration_time) {
    RETURN_NOT_OK(ToProto(*info.expiration_time, pb_info->mutable_expiration_time()));
  }
  return Status::OK();
}

// CancelFlightInfoRequest

Status FromProto(const pb::CancelFlightInfoRequest& pb_request,
                 CancelFlightInfoRequest* request) {
  ARROW_ASSIGN_OR_RAISE(FlightInfo info, FromProto(pb_request.info()));
  request->info = std::make_unique<FlightInfo>(std::move(info));
  return Status::OK();
}

Status ToProto(const CancelFlightInfoRequest& request,
               pb::CancelFlightInfoRequest* pb_request) {
  RETURN_NOT_OK(ToProto(*request.info, pb_request->mutable_info()));
  return Status::OK();
}

Status ToProto(const SchemaResult& result, pb::SchemaResult* pb_result) {
  pb_result->set_schema(result.serialized_schema());
  return Status::OK();
}

Status ToPayload(const FlightDescriptor& descr, std::shared_ptr<Buffer>* out) {
  // TODO(ARROW-15612): make these use Result<T>
  std::string str_descr;
  pb::FlightDescriptor pb_descr;
  RETURN_NOT_OK(ToProto(descr, &pb_descr));
  if (!pb_descr.SerializeToString(&str_descr)) {
    return Status::UnknownError("Failed to serialize Flight descriptor");
  }
  *out = Buffer::FromString(std::move(str_descr));
  return Status::OK();
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
