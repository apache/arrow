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

#include <google/protobuf/any.pb.h>

#include "arrow/buffer.h"
#include "arrow/flight/protocol_internal.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/result.h"
#include "arrow/status.h"

// Lambda helper & CTAD
template <class... Ts>
struct overloaded : Ts... {
  using Ts::operator()...;
};
template <class... Ts>  // CTAD will not be needed for >=C++20
overloaded(Ts...)->overloaded<Ts...>;

namespace arrow {
namespace flight {
namespace internal {

namespace {

Status PackToAnyAndSerialize(const google::protobuf::Message& command, std::string* out) {
  google::protobuf::Any any;
#if PROTOBUF_VERSION >= 3015000
  if (!any.PackFrom(command)) {
    return Status::SerializationError("Failed to pack ", command.GetTypeName());
  }
#else
  any.PackFrom(command);
#endif

#if PROTOBUF_VERSION >= 3015000
  if (!any.SerializeToString(out)) {
    return Status::SerializationError("Failed to serialize ", command.GetTypeName());
  }
#else
  any.SerializeToString(out);
#endif
  return Status::OK();
}

}  // namespace

Status PackProtoCommand(const google::protobuf::Message& command, FlightDescriptor* out) {
  std::string buf;
  RETURN_NOT_OK(PackToAnyAndSerialize(command, &buf));
  *out = FlightDescriptor::Command(std::move(buf));
  return Status::OK();
}

Status PackProtoAction(std::string action_type, const google::protobuf::Message& action,
                       Action* out) {
  std::string buf;
  RETURN_NOT_OK(PackToAnyAndSerialize(action, &buf));
  out->type = std::move(action_type);
  out->body = Buffer::FromString(std::move(buf));
  return Status::OK();
}

Status UnpackProtoAction(const Action& action, google::protobuf::Message* out) {
  google::protobuf::Any any;
  if (!any.ParseFromArray(action.body->data(), static_cast<int>(action.body->size()))) {
    return Status::Invalid("Unable to parse action ", action.type);
  }
  if (!any.UnpackTo(out)) {
    return Status::Invalid("Unable to unpack ", out->GetTypeName());
  }
  return Status::OK();
}

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

Status FromProto(const pb::FlightInfo& pb_info, FlightInfo::Data* info) {
  RETURN_NOT_OK(FromProto(pb_info.flight_descriptor(), &info->descriptor));

  info->schema = pb_info.schema();

  info->endpoints.resize(pb_info.endpoint_size());
  for (int i = 0; i < pb_info.endpoint_size(); ++i) {
    RETURN_NOT_OK(FromProto(pb_info.endpoint(i), &info->endpoints[i]));
  }

  info->total_records = pb_info.total_records();
  info->total_bytes = pb_info.total_bytes();
  info->ordered = pb_info.ordered();
  info->app_metadata = pb_info.app_metadata();
  return Status::OK();
}

Status FromProto(const pb::FlightInfo& pb_info, std::unique_ptr<FlightInfo>* info) {
  FlightInfo::Data info_data;
  RETURN_NOT_OK(FromProto(pb_info, &info_data));
  *info = std::make_unique<FlightInfo>(std::move(info_data));
  return Status::OK();
}

Status FromProto(const pb::BasicAuth& pb_basic_auth, BasicAuth* basic_auth) {
  basic_auth->password = pb_basic_auth.password();
  basic_auth->username = pb_basic_auth.username();

  return Status::OK();
}

Status FromProto(const pb::SchemaResult& pb_result, SchemaResult* result) {
  *result = SchemaResult{pb_result.schema()};
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
    FlightInfo::Data info_data;
    RETURN_NOT_OK(FromProto(pb_info.info(), &info_data));
    info->info = std::make_unique<FlightInfo>(std::move(info_data));
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

Status FromProto(const pb::PollInfo& pb_info, std::unique_ptr<PollInfo>* info) {
  PollInfo poll_info;
  RETURN_NOT_OK(FromProto(pb_info, &poll_info));
  *info = std::make_unique<PollInfo>(std::move(poll_info));
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
  FlightInfo::Data info_data;
  RETURN_NOT_OK(FromProto(pb_request.info(), &info_data));
  request->info = std::make_unique<FlightInfo>(std::move(info_data));
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

namespace {

// SessionOptionValue

Status FromProto(const pb::SessionOptionValue& pb_val, SessionOptionValue* val) {
  switch (pb_val.option_value_case()) {
    case pb::SessionOptionValue::OPTION_VALUE_NOT_SET:
      *val = std::monostate{};
      break;
    case pb::SessionOptionValue::kStringValue:
      *val = pb_val.string_value();
      break;
    case pb::SessionOptionValue::kBoolValue:
      *val = pb_val.bool_value();
      break;
    case pb::SessionOptionValue::kInt64Value:
      *val = pb_val.int64_value();
      break;
    case pb::SessionOptionValue::kDoubleValue:
      *val = pb_val.double_value();
      break;
    case pb::SessionOptionValue::kStringListValue: {
      std::vector<std::string> vec;
      vec.reserve(pb_val.string_list_value().values_size());
      for (const std::string& s : pb_val.string_list_value().values()) {
        vec.push_back(s);
      }
      (*val).emplace<std::vector<std::string>>(std::move(vec));
      break;
    }
  }
  return Status::OK();
}

Status ToProto(const SessionOptionValue& val, pb::SessionOptionValue* pb_val) {
  std::visit(overloaded{[&](std::monostate v) { pb_val->clear_option_value(); },
                        [&](std::string v) { pb_val->set_string_value(v); },
                        [&](bool v) { pb_val->set_bool_value(v); },
                        [&](int64_t v) { pb_val->set_int64_value(v); },
                        [&](double v) { pb_val->set_double_value(v); },
                        [&](std::vector<std::string> v) {
                          auto* string_list_value = pb_val->mutable_string_list_value();
                          for (const std::string& s : v) string_list_value->add_values(s);
                        }},
             val);
  return Status::OK();
}

// map<string, SessionOptionValue>

Status FromProto(const google::protobuf::Map<std::string, pb::SessionOptionValue>& pb_map,
                 std::map<std::string, SessionOptionValue>* map) {
  if (pb_map.empty()) {
    return Status::OK();
  }
  for (const auto& [name, pb_val] : pb_map) {
    RETURN_NOT_OK(FromProto(pb_val, &(*map)[name]));
  }
  return Status::OK();
}

Status ToProto(const std::map<std::string, SessionOptionValue>& map,
               google::protobuf::Map<std::string, pb::SessionOptionValue>* pb_map) {
  for (const auto& [name, val] : map) {
    RETURN_NOT_OK(ToProto(val, &(*pb_map)[name]));
  }
  return Status::OK();
}

}  // namespace

// SetSessionOptionsRequest

Status FromProto(const pb::SetSessionOptionsRequest& pb_request,
                 SetSessionOptionsRequest* request) {
  RETURN_NOT_OK(FromProto(pb_request.session_options(), &request->session_options));
  return Status::OK();
}

Status ToProto(const SetSessionOptionsRequest& request,
               pb::SetSessionOptionsRequest* pb_request) {
  RETURN_NOT_OK(ToProto(request.session_options, pb_request->mutable_session_options()));
  return Status::OK();
}

// SetSessionOptionsResult

Status FromProto(const pb::SetSessionOptionsResult& pb_result,
                 SetSessionOptionsResult* result) {
  for (const auto& [k, pb_v] : pb_result.errors()) {
    result->errors.insert({k, {static_cast<SetSessionOptionErrorValue>(pb_v.value())}});
  }
  return Status::OK();
}

Status ToProto(const SetSessionOptionsResult& result,
               pb::SetSessionOptionsResult* pb_result) {
  auto* pb_errors = pb_result->mutable_errors();
  for (const auto& [k, v] : result.errors) {
    pb::SetSessionOptionsResult::Error e;
    e.set_value(static_cast<pb::SetSessionOptionsResult::ErrorValue>(v.value));
    (*pb_errors)[k] = std::move(e);
  }
  return Status::OK();
}

// GetSessionOptionsRequest

Status FromProto(const pb::GetSessionOptionsRequest& pb_request,
                 GetSessionOptionsRequest* request) {
  return Status::OK();
}

Status ToProto(const GetSessionOptionsRequest& request,
               pb::GetSessionOptionsRequest* pb_request) {
  return Status::OK();
}

// GetSessionOptionsResult

Status FromProto(const pb::GetSessionOptionsResult& pb_result,
                 GetSessionOptionsResult* result) {
  RETURN_NOT_OK(FromProto(pb_result.session_options(), &result->session_options));
  return Status::OK();
}

Status ToProto(const GetSessionOptionsResult& result,
               pb::GetSessionOptionsResult* pb_result) {
  RETURN_NOT_OK(ToProto(result.session_options, pb_result->mutable_session_options()));
  return Status::OK();
}

// CloseSessionRequest

Status FromProto(const pb::CloseSessionRequest& pb_request,
                 CloseSessionRequest* request) {
  return Status::OK();
}

Status ToProto(const CloseSessionRequest& request, pb::CloseSessionRequest* pb_request) {
  return Status::OK();
}

// CloseSessionResult

Status FromProto(const pb::CloseSessionResult& pb_result, CloseSessionResult* result) {
  result->status = static_cast<CloseSessionStatus>(pb_result.status());
  return Status::OK();
}

Status ToProto(const CloseSessionResult& result, pb::CloseSessionResult* pb_result) {
  pb_result->set_status(static_cast<protocol::CloseSessionResult::Status>(result.status));
  return Status::OK();
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
