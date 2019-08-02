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

#include "arrow/flight/internal.h"
#include "arrow/flight/platform.h"
#include "arrow/flight/protocol-internal.h"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#else
#include <grpc++/grpc++.h>
#endif

#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/string_builder.h"

namespace arrow {
namespace flight {
namespace internal {

const char* kGrpcAuthHeader = "auth-token-bin";

Status FromGrpcStatus(const grpc::Status& grpc_status) {
  if (grpc_status.ok()) {
    return Status::OK();
  }

  switch (grpc_status.error_code()) {
    case grpc::StatusCode::OK:
      return Status::OK();
    case grpc::StatusCode::CANCELLED:
      return Status::IOError("gRPC cancelled call, with message: ",
                             grpc_status.error_message())
          .WithDetail(std::make_shared<FlightStatusDetail>(FlightStatusCode::Cancelled));
    case grpc::StatusCode::UNKNOWN:
      return Status::UnknownError("gRPC returned unknown error, with message: ",
                                  grpc_status.error_message());
    case grpc::StatusCode::INVALID_ARGUMENT:
      return Status::Invalid("gRPC returned invalid argument error, with message: ",
                             grpc_status.error_message());
    case grpc::StatusCode::DEADLINE_EXCEEDED:
      return Status::IOError("gRPC returned deadline exceeded error, with message: ",
                             grpc_status.error_message())
          .WithDetail(std::make_shared<FlightStatusDetail>(FlightStatusCode::TimedOut));
    case grpc::StatusCode::NOT_FOUND:
      return Status::KeyError("gRPC returned not found error, with message: ",
                              grpc_status.error_message());
    case grpc::StatusCode::ALREADY_EXISTS:
      return Status::AlreadyExists("gRPC returned already exists error, with message: ",
                                   grpc_status.error_message());
    case grpc::StatusCode::PERMISSION_DENIED:
      return Status::IOError("gRPC returned permission denied error, with message: ",
                             grpc_status.error_message())
          .WithDetail(
              std::make_shared<FlightStatusDetail>(FlightStatusCode::Unauthorized));
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
      return Status::Invalid("gRPC returned resource exhausted error, with message: ",
                             grpc_status.error_message());
    case grpc::StatusCode::FAILED_PRECONDITION:
      return Status::Invalid("gRPC returned precondition failed error, with message: ",
                             grpc_status.error_message());
    case grpc::StatusCode::ABORTED:
      return Status::IOError("gRPC returned aborted error, with message: ",
                             grpc_status.error_message())
          .WithDetail(std::make_shared<FlightStatusDetail>(FlightStatusCode::Internal));
    case grpc::StatusCode::OUT_OF_RANGE:
      return Status::Invalid("gRPC returned out-of-range error, with message: ",
                             grpc_status.error_message());
    case grpc::StatusCode::UNIMPLEMENTED:
      return Status::NotImplemented("gRPC returned unimplemented error, with message: ",
                                    grpc_status.error_message());
    case grpc::StatusCode::INTERNAL:
      return Status::IOError("gRPC returned internal error, with message: ",
                             grpc_status.error_message())
          .WithDetail(std::make_shared<FlightStatusDetail>(FlightStatusCode::Internal));
    case grpc::StatusCode::UNAVAILABLE:
      return Status::IOError("gRPC returned unavailable error, with message: ",
                             grpc_status.error_message())
          .WithDetail(
              std::make_shared<FlightStatusDetail>(FlightStatusCode::Unavailable));
    case grpc::StatusCode::DATA_LOSS:
      return Status::IOError("gRPC returned data loss error, with message: ",
                             grpc_status.error_message())
          .WithDetail(std::make_shared<FlightStatusDetail>(FlightStatusCode::Internal));
    case grpc::StatusCode::UNAUTHENTICATED:
      return Status::IOError("gRPC returned unauthenticated error, with message: ",
                             grpc_status.error_message())
          .WithDetail(
              std::make_shared<FlightStatusDetail>(FlightStatusCode::Unauthenticated));
    default:
      return Status::UnknownError("gRPC failed with error code ",
                                  grpc_status.error_code(),
                                  " and message: ", grpc_status.error_message());
  }
}

grpc::Status ToGrpcStatus(const Status& arrow_status) {
  if (arrow_status.ok()) {
    return grpc::Status::OK;
  }

  grpc::StatusCode grpc_code = grpc::StatusCode::UNKNOWN;
  std::string message = arrow_status.message();
  if (arrow_status.detail()) {
    message += ". Detail: ";
    message += arrow_status.detail()->ToString();
  }

  std::shared_ptr<FlightStatusDetail> flight_status =
      FlightStatusDetail::UnwrapStatus(arrow_status);
  if (flight_status) {
    switch (flight_status->code()) {
      case FlightStatusCode::Internal:
        grpc_code = grpc::StatusCode::INTERNAL;
        break;
      case FlightStatusCode::TimedOut:
        grpc_code = grpc::StatusCode::DEADLINE_EXCEEDED;
        break;
      case FlightStatusCode::Cancelled:
        grpc_code = grpc::StatusCode::CANCELLED;
        break;
      case FlightStatusCode::Unauthenticated:
        grpc_code = grpc::StatusCode::UNAUTHENTICATED;
        break;
      case FlightStatusCode::Unauthorized:
        grpc_code = grpc::StatusCode::PERMISSION_DENIED;
        break;
      case FlightStatusCode::Unavailable:
        grpc_code = grpc::StatusCode::UNAVAILABLE;
        break;
      default:
        break;
    }
  } else if (arrow_status.IsNotImplemented()) {
    grpc_code = grpc::StatusCode::UNIMPLEMENTED;
  } else if (arrow_status.IsInvalid()) {
    grpc_code = grpc::StatusCode::INVALID_ARGUMENT;
  }
  return grpc::Status(grpc_code, message);
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
  return Buffer::FromString(pb_action.body(), &action->body);
}

Status ToProto(const Action& action, pb::Action* pb_action) {
  pb_action->set_type(action.type);
  pb_action->set_body(action.body->ToString());
  return Status::OK();
}

// Result (of an Action)

Status FromProto(const pb::Result& pb_result, Result* result) {
  // ARROW-3250; can avoid copy. Can also write custom deserializer if it
  // becomes an issue
  return Buffer::FromString(pb_result.body(), &result->body);
}

Status ToProto(const Result& result, pb::Result* pb_result) {
  pb_result->set_body(result.body->ToString());
  return Status::OK();
}

// Criteria

Status FromProto(const pb::Criteria& pb_criteria, Criteria* criteria) {
  return Status::OK();
}

// Location

Status FromProto(const pb::Location& pb_location, Location* location) {
  return Location::Parse(pb_location.uri(), location);
}

void ToProto(const Location& location, pb::Location* pb_location) {
  pb_location->set_uri(location.ToString());
}

// Ticket

Status FromProto(const pb::Ticket& pb_ticket, Ticket* ticket) {
  ticket->ticket = pb_ticket.ticket();
  return Status::OK();
}

void ToProto(const Ticket& ticket, pb::Ticket* pb_ticket) {
  pb_ticket->set_ticket(ticket.ticket);
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
  return ipc::Message::Open(header_buf, body_buf, message);
}

// FlightEndpoint

Status FromProto(const pb::FlightEndpoint& pb_endpoint, FlightEndpoint* endpoint) {
  RETURN_NOT_OK(FromProto(pb_endpoint.ticket(), &endpoint->ticket));
  endpoint->locations.resize(pb_endpoint.location_size());
  for (int i = 0; i < pb_endpoint.location_size(); ++i) {
    RETURN_NOT_OK(FromProto(pb_endpoint.location(i), &endpoint->locations[i]));
  }
  return Status::OK();
}

void ToProto(const FlightEndpoint& endpoint, pb::FlightEndpoint* pb_endpoint) {
  ToProto(endpoint.ticket, pb_endpoint->mutable_ticket());
  pb_endpoint->clear_location();
  for (const Location& location : endpoint.locations) {
    ToProto(location, pb_endpoint->add_location());
  }
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
  return Status::OK();
}

Status SchemaToString(const Schema& schema, std::string* out) {
  // TODO(wesm): Do we care about better memory efficiency here?
  std::shared_ptr<Buffer> serialized_schema;
  ipc::DictionaryMemo unused_dict_memo;
  RETURN_NOT_OK(ipc::SerializeSchema(schema, &unused_dict_memo, default_memory_pool(),
                                     &serialized_schema));
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
    ToProto(endpoint, pb_info->add_endpoint());
  }

  pb_info->set_total_records(info.total_records());
  pb_info->set_total_bytes(info.total_bytes());
  return Status::OK();
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
