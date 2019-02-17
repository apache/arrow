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

#include "arrow/flight/customize_protobuf.h"

#include <memory>
#include <string>
#include <utility>

#include <grpcpp/grpcpp.h>

#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace flight {
namespace internal {

Status FromGrpcStatus(const grpc::Status& grpc_status) {
  if (grpc_status.ok()) {
    return Status::OK();
  }

  if (grpc_status.error_code() == grpc::StatusCode::UNIMPLEMENTED) {
    return Status::NotImplemented("gRPC returned unimplemented error, with message: ",
                                  grpc_status.error_message());
  } else {
    return Status::IOError("gRPC failed with error code ", grpc_status.error_code(),
                           " and message: ", grpc_status.error_message());
  }
}

grpc::Status ToGrpcStatus(const Status& arrow_status) {
  if (arrow_status.ok()) {
    return grpc::Status::OK;
  } else {
    grpc::StatusCode grpc_code = grpc::StatusCode::UNKNOWN;
    if (arrow_status.IsNotImplemented()) {
      grpc_code = grpc::StatusCode::UNIMPLEMENTED;
    } else if (arrow_status.IsInvalid()) {
      grpc_code = grpc::StatusCode::INVALID_ARGUMENT;
    }
    return grpc::Status(grpc_code, arrow_status.message());
  }
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
  location->host = pb_location.host();
  location->port = pb_location.port();
  return Status::OK();
}

void ToProto(const Location& location, pb::Location* pb_location) {
  pb_location->set_host(location.host);
  pb_location->set_port(location.port);
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

// FlightGetInfo

Status FromProto(const pb::FlightGetInfo& pb_info, FlightInfo::Data* info) {
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
  RETURN_NOT_OK(ipc::SerializeSchema(schema, default_memory_pool(), &serialized_schema));
  *out = std::string(reinterpret_cast<const char*>(serialized_schema->data()),
                     static_cast<size_t>(serialized_schema->size()));
  return Status::OK();
}

Status ToProto(const FlightInfo& info, pb::FlightGetInfo* pb_info) {
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
