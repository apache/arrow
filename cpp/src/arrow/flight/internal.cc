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

#include <cstddef>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/flight/platform.h"
#include "arrow/flight/protocol_internal.h"
#include "arrow/flight/types.h"

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
const char* kGrpcStatusCodeHeader = "x-arrow-status";
const char* kGrpcStatusMessageHeader = "x-arrow-status-message-bin";
const char* kGrpcStatusDetailHeader = "x-arrow-status-detail-bin";
const char* kBinaryErrorDetailsKey = "grpc-status-details-bin";

static Status StatusCodeFromString(const grpc::string_ref& code_ref, StatusCode* code) {
  // Bounce through std::string to get a proper null-terminated C string
  const auto code_int = std::atoi(std::string(code_ref.data(), code_ref.size()).c_str());
  switch (code_int) {
    case static_cast<int>(StatusCode::OutOfMemory):
    case static_cast<int>(StatusCode::KeyError):
    case static_cast<int>(StatusCode::TypeError):
    case static_cast<int>(StatusCode::Invalid):
    case static_cast<int>(StatusCode::IOError):
    case static_cast<int>(StatusCode::CapacityError):
    case static_cast<int>(StatusCode::IndexError):
    case static_cast<int>(StatusCode::UnknownError):
    case static_cast<int>(StatusCode::NotImplemented):
    case static_cast<int>(StatusCode::SerializationError):
    case static_cast<int>(StatusCode::RError):
    case static_cast<int>(StatusCode::CodeGenError):
    case static_cast<int>(StatusCode::ExpressionValidationError):
    case static_cast<int>(StatusCode::ExecutionError):
    case static_cast<int>(StatusCode::AlreadyExists): {
      *code = static_cast<StatusCode>(code_int);
      return Status::OK();
    }
    default:
      // Code is invalid
      return Status::UnknownError("Unknown Arrow status code", code_ref);
  }
}

/// Try to extract a status from gRPC trailers.
/// Return Status::OK if found, an error otherwise.
static Status FromGrpcContext(const grpc::ClientContext& ctx, Status* status,
                              std::shared_ptr<FlightStatusDetail> flightStatusDetail) {
  const std::multimap<grpc::string_ref, grpc::string_ref>& trailers =
      ctx.GetServerTrailingMetadata();
  const auto code_val = trailers.find(kGrpcStatusCodeHeader);
  if (code_val == trailers.end()) {
    return Status::IOError("Status code header not found");
  }

  const grpc::string_ref code_ref = code_val->second;
  StatusCode code = {};
  RETURN_NOT_OK(StatusCodeFromString(code_ref, &code));

  const auto message_val = trailers.find(kGrpcStatusMessageHeader);
  if (message_val == trailers.end()) {
    return Status::IOError("Status message header not found");
  }

  const grpc::string_ref message_ref = message_val->second;
  std::string message = std::string(message_ref.data(), message_ref.size());
  const auto detail_val = trailers.find(kGrpcStatusDetailHeader);
  if (detail_val != trailers.end()) {
    const grpc::string_ref detail_ref = detail_val->second;
    message += ". Detail: ";
    message += std::string(detail_ref.data(), detail_ref.size());
  }
  const auto grpc_detail_val = trailers.find(kBinaryErrorDetailsKey);
  if (grpc_detail_val != trailers.end()) {
    const grpc::string_ref detail_ref = grpc_detail_val->second;
    std::string bin_detail = std::string(detail_ref.data(), detail_ref.size());
    if (!flightStatusDetail) {
      flightStatusDetail =
          std::make_shared<FlightStatusDetail>(FlightStatusCode::Internal);
    }
    flightStatusDetail->set_extra_info(bin_detail);
  }
  *status = Status(code, message, flightStatusDetail);
  return Status::OK();
}

/// Convert a gRPC status to an Arrow status, ignoring any
/// implementation-defined headers that encode further detail.
static Status FromGrpcCode(const grpc::Status& grpc_status) {
  switch (grpc_status.error_code()) {
    case grpc::StatusCode::OK:
      return Status::OK();
    case grpc::StatusCode::CANCELLED:
      return Status::IOError("gRPC cancelled call, with message: ",
                             grpc_status.error_message())
          .WithDetail(std::make_shared<FlightStatusDetail>(FlightStatusCode::Cancelled));
    case grpc::StatusCode::UNKNOWN: {
      std::stringstream ss;
      ss << "Flight RPC failed with message: " << grpc_status.error_message();
      return Status::UnknownError(ss.str()).WithDetail(
          std::make_shared<FlightStatusDetail>(FlightStatusCode::Failed));
    }
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

Status FromGrpcStatus(const grpc::Status& grpc_status, grpc::ClientContext* ctx) {
  const Status status = FromGrpcCode(grpc_status);

  if (!status.ok() && ctx) {
    Status arrow_status;

    if (!FromGrpcContext(*ctx, &arrow_status, FlightStatusDetail::UnwrapStatus(status))
             .ok()) {
      // If we fail to decode a more detailed status from the headers,
      // proceed normally
      return status;
    }

    return arrow_status;
  }
  return status;
}

/// Convert an Arrow status to a gRPC status.
static grpc::Status ToRawGrpcStatus(const Status& arrow_status) {
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
  } else if (arrow_status.IsKeyError()) {
    grpc_code = grpc::StatusCode::NOT_FOUND;
  } else if (arrow_status.IsAlreadyExists()) {
    grpc_code = grpc::StatusCode::ALREADY_EXISTS;
  }
  return grpc::Status(grpc_code, message);
}

/// Convert an Arrow status to a gRPC status, and add extra headers to
/// the response to encode the original Arrow status.
grpc::Status ToGrpcStatus(const Status& arrow_status, grpc::ServerContext* ctx) {
  grpc::Status status = ToRawGrpcStatus(arrow_status);
  if (!status.ok() && ctx) {
    const std::string code = std::to_string(static_cast<int>(arrow_status.code()));
    ctx->AddTrailingMetadata(internal::kGrpcStatusCodeHeader, code);
    ctx->AddTrailingMetadata(internal::kGrpcStatusMessageHeader, arrow_status.message());
    if (arrow_status.detail()) {
      const std::string detail_string = arrow_status.detail()->ToString();
      ctx->AddTrailingMetadata(internal::kGrpcStatusDetailHeader, detail_string);
    }
    auto fsd = FlightStatusDetail::UnwrapStatus(arrow_status);
    if (fsd && !fsd->extra_info().empty()) {
      ctx->AddTrailingMetadata(internal::kBinaryErrorDetailsKey, fsd->extra_info());
    }
  }

  return status;
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
  return Location::Parse(pb_location.uri(), location);
}

void ToProto(const Location& location, pb::Location* pb_location) {
  pb_location->set_uri(location.ToString());
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
  return ipc::Message::Open(header_buf, body_buf).Value(message);
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
    ToProto(endpoint, pb_info->add_endpoint());
  }

  pb_info->set_total_records(info.total_records());
  pb_info->set_total_bytes(info.total_bytes());
  return Status::OK();
}

Status ToProto(const SchemaResult& result, pb::SchemaResult* pb_result) {
  pb_result->set_schema(result.serialized_schema());
  return Status::OK();
}

Status ToPayload(const FlightDescriptor& descr, std::shared_ptr<Buffer>* out) {
  // TODO(lidavidm): make these use Result<T>
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
