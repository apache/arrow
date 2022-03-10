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

#include "arrow/flight/transport/grpc/util_internal.h"

#include <cstdlib>
#include <map>
#include <memory>
#include <sstream>
#include <string>

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#else
#include <grpc++/grpc++.h>
#endif

#include "arrow/flight/types.h"
#include "arrow/status.h"

namespace arrow {
namespace flight {
namespace transport {
namespace grpc {

const char* kGrpcAuthHeader = "auth-token-bin";
const char* kGrpcStatusCodeHeader = "x-arrow-status";
const char* kGrpcStatusMessageHeader = "x-arrow-status-message-bin";
const char* kGrpcStatusDetailHeader = "x-arrow-status-detail-bin";
const char* kBinaryErrorDetailsKey = "grpc-status-details-bin";

static Status StatusCodeFromString(const ::grpc::string_ref& code_ref, StatusCode* code) {
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
static Status FromGrpcContext(const ::grpc::ClientContext& ctx, Status* status,
                              std::shared_ptr<FlightStatusDetail> flight_status_detail) {
  const std::multimap<::grpc::string_ref, ::grpc::string_ref>& trailers =
      ctx.GetServerTrailingMetadata();
  const auto code_val = trailers.find(kGrpcStatusCodeHeader);
  if (code_val == trailers.end()) {
    return Status::IOError("Status code header not found");
  }

  const ::grpc::string_ref code_ref = code_val->second;
  StatusCode code = {};
  RETURN_NOT_OK(StatusCodeFromString(code_ref, &code));

  const auto message_val = trailers.find(kGrpcStatusMessageHeader);
  if (message_val == trailers.end()) {
    return Status::IOError("Status message header not found");
  }

  const ::grpc::string_ref message_ref = message_val->second;
  std::string message = std::string(message_ref.data(), message_ref.size());
  const auto detail_val = trailers.find(kGrpcStatusDetailHeader);
  if (detail_val != trailers.end()) {
    const ::grpc::string_ref detail_ref = detail_val->second;
    message += ". Detail: ";
    message += std::string(detail_ref.data(), detail_ref.size());
  }
  const auto grpc_detail_val = trailers.find(kBinaryErrorDetailsKey);
  if (grpc_detail_val != trailers.end()) {
    const ::grpc::string_ref detail_ref = grpc_detail_val->second;
    std::string bin_detail = std::string(detail_ref.data(), detail_ref.size());
    if (!flight_status_detail) {
      flight_status_detail =
          std::make_shared<FlightStatusDetail>(FlightStatusCode::Internal);
    }
    flight_status_detail->set_extra_info(bin_detail);
  }
  *status = Status(code, message, flight_status_detail);
  return Status::OK();
}

/// Convert a gRPC status to an Arrow status, ignoring any
/// implementation-defined headers that encode further detail.
static Status FromGrpcCode(const ::grpc::Status& grpc_status) {
  switch (grpc_status.error_code()) {
    case ::grpc::StatusCode::OK:
      return Status::OK();
    case ::grpc::StatusCode::CANCELLED:
      return Status::IOError("gRPC cancelled call, with message: ",
                             grpc_status.error_message())
          .WithDetail(std::make_shared<FlightStatusDetail>(FlightStatusCode::Cancelled));
    case ::grpc::StatusCode::UNKNOWN: {
      std::stringstream ss;
      ss << "Flight RPC failed with message: " << grpc_status.error_message();
      return Status::UnknownError(ss.str()).WithDetail(
          std::make_shared<FlightStatusDetail>(FlightStatusCode::Failed));
    }
    case ::grpc::StatusCode::INVALID_ARGUMENT:
      return Status::Invalid("gRPC returned invalid argument error, with message: ",
                             grpc_status.error_message());
    case ::grpc::StatusCode::DEADLINE_EXCEEDED:
      return Status::IOError("gRPC returned deadline exceeded error, with message: ",
                             grpc_status.error_message())
          .WithDetail(std::make_shared<FlightStatusDetail>(FlightStatusCode::TimedOut));
    case ::grpc::StatusCode::NOT_FOUND:
      return Status::KeyError("gRPC returned not found error, with message: ",
                              grpc_status.error_message());
    case ::grpc::StatusCode::ALREADY_EXISTS:
      return Status::AlreadyExists("gRPC returned already exists error, with message: ",
                                   grpc_status.error_message());
    case ::grpc::StatusCode::PERMISSION_DENIED:
      return Status::IOError("gRPC returned permission denied error, with message: ",
                             grpc_status.error_message())
          .WithDetail(
              std::make_shared<FlightStatusDetail>(FlightStatusCode::Unauthorized));
    case ::grpc::StatusCode::RESOURCE_EXHAUSTED:
      return Status::Invalid("gRPC returned resource exhausted error, with message: ",
                             grpc_status.error_message());
    case ::grpc::StatusCode::FAILED_PRECONDITION:
      return Status::Invalid("gRPC returned precondition failed error, with message: ",
                             grpc_status.error_message());
    case ::grpc::StatusCode::ABORTED:
      return Status::IOError("gRPC returned aborted error, with message: ",
                             grpc_status.error_message())
          .WithDetail(std::make_shared<FlightStatusDetail>(FlightStatusCode::Internal));
    case ::grpc::StatusCode::OUT_OF_RANGE:
      return Status::Invalid("gRPC returned out-of-range error, with message: ",
                             grpc_status.error_message());
    case ::grpc::StatusCode::UNIMPLEMENTED:
      return Status::NotImplemented("gRPC returned unimplemented error, with message: ",
                                    grpc_status.error_message());
    case ::grpc::StatusCode::INTERNAL:
      return Status::IOError("gRPC returned internal error, with message: ",
                             grpc_status.error_message())
          .WithDetail(std::make_shared<FlightStatusDetail>(FlightStatusCode::Internal));
    case ::grpc::StatusCode::UNAVAILABLE:
      return Status::IOError("gRPC returned unavailable error, with message: ",
                             grpc_status.error_message())
          .WithDetail(
              std::make_shared<FlightStatusDetail>(FlightStatusCode::Unavailable));
    case ::grpc::StatusCode::DATA_LOSS:
      return Status::IOError("gRPC returned data loss error, with message: ",
                             grpc_status.error_message())
          .WithDetail(std::make_shared<FlightStatusDetail>(FlightStatusCode::Internal));
    case ::grpc::StatusCode::UNAUTHENTICATED:
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

Status FromGrpcStatus(const ::grpc::Status& grpc_status, ::grpc::ClientContext* ctx) {
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
static ::grpc::Status ToRawGrpcStatus(const Status& arrow_status) {
  if (arrow_status.ok()) {
    return ::grpc::Status::OK;
  }

  ::grpc::StatusCode grpc_code = ::grpc::StatusCode::UNKNOWN;
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
        grpc_code = ::grpc::StatusCode::INTERNAL;
        break;
      case FlightStatusCode::TimedOut:
        grpc_code = ::grpc::StatusCode::DEADLINE_EXCEEDED;
        break;
      case FlightStatusCode::Cancelled:
        grpc_code = ::grpc::StatusCode::CANCELLED;
        break;
      case FlightStatusCode::Unauthenticated:
        grpc_code = ::grpc::StatusCode::UNAUTHENTICATED;
        break;
      case FlightStatusCode::Unauthorized:
        grpc_code = ::grpc::StatusCode::PERMISSION_DENIED;
        break;
      case FlightStatusCode::Unavailable:
        grpc_code = ::grpc::StatusCode::UNAVAILABLE;
        break;
      default:
        break;
    }
  } else if (arrow_status.IsNotImplemented()) {
    grpc_code = ::grpc::StatusCode::UNIMPLEMENTED;
  } else if (arrow_status.IsInvalid()) {
    grpc_code = ::grpc::StatusCode::INVALID_ARGUMENT;
  } else if (arrow_status.IsKeyError()) {
    grpc_code = ::grpc::StatusCode::NOT_FOUND;
  } else if (arrow_status.IsAlreadyExists()) {
    grpc_code = ::grpc::StatusCode::ALREADY_EXISTS;
  }
  return ::grpc::Status(grpc_code, message);
}

/// Convert an Arrow status to a gRPC status, and add extra headers to
/// the response to encode the original Arrow status.
::grpc::Status ToGrpcStatus(const Status& arrow_status, ::grpc::ServerContext* ctx) {
  ::grpc::Status status = ToRawGrpcStatus(arrow_status);
  if (!status.ok() && ctx) {
    const std::string code = std::to_string(static_cast<int>(arrow_status.code()));
    ctx->AddTrailingMetadata(kGrpcStatusCodeHeader, code);
    ctx->AddTrailingMetadata(kGrpcStatusMessageHeader, arrow_status.message());
    if (arrow_status.detail()) {
      const std::string detail_string = arrow_status.detail()->ToString();
      ctx->AddTrailingMetadata(kGrpcStatusDetailHeader, detail_string);
    }
    auto fsd = FlightStatusDetail::UnwrapStatus(arrow_status);
    if (fsd && !fsd->extra_info().empty()) {
      ctx->AddTrailingMetadata(kBinaryErrorDetailsKey, fsd->extra_info());
    }
  }

  return status;
}

}  // namespace grpc
}  // namespace transport
}  // namespace flight
}  // namespace arrow
