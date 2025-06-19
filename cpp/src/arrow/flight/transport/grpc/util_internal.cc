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
#include <optional>
#include <string>

#include <grpcpp/grpcpp.h>

#include "arrow/flight/transport.h"
#include "arrow/flight/types.h"
#include "arrow/status.h"
#include "arrow/util/string.h"
#include "arrow/util/string_util.h"

namespace arrow {

using internal::ToChars;

namespace flight {
namespace transport {
namespace grpc {

using internal::TransportStatus;

const char* kGrpcAuthHeader = "auth-token-bin";
const char* kGrpcStatusCodeHeader = "x-arrow-status";
const char* kGrpcStatusMessageHeader = "x-arrow-status-message-bin";
const char* kGrpcStatusDetailHeader = "x-arrow-status-detail-bin";
const char* kBinaryErrorDetailsKey = "grpc-status-details-bin";

/// Try to extract a status from gRPC trailers.
/// Return Status::OK if found, an error otherwise.
static bool FromGrpcContext(const ::grpc::ClientContext& ctx,
                            const Status& current_status, Status* status,
                            std::shared_ptr<FlightStatusDetail> flight_status_detail) {
  const std::multimap<::grpc::string_ref, ::grpc::string_ref>& trailers =
      ctx.GetServerTrailingMetadata();

  const auto code_val = trailers.find(kGrpcStatusCodeHeader);
  if (code_val == trailers.end()) return false;

  const auto message_val = trailers.find(kGrpcStatusMessageHeader);
  const std::optional<std::string> message =
      message_val == trailers.end()
          ? std::nullopt
          : std::optional<std::string>(
                std::string(message_val->second.data(), message_val->second.size()));

  const auto detail_val = trailers.find(kGrpcStatusDetailHeader);
  const std::optional<std::string> detail_message =
      detail_val == trailers.end()
          ? std::nullopt
          : std::optional<std::string>(
                std::string(detail_val->second.data(), detail_val->second.size()));

  const auto grpc_detail_val = trailers.find(kBinaryErrorDetailsKey);
  const std::optional<std::string> detail_bin =
      grpc_detail_val == trailers.end()
          ? std::nullopt
          : std::optional<std::string>(std::string(grpc_detail_val->second.data(),
                                                   grpc_detail_val->second.size()));

  std::string code_str(code_val->second.data(), code_val->second.size());
  *status = internal::ReconstructStatus(code_str, current_status, std::move(message),
                                        std::move(detail_message), std::move(detail_bin),
                                        std::move(flight_status_detail));
  return true;
}

static TransportStatus TransportStatusFromGrpc(const ::grpc::Status& grpc_status) {
  switch (grpc_status.error_code()) {
    case ::grpc::StatusCode::OK:
      return TransportStatus{TransportStatusCode::kOk, ""};
    case ::grpc::StatusCode::CANCELLED:
      return TransportStatus{TransportStatusCode::kCancelled,
                             grpc_status.error_message()};
    case ::grpc::StatusCode::UNKNOWN:
      return TransportStatus{TransportStatusCode::kUnknown, grpc_status.error_message()};
    case ::grpc::StatusCode::INVALID_ARGUMENT:
      return TransportStatus{TransportStatusCode::kInvalidArgument,
                             grpc_status.error_message()};
    case ::grpc::StatusCode::DEADLINE_EXCEEDED:
      return TransportStatus{TransportStatusCode::kTimedOut, grpc_status.error_message()};
    case ::grpc::StatusCode::NOT_FOUND:
      return TransportStatus{TransportStatusCode::kNotFound, grpc_status.error_message()};
    case ::grpc::StatusCode::ALREADY_EXISTS:
      return TransportStatus{TransportStatusCode::kAlreadyExists,
                             grpc_status.error_message()};
    case ::grpc::StatusCode::PERMISSION_DENIED:
      return TransportStatus{TransportStatusCode::kUnauthorized,
                             grpc_status.error_message()};
    case ::grpc::StatusCode::RESOURCE_EXHAUSTED:
      return TransportStatus{TransportStatusCode::kUnavailable,
                             grpc_status.error_message()};
    case ::grpc::StatusCode::FAILED_PRECONDITION:
      return TransportStatus{TransportStatusCode::kUnavailable,
                             grpc_status.error_message()};
    case ::grpc::StatusCode::ABORTED:
      return TransportStatus{TransportStatusCode::kUnavailable,
                             grpc_status.error_message()};
    case ::grpc::StatusCode::OUT_OF_RANGE:
      return TransportStatus{TransportStatusCode::kInvalidArgument,
                             grpc_status.error_message()};
    case ::grpc::StatusCode::UNIMPLEMENTED:
      return TransportStatus{TransportStatusCode::kUnimplemented,
                             grpc_status.error_message()};
    case ::grpc::StatusCode::INTERNAL:
      return TransportStatus{TransportStatusCode::kInternal, grpc_status.error_message()};
    case ::grpc::StatusCode::UNAVAILABLE:
      return TransportStatus{TransportStatusCode::kUnavailable,
                             grpc_status.error_message()};
    case ::grpc::StatusCode::DATA_LOSS:
      return TransportStatus{TransportStatusCode::kInternal, grpc_status.error_message()};
    case ::grpc::StatusCode::UNAUTHENTICATED:
      return TransportStatus{TransportStatusCode::kUnauthenticated,
                             grpc_status.error_message()};
    default:
      return TransportStatus{
          TransportStatusCode::kUnknown,
          arrow::internal::JoinToString("(", grpc_status.error_code(), ")",
                                        grpc_status.error_message())};
  }
}

Status CombinedTransportStatus(const ::grpc::Status& grpc_status,
                               arrow::Status arrow_status, ::grpc::ClientContext* ctx) {
  if (grpc_status.ok() && arrow_status.ok()) {
    return Status::OK();
  } else if (grpc_status.ok() && !arrow_status.ok()) {
    return arrow_status;
  }

  // Can't share with FromGrpcCode because that function sometimes constructs an Arrow
  // Status directly
  const TransportStatus base_status = TransportStatusFromGrpc(grpc_status);

  std::vector<std::pair<std::string, std::string>> details;
  if (!grpc_status.ok() && ctx) {
    // Attach rich error details
    const std::multimap<::grpc::string_ref, ::grpc::string_ref>& trailers =
        ctx->GetServerTrailingMetadata();

    for (const auto key : {
             // gRPC error details
             kBinaryErrorDetailsKey,
             // Sync C++ servers send information about the Arrow status
             kGrpcStatusCodeHeader,
             kGrpcStatusMessageHeader,
             kGrpcStatusDetailHeader,
         }) {
      for (auto [it, end] = trailers.equal_range(key); it != end; it++) {
        details.emplace_back(key, std::string(it->second.data(), it->second.size()));
      }
    }
  }

  if (arrow_status.ok()) {
    arrow_status = base_status.ToStatus();
  }

  if (!details.empty()) {
    return arrow_status.WithDetail(std::make_shared<TransportStatusDetail>(
        base_status.code, std::move(base_status.message), std::move(details)));
  }
  return arrow_status;
}

/// Convert a gRPC status to an Arrow status, ignoring any
/// implementation-defined headers that encode further detail.
static Status FromGrpcCode(const ::grpc::Status& grpc_status) {
  switch (grpc_status.error_code()) {
    case ::grpc::StatusCode::OK:
      return Status::OK();
    case ::grpc::StatusCode::CANCELLED:
      return TransportStatus{TransportStatusCode::kCancelled, grpc_status.error_message()}
          .ToStatus();
    case ::grpc::StatusCode::UNKNOWN:
      return TransportStatus{TransportStatusCode::kUnknown, grpc_status.error_message()}
          .ToStatus();
    case ::grpc::StatusCode::INVALID_ARGUMENT:
      return TransportStatus{TransportStatusCode::kInvalidArgument,
                             grpc_status.error_message()}
          .ToStatus();
    case ::grpc::StatusCode::DEADLINE_EXCEEDED:
      return TransportStatus{TransportStatusCode::kTimedOut, grpc_status.error_message()}
          .ToStatus();
    case ::grpc::StatusCode::NOT_FOUND:
      return TransportStatus{TransportStatusCode::kNotFound, grpc_status.error_message()}
          .ToStatus();
    case ::grpc::StatusCode::ALREADY_EXISTS:
      return TransportStatus{TransportStatusCode::kAlreadyExists,
                             grpc_status.error_message()}
          .ToStatus();
    case ::grpc::StatusCode::PERMISSION_DENIED:
      return TransportStatus{TransportStatusCode::kUnauthorized,
                             grpc_status.error_message()}
          .ToStatus();
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
      return TransportStatus{TransportStatusCode::kUnimplemented,
                             grpc_status.error_message()}
          .ToStatus();
    case ::grpc::StatusCode::INTERNAL:
      return TransportStatus{TransportStatusCode::kInternal, grpc_status.error_message()}
          .ToStatus();
    case ::grpc::StatusCode::UNAVAILABLE:
      return TransportStatus{TransportStatusCode::kUnavailable,
                             grpc_status.error_message()}
          .ToStatus();
    case ::grpc::StatusCode::DATA_LOSS:
      return Status::IOError("gRPC returned data loss error, with message: ",
                             grpc_status.error_message())
          .WithDetail(std::make_shared<FlightStatusDetail>(FlightStatusCode::Internal));
    case ::grpc::StatusCode::UNAUTHENTICATED:
      return TransportStatus{TransportStatusCode::kUnauthenticated,
                             grpc_status.error_message()}
          .ToStatus();
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
    if (FromGrpcContext(*ctx, status, &arrow_status,
                        FlightStatusDetail::UnwrapStatus(status))) {
      return arrow_status;
    }
    // If we fail to decode a more detailed status from the headers,
    // proceed normally
  }
  return status;
}

/// Convert an Arrow status to a gRPC status.
static ::grpc::Status ToRawGrpcStatus(const Status& arrow_status) {
  if (arrow_status.ok()) return ::grpc::Status::OK;

  TransportStatus transport_status = TransportStatus::FromStatus(arrow_status);
  ::grpc::StatusCode grpc_code = ::grpc::StatusCode::UNKNOWN;
  switch (transport_status.code) {
    case TransportStatusCode::kOk:
      return ::grpc::Status::OK;
    case TransportStatusCode::kUnknown:
      grpc_code = ::grpc::StatusCode::UNKNOWN;
      break;
    case TransportStatusCode::kInternal:
      grpc_code = ::grpc::StatusCode::INTERNAL;
      break;
    case TransportStatusCode::kInvalidArgument:
      grpc_code = ::grpc::StatusCode::INVALID_ARGUMENT;
      break;
    case TransportStatusCode::kTimedOut:
      grpc_code = ::grpc::StatusCode::DEADLINE_EXCEEDED;
      break;
    case TransportStatusCode::kNotFound:
      grpc_code = ::grpc::StatusCode::NOT_FOUND;
      break;
    case TransportStatusCode::kAlreadyExists:
      grpc_code = ::grpc::StatusCode::ALREADY_EXISTS;
      break;
    case TransportStatusCode::kCancelled:
      grpc_code = ::grpc::StatusCode::CANCELLED;
      break;
    case TransportStatusCode::kUnauthenticated:
      grpc_code = ::grpc::StatusCode::UNAUTHENTICATED;
      break;
    case TransportStatusCode::kUnauthorized:
      grpc_code = ::grpc::StatusCode::PERMISSION_DENIED;
      break;
    case TransportStatusCode::kUnimplemented:
      grpc_code = ::grpc::StatusCode::UNIMPLEMENTED;
      break;
    case TransportStatusCode::kUnavailable:
      grpc_code = ::grpc::StatusCode::UNAVAILABLE;
      break;
    default:
      grpc_code = ::grpc::StatusCode::UNKNOWN;
      break;
  }
  return {grpc_code, std::move(transport_status.message)};
}

/// Convert an Arrow status to a gRPC status, and add extra headers to
/// the response to encode the original Arrow status.
::grpc::Status ToGrpcStatus(const Status& arrow_status, ::grpc::ServerContext* ctx) {
  ::grpc::Status status = ToRawGrpcStatus(arrow_status);
  if (!status.ok() && ctx) {
    const std::string code = ToChars(static_cast<int>(arrow_status.code()));
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
