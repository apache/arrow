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

#include "arrow/flight/transport/grpc/protocol_grpc_internal.h"
#include "arrow/flight/visibility.h"
#include "arrow/util/macros.h"

namespace grpc {

class Status;

}  // namespace grpc

namespace arrow {

class Status;

namespace flight {

#define GRPC_RETURN_NOT_OK(expr)                                 \
  do {                                                           \
    ::arrow::Status _s = (expr);                                 \
    if (ARROW_PREDICT_FALSE(!_s.ok())) {                         \
      return ::arrow::flight::transport::grpc::ToGrpcStatus(_s); \
    }                                                            \
  } while (0)

#define GRPC_RETURN_NOT_GRPC_OK(expr)    \
  do {                                   \
    ::grpc::Status _s = (expr);          \
    if (ARROW_PREDICT_FALSE(!_s.ok())) { \
      return _s;                         \
    }                                    \
  } while (0)

namespace transport {
namespace grpc {

/// The name of the header used to pass authentication tokens.
ARROW_FLIGHT_EXPORT
extern const char* kGrpcAuthHeader;

/// The name of the header used to pass the exact Arrow status code.
ARROW_FLIGHT_EXPORT
extern const char* kGrpcStatusCodeHeader;

/// The name of the header used to pass the exact Arrow status message.
ARROW_FLIGHT_EXPORT
extern const char* kGrpcStatusMessageHeader;

/// The name of the header used to pass the exact Arrow status detail.
ARROW_FLIGHT_EXPORT
extern const char* kGrpcStatusDetailHeader;

ARROW_FLIGHT_EXPORT
extern const char* kBinaryErrorDetailsKey;

/// Convert a gRPC status to an Arrow status. Optionally, provide a
/// ClientContext to recover the exact Arrow status if it was passed
/// over the wire.
ARROW_FLIGHT_EXPORT
Status FromGrpcStatus(const ::grpc::Status& grpc_status,
                      ::grpc::ClientContext* ctx = nullptr);

ARROW_FLIGHT_EXPORT
::grpc::Status ToGrpcStatus(const Status& arrow_status,
                            ::grpc::ServerContext* ctx = nullptr);

}  // namespace grpc
}  // namespace transport
}  // namespace flight
}  // namespace arrow
