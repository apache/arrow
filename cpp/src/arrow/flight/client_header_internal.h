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

// Interfaces for defining middleware for Flight clients. Currently
// experimental.

#pragma once

#include "arrow/flight/client_middleware.h"
#include "arrow/result.h"

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#if defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
#include <grpcpp/security/tls_credentials_options.h>
#endif
#else
#include <grpc++/grpc++.h>
#endif

namespace arrow {
namespace flight {
namespace internal {

/// \brief Add basic authentication header key value pair to context.
///
/// \param context grpc context variable to add header to.
/// \param username username to encode into header.
/// \param password password to to encode into header.
void ARROW_FLIGHT_EXPORT AddBasicAuthHeaders(grpc::ClientContext* context,
                                             const std::string& username,
                                             const std::string& password);

/// \brief Get bearer token from incoming headers.
///
/// \param context context that contains headers which hold the bearer token.
/// \return Bearer token, parsed from headers, empty if one is not present.
arrow::Result<std::pair<std::string, std::string>> ARROW_FLIGHT_EXPORT
GetBearerTokenHeader(grpc::ClientContext& context);

}  // namespace internal
}  // namespace flight
}  // namespace arrow
