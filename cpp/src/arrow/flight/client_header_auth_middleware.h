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
#include "arrow/flight/client_auth.h"
#include "arrow/flight/client.h"

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#if defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
#include <grpcpp/security/tls_credentials_options.h>
#endif
#else
#include <grpc++/grpc++.h>
#endif

#include <algorithm>
#include <iostream>
#include <cctype>
#include <string>

const std::string AUTH_HEADER = "authorization";
const std::string BEARER_PREFIX = "Bearer ";
const std::string BASIC_PREFIX = "Basic ";

namespace arrow {
namespace flight {

// TODO: Need to add documentation in this file.
void ARROW_FLIGHT_EXPORT AddBasicAuthHeaders(grpc::ClientContext* context, 
                                             const std::string& username, 
                                             const std::string& password);

class ARROW_FLIGHT_EXPORT ClientBearerTokenMiddleware : public ClientMiddleware {
 public:
  explicit ClientBearerTokenMiddleware(
    std::pair<std::string, std::string>* bearer_token_);

  void SendingHeaders(AddCallHeaders* outgoing_headers);
  void ReceivedHeaders(const CallHeaders& incoming_headers);
  void CallCompleted(const Status& status);

 private:
  std::pair<std::string, std::string>* bearer_token;
};

class ARROW_FLIGHT_EXPORT ClientBearerTokenFactory : public ClientMiddlewareFactory {
 public:
  explicit ClientBearerTokenFactory(std::pair<std::string, std::string>* bearer_token_)
    : bearer_token(bearer_token_) {}

  void StartCall(const CallInfo& info, std::unique_ptr<ClientMiddleware>* middleware);
  void Reset();

 private:
  std::pair<std::string, std::string>* bearer_token;
};
}  // namespace flight
}  // namespace arrow
