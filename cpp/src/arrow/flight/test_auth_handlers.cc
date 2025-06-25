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

#include <string>

#include "arrow/flight/client_auth.h"
#include "arrow/flight/server.h"
#include "arrow/flight/server_auth.h"
#include "arrow/flight/test_auth_handlers.h"
#include "arrow/flight/types.h"
#include "arrow/flight/visibility.h"
#include "arrow/status.h"

namespace arrow::flight {

// TestServerAuthHandler

TestServerAuthHandler::TestServerAuthHandler(const std::string& username,
                                             const std::string& password)
    : username_(username), password_(password) {}

TestServerAuthHandler::~TestServerAuthHandler() {}

Status TestServerAuthHandler::Authenticate(const ServerCallContext& context,
                                           ServerAuthSender* outgoing,
                                           ServerAuthReader* incoming) {
  std::string token;
  RETURN_NOT_OK(incoming->Read(&token));
  if (token != password_) {
    return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid token");
  }
  RETURN_NOT_OK(outgoing->Write(username_));
  return Status::OK();
}

Status TestServerAuthHandler::IsValid(const ServerCallContext& context,
                                      const std::string& token,
                                      std::string* peer_identity) {
  if (token != password_) {
    return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid token");
  }
  *peer_identity = username_;
  return Status::OK();
}

// TestServerBasicAuthHandler

TestServerBasicAuthHandler::TestServerBasicAuthHandler(const std::string& username,
                                                       const std::string& password) {
  basic_auth_.username = username;
  basic_auth_.password = password;
}

TestServerBasicAuthHandler::~TestServerBasicAuthHandler() {}

Status TestServerBasicAuthHandler::Authenticate(const ServerCallContext& context,
                                                ServerAuthSender* outgoing,
                                                ServerAuthReader* incoming) {
  std::string token;
  RETURN_NOT_OK(incoming->Read(&token));
  ARROW_ASSIGN_OR_RAISE(BasicAuth incoming_auth, BasicAuth::Deserialize(token));
  if (incoming_auth.username != basic_auth_.username ||
      incoming_auth.password != basic_auth_.password) {
    return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid token");
  }
  RETURN_NOT_OK(outgoing->Write(basic_auth_.username));
  return Status::OK();
}

Status TestServerBasicAuthHandler::IsValid(const ServerCallContext& context,
                                           const std::string& token,
                                           std::string* peer_identity) {
  if (token != basic_auth_.username) {
    return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid token");
  }
  *peer_identity = basic_auth_.username;
  return Status::OK();
}

// TestClientAuthHandler

TestClientAuthHandler::TestClientAuthHandler(const std::string& username,
                                             const std::string& password)
    : username_(username), password_(password) {}

TestClientAuthHandler::~TestClientAuthHandler() {}

Status TestClientAuthHandler::Authenticate(ClientAuthSender* outgoing,
                                           ClientAuthReader* incoming) {
  RETURN_NOT_OK(outgoing->Write(password_));
  std::string username;
  RETURN_NOT_OK(incoming->Read(&username));
  if (username != username_) {
    return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid token");
  }
  return Status::OK();
}

Status TestClientAuthHandler::GetToken(std::string* token) {
  *token = password_;
  return Status::OK();
}

// TestClientBasicAuthHandler

TestClientBasicAuthHandler::TestClientBasicAuthHandler(const std::string& username,
                                                       const std::string& password) {
  basic_auth_.username = username;
  basic_auth_.password = password;
}

TestClientBasicAuthHandler::~TestClientBasicAuthHandler() {}

Status TestClientBasicAuthHandler::Authenticate(ClientAuthSender* outgoing,
                                                ClientAuthReader* incoming) {
  ARROW_ASSIGN_OR_RAISE(std::string pb_result, basic_auth_.SerializeToString());
  RETURN_NOT_OK(outgoing->Write(pb_result));
  RETURN_NOT_OK(incoming->Read(&token_));
  return Status::OK();
}

Status TestClientBasicAuthHandler::GetToken(std::string* token) {
  *token = token_;
  return Status::OK();
}

}  // namespace arrow::flight
