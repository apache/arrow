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

#include "arrow/flight/client_header_auth_middleware_internal.h"
#include "arrow/flight/client.h"
#include "arrow/flight/client_auth.h"
#include "arrow/util/base64.h"
#include "arrow/util/make_unique.h"

#include <algorithm>
#include <cctype>
#include <memory>
#include <string>

const char kAuthHeader[] = "authorization";
const char kBearerPrefix[] = "Bearer ";
const char kBasicPrefix[] = "Basic ";

namespace arrow {
namespace flight {
namespace internal {

// Add base64 encoded credentials to the outbound headers.
//
// @param context Context object to add the headers to.
// @param username Username to format and encode.
// @param password Password to format and encode.
void AddBasicAuthHeaders(grpc::ClientContext* context, const std::string& username,
                         const std::string& password) {
  const std::string credentials = username + ":" + password;
  context->AddMetadata(
      kAuthHeader,
      kBasicPrefix + arrow::util::base64_encode((const unsigned char*)credentials.c_str(),
                                                credentials.size()));
}

class ClientBearerTokenFactory::Impl {
 public:
  explicit Impl(std::pair<std::string, std::string>* bearer_token)
      : bearer_token_(bearer_token) {}

  void StartCall(const CallInfo& info, std::unique_ptr<ClientMiddleware>* middleware) {
    ARROW_UNUSED(info);
    *middleware =
        arrow::internal::make_unique<ClientBearerTokenMiddleware>(bearer_token_);
  }

 private:
  class ClientBearerTokenMiddleware : public ClientMiddleware {
   public:
    explicit ClientBearerTokenMiddleware(
        std::pair<std::string, std::string>* bearer_token)
        : bearer_token_(bearer_token) {}

    void SendingHeaders(AddCallHeaders* outgoing_headers) override {}

    void ReceivedHeaders(const CallHeaders& incoming_headers) override {
      // Lambda function to compare characters without case sensitivity.
      auto char_compare = [](const char& char1, const char& char2) {
        return (std::toupper(char1) == std::toupper(char2));
      };

      // Grab the auth token if one exists.
      const auto bearer_iter = incoming_headers.find(kAuthHeader);
      if (bearer_iter == incoming_headers.end()) {
        return;
      }

      // Check if the value of the auth token starts with the bearer prefix and latch it.
      const std::string bearer_val = bearer_iter->second.to_string();
      if (bearer_val.size() > strlen(kBearerPrefix)) {
        if (std::equal(bearer_val.begin(), bearer_val.begin() + strlen(kBearerPrefix),
                       kBearerPrefix, char_compare)) {
          *bearer_token_ = std::make_pair(kAuthHeader, bearer_val);
        }
      }
    }

    void CallCompleted(const Status& status) override {}

   private:
    std::pair<std::string, std::string>* bearer_token_;
  };

 private:
  std::pair<std::string, std::string>* bearer_token_;
};

ClientBearerTokenFactory::ClientBearerTokenFactory(
    std::pair<std::string, std::string>* bearer_token)
    : impl_(new ClientBearerTokenFactory::Impl(bearer_token)) {}

ClientBearerTokenFactory::~ClientBearerTokenFactory() {}

void ClientBearerTokenFactory::StartCall(const CallInfo& info,
                                         std::unique_ptr<ClientMiddleware>* middleware) {
  impl_->StartCall(info, middleware);
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
