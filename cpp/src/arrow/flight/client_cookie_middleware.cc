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

#include "arrow/flight/client_cookie_middleware.h"
#include "arrow/flight/client_header_internal.h"
#include "arrow/util/value_parsing.h"

namespace arrow {
namespace flight {

/// \brief Client-side middleware for sending/receiving HTTP style cookies.
class ClientCookieMiddlewareFactory : public ClientMiddlewareFactory {
 public:
  void StartCall(const CallInfo& info, std::unique_ptr<ClientMiddleware>* middleware) {
    ARROW_UNUSED(info);
    *middleware = std::unique_ptr<ClientMiddleware>(new ClientCookieMiddleware(*this));
  }

 private:
  class ClientCookieMiddleware : public ClientMiddleware {
   public:
    explicit ClientCookieMiddleware(ClientCookieMiddlewareFactory& factory)
        : factory_(factory) {}

    void SendingHeaders(AddCallHeaders* outgoing_headers) override {
      const std::string& cookie_string = factory_.cookie_cache_.GetValidCookiesAsString();
      if (!cookie_string.empty()) {
        outgoing_headers->AddHeader("cookie", cookie_string);
      }
    }

    void ReceivedHeaders(const CallHeaders& incoming_headers) override {
      factory_.cookie_cache_.UpdateCachedCookies(incoming_headers);
    }

    void CallCompleted(const Status& status) override {}

   private:
    ClientCookieMiddlewareFactory& factory_;
  };

  // Cookie cache has mutex to protect itself.
  internal::CookieCache cookie_cache_;
};

std::shared_ptr<ClientMiddlewareFactory> GetCookieFactory() {
  return std::make_shared<ClientCookieMiddlewareFactory>();
}

}  // namespace flight
}  // namespace arrow
