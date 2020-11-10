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

// Platform-specific defines
#include "arrow/flight/platform.h"

namespace arrow {

namespace flight {

class ClientCookieMiddleware::Impl {
 public:
  void SendingHeaders(AddCallHeaders* outgoing_headers) {
  }

  void ReceivedHeaders(const CallHeaders& incoming_headers) {
      // Parse the Set-Cookie header values here, and write to the collection managed in Factory::Impl
  }

  void CallCompleted(const Status& status) {
  }
};

class ClientCookieMiddleware::Factory::Impl {
 public:
  void StartCall(const CallInfo& info,
                            std::unique_ptr<ClientMiddleware::Impl>* middleware) {
    // Instantiate middleware and have it store the 
  }
 private:
      // Create a object class and store a collection of them here.
      // Needs to be a concurrent data structure, or use locks to protected it.

};

void ClientCookieMiddleware::SendingHeaders(AddCallHeaders* outgoing_headers) {
  impl_->SendingHeaders(outgoing_headers);
}

void ClientCookieMiddleware::ReceivedHeaders(const CallHeaders& incoming_headers) {
  impl_->ReceivedHeaders(incoming_headers);
}

void ClientCookieMiddleware::CallCompleted(const Status& status) {
  impl_->CallCompleted(status);

ClientCookieMiddleware::ClientCookieMiddleware() {
}

ClientCookieMiddleware::Factory::Factory() {
}

Cli

}  // namespace flight
}  // namespace arrow