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

// Middleware implementation for sending and receiving HTTP cookies.

#pragma once

#include <memory>

#include "arrow/flight/client_middleware.h"

namespace arrow {
namespace flight {

/// \brief Client-side middleware for sending/receiving HTTP cookies.
class ARROW_FLIGHT_EXPORT ClientCookieMiddleware : public ClientMiddleware {
 public:
  void SendingHeaders(AddCallHeaders* outgoing_headers) override;

  void ReceivedHeaders(const CallHeaders& incoming_headers) override;

  void CallCompleted(const Status& status) override;

  class Factory : public ClientMiddlewareFactory {
   public:
     void StartCall(const CallInfo& info,
                            std::unique_ptr<ClientMiddleware>* middleware) override;
   private:
     Factory();
     class Impl;
     std::unique_ptr<Factory::Impl> impl_;
  };

 private:
  ClientCookieMiddleware();
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace flight
}  // namespace arrow
