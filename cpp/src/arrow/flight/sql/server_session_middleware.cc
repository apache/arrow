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

#include "arrow/flight/sql/server_session_middleware.h"
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace arrow {
namespace flight {
namespace sql {

ServerSessionMiddleware::ServerSessionMiddleware(ServerSessionMiddlewareFactory* factory,
                                                 const CallHeaders& headers)
    : factory_(factory), headers_(headers), existing_session(false) {}
ServerSessionMiddleware::ServerSessionMiddleware(
    ServerSessionMiddlewareFactory* factory, const CallHeaders& headers,
    std::shared_ptr<std::map<std::string, SessionOptionValue>> session)
    : factory_(factory), headers_(headers), session_(session), existing_session(true) {}

void ServerSessionMiddleware::SendingHeaders(AddCallHeaders* addCallHeaders) {
  if (!existing_session && session_) {
    // FIXME impl
    // add Set-Cookie header w/ flight_sql_session_id
    // FIXME I also need to know my own session key...
  }
}
void ServerSessionMiddleware::CallCompleted(const Status&) {}
bool ServerSessionMiddleware::HasSession() {
  return static_cast<bool> session_;
}
std::shared_ptr<std::map<std::string, SessionOptionValue>>
ServerSessionMiddleware::GetSession() {
  if (session_)
    return session_;
  else {
    auto new_session = factory_->GetNewSession();
    // FIXME
  }
}
const CallHeaders& ServerSessionMiddleware::GetCallHeaders() {
  return headers_;
}

/// \brief FIXME
class ServerSessionMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  Status StartCall(const CallInfo &info, const CallHeaders &incoming_headers,
                   std::shared_ptr<ServerMiddleware> *middleware) {
    // FIXME impl

  }
  std::pair<std::string, std::shared_ptr<std::map<std::string, SessionOptionValue>>>
  GetNewSession() {
    // FIXME impl
  }
 private:
  std::map<std::string,
      std::shared_ptr<std::map<std::string, SessionOptionValue>>> session_store_;
  boost::uuids::random_generator uuid_generator_;
};

std::shared_ptr<ServerMiddlewareFactory> MakeServerSessionMiddlewareFactory() {
  return std::make_shared<ServerSessionMiddlewareFactory>();
}

} // namespace sql
} // namespace flight
} // namespace arrow