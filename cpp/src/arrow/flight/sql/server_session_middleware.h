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

// Middleware for handling Flight SQL Sessions including session cookie handling.
// Currently experimental.

#pragma once

#include "arrow/flight/server_middleware.h"
#include "arrow/flight/sql/types.h"

namespace arrow {
namespace flight {
namespace sql {

class ServerSessionMiddlewareFactory;

// FIXME put the cookie name somewhere static out here

/// \brief A middleware to handle Session option persistence and related *Cookie headers.
class ARROW_FLIGHT_SQL_EXPORT ServerSessionMiddleware
    : public ServerMiddleware {
 public:
  static constexpr char const kMiddlewareName[] =
      "arrow::flight::sql::ServerSessionMiddleware";

  std::string name() const override { return kMiddlewareName; }
  void SendingHeaders(AddCallHeaders*) override;
  void CallCompleted(const Status&) override;

  /// \brief Is there an existing session (either existing or new)
  bool HasSession() const;
  /// \brief Get a mutable Map of session options.
  std::shared_ptr<std::map<std::string, SessionOption>> GetSession(std::string*);
  /// \brief Get request headers, in lieu of a provided or created session.
  const CallHeaders& GetCallHeaders() const;

 private:
    friend class ServerSessionMiddlewareFactory;
  std::shared_ptr<std::map<std::string, SessionOptionValue>> session_;
  std::string session_id_;
  const CallHeaders& headers_;
  const bool existing_session;
  ServerSessionMiddlewareFactory* factory_;

  explicit ServerSessionMiddleware(ServerSessionMiddlewareFactory*,
                                   const CallHeaders& headers);
  ServerSessionMiddleware(ServerSessionMiddlewareFactory*,
                          const CallHeaders&,
                          std::shared_ptr<std::map<std::string, SessionOptionValue>>,
                          std::string)
};

/// \brief Returns a ServerMiddlewareFactory that handles Session option storage.
ARROW_FLIGHT_SQL_EXPORT std::shared_ptr<ServerMiddlewareFactory>
MakeServerSessionMiddlewareFactory();

} // namespace sql
} // namespace flight
} // namespace arrow