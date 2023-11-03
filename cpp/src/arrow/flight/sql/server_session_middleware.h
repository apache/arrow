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

#include <shared_mutex>
#include <string_view>

#include "arrow/flight/server_middleware.h"
#include "arrow/flight/sql/types.h"

namespace arrow {
namespace flight {
namespace sql {

class ServerSessionMiddlewareFactory;

static constexpr char const kSessionCookieName[] = "flight_sql_session_id";

class FlightSqlSession {
 protected:
  std::map<std::string, SessionOptionValue> map_;
  std::shared_mutex map_lock_;

 public:
  /// \brief Get session option by key
  ::arrow::Result<SessionOptionValue> GetSessionOption(const std::string&);
  /// \brief Set session option by key to given value
  void SetSessionOption(const std::string&, const SessionOptionValue&);
  /// \brief Idempotently remove key from this call's Session, if Session & key exist
  void EraseSessionOption(const std::string&);
};

/// \brief A middleware to handle Session option persistence and related *Cookie headers.
class ARROW_FLIGHT_SQL_EXPORT ServerSessionMiddleware : public ServerMiddleware {
 public:
  static constexpr char const kMiddlewareName[] =
      "arrow::flight::sql::ServerSessionMiddleware";

  std::string name() const override { return kMiddlewareName; }
  void SendingHeaders(AddCallHeaders*) override;
  void CallCompleted(const Status&) override;

  /// \brief Is there an existing session (either existing or new)
  bool HasSession() const;
  /// \brief Get existing or new call-associated session
  std::shared_ptr<FlightSqlSession> GetSession();
  /// \brief Get request headers, in lieu of a provided or created session.
  const CallHeaders& GetCallHeaders() const;

 protected:
  friend class ServerSessionMiddlewareFactory;
  ServerSessionMiddlewareFactory* factory_;
  const CallHeaders& headers_;
  std::shared_ptr<FlightSqlSession> session_;
  std::string session_id_;
  const bool existing_session;

  ServerSessionMiddleware(ServerSessionMiddlewareFactory*, const CallHeaders&);
  ServerSessionMiddleware(ServerSessionMiddlewareFactory*, const CallHeaders&,
                          std::shared_ptr<FlightSqlSession>, std::string session_id);
};

/// \brief Returns a ServerMiddlewareFactory that handles Session option storage.
ARROW_FLIGHT_SQL_EXPORT std::shared_ptr<ServerMiddlewareFactory>
MakeServerSessionMiddlewareFactory();

}  // namespace sql
}  // namespace flight
}  // namespace arrow
