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

// (FIXME)

#pragma once

#include "arrow/flight/server_middleware.h"
#include "arrow/flight/sql/types.h"

namespace arrow {
namespace flight {
namespace sql {

class ServerSessionMiddlewareFactory;

/// \brief FIXME
class ARROW_FLIGHT_SQL_EXPORT ServerSessionMiddleware
    : public ServerMiddleware {
 public:
  ~ServerSessionMiddleware();

  static constexpr char const kMiddlewareName[] =
      "arrow::flight::ServerSessionMiddleware";

  std::string name() const override { return kMiddlewareName; }
  void SendingHeaders(AddCallHeaders*) override;
  void CallCompleted(const Status&) override;

  bool HasSession();
  std::vector<SessionOption> *GetSessionOptionsMap();
  const CallHeaders& GetCallHeaders();

 private:
  std::shared_ptr<std::map<std::string, SessionOptionValue>> session_;
  const CallHeaders& headers_;
  const bool existing_session;
  ServerSessionMiddlewareFactory* factory_;


  friend class ServerSessionMiddlewareFactory;

  explicit ServerSessionMiddleware(ServerSessionMiddlewareFactory*,
                                   const CallHeaders& headers);
  ServerSessionMiddleware(ServerSessionMiddlewareFactory*,
                          const CallHeaders&,
                          std::map<std::string, SessionOptionValue>*)
};

/// \brief Returns a ServerMiddlewareFactory that handles Session option storage.
ARROW_FLIGHT_SQL_EXPORT std::shared_ptr<ServerMiddlewareFactory>
MakeServerSessionMiddlewareFactory();

} // namespace sql
} // namespace flight
} // namespace arrow