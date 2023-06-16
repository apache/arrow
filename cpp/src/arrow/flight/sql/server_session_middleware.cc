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

#include <shared_mutex>
#include "arrow/flight/sql/server_session_middleware.h"
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace arrow {
namespace flight {
namespace sql {

// FIXME there isn't a sensible public client/server common header to move this to(?)
static constexpr char const kSessionCookieName[] =
    "flight_sql_session_id";

ServerSessionMiddleware::ServerSessionMiddleware(ServerSessionMiddlewareFactory* factory,
                                                 const CallHeaders& headers)
    : factory_(factory), headers_(headers), existing_session(false) {}

ServerSessionMiddleware::ServerSessionMiddleware(
    ServerSessionMiddlewareFactory* factory, const CallHeaders& headers,
    std::shared_ptr<std::map<std::string, SessionOptionValue>> session,
    std::string session_id)
    : factory_(factory), headers_(headers), session_(session), existing_session(true) {}

void ServerSessionMiddleware::SendingHeaders(AddCallHeaders* addCallHeaders) {
  if (!existing_session && session_) {
    // FIXME impl
    // add Set-Cookie header w/ flight_sql_session_id
    // FIXME I also need to know my own session key... (same fixme as header)
  }
}
void ServerSessionMiddleware::CallCompleted(const Status&) {}

bool ServerSessionMiddleware::HasSession() {
  return static_cast<bool> session_;
}
std::shared_ptr<std::map<std::string, SessionOptionValue>>&
ServerSessionMiddleware::GetSession() {
  if (!session_)
    session_ = factory_->GetNewSession(&session_id_);
  return session_;
}
const CallHeaders& ServerSessionMiddleware::GetCallHeaders() {
  return headers_;
}

/// \brief A factory for ServerSessionMiddleware, itself storing session data.
class ServerSessionMiddlewareFactory : public ServerMiddlewareFactory {
 private:
  std::map<std::string,
           std::shared_ptr<std::map<std::string, SessionOptionValue>>> session_store_;
  std::share_mutex session_store_lock_;
  boost::uuids::random_generator uuid_generator_;

  std::vector<std::pair<std::string, std::string>> ParseCookieString(
      const std::string_view& s) {
    const std::string list_sep = "; ";
    const std::string pair_sep = "=";
    const size_t pair_sep_len = pair_sep.len();

    std::vector<std::pair<std::string>> result;

    size_t cur = 0;
    while (cur < s.length()) {
      const size_t end = s.find(list_sep, cur);
      size_t len;
      if (end == std::string::npos) {
        // No (further) list delimiters
        len = str::string::npos;
        cur = s.length();
      } else {
        len = end - cur;
        cur = end;
      }
      const std::string tok = s.substr(cur, len);

      const size_t val_pos = tok.find(pair_sep);
      result.emplace_back(
        tok.substr(0, val_pos),
        tok.substr(val_pos + pair_sep_len, std::string::npos);
      );
    }

    return result;
  }

 public:
  Status StartCall(const CallInfo &, const CallHeaders &incoming_headers,
                   std::shared_ptr<ServerMiddleware> *middleware) {
    std::string session_id;

    const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>&
        headers_it_pr = incoming_headers.equal_range("cookie");
    for (auto itr = headers_it_pr.first; itr != headers_it_pr.second, ++itr) {
      const std::string_view& cookie_header = it->second;
      const std::vector<std::pair<std::string, std::string>> cookies =
          ParseCookieString(cookie_header);
      for (const std::pair<std::string, std::string>& cookie : cookies) {
        if (cookie.first == kSessionCookieName) {
          session_id = cookie.second;
          if (!session_id.length())
            return Status::Invalid("Empty " + kSessionCookieName + " cookie value.")
        }
      }
      if (session_id.length()) break;
    }

    if (!session_id.length()) {
      // No cookie was found
      *middleware = std::make_shared<ServerSessionMiddleware>(this, incoming_headers);
    } else {
      try {
        const std::shared_lock<std::shared_mutex> l(session_store_lock_);
        auto session = session_store_.at(session_id);
        *middleware = std::make_shared<ServerSessionMiddleware>(
            this, incoming_headers,  session, session_id);
      } catch (std::out_of_range& e) {
        return Status::Invalid("Invalid or expired " + kSessionCookieName + " cookie.")
      }
    }

    return Status::OK();
  }

  /// \brief Get a new, empty session option map and its id key.
  std::shared_ptr<std::map<std::string, SessionOptionValue>>
  GetNewSession(std::string* session_id) {
    std::string new_id = boost::lexical_cast<std::string>(uuid_generator_());
    *session_id = new_id;
    auto session = std::make_shared<std::map<std::string, SessionOptionValue>>();

    const std::unique_lock<std::shared_mutex> l(session_store_lock_);
    session_store_[new_id] = session;
    
    return session;
  }
};

std::shared_ptr<ServerMiddlewareFactory> MakeServerSessionMiddlewareFactory() {
  return std::make_shared<ServerSessionMiddlewareFactory>();
}

} // namespace sql
} // namespace flight
} // namespace arrow