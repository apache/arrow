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

#include "arrow/util/make_unique.h"

#include <map>
#include <mutex>
#include <string>

namespace {
  struct Cookie {
    static Cookie parse(const arrow::util::string_view& cookie_value) {
      // Parse the cookie string. If the cookie has an expiration, record it.
      // If the cookie has a max-age, calculate the current time + max_age and set that as the expiration.
      return Cookie();
    }

    bool IsExpired() {
      // Check if current-time is less than creation time.
      return false;
    }

    std::string AsCookieString() {
      // Return the string for the cookie as it would appear in a Cookie header.
      // Keys must be wrapped in quotes depending on if this is a v1 or v2 cookie.
      return "";
    }

    std::string cookie_name_;
    std::string cookie_value_;
    time_t expiration_time_;
    bool is_v1_cookie_;
  };
} // end of anonymous namespace

namespace arrow {

namespace flight {

using CookieCache = std::map<std::string, Cookie>;

class ClientCookieMiddlewareFactory::Impl {
 public:
  void StartCall(const CallInfo& info,
                            std::unique_ptr<ClientMiddleware>* middleware) {
    ARROW_UNUSED(info);
    *middleware = internal::make_unique<ClientCookieMiddleware>(*this);
  }
 private:
  class ClientCookieMiddleware : public ClientMiddleware {
   public:
    // This is public so that it can be used with make_unique.
    ClientCookieMiddleware(ClientCookieMiddlewareFactory::Impl& factory_impl) :
     factory_impl_(factory_impl) {
    }

    void SendingHeaders(AddCallHeaders* outgoing_headers) override {
      const std::string& cookie_string = factory_impl_.GetValidCookiesAsString();
      if (!cookie_string.empty()) {
        outgoing_headers->AddHeader("Cookie", cookie_string);
      }
    }

    void ReceivedHeaders(const CallHeaders& incoming_headers) override {
      const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>& cookie_header_values =
        incoming_headers.equal_range("Set-Cookie");
      factory_impl_.UpdateCachedCookies(cookie_header_values);
    }

    void CallCompleted(const Status& status) override {
    }
   private:
    ClientCookieMiddlewareFactory::Impl& factory_impl_;
  };

  // Retrieve the cached cookie values as a string.
  //
  // @return a string that can be used in a Cookie header representing the cookies that have been cached.
  std::string GetValidCookiesAsString() {
    const std::lock_guard<std::mutex> guard(mutex_);

    DiscardExpiredCookies();
    return "";
  }

  // Updates the cache of cookies with new Set-Cookie header values.
  //
  // @param header_values The range representing header values.
  void UpdateCachedCookies(const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>& header_values) {
    const std::lock_guard<std::mutex> guard(mutex_);

    for (auto it = header_values.first; it != header_values.second; ++it) {
      const util::string_view& value = header_values.first->second;
      Cookie cookie = Cookie::parse(value);
      cookie_cache_.insert({cookie.cookie_name_, cookie});
    }

    DiscardExpiredCookies();
  }

  // Removes cookies that are marked as expired from the cache.
  void DiscardExpiredCookies() {
    const std::lock_guard<std::mutex> guard(mutex_);

    for (auto it = cookie_cache_.begin(); it != cookie_cache_.end(); ) {
      if (it->second.IsExpired()) {
        it = cookie_cache_.erase(it);
      } else {
        ++it;
      }
    }
  }

  // The cached cookies. Access should be protected by mutex_.
  CookieCache cookie_cache_;
  std::mutex mutex_;
};

ClientCookieMiddlewareFactory::ClientCookieMiddlewareFactory() :
 impl_(internal::make_unique<ClientCookieMiddlewareFactory::Impl>()) {
}

}  // namespace flight
}  // namespace arrow