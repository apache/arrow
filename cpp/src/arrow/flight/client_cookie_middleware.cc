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

namespace {
  class Cookie {
   public:
    static Cookie parse(const util::string_view& cookie_value) {
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

   private:
    std::string cookie_name_;
    std::string cookie_value_;
    time_t expiration_time_;
    bool is_v1_cookie_;
  };
} // end of anonymous namespace

namespace arrow {

namespace flight {

using CookieCache = std::map<std::string, std::string>;

class ClientCookieMiddleware::Impl {
 public:
  void SendingHeaders(AddCallHeaders* outgoing_headers) {
    const std::string& cookie_string = factory_impl_.GetValidCookiesAsString();
    if (!cookie_string.empty()) {
      outgoing_headers->AddHeader("Cookie", cookie_string);
    }
  }

  void ReceivedHeaders(const CallHeaders& incoming_headers) {
    const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>& iter_pair =
      incoming_headers.equal_range("Set-Cookie");
    for (auto it = iter_pair.first; it != iter_pair.second; ++it) {
      // Alternatively we can send every cookie to the factory in one call to reduce
      // the amount of locking (by sending the interval of Set-Cookie values).
      const util::string_view& value = (*iter_pair.first).second;
      factory_impl_.UpdateCachedCookie(value);
    }

    factory_impl_.DiscardExpiredCookies();
  }

  void CallCompleted(const Status& status) {
  }
 private:
  Factory::Impl factory_impl_;
  Impl(Factory::Impl& factory_impl) :
   factory_impl_(factory_impl) {
   }
};

class ClientCookieMiddleware::Factory::Impl {
 public:
  void StartCall(const CallInfo& info,
                            std::unique_ptr<ClientMiddleware>* middleware) {
    ARROW_UNUSED(info);
    std::unique_ptr<ClientCookieMiddleware> cookie_middleware = std::make_shared<ClientCookieMiddleware>();
    cookie_middleware->impl_ = std::make_shared(*this);
    *middleware = std::move(cookie_middleware);
  }
 private:
  std::string GetValidCookiesAsString() const {
    return "";
  }

  void UpdateCachedCookie(const util::string_view& cookie_value) {
    std::lock_guard<std::mutex> guard(mutex_);

    // Parse the Set-Cookie header values here
  }

  void DiscardExpiredCookies() {
    std::lock_guard<std::mutex> guard(mutex_);

    for (auto it = cookie_cache_.begin(); it != cookie_cache_.end(); ) {
      if (it->second.IsExpired()) {
        it = cookie_cache_.erase(it);
      } else {
        ++it;
      }
    }
  }

  CookieCache cookie_cache_;
  std::mutex mutex_;
};

void ClientCookieMiddleware::SendingHeaders(AddCallHeaders* outgoing_headers) {
  impl_->SendingHeaders(outgoing_headers);
}

void ClientCookieMiddleware::ReceivedHeaders(const CallHeaders& incoming_headers) {
  impl_->ReceivedHeaders(incoming_headers);
}

void ClientCookieMiddleware::CallCompleted(const Status& status) {
  impl_->CallCompleted(status);
}

ClientCookieMiddleware::ClientCookieMiddleware() {
}

ClientCookieMiddleware::Factory::Factory() :
 impl_(std::make_shared<>()) {

}

}  // namespace flight
}  // namespace arrow