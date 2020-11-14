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
#include "arrow/util/value_parsing.h"
#include "arrow/util/string.h"
#include "arrow/util/uri.h"

#include <chrono>
#include <map>
#include <mutex>
#include <string>

namespace {
  #ifdef _WIN32
  #define strcasecmp stricmp
  #endif

  struct CaseInsensitiveComparator : public std::binary_function<std::string, std::string, bool> {
    bool operator()(const std::string &lhs, const std::string &rhs) const {
      return strcasecmp(lhs.c_str(), rhs.c_str()) < 0;
    }
  };

  // Parse a cookie header string beginning at the given start_pos and identify the name and value of an attribute.
  //
  // @param cookie_header_value The value of the Set-Cookie header.
  // @param start_pos           An input/output parameter indicating the starting position of the attribute.
  //                            It will store the position of the next attribute when the function returns.
  // @param out_key             The name of the attribute.
  // @param out_value           The value of the attribute.
  //
  // @return true if an attribute is found.
  bool ParseCookieAttribute(std::string cookie_header_value, std::string::size_type& start_pos,
    std::string& out_key, std::string& out_value) {
      std::string::size_type equals_pos = cookie_header_value.find('=', start_pos);
      if (std::string::npos == equals_pos) {
        // No cookie attribute.
        return false;
      }

      std::string::size_type semi_col_pos = cookie_header_value.find(';', equals_pos);
      out_key = arrow::internal::TrimString(cookie_header_value.substr(start_pos, equals_pos));
      if (std::string::npos == semi_col_pos && semi_col_pos > equals_pos) {
        out_value = arrow::internal::TrimString(cookie_header_value.substr(equals_pos+1));
      } else {
        out_value = arrow::internal::TrimString(cookie_header_value.substr(equals_pos+1, semi_col_pos - equals_pos));
      }

      // Key/Value may be URI-encoded.
      out_key = arrow::internal::UriUnescape(arrow::util::string_view(out_key));
      out_value = arrow::internal::UriUnescape(arrow::util::string_view(out_value));

      // Strip outer quotes on the value.
      if (out_value.size() >= 2 && out_value[0] == '"' && out_value[out_value.size() - 1] == '"') {
        out_value = out_value.substr(1, out_value.size() - 2);
      }

      // Update the start position for subsequent calls to this function.
      start_pos = semi_col_pos + 1;
      return true;
  }

  struct Cookie {
    static Cookie parse(const arrow::util::string_view& cookie_header_value) {
      // Parse the cookie string. If the cookie has an expiration, record it.
      // If the cookie has a max-age, calculate the current time + max_age and set that as the expiration.
      Cookie cookie;
      cookie.has_expiry_ = false;
      std::string cookie_value_str(cookie_header_value);

      // There should always be a first match which should be the name and value of the cookie.
      std::string::size_type pos = 0;
      if (!ParseCookieAttribute(cookie_value_str, pos, cookie.cookie_name_, cookie.cookie_value_)) {
        // No cookie found. Mark the output cookie as expired.
        cookie.has_expiry_ = true;
        cookie.expiration_time_ = std::chrono::system_clock::now();
      }

      std::string cookie_attr_name;
      std::string cookie_attr_value;
      while (pos < cookie_value_str.size() &&
          ParseCookieAttribute(cookie_value_str, pos, cookie_attr_name, cookie_attr_value)) {
        if (arrow::internal::AsciiEqualsCaseInsensitive(cookie_attr_name, "max-age")) {
          // Note: max-age takes precedence over expires. We don't really care about other attributes and will
          // arbitrarily take the first max-age. We can stop the loop here.
          cookie.has_expiry_ = true;
          int max_age = std::stoi(cookie_attr_value);
          if (max_age <= 0) {
            // Force expiration.
            cookie.expiration_time_ = std::chrono::system_clock::now();
          } else {
            // Max-age is in seconds.
            cookie.expiration_time_ = std::chrono::system_clock::now() + std::chrono::seconds(max_age);
          }
          break;
        } else if (arrow::internal::AsciiEqualsCaseInsensitive(cookie_attr_name, "expires")) {
          cookie.has_expiry_ = true;
          int64_t seconds = 0;
          const char* COOKIE_EXPIRES_FORMAT = "%a, %d %b %Y %H:%M:%S GMT";
          if (arrow::internal::ParseTimestampStrptime(cookie_attr_value.c_str(), cookie_attr_value.size(),
              COOKIE_EXPIRES_FORMAT, false, true, arrow::TimeUnit::SECOND, &seconds)) {

            cookie.expiration_time_ = std::chrono::system_clock::from_time_t(static_cast<time_t>(seconds));
          }
        }
      }

      return cookie;
    }

    bool IsExpired() const {
      // Check if current-time is less than creation time.
      return has_expiry_ && expiration_time_ >= std::chrono::system_clock::now();
    }

    std::string AsCookieString() {
      // Return the string for the cookie as it would appear in a Cookie header.
      // Keys must be wrapped in quotes depending on if this is a v1 or v2 cookie.
      return cookie_name_ + "=\"" + cookie_value_ + "\"";
    }

    std::string cookie_name_;
    std::string cookie_value_;
    std::chrono::time_point<std::chrono::system_clock> expiration_time_;
    bool has_expiry_;
  };
} // end of anonymous namespace

namespace arrow {

namespace flight {

using CookieCache = std::map<std::string, Cookie, CaseInsensitiveComparator>;

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
    explicit ClientCookieMiddleware(ClientCookieMiddlewareFactory::Impl& factory_impl) :
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
    std::string cookie_string;
    bool first = true;
    for (auto it = cookie_cache_.begin(); cookie_cache_.end() != it; ++it) {
      if (first) {
        first = false;
      } else {
        cookie_string += "; ";
      }
      cookie_string += it->second.AsCookieString();
    }
    return cookie_string;
  }

  // Updates the cache of cookies with new Set-Cookie header values.
  //
  // @param header_values The range representing header values.
  void UpdateCachedCookies(const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>& header_values) {
    const std::lock_guard<std::mutex> guard(mutex_);

    for (auto it = header_values.first; it != header_values.second; ++it) {
      const util::string_view& value = header_values.first->second;
      Cookie cookie = Cookie::parse(value);

      // Cache cookies regardless of whether or not they are expired. The server may have explicitly
      // sent a Set-Cookie to expire a cached cookie.
      cookie_cache_.insert({cookie.cookie_name_, cookie});
    }

    DiscardExpiredCookies();
  }

  // Removes cookies that are marked as expired from the cache.
  void DiscardExpiredCookies() {
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
