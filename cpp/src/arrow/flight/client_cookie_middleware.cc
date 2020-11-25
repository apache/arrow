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

#include <chrono>
#include <map>
#include <mutex>
#include <string>

#include "arrow/flight/platform.h"
#include "arrow/util/string.h"
#include "arrow/util/uri.h"
#include "arrow/util/value_parsing.h"

namespace {
#ifdef _WIN32
#define strcasecmp stricmp
#endif

struct CaseInsensitiveComparator
    : public std::binary_function<std::string, std::string, bool> {
  bool operator()(const std::string& lhs, const std::string& rhs) const {
    return strcasecmp(lhs.c_str(), rhs.c_str()) < 0;
  }
};

// Parse a cookie header string beginning at the given start_pos and identify the name and
// value of an attribute.
//
// @param cookie_header_value The value of the Set-Cookie header.
// @param start_pos           An input/output parameter indicating the starting position
// of the attribute.
//                            It will store the position of the next attribute when the
//                            function returns.
// @param out_key             The name of the attribute.
// @param out_value           The value of the attribute.
//
// @return true if an attribute is found.
bool ParseCookieAttribute(std::string cookie_header_value,
                          std::string::size_type& start_pos, std::string& out_key,
                          std::string& out_value) {
  std::string::size_type equals_pos = cookie_header_value.find('=', start_pos);
  if (std::string::npos == equals_pos) {
    // No cookie attribute.
    return false;
  }

  std::string::size_type semi_col_pos = cookie_header_value.find(';', equals_pos);
  out_key = arrow::internal::TrimString(
      cookie_header_value.substr(start_pos, equals_pos - start_pos));
  if (std::string::npos == semi_col_pos && semi_col_pos > equals_pos) {
    // Last item - set start pos to end
    out_value = arrow::internal::TrimString(cookie_header_value.substr(equals_pos + 1));
    start_pos = std::string::npos;
  } else {
    out_value = arrow::internal::TrimString(
        cookie_header_value.substr(equals_pos + 1, semi_col_pos - equals_pos - 1));
    start_pos = semi_col_pos + 1;
  }

  // Key/Value may be URI-encoded.
  out_key = arrow::internal::UriUnescape(arrow::util::string_view(out_key));
  out_value = arrow::internal::UriUnescape(arrow::util::string_view(out_value));

  // Strip outer quotes on the value.
  if (out_value.size() >= 2 && out_value[0] == '"' &&
      out_value[out_value.size() - 1] == '"') {
    out_value = out_value.substr(1, out_value.size() - 2);
  }

  // Update the start position for subsequent calls to this function.
  return true;
}

// Custom parser for the date in format expected for cookies. This is required because
// Windows doesn't have support for multiple formatting requirements in the cookie
// formatting.
//
// @param date Input formatted date to parse.
//
// @return 0 on error, epoch seconds for date otherwise.
std::chrono::seconds ParseDate(const std::string& date) {
  // Abbreviated months in order.
  static const std::vector<std::string> months = {
      "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"};

  // Lambda function to convert abbreviated month string to number.
  auto month_str_to_int = [](std::string month) {
    std::transform(month.begin(), month.end(), month.begin(), ::toupper);
    auto it = std::find(months.begin(), months.end(), month);
    if (it != months.end()) {
      return static_cast<int32_t>(std::distance(months.begin(), it));
    } else {
      return int32_t(-1);
    }
  };

  // Lambda function to look in date string for token using offset and size.
  auto date_find = [&](const std::string& token, size_t& offset, size_t size) {
    offset = date.find(token, offset);
    if (offset == std::string::npos) {
      return std::string("");
    }
    offset += token.length();
    return date.substr(offset + 1, size);
  };

  // Lambda function to read incoming stream to a long.
  auto read_str = [](const std::string& str) {
    char* end_ptr;
    int32_t val = static_cast<int32_t>(std::strtol(str.c_str(), &end_ptr, 10));
    if (end_ptr == str.c_str()) {
      val = int32_t(-1);
    }
    return val;
  };

  // Parse out day, month, year, hour, minute, and second. If any come back empty, return
  // 0.
  size_t offset = 0;
  const std::string str_day = date_find(",", offset, 2);
  const std::string str_month = date_find(str_day, offset, 3);
  const std::string str_year = date_find(str_month, offset, 4);
  const std::string str_hour = date_find(str_year, offset, 2);
  const std::string str_min = date_find(str_hour, offset, 2);
  const std::string str_sec = date_find(str_min, offset, 2);
  if (str_month.empty() || str_day.empty() || str_year.empty() || str_hour.empty() ||
      str_min.empty() || str_sec.empty()) {
    return std::chrono::seconds(0);
  }

  // Attempt to convert parsed values to longs. If any come back as -1, return 0.
  int32_t month = month_str_to_int(str_month);
  int32_t day = read_str(str_day);
  int32_t year = read_str(str_year);
  int32_t hour = read_str(str_hour);
  int32_t min = read_str(str_min);
  int32_t sec = read_str(str_sec);

  if ((year == -1) || (month == -1) || (day == -1) || (hour == -1) || (min == -1) ||
      (sec == -1)) {
    return std::chrono::seconds(0);
  }

  arrow_vendored::date::sys_seconds secs =
      arrow_vendored::date::sys_days(arrow_vendored::date::year(year) / (month + 1) /
                                     day) +
      (std::chrono::hours(hour) + std::chrono::minutes(min) + std::chrono::seconds(sec));
  return secs.time_since_epoch();
}

struct Cookie {
  static Cookie parse(const arrow::util::string_view& cookie_header_value) {
    // Parse the cookie string. If the cookie has an expiration, record it.
    // If the cookie has a max-age, calculate the current time + max_age and set that as
    // the expiration.
    Cookie cookie;
    cookie.has_expiry_ = false;
    std::string cookie_value_str(cookie_header_value);

    // There should always be a first match which should be the name and value of the
    // cookie.
    std::string::size_type pos = 0;
    if (!ParseCookieAttribute(cookie_value_str, pos, cookie.cookie_name_,
                              cookie.cookie_value_)) {
      // No cookie found. Mark the output cookie as expired.
      cookie.has_expiry_ = true;
      cookie.expiration_time_ = std::chrono::system_clock::now();
    }

    std::string cookie_attr_name;
    std::string cookie_attr_value;
    while (pos < cookie_value_str.size() &&
           ParseCookieAttribute(cookie_value_str, pos, cookie_attr_name,
                                cookie_attr_value)) {
      if (arrow::internal::AsciiEqualsCaseInsensitive(cookie_attr_name, "max-age")) {
        // Note: max-age takes precedence over expires. We don't really care about other
        // attributes and will arbitrarily take the first max-age. We can stop the loop
        // here.
        cookie.has_expiry_ = true;
        int max_age = std::stoi(cookie_attr_value);
        if (max_age <= 0) {
          // Force expiration.
          cookie.expiration_time_ = std::chrono::system_clock::now();
        } else {
          // Max-age is in seconds.
          cookie.expiration_time_ =
              std::chrono::system_clock::now() + std::chrono::seconds(max_age);
        }
        break;
      } else if (arrow::internal::AsciiEqualsCaseInsensitive(cookie_attr_name,
                                                             "expires")) {
        cookie.has_expiry_ = true;
        cookie.expiration_time_ = std::chrono::time_point<std::chrono::system_clock>(
            ParseDate(cookie_attr_value));
      }
    }

    return cookie;
  }

  bool IsExpired() const {
    // Check if current-time is less than creation time.
    if (has_expiry_) {
      // Converting std::chrono time_points to uint64_t ticks because MinGW returns a
      // negative number for large values.
      uint64_t expiry_time =
          static_cast<uint64_t>(expiration_time_.time_since_epoch().count());
      uint64_t current_time = static_cast<uint64_t>(
          std::chrono::system_clock::now().time_since_epoch().count());
      return (expiry_time <= current_time);
    }
    return false;
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
}  // end of anonymous namespace

namespace arrow {

namespace flight {

using CookieCache = std::map<std::string, Cookie, CaseInsensitiveComparator>;

class ClientCookieMiddlewareFactory::Impl {
 public:
  void StartCall(const CallInfo& info, std::unique_ptr<ClientMiddleware>* middleware) {
    ARROW_UNUSED(info);
    *middleware = std::unique_ptr<ClientMiddleware>(new ClientCookieMiddleware(*this));
  }

 private:
  class ClientCookieMiddleware : public ClientMiddleware {
   public:
    explicit ClientCookieMiddleware(ClientCookieMiddlewareFactory::Impl& factory_impl)
        : factory_impl_(factory_impl) {}

    void SendingHeaders(AddCallHeaders* outgoing_headers) override {
      const std::string& cookie_string = factory_impl_.GetValidCookiesAsString();
      if (!cookie_string.empty()) {
        outgoing_headers->AddHeader("cookie", cookie_string);
      }
    }

    void ReceivedHeaders(const CallHeaders& incoming_headers) override {
      const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>&
          cookie_header_values = incoming_headers.equal_range("set-cookie");
      factory_impl_.UpdateCachedCookies(cookie_header_values);
    }

    void CallCompleted(const Status& status) override {}

   private:
    ClientCookieMiddlewareFactory::Impl& factory_impl_;
  };

  // Retrieve the cached cookie values as a string.
  //
  // @return a string that can be used in a Cookie header representing the cookies that
  // have been cached.
  std::string GetValidCookiesAsString() {
    const std::lock_guard<std::mutex> guard(mutex_);

    DiscardExpiredCookies();
    if (cookie_cache_.empty()) {
      return "";
    }

    std::string cookie_string = cookie_cache_.begin()->second.AsCookieString();
    for (auto it = (++cookie_cache_.begin()); cookie_cache_.end() != it; ++it) {
      cookie_string += "; " + it->second.AsCookieString();
    }
    return cookie_string;
  }

  // Updates the cache of cookies with new Set-Cookie header values.
  //
  // @param header_values The range representing header values.
  void UpdateCachedCookies(const std::pair<CallHeaders::const_iterator,
                                           CallHeaders::const_iterator>& header_values) {
    const std::lock_guard<std::mutex> guard(mutex_);

    for (auto it = header_values.first; it != header_values.second; ++it) {
      const util::string_view& value = it->second;
      Cookie cookie = Cookie::parse(value);

      // Cache cookies regardless of whether or not they are expired. The server may have
      // explicitly sent a Set-Cookie to expire a cached cookie.
      std::pair<CookieCache::iterator, bool> insertable =
          cookie_cache_.insert({cookie.cookie_name_, cookie});

      // Force overwrite on insert collision.
      if (!insertable.second) {
        insertable.first->second = cookie;
      }
    }

    DiscardExpiredCookies();
  }

  // Removes cookies that are marked as expired from the cache.
  void DiscardExpiredCookies() {
    for (auto it = cookie_cache_.begin(); it != cookie_cache_.end();) {
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

ClientCookieMiddlewareFactory::ClientCookieMiddlewareFactory()
    : impl_(new ClientCookieMiddlewareFactory::Impl()) {}

ClientCookieMiddlewareFactory::~ClientCookieMiddlewareFactory() {}

void ClientCookieMiddlewareFactory::StartCall(
    const CallInfo& info, std::unique_ptr<ClientMiddleware>* middleware) {
  impl_->StartCall(info, middleware);
}

}  // namespace flight
}  // namespace arrow
