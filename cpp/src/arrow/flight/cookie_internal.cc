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

// Interfaces for defining middleware for Flight clients. Currently
// experimental.

#include "arrow/flight/cookie_internal.h"
#include "arrow/flight/client.h"
#include "arrow/flight/client_auth.h"
#include "arrow/flight/platform.h"
#include "arrow/util/base64.h"
#include "arrow/util/string.h"
#include "arrow/util/uri.h"
#include "arrow/util/value_parsing.h"

// Mingw-w64 defines strcasecmp in string.h
#if defined(_WIN32) && !defined(strcasecmp)
#define strcasecmp stricmp
#endif

#include <algorithm>
#include <cctype>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>

const char kCookieExpiresFormat[] = "%d %m %Y %H:%M:%S";

namespace arrow {

using internal::ToChars;

namespace flight {
namespace internal {

using CookiePair = std::optional<std::pair<std::string, std::string>>;
using CookieHeaderPair =
    const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>&;

bool CaseInsensitiveComparator::operator()(const std::string& lhs,
                                           const std::string& rhs) const {
  return strcasecmp(lhs.c_str(), rhs.c_str()) < 0;
}

size_t CaseInsensitiveHash::operator()(const std::string& key) const {
  std::string upper_string = key;
  std::transform(upper_string.begin(), upper_string.end(), upper_string.begin(),
                 ::toupper);
  return std::hash<std::string>{}(upper_string);
}

Cookie Cookie::Parse(std::string_view cookie_header_value) {
  // Parse the cookie string. If the cookie has an expiration, record it.
  // If the cookie has a max-age, calculate the current time + max_age and set that as
  // the expiration.
  Cookie cookie;
  cookie.has_expiry_ = false;
  std::string cookie_value_str(cookie_header_value);

  // There should always be a first match which should be the name and value of the
  // cookie.
  std::string::size_type pos = 0;
  CookiePair cookie_pair = ParseCookieAttribute(cookie_value_str, &pos);
  if (!cookie_pair.has_value()) {
    // No cookie found. Mark the output cookie as expired.
    cookie.has_expiry_ = true;
    cookie.expiration_time_ = std::chrono::system_clock::now();
  } else {
    cookie.cookie_name_ = cookie_pair.value().first;
    cookie.cookie_value_ = cookie_pair.value().second;
  }

  while (pos < cookie_value_str.size()) {
    cookie_pair = ParseCookieAttribute(cookie_value_str, &pos);
    if (!cookie_pair.has_value()) {
      break;
    }

    std::string cookie_attr_value_str = cookie_pair.value().second;
    if (arrow::internal::AsciiEqualsCaseInsensitive(cookie_pair.value().first,
                                                    "max-age")) {
      // Note: max-age takes precedence over expires. We don't really care about other
      // attributes and will arbitrarily take the first max-age. We can stop the loop
      // here.
      cookie.has_expiry_ = true;
      int max_age = -1;
      try {
        max_age = std::stoi(cookie_attr_value_str);
      } catch (...) {
        // stoi throws an exception when it fails, just ignore and leave max_age as -1.
      }

      if (max_age <= 0) {
        // Force expiration.
        cookie.expiration_time_ = std::chrono::system_clock::now();
      } else {
        // Max-age is in seconds.
        cookie.expiration_time_ =
            std::chrono::system_clock::now() + std::chrono::seconds(max_age);
      }
      break;
    } else if (arrow::internal::AsciiEqualsCaseInsensitive(cookie_pair.value().first,
                                                           "expires")) {
      cookie.has_expiry_ = true;
      int64_t seconds = 0;
      ConvertCookieDate(&cookie_attr_value_str);
      if (arrow::internal::ParseTimestampStrptime(
              cookie_attr_value_str.c_str(), cookie_attr_value_str.size(),
              kCookieExpiresFormat, false, true, arrow::TimeUnit::SECOND, &seconds)) {
        cookie.expiration_time_ = std::chrono::time_point<std::chrono::system_clock>(
            std::chrono::seconds(seconds));
      } else {
        // Force expiration.
        cookie.expiration_time_ = std::chrono::system_clock::now();
      }
    }
  }

  return cookie;
}

CookiePair Cookie::ParseCookieAttribute(const std::string& cookie_header_value,
                                        std::string::size_type* start_pos) {
  std::string::size_type equals_pos = cookie_header_value.find('=', *start_pos);
  if (std::string::npos == equals_pos) {
    // No cookie attribute.
    *start_pos = std::string::npos;
    return std::nullopt;
  }

  std::string::size_type semi_col_pos = cookie_header_value.find(';', equals_pos);
  std::string out_key = arrow::internal::TrimString(
      cookie_header_value.substr(*start_pos, equals_pos - *start_pos));
  std::string out_value;
  if (std::string::npos == semi_col_pos) {
    // Last item - set start pos to end
    out_value = arrow::internal::TrimString(cookie_header_value.substr(equals_pos + 1));
    *start_pos = std::string::npos;
  } else {
    out_value = arrow::internal::TrimString(
        cookie_header_value.substr(equals_pos + 1, semi_col_pos - equals_pos - 1));
    *start_pos = semi_col_pos + 1;
  }

  // Key/Value may be URI-encoded.
  out_key = arrow::internal::UriUnescape(out_key);
  out_value = arrow::internal::UriUnescape(out_value);

  // Strip outer quotes on the value.
  if (out_value.size() >= 2 && out_value[0] == '"' &&
      out_value[out_value.size() - 1] == '"') {
    out_value = out_value.substr(1, out_value.size() - 2);
  }

  // Update the start position for subsequent calls to this function.
  return std::make_pair(out_key, out_value);
}

void Cookie::ConvertCookieDate(std::string* date) {
  // Abbreviated months in order.
  static const std::vector<std::string> months = {
      "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"};

  // The date comes in with the following format: Wed, 01 Jan 3000 22:15:36 GMT
  // Symbolics are not supported by Windows parsing, so we need to convert to
  // the following format: 01 01 3000 22:15:36

  // String is currently in regular format: 'Wed, 01 Jan 3000 22:15:36 GMT'
  // Start by removing comma and everything before it, then trimming space.
  auto comma_loc = date->find(",");
  if (comma_loc == std::string::npos) {
    return;
  }
  *date = arrow::internal::TrimString(date->substr(comma_loc + 1));

  // String is now in trimmed format: '01 Jan 3000 22:15:36 GMT'
  // Now swap month to proper month format for Windows.
  // Start by removing case sensitivity.
  std::transform(date->begin(), date->end(), date->begin(), ::toupper);

  // Loop through months.
  for (size_t i = 0; i < months.size(); i++) {
    // Search the date for the month.
    auto it = date->find(months[i]);
    if (it != std::string::npos) {
      // Create month integer, pad with leading zeros if required.
      std::string padded_month;
      if ((i + 1) < 10) {
        padded_month = "0";
      }
      padded_month += ToChars(i + 1);

      // Replace symbolic month with numeric month.
      date->replace(it, months[i].length(), padded_month);

      // String is now in format: '01 01 3000 22:15:36 GMT'.
      break;
    }
  }

  // String is now in format '01 01 3000 22:15:36'.
  auto gmt = date->find(" GMT");
  if (gmt == std::string::npos) {
    return;
  }
  date->erase(gmt, 4);

  // Sometimes a semicolon is added at the end, if this is the case, remove it.
  if (date->back() == ';') {
    date->pop_back();
  }
}

bool Cookie::IsExpired() const {
  // Check if current-time is less than creation time.
  return (has_expiry_ && (expiration_time_ <= std::chrono::system_clock::now()));
}

std::string Cookie::AsCookieString() const {
  // Return the string for the cookie as it would appear in a Cookie header.
  return cookie_name_ + "=" + cookie_value_;
}

std::string Cookie::GetName() const { return cookie_name_; }

void CookieCache::DiscardExpiredCookies() {
  for (auto it = cookies.begin(); it != cookies.end();) {
    if (it->second.IsExpired()) {
      it = cookies.erase(it);
    } else {
      ++it;
    }
  }
}

void CookieCache::UpdateCachedCookies(const CallHeaders& incoming_headers) {
  CookieHeaderPair header_values = incoming_headers.equal_range("set-cookie");
  const std::lock_guard<std::mutex> guard(mutex_);

  for (auto it = header_values.first; it != header_values.second; ++it) {
    const std::string_view& value = it->second;
    Cookie cookie = Cookie::Parse(value);

    // Cache cookies regardless of whether or not they are expired. The server may have
    // explicitly sent a Set-Cookie to expire a cached cookie.
    auto insertable = cookies.insert({cookie.GetName(), cookie});

    // Force overwrite on insert collision.
    if (!insertable.second) {
      insertable.first->second = cookie;
    }
  }
}

std::string CookieCache::GetValidCookiesAsString() {
  const std::lock_guard<std::mutex> guard(mutex_);

  DiscardExpiredCookies();
  if (cookies.empty()) {
    return "";
  }

  std::string cookie_string = cookies.begin()->second.AsCookieString();
  for (auto it = (++cookies.begin()); cookies.end() != it; ++it) {
    cookie_string += "; " + it->second.AsCookieString();
  }
  return cookie_string;
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
