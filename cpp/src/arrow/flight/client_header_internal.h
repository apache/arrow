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

#pragma once

#include "arrow/flight/client_middleware.h"
#include "arrow/result.h"
#include "arrow/util/optional.h"
#include "arrow/util/value_parsing.h"

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#if defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
#include <grpcpp/security/tls_credentials_options.h>
#endif
#else
#include <grpc++/grpc++.h>
#endif

namespace arrow {
namespace flight {
namespace internal {

/// \brief Case insensitive comparator for use by cookie caching map. Cookies are not
/// case sensitive.
struct CaseInsensitiveComparator
    : public std::binary_function<std::string, std::string, bool> {
  bool operator()(const std::string& lhs, const std::string& rhs) const;
};

/// \brief Class to represent a cookie.
class Cookie {
 public:
  /// \brief Parse function to parse a cookie header value and return a Cookie object.
  ///
  /// \return Cookie object based on cookie header value.
  static Cookie parse(const arrow::util::string_view& cookie_header_value);

  /// \brief Parse a cookie header string beginning at the given start_pos and identify
  /// the name and value of an attribute.
  ///
  /// \param cookie_header_value The value of the Set-Cookie header.
  /// \param[out] start_pos An input/output parameter indicating the starting position
  /// of the attribute. It will store the position of the next attribute when the
  /// function returns.
  ///
  /// \return Optional cookie key value pair.
  static arrow::util::optional<std::pair<std::string, std::string>> ParseCookieAttribute(
      const std::string& cookie_header_value, std::string::size_type* start_pos);

  /// \brief Function to fix cookie format date string so it is accepted by Windows
  ///
  /// parsers.
  /// \param date Date to fix.
  static void ConvertCookieDate(std::string* date);

  /// \brief Function to check if the cookie has expired.
  ///
  /// \return Returns true if the cookie has expired.
  bool IsExpired() const;

  /// \brief Function to get cookie as a string.
  ///
  /// \return Cookie as a string.
  std::string AsCookieString() const;

  /// \brief Function to get name of the cookie as a string.
  ///
  /// \return Name of the cookie as a string.
  std::string GetName() const;

 private:
  std::string cookie_name_;
  std::string cookie_value_;
  std::chrono::time_point<std::chrono::system_clock> expiration_time_;
  bool has_expiry_;
};

/// \brief Class to handle updating a cookie cache.
class CookieCache {
 public:
  /// \brief Updates the cache of cookies with new Set-Cookie header values.
  ///
  /// \param header_values The range representing header values.
  void UpdateCachedCookies(const CallHeaders& incoming_headers);

  /// \brief Retrieve the cached cookie values as a string. This function discards
  /// cookies that have expired.
  ///
  /// \return a string that can be used in a Cookie header representing the cookies that
  /// have been cached.
  std::string GetValidCookiesAsString();

 private:
  /// \brief Removes cookies that are marked as expired from the cache.
  void DiscardExpiredCookies();

  // Mutex must be used to protect cookie cache.
  std::mutex mutex_;
  std::map<std::string, Cookie, CaseInsensitiveComparator> cookies;
};

/// \brief Add basic authentication header key value pair to context.
///
/// \param context grpc context variable to add header to.
/// \param username username to encode into header.
/// \param password password to to encode into header.
void ARROW_FLIGHT_EXPORT AddBasicAuthHeaders(grpc::ClientContext* context,
                                             const std::string& username,
                                             const std::string& password);

/// \brief Get bearer token from incoming headers.
///
/// \param context context that contains headers which hold the bearer token.
/// \return Bearer token, parsed from headers, empty if one is not present.
arrow::Result<std::pair<std::string, std::string>> ARROW_FLIGHT_EXPORT
GetBearerTokenHeader(grpc::ClientContext& context);

}  // namespace internal
}  // namespace flight
}  // namespace arrow
