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

#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/result.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Status;

ARROW_EXPORT std::string HexEncode(const uint8_t* data, size_t length);

ARROW_EXPORT std::string Escape(const char* data, size_t length);

ARROW_EXPORT std::string HexEncode(const char* data, size_t length);

ARROW_EXPORT std::string HexEncode(std::string_view str);

ARROW_EXPORT std::string Escape(std::string_view str);

ARROW_EXPORT Status ParseHexValue(const char* data, uint8_t* out);

namespace internal {

/// Like std::string_view::starts_with in C++20
inline bool StartsWith(std::string_view s, std::string_view prefix) {
  return s.length() >= prefix.length() &&
         (s.empty() || s.substr(0, prefix.length()) == prefix);
}

/// Like std::string_view::ends_with in C++20
inline bool EndsWith(std::string_view s, std::string_view suffix) {
  return s.length() >= suffix.length() &&
         (s.empty() || s.substr(s.length() - suffix.length()) == suffix);
}

/// \brief Split a string with a delimiter
ARROW_EXPORT
std::vector<std::string_view> SplitString(std::string_view v, char delim,
                                          int64_t limit = 0);

/// \brief Join strings with a delimiter
ARROW_EXPORT
std::string JoinStrings(const std::vector<std::string_view>& strings,
                        std::string_view delimiter);

/// \brief Join strings with a delimiter
ARROW_EXPORT
std::string JoinStrings(const std::vector<std::string>& strings,
                        std::string_view delimiter);

/// \brief Trim whitespace from left and right sides of string
ARROW_EXPORT
std::string TrimString(std::string value);

ARROW_EXPORT
bool AsciiEqualsCaseInsensitive(std::string_view left, std::string_view right);

ARROW_EXPORT
std::string AsciiToLower(std::string_view value);

ARROW_EXPORT
std::string AsciiToUpper(std::string_view value);

/// \brief Search for the first instance of a token and replace it or return nullopt if
/// the token is not found.
ARROW_EXPORT
std::optional<std::string> Replace(std::string_view s, std::string_view token,
                                   std::string_view replacement);

/// \brief Get boolean value from string
///
/// If "1", "true" (case-insensitive), returns true
/// If "0", "false" (case-insensitive), returns false
/// Otherwise, returns Status::Invalid
ARROW_EXPORT
arrow::Result<bool> ParseBoolean(std::string_view value);

}  // namespace internal
}  // namespace arrow
