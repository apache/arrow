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

#include <string>
#include <vector>

#include "arrow/util/optional.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Status;

ARROW_EXPORT std::string HexEncode(const uint8_t* data, size_t length);

ARROW_EXPORT std::string Escape(const char* data, size_t length);

ARROW_EXPORT std::string HexEncode(const char* data, size_t length);

ARROW_EXPORT std::string HexEncode(util::string_view str);

ARROW_EXPORT std::string Escape(util::string_view str);

ARROW_EXPORT Status ParseHexValue(const char* data, uint8_t* out);

namespace internal {

/// \brief Split a string with a delimiter
ARROW_EXPORT
std::vector<util::string_view> SplitString(util::string_view v, char delim);

/// \brief Join strings with a delimiter
ARROW_EXPORT
std::string JoinStrings(const std::vector<util::string_view>& strings,
                        util::string_view delimiter);

/// \brief Join strings with a delimiter
ARROW_EXPORT
std::string JoinStrings(const std::vector<std::string>& strings,
                        util::string_view delimiter);

/// \brief Trim whitespace from left and right sides of string
ARROW_EXPORT
std::string TrimString(std::string value);

ARROW_EXPORT
bool AsciiEqualsCaseInsensitive(util::string_view left, util::string_view right);

ARROW_EXPORT
std::string AsciiToLower(util::string_view value);

ARROW_EXPORT
std::string AsciiToUpper(util::string_view value);

/// \brief Search for the first instance of a token and replace it or return nullopt if
/// the token is not found.
ARROW_EXPORT
util::optional<std::string> Replace(util::string_view s, util::string_view token,
                                    util::string_view replacement);

}  // namespace internal
}  // namespace arrow
