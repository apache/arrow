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

#include "arrow/util/string.h"

#include <algorithm>
#include <cctype>
#include <memory>

#include "arrow/status.h"

namespace arrow {

static const char* kAsciiTable = "0123456789ABCDEF";

std::string HexEncode(const uint8_t* data, size_t length) {
  std::string hex_string;
  hex_string.reserve(length * 2);
  for (size_t j = 0; j < length; ++j) {
    // Convert to 2 base16 digits
    hex_string.push_back(kAsciiTable[data[j] >> 4]);
    hex_string.push_back(kAsciiTable[data[j] & 15]);
  }
  return hex_string;
}

std::string Escape(const char* data, size_t length) {
  std::string escaped_string;
  escaped_string.reserve(length);
  for (size_t j = 0; j < length; ++j) {
    switch (data[j]) {
      case '"':
        escaped_string += R"(\")";
        break;
      case '\\':
        escaped_string += R"(\\)";
        break;
      case '\t':
        escaped_string += R"(\t)";
        break;
      case '\r':
        escaped_string += R"(\r)";
        break;
      case '\n':
        escaped_string += R"(\n)";
        break;
      default:
        escaped_string.push_back(data[j]);
    }
  }
  return escaped_string;
}

std::string HexEncode(const char* data, size_t length) {
  return HexEncode(reinterpret_cast<const uint8_t*>(data), length);
}

std::string HexEncode(util::string_view str) { return HexEncode(str.data(), str.size()); }

std::string Escape(util::string_view str) { return Escape(str.data(), str.size()); }

Status ParseHexValue(const char* data, uint8_t* out) {
  char c1 = data[0];
  char c2 = data[1];

  const char* kAsciiTableEnd = kAsciiTable + 16;
  const char* pos1 = std::lower_bound(kAsciiTable, kAsciiTableEnd, c1);
  const char* pos2 = std::lower_bound(kAsciiTable, kAsciiTableEnd, c2);

  // Error checking
  if (pos1 == kAsciiTableEnd || pos2 == kAsciiTableEnd || *pos1 != c1 || *pos2 != c2) {
    return Status::Invalid("Encountered non-hex digit");
  }

  *out = static_cast<uint8_t>((pos1 - kAsciiTable) << 4 | (pos2 - kAsciiTable));
  return Status::OK();
}

namespace internal {

std::vector<util::string_view> SplitString(util::string_view v, char delimiter) {
  std::vector<util::string_view> parts;
  size_t start = 0, end;
  while (true) {
    end = v.find(delimiter, start);
    parts.push_back(v.substr(start, end - start));
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }
  return parts;
}

template <typename StringLike>
static std::string JoinStringLikes(const std::vector<StringLike>& strings,
                                   util::string_view delimiter) {
  if (strings.size() == 0) {
    return "";
  }
  std::string out = std::string(strings.front());
  for (size_t i = 1; i < strings.size(); ++i) {
    out.append(delimiter.begin(), delimiter.end());
    out.append(strings[i].begin(), strings[i].end());
  }
  return out;
}

std::string JoinStrings(const std::vector<util::string_view>& strings,
                        util::string_view delimiter) {
  return JoinStringLikes(strings, delimiter);
}

std::string JoinStrings(const std::vector<std::string>& strings,
                        util::string_view delimiter) {
  return JoinStringLikes(strings, delimiter);
}

static constexpr bool IsWhitespace(char c) { return c == ' ' || c == '\t'; }

std::string TrimString(std::string value) {
  size_t ltrim_chars = 0;
  while (ltrim_chars < value.size() && IsWhitespace(value[ltrim_chars])) {
    ++ltrim_chars;
  }
  value.erase(0, ltrim_chars);
  size_t rtrim_chars = 0;
  while (rtrim_chars < value.size() &&
         IsWhitespace(value[value.size() - 1 - rtrim_chars])) {
    ++rtrim_chars;
  }
  value.erase(value.size() - rtrim_chars, rtrim_chars);
  return value;
}

bool AsciiEqualsCaseInsensitive(util::string_view left, util::string_view right) {
  // TODO: ASCII validation
  if (left.size() != right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(left[i])) !=
        std::tolower(static_cast<unsigned char>(right[i]))) {
      return false;
    }
  }
  return true;
}

std::string AsciiToLower(util::string_view value) {
  // TODO: ASCII validation
  std::string result = std::string(value);
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return result;
}

std::string AsciiToUpper(util::string_view value) {
  // TODO: ASCII validation
  std::string result = std::string(value);
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return result;
}

util::optional<std::string> Replace(util::string_view s, util::string_view token,
                                    util::string_view replacement) {
  size_t token_start = s.find(token);
  if (token_start == std::string::npos) {
    return util::nullopt;
  }
  return s.substr(0, token_start).to_string() + replacement.to_string() +
         s.substr(token_start + token.size()).to_string();
}

}  // namespace internal
}  // namespace arrow
