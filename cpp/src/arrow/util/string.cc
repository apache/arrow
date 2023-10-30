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

std::string HexEncode(const uint8_t* data, size_t length) {
  std::string hex_string(length * 2, '\0');
  for (size_t j = 0, i = 0; j < length; ++j) {
    // Convert to 2 base16 digits
    constexpr auto kHexDigitTable = "0123456789ABCDEF";
    hex_string[i++] = kHexDigitTable[data[j] >> 4];
    hex_string[i++] = kHexDigitTable[data[j] & 0b1111];
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

std::string HexEncode(std::string_view str) { return HexEncode(str.data(), str.size()); }

std::string Escape(std::string_view str) { return Escape(str.data(), str.size()); }

constexpr uint8_t kInvalidHexDigit = -1;

constexpr uint8_t ParseHexDigit(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return kInvalidHexDigit;
}

Status ParseHexValue(const char* data, uint8_t* out) {
  uint8_t high = ParseHexDigit(data[0]);
  uint8_t low = ParseHexDigit(data[1]);

  // Error checking
  if (high == kInvalidHexDigit || low == kInvalidHexDigit) {
    return Status::Invalid("Encountered non-hex digit");
  }

  *out = static_cast<uint8_t>(high << 4 | low);
  return Status::OK();
}

Status ParseHexValues(std::string_view hex_string, uint8_t* out) {
  if (hex_string.size() % 2 != 0) {
    return Status::Invalid("Expected base16 hex string");
  }
  for (size_t j = 0; j < hex_string.size() / 2; ++j) {
    RETURN_NOT_OK(ParseHexValue(hex_string.data() + j * 2, out + j));
  }
  return Status::OK();
}

namespace internal {

std::vector<std::string_view> SplitString(std::string_view v, char delimiter,
                                          int64_t limit) {
  std::vector<std::string_view> parts;
  size_t start = 0, end;
  while (true) {
    if (limit > 0 && static_cast<size_t>(limit - 1) <= parts.size()) {
      end = std::string::npos;
    } else {
      end = v.find(delimiter, start);
    }
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
                                   std::string_view delimiter) {
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

std::string JoinStrings(const std::vector<std::string_view>& strings,
                        std::string_view delimiter) {
  return JoinStringLikes(strings, delimiter);
}

std::string JoinStrings(const std::vector<std::string>& strings,
                        std::string_view delimiter) {
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

bool AsciiEqualsCaseInsensitive(std::string_view left, std::string_view right) {
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

std::string AsciiToLower(std::string_view value) {
  // TODO: ASCII validation
  std::string result = std::string(value);
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return result;
}

std::string AsciiToUpper(std::string_view value) {
  // TODO: ASCII validation
  std::string result = std::string(value);
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return result;
}

std::optional<std::string> Replace(std::string_view s, std::string_view token,
                                   std::string_view replacement) {
  size_t token_start = s.find(token);
  if (token_start == std::string::npos) {
    return std::nullopt;
  }
  return std::string(s.substr(0, token_start)) + std::string(replacement) +
         std::string(s.substr(token_start + token.size()));
}

Result<bool> ParseBoolean(std::string_view value) {
  if (AsciiEqualsCaseInsensitive(value, "true") || value == "1") {
    return true;
  } else if (AsciiEqualsCaseInsensitive(value, "false") || value == "0") {
    return false;
  } else {
    return Status::Invalid("String is not a valid boolean value: '", value, "'.");
  }
}

}  // namespace internal
}  // namespace arrow
