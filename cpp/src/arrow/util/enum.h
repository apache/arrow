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
#include <type_traits>
#include <vector>

#include "arrow/util/string_view.h"

namespace arrow {
namespace internal {

constexpr bool IsSpace(char c) { return c == ' ' || c == '\n' || c == '\r'; }

constexpr char ToLower(char c) { return c >= 'A' && c <= 'Z' ? c - 'A' + 'a' : c; }

constexpr bool CaseInsensitiveEquals(const char* l, const char* r,
                                     size_t limit = util::string_view::npos) {
  return limit == 0
             ? true
             : ToLower(l[0]) != ToLower(r[0])
                   ? false
                   : l[0] == '\0' ? true : CaseInsensitiveEquals(l + 1, r + 1, limit - 1);
}

constexpr bool CaseInsensitiveEquals(util::string_view l, util::string_view r) {
  return l.size() == r.size() && CaseInsensitiveEquals(l.data(), r.data(), l.size());
}

constexpr const char* SkipWhitespace(const char* raw) {
  return *raw == '\0' || !IsSpace(*raw) ? raw : SkipWhitespace(raw + 1);
}

constexpr const char* SkipNonWhitespace(const char* raw) {
  return *raw == '\0' || IsSpace(*raw) ? raw : SkipNonWhitespace(raw + 1);
}

constexpr size_t TokenSize(const char* token_start) {
  return SkipNonWhitespace(token_start) - token_start;
}

constexpr size_t NextTokenStart(const char* raw, size_t token_start) {
  return SkipWhitespace(SkipNonWhitespace(raw + token_start)) - raw;
}

template <typename Raw, size_t... Offsets>
struct EnumTypeImpl {
  static constexpr int kSize = sizeof...(Offsets);

  static constexpr util::string_view kValueStrs[sizeof...(Offsets)] = {
      {Raw::kValues + Offsets, TokenSize(Raw::kValues + Offsets)}...};

  static constexpr int GetIndex(util::string_view repr, int i = 0) {
    return i == kSize
               ? -1
               : CaseInsensitiveEquals(kValueStrs[i], repr) ? i : GetIndex(repr, i + 1);
  }
};

template <typename Raw, size_t... Offsets>
constexpr util::string_view const
    EnumTypeImpl<Raw, Offsets...>::kValueStrs[sizeof...(Offsets)];

/// \cond false
template <typename Raw, bool IsEnd = false,
          size_t MaxOffset = SkipWhitespace(Raw::kValues) - Raw::kValues,
          size_t... Offsets>
struct EnumTypeBuilder
    : EnumTypeBuilder<Raw, Raw::kValues[NextTokenStart(Raw::kValues, MaxOffset)] == '\0',
                      NextTokenStart(Raw::kValues, MaxOffset), Offsets..., MaxOffset> {};

template <typename Raw, size_t TerminalNullOffset, size_t... Offsets>
struct EnumTypeBuilder<Raw, /*IsEnd=*/true, TerminalNullOffset, Offsets...> {
  using ImplType = EnumTypeImpl<Raw, Offsets...>;
};

// reuse struct as an alias for typename EnumTypeBuilder<Raw>::ImplType
template <typename Raw>
struct EnumTypeImpl<Raw> : EnumTypeBuilder<Raw>::ImplType {};
/// \endcond

struct EnumTypeTag {};

/// \brief An enum replacement with minimal reflection capabilities.
///
/// Declare an enum by inheriting from this helper with CRTP, including a
/// static string literal data member containing the enum's values:
///
///     struct Color : EnumType<Color> {
///       using EnumType::EnumType;
///       static constexpr char* kValues = "red green blue";
///     };
///
/// Ensure the doccomment includes a description of each enum value.
///
/// Values of enumerations declared in this way can be constructed from their string
/// representations at compile time, and can be converted to their string representation
/// for easier debugging/logging/...
template <typename Raw>
struct EnumType : EnumTypeTag {
  constexpr EnumType() = default;

  constexpr explicit EnumType(int index)
      : index{index >= 0 && index < EnumTypeImpl<Raw>::kSize ? index : -1} {}

  constexpr explicit EnumType(util::string_view repr)
      : index{EnumTypeImpl<Raw>::GetIndex(repr)} {}

  constexpr bool operator==(EnumType other) const { return index == other.index; }
  constexpr bool operator!=(EnumType other) const { return index != other.index; }

  /// Return the string representation of this enum value.
  std::string ToString() const {
    return EnumTypeImpl<Raw>::kValueStrs[index].to_string();
  }

  /// \brief Valid enum values will be truthy.
  ///
  /// Invalid enums are constructed with indices outside the range [0, size), with strings
  /// not present in EnumType::value_strings(), or by default construction.
  constexpr explicit operator bool() const { return index != -1; }

  /// Convert this enum value to its integer index.
  constexpr int operator*() const { return index; }

  /// The number of values in this enumeration.
  static constexpr int size() { return EnumTypeImpl<Raw>::kSize; }

  /// String representations of each value in this enumeration.
  static std::vector<util::string_view> value_strings() {
    const util::string_view* begin = EnumTypeImpl<Raw>::kValueStrs;
    return {begin, begin + size()};
  }

  int index = -1;

  friend inline void PrintTo(const EnumType& e, std::ostream* os) {
    PrintTo(e.ToString(), os);
  }
};

template <typename T>
using is_reflection_enum = std::is_base_of<EnumTypeTag, T>;

}  // namespace internal
}  // namespace arrow
