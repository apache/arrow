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

template <typename Raw, size_t MaxOffset, size_t... Offsets>
struct EnumTypeBuilder<Raw, true, MaxOffset, Offsets...> {
  using ImplType = EnumTypeImpl<Raw, Offsets...>;
};

template <typename Raw>
struct EnumTypeImpl<Raw> : EnumTypeBuilder<Raw>::ImplType {};
/// \endcond

template <typename Raw>
struct EnumType {
  constexpr EnumType() = default;
  constexpr explicit EnumType(int i)
      : index{i >= 0 && i < EnumTypeImpl<Raw>::kSize ? i : -1} {}
  constexpr explicit EnumType(util::string_view repr)
      : index{EnumTypeImpl<Raw>::GetIndex(repr)} {}

  constexpr bool operator==(EnumType other) const { return index == other.index; }
  constexpr bool operator!=(EnumType other) const { return index != other.index; }

  std::string ToString() const {
    return EnumTypeImpl<Raw>::kValueStrs[index].to_string();
  }
  constexpr explicit operator bool() const { return index != -1; }
  constexpr operator int() const { return index; }  // NOLINT runtime/explicit

  static constexpr int size() { return EnumTypeImpl<Raw>::kSize; }

  int index = -1;

  friend inline void PrintTo(const EnumType& e, std::ostream* os) {
    PrintTo(e.repr(), os);
  }
};

}  // namespace internal
}  // namespace arrow
