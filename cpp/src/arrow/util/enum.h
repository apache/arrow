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

static_assert(CaseInsensitiveEquals("a", "a"), "");
static_assert(CaseInsensitiveEquals("Ab", "ab"), "");
static_assert(CaseInsensitiveEquals("Ab ", "ab", 2), "");
static_assert(CaseInsensitiveEquals(util::string_view{"Ab ", 2}, "ab"), "");

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

static_assert(CaseInsensitiveEquals(SkipWhitespace("  a"), "a"), "");

using StringConstant = const char* const&;

template <StringConstant Raw, size_t... Offsets>
struct EnumTypeImpl {
  static constexpr const char* raw = SkipWhitespace(Raw);

  static constexpr int size = sizeof...(Offsets);

  static constexpr util::string_view values[sizeof...(Offsets)] = {
      {raw + Offsets, TokenSize(raw + Offsets)}...};

  static constexpr int GetIndex(util::string_view repr, int i = 0) {
    return i == size ? -1
                     : CaseInsensitiveEquals(values[i], repr) ? i : GetIndex(repr, i + 1);
  }
};

template <StringConstant Raw, size_t... Offsets>
constexpr util::string_view const
    EnumTypeImpl<Raw, Offsets...>::values[sizeof...(Offsets)];

template <StringConstant Raw, bool IsEnd = false,
          size_t MaxOffset = SkipWhitespace(Raw) - Raw, size_t... Offsets>
struct EnumTypeBuilder
    : EnumTypeBuilder<Raw, Raw[NextTokenStart(Raw, MaxOffset)] == '\0',
                      NextTokenStart(Raw, MaxOffset), Offsets..., MaxOffset> {};

template <StringConstant Raw, size_t MaxOffset, size_t... Offsets>
struct EnumTypeBuilder<Raw, true, MaxOffset, Offsets...> {
  using ImplType = EnumTypeImpl<Raw, Offsets...>;
};

template <StringConstant Raw>
struct EnumType : EnumTypeBuilder<Raw>::ImplType {
  constexpr EnumType() = default;
  constexpr explicit EnumType(int i) : index{i < this->size ? i : -1} {}
  constexpr explicit EnumType(util::string_view repr) : index{this->GetIndex(repr)} {}

  constexpr bool operator==(EnumType other) const { return index == other.index; }
  constexpr bool operator!=(EnumType other) const { return index != other.index; }

  std::string ToString() const { return this->values[index].to_string(); }
  constexpr explicit operator bool() const { return index != -1; }
  constexpr operator int() const { return index; }  // NOLINT runtime/explicit

  int index = -1;

  friend inline void PrintTo(const EnumType& e, std::ostream* os) {
    PrintTo(e.repr(), os);
  }
};

}  // namespace internal
}  // namespace arrow
