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

#include "arrow/result.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace internal {

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

}  // namespace internal

template <int N>
struct EnumStrings {
  template <int M>
  static constexpr bool assert_count() {
    static_assert(M == N, "Incorrect number of enum strings provided");
    return false;
  }

  template <typename... Strs>
  constexpr EnumStrings(const Strs&... strs)  // NOLINT runtime/explicit
      : dummy_{assert_count<sizeof...(Strs)>()}, strings_{util::string_view(strs)...} {}

  constexpr int GetIndex(util::string_view repr, int i = 0) const {
    return i == N ? -1
                  : internal::CaseInsensitiveEquals(strings_[i], repr)
                        ? i
                        : GetIndex(repr, i + 1);
  }

  using value_type = util::string_view;
  using const_iterator = const util::string_view*;

  constexpr int size() const { return N; }
  constexpr const util::string_view* data() const { return strings_; }
  constexpr const_iterator begin() const { return data(); }
  constexpr const_iterator end() const { return begin() + size(); }
  constexpr util::string_view operator[](int i) const { return strings_[i]; }

  bool dummy_;
  util::string_view strings_[N];  // NOLINT modernize
};

struct EnumTypeTag {};

/// \brief An enum replacement with minimal reflection capabilities.
///
/// Declare an enum by inheriting from this helper with CRTP, including a
/// static string literal member function returning the enum's values:
///
///     struct Color : EnumType<Color> {
///       using EnumType::EnumType;
///       static constexpr EnumStrings<3> values() { return {"red", "green", "blue"}; }
///       static constexpr const char* name() { return "Color"; }
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
      : index{index >= 0 && index < Raw::values().size() ? index : -1} {}

  constexpr explicit EnumType(util::string_view repr)
      : index{Raw::values().GetIndex(repr)} {}

  constexpr bool operator==(EnumType other) const { return index == other.index; }
  constexpr bool operator!=(EnumType other) const { return index != other.index; }

  /// Return the string representation of this enum value.
  std::string ToString() const { return Raw::values()[index].to_string(); }

  /// \brief Valid enum values will be truthy.
  ///
  /// Invalid enums are constructed with indices outside the range [0, size), with strings
  /// not present in EnumType::value_strings(), or by default construction.
  constexpr explicit operator bool() const { return index != -1; }

  /// Convert this enum value to its integer index.
  constexpr int operator*() const { return index; }

  /// The number of values in this enumeration.
  static constexpr int size() { return Raw::values().size(); }

  /// Construct a valid enum from int or raise an error
  static Result<Raw> Make(int index) {
    if (auto valid = Raw(index)) return valid;
    return Status::Invalid("index ", index, " for enum ", Raw::name(),
                           "- index should be in range [0, ", Raw::values().size(), ")");
  }

  /// Construct a valid enum from repr or raise an error
  static Result<Raw> Make(util::string_view repr) {
    if (auto valid = Raw(repr)) return valid;

    std::string values;
    static std::string sep = ", ";
    for (auto value : Raw::values()) {
      values.append("'");
      values.append(value.data(), value.size());
      values.append("'");
      values.append(sep);
    }
    values.resize(values.size() - sep.size());

    return Status::Invalid("string '", repr, "' for enum ", Raw::name(),
                           "- string should be one of {", values, "}");
  }

  int index = -1;

  friend inline void PrintTo(const EnumType& e, std::ostream* os) {
    PrintTo(e.ToString(), os);
  }
};

template <typename T>
using is_reflection_enum = std::is_base_of<EnumTypeTag, T>;

}  // namespace arrow
