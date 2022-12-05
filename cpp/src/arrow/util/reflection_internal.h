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

#include <array>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include "arrow/type_traits.h"

namespace arrow::internal {

template <typename...>
struct all_same : std::true_type {};

template <typename One>
struct all_same<One> : std::true_type {};

template <typename Same, typename... Rest>
struct all_same<Same, Same, Rest...> : all_same<Same, Rest...> {};

template <typename One, typename Other, typename... Rest>
struct all_same<One, Other, Rest...> : std::false_type {};

template <size_t... I, typename... T, typename Fn>
void ForEachTupleMemberImpl(const std::tuple<T...>& tup, Fn&& fn,
                            std::index_sequence<I...>) {
  (..., fn(std::get<I>(tup), I));
}

template <typename... T, typename Fn>
void ForEachTupleMember(const std::tuple<T...>& tup, Fn&& fn) {
  ForEachTupleMemberImpl(tup, fn, std::index_sequence_for<T...>());
}

template <typename C, typename T>
struct DataMemberProperty {
  using Class = C;
  using Type = T;

  constexpr const Type& get(const Class& obj) const { return obj.*ptr_; }

  void set(Class* obj, Type value) const { (*obj).*ptr_ = std::move(value); }

  constexpr std::string_view name() const { return name_; }

  std::string_view name_;
  Type Class::*ptr_;
};

template <typename Class, typename Type>
constexpr DataMemberProperty<Class, Type> DataMember(std::string_view name,
                                                     Type Class::*ptr) {
  return {name, ptr};
}

template <typename... Properties>
struct PropertyTuple {
  template <typename Fn>
  void ForEach(Fn&& fn) const {
    ForEachTupleMember(props_, fn);
  }

  static_assert(all_same<typename Properties::Class...>::value,
                "All properties must be properties of the same class");

  size_t size() const { return sizeof...(Properties); }

  std::tuple<Properties...> props_;
};

template <typename... Properties>
PropertyTuple<Properties...> MakeProperties(Properties... props) {
  return {std::make_tuple(props...)};
}

/// Returns a string_view containing the name of a value.
template <auto>
constexpr std::string_view nameof(bool include_leading_k = false);

/// Returns a string_view containing the (unqualified) name of a type.
template <typename>
constexpr std::string_view nameof();

/// Returns a string_view containing the name of an enumeration member.
template <typename Enum>
constexpr std::string_view enum_name(Enum e, bool include_leading_k = false);

/// Returns the enumeration member corresponding to name, or
/// nullopt if name doesn't correspond to an enum member.
template <typename Enum>
constexpr std::optional<Enum> enum_cast(std::string_view name);

/// Returns the enumeration member corresponding to an integer, or
/// nullopt if the integer doesn't correspond to an enum member.
template <typename Enum, typename Int>
constexpr auto enum_cast(Int i) -> std::optional<decltype(static_cast<Enum>(i))>;

namespace impl {
template <auto V>
constexpr std::string_view pretty_function() {
#ifdef _MSC_VER
  return __FUNCSIG__;
#else
  return __PRETTY_FUNCTION__;
#endif
}

template <typename T>
constexpr std::string_view pretty_function() {
#ifdef _MSC_VER
  return __FUNCSIG__;
#else
  return __PRETTY_FUNCTION__;
#endif
}

constexpr auto kValueNamePrefixSuffix = [] {
  size_t prefix{}, suffix{};

  auto raw = pretty_function<2234527>();
  if (prefix = raw.find("2234527"); prefix != std::string_view::npos) {
    suffix = raw.size() - prefix - std::string_view{"2234527"}.size();
  } else {
    // some compilers render hexadecimal integer template arguments
    raw = pretty_function<0x346243>();
    prefix = raw.find("0x346243");
    suffix = raw.size() - prefix - std::string_view{"0x346243"}.size();
  }

  return std::pair{prefix, suffix};
}();

constexpr auto kTypeNamePrefixSuffix = [] {
  auto raw = pretty_function<double>();
  size_t prefix = raw.find("double");
  size_t suffix = raw.size() - prefix - std::string_view{"double"}.size();
  return std::pair{prefix, suffix};
}();

/// std::array is not constexpr in all STL impls
template <typename T, size_t N>
class array {
 public:
  constexpr array() = default;

  constexpr explicit array(const T* ptr) {
    for (T& value : values_) {
      value = *ptr++;
    }
  }

  constexpr explicit array(const T (&arr)[N])  // NOLINT
      : array{static_cast<const T*>(arr)} {}

  [[nodiscard]] static constexpr size_t size() { return N; }
  [[nodiscard]] constexpr const T* data() const { return values_; }
  [[nodiscard]] constexpr T* data() { return values_; }
  [[nodiscard]] constexpr const T* begin() const { return values_; }
  [[nodiscard]] constexpr const T* end() const { return begin() + N; }
  [[nodiscard]] constexpr const T& operator[](size_t i) const { return begin()[i]; }

 private:
  T values_[N] = {};  // NOLINT
};

template <typename T, size_t N, size_t M>
constexpr bool operator==(const array<T, N>& l, const array<T, M>& r) {
  if constexpr (N != M) {
    return false;
  } else {
    for (size_t i = 0; i < N; ++i) {
      if (l[i] != r[i]) return false;
    }
    return true;
  }
}

template <typename T, size_t N, typename R>
constexpr bool operator!=(const array<T, N>& l, const R& r) {
  return !(l == r);
}

constexpr std::string_view TrimNamespace(std::string_view name) {
  for (size_t i = name.size(); i != 0; --i) {
    char c = name[i - 1];
    if ('A' <= c && c <= 'Z') continue;
    if ('a' <= c && c <= 'z') continue;
    if ('0' <= c && c <= '9') continue;
    if (c == '_') continue;
    name = name.substr(i);
    break;
  }
  return name;
}

template <auto Value>
constexpr auto kValueNameStorage = [] {
  constexpr std::string_view name = [] {
    std::string_view name = pretty_function<Value>();
    auto [prefix, suffix] = kValueNamePrefixSuffix;
    name.remove_prefix(prefix);
    name.remove_suffix(suffix);
    name = TrimNamespace(name);
    return name;
  }();

  // copying out of the string_view here ensures that characters which were
  // sliced out are not present in (release)binaries
  return array<char, name.size()>{name.data()};
}();

template <>
static constexpr array<char, 1> kValueNameStorage<0>{{'0'}};

template <typename T>
constexpr auto kTypeNameStorage = [] {
  constexpr std::string_view name = [] {
    std::string_view name = pretty_function<T>();
    auto [prefix, suffix] = kTypeNamePrefixSuffix;
    name.remove_prefix(prefix);
    name.remove_suffix(suffix);
    return TrimNamespace(name);
  }();
  return array<char, name.size()>{name.data()};
}();

template <typename Enum, int I,
          char FirstChar = kValueNameStorage<static_cast<Enum>(I)>[0]>
constexpr bool IsValidEnumMember() {
  return FirstChar < '0' || FirstChar > '9';
}

template <typename Enum, int I, int Max>
constexpr size_t GetDefaultEnumMemberCount() {
  if constexpr (I < Max + 1) {
    return GetDefaultEnumMemberCount<Enum, I + 1, Max>() + IsValidEnumMember<Enum, I>();
  } else {
    return 0;
  }
}

template <typename Enum, int I, int Max>
constexpr void GetDefaultEnumMembers(Enum* members) {
  if constexpr (I < Max + 1) {
    if constexpr (IsValidEnumMember<Enum, I>()) {
      *members++ = static_cast<Enum>(I);
    }
    GetDefaultEnumMembers<Enum, I + 1, Max>(members);
  }
}

constexpr std::string_view TrimLeadingK(std::string_view name) {
  if (name.size() > 1 && name[0] == 'k') {
    if (name[1] >= 'A' && name[1] <= 'Z') {
      name.remove_prefix(1);
    }
  }
  return name;
}

}  // namespace impl

template <auto Value>
constexpr std::string_view nameof(bool include_leading_k) {
  constexpr auto& storage = impl::kValueNameStorage<Value>;
  std::string_view name{storage.data(), storage.size()};
  return include_leading_k ? name : impl::TrimLeadingK(name);
}

template <typename T>
constexpr std::string_view nameof() {
  constexpr auto& storage = impl::kTypeNameStorage<T>;
  return {storage.data(), storage.size()};
}

/// A constexpr array containing all members of the enumeration.
/// In the common case of enumerations which are one byte wide, a
/// default is provided which automatically detects valid enum members.
/// The value can also be specialized to provide an explicit listing
/// of the enumeration's members.
template <typename Enum>
constexpr auto kEnumMembers = [] {
  static_assert(std::is_enum_v<Enum>);

  static_assert(sizeof(Enum) == 1,
                "Automatic discovery of enum values is not supported for Enums which "
                "aren't a single byte. Explicitly specialize kEnumMembers like so:    "
                "namespace arrow::internal {    template<> constexpr impl::array "
                "kEnumMembers<Color>{{kRed, kGreen, kBlue}};    }");

  using Limits = std::numeric_limits<std::underlying_type_t<Enum>>;

  impl::array<Enum, impl::GetDefaultEnumMemberCount<Enum, Limits::min(), Limits::max()>()>
      members;
  impl::GetDefaultEnumMembers<Enum, Limits::min(), Limits::max()>(members.data());

  return members;
}();

namespace impl {
template <typename Enum, size_t I = 0>
constexpr void GetEnumMemberNames(std::string_view* names) {
  if constexpr (I < kEnumMembers<Enum>.size()) {
    *names++ = nameof<kEnumMembers<Enum>[I]>(/*include_leading_k=*/true);
    GetEnumMemberNames<Enum, I + 1>(names);
  }
}
}  // namespace impl

template <typename Enum>
constexpr std::string_view enum_name(Enum e, bool include_leading_k) {
  constexpr auto kNames = [] {
    impl::array<std::string_view, kEnumMembers<Enum>.size()> names;
    impl::GetEnumMemberNames<Enum>(names.data());
    return names;
  }();

  for (size_t i = 0; i < kEnumMembers<Enum>.size(); ++i) {
    if (e == kEnumMembers<Enum>[i]) {
      return include_leading_k ? kNames[i] : impl::TrimLeadingK(kNames[i]);
    }
  }
  return {};
}

template <typename Enum>
constexpr std::optional<Enum> enum_cast(std::string_view name) {
  for (Enum e : kEnumMembers<Enum>) {
    if (enum_name(e) == name) return e;
  }
  return {};
}

template <typename Enum, typename Int>
constexpr auto enum_cast(Int i) -> std::optional<decltype(static_cast<Enum>(i))> {
  for (Enum e : kEnumMembers<Enum>) {
    if (static_cast<Enum>(i) == e) return e;
  }
  return {};
}
}  // namespace arrow::internal
