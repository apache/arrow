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

template <typename Enum>
struct EnumTraits {};

template <typename Enum, Enum... Values>
struct BasicEnumTraits {
  using CType = typename std::underlying_type<Enum>::type;
  using Type = typename CTypeTraits<CType>::ArrowType;
  static std::array<Enum, sizeof...(Values)> values() { return {Values...}; }
};

template <typename T, typename Enable = void>
struct has_enum_traits : std::false_type {};

template <typename T>
struct has_enum_traits<T, void_t<typename EnumTraits<T>::Type>> : std::true_type {};

/// Returns a string_view containing the name of a value.
template <auto Value, bool IncludeLeadingK = false>
constexpr std::string_view nameof();

/// Returns a string_view containing the name of an enumeration member.
template <typename Enum>
constexpr std::string_view enum_name(Enum e);

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

constexpr int kCrib = 27892354;
constexpr size_t kValueNamePrefix = pretty_function<kCrib>().find("27892354");
constexpr size_t kValueNameSuffix =
    pretty_function<kCrib>()
        .substr(kValueNamePrefix + std::string_view{"27892354"}.size())
        .size();

template <size_t... I, typename F>
constexpr auto Spread(std::index_sequence<I...>, const F& f) {
  return f(std::integral_constant<size_t, I>{}...);
}

template <size_t N, typename F>
constexpr auto Spread(const F& f) {
  return Spread(std::make_index_sequence<N>{}, f);
}

/// std::array is not constexpr in all STL impls
template <typename T, size_t N>
struct array {
  constexpr array() {
    for (T& value : values_) {
      value = T{};
    }
  }
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
  T values_[N];  // NOLINT
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

template <auto Value, bool IncludeLeadingK>
constexpr auto kNameStorage = [] {
  constexpr std::string_view name = [] {
    std::string_view name = impl::pretty_function<Value>();
    name.remove_prefix(impl::kValueNamePrefix);
    name.remove_suffix(impl::kValueNameSuffix);

    for (size_t i = name.size(); i != 0; --i) {
      if (name[i - 1] == ':') {
        name = name.substr(i);
        break;
      }
    }

    if (!IncludeLeadingK) {
      if (name.size() > 1 && name[0] == 'k') {
        if (name[1] >= 'A' && name[1] <= 'Z') {
          name.remove_prefix(1);
        }
      }
    }
    return name;
  }();

  // copying out of the string_view here ensures that characters which were
  // sliced out are not present in (release)binaries
  return impl::array<char, name.size()>{name.data()};
}();

}  // namespace impl

template <auto Value, bool IncludeLeadingK>
constexpr std::string_view nameof() {
  constexpr auto& storage = impl::kNameStorage<Value, IncludeLeadingK>;
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
  static_assert(
      sizeof(Enum) == 1,
      "Automatic discovery of enum values is not supported for Enums which "
      "aren't a single byte. Explicitly specialize kEnumMembers like so:    "
      "namespace arrow::internal {    template<> constexpr auto kEnumMembers<Color> "
      "= impl::array{{kRed, kGreen, kBlue}};    }");

  constexpr int kUnderlyingMin = std::numeric_limits<std::underlying_type_t<Enum>>::min();
  constexpr int kUnderlyingMax = std::numeric_limits<std::underlying_type_t<Enum>>::max();
  constexpr int kRange = kUnderlyingMax + 1 - kUnderlyingMin;

  constexpr auto IsValid = [](auto i) {
    constexpr int int_value = kUnderlyingMin + static_cast<int>(i);
    return nameof<static_cast<Enum>(int_value)>() != nameof<int_value>();
  };

  constexpr size_t N =
      impl::Spread<kRange>([&](auto... i) { return (... + IsValid(i)); });

  impl::array<Enum, N> out;

  impl::Spread<kRange>([&](auto... i) {
    Enum* e = out.data();
    for (auto [is_valid, int_value] : {std::pair{IsValid(i), static_cast<int>(i)}...}) {
      if (is_valid) {
        *e++ = static_cast<Enum>(kUnderlyingMin + int_value);
      }
    }
  });
  return out;
}();

namespace impl {
template <typename Enum, const auto& values = kEnumMembers<Enum>>
constexpr auto kEnumNamesStorage = Spread<values.size()>([](auto... i) {
  constexpr size_t N = (... + nameof<values[i]>().size());
  struct {
    array<uint16_t, values.size() + 1> offsets;
    array<char, N> data;
  } out;

  uint16_t* offset = out.offsets.data();
  offset[0] = 0;

  char* data = out.data.data();

  for (auto name : {nameof<values[i]>()...}) {
    for (char c : name) {
      *data++ = c;
    }
    offset[1] = offset[0] + name.size();
    ++offset;
  }
  return out;
});
}  // namespace impl

template <typename Enum>
constexpr std::string_view enum_name(Enum e) {
  constexpr auto& names = impl::kEnumNamesStorage<Enum>;
  size_t i = 0;
  for (Enum value : kEnumMembers<Enum>) {
    if (value == e) {
      uint16_t offset = names.offsets[i];
      uint16_t size = names.offsets[i + 1] - offset;
      return {names.data.data() + offset, size};
    }
    ++i;
  }
  return "";
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
