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
#include <tuple>
#include <utility>

#include "arrow/type_traits.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace internal {

template <size_t...>
struct index_sequence {};

template <size_t N, size_t Head = N, size_t... Tail>
struct make_index_sequence_impl;

template <size_t N>
using make_index_sequence = typename make_index_sequence_impl<N>::type;

template <typename... T>
using index_sequence_for = make_index_sequence<sizeof...(T)>;

template <size_t N, size_t... I>
struct make_index_sequence_impl<N, 0, I...> {
  using type = index_sequence<I...>;
};

template <size_t N, size_t H, size_t... I>
struct make_index_sequence_impl : make_index_sequence_impl<N, H - 1, H - 1, I...> {};

static_assert(std::is_same<index_sequence<>, make_index_sequence<0>>::value, "");
static_assert(std::is_same<index_sequence<0, 1, 2>, make_index_sequence<3>>::value, "");

template <typename...>
struct all_same : std::true_type {};

template <typename One>
struct all_same<One> : std::true_type {};

template <typename Same, typename... Rest>
struct all_same<Same, Same, Rest...> : all_same<Same, Rest...> {};

template <typename One, typename Other, typename... Rest>
struct all_same<One, Other, Rest...> : std::false_type {};

template <size_t... I, typename... T, typename Fn>
void ForEachTupleMemberImpl(const std::tuple<T...>& tup, Fn&& fn, index_sequence<I...>) {
  (void)std::make_tuple((fn(std::get<I>(tup), I), std::ignore)...);
}

template <typename... T, typename Fn>
void ForEachTupleMember(const std::tuple<T...>& tup, Fn&& fn) {
  ForEachTupleMemberImpl(tup, fn, index_sequence_for<T...>());
}

template <typename C, typename T>
struct DataMemberProperty {
  using Class = C;
  using Type = T;

  constexpr const Type& get(const Class& obj) const { return obj.*ptr_; }

  void set(Class* obj, Type value) const { (*obj).*ptr_ = std::move(value); }

  constexpr util::string_view name() const { return name_; }

  util::string_view name_;
  Type Class::*ptr_;
};

template <typename Class, typename Type>
constexpr DataMemberProperty<Class, Type> DataMember(util::string_view name,
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

}  // namespace internal
}  // namespace arrow
