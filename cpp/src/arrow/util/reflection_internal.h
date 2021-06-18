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
#include <utility>

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

static_assert(std::is_base_of<index_sequence<>, make_index_sequence<0>>::value, "");
static_assert(std::is_base_of<index_sequence<0, 1, 2>, make_index_sequence<3>>::value,
              "");

template <size_t I>
using index_constant = std::integral_constant<size_t, I>;

template <typename...>
struct all_same : std::true_type {};

template <typename One>
struct all_same<One> : std::true_type {};

template <typename Same, typename... Rest>
struct all_same<Same, Same, Rest...> : all_same<Same, Rest...> {};

template <typename One, typename Other, typename... Rest>
struct all_same<One, Other, Rest...> : std::false_type {};

template <size_t I, typename T>
struct TupleMember {
  friend constexpr const T& GetTupleMember(const TupleMember& member, index_constant<I>) {
    return member.value_;
  }

  T value_;
};

template <typename...>
struct TupleImpl;

template <>
struct TupleImpl<> {};

template <size_t I0, size_t... I, typename T0, typename... T>
struct TupleImpl<TupleMember<I0, T0>, TupleMember<I, T>...>
    : TupleMember<I0, T0>, TupleImpl<TupleMember<I, T>...> {
  constexpr static size_t size() { return sizeof...(T); }

  constexpr explicit TupleImpl(T0 value0, T... values)
      : TupleMember<I0, T0>{value0}, TupleImpl<TupleMember<I, T>...>{values...} {}
};

template <size_t... I, typename... T>
TupleImpl<TupleMember<I, T>...> TupleImplForImpl(index_sequence<I...>, T... values);

template <typename... T>
using TupleImplFor =
    decltype(TupleImplForImpl(index_sequence_for<T...>(), std::declval<T>()...));

template <typename... T>
struct Tuple : TupleImplFor<T...> {
  using TupleImplFor<T...>::TupleImplFor;
};

template <typename... T>
constexpr Tuple<T...> MakeTuple(T... values) {
  return Tuple<T...>(values...);
}

constexpr auto tup = MakeTuple(1, "h", 3);
static_assert(GetTupleMember(tup, index_constant<0>()) == 1, "");
static_assert(GetTupleMember(tup, index_constant<1>())[0] == 'h', "");
static_assert(GetTupleMember(tup, index_constant<2>()) == 3, "");

template <size_t... I, typename... T, typename Fn>
void ForEachTupleMemberImpl(const Tuple<T...>& tup, Fn&& fn, index_sequence<I...>) {
  (void)MakeTuple((fn(GetTupleMember(tup, index_constant<I>()), index_constant<I>()),
                   std::ignore)...);
}

template <typename... T, typename Fn>
void ForEachTupleMember(const Tuple<T...>& tup, Fn&& fn) {
  return ForEachTupleMemberImpl(tup, fn, index_sequence_for<T...>());
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
constexpr Tuple<Properties...> MakeProperties(Properties... props) {
  static_assert(all_same<typename Properties::Class...>::value,
                "All properties must be properties of the same class");
  return MakeTuple(props...);
}

}  // namespace internal
}  // namespace arrow
