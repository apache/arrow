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
#include <tuple>
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

template <typename Props, typename Fn, size_t... I>
void ForEachPropertyImpl(const Props& props, Fn&& fn, const index_sequence<I...>&) {
  struct {
  } dummy;
  std::make_tuple((std::forward<Fn>(fn)(std::get<I>(props), I), dummy)...);
}

template <typename... Properties, typename Fn>
void ForEachProperty(const std::tuple<Properties...>& props, Fn&& fn) {
  ForEachPropertyImpl(props, std::forward<Fn>(fn), index_sequence_for<Properties...>{});
}

template <typename Class, typename Type>
struct DataMemberPtr {
  using type = Type;

  constexpr const type& get(const Class& obj) const { return obj.*ptr_; }

  void set(Class* obj, type value) const { (*obj).*ptr_ = std::move(value); }

  constexpr util::string_view name() const { return name_; }

  util::string_view name_;
  Type Class::*ptr_;
};

template <typename... Properties>
struct PropertySet : std::tuple<Properties...> {
  using std::tuple<Properties...>::tuple;

  template <typename Added, size_t... I>
  constexpr PropertySet<Properties..., Added> AddImpl(Added added,
                                                      index_sequence<I...>) const {
    return {std::get<I>(*this)..., added};
  }

  template <typename Class, typename Type, typename Added = DataMemberPtr<Class, Type>>
  constexpr PropertySet<Properties..., Added> Add(util::string_view name,
                                                  Type Class::*ptr) const {
    return AddImpl(Added{name, ptr}, index_sequence_for<Properties...>{});
  }
};

}  // namespace internal
}  // namespace arrow
