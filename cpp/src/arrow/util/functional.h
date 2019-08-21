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

#include <tuple>

#include "arrow/util/macros.h"

namespace arrow {

/// Helper struct for examining lambdas and other callables.
/// If the callable is not overloaded, the argument types of its call operator can be
/// extracted via single_call::argument_type<Index, Function>
struct single_call {
  template <typename R, typename... A>
  static std::true_type check_impl(R(A...));

  template <typename F>
  static std::true_type check_impl(decltype(&F::operator())*);

  template <typename F>
  static std::false_type check_impl(...);

  template <std::size_t I, typename F, typename R, typename... A>
  static typename std::tuple_element<I, std::tuple<A...>>::type argument_type_impl(
      R (F::*)(A...));

  template <std::size_t I, typename F, typename R, typename... A>
  static typename std::tuple_element<I, std::tuple<A...>>::type argument_type_impl(
      R (F::*)(A...) const);

  template <typename F>
  static constexpr bool check() {
    return decltype(check_impl<typename std::decay<F>::type>(NULLPTR))::value;
  }

  template <typename F, typename T = void>
  using enable_if = typename std::enable_if<check<F>(), T>::type;

  template <std::size_t I, typename F>
  using argument_type = decltype(argument_type_impl<I>(&std::decay<F>::type::operator()));
};

}  // namespace arrow
