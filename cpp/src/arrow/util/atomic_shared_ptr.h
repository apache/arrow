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

#include <atomic>
#include <memory>
#include <utility>

#include "arrow/type_traits.h"

namespace arrow {
namespace internal {

// Atomic shared_ptr operations only appeared in libstdc++ since GCC 5,
// emulate them with unsafe ops if unavailable.
// See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=57250

template <typename T, typename = void>
struct is_atomic_load_shared_ptr_available : std::false_type {};

template <typename T>
struct is_atomic_load_shared_ptr_available<
    T, void_t<decltype(std::atomic_load(std::declval<const std::shared_ptr<T>*>()))>>
    : std::true_type {};

template <typename T>
using enable_if_atomic_load_shared_ptr_available =
    enable_if_t<is_atomic_load_shared_ptr_available<T>::value, T>;

template <typename T>
using enable_if_atomic_load_shared_ptr_unavailable =
    enable_if_t<!is_atomic_load_shared_ptr_available<T>::value, T>;

template <class T>
inline enable_if_atomic_load_shared_ptr_available<std::shared_ptr<T>> atomic_load(
    const std::shared_ptr<T>* p) {
  return std::atomic_load(p);
}

template <class T>
inline enable_if_atomic_load_shared_ptr_unavailable<std::shared_ptr<T>> atomic_load(
    const std::shared_ptr<T>* p) {
  return *p;
}

template <typename T, typename = void>
struct is_atomic_store_shared_ptr_available : std::false_type {};

template <typename T>
struct is_atomic_store_shared_ptr_available<
    T, void_t<decltype(std::atomic_store(std::declval<std::shared_ptr<T>*>(),
                                         std::declval<std::shared_ptr<T>>()))>>
    : std::true_type {};

template <typename T>
using enable_if_atomic_store_shared_ptr_available =
    enable_if_t<is_atomic_store_shared_ptr_available<T>::value, T>;

template <typename T>
using enable_if_atomic_store_shared_ptr_unavailable =
    enable_if_t<!is_atomic_store_shared_ptr_available<T>::value, T>;

template <class T>
inline void atomic_store(
    enable_if_atomic_store_shared_ptr_available<std::shared_ptr<T>*> p,
    std::shared_ptr<T> r) {
  std::atomic_store(p, std::move(r));
}

template <class T>
inline void atomic_store(
    enable_if_atomic_store_shared_ptr_unavailable<std::shared_ptr<T>*> p,
    std::shared_ptr<T> r) {
  *p = r;
}

}  // namespace internal
}  // namespace arrow
