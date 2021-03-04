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

#include <memory>
#include <type_traits>
#include <utility>

namespace arrow {
namespace internal {

template <typename T, typename... A>
typename std::enable_if<!std::is_array<T>::value, std::unique_ptr<T>>::type make_unique(
    A&&... args) {
  return std::unique_ptr<T>(new T(std::forward<A>(args)...));
}

template <typename T>
typename std::enable_if<std::is_array<T>::value && std::extent<T>::value == 0,
                        std::unique_ptr<T>>::type
make_unique(std::size_t n) {
  using value_type = typename std::remove_extent<T>::type;
  return std::unique_ptr<value_type[]>(new value_type[n]);
}

}  // namespace internal
}  // namespace arrow
