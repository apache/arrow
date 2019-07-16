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

namespace arrow {
namespace internal {

#if !defined(__clang__) && defined(__GNUC__) && __GNUC__ < 5

// atomic shared_ptr operations only appeared in gcc 5,
// emulate them with unsafe ops on gcc 4.x.

template <class T>
inline std::shared_ptr<T> atomic_load(const std::shared_ptr<T>* p) {
  return *p;
}

template <class T>
inline void atomic_store(std::shared_ptr<T>* p, std::shared_ptr<T> r) {
  *p = r;
}

#else

template <class T>
inline std::shared_ptr<T> atomic_load(const std::shared_ptr<T>* p) {
  return std::atomic_load(p);
}

template <class T>
inline void atomic_store(std::shared_ptr<T>* p, std::shared_ptr<T> r) {
  std::atomic_store(p, std::move(r));
}

#endif

}  // namespace internal
}  // namespace arrow
