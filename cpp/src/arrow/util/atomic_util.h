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
#include <type_traits>

namespace arrow {
namespace util {

// Updates `to_max` to contain the maximum of `to_max` and `val`
// and returns the new maximum. It is expected that `to_max` be treated
// as a shared maximum.
template <typename T>
inline T AtomicMax(std::atomic<T>& to_max, T val) {
  static_assert(std::is_arithmetic<T>::value,
                "Maximum only makes sense on numeric types!");
  T local_to_max = to_max.load(std::memory_order_relaxed);
  while (val > local_to_max &&
         !to_max.compare_exchange_weak(local_to_max, val, std::memory_order_release,
                                       std::memory_order_relaxed)) {
  }
  return to_max.load(std::memory_order_relaxed);
}

}  // namespace util
}  // namespace arrow
