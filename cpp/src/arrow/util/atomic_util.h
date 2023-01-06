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

#if defined(__clang) || defined(__GNUC__)
template <typename T>
inline T AtomicLoad(T* addr,
                    std::memory_order order = std::memory_order_seq_cst) noexcept {
  T ret;
  __atomic_load(addr, &ret, order);
  return ret;
}

template <typename T>
inline void AtomicStore(T* addr, T& val,
                        std::memory_order order = std::memory_order_seq_cst) noexcept {
  __atomic_store(addr, val, order);
}

template <typename T>
inline T AtomicFetchAdd(T* addr, T& val,
                        std::memory_order order = std::memory_order_seq_cst) noexcept {
  static_assert(std::is_integral<T>::value,
                "AtomicFetchAdd can only be used on integral types");
  return __atomic_fetch_add(addr, val, order);
}

template <typename T>
inline T AtomicFetchSub(T* addr, T& val,
                        std::memory_order order = std::memory_order_seq_cst) noexcept {
  static_assert(std::is_integral<T>::value,
                "AtomicFetchSub can only be used on integral types");
  return __atomic_fetch_sub(addr, val, order);
}

#elif defined(_MSC_VER)
#include <intrin.h>
template <typename T>
inline T AtomicLoad(T* addr, std::memory_order /*order*/) noexcept {
  T val = *addr;
  _ReadWriteBarrier();
  return val;
}

template <typename T>
inline void AtomicStore(T* addr, T& val, std::memory_order /*order*/) noexcept {
  _ReadWriteBarrier();
  *addr = val;
}

template <typename T>
inline T AtomicFetchAdd(T* addr, T& val, std::memory_order /*order*/) noexcept {
  static_assert(std::is_integral<T>::value,
                "AtomicFetchAdd can only be used on integral types");
  if constexpr (sizeof(T) == 1) return _InterlockedExchangeAdd8(addr, val);
  if constexpr (sizeof(T) == 2) return _InterlockedExchangeAdd16(addr, val);
  if constexpr (sizeof(T) == 4) return _InterlockedExchangeAdd(addr, val);
  if constexpr (sizeof(T) == 8) {
#if _WIN64
    return _InterlockedExchangeAdd64(addr, val);
#else
    _ReadWriteBarrier();
    T expected = *addr;
    for (;;) {
      T new_val = expected + val;
      T prev = _InterlockedCompareExchange64(addr, new_val, expected);
      if (prev == expected) return prev;
      expected = prev;
    }
  }
#endif
  }

  template <typename T>
  inline T AtomicFetchSub(T * addr, T & val, std::memory_order /*order*/) noexcept {
    return AtomicFetchAdd(addr, -val);
  }
#endif
}  // namespace util
}  // namespace arrow
