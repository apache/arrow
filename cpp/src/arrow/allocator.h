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

#ifndef ARROW_ALLOCATOR_H
#define ARROW_ALLOCATOR_H

#include <cstddef>
#include <memory>
#include <utility>

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"

namespace arrow {

template <class T>
class stl_allocator {
 public:
  using value_type = T;
  using pointer = T*;
  using const_pointer = const T*;
  using reference = T&;
  using const_reference = const T&;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;

  template <class U>
  struct rebind {
    using other = stl_allocator<U>;
  };

  stl_allocator() noexcept : pool_(default_memory_pool()) {}
  explicit stl_allocator(MemoryPool* pool) noexcept : pool_(pool) {}

  template <class U>
  stl_allocator(const stl_allocator<U>& rhs) noexcept : pool_(rhs.pool_) {}

  ~stl_allocator() { pool_ = NULLPTR; }

  pointer address(reference r) const noexcept { return std::addressof(r); }

  const_pointer address(const_reference r) const noexcept { return std::addressof(r); }

  pointer allocate(size_type n, const void* /*hint*/ = NULLPTR) {
    uint8_t* data;
    Status s = pool_->Allocate(n * sizeof(T), &data);
    if (!s.ok()) throw std::bad_alloc();
    return reinterpret_cast<pointer>(data);
  }

  void deallocate(pointer p, size_type n) {
    pool_->Free(reinterpret_cast<uint8_t*>(p), n * sizeof(T));
  }

  size_type size_max() const noexcept { return size_type(-1) / sizeof(T); }

  template <class U, class... Args>
  void construct(U* p, Args&&... args) {
    new (reinterpret_cast<void*>(p)) U(std::forward<Args>(args)...);
  }

  template <class U>
  void destroy(U* p) {
    p->~U();
  }

  MemoryPool* pool() const noexcept { return pool_; }

 private:
  MemoryPool* pool_;
};

template <class T1, class T2>
bool operator==(const stl_allocator<T1>& lhs, const stl_allocator<T2>& rhs) noexcept {
  return lhs.pool() == rhs.pool();
}

template <class T1, class T2>
bool operator!=(const stl_allocator<T1>& lhs, const stl_allocator<T2>& rhs) noexcept {
  return !(lhs == rhs);
}

}  // namespace arrow

#endif  // ARROW_ALLOCATOR_H
