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

#include <cstdint>
#include <memory>

#include "arrow/util/macros.h"

namespace arrow {
namespace internal {

// A helper function for doing memcpy with multiple threads. This is required
// to saturate the memory bandwidth of modern cpus.
void parallel_memcopy(uint8_t* dst, const uint8_t* src, int64_t nbytes,
                      uintptr_t block_size, int num_threads);

// A helper function for checking if two wrapped objects implementing `Equals`
// are equal.
template <typename T>
bool SharedPtrEquals(const std::shared_ptr<T>& left, const std::shared_ptr<T>& right) {
  if (left == right) return true;
  if (left == NULLPTR || right == NULLPTR) return false;
  return left->Equals(*right);
}

// Used to wrap potentially null buffers (which willl never be accessed if null).
// Pointer arithmetic on nullptr (for example to apply a byte offset) is undefined
// behavior even if the pointer is never dereferenced, so provide an alternative
// nonsense value which can be safely added to.
inline const uint8_t* EnsureNotNull(const uint8_t* ptr) {
  static const uint8_t byte{};
  return ptr == nullptr ? &byte : ptr;
}

}  // namespace internal
}  // namespace arrow
