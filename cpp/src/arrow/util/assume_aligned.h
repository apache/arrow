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

#include <cstddef>

namespace arrow::util {

template <size_t N, typename P>
[[nodiscard]] constexpr P* AssumeAligned(P* ptr) {
#if defined(__has_builtin)
#if __has_builtin(__builtin_assume_aligned)
#define ARROW_HAS_BUILTIN_ASSUME_ALIGNED
#endif
#endif

#if defined(ARROW_HAS_BUILTIN_ASSUME_ALIGNED)
#undef ARROW_HAS_BUILTIN_ASSUME_ALIGNED
  return static_cast<P*>(__builtin_assume_aligned(ptr, N));
#else
  return ptr;
#endif
}

template <typename T, typename P>
[[nodiscard]] constexpr P* AssumeAlignedAs(P* ptr) {
  return AssumeAligned<alignof(T)>(ptr);
}

}  // namespace arrow::util
