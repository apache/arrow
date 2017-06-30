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

#ifndef ARROW_UTIL_COMPILER_UTIL_H
#define ARROW_UTIL_COMPILER_UTIL_H

// Branch prediction macro hints for GCC
#ifdef LIKELY
#undef LIKELY
#endif

#ifdef UNLIKELY
#undef UNLIKELY
#endif

#ifdef _MSC_VER
#define LIKELY(expr) expr
#define UNLIKELY(expr) expr
#else
#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#endif

#define PREFETCH(addr) __builtin_prefetch(addr)

// macros to disable padding
// these macros are portable across different compilers and platforms
//[https://github.com/google/flatbuffers/blob/master/include/flatbuffers/flatbuffers.h#L1355]
#if defined(_MSC_VER)
#define MANUALLY_ALIGNED_STRUCT(alignment) \
  __pragma(pack(1));                       \
  struct __declspec(align(alignment))
#define STRUCT_END(name, size) \
  __pragma(pack());            \
  static_assert(sizeof(name) == size, "compiler breaks packing rules")
#elif defined(__GNUC__) || defined(__clang__)
#define MANUALLY_ALIGNED_STRUCT(alignment) \
  _Pragma("pack(1)") struct __attribute__((aligned(alignment)))
#define STRUCT_END(name, size) \
  _Pragma("pack()") static_assert(sizeof(name) == size, "compiler breaks packing rules")
#else
#error Unknown compiler, please define structure alignment macros
#endif

#endif  // ARROW_UTIL_COMPILER_UTIL_H
