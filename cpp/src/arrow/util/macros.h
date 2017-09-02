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

#ifndef ARROW_UTIL_MACROS_H
#define ARROW_UTIL_MACROS_H

// From Google gutil
#ifndef ARROW_DISALLOW_COPY_AND_ASSIGN
#define ARROW_DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&) = delete;            \
  TypeName& operator=(const TypeName&) = delete
#endif

#define ARROW_UNUSED(x) (void)x

//
// GCC can be told that a certain branch is not likely to be taken (for
// instance, a CHECK failure), and use that information in static analysis.
// Giving it this information can help it optimize for the common case in
// the absence of better information (ie. -fprofile-arcs).
//
#if defined(__GNUC__)
#define ARROW_PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define ARROW_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#define ARROW_NORETURN __attribute__((noreturn))
#define ARROW_PREFETCH(addr) __builtin_prefetch(addr)
#elif defined(_MSC_VER)
#define ARROW_NORETURN __declspec(noreturn)
#define ARROW_PREDICT_FALSE(x) x
#define ARROW_PREDICT_TRUE(x) x
#define ARROW_PREFETCH(addr)
#else
#define ARROW_NORETURN
#define ARROW_PREDICT_FALSE(x) x
#define ARROW_PREDICT_TRUE(x) x
#define ARROW_PREFETCH(addr)
#endif

#if (defined(__GNUC__) || defined(__APPLE__))
#define ARROW_MUST_USE_RESULT __attribute__((warn_unused_result))
#elif defined(_MSC_VER)
#define ARROW_MUST_USE_RESULT
#else
#define ARROW_MUST_USE_RESULT
#endif

// macros to disable padding
// these macros are portable across different compilers and platforms
//[https://github.com/google/flatbuffers/blob/master/include/flatbuffers/flatbuffers.h#L1355]
#if !defined(MANUALLY_ALIGNED_STRUCT)
#if defined(_MSC_VER)
#define MANUALLY_ALIGNED_STRUCT(alignment) \
  __pragma(pack(1));                             \
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
#endif // !defined(MANUALLY_ALIGNED_STRUCT)

#endif  // ARROW_UTIL_MACROS_H
