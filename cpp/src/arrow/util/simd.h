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

#ifdef _MSC_VER
// MSVC x86_64/arm64

#if defined(_M_AMD64) || defined(_M_X64)
#include <intrin.h>
#elif defined(_M_ARM64)
#include <arm64_neon.h>
#endif

#else
// gcc/clang (possibly others)

#if defined(ARROW_HAVE_AVX2) || defined(ARROW_HAVE_AVX512) || \
    defined(ARROW_HAVE_RUNTIME_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX512)
#include <immintrin.h>
#elif defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_RUNTIME_SSE4_2)
#include <nmmintrin.h>
#endif

#ifdef ARROW_HAVE_NEON
#include <arm_neon.h>
#endif

#ifdef ARROW_HAVE_ARMV8_CRC
#include <arm_acle.h>
#endif

#endif

#define STRINGIFY(a) #a

// Push the SIMD compiler flag online to build the code path
#if defined(__clang__)
#define TARGET_CODE_START(Target) \
  _Pragma(STRINGIFY(              \
      clang attribute push(__attribute__((target(Target))), apply_to = function)))
#define TARGET_CODE_STOP _Pragma("clang attribute pop")
#elif defined(__GNUC__)
#define TARGET_CODE_START(Target) \
  _Pragma("GCC push_options") _Pragma(STRINGIFY(GCC target(Target)))
#define TARGET_CODE_STOP _Pragma("GCC pop_options")
#else
// MSVS can use intrinsic without appending the compiler SIMD flag
#define TARGET_CODE_START(Target)
#define TARGET_CODE_STOP
#endif

#define TARGET_CODE_START_SSE4_2 TARGET_CODE_START("sse4.2")
#define TARGET_CODE_START_AVX2 TARGET_CODE_START("avx2")
#define TARGET_CODE_START_AVX512 TARGET_CODE_START("avx512f")

#define TARGET_CODE_START_AVX2_BMI2 TARGET_CODE_START("avx2,bmi2")
