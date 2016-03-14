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

// From Apache Impala as of 2016-01-29. Pared down to a minimal set of
// functions needed for parquet-cpp

#ifndef PARQUET_UTIL_SSE_UTIL_H
#define PARQUET_UTIL_SSE_UTIL_H

#ifdef PARQUET_USE_SSE
#include <emmintrin.h>
#endif

namespace parquet_cpp {


/// This class contains constants useful for text processing with SSE4.2 intrinsics.
namespace SSEUtil {
  /// Number of characters that fit in 64/128 bit register.  SSE provides instructions
  /// for loading 64 or 128 bits into a register at a time.
  static const int CHARS_PER_64_BIT_REGISTER = 8;
  static const int CHARS_PER_128_BIT_REGISTER = 16;

  /// SSE4.2 adds instructions for text processing.  The instructions have a control
  /// byte that determines some of functionality of the instruction.  (Equivalent to
  /// GCC's _SIDD_CMP_EQUAL_ANY, etc).
  static const int PCMPSTR_EQUAL_ANY    = 0x00; // strchr
  static const int PCMPSTR_EQUAL_EACH   = 0x08; // strcmp
  static const int PCMPSTR_UBYTE_OPS    = 0x00; // unsigned char (8-bits, rather than 16)
  static const int PCMPSTR_NEG_POLARITY = 0x10; // see Intel SDM chapter 4.1.4.

  /// In this mode, SSE text processing functions will return a mask of all the
  /// characters that matched.
  static const int STRCHR_MODE = PCMPSTR_EQUAL_ANY | PCMPSTR_UBYTE_OPS;

  /// In this mode, SSE text processing functions will return the number of
  /// bytes that match consecutively from the beginning.
  static const int STRCMP_MODE = PCMPSTR_EQUAL_EACH | PCMPSTR_UBYTE_OPS |
      PCMPSTR_NEG_POLARITY;

  /// Precomputed mask values up to 16 bits.
  static const int SSE_BITMASK[CHARS_PER_128_BIT_REGISTER] = {
    1 << 0,
    1 << 1,
    1 << 2,
    1 << 3,
    1 << 4,
    1 << 5,
    1 << 6,
    1 << 7,
    1 << 8,
    1 << 9,
    1 << 10,
    1 << 11,
    1 << 12,
    1 << 13,
    1 << 14,
    1 << 15,
  };
} // namespace SSEUtil

#ifdef PARQUET_USE_SSE

/// Define the SSE 4.2 intrinsics.  The caller must first verify at runtime (or codegen
/// IR load time) that the processor supports SSE 4.2 before calling these.  These are
/// defined outside the namespace because the IR w/ SSE 4.2 case needs to use macros.
#ifndef IR_COMPILE
/// When compiling to native code (i.e. not IR), we cannot use the -msse4.2 compiler
/// flag.  Otherwise, the compiler will emit SSE 4.2 instructions outside of the runtime
/// SSE 4.2 checks and Impala will crash on CPUs that don't support SSE 4.2
/// (IMPALA-1399/1646).  The compiler intrinsics cannot be used without -msse4.2, so we
/// define our own implementations of the intrinsics instead.

/// The PCMPxSTRy instructions require that the control byte 'mode' be encoded as an
/// immediate.  So, those need to be always inlined in order to always propagate the
/// mode constant into the inline asm.
#define SSE_ALWAYS_INLINE inline __attribute__ ((__always_inline__))

template<int MODE>
static inline __m128i SSE4_cmpestrm(__m128i str1, int len1, __m128i str2, int len2) {
#ifdef __clang__
  /// Use asm reg rather than Yz output constraint to workaround LLVM bug 13199 -
  /// clang doesn't support Y-prefixed asm constraints.
  register volatile __m128i result asm("xmm0");
  __asm__ volatile ("pcmpestrm %5, %2, %1"
      : "=x"(result) : "x"(str1), "xm"(str2), "a"(len1), "d"(len2), "i"(MODE) : "cc");
#else
  __m128i result;
  __asm__ volatile ("pcmpestrm %5, %2, %1"
      : "=Yz"(result) : "x"(str1), "xm"(str2), "a"(len1), "d"(len2), "i"(MODE) : "cc");
#endif
  return result;
}

template<int MODE>
static inline int SSE4_cmpestri(__m128i str1, int len1, __m128i str2, int len2) {
  int result;
  __asm__("pcmpestri %5, %2, %1"
      : "=c"(result) : "x"(str1), "xm"(str2), "a"(len1), "d"(len2), "i"(MODE) : "cc");
  return result;
}

static inline uint32_t SSE4_crc32_u8(uint32_t crc, uint8_t v) {
  __asm__("crc32b %1, %0" : "+r"(crc) : "rm"(v));
  return crc;
}

static inline uint32_t SSE4_crc32_u16(uint32_t crc, uint16_t v) {
  __asm__("crc32w %1, %0" : "+r"(crc) : "rm"(v));
  return crc;
}

static inline uint32_t SSE4_crc32_u32(uint32_t crc, uint32_t v) {
  __asm__("crc32l %1, %0" : "+r"(crc) : "rm"(v));
  return crc;
}

static inline uint32_t SSE4_crc32_u64(uint32_t crc, uint64_t v) {
  uint64_t result = crc;
  __asm__("crc32q %1, %0" : "+r"(result) : "rm"(v));
  return result;
}

static inline int64_t POPCNT_popcnt_u64(uint64_t a) {
  int64_t result;
  __asm__("popcntq %1, %0" : "=r"(result) : "mr"(a) : "cc");
  return result;
}

#undef SSE_ALWAYS_INLINE

#elif defined(__SSE4_2__) // IR_COMPILE for SSE 4.2.
/// When cross-compiling to IR, we cannot use inline asm because LLVM JIT does not
/// support it.  However, the cross-compiled IR is compiled twice: with and without
/// -msse4.2.  When -msse4.2 is enabled in the cross-compile, we can just use the
/// compiler intrinsics.

#include <smmintrin.h>

template<int MODE>
static inline __m128i SSE4_cmpestrm(
    __m128i str1, int len1, __m128i str2, int len2) {
  return _mm_cmpestrm(str1, len1, str2, len2, MODE);
}

template<int MODE>
static inline int SSE4_cmpestri(
    __m128i str1, int len1, __m128i str2, int len2) {
  return _mm_cmpestri(str1, len1, str2, len2, MODE);
}

#define SSE4_crc32_u8 _mm_crc32_u8
#define SSE4_crc32_u16 _mm_crc32_u16
#define SSE4_crc32_u32 _mm_crc32_u32
#define SSE4_crc32_u64 _mm_crc32_u64
#define POPCNT_popcnt_u64 _mm_popcnt_u64

#else  // IR_COMPILE without SSE 4.2.
/// When cross-compiling to IR without SSE 4.2 support (i.e. no -msse4.2), we cannot use
/// SSE 4.2 instructions.  Otherwise, the IR loading will fail on CPUs that don't
/// support SSE 4.2.  However, because the caller isn't allowed to call these routines
/// on CPUs that lack SSE 4.2 anyway, we can implement stubs for this case.

template<int MODE>
static inline __m128i SSE4_cmpestrm(__m128i str1, int len1, __m128i str2, int len2) {
  DCHECK(false) << "CPU doesn't support SSE 4.2";
  return (__m128i) { 0 }; // NOLINT
}

template<int MODE>
static inline int SSE4_cmpestri(__m128i str1, int len1, __m128i str2, int len2) {
  DCHECK(false) << "CPU doesn't support SSE 4.2";
  return 0;
}

static inline uint32_t SSE4_crc32_u8(uint32_t crc, uint8_t v) {
  DCHECK(false) << "CPU doesn't support SSE 4.2";
  return 0;
}

static inline uint32_t SSE4_crc32_u16(uint32_t crc, uint16_t v) {
  DCHECK(false) << "CPU doesn't support SSE 4.2";
  return 0;
}

static inline uint32_t SSE4_crc32_u32(uint32_t crc, uint32_t v) {
  DCHECK(false) << "CPU doesn't support SSE 4.2";
  return 0;
}

static inline uint32_t SSE4_crc32_u64(uint32_t crc, uint64_t v) {
  DCHECK(false) << "CPU doesn't support SSE 4.2";
  return 0;
}

static inline int64_t POPCNT_popcnt_u64(uint64_t a) {
  DCHECK(false) << "CPU doesn't support SSE 4.2";
  return 0;
}

#endif // IR_COMPILE

#else

static inline uint32_t SSE4_crc32_u8(uint32_t crc, uint8_t v) {
  DCHECK(false) << "SSE support is not enabled";
  return 0;
}

static inline uint32_t SSE4_crc32_u16(uint32_t crc, uint16_t v) {
  DCHECK(false) << "SSE support is not enabled";
  return 0;
}

static inline uint32_t SSE4_crc32_u32(uint32_t crc, uint32_t v) {
  DCHECK(false) << "SSE support is not enabled";
  return 0;
}

static inline uint32_t SSE4_crc32_u64(uint32_t crc, uint64_t v) {
  DCHECK(false) << "SSE support is not enabled";
  return 0;
}

static inline int64_t POPCNT_popcnt_u64(uint64_t a) {
  DCHECK(false) << "SSE support is not enabled";
  return 0;
}

#endif // PARQUET_USE_SSE

} // namespace parquet_cpp

#endif //  PARQUET_UTIL_SSE_UTIL_H
