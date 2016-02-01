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

#ifndef PARQUET_UTIL_BIT_UTIL_H
#define PARQUET_UTIL_BIT_UTIL_H

#if defined(__APPLE__)
  #include <machine/endian.h>
#else
  #include <endian.h>
#endif

#include "parquet/util/compiler-util.h"
#include "parquet/util/logging.h"

namespace parquet_cpp {

// Utility class to do standard bit tricks
// TODO: is this in boost or something else like that?
class BitUtil {
 public:
  // Returns the ceil of value/divisor
  static inline int Ceil(int value, int divisor) {
    return value / divisor + (value % divisor != 0);
  }

  // Returns 'value' rounded up to the nearest multiple of 'factor'
  static inline int RoundUp(int value, int factor) {
    return (value + (factor - 1)) / factor * factor;
  }

  // Returns 'value' rounded down to the nearest multiple of 'factor'
  static inline int RoundDown(int value, int factor) {
    return (value / factor) * factor;
  }

  // Returns the number of set bits in x
  static inline int Popcount(uint64_t x) {
    int count = 0;
    for (; x != 0; ++count) x &= x-1;
    return count;
  }

  // Returns the 'num_bits' least-significant bits of 'v'.
  static inline uint64_t TrailingBits(uint64_t v, int num_bits) {
    if (UNLIKELY(num_bits == 0)) return 0;
    if (UNLIKELY(num_bits >= 64)) return v;
    int n = 64 - num_bits;
    return (v << n) >> n;
  }

  // Returns ceil(log2(x)).
  // TODO: this could be faster if we use __builtin_clz.  Fix this if this ever shows up
  // in a hot path.
  static inline int Log2(uint64_t x) {
    if (x == 0) return 0;
    // Compute result = ceil(log2(x))
    //                = floor(log2(x - 1)) + 1, for x > 1
    // by finding the position of the most significant bit (1-indexed) of x - 1
    // (floor(log2(n)) = MSB(n) (0-indexed))
    --x;
    int result = 1;
    while (x >>= 1) ++result;
    return result;
  }

  // Returns the minimum number of bits needed to represent the value of 'x'
  static inline int NumRequiredBits(uint64_t x) {
    for (int i = 63; i >= 0; --i) {
      if (x & 1L << i) return i + 1;
    }
    return 0;
  }

  // Swaps the byte order (i.e. endianess)
  static inline int64_t ByteSwap(int64_t value) {
    return __builtin_bswap64(value);
  }
  static inline uint64_t ByteSwap(uint64_t value) {
    return static_cast<uint64_t>(__builtin_bswap64(value));
  }
  static inline int32_t ByteSwap(int32_t value) {
    return __builtin_bswap32(value);
  }
  static inline uint32_t ByteSwap(uint32_t value) {
    return static_cast<uint32_t>(__builtin_bswap32(value));
  }
  static inline int16_t ByteSwap(int16_t value) {
    return (((value >> 8) & 0xff) | ((value & 0xff) << 8));
  }
  static inline uint16_t ByteSwap(uint16_t value) {
    return static_cast<uint16_t>(ByteSwap(static_cast<int16_t>(value)));
  }

  // Write the swapped bytes into dst. Src and st cannot overlap.
  static inline void ByteSwap(void* dst, const void* src, int len) {
    switch (len) {
      case 1:
        *reinterpret_cast<int8_t*>(dst) = *reinterpret_cast<const int8_t*>(src);
        return;
      case 2:
        *reinterpret_cast<int16_t*>(dst) =
            ByteSwap(*reinterpret_cast<const int16_t*>(src));
        return;
      case 4:
        *reinterpret_cast<int32_t*>(dst) =
            ByteSwap(*reinterpret_cast<const int32_t*>(src));
        return;
      case 8:
        *reinterpret_cast<int64_t*>(dst) =
            ByteSwap(*reinterpret_cast<const int64_t*>(src));
        return;
      default: break;
    }

    uint8_t* d = reinterpret_cast<uint8_t*>(dst);
    const uint8_t* s = reinterpret_cast<const uint8_t*>(src);
    for (int i = 0; i < len; ++i) {
      d[i] = s[len - i - 1];
    }
  }

  // Converts to big endian format (if not already in big endian) from the
  // machine's native endian format.
#if __BYTE_ORDER == __LITTLE_ENDIAN
  static inline int64_t  ToBigEndian(int64_t value)  { return ByteSwap(value); }
  static inline uint64_t ToBigEndian(uint64_t value) { return ByteSwap(value); }
  static inline int32_t  ToBigEndian(int32_t value)  { return ByteSwap(value); }
  static inline uint32_t ToBigEndian(uint32_t value) { return ByteSwap(value); }
  static inline int16_t  ToBigEndian(int16_t value)  { return ByteSwap(value); }
  static inline uint16_t ToBigEndian(uint16_t value) { return ByteSwap(value); }
#else
  static inline int64_t  ToBigEndian(int64_t val)  { return val; }
  static inline uint64_t ToBigEndian(uint64_t val) { return val; }
  static inline int32_t  ToBigEndian(int32_t val)  { return val; }
  static inline uint32_t ToBigEndian(uint32_t val) { return val; }
  static inline int16_t  ToBigEndian(int16_t val)  { return val; }
  static inline uint16_t ToBigEndian(uint16_t val) { return val; }
#endif

  // Converts from big endian format to the machine's native endian format.
#if __BYTE_ORDER == __LITTLE_ENDIAN
  static inline int64_t  FromBigEndian(int64_t value)  { return ByteSwap(value); }
  static inline uint64_t FromBigEndian(uint64_t value) { return ByteSwap(value); }
  static inline int32_t  FromBigEndian(int32_t value)  { return ByteSwap(value); }
  static inline uint32_t FromBigEndian(uint32_t value) { return ByteSwap(value); }
  static inline int16_t  FromBigEndian(int16_t value)  { return ByteSwap(value); }
  static inline uint16_t FromBigEndian(uint16_t value) { return ByteSwap(value); }
#else
  static inline int64_t  FromBigEndian(int64_t val)  { return val; }
  static inline uint64_t FromBigEndian(uint64_t val) { return val; }
  static inline int32_t  FromBigEndian(int32_t val)  { return val; }
  static inline uint32_t FromBigEndian(uint32_t val) { return val; }
  static inline int16_t  FromBigEndian(int16_t val)  { return val; }
  static inline uint16_t FromBigEndian(uint16_t val) { return val; }
#endif
};

} // namespace parquet_cpp

#endif // PARQUET_UTIL_BIT_UTIL_H
