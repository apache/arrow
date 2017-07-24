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

#ifndef ARROW_UTIL_BIT_UTIL_H
#define ARROW_UTIL_BIT_UTIL_H

#if defined(__APPLE__)
#include <machine/endian.h>
#elif defined(_WIN32)
#define __LITTLE_ENDIAN 1
#else
#include <endian.h>
#endif

#if defined(_MSC_VER)
#define ARROW_BYTE_SWAP64 _byteswap_uint64
#define ARROW_BYTE_SWAP32 _byteswap_ulong
#else
#define ARROW_BYTE_SWAP64 __builtin_bswap64
#define ARROW_BYTE_SWAP32 __builtin_bswap32
#endif

#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "arrow/util/compiler-util.h"
#include "arrow/util/visibility.h"

#ifdef ARROW_USE_SSE
#include "arrow/util/cpu-info.h"
#include "arrow/util/sse-util.h"
#endif

namespace arrow {

#define INIT_BITSET(valid_bits_vector, valid_bits_index)        \
  int byte_offset_##valid_bits_vector = (valid_bits_index) / 8; \
  int bit_offset_##valid_bits_vector = (valid_bits_index) % 8;  \
  uint8_t bitset_##valid_bits_vector = valid_bits_vector[byte_offset_##valid_bits_vector];

#define READ_NEXT_BITSET(valid_bits_vector)                                          \
  bit_offset_##valid_bits_vector++;                                                  \
  if (bit_offset_##valid_bits_vector == 8) {                                         \
    bit_offset_##valid_bits_vector = 0;                                              \
    byte_offset_##valid_bits_vector++;                                               \
    bitset_##valid_bits_vector = valid_bits_vector[byte_offset_##valid_bits_vector]; \
  }

// TODO(wesm): The source from Impala was depending on boost::make_unsigned
//
// We add a partial stub implementation here

template <typename T>
struct make_unsigned {};

template <>
struct make_unsigned<int8_t> {
  typedef uint8_t type;
};

template <>
struct make_unsigned<int16_t> {
  typedef uint16_t type;
};

template <>
struct make_unsigned<int32_t> {
  typedef uint32_t type;
};

template <>
struct make_unsigned<int64_t> {
  typedef uint64_t type;
};

class Buffer;
class MemoryPool;
class MutableBuffer;
class Status;

namespace BitUtil {

static constexpr uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

// the ~i byte version of kBitmaks
static constexpr uint8_t kFlippedBitmask[] = {254, 253, 251, 247, 239, 223, 191, 127};

static inline int64_t CeilByte(int64_t size) { return (size + 7) & ~7; }

static inline int64_t BytesForBits(int64_t size) { return CeilByte(size) / 8; }

static inline int64_t Ceil2Bytes(int64_t size) { return (size + 15) & ~15; }

static inline bool GetBit(const uint8_t* bits, int64_t i) {
  return (bits[i / 8] & kBitmask[i % 8]) != 0;
}

static inline bool BitNotSet(const uint8_t* bits, int64_t i) {
  return (bits[i / 8] & kBitmask[i % 8]) == 0;
}

static inline void ClearBit(uint8_t* bits, int64_t i) {
  bits[i / 8] &= kFlippedBitmask[i % 8];
}

static inline void SetBit(uint8_t* bits, int64_t i) { bits[i / 8] |= kBitmask[i % 8]; }

/// Set bit if is_set is true, but cannot clear bit
static inline void SetArrayBit(uint8_t* bits, int i, bool is_set) {
  if (is_set) {
    SetBit(bits, i);
  }
}

static inline void SetBitTo(uint8_t* bits, int64_t i, bool bit_is_set) {
  // TODO: speed up. See https://graphics.stanford.edu/~seander/bithacks.html
  // "Conditionally set or clear bits without branching"
  if (bit_is_set) {
    SetBit(bits, i);
  } else {
    ClearBit(bits, i);
  }
}

// Returns the minimum number of bits needed to represent the value of 'x'
static inline int NumRequiredBits(uint64_t x) {
  for (int i = 63; i >= 0; --i) {
    if (x & (UINT64_C(1) << i)) return i + 1;
  }
  return 0;
}

/// Returns the smallest power of two that contains v. Taken from
/// http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
/// TODO: Pick a better name, as it is not clear what happens when the input is
/// already a power of two.
static inline int64_t NextPower2(int64_t n) {
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  n++;
  return n;
}

static inline bool IsMultipleOf64(int64_t n) { return (n & 63) == 0; }

static inline bool IsMultipleOf8(int64_t n) { return (n & 7) == 0; }

/// Returns the ceil of value/divisor
static inline int64_t Ceil(int64_t value, int64_t divisor) {
  return value / divisor + (value % divisor != 0);
}

/// Returns 'value' rounded up to the nearest multiple of 'factor'
inline int64_t RoundUp(int64_t value, int64_t factor) {
  return (value + (factor - 1)) / factor * factor;
}

/// Returns 'value' rounded down to the nearest multiple of 'factor'
static inline int64_t RoundDown(int64_t value, int64_t factor) {
  return (value / factor) * factor;
}

/// Returns 'value' rounded up to the nearest multiple of 'factor' when factor is
/// a power of two
static inline int RoundUpToPowerOf2(int value, int factor) {
  // DCHECK((factor > 0) && ((factor & (factor - 1)) == 0));
  return (value + (factor - 1)) & ~(factor - 1);
}

static inline int RoundDownToPowerOf2(int value, int factor) {
  // DCHECK((factor > 0) && ((factor & (factor - 1)) == 0));
  return value & ~(factor - 1);
}

/// Specialized round up and down functions for frequently used factors,
/// like 8 (bits->bytes), 32 (bits->i32), and 64 (bits->i64).
/// Returns the rounded up number of bytes that fit the number of bits.
static inline uint32_t RoundUpNumBytes(uint32_t bits) { return (bits + 7) >> 3; }

/// Returns the rounded down number of bytes that fit the number of bits.
static inline uint32_t RoundDownNumBytes(uint32_t bits) { return bits >> 3; }

/// Returns the rounded up to 32 multiple. Used for conversions of bits to i32.
static inline uint32_t RoundUpNumi32(uint32_t bits) { return (bits + 31) >> 5; }

/// Returns the rounded up 32 multiple.
static inline uint32_t RoundDownNumi32(uint32_t bits) { return bits >> 5; }

/// Returns the rounded up to 64 multiple. Used for conversions of bits to i64.
static inline uint32_t RoundUpNumi64(uint32_t bits) { return (bits + 63) >> 6; }

/// Returns the rounded down to 64 multiple.
static inline uint32_t RoundDownNumi64(uint32_t bits) { return bits >> 6; }

static inline int64_t RoundUpToMultipleOf64(int64_t num) {
  // TODO(wesm): is this definitely needed?
  // DCHECK_GE(num, 0);
  constexpr int64_t round_to = 64;
  constexpr int64_t force_carry_addend = round_to - 1;
  constexpr int64_t truncate_bitmask = ~(round_to - 1);
  constexpr int64_t max_roundable_num = std::numeric_limits<int64_t>::max() - round_to;
  if (num <= max_roundable_num) {
    return (num + force_carry_addend) & truncate_bitmask;
  }
  // handle overflow case.  This should result in a malloc error upstream
  return num;
}

/// Non hw accelerated pop count.
/// TODO: we don't use this in any perf sensitive code paths currently.  There
/// might be a much faster way to implement this.
static inline int PopcountNoHw(uint64_t x) {
  int count = 0;
  for (; x != 0; ++count) x &= x - 1;
  return count;
}

/// Returns the number of set bits in x
static inline int Popcount(uint64_t x) {
#ifdef ARROW_USE_SSE
  if (LIKELY(CpuInfo::IsSupported(CpuInfo::POPCNT))) {
    return POPCNT_popcnt_u64(x);
  } else {
    return PopcountNoHw(x);
  }
#else
  return PopcountNoHw(x);
#endif
}

// Compute correct population count for various-width signed integers
template <typename T>
static inline int PopcountSigned(T v) {
  // Converting to same-width unsigned then extending preserves the bit pattern.
  return BitUtil::Popcount(static_cast<typename make_unsigned<T>::type>(v));
}

/// Returns the 'num_bits' least-significant bits of 'v'.
static inline uint64_t TrailingBits(uint64_t v, int num_bits) {
  if (UNLIKELY(num_bits == 0)) return 0;
  if (UNLIKELY(num_bits >= 64)) return v;
  int n = 64 - num_bits;
  return (v << n) >> n;
}

/// Returns ceil(log2(x)).
/// TODO: this could be faster if we use __builtin_clz.  Fix this if this ever shows up
/// in a hot path.
static inline int Log2(uint64_t x) {
  // DCHECK_GT(x, 0);
  if (x == 1) return 0;
  // Compute result = ceil(log2(x))
  //                = floor(log2(x - 1)) + 1, for x > 1
  // by finding the position of the most significant bit (1-indexed) of x - 1
  // (floor(log2(n)) = MSB(n) (0-indexed))
  --x;
  int result = 1;
  while (x >>= 1) ++result;
  return result;
}

/// Swaps the byte order (i.e. endianess)
static inline int64_t ByteSwap(int64_t value) { return ARROW_BYTE_SWAP64(value); }
static inline uint64_t ByteSwap(uint64_t value) {
  return static_cast<uint64_t>(ARROW_BYTE_SWAP64(value));
}
static inline int32_t ByteSwap(int32_t value) { return ARROW_BYTE_SWAP32(value); }
static inline uint32_t ByteSwap(uint32_t value) {
  return static_cast<uint32_t>(ARROW_BYTE_SWAP32(value));
}
static inline int16_t ByteSwap(int16_t value) {
  constexpr int16_t m = static_cast<int16_t>(0xff);
  return static_cast<int16_t>(((value >> 8) & m) | ((value & m) << 8));
}
static inline uint16_t ByteSwap(uint16_t value) {
  return static_cast<uint16_t>(ByteSwap(static_cast<int16_t>(value)));
}

/// Write the swapped bytes into dst. Src and st cannot overlap.
static inline void ByteSwap(void* dst, const void* src, int len) {
  switch (len) {
    case 1:
      *reinterpret_cast<int8_t*>(dst) = *reinterpret_cast<const int8_t*>(src);
      return;
    case 2:
      *reinterpret_cast<int16_t*>(dst) = ByteSwap(*reinterpret_cast<const int16_t*>(src));
      return;
    case 4:
      *reinterpret_cast<int32_t*>(dst) = ByteSwap(*reinterpret_cast<const int32_t*>(src));
      return;
    case 8:
      *reinterpret_cast<int64_t*>(dst) = ByteSwap(*reinterpret_cast<const int64_t*>(src));
      return;
    default:
      break;
  }

  uint8_t* d = reinterpret_cast<uint8_t*>(dst);
  const uint8_t* s = reinterpret_cast<const uint8_t*>(src);
  for (int i = 0; i < len; ++i) {
    d[i] = s[len - i - 1];
  }
}

/// Converts to big endian format (if not already in big endian) from the
/// machine's native endian format.
#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline int64_t ToBigEndian(int64_t value) { return ByteSwap(value); }
static inline uint64_t ToBigEndian(uint64_t value) { return ByteSwap(value); }
static inline int32_t ToBigEndian(int32_t value) { return ByteSwap(value); }
static inline uint32_t ToBigEndian(uint32_t value) { return ByteSwap(value); }
static inline int16_t ToBigEndian(int16_t value) { return ByteSwap(value); }
static inline uint16_t ToBigEndian(uint16_t value) { return ByteSwap(value); }
#else
static inline int64_t ToBigEndian(int64_t val) { return val; }
static inline uint64_t ToBigEndian(uint64_t val) { return val; }
static inline int32_t ToBigEndian(int32_t val) { return val; }
static inline uint32_t ToBigEndian(uint32_t val) { return val; }
static inline int16_t ToBigEndian(int16_t val) { return val; }
static inline uint16_t ToBigEndian(uint16_t val) { return val; }
#endif

/// Converts from big endian format to the machine's native endian format.
#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline int64_t FromBigEndian(int64_t value) { return ByteSwap(value); }
static inline uint64_t FromBigEndian(uint64_t value) { return ByteSwap(value); }
static inline int32_t FromBigEndian(int32_t value) { return ByteSwap(value); }
static inline uint32_t FromBigEndian(uint32_t value) { return ByteSwap(value); }
static inline int16_t FromBigEndian(int16_t value) { return ByteSwap(value); }
static inline uint16_t FromBigEndian(uint16_t value) { return ByteSwap(value); }
#else
static inline int64_t FromBigEndian(int64_t val) { return val; }
static inline uint64_t FromBigEndian(uint64_t val) { return val; }
static inline int32_t FromBigEndian(int32_t val) { return val; }
static inline uint32_t FromBigEndian(uint32_t val) { return val; }
static inline int16_t FromBigEndian(int16_t val) { return val; }
static inline uint16_t FromBigEndian(uint16_t val) { return val; }
#endif

// Logical right shift for signed integer types
// This is needed because the C >> operator does arithmetic right shift
// Negative shift amounts lead to undefined behavior
template <typename T>
static T ShiftRightLogical(T v, int shift) {
  // Conversion to unsigned ensures most significant bits always filled with 0's
  return static_cast<typename make_unsigned<T>::type>(v) >> shift;
}

void FillBitsFromBytes(const std::vector<uint8_t>& bytes, uint8_t* bits);
ARROW_EXPORT Status BytesToBits(const std::vector<uint8_t>&, std::shared_ptr<Buffer>*);

}  // namespace BitUtil

// ----------------------------------------------------------------------
// Bitmap utilities

Status ARROW_EXPORT GetEmptyBitmap(MemoryPool* pool, int64_t length,
                                   std::shared_ptr<MutableBuffer>* result);

/// Copy a bit range of an existing bitmap
///
/// \param[in] pool memory pool to allocate memory from
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
/// \param[out] out the resulting copy
///
/// \return Status message
Status ARROW_EXPORT CopyBitmap(MemoryPool* pool, const uint8_t* bitmap, int64_t offset,
                               int64_t length, std::shared_ptr<Buffer>* out);

/// Compute the number of 1's in the given data array
///
/// \param[in] data a packed LSB-ordered bitmap as a byte array
/// \param[in] bit_offset a bitwise offset into the bitmap
/// \param[in] length the number of bits to inspect in the bitmap relative to the offset
///
/// \return The number of set (1) bits in the range
int64_t ARROW_EXPORT CountSetBits(const uint8_t* data, int64_t bit_offset,
                                  int64_t length);

bool ARROW_EXPORT BitmapEquals(const uint8_t* left, int64_t left_offset,
                               const uint8_t* right, int64_t right_offset,
                               int64_t bit_length);
}  // namespace arrow

#endif  // ARROW_UTIL_BIT_UTIL_H
