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

#ifdef _WIN32
#define ARROW_LITTLE_ENDIAN 1
#else
#ifdef __APPLE__
#include <machine/endian.h>
#else
#include <endian.h>
#endif
#
#ifndef __BYTE_ORDER__
#error "__BYTE_ORDER__ not defined"
#endif
#
#ifndef __ORDER_LITTLE_ENDIAN__
#error "__ORDER_LITTLE_ENDIAN__ not defined"
#endif
#
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define ARROW_LITTLE_ENDIAN 1
#else
#define ARROW_LITTLE_ENDIAN 0
#endif
#endif

#if defined(_MSC_VER)
#include <intrin.h>
#pragma intrinsic(_BitScanReverse)
#define ARROW_BYTE_SWAP64 _byteswap_uint64
#define ARROW_BYTE_SWAP32 _byteswap_ulong
#else
#define ARROW_BYTE_SWAP64 __builtin_bswap64
#define ARROW_BYTE_SWAP32 __builtin_bswap32
#endif

#include <cstdint>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

#include "arrow/util/macros.h"
#include "arrow/util/type_traits.h"
#include "arrow/util/visibility.h"

#ifdef ARROW_USE_SSE
#include "arrow/util/cpu-info.h"
#include "arrow/util/sse-util.h"
#endif

namespace arrow {

namespace detail {

template <typename Integer>
typename std::make_unsigned<Integer>::type as_unsigned(Integer x) {
  return static_cast<typename std::make_unsigned<Integer>::type>(x);
}

}  // namespace detail

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
  // https://graphics.stanford.edu/~seander/bithacks.html
  // "Conditionally set or clear bits without branching"
  bits[i / 8] ^= static_cast<uint8_t>(-static_cast<uint8_t>(bit_is_set) ^ bits[i / 8]) &
                 kBitmask[i % 8];
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

template <int64_t ROUND_TO>
static inline int64_t RoundToPowerOfTwo(int64_t num) {
  // TODO(wesm): is this definitely needed?
  // DCHECK_GE(num, 0);
  constexpr int64_t force_carry_addend = ROUND_TO - 1;
  constexpr int64_t truncate_bitmask = ~(ROUND_TO - 1);
  constexpr int64_t max_roundable_num = std::numeric_limits<int64_t>::max() - ROUND_TO;
  if (num <= max_roundable_num) {
    return (num + force_carry_addend) & truncate_bitmask;
  }
  // handle overflow case.  This should result in a malloc error upstream
  return num;
}

static inline int64_t RoundUpToMultipleOf64(int64_t num) {
  return RoundToPowerOfTwo<64>(num);
}

static inline int64_t RoundUpToMultipleOf8(int64_t num) {
  return RoundToPowerOfTwo<8>(num);
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
  if (ARROW_PREDICT_TRUE(CpuInfo::IsSupported(CpuInfo::POPCNT))) {
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
  return BitUtil::Popcount(detail::as_unsigned(v));
}

/// Returns the 'num_bits' least-significant bits of 'v'.
static inline uint64_t TrailingBits(uint64_t v, int num_bits) {
  if (ARROW_PREDICT_FALSE(num_bits == 0)) return 0;
  if (ARROW_PREDICT_FALSE(num_bits >= 64)) return v;
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

/// \brief Count the number of leading zeros in a 32 bit integer.
static inline int64_t CountLeadingZeros(uint32_t value) {
// DCHECK_NE(value, 0);
#if defined(__clang__) || defined(__GNUC__)
  return static_cast<int64_t>(__builtin_clz(value));
#elif defined(_MSC_VER)
  unsigned long index;                                         // NOLINT
  _BitScanReverse(&index, static_cast<unsigned long>(value));  // NOLINT
  return 31LL - static_cast<int64_t>(index);
#else
  int64_t bitpos = 0;
  while (value != 0) {
    value >>= 1;
    ++bitpos;
  }
  return 32LL - bitpos;
#endif
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
  constexpr auto m = static_cast<int16_t>(0xff);
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

  auto d = reinterpret_cast<uint8_t*>(dst);
  auto s = reinterpret_cast<const uint8_t*>(src);
  for (int i = 0; i < len; ++i) {
    d[i] = s[len - i - 1];
  }
}

/// Converts to big endian format (if not already in big endian) from the
/// machine's native endian format.
#if ARROW_LITTLE_ENDIAN
template <typename T, typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                                 int16_t, uint16_t>>
static inline T ToBigEndian(T value) {
  return ByteSwap(value);
}

template <typename T, typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                                 int16_t, uint16_t>>
static inline T ToLittleEndian(T value) {
  return value;
}
#else
template <typename T, typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                                 int16_t, uint16_t>>
static inline T ToBigEndian(T value) {
  return value;
}
#endif

/// Converts from big endian format to the machine's native endian format.
#if ARROW_LITTLE_ENDIAN
template <typename T, typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                                 int16_t, uint16_t>>
static inline T FromBigEndian(T value) {
  return ByteSwap(value);
}

template <typename T, typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                                 int16_t, uint16_t>>
static inline T FromLittleEndian(T value) {
  return value;
}
#else
template <typename T, typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                                 int16_t, uint16_t>>
static inline T FromBigEndian(T value) {
  return value;
}

template <typename T, typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                                 int16_t, uint16_t>>
static inline T FromLittleEndian(T value) {
  return ByteSwap(value);
}
#endif

// Logical right shift for signed integer types
// This is needed because the C >> operator does arithmetic right shift
// Negative shift amounts lead to undefined behavior
template <typename T>
static T ShiftRightLogical(T v, int shift) {
  // Conversion to unsigned ensures most significant bits always filled with 0's
  return detail::as_unsigned(v) >> shift;
}

void FillBitsFromBytes(const std::vector<uint8_t>& bytes, uint8_t* bits);

/// \brief Convert vector of bytes to bitmap buffer
ARROW_EXPORT
Status BytesToBits(const std::vector<uint8_t>&, MemoryPool*, std::shared_ptr<Buffer>*);

}  // namespace BitUtil

namespace internal {

class BitmapReader {
 public:
  BitmapReader(const uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0), length_(length) {
    current_byte_ = 0;
    byte_offset_ = start_offset / 8;
    bit_offset_ = start_offset % 8;
    if (length > 0) {
      current_byte_ = bitmap[byte_offset_];
    }
  }

#if defined(_MSC_VER)
  // MSVC is finicky about this cast
  bool IsSet() const { return (current_byte_ & (1 << bit_offset_)) != 0; }
#else
  bool IsSet() const { return current_byte_ & (1 << bit_offset_); }
#endif

  bool IsNotSet() const { return (current_byte_ & (1 << bit_offset_)) == 0; }

  void Next() {
    ++bit_offset_;
    ++position_;
    if (bit_offset_ == 8) {
      bit_offset_ = 0;
      ++byte_offset_;
      if (ARROW_PREDICT_TRUE(position_ < length_)) {
        current_byte_ = bitmap_[byte_offset_];
      }
    }
  }

 private:
  const uint8_t* bitmap_;
  int64_t position_;
  int64_t length_;

  uint8_t current_byte_;
  int64_t byte_offset_;
  int64_t bit_offset_;
};

class BitmapWriter {
 public:
  BitmapWriter(uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0), length_(length) {
    current_byte_ = 0;
    byte_offset_ = start_offset / 8;
    bit_offset_ = start_offset % 8;
    if (length > 0) {
      current_byte_ = bitmap[byte_offset_];
    }
  }

  void Set() { current_byte_ |= BitUtil::kBitmask[bit_offset_]; }

  void Clear() { current_byte_ &= BitUtil::kFlippedBitmask[bit_offset_]; }

  void Next() {
    ++bit_offset_;
    ++position_;
    bitmap_[byte_offset_] = current_byte_;
    if (bit_offset_ == 8) {
      bit_offset_ = 0;
      ++byte_offset_;
      if (ARROW_PREDICT_TRUE(position_ < length_)) {
        current_byte_ = bitmap_[byte_offset_];
      }
    }
  }

  void Finish() {
    if (ARROW_PREDICT_TRUE(position_ < length_)) {
      if (bit_offset_ != 0) {
        bitmap_[byte_offset_] = current_byte_;
      }
    }
  }

  int64_t position() const { return position_; }

 private:
  uint8_t* bitmap_;
  int64_t position_;
  int64_t length_;

  uint8_t current_byte_;
  int64_t byte_offset_;
  int64_t bit_offset_;
};

}  // namespace internal

// ----------------------------------------------------------------------
// Bitmap utilities

ARROW_EXPORT
Status GetEmptyBitmap(MemoryPool* pool, int64_t length, std::shared_ptr<Buffer>* result);

/// Copy a bit range of an existing bitmap
///
/// \param[in] pool memory pool to allocate memory from
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
/// \param[out] out the resulting copy
///
/// \return Status message
ARROW_EXPORT
Status CopyBitmap(MemoryPool* pool, const uint8_t* bitmap, int64_t offset, int64_t length,
                  std::shared_ptr<Buffer>* out);

/// Compute the number of 1's in the given data array
///
/// \param[in] data a packed LSB-ordered bitmap as a byte array
/// \param[in] bit_offset a bitwise offset into the bitmap
/// \param[in] length the number of bits to inspect in the bitmap relative to the offset
///
/// \return The number of set (1) bits in the range
ARROW_EXPORT
int64_t CountSetBits(const uint8_t* data, int64_t bit_offset, int64_t length);

ARROW_EXPORT
bool BitmapEquals(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                  int64_t right_offset, int64_t bit_length);

ARROW_EXPORT
Status BitmapAnd(MemoryPool* pool, const uint8_t* left, int64_t left_offset,
                 const uint8_t* right, int64_t right_offset, int64_t length,
                 int64_t out_offset, std::shared_ptr<Buffer>* out_buffer);

}  // namespace arrow

#endif  // ARROW_UTIL_BIT_UTIL_H
