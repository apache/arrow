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

// From Apache Impala (incubating) as of 2016-01-29

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "arrow/util/bit_util.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/ubsan.h"

namespace arrow::bit_util {

/// Utility class to write bit/byte streams.  This class can write data to either be
/// bit packed or byte aligned (and a single stream that has a mix of both).
/// This class does not allocate memory.
class BitWriter {
 public:
  /// buffer: buffer to write bits to.  Buffer should be preallocated with
  /// 'buffer_len' bytes.
  BitWriter(uint8_t* buffer, int buffer_len) : buffer_(buffer), max_bytes_(buffer_len) {
    Clear();
  }

  void Clear() {
    buffered_values_ = 0;
    byte_offset_ = 0;
    bit_offset_ = 0;
  }

  /// The number of current bytes written, including the current byte (i.e. may include a
  /// fraction of a byte). Includes buffered values.
  int bytes_written() const {
    return byte_offset_ + static_cast<int>(bit_util::BytesForBits(bit_offset_));
  }
  uint8_t* buffer() const { return buffer_; }
  int buffer_len() const { return max_bytes_; }

  /// Writes a value to buffered_values_, flushing to buffer_ if necessary.  This is bit
  /// packed.  Returns false if there was not enough space. num_bits must be <= 32.
  bool PutValue(uint64_t v, int num_bits);

  /// Writes v to the next aligned byte using num_bytes. If T is larger than
  /// num_bytes, the extra high-order bytes will be ignored. Returns false if
  /// there was not enough space.
  /// Assume the v is stored in buffer_ as a little-endian format
  template <typename T>
  bool PutAligned(T v, int num_bytes);

  /// Write a Vlq encoded int to the buffer.  Returns false if there was not enough
  /// room.  The value is written byte aligned.
  /// For more details on vlq:
  /// en.wikipedia.org/wiki/Variable-length_quantity
  template <typename Int>
  bool PutVlqInt(Int v);

  /// Writes a zigzag encoded signed integer.
  /// Zigzag encoding is used to encode possibly negative numbers by alternating positive
  /// and negative ones.
  template <typename Int>
  bool PutZigZagVlqInt(Int v);

  /// Get a pointer to the next aligned byte and advance the underlying buffer
  /// by num_bytes.
  /// Returns NULL if there was not enough space.
  uint8_t* GetNextBytePtr(int num_bytes = 1);

  /// Flushes all buffered values to the buffer. Call this when done writing to
  /// the buffer.  If 'align' is true, buffered_values_ is reset and any future
  /// writes will be written to the next byte boundary.
  void Flush(bool align = false);

 private:
  uint8_t* buffer_;
  int max_bytes_;

  /// Bit-packed values are initially written to this variable before being memcpy'd to
  /// buffer_. This is faster than writing values byte by byte directly to buffer_.
  uint64_t buffered_values_;

  int byte_offset_;  // Offset in buffer_
  int bit_offset_;   // Offset in buffered_values_
};

namespace detail {

inline uint64_t ReadLittleEndianWord(const uint8_t* buffer, int bytes_remaining) {
  uint64_t le_value = 0;
  if (ARROW_PREDICT_TRUE(bytes_remaining >= 8)) {
    memcpy(&le_value, buffer, 8);
  } else {
    memcpy(&le_value, buffer, bytes_remaining);
  }
  return arrow::bit_util::FromLittleEndian(le_value);
}

}  // namespace detail

/// Utility class to read bit/byte stream.  This class can read bits or bytes
/// that are either byte aligned or not.  It also has utilities to read multiple
/// bytes in one read (e.g. encoded int).
class BitReader {
 public:
  BitReader() noexcept = default;

  /// 'buffer' is the buffer to read from.  The buffer's length is 'buffer_len'.
  BitReader(const uint8_t* buffer, int buffer_len) : BitReader() {
    Reset(buffer, buffer_len);
  }

  void Reset(const uint8_t* buffer, int buffer_len) noexcept {
    buffer_ = buffer;
    max_bytes_ = buffer_len;
    byte_offset_ = 0;
    bit_offset_ = 0;
    buffered_values_ =
        detail::ReadLittleEndianWord(buffer_ + byte_offset_, max_bytes_ - byte_offset_);
  }

  /// Gets the next value from the buffer.  Returns true if 'v' could be read or false if
  /// there are not enough bytes left.
  template <typename T>
  bool GetValue(int num_bits, T* v);

  /// Get a number of values from the buffer. Return the number of values actually read.
  template <typename T>
  int GetBatch(int num_bits, T* v, int batch_size);

  /// Reads a 'num_bytes'-sized value from the buffer and stores it in 'v'. T
  /// needs to be a little-endian native type and big enough to store
  /// 'num_bytes'. The value is assumed to be byte-aligned so the stream will
  /// be advanced to the start of the next byte before 'v' is read. Returns
  /// false if there are not enough bytes left.
  /// Assume the v was stored in buffer_ as a little-endian format
  template <typename T>
  bool GetAligned(int num_bytes, T* v);

  /// Advances the stream by a number of bits. Returns true if succeed or false if there
  /// are not enough bits left.
  bool Advance(int64_t num_bits);

  /// Reads a vlq encoded int from the stream.  The encoded int must start at
  /// the beginning of a byte. Return false if there were not enough bytes in
  /// the buffer.
  template <typename Int>
  bool GetVlqInt(Int* v);

  /// Reads a zigzag encoded integer into a signed integer output v.
  /// Zigzag encoding is used to decode possibly negative numbers by alternating positive
  /// and negative ones.
  template <typename Int>
  bool GetZigZagVlqInt(Int* v);

  /// Returns the number of bytes left in the stream, not including the current
  /// byte (i.e., there may be an additional fraction of a byte).
  int bytes_left() const {
    return max_bytes_ -
           (byte_offset_ + static_cast<int>(bit_util::BytesForBits(bit_offset_)));
  }

 private:
  const uint8_t* buffer_;
  int max_bytes_;

  /// Bytes are memcpy'd from buffer_ and values are read from this variable. This is
  /// faster than reading values byte by byte directly from buffer_.
  uint64_t buffered_values_;

  int byte_offset_;  // Offset in buffer_
  int bit_offset_;   // Offset in buffered_values_
};

inline bool BitWriter::PutValue(uint64_t v, int num_bits) {
  ARROW_DCHECK_LE(num_bits, 64);
  if (num_bits < 64) {
    ARROW_DCHECK_EQ(v >> num_bits, 0) << "v = " << v << ", num_bits = " << num_bits;
  }

  if (ARROW_PREDICT_FALSE(byte_offset_ * 8 + bit_offset_ + num_bits > max_bytes_ * 8))
    return false;

  buffered_values_ |= v << bit_offset_;
  bit_offset_ += num_bits;

  if (ARROW_PREDICT_FALSE(bit_offset_ >= 64)) {
    // Flush buffered_values_ and write out bits of v that did not fit
    buffered_values_ = arrow::bit_util::ToLittleEndian(buffered_values_);
    memcpy(buffer_ + byte_offset_, &buffered_values_, 8);
    buffered_values_ = 0;
    byte_offset_ += 8;
    bit_offset_ -= 64;
    buffered_values_ =
        (num_bits - bit_offset_ == 64) ? 0 : (v >> (num_bits - bit_offset_));
  }
  ARROW_DCHECK_LT(bit_offset_, 64);
  return true;
}

inline void BitWriter::Flush(bool align) {
  int num_bytes = static_cast<int>(bit_util::BytesForBits(bit_offset_));
  ARROW_DCHECK_LE(byte_offset_ + num_bytes, max_bytes_);
  auto buffered_values = arrow::bit_util::ToLittleEndian(buffered_values_);
  memcpy(buffer_ + byte_offset_, &buffered_values, num_bytes);

  if (align) {
    buffered_values_ = 0;
    byte_offset_ += num_bytes;
    bit_offset_ = 0;
  }
}

inline uint8_t* BitWriter::GetNextBytePtr(int num_bytes) {
  Flush(/* align */ true);
  ARROW_DCHECK_LE(byte_offset_, max_bytes_);
  if (byte_offset_ + num_bytes > max_bytes_) return NULL;
  uint8_t* ptr = buffer_ + byte_offset_;
  byte_offset_ += num_bytes;
  return ptr;
}

template <typename T>
inline bool BitWriter::PutAligned(T val, int num_bytes) {
  uint8_t* ptr = GetNextBytePtr(num_bytes);
  if (ptr == NULL) return false;
  val = arrow::bit_util::ToLittleEndian(val);
  memcpy(ptr, &val, num_bytes);
  return true;
}

namespace detail {

template <typename T>
inline void GetValue_(int num_bits, T* v, int max_bytes, const uint8_t* buffer,
                      int* bit_offset, int* byte_offset, uint64_t* buffered_values) {
#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4800)
#endif
  *v = static_cast<T>(bit_util::TrailingBits(*buffered_values, *bit_offset + num_bits) >>
                      *bit_offset);
#ifdef _MSC_VER
#  pragma warning(pop)
#endif
  *bit_offset += num_bits;
  if (*bit_offset >= 64) {
    *byte_offset += 8;
    *bit_offset -= 64;

    *buffered_values =
        detail::ReadLittleEndianWord(buffer + *byte_offset, max_bytes - *byte_offset);
#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4800 4805)
#endif
    // Read bits of v that crossed into new buffered_values_
    if (ARROW_PREDICT_TRUE(num_bits - *bit_offset < static_cast<int>(8 * sizeof(T)))) {
      // if shift exponent(num_bits - *bit_offset) is not less than sizeof(T), *v will not
      // change and the following code may cause a runtime error that the shift exponent
      // is too large
      *v = *v | static_cast<T>(bit_util::TrailingBits(*buffered_values, *bit_offset)
                               << (num_bits - *bit_offset));
    }
#ifdef _MSC_VER
#  pragma warning(pop)
#endif
    ARROW_DCHECK_LE(*bit_offset, 64);
  }
}

}  // namespace detail

template <typename T>
inline bool BitReader::GetValue(int num_bits, T* v) {
  return GetBatch(num_bits, v, 1) == 1;
}

namespace internal_bit_reader {
template <typename T>
struct unpack_detect {
  using type = std::make_unsigned_t<T>;
};

template <>
struct unpack_detect<bool> {
  using type = bool;
};
}  // namespace internal_bit_reader

template <typename T>
inline int BitReader::GetBatch(int num_bits, T* v, int batch_size) {
  ARROW_DCHECK(buffer_ != NULL);
  ARROW_DCHECK_LE(num_bits, static_cast<int>(sizeof(T) * 8)) << "num_bits: " << num_bits;

  int bit_offset = bit_offset_;
  int byte_offset = byte_offset_;
  uint64_t buffered_values = buffered_values_;
  int max_bytes = max_bytes_;
  const uint8_t* buffer = buffer_;

  const int64_t needed_bits = num_bits * static_cast<int64_t>(batch_size);
  constexpr uint64_t kBitsPerByte = 8;
  const int64_t remaining_bits =
      static_cast<int64_t>(max_bytes - byte_offset) * kBitsPerByte - bit_offset;
  if (remaining_bits < needed_bits) {
    batch_size = static_cast<int>(remaining_bits / num_bits);
  }

  int i = 0;
  if (ARROW_PREDICT_FALSE(bit_offset != 0)) {
    for (; i < batch_size && bit_offset != 0; ++i) {
      detail::GetValue_(num_bits, &v[i], max_bytes, buffer, &bit_offset, &byte_offset,
                        &buffered_values);
    }
  }

  using unpack_t = typename internal_bit_reader::unpack_detect<T>::type;

  int num_unpacked = ::arrow::internal::unpack(
      buffer + byte_offset, reinterpret_cast<unpack_t*>(v + i), batch_size - i, num_bits);
  i += num_unpacked;
  byte_offset += num_unpacked * num_bits / 8;

  buffered_values =
      detail::ReadLittleEndianWord(buffer + byte_offset, max_bytes - byte_offset);

  for (; i < batch_size; ++i) {
    detail::GetValue_(num_bits, &v[i], max_bytes, buffer, &bit_offset, &byte_offset,
                      &buffered_values);
  }

  bit_offset_ = bit_offset;
  byte_offset_ = byte_offset;
  buffered_values_ = buffered_values;

  return batch_size;
}

template <typename T>
inline bool BitReader::GetAligned(int num_bytes, T* v) {
  if (ARROW_PREDICT_FALSE(num_bytes > static_cast<int>(sizeof(T)))) {
    return false;
  }

  int bytes_read = static_cast<int>(bit_util::BytesForBits(bit_offset_));
  if (ARROW_PREDICT_FALSE(byte_offset_ + bytes_read + num_bytes > max_bytes_)) {
    return false;
  }

  // Advance byte_offset to next unread byte and read num_bytes
  byte_offset_ += bytes_read;
  if constexpr (std::is_same_v<T, bool>) {
    // ARROW-18031: if we're trying to get an aligned bool, just check
    // the LSB of the next byte and move on. If we memcpy + FromLittleEndian
    // as usual, we have potential undefined behavior for bools if the value
    // isn't 0 or 1
    *v = *(buffer_ + byte_offset_) & 1;
  } else {
    memcpy(v, buffer_ + byte_offset_, num_bytes);
    *v = arrow::bit_util::FromLittleEndian(*v);
  }
  byte_offset_ += num_bytes;

  bit_offset_ = 0;
  buffered_values_ =
      detail::ReadLittleEndianWord(buffer_ + byte_offset_, max_bytes_ - byte_offset_);
  return true;
}

inline bool BitReader::Advance(int64_t num_bits) {
  int64_t bits_required = bit_offset_ + num_bits;
  int64_t bytes_required = bit_util::BytesForBits(bits_required);
  if (ARROW_PREDICT_FALSE(bytes_required > max_bytes_ - byte_offset_)) {
    return false;
  }
  byte_offset_ += static_cast<int>(bits_required >> 3);
  bit_offset_ = static_cast<int>(bits_required & 7);
  buffered_values_ =
      detail::ReadLittleEndianWord(buffer_ + byte_offset_, max_bytes_ - byte_offset_);
  return true;
}

template <typename Int>
inline bool BitWriter::PutVlqInt(Int v) {
  static_assert(std::is_integral_v<Int>);

  constexpr auto kBufferSize = kMaxLEB128ByteLenFor<Int>;

  uint8_t buffer[kBufferSize] = {};
  const auto bytes_written = WriteLEB128(v, buffer, kBufferSize);
  ARROW_DCHECK_LE(bytes_written, kBufferSize);
  if constexpr (std::is_signed_v<Int>) {
    // Can fail if negative
    if (ARROW_PREDICT_FALSE(!bytes_written == 0)) {
      return false;
    }
  } else {
    // Cannot fail since we gave max space
    ARROW_DCHECK_GT(bytes_written, 0);
  }

  for (int i = 0; i < bytes_written; ++i) {
    const bool success = PutAligned(buffer[i], 1);
    if (ARROW_PREDICT_FALSE(!success)) {
      return false;
    }
  }

  return true;
}

template <typename Int>
inline bool BitReader::GetVlqInt(Int* v) {
  static_assert(std::is_integral_v<Int>);

  // The data that we will pass to the LEB128 parser
  // In all case, we read a byte-aligned value, skipping remaining bits
  const uint8_t* data = NULLPTR;
  int max_size = 0;

  // Number of bytes left in the buffered values, not including the current
  // byte (i.e., there may be an additional fraction of a byte).
  const int bytes_left_in_cache =
      sizeof(buffered_values_) - static_cast<int>(bit_util::BytesForBits(bit_offset_));

  // If there are clearly enough bytes left we can try to parse from the cache
  if (bytes_left_in_cache >= kMaxLEB128ByteLenFor<Int>) {
    max_size = bytes_left_in_cache;
    data = reinterpret_cast<const uint8_t*>(&buffered_values_) +
           bit_util::BytesForBits(bit_offset_);
    // Otherwise, we try straight from buffer (ignoring few bytes that may be cached)
  } else {
    max_size = bytes_left();
    data = buffer_ + (max_bytes_ - max_size);
  }

  const auto bytes_read = bit_util::ParseLeadingLEB128(data, max_size, v);
  if (ARROW_PREDICT_FALSE(bytes_read == 0)) {
    // Corrupt LEB128
    return false;
  }

  // Advance for the bytes we have read + the bits we skipped
  return Advance((8 * bytes_read) + (bit_offset_ % 8));
}

template <typename Int>
inline bool BitWriter::PutZigZagVlqInt(Int v) {
  static_assert(std::is_integral_v<Int>);
  static_assert(std::is_signed_v<Int>);
  using UInt = std::make_unsigned_t<Int>;
  constexpr auto kBitSize = 8 * sizeof(Int);

  UInt u_v = ::arrow::util::SafeCopy<UInt>(v);
  u_v = (u_v << 1) ^ static_cast<UInt>(v >> (kBitSize - 1));
  return PutVlqInt(u_v);
}

template <typename Int>
inline bool BitReader::GetZigZagVlqInt(Int* v) {
  static_assert(std::is_integral_v<Int>);
  static_assert(std::is_signed_v<Int>);

  std::make_unsigned_t<Int> u;
  if (!GetVlqInt(&u)) return false;
  u = (u >> 1) ^ (~(u & 1) + 1);
  *v = ::arrow::util::SafeCopy<Int>(u);
  return true;
}

}  // namespace arrow::bit_util
