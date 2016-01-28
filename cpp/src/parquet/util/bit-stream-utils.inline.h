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

#ifndef PARQUET_UTIL_BIT_STREAM_UTILS_INLINE_H
#define PARQUET_UTIL_BIT_STREAM_UTILS_INLINE_H

#include "parquet/util/bit-stream-utils.h"

namespace parquet_cpp {

inline bool BitWriter::PutValue(uint64_t v, int num_bits) {
  // TODO: revisit this limit if necessary (can be raised to 64 by fixing some edge cases)
  DCHECK_LE(num_bits, 32);
  DCHECK_EQ(v >> num_bits, 0) << "v = " << v << ", num_bits = " << num_bits;

  if (UNLIKELY(byte_offset_ * 8 + bit_offset_ + num_bits > max_bytes_ * 8)) return false;

  buffered_values_ |= v << bit_offset_;
  bit_offset_ += num_bits;

  if (UNLIKELY(bit_offset_ >= 64)) {
    // Flush buffered_values_ and write out bits of v that did not fit
    memcpy(buffer_ + byte_offset_, &buffered_values_, 8);
    buffered_values_ = 0;
    byte_offset_ += 8;
    bit_offset_ -= 64;
    buffered_values_ = v >> (num_bits - bit_offset_);
  }
  DCHECK_LT(bit_offset_, 64);
  return true;
}

inline void BitWriter::Flush(bool align) {
  int num_bytes = BitUtil::Ceil(bit_offset_, 8);
  DCHECK_LE(byte_offset_ + num_bytes, max_bytes_);
  memcpy(buffer_ + byte_offset_, &buffered_values_, num_bytes);

  if (align) {
    buffered_values_ = 0;
    byte_offset_ += num_bytes;
    bit_offset_ = 0;
  }
}

inline uint8_t* BitWriter::GetNextBytePtr(int num_bytes) {
  Flush(/* align */ true);
  DCHECK_LE(byte_offset_, max_bytes_);
  if (byte_offset_ + num_bytes > max_bytes_) return NULL;
  uint8_t* ptr = buffer_ + byte_offset_;
  byte_offset_ += num_bytes;
  return ptr;
}

template<typename T>
inline bool BitWriter::PutAligned(T val, int num_bytes) {
  uint8_t* ptr = GetNextBytePtr(num_bytes);
  if (ptr == NULL) return false;
  memcpy(ptr, &val, num_bytes);
  return true;
}

inline bool BitWriter::PutVlqInt(uint32_t v) {
  bool result = true;
  while ((v & 0xFFFFFF80) != 0L) {
    result &= PutAligned<uint8_t>((v & 0x7F) | 0x80, 1);
    v >>= 7;
  }
  result &= PutAligned<uint8_t>(v & 0x7F, 1);
  return result;
}

inline bool BitWriter::PutZigZagVlqInt(int32_t v) {
  uint32_t u = (v << 1) ^ (v >> 31);
  return PutVlqInt(u);
}

template<typename T>
inline bool BitReader::GetValue(int num_bits, T* v) {
  // TODO: revisit this limit if necessary
  DCHECK_LE(num_bits, 32);
  DCHECK_LE(num_bits, sizeof(T) * 8);

  if (UNLIKELY(byte_offset_ * 8 + bit_offset_ + num_bits > max_bytes_ * 8)) return false;

  *v = BitUtil::TrailingBits(buffered_values_, bit_offset_ + num_bits) >> bit_offset_;

  bit_offset_ += num_bits;
  if (bit_offset_ >= 64) {
    byte_offset_ += 8;
    bit_offset_ -= 64;

    int bytes_remaining = max_bytes_ - byte_offset_;
    if (LIKELY(bytes_remaining >= 8)) {
      memcpy(&buffered_values_, buffer_ + byte_offset_, 8);
    } else {
      memcpy(&buffered_values_, buffer_ + byte_offset_, bytes_remaining);
    }

    // Read bits of v that crossed into new buffered_values_
    *v |= BitUtil::TrailingBits(buffered_values_, bit_offset_)
          << (num_bits - bit_offset_);
  }
  DCHECK_LE(bit_offset_, 64);
  return true;
}

template<typename T>
inline bool BitReader::GetAligned(int num_bytes, T* v) {
  DCHECK_LE(num_bytes, sizeof(T));
  int bytes_read = BitUtil::Ceil(bit_offset_, 8);
  if (UNLIKELY(byte_offset_ + bytes_read + num_bytes > max_bytes_)) return false;

  // Advance byte_offset to next unread byte and read num_bytes
  byte_offset_ += bytes_read;
  memcpy(v, buffer_ + byte_offset_, num_bytes);
  byte_offset_ += num_bytes;

  // Reset buffered_values_
  bit_offset_ = 0;
  int bytes_remaining = max_bytes_ - byte_offset_;
  if (LIKELY(bytes_remaining >= 8)) {
    memcpy(&buffered_values_, buffer_ + byte_offset_, 8);
  } else {
    memcpy(&buffered_values_, buffer_ + byte_offset_, bytes_remaining);
  }
  return true;
}

inline bool BitReader::GetVlqInt(uint64_t* v) {
  *v = 0;
  int shift = 0;
  int num_bytes = 0;
  uint8_t byte = 0;
  do {
    if (!GetAligned<uint8_t>(1, &byte)) return false;
    *v |= (byte & 0x7F) << shift;
    shift += 7;
    DCHECK_LE(++num_bytes, MAX_VLQ_BYTE_LEN);
  } while ((byte & 0x80) != 0);
  return true;
}

inline bool BitReader::GetZigZagVlqInt(int64_t* v) {
  uint64_t u;
  if (!GetVlqInt(&u)) return false;
  *reinterpret_cast<uint64_t*>(v) = (u >> 1) ^ -(u & 1);
  return true;
}

} // namespace parquet_cpp

#endif // PARQUET_UTIL_BIT_STREAM_UTILS_INLINE_H
