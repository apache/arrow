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

// From Apache Impala as of 2016-01-29

#ifndef PARQUET_UTIL_BIT_STREAM_UTILS_H
#define PARQUET_UTIL_BIT_STREAM_UTILS_H

#include <string.h>
#include <algorithm>
#include <cstdint>

#include "parquet/util/compiler-util.h"
#include "parquet/util/logging.h"
#include "parquet/util/bit-util.h"

namespace parquet_cpp {

/// Utility class to write bit/byte streams.  This class can write data to either be
/// bit packed or byte aligned (and a single stream that has a mix of both).
/// This class does not allocate memory.
class BitWriter {
 public:
  /// buffer: buffer to write bits to.  Buffer should be preallocated with
  /// 'buffer_len' bytes.
  BitWriter(uint8_t* buffer, int buffer_len) :
      buffer_(buffer),
      max_bytes_(buffer_len) {
    Clear();
  }

  void Clear() {
    buffered_values_ = 0;
    byte_offset_ = 0;
    bit_offset_ = 0;
  }

  /// The number of current bytes written, including the current byte (i.e. may include a
  /// fraction of a byte). Includes buffered values.
  int bytes_written() const { return byte_offset_ + BitUtil::Ceil(bit_offset_, 8); }
  uint8_t* buffer() const { return buffer_; }
  int buffer_len() const { return max_bytes_; }

  /// Writes a value to buffered_values_, flushing to buffer_ if necessary.  This is bit
  /// packed.  Returns false if there was not enough space. num_bits must be <= 32.
  bool PutValue(uint64_t v, int num_bits);

  /// Writes v to the next aligned byte using num_bytes. If T is larger than
  /// num_bytes, the extra high-order bytes will be ignored. Returns false if
  /// there was not enough space.
  template<typename T>
  bool PutAligned(T v, int num_bytes);

  /// Write a Vlq encoded int to the buffer.  Returns false if there was not enough
  /// room.  The value is written byte aligned.
  /// For more details on vlq:
  /// en.wikipedia.org/wiki/Variable-length_quantity
  bool PutVlqInt(uint32_t v);

  // Writes an int zigzag encoded.
  bool PutZigZagVlqInt(int32_t v);

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

  int byte_offset_;       // Offset in buffer_
  int bit_offset_;        // Offset in buffered_values_
};

/// Utility class to read bit/byte stream.  This class can read bits or bytes
/// that are either byte aligned or not.  It also has utilities to read multiple
/// bytes in one read (e.g. encoded int).
class BitReader {
 public:
  /// 'buffer' is the buffer to read from.  The buffer's length is 'buffer_len'.
  BitReader(const uint8_t* buffer, int buffer_len) :
      buffer_(buffer),
      max_bytes_(buffer_len),
      byte_offset_(0),
      bit_offset_(0) {
    int num_bytes = std::min(8, max_bytes_ - byte_offset_);
    memcpy(&buffered_values_, buffer_ + byte_offset_, num_bytes);
  }

  BitReader() : buffer_(NULL), max_bytes_(0) {}

  void Reset(const uint8_t* buffer, int buffer_len) {
    buffer_ = buffer;
    max_bytes_ = buffer_len;
    byte_offset_ = 0;
    bit_offset_ = 0;
    int num_bytes = std::min(8, max_bytes_ - byte_offset_);
    memcpy(&buffered_values_, buffer_ + byte_offset_, num_bytes);
  }

  /// Gets the next value from the buffer.  Returns true if 'v' could be read or false if
  /// there are not enough bytes left. num_bits must be <= 32.
  template<typename T>
  bool GetValue(int num_bits, T* v);

  /// Reads a 'num_bytes'-sized value from the buffer and stores it in 'v'. T
  /// needs to be a little-endian native type and big enough to store
  /// 'num_bytes'. The value is assumed to be byte-aligned so the stream will
  /// be advanced to the start of the next byte before 'v' is read. Returns
  /// false if there are not enough bytes left.
  template<typename T>
  bool GetAligned(int num_bytes, T* v);

  /// Reads a vlq encoded int from the stream.  The encoded int must start at
  /// the beginning of a byte. Return false if there were not enough bytes in
  /// the buffer.
  bool GetVlqInt(int32_t* v);

  // Reads a zigzag encoded int `into` v.
  bool GetZigZagVlqInt(int32_t* v);

  /// Returns the number of bytes left in the stream, not including the current
  /// byte (i.e., there may be an additional fraction of a byte).
  int bytes_left() { return max_bytes_ - (byte_offset_ + BitUtil::Ceil(bit_offset_, 8)); }

  /// Maximum byte length of a vlq encoded int
  static const int MAX_VLQ_BYTE_LEN = 5;

 private:
  const uint8_t* buffer_;
  int max_bytes_;

  /// Bytes are memcpy'd from buffer_ and values are read from this variable. This is
  /// faster than reading values byte by byte directly from buffer_.
  uint64_t buffered_values_;

  int byte_offset_;       // Offset in buffer_
  int bit_offset_;        // Offset in buffered_values_
};

} // namespace parquet_cpp

#endif // PARQUET_UTIL_BIT_STREAM_UTILS_H
