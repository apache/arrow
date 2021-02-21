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

#include <cstdint>
#include <cstring>

#include "arrow/buffer.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/macros.h"

namespace arrow {
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

  bool IsSet() const { return (current_byte_ & (1 << bit_offset_)) != 0; }

  bool IsNotSet() const { return (current_byte_ & (1 << bit_offset_)) == 0; }

  void Next() {
    ++bit_offset_;
    ++position_;
    if (ARROW_PREDICT_FALSE(bit_offset_ == 8)) {
      bit_offset_ = 0;
      ++byte_offset_;
      if (ARROW_PREDICT_TRUE(position_ < length_)) {
        current_byte_ = bitmap_[byte_offset_];
      }
    }
  }

  int64_t position() const { return position_; }

  int64_t length() const { return length_; }

 private:
  const uint8_t* bitmap_;
  int64_t position_;
  int64_t length_;

  uint8_t current_byte_;
  int64_t byte_offset_;
  int64_t bit_offset_;
};

// XXX Cannot name it BitmapWordReader because the name is already used
// in bitmap_ops.cc

class BitmapUInt64Reader {
 public:
  BitmapUInt64Reader(const uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap + start_offset / 8),
        num_carry_bits_(8 - start_offset % 8),
        length_(length),
        remaining_length_(length_) {
    if (length_ > 0) {
      // Load carry bits from the first byte's MSBs
      if (length_ >= num_carry_bits_) {
        carry_bits_ =
            LoadPartialWord(static_cast<int8_t>(8 - num_carry_bits_), num_carry_bits_);
      } else {
        carry_bits_ = LoadPartialWord(static_cast<int8_t>(8 - num_carry_bits_), length_);
      }
    }
  }

  uint64_t NextWord() {
    if (ARROW_PREDICT_TRUE(remaining_length_ >= 64 + num_carry_bits_)) {
      // We can load a full word
      uint64_t next_word = LoadFullWord();
      // Carry bits come first, then the (64 - num_carry_bits_) LSBs from next_word
      uint64_t word = carry_bits_ | (next_word << num_carry_bits_);
      carry_bits_ = next_word >> (64 - num_carry_bits_);
      remaining_length_ -= 64;
      return word;
    } else if (remaining_length_ > num_carry_bits_) {
      // We can load a partial word
      uint64_t next_word =
          LoadPartialWord(/*bit_offset=*/0, remaining_length_ - num_carry_bits_);
      uint64_t word = carry_bits_ | (next_word << num_carry_bits_);
      carry_bits_ = next_word >> (64 - num_carry_bits_);
      remaining_length_ = std::max<int64_t>(remaining_length_ - 64, 0);
      return word;
    } else {
      remaining_length_ = 0;
      return carry_bits_;
    }
  }

  int64_t position() const { return length_ - remaining_length_; }

  int64_t length() const { return length_; }

 private:
  uint64_t LoadFullWord() {
    uint64_t word;
    memcpy(&word, bitmap_, 8);
    bitmap_ += 8;
    return BitUtil::ToLittleEndian(word);
  }

  uint64_t LoadPartialWord(int8_t bit_offset, int64_t num_bits) {
    uint64_t word = 0;
    const int64_t num_bytes = BitUtil::BytesForBits(num_bits);
    memcpy(&word, bitmap_, num_bytes);
    bitmap_ += num_bytes;
    return (BitUtil::ToLittleEndian(word) >> bit_offset) &
           BitUtil::LeastSignificantBitMask(num_bits);
  }

  const uint8_t* bitmap_;
  const int64_t num_carry_bits_;  // in [1, 8]
  const int64_t length_;
  int64_t remaining_length_;
  uint64_t carry_bits_;
};

/// \brief Index into a possibly non-existent bitmap
struct OptionalBitIndexer {
  const uint8_t* bitmap;
  const int64_t offset;

  explicit OptionalBitIndexer(const std::shared_ptr<Buffer>& buffer, int64_t offset = 0)
      : bitmap(buffer == NULLPTR ? NULLPTR : buffer->data()), offset(offset) {}

  bool operator[](int64_t i) const {
    return bitmap == NULLPTR ? true : BitUtil::GetBit(bitmap, offset + i);
  }
};

}  // namespace internal
}  // namespace arrow
