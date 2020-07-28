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

#include "arrow/util/bitmap_ops.h"

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>

#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/util/align_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace internal {

int64_t CountSetBits(const uint8_t* data, int64_t bit_offset, int64_t length) {
  constexpr int64_t pop_len = sizeof(uint64_t) * 8;
  DCHECK_GE(bit_offset, 0);
  int64_t count = 0;

  const auto p = BitmapWordAlign<pop_len / 8>(data, bit_offset, length);
  for (int64_t i = bit_offset; i < bit_offset + p.leading_bits; ++i) {
    if (BitUtil::GetBit(data, i)) {
      ++count;
    }
  }

  if (p.aligned_words > 0) {
    // popcount as much as possible with the widest possible count
    const uint64_t* u64_data = reinterpret_cast<const uint64_t*>(p.aligned_start);
    DCHECK_EQ(reinterpret_cast<size_t>(u64_data) & 7, 0);
    const uint64_t* end = u64_data + p.aligned_words;

    for (auto iter = u64_data; iter < end; ++iter) {
      count += BitUtil::PopCount(*iter);
    }
  }

  // Account for left over bits (in theory we could fall back to smaller
  // versions of popcount but the code complexity is likely not worth it)
  for (int64_t i = p.trailing_bit_offset; i < bit_offset + length; ++i) {
    if (BitUtil::GetBit(data, i)) {
      ++count;
    }
  }

  return count;
}

namespace {

template <typename Word>
class BitmapWordReader {
 public:
  BitmapWordReader(const uint8_t* bitmap, int64_t offset, int64_t length) {
    bitmap_ = bitmap + offset / 8;
    offset_ = offset % 8;
    bitmap_end_ = bitmap_ + BitUtil::BytesForBits(offset_ + length);

    // decrement word count by one as we may touch two adjacent words in one iteration
    nwords_ = length / (sizeof(Word) * 8) - 1;
    if (nwords_ < 0) {
      nwords_ = 0;
    }
    trailing_bits_ = static_cast<int>(length - nwords_ * sizeof(Word) * 8);
    trailing_bytes_ = static_cast<int>(BitUtil::BytesForBits(trailing_bits_));

    if (nwords_ > 0) {
      current_word_ = load<Word>(bitmap_);
    } else if (length > 0) {
      current_byte_ = load<uint8_t>(bitmap_);
    }
  }

  Word NextWord() {
    bitmap_ += sizeof(Word);
    const Word next_word = load<Word>(bitmap_);
    Word word = current_word_;
    if (offset_) {
      // combine two adjacent words into one word
      // |<------ next ----->|<---- current ---->|
      // +-------------+-----+-------------+-----+
      // |     ---     |  A  |      B      | --- |
      // +-------------+-----+-------------+-----+
      //                  |         |       offset
      //                  v         v
      //               +-----+-------------+
      //               |  A  |      B      |
      //               +-----+-------------+
      //               |<------ word ----->|
      word >>= offset_;
      word |= next_word << (sizeof(Word) * 8 - offset_);
    }
    current_word_ = next_word;
    return word;
  }

  uint8_t NextTrailingByte(int& valid_bits) {
    uint8_t byte;
    DCHECK_GT(trailing_bits_, 0);

    if (trailing_bits_ <= 8) {
      // last byte
      valid_bits = trailing_bits_;
      trailing_bits_ = 0;
      byte = 0;
      internal::BitmapReader reader(bitmap_, offset_, valid_bits);
      for (int i = 0; i < valid_bits; ++i) {
        byte >>= 1;
        if (reader.IsSet()) {
          byte |= 0x80;
        }
        reader.Next();
      }
      byte >>= (8 - valid_bits);
    } else {
      ++bitmap_;
      const uint8_t next_byte = load<uint8_t>(bitmap_);
      byte = current_byte_;
      if (offset_) {
        byte >>= offset_;
        byte |= next_byte << (8 - offset_);
      }
      current_byte_ = next_byte;
      trailing_bits_ -= 8;
      valid_bits = 8;
    }
    return byte;
  }

  int64_t words() const { return nwords_; }
  int trailing_bytes() const { return trailing_bytes_; }

 private:
  int64_t offset_;
  const uint8_t* bitmap_;

  const uint8_t* bitmap_end_;
  int64_t nwords_;
  int trailing_bits_;
  int trailing_bytes_;
  union {
    Word current_word_;
    struct {
#if ARROW_LITTLE_ENDIAN == 0
      uint8_t padding_bytes_[sizeof(Word) - 1];
#endif
      uint8_t current_byte_;
    };
  };

  template <typename DType>
  DType load(const uint8_t* bitmap) {
    DCHECK_LE(bitmap + sizeof(DType), bitmap_end_);
    return BitUtil::ToLittleEndian(util::SafeLoadAs<DType>(bitmap));
  }
};

template <typename Word>
class BitmapWordWriter {
 public:
  BitmapWordWriter(uint8_t* bitmap, int64_t offset, int64_t length) {
    bitmap_ = bitmap + offset / 8;
    offset_ = offset % 8;
    bitmap_end_ = bitmap_ + BitUtil::BytesForBits(offset_ + length);
    mask_ = (1U << offset_) - 1;

    if (offset_) {
      if (length >= static_cast<int>(sizeof(Word) * 8)) {
        current_word_ = load<Word>(bitmap_);
      } else if (length > 0) {
        current_byte_ = load<uint8_t>(bitmap_);
      }
    }
  }

  void PutNextWord(Word word) {
    if (offset_) {
      // split one word into two adjacent words, don't touch unused bits
      //               |<------ word ----->|
      //               +-----+-------------+
      //               |  A  |      B      |
      //               +-----+-------------+
      //                  |         |
      //                  v         v       offset
      // +-------------+-----+-------------+-----+
      // |     ---     |  A  |      B      | --- |
      // +-------------+-----+-------------+-----+
      // |<------ next ----->|<---- current ---->|
      word = (word << offset_) | (word >> (sizeof(Word) * 8 - offset_));
      Word next_word = load<Word>(bitmap_ + sizeof(Word));
      current_word_ = (current_word_ & mask_) | (word & ~mask_);
      next_word = (next_word & ~mask_) | (word & mask_);
      store<Word>(bitmap_, current_word_);
      store<Word>(bitmap_ + sizeof(Word), next_word);
      current_word_ = next_word;
    } else {
      store<Word>(bitmap_, word);
    }
    bitmap_ += sizeof(Word);
  }

  void PutNextTrailingByte(uint8_t byte, int valid_bits) {
    if (valid_bits == 8) {
      if (offset_) {
        byte = (byte << offset_) | (byte >> (8 - offset_));
        uint8_t next_byte = load<uint8_t>(bitmap_ + 1);
        current_byte_ = (current_byte_ & mask_) | (byte & ~mask_);
        next_byte = (next_byte & ~mask_) | (byte & mask_);
        store<uint8_t>(bitmap_, current_byte_);
        store<uint8_t>(bitmap_ + 1, next_byte);
        current_byte_ = next_byte;
      } else {
        store<uint8_t>(bitmap_, byte);
      }
      ++bitmap_;
    } else {
      DCHECK_GT(valid_bits, 0);
      DCHECK_LT(valid_bits, 8);
      DCHECK_LE(bitmap_ + BitUtil::BytesForBits(offset_ + valid_bits), bitmap_end_);
      internal::BitmapWriter writer(bitmap_, offset_, valid_bits);
      for (int i = 0; i < valid_bits; ++i) {
        (byte & 0x01) ? writer.Set() : writer.Clear();
        writer.Next();
        byte >>= 1;
      }
      writer.Finish();
    }
  }

 private:
  int64_t offset_;
  uint8_t* bitmap_;

  const uint8_t* bitmap_end_;
  uint64_t mask_;
  union {
    Word current_word_;
    struct {
#if ARROW_LITTLE_ENDIAN == 0
      uint8_t padding_bytes_[sizeof(Word) - 1];
#endif
      uint8_t current_byte_;
    };
  };

  template <typename DType>
  DType load(const uint8_t* bitmap) {
    DCHECK_LE(bitmap + sizeof(DType), bitmap_end_);
    return BitUtil::ToLittleEndian(util::SafeLoadAs<DType>(bitmap));
  }

  template <typename DType>
  void store(uint8_t* bitmap, DType data) {
    DCHECK_LE(bitmap + sizeof(DType), bitmap_end_);
    util::SafeStore(bitmap, BitUtil::FromLittleEndian(data));
  }
};

}  // namespace

enum class TransferMode : bool { Copy, Invert };

template <TransferMode mode>
void TransferBitmap(const uint8_t* data, int64_t offset, int64_t length,
                    int64_t dest_offset, uint8_t* dest) {
  int64_t bit_offset = offset % 8;
  int64_t dest_bit_offset = dest_offset % 8;

  if (bit_offset || dest_bit_offset) {
    auto reader = internal::BitmapWordReader<uint64_t>(data, offset, length);
    auto writer = internal::BitmapWordWriter<uint64_t>(dest, dest_offset, length);

    auto nwords = reader.words();
    while (nwords--) {
      auto word = reader.NextWord();
      writer.PutNextWord(mode == TransferMode::Invert ? ~word : word);
    }
    auto nbytes = reader.trailing_bytes();
    while (nbytes--) {
      int valid_bits;
      auto byte = reader.NextTrailingByte(valid_bits);
      writer.PutNextTrailingByte(mode == TransferMode::Invert ? ~byte : byte, valid_bits);
    }
  } else if (length) {
    int64_t num_bytes = BitUtil::BytesForBits(length);

    // Shift by its byte offset
    data += offset / 8;
    dest += dest_offset / 8;

    // Take care of the trailing bits in the last byte
    // E.g., if trailing_bits = 5, last byte should be
    // - low  3 bits: new bits from last byte of data buffer
    // - high 5 bits: old bits from last byte of dest buffer
    int64_t trailing_bits = num_bytes * 8 - length;
    uint8_t trail_mask = (1U << (8 - trailing_bits)) - 1;
    uint8_t last_data;

    if (mode == TransferMode::Invert) {
      for (int64_t i = 0; i < num_bytes - 1; i++) {
        dest[i] = static_cast<uint8_t>(~(data[i]));
      }
      last_data = ~data[num_bytes - 1];
    } else {
      std::memcpy(dest, data, static_cast<size_t>(num_bytes - 1));
      last_data = data[num_bytes - 1];
    }

    // Set last byte
    dest[num_bytes - 1] &= ~trail_mask;
    dest[num_bytes - 1] |= last_data & trail_mask;
  }
}

template <TransferMode mode>
Result<std::shared_ptr<Buffer>> TransferBitmap(MemoryPool* pool, const uint8_t* data,
                                               int64_t offset, int64_t length) {
  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateEmptyBitmap(length, pool));
  uint8_t* dest = buffer->mutable_data();

  TransferBitmap<mode>(data, offset, length, 0, dest);

  // As we have freshly allocated this bitmap, we should take care of zeroing the
  // remaining bits.
  int64_t num_bytes = BitUtil::BytesForBits(length);
  int64_t bits_to_zero = num_bytes * 8 - length;
  for (int64_t i = length; i < length + bits_to_zero; ++i) {
    // Both branches may copy extra bits - unsetting to match specification.
    BitUtil::ClearBit(dest, i);
  }
  return buffer;
}

void CopyBitmap(const uint8_t* data, int64_t offset, int64_t length, uint8_t* dest,
                int64_t dest_offset) {
  TransferBitmap<TransferMode::Copy>(data, offset, length, dest_offset, dest);
}

void InvertBitmap(const uint8_t* data, int64_t offset, int64_t length, uint8_t* dest,
                  int64_t dest_offset) {
  TransferBitmap<TransferMode::Invert>(data, offset, length, dest_offset, dest);
}

Result<std::shared_ptr<Buffer>> CopyBitmap(MemoryPool* pool, const uint8_t* data,
                                           int64_t offset, int64_t length) {
  return TransferBitmap<TransferMode::Copy>(pool, data, offset, length);
}

Result<std::shared_ptr<Buffer>> InvertBitmap(MemoryPool* pool, const uint8_t* data,
                                             int64_t offset, int64_t length,
                                             std::shared_ptr<Buffer>* out) {
  return TransferBitmap<TransferMode::Invert>(pool, data, offset, length);
}

bool BitmapEquals(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                  int64_t right_offset, int64_t length) {
  if (left_offset % 8 == 0 && right_offset % 8 == 0) {
    // byte aligned, can use memcmp
    bool bytes_equal =
        std::memcmp(left + left_offset / 8, right + right_offset / 8, length / 8) == 0;
    if (!bytes_equal) {
      return false;
    }
    for (int64_t i = (length / 8) * 8; i < length; ++i) {
      if (BitUtil::GetBit(left, left_offset + i) !=
          BitUtil::GetBit(right, right_offset + i)) {
        return false;
      }
    }
    return true;
  }

  // Unaligned slow case
  auto left_reader = internal::BitmapWordReader<uint64_t>(left, left_offset, length);
  auto right_reader = internal::BitmapWordReader<uint64_t>(right, right_offset, length);

  auto nwords = left_reader.words();
  while (nwords--) {
    if (left_reader.NextWord() != right_reader.NextWord()) {
      return false;
    }
  }
  auto nbytes = left_reader.trailing_bytes();
  while (nbytes--) {
    int valid_bits;
    if (left_reader.NextTrailingByte(valid_bits) !=
        right_reader.NextTrailingByte(valid_bits)) {
      return false;
    }
  }
  return true;
}

namespace {

template <template <typename> class BitOp>
void AlignedBitmapOp(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                     int64_t right_offset, uint8_t* out, int64_t out_offset,
                     int64_t length) {
  BitOp<uint8_t> op;
  DCHECK_EQ(left_offset % 8, right_offset % 8);
  DCHECK_EQ(left_offset % 8, out_offset % 8);

  const int64_t nbytes = BitUtil::BytesForBits(length + left_offset % 8);
  left += left_offset / 8;
  right += right_offset / 8;
  out += out_offset / 8;
  for (int64_t i = 0; i < nbytes; ++i) {
    out[i] = op(left[i], right[i]);
  }
}

template <template <typename> class BitOp>
void UnalignedBitmapOp(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                       int64_t right_offset, uint8_t* out, int64_t out_offset,
                       int64_t length) {
  BitOp<uint64_t> op_word;
  BitOp<uint8_t> op_byte;

  auto left_reader = internal::BitmapWordReader<uint64_t>(left, left_offset, length);
  auto right_reader = internal::BitmapWordReader<uint64_t>(right, right_offset, length);
  auto writer = internal::BitmapWordWriter<uint64_t>(out, out_offset, length);

  auto nwords = left_reader.words();
  while (nwords--) {
    writer.PutNextWord(op_word(left_reader.NextWord(), right_reader.NextWord()));
  }
  auto nbytes = left_reader.trailing_bytes();
  while (nbytes--) {
    int left_valid_bits, right_valid_bits;
    uint8_t left_byte = left_reader.NextTrailingByte(left_valid_bits);
    uint8_t right_byte = right_reader.NextTrailingByte(right_valid_bits);
    DCHECK_EQ(left_valid_bits, right_valid_bits);
    writer.PutNextTrailingByte(op_byte(left_byte, right_byte), left_valid_bits);
  }
}

template <template <typename> class BitOp>
void BitmapOp(const uint8_t* left, int64_t left_offset, const uint8_t* right,
              int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* dest) {
  if ((out_offset % 8 == left_offset % 8) && (out_offset % 8 == right_offset % 8)) {
    // Fast case: can use bytewise AND
    AlignedBitmapOp<BitOp>(left, left_offset, right, right_offset, dest, out_offset,
                           length);
  } else {
    // Unaligned
    UnalignedBitmapOp<BitOp>(left, left_offset, right, right_offset, dest, out_offset,
                             length);
  }
}

template <template <typename> class BitOp>
Result<std::shared_ptr<Buffer>> BitmapOp(MemoryPool* pool, const uint8_t* left,
                                         int64_t left_offset, const uint8_t* right,
                                         int64_t right_offset, int64_t length,
                                         int64_t out_offset) {
  const int64_t phys_bits = length + out_offset;
  ARROW_ASSIGN_OR_RAISE(auto out_buffer, AllocateEmptyBitmap(phys_bits, pool));
  BitmapOp<BitOp>(left, left_offset, right, right_offset, length, out_offset,
                  out_buffer->mutable_data());
  return out_buffer;
}

}  // namespace

Result<std::shared_ptr<Buffer>> BitmapAnd(MemoryPool* pool, const uint8_t* left,
                                          int64_t left_offset, const uint8_t* right,
                                          int64_t right_offset, int64_t length,
                                          int64_t out_offset) {
  return BitmapOp<std::bit_and>(pool, left, left_offset, right, right_offset, length,
                                out_offset);
}

void BitmapAnd(const uint8_t* left, int64_t left_offset, const uint8_t* right,
               int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out) {
  BitmapOp<std::bit_and>(left, left_offset, right, right_offset, length, out_offset, out);
}

Result<std::shared_ptr<Buffer>> BitmapOr(MemoryPool* pool, const uint8_t* left,
                                         int64_t left_offset, const uint8_t* right,
                                         int64_t right_offset, int64_t length,
                                         int64_t out_offset) {
  return BitmapOp<std::bit_or>(pool, left, left_offset, right, right_offset, length,
                               out_offset);
}

void BitmapOr(const uint8_t* left, int64_t left_offset, const uint8_t* right,
              int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out) {
  BitmapOp<std::bit_or>(left, left_offset, right, right_offset, length, out_offset, out);
}

Result<std::shared_ptr<Buffer>> BitmapXor(MemoryPool* pool, const uint8_t* left,
                                          int64_t left_offset, const uint8_t* right,
                                          int64_t right_offset, int64_t length,
                                          int64_t out_offset) {
  return BitmapOp<std::bit_xor>(pool, left, left_offset, right, right_offset, length,
                                out_offset);
}

void BitmapXor(const uint8_t* left, int64_t left_offset, const uint8_t* right,
               int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out) {
  BitmapOp<std::bit_xor>(left, left_offset, right, right_offset, length, out_offset, out);
}

}  // namespace internal
}  // namespace arrow
