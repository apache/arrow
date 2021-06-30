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

    constexpr int64_t kCountUnrollFactor = 4;
    const int64_t words_rounded = BitUtil::RoundDown(p.aligned_words, kCountUnrollFactor);
    int64_t count_unroll[kCountUnrollFactor] = {0};

    // Unroll the loop for better performance
    for (int64_t i = 0; i < words_rounded; i += kCountUnrollFactor) {
      for (int64_t k = 0; k < kCountUnrollFactor; k++) {
        count_unroll[k] += BitUtil::PopCount(u64_data[k]);
      }
      u64_data += kCountUnrollFactor;
    }
    for (int64_t k = 0; k < kCountUnrollFactor; k++) {
      count += count_unroll[k];
    }

    // The trailing part
    for (; u64_data < end; ++u64_data) {
      count += BitUtil::PopCount(*u64_data);
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
                                             int64_t offset, int64_t length) {
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

bool OptionalBitmapEquals(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                          int64_t right_offset, int64_t length) {
  if (left == nullptr && right == nullptr) {
    return true;
  } else if (left != nullptr && right != nullptr) {
    return BitmapEquals(left, left_offset, right, right_offset, length);
  } else if (left != nullptr) {
    return CountSetBits(left, left_offset, length) == length;
  } else {
    return CountSetBits(right, right_offset, length) == length;
  }
}

bool OptionalBitmapEquals(const std::shared_ptr<Buffer>& left, int64_t left_offset,
                          const std::shared_ptr<Buffer>& right, int64_t right_offset,
                          int64_t length) {
  return OptionalBitmapEquals(left ? left->data() : nullptr, left_offset,
                              right ? right->data() : nullptr, right_offset, length);
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

template <typename T>
struct AndNotOp {
  constexpr T operator()(const T& l, const T& r) const { return l & ~r; }
};

Result<std::shared_ptr<Buffer>> BitmapAndNot(MemoryPool* pool, const uint8_t* left,
                                             int64_t left_offset, const uint8_t* right,
                                             int64_t right_offset, int64_t length,
                                             int64_t out_offset) {
  return BitmapOp<AndNotOp>(pool, left, left_offset, right, right_offset, length,
                            out_offset);
}

void BitmapAndNot(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                  int64_t right_offset, int64_t length, int64_t out_offset,
                  uint8_t* out) {
  BitmapOp<AndNotOp>(left, left_offset, right, right_offset, length, out_offset, out);
}

template <typename T>
struct OrNotOp {
  constexpr T operator()(const T& l, const T& r) const { return l | ~r; }
};

Result<std::shared_ptr<Buffer>> BitmapOrNot(MemoryPool* pool, const uint8_t* left,
                                            int64_t left_offset, const uint8_t* right,
                                            int64_t right_offset, int64_t length,
                                            int64_t out_offset) {
  return BitmapOp<OrNotOp>(pool, left, left_offset, right, right_offset, length,
                           out_offset);
}

void BitmapOrNot(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                 int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out) {
  BitmapOp<OrNotOp>(left, left_offset, right, right_offset, length, out_offset, out);
}

}  // namespace internal
}  // namespace arrow
