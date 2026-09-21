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

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/util/align_util.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/logging_internal.h"

namespace arrow::internal {

int64_t CountSetBits(const uint8_t* data, int64_t bit_offset, int64_t length) {
  constexpr int64_t pop_len = sizeof(uint64_t) * 8;
  DCHECK_GE(bit_offset, 0);
  int64_t count = 0;

  const auto p = BitmapWordAlign<pop_len / 8>(data, bit_offset, length);
  for (int64_t i = bit_offset; i < bit_offset + p.leading_bits; ++i) {
    if (bit_util::GetBit(data, i)) {
      ++count;
    }
  }

  if (p.aligned_words > 0) {
    // popcount as much as possible with the widest possible count
    const uint64_t* u64_data = reinterpret_cast<const uint64_t*>(p.aligned_start);
    DCHECK_EQ(reinterpret_cast<size_t>(u64_data) & 7, 0);
    const uint64_t* end = u64_data + p.aligned_words;

    constexpr int64_t kCountUnrollFactor = 4;
    const int64_t words_rounded =
        bit_util::RoundDown(p.aligned_words, kCountUnrollFactor);
    std::array<int64_t, kCountUnrollFactor> count_unroll{};

    // Unroll the loop for better performance
    for (int64_t i = 0; i < words_rounded; i += kCountUnrollFactor) {
      // (hand-unrolled as some gcc versions would unnest a nested `for` loop)
      count_unroll[0] += std::popcount(u64_data[0]);
      count_unroll[1] += std::popcount(u64_data[1]);
      count_unroll[2] += std::popcount(u64_data[2]);
      count_unroll[3] += std::popcount(u64_data[3]);
      u64_data += kCountUnrollFactor;
    }
    for (int64_t k = 0; k < kCountUnrollFactor; k++) {
      count += count_unroll[k];
    }

    // The trailing part
    for (; u64_data < end; ++u64_data) {
      count += std::popcount(*u64_data);
    }
  }

  // Account for left over bits (in theory we could fall back to smaller
  // versions of popcount but the code complexity is likely not worth it)
  for (int64_t i = p.trailing_bit_offset; i < bit_offset + length; ++i) {
    if (bit_util::GetBit(data, i)) {
      ++count;
    }
  }

  return count;
}

int64_t CountAndSetBits(const uint8_t* left_bitmap, int64_t left_offset,
                        const uint8_t* right_bitmap, int64_t right_offset,
                        int64_t length) {
  BinaryBitBlockCounter bit_counter(left_bitmap, left_offset, right_bitmap, right_offset,
                                    length);
  int64_t count = 0;
  while (true) {
    BitBlockCount block = bit_counter.NextAndWord();
    if (block.length == 0) {
      break;
    }
    count += block.popcount;
  }
  return count;
}

namespace {

// Equivalent to std::identity, which is not available in the libc++ shipped with
// older macOS SDKs (< 12) that we still need to support.
struct Identity {
  template <typename T>
  constexpr T&& operator()(T&& value) const noexcept {
    return std::forward<T>(value);
  }
};

// Reverse all bits from entire byte(uint8)
uint8_t ReverseUint8(uint8_t num) {
  num = ((num & 0xf0) >> 4) | ((num & 0x0f) << 4);
  num = ((num & 0xcc) >> 2) | ((num & 0x33) << 2);
  num = ((num & 0xaa) >> 1) | ((num & 0x55) << 1);
  return num;
}

// Get a reverse block of byte(uint8) using offsets, the result can be
// part of a left block and right block, length indicates the number of bits
// to be taken from the right block
uint8_t GetReversedBlock(uint8_t block_left, uint8_t block_right, uint8_t length) {
  return ReverseUint8(((block_right << 8) + block_left) >> length);
}

/// Map output from readers and save it with the writer.
///
/// All readers and writer must span over the same number of values.
///
/// @tparam Op a function of as many input as there are readers.
template <typename Op>
void MapReadersWriter(auto&& writer, auto&& reader, auto&&... readers) {
  constexpr auto kReaderCount = sizeof...(readers) + 1;
  constexpr auto op = Op{};

  // Need a real function so that the fold expression remains valid in release
  [[maybe_unused]] const auto check_eq = [](auto a, auto b) { ARROW_DCHECK_EQ(a, b); };

  auto nwords = reader.words();
  ((check_eq(readers.words(), nwords)), ...);
  while (nwords--) {
    writer.PutNextWord(op(reader.NextWord(), readers.NextWord()...));
  }

  auto nbytes = reader.trailing_bytes();
  ((check_eq(readers.trailing_bytes(), nbytes)), ...);
  while (nbytes--) {
    int valid_bits = 0;
    std::array<uint8_t, kReaderCount> bytes = {};
    {
      auto b = bytes.begin();
      *b++ = reader.NextTrailingByte(valid_bits);
      [[maybe_unused]] auto read = [&](auto& r) {
        int vb = 0;
        *b++ = r.NextTrailingByte(vb);
        check_eq(vb, valid_bits);
      };
      (read(readers), ...);
    }
    writer.PutNextTrailingByte(std::apply(op, bytes), valid_bits);
  }
}

template <typename Byte = uint8_t>
struct BitmapPtr {
  Byte* data;
  int64_t offset;

  BitmapPtr operator+(int64_t extra) { return {.data = data, .offset = offset + extra}; }
};

using BitmapConstPtr = BitmapPtr<const uint8_t>;
using BitmapMutPtr = BitmapPtr<uint8_t>;

/// Map inputs with a given operation and sace to output.
///
/// This function assumes general non bit-aligned input and outputs.
/// It will first process less than a byte in order to bit-align the writer, and then
/// keep on going with an aligned writer.
/// Aligning the writer is what delivers significant speedup.
///
/// @tparam Op a function of as many input as there are readers.
template <typename Op, typename Word = uint64_t>
void FastMapReadersWriter(BitmapMutPtr out, int64_t length, auto&&... in) {
  const int64_t out_bit_offset = out.offset % 8;

  if (length == 0) {
    return;
  } else if (out_bit_offset) {
    using Reader = internal::BitmapWordReader<uint8_t>;
    using Writer = internal::BitmapWordWriter<uint8_t>;

    const auto count = std::min(8 - out_bit_offset, length);
    auto writer = Writer(out.data, out.offset, count);
    MapReadersWriter<Op>(writer, Reader(in.data, in.offset, count)...);
    FastMapReadersWriter<Op>(out + count, length - count, in + count...);
  } else {
    using Reader = internal::BitmapWordReader<Word>;
    using Writer = internal::BitmapWordWriter<Word, false>;

    auto writer = Writer(out.data, out.offset, length);
    MapReadersWriter<Op>(writer, Reader(in.data, in.offset, length)...);
  }
}

template <typename Op>
void MapBitmapUnary(const uint8_t* data, int64_t offset, int64_t length,
                    int64_t dest_offset, uint8_t* dest) {
  const int64_t bit_offset = offset % 8;
  const int64_t dest_bit_offset = dest_offset % 8;

  if (bit_offset || dest_bit_offset) {
    FastMapReadersWriter<Op>({.data = dest, .offset = dest_offset}, length,
                             BitmapConstPtr{.data = data, .offset = offset});
  } else if (length > 0) {
    const int64_t num_bytes = bit_util::BytesForBits(length);

    // Shift by its byte offset
    data += offset / 8;
    dest += dest_offset / 8;

    // Take care of the trailing bits in the last byte
    // E.g., if trailing_bits = 5, last byte should be
    // - low  3 bits: new bits from last byte of data buffer
    // - high 5 bits: old bits from last byte of dest buffer
    const int64_t trailing_bits = num_bytes * 8 - length;
    const uint8_t trail_mask = (1U << (8 - trailing_bits)) - 1;
    uint8_t last_data;

    if constexpr (std::is_same_v<std::decay_t<Op>, Identity>) {
      std::memcpy(dest, data, static_cast<size_t>(num_bytes - 1));
      last_data = data[num_bytes - 1];
    } else {
      constexpr auto op = Op{};
      for (int64_t i = 0; i < num_bytes - 1; i++) {
        dest[i] = static_cast<uint8_t>(op(data[i]));
      }
      last_data = op(data[num_bytes - 1]);
    }

    // Set last byte
    dest[num_bytes - 1] &= ~trail_mask;
    dest[num_bytes - 1] |= last_data & trail_mask;
  }
}

void ReverseBlockOffsets(const uint8_t* data, int64_t offset, int64_t length,
                         int64_t dest_offset, uint8_t* dest) {
  int64_t num_bytes = bit_util::BytesForBits(offset % 8 + length);
  // Shift by its byte offset
  data += offset / 8;
  dest += dest_offset / 8;

  int64_t j_src = num_bytes - 1;
  int64_t i_dest = 0;

  while (length > 0) {
    uint8_t right_trailing_bits_src = (length + offset) % 8;
    right_trailing_bits_src = !right_trailing_bits_src ? 8 : right_trailing_bits_src;

    uint8_t left_trailing_bits_dest = 8 - dest_offset % 8;
    uint8_t left_trailing_mask_dest = 0xFF << (8 - left_trailing_bits_dest);
    if (length <= 8 && (dest_offset % 8) + length < 8) {
      uint8_t extra_bits = static_cast<uint8_t>(8 - ((dest_offset % 8) + length));
      left_trailing_mask_dest <<= extra_bits;
      left_trailing_mask_dest >>= extra_bits;
    }

    uint8_t right_reversed_block;
    if (j_src == 0) {
      right_reversed_block = static_cast<uint8_t>(
          GetReversedBlock(data[0], data[0], right_trailing_bits_src));
    } else {
      right_reversed_block = static_cast<uint8_t>(
          GetReversedBlock(data[j_src - 1], data[j_src], right_trailing_bits_src));
    }

    dest[i_dest] &= ~left_trailing_mask_dest;
    dest[i_dest] |=
        (right_reversed_block << (8 - left_trailing_bits_dest)) & left_trailing_mask_dest;

    dest_offset += left_trailing_bits_dest;
    length -= left_trailing_bits_dest;

    if (left_trailing_bits_dest >= right_trailing_bits_src) j_src--;
    i_dest++;
  }
}

template <typename Op>
Result<std::shared_ptr<Buffer>> MapBitmapUnary(MemoryPool* pool, const uint8_t* data,
                                               int64_t offset, int64_t length,
                                               int64_t out_offset) {
  const int64_t phys_bits = length + out_offset;
  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateEmptyBitmap(phys_bits, pool));
  uint8_t* dest = buffer->mutable_data();

  MapBitmapUnary<Op>(data, offset, length, out_offset, dest);

  return buffer;
}

}  // namespace

void CopyBitmap(const uint8_t* data, int64_t offset, int64_t length, uint8_t* dest,
                int64_t dest_offset) {
  MapBitmapUnary<Identity>(data, offset, length, dest_offset, dest);
}

void InvertBitmap(const uint8_t* data, int64_t offset, int64_t length, uint8_t* dest,
                  int64_t dest_offset) {
  MapBitmapUnary<std::bit_not<>>(data, offset, length, dest_offset, dest);
}

void ReverseBitmap(const uint8_t* data, int64_t offset, int64_t length, uint8_t* dest,
                   int64_t dest_offset) {
  ReverseBlockOffsets(data, offset, length, dest_offset, dest);
}

Result<std::shared_ptr<Buffer>> CopyBitmap(MemoryPool* pool, const uint8_t* data,
                                           int64_t offset, int64_t length,
                                           int64_t out_offset) {
  return MapBitmapUnary<Identity>(pool, data, offset, length, out_offset);
}

Result<std::shared_ptr<Buffer>> InvertBitmap(MemoryPool* pool, const uint8_t* data,
                                             int64_t offset, int64_t length) {
  return MapBitmapUnary<std::bit_not<>>(pool, data, offset, length, /*out_offset=*/0);
}

Result<std::shared_ptr<Buffer>> ReverseBitmap(MemoryPool* pool, const uint8_t* data,
                                              int64_t offset, int64_t length) {
  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateEmptyBitmap(length, pool));
  uint8_t* dest = buffer->mutable_data();

  ReverseBlockOffsets(data, offset, length, /*start_offset=*/0, dest);

  return buffer;
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
      if (bit_util::GetBit(left, left_offset + i) !=
          bit_util::GetBit(right, right_offset + i)) {
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

Result<std::shared_ptr<Buffer>> OptionalBitmapAnd(MemoryPool* pool,
                                                  const std::shared_ptr<Buffer>& left,
                                                  int64_t left_offset,
                                                  const std::shared_ptr<Buffer>& right,
                                                  int64_t right_offset, int64_t length,
                                                  int64_t out_offset) {
  if (left == nullptr && right == nullptr) {
    return nullptr;
  }
  if (left == nullptr) {
    if (right_offset >= out_offset && (right_offset - out_offset) % 8 == 0) {
      int64_t byte_shift = (right_offset - out_offset) / 8;
      int64_t byte_length = bit_util::BytesForBits(out_offset + length);
      return SliceBuffer(right, byte_shift, byte_length);
    }
    return CopyBitmap(pool, right->data(), right_offset, length, out_offset);
  }
  if (right == nullptr) {
    if (left_offset >= out_offset && (left_offset - out_offset) % 8 == 0) {
      int64_t byte_shift = (left_offset - out_offset) / 8;
      int64_t byte_length = bit_util::BytesForBits(out_offset + length);
      return SliceBuffer(left, byte_shift, byte_length);
    }
    return CopyBitmap(pool, left->data(), left_offset, length, out_offset);
  }

  return BitmapAnd(pool, left->data(), left_offset, right->data(), right_offset, length,
                   out_offset);
}

namespace {

template <typename Op>
void AlignedBitmapOp(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                     int64_t right_offset, uint8_t* out, int64_t out_offset,
                     int64_t length) {
  constexpr auto op = Op{};
  DCHECK_EQ(left_offset % 8, right_offset % 8);
  DCHECK_EQ(left_offset % 8, out_offset % 8);

  const int64_t nbytes = bit_util::BytesForBits(length + left_offset % 8);
  left += left_offset / 8;
  right += right_offset / 8;
  out += out_offset / 8;
  for (int64_t i = 0; i < nbytes; ++i) {
    out[i] = op(left[i], right[i]);
  }
}

template <typename Op>
void UnalignedBitmapOp(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                       int64_t right_offset, uint8_t* out, int64_t out_offset,
                       int64_t length) {
  FastMapReadersWriter<Op>({.data = out, .offset = out_offset}, length,
                           BitmapConstPtr{.data = left, .offset = left_offset},
                           BitmapConstPtr{.data = right, .offset = right_offset});
}

// XXX: The bits before left/right/out_offset, if unaligned, are untouched. But not for
// the bits after length. Caller should ensure proper alignment for the tail bits if
// necessary, or correct the tail bits by subsequent calls.
template <typename Op>
void BitmapOp(const uint8_t* left, int64_t left_offset, const uint8_t* right,
              int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* dest) {
  if (out_offset % 8 == left_offset % 8 && out_offset % 8 == right_offset % 8) {
    // Fast case: can use byte-wise BitOp after handling leading unaligned bits.
    int64_t leading_unaligned_bits = (8 - left_offset % 8) % 8;
    if (leading_unaligned_bits > 0) {
      UnalignedBitmapOp<Op>(left, left_offset, right, right_offset, dest, out_offset,
                            leading_unaligned_bits);
    }
    if (length > leading_unaligned_bits) {
      AlignedBitmapOp<Op>(left, left_offset + leading_unaligned_bits, right,
                          right_offset + leading_unaligned_bits, dest,
                          out_offset + leading_unaligned_bits,
                          length - leading_unaligned_bits);
    }
  } else {
    // Unaligned
    UnalignedBitmapOp<Op>(left, left_offset, right, right_offset, dest, out_offset,
                          length);
  }
}

template <typename Op>
Result<std::shared_ptr<Buffer>> BitmapOp(MemoryPool* pool, const uint8_t* left,
                                         int64_t left_offset, const uint8_t* right,
                                         int64_t right_offset, int64_t length,
                                         int64_t out_offset) {
  const int64_t phys_bits = length + out_offset;
  ARROW_ASSIGN_OR_RAISE(auto out_buffer, AllocateEmptyBitmap(phys_bits, pool));
  BitmapOp<Op>(left, left_offset, right, right_offset, length, out_offset,
               out_buffer->mutable_data());
  return out_buffer;
}

}  // namespace

Result<std::shared_ptr<Buffer>> BitmapAnd(MemoryPool* pool, const uint8_t* left,
                                          int64_t left_offset, const uint8_t* right,
                                          int64_t right_offset, int64_t length,
                                          int64_t out_offset) {
  return BitmapOp<std::bit_and<>>(pool, left, left_offset, right, right_offset, length,
                                  out_offset);
}

void BitmapAnd(const uint8_t* left, int64_t left_offset, const uint8_t* right,
               int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out) {
  BitmapOp<std::bit_and<>>(left, left_offset, right, right_offset, length, out_offset,
                           out);
}

Result<std::shared_ptr<Buffer>> BitmapOr(MemoryPool* pool, const uint8_t* left,
                                         int64_t left_offset, const uint8_t* right,
                                         int64_t right_offset, int64_t length,
                                         int64_t out_offset) {
  return BitmapOp<std::bit_or<>>(pool, left, left_offset, right, right_offset, length,
                                 out_offset);
}

void BitmapOr(const uint8_t* left, int64_t left_offset, const uint8_t* right,
              int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out) {
  BitmapOp<std::bit_or<>>(left, left_offset, right, right_offset, length, out_offset,
                          out);
}

Result<std::shared_ptr<Buffer>> BitmapXor(MemoryPool* pool, const uint8_t* left,
                                          int64_t left_offset, const uint8_t* right,
                                          int64_t right_offset, int64_t length,
                                          int64_t out_offset) {
  return BitmapOp<std::bit_xor<>>(pool, left, left_offset, right, right_offset, length,
                                  out_offset);
}

void BitmapXor(const uint8_t* left, int64_t left_offset, const uint8_t* right,
               int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out) {
  BitmapOp<std::bit_xor<>>(left, left_offset, right, right_offset, length, out_offset,
                           out);
}

struct AndNotOp {
  template <typename T, typename U>
  constexpr auto operator()(const T& l, const U& r) const {
    return l & ~r;
  }
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

struct OrNotOp {
  template <typename T, typename U>
  constexpr auto operator()(const T& l, const U& r) const {
    return l | ~r;
  }
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

}  // namespace arrow::internal
