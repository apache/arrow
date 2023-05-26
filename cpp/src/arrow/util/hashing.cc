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

#include "arrow/util/hashing.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace internal {

namespace {

/// \brief Rotate-right, 64-bit.
///
/// This compiles to a single instruction on CPUs that have rotation instructions.
///
/// \pre n must be in the range [1, 64].
#define ROTR64(v, n) (((v) >> (n)) | ((v) << (64 - (n))))

/// \brief A hash function for bitmaps that can handle offsets and lengths in
/// terms of number of bits. The hash only depends on the bits actually hashed.
///
/// This implementation is based on 64-bit versions of MurmurHash2 by Austin Appleby.
///
/// The key (a bitmap) is read as a sequence of 64-bit words before the trailing
/// bytes. So if the input is 64-bit aligned, all memory accesses are aligned.
///
/// It's the caller's responsibility to ensure that bits_offset + num_bits are
/// readable from the bitmap.
///
/// \param key The pointer to the bitmap.
/// \param seed The seed for the hash function (useful when chaining hash functions).
/// \param bits_offset The offset in bits relative to the start of the bitmap.
/// \param num_bits The number of bits after the offset to be hashed.
uint64_t MurmurHashBitmap64A(const uint8_t* key, uint64_t seed, uint64_t bits_offset,
                             uint64_t num_bits) {
  const uint64_t m = 0xc6a4a7935bd1e995LLU;
  const int r = 47;

  uint64_t h = seed ^ (num_bits * m);

#define HASHING_ROUND(k) \
  (k) *= m;              \
  (k) ^= (k) >> r;       \
  (k) *= m;              \
                         \
  h ^= (k);              \
  h *= m

  // Shift key pointer by as many words as possible.
  key += (bits_offset / 64) * 8;
  // The shift within each word.
  const uint64_t shift = bits_offset % 64;
  const uint64_t readable_bits = shift + num_bits;

  const auto* data = reinterpret_cast<const uint64_t*>(key);
  const auto* end = data + (readable_bits / 64);
  if (data == end) {
    // The bitmap is entirely contained in a single word, but not all bytes of
    // the word are necessarily accessible, so access is performed byte-by-byte.
    // ROTR644 is not necessary and shift-right is safe because readable_bits < 64
    // when data == end.
    if (num_bits > 0) {
      // clang-format off
      switch (uint64_t k0 = 0; (readable_bits + 7) / 8) {
        case 8: k0 |= static_cast<uint64_t>(key[7]) << 56;
        case 7: k0 |= static_cast<uint64_t>(key[6]) << 48;
        case 6: k0 |= static_cast<uint64_t>(key[5]) << 40;
        case 5: k0 |= static_cast<uint64_t>(key[4]) << 32;
        case 4: k0 |= static_cast<uint64_t>(key[3]) << 24;
        case 3: k0 |= static_cast<uint64_t>(key[2]) << 16;
        case 2: k0 |= static_cast<uint64_t>(key[1]) << 8;
        case 1: k0 |= static_cast<uint64_t>(key[0]);
                k0 &= (0x1LLU << readable_bits) - 1;
                k0 >>= shift;
                HASHING_ROUND(k0);
      }
      // clang-format on
    }
  } else {
    const uint64_t lsb_mask = shift ? (0x1LLU << shift) - 1 : -1;
    const uint64_t msb_mask = ~lsb_mask;
    uint64_t k0 = 0;
    if (ARROW_PREDICT_TRUE(shift == 0)) {
      // Fast case: no shift.
      do {
        uint64_t k1 = *data;
        ++data;
        HASHING_ROUND(k1);
      } while (data != end);
    } else {
      // General case: shift.
      k0 = *data & msb_mask;
      ++data;
      while (data != end) {
        uint64_t k1 = k0 | (*data & lsb_mask);
        if (shift > 0) {
          k1 = ROTR64(k1, shift);
        }
        k0 = *data & msb_mask;
        ++data;
        HASHING_ROUND(k1);
      }
    }

    const auto* trailing = reinterpret_cast<const uint8_t*>(data);
    const auto trailing_bits = readable_bits % 64;
    uint64_t k1 = k0;
    // clang-format off
    switch (uint64_t final_blk = 0; (trailing_bits + 7) / 8) {
      case 8: final_blk |= static_cast<uint64_t>(trailing[7]) << 56;
      case 7: final_blk |= static_cast<uint64_t>(trailing[6]) << 48;
      case 6: final_blk |= static_cast<uint64_t>(trailing[5]) << 40;
      case 5: final_blk |= static_cast<uint64_t>(trailing[4]) << 32;
      case 4: final_blk |= static_cast<uint64_t>(trailing[3]) << 24;
      case 3: final_blk |= static_cast<uint64_t>(trailing[2]) << 16;
      case 2: final_blk |= static_cast<uint64_t>(trailing[1]) << 8;
      case 1: final_blk |= static_cast<uint64_t>(trailing[0]);
              final_blk &= (0x1LLU << trailing_bits) - 1;
              // Combine with the last key block (k0).
              k1 |= final_blk & lsb_mask;
              if (shift > 0) {
                k1 = ROTR64(k1, shift);
              }
              k0 = final_blk & msb_mask;
              HASHING_ROUND(k1);
    }
    // clang-format on
    // Perform a final round with bits from k0 if it still contains relevant bits.
    if (shift > 0 && (trailing_bits == 0 || trailing_bits > shift)) {
      k1 = ROTR64(k0, shift);
      HASHING_ROUND(k1);
    }
  }

  // Finalize.
  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}

}  // namespace

hash_t ComputeBitmapHash(const uint8_t* bitmap, int64_t length, hash_t seed,
                         int64_t bits_offset, int64_t num_bits) {
  DCHECK_GE(bits_offset, 0);
  DCHECK_GE(num_bits, 0);
  DCHECK_LE((bits_offset + num_bits + 7) / 8, length);
  return MurmurHashBitmap64A(bitmap, seed, bits_offset, num_bits);
}

}  // namespace internal
}  // namespace arrow
