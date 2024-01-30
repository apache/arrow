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
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace internal {

namespace {

/// \brief A hash function for bitmaps that can handle offsets and lengths in
/// terms of number of bits. The hash only depends on the bits actually hashed.
///
/// This implementation is based on 64-bit versions of MurmurHash2 by Austin Appleby.
///
/// It's the caller's responsibility to ensure that bits_offset + num_bits are
/// readable from the bitmap.
///
/// \param key The pointer to the bitmap.
/// \param seed The seed for the hash function (useful when chaining hash functions).
/// \param bits_offset The offset in bits relative to the start of the bitmap.
/// \param num_bits The number of bits after the offset to be hashed.
uint64_t MurmurHashBitmap64(const uint8_t* key, uint64_t seed, uint64_t bits_offset,
                            uint64_t num_bits) {
  const uint64_t m = 0xc6a4a7935bd1e995LLU;
  const int r = 47;

  uint64_t h = seed ^ (num_bits * m);

  BitmapWordReader<uint64_t> reader(key, bits_offset, num_bits);
  auto nwords = reader.words();
  while (nwords--) {
    auto k = reader.NextWord();
    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }
  int valid_bits;
  auto nbytes = reader.trailing_bytes();
  if (nbytes) {
    uint64_t k = 0;
    do {
      auto byte = reader.NextTrailingByte(valid_bits);
      k = (k << 8) | static_cast<uint64_t>(byte);
    } while (--nbytes);
    h ^= k;
    h *= m;
  }

  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}

}  // namespace

hash_t ComputeBitmapHash(const uint8_t* bitmap, hash_t seed, int64_t bits_offset,
                         int64_t num_bits) {
  DCHECK_GE(bits_offset, 0);
  DCHECK_GE(num_bits, 0);
  return MurmurHashBitmap64(bitmap, seed, bits_offset, num_bits);
}

}  // namespace internal
}  // namespace arrow
