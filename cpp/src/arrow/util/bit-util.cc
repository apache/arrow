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

// Alias MSVC popcount to GCC name
#ifdef _MSC_VER
#include <intrin.h>
#define __builtin_popcount __popcnt
#include <nmmintrin.h>
#define __builtin_popcountll _mm_popcnt_u64
#endif

#include <algorithm>
#include <cstring>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/bit-util.h"

namespace arrow {

void BitUtil::BytesToBits(const std::vector<uint8_t>& bytes, uint8_t* bits) {
  for (size_t i = 0; i < bytes.size(); ++i) {
    if (bytes[i] > 0) { SetBit(bits, i); }
  }
}

Status BitUtil::BytesToBits(
    const std::vector<uint8_t>& bytes, std::shared_ptr<Buffer>* out) {
  int64_t bit_length = BitUtil::BytesForBits(bytes.size());

  std::shared_ptr<MutableBuffer> buffer;
  RETURN_NOT_OK(AllocateBuffer(default_memory_pool(), bit_length, &buffer));

  memset(buffer->mutable_data(), 0, bit_length);
  BytesToBits(bytes, buffer->mutable_data());

  *out = buffer;
  return Status::OK();
}

int64_t CountSetBits(const uint8_t* data, int64_t bit_offset, int64_t length) {
  constexpr int64_t pop_len = sizeof(uint64_t) * 8;

  int64_t count = 0;

  // The first bit offset where we can use a 64-bit wide hardware popcount
  const int64_t fast_count_start = BitUtil::RoundUp(bit_offset, pop_len);

  // The number of bits until fast_count_start
  const int64_t initial_bits = std::min(length, fast_count_start - bit_offset);
  for (int64_t i = bit_offset; i < bit_offset + initial_bits; ++i) {
    if (BitUtil::GetBit(data, i)) { ++count; }
  }

  const int64_t fast_counts = (length - initial_bits) / pop_len;

  // Advance until the first aligned 8-byte word after the initial bits
  const uint64_t* u64_data =
      reinterpret_cast<const uint64_t*>(data) + fast_count_start / pop_len;

  const uint64_t* end = u64_data + fast_counts;

  // popcount as much as possible with the widest possible count
  for (auto iter = u64_data; iter < end; ++iter) {
    count += __builtin_popcountll(*iter);
  }

  // Account for left over bit (in theory we could fall back to smaller
  // versions of popcount but the code complexity is likely not worth it)
  const int64_t tail_index = bit_offset + initial_bits + fast_counts * pop_len;
  for (int64_t i = tail_index; i < bit_offset + length; ++i) {
    if (BitUtil::GetBit(data, i)) { ++count; }
  }

  return count;
}

Status GetEmptyBitmap(
    MemoryPool* pool, int64_t length, std::shared_ptr<MutableBuffer>* result) {
  RETURN_NOT_OK(AllocateBuffer(pool, BitUtil::BytesForBits(length), result));
  memset((*result)->mutable_data(), 0, (*result)->size());
  return Status::OK();
}

Status CopyBitmap(MemoryPool* pool, const uint8_t* data, int64_t offset, int64_t length,
    std::shared_ptr<Buffer>* out) {
  std::shared_ptr<MutableBuffer> buffer;
  RETURN_NOT_OK(GetEmptyBitmap(pool, length, &buffer));
  uint8_t* dest = buffer->mutable_data();
  for (int64_t i = 0; i < length; ++i) {
    BitUtil::SetBitTo(dest, i, BitUtil::GetBit(data, i + offset));
  }
  *out = buffer;
  return Status::OK();
}

bool BitmapEquals(const uint8_t* left, int64_t left_offset, const uint8_t* right,
    int64_t right_offset, int64_t bit_length) {
  // TODO(wesm): Make this faster using word-wise comparisons
  for (int64_t i = 0; i < bit_length; ++i) {
    if (BitUtil::GetBit(left, left_offset + i) !=
        BitUtil::GetBit(right, right_offset + i)) {
      return false;
    }
  }
  return true;
}

}  // namespace arrow
