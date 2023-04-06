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

#include <algorithm>

#include "arrow/memory_pool.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_util.h"

namespace arrow {
namespace internal {

struct BitmapWordAlignParams {
  int64_t leading_bits;
  int64_t trailing_bits;
  int64_t trailing_bit_offset;
  const uint8_t* aligned_start;
  int64_t aligned_bits;
  int64_t aligned_words;
};

// Compute parameters for accessing a bitmap using aligned word instructions.
// The returned parameters describe:
// - a leading area of size `leading_bits` before the aligned words
// - a word-aligned area of size `aligned_bits`
// - a trailing area of size `trailing_bits` after the aligned words
template <uint64_t ALIGN_IN_BYTES>
inline BitmapWordAlignParams BitmapWordAlign(const uint8_t* data, int64_t bit_offset,
                                             int64_t length) {
  static_assert(bit_util::IsPowerOf2(ALIGN_IN_BYTES),
                "ALIGN_IN_BYTES should be a positive power of two");
  constexpr uint64_t ALIGN_IN_BITS = ALIGN_IN_BYTES * 8;

  BitmapWordAlignParams p;

  // Compute a "bit address" that we can align up to ALIGN_IN_BITS.
  // We don't care about losing the upper bits since we are only interested in the
  // difference between both addresses.
  const uint64_t bit_addr =
      reinterpret_cast<size_t>(data) * 8 + static_cast<uint64_t>(bit_offset);
  const uint64_t aligned_bit_addr = bit_util::RoundUpToPowerOf2(bit_addr, ALIGN_IN_BITS);

  p.leading_bits = std::min<int64_t>(length, aligned_bit_addr - bit_addr);
  p.aligned_words = (length - p.leading_bits) / ALIGN_IN_BITS;
  p.aligned_bits = p.aligned_words * ALIGN_IN_BITS;
  p.trailing_bits = length - p.leading_bits - p.aligned_bits;
  p.trailing_bit_offset = bit_offset + p.leading_bits + p.aligned_bits;

  p.aligned_start = data + (bit_offset + p.leading_bits) / 8;
  return p;
}
}  // namespace internal

namespace util {

// Functions to check if the provided Arrow object is aligned by the specified alignment
ARROW_EXPORT bool CheckAlignment(const Buffer& buffer, int64_t alignment);
ARROW_EXPORT bool CheckAlignment(const ArrayData& array, int64_t alignment);
ARROW_EXPORT bool CheckAlignment(const Array& array, int64_t alignment);

// Following functions require an additional boolean vector which stores the
// alignment check bits of the constituent objects.
// For example, needs_alignment vector for a ChunkedArray will contain the
// check bits of the constituent Arrays.
// The boolean vector check was introduced to minimize the repetitive checks
// of the constituent objects during the EnsureAlignment function where certain
// objects can be ignored for further checking if we already know that they are
// completely aligned.
ARROW_EXPORT bool CheckAlignment(const ChunkedArray& array, int64_t alignment,
                                 std::vector<bool>* needs_alignment, int offset = 0);
ARROW_EXPORT bool CheckAlignment(const RecordBatch& batch, int64_t alignment,
                                 std::vector<bool>* needs_alignment);
ARROW_EXPORT bool CheckAlignment(const Table& table, int64_t alignment,
                                 std::vector<bool>* needs_alignment);

ARROW_EXPORT Result<std::shared_ptr<Buffer>> EnsureAlignment(
    std::shared_ptr<Buffer> buffer, int64_t alignment, MemoryPool* memory_pool);

ARROW_EXPORT Result<std::shared_ptr<ArrayData>> EnsureAlignment(
    std::shared_ptr<ArrayData> array_data, int64_t alignment, MemoryPool* memory_pool);

ARROW_EXPORT Result<std::shared_ptr<Array>> EnsureAlignment(std::shared_ptr<Array> array,
                                                            int64_t alignment,
                                                            MemoryPool* memory_pool);

ARROW_EXPORT Result<std::shared_ptr<ChunkedArray>> EnsureAlignment(
    std::shared_ptr<ChunkedArray> array, int64_t alignment, MemoryPool* memory_pool);

ARROW_EXPORT Result<std::shared_ptr<RecordBatch>> EnsureAlignment(
    std::shared_ptr<RecordBatch> batch, int64_t alignment, MemoryPool* memory_pool);

ARROW_EXPORT Result<std::shared_ptr<Table>> EnsureAlignment(std::shared_ptr<Table> table,
                                                            int64_t alignment,
                                                            MemoryPool* memory_pool);

}  // namespace util
}  // namespace arrow
