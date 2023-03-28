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

#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/align_util.h"

namespace arrow {
namespace internal {

template <int64_t NBYTES>
void CheckBitmapWordAlign(const uint8_t* data, int64_t bit_offset, int64_t length,
                          BitmapWordAlignParams expected) {
  auto p = BitmapWordAlign<static_cast<uint64_t>(NBYTES)>(data, bit_offset, length);

  ASSERT_EQ(p.leading_bits, expected.leading_bits);
  ASSERT_EQ(p.trailing_bits, expected.trailing_bits);
  if (p.trailing_bits > 0) {
    // Only relevant if trailing_bits > 0
    ASSERT_EQ(p.trailing_bit_offset, expected.trailing_bit_offset);
  }
  ASSERT_EQ(p.aligned_bits, expected.aligned_bits);
  ASSERT_EQ(p.aligned_words, expected.aligned_words);
  if (p.aligned_bits > 0) {
    // Only relevant if aligned_bits > 0
    ASSERT_EQ(p.aligned_start, expected.aligned_start);
  }

  // Invariants
  ASSERT_LT(p.leading_bits, NBYTES * 8);
  ASSERT_LT(p.trailing_bits, NBYTES * 8);
  ASSERT_EQ(p.leading_bits + p.aligned_bits + p.trailing_bits, length);
  ASSERT_EQ(p.aligned_bits, NBYTES * 8 * p.aligned_words);
  if (p.aligned_bits > 0) {
    ASSERT_EQ(reinterpret_cast<size_t>(p.aligned_start) & (NBYTES - 1), 0);
  }
  if (p.trailing_bits > 0) {
    ASSERT_EQ(p.trailing_bit_offset, bit_offset + p.leading_bits + p.aligned_bits);
    ASSERT_EQ(p.trailing_bit_offset + p.trailing_bits, bit_offset + length);
  }
}

arrow::Result<std::shared_ptr<Array>> CreateUnalignedArray(const Array& array) {
  // Slicing by 1 would create an invalid array if the type was wider than
  // 1 byte so double-check that the array is a 1-byte type
  EXPECT_EQ(array.type_id(), Type::UINT8);
  BufferVector sliced_buffers(array.data()->buffers.size(), nullptr);
  for (std::size_t i = 0; i < array.data()->buffers.size(); ++i) {
    if (array.data()->buffers[i]) {
      sliced_buffers[i] = SliceBuffer(array.data()->buffers[i], 1, 2);
    }
  }
  auto sliced_array_data =
      ArrayData::Make(array.type(), /*length=*/2, std::move(sliced_buffers));
  return MakeArray(std::move(sliced_array_data));
}

TEST(BitmapWordAlign, AlignedDataStart) {
  alignas(8) char buf[136] = {0};

  // A 8-byte aligned pointer
  const uint8_t* P = reinterpret_cast<const uint8_t*>(buf);
  const uint8_t* A = P;

  // {leading_bits, trailing_bits, trailing_bit_offset,
  //  aligned_start, aligned_bits, aligned_words}
  CheckBitmapWordAlign<8>(P, 0, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 13, {0, 13, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 63, {0, 63, 0, A, 0, 0});

  CheckBitmapWordAlign<8>(P, 0, 64, {0, 0, 0, A, 64, 1});
  CheckBitmapWordAlign<8>(P, 0, 73, {0, 9, 64, A, 64, 1});
  CheckBitmapWordAlign<8>(P, 0, 191, {0, 63, 128, A, 128, 2});

  CheckBitmapWordAlign<8>(P, 5, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 13, {13, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 59, {59, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 60, {59, 1, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 64, {59, 5, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 122, {59, 63, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 123, {59, 0, 64, A + 8, 64, 1});
  CheckBitmapWordAlign<8>(P, 5, 314, {59, 63, 256, A + 8, 192, 3});
  CheckBitmapWordAlign<8>(P, 5, 315, {59, 0, 320, A + 8, 256, 4});

  CheckBitmapWordAlign<8>(P, 63, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 63, 1, {1, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 63, 2, {1, 1, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 63, 64, {1, 63, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 63, 65, {1, 0, 128, A + 8, 64, 1});
  CheckBitmapWordAlign<8>(P, 63, 128, {1, 63, 128, A + 8, 64, 1});
  CheckBitmapWordAlign<8>(P, 63, 129, {1, 0, 192, A + 8, 128, 2});

  CheckBitmapWordAlign<8>(P, 1024, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1024, 130, {0, 2, 1152, A + 128, 128, 2});

  CheckBitmapWordAlign<8>(P, 1025, 1, {1, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1025, 63, {63, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1025, 64, {63, 1, 1088, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1025, 128, {63, 1, 1152, A + 136, 64, 1});
}

TEST(BitmapWordAlign, UnalignedDataStart) {
  alignas(8) char buf[136] = {0};

  const uint8_t* P = reinterpret_cast<const uint8_t*>(buf) + 1;
  const uint8_t* A = P + 7;

  // {leading_bits, trailing_bits, trailing_bit_offset,
  //  aligned_start, aligned_bits, aligned_words}
  CheckBitmapWordAlign<8>(P, 0, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 13, {13, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 56, {56, 0, 0, A, 0, 0});

  CheckBitmapWordAlign<8>(P, 0, 57, {56, 1, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 119, {56, 63, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 120, {56, 0, 120, A, 64, 1});
  CheckBitmapWordAlign<8>(P, 0, 184, {56, 0, 184, A, 128, 2});
  CheckBitmapWordAlign<8>(P, 0, 185, {56, 1, 184, A, 128, 2});

  CheckBitmapWordAlign<8>(P, 55, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 55, 1, {1, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 55, 2, {1, 1, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 55, 66, {1, 1, 120, A, 64, 1});

  // (P + 56 bits) is 64-bit aligned
  CheckBitmapWordAlign<8>(P, 56, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 56, 1, {0, 1, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 56, 63, {0, 63, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 56, 191, {0, 63, 184, A, 128, 2});

  // (P + 1016 bits) is 64-bit aligned
  CheckBitmapWordAlign<8>(P, 1016, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1016, 5, {0, 5, 1016, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1016, 63, {0, 63, 1016, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1016, 64, {0, 0, 1080, A + 120, 64, 1});
  CheckBitmapWordAlign<8>(P, 1016, 129, {0, 1, 1144, A + 120, 128, 2});

  CheckBitmapWordAlign<8>(P, 1017, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1017, 1, {1, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1017, 63, {63, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1017, 64, {63, 1, 1080, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1017, 128, {63, 1, 1144, A + 128, 64, 1});
}
}  // namespace internal

TEST(EnsureAlignment, Array) {
  MemoryPool* pool = default_memory_pool();
  auto rand = ::arrow::random::RandomArrayGenerator(1923);
  auto random_array = rand.UInt8(/*size*/ 50, /*min*/ 0, /*max*/ 100,
                                 /*null_probability*/ 0, /*alignment*/ 512, pool);

  // for having buffers which are not aligned by 2048
  ASSERT_OK_AND_ASSIGN(auto sliced_array,
                       arrow::internal::CreateUnalignedArray(*random_array));

  ASSERT_EQ(util::CheckAlignment(*sliced_array, 2048), false);
  ASSERT_OK_AND_ASSIGN(auto aligned_array,
                       util::EnsureAlignment(std::move(sliced_array), 2048, pool));
  ASSERT_EQ(util::CheckAlignment(*aligned_array, 2048), true);
}

TEST(EnsureAlignment, ChunkedArray) {
  MemoryPool* pool = default_memory_pool();
  auto rand = ::arrow::random::RandomArrayGenerator(1923);
  auto random_array_1 = rand.UInt8(/*size*/ 50, /*min*/ 0, /*max*/ 100,
                                   /*null_probability*/ 0, /*alignment*/ 512, pool);
  auto random_array_2 = rand.UInt8(/*size*/ 100, /*min*/ 10, /*max*/ 50,
                                   /*null_probability*/ 0, /*alignment*/ 1024, pool);

  ASSERT_OK_AND_ASSIGN(auto sliced_array_1,
                       arrow::internal::CreateUnalignedArray(*random_array_1));
  ASSERT_OK_AND_ASSIGN(auto sliced_array_2,
                       arrow::internal::CreateUnalignedArray(*random_array_2));

  ASSERT_OK_AND_ASSIGN(
      auto chunked_array,
      ChunkedArray::Make({sliced_array_1, sliced_array_2}, sliced_array_1->type()));

  std::vector<bool> needs_alignment;
  ASSERT_EQ(util::CheckAlignment(*chunked_array, 2048, &needs_alignment), false);

  ASSERT_OK_AND_ASSIGN(auto aligned_chunked_array,
                       util::EnsureAlignment(std::move(chunked_array), 2048, pool));
  ASSERT_EQ(util::CheckAlignment(*aligned_chunked_array, 2048, &needs_alignment), true);
}

TEST(EnsureAlignment, RecordBatch) {
  MemoryPool* pool = default_memory_pool();
  auto rand = ::arrow::random::RandomArrayGenerator(1923);
  auto random_array_1 = rand.UInt8(/*size*/ 50, /*min*/ 0, /*max*/ 100,
                                   /*null_probability*/ 0, /*alignment*/ 512, pool);
  auto random_array_2 = rand.UInt8(/*size*/ 50, /*min*/ 10, /*max*/ 50,
                                   /*null_probability*/ 0, /*alignment*/ 1024, pool);

  ASSERT_OK_AND_ASSIGN(auto sliced_array_1,
                       arrow::internal::CreateUnalignedArray(*random_array_1));
  ASSERT_OK_AND_ASSIGN(auto sliced_array_2,
                       arrow::internal::CreateUnalignedArray(*random_array_2));

  auto f0 = field("f0", uint8());
  auto f1 = field("f1", uint8());
  std::vector<std::shared_ptr<Field>> fields = {f0, f1};
  auto schema = ::arrow::schema({f0, f1});

  auto record_batch = RecordBatch::Make(schema, 50, {sliced_array_1, sliced_array_2});

  std::vector<bool> needs_alignment;
  ASSERT_EQ(util::CheckAlignment(*record_batch, 2048, &needs_alignment), false);

  ASSERT_OK_AND_ASSIGN(auto aligned_record_batch,
                       util::EnsureAlignment(std::move(record_batch), 2048, pool));
  ASSERT_EQ(util::CheckAlignment(*aligned_record_batch, 2048, &needs_alignment), true);
}

TEST(EnsureAlignment, Table) {
  MemoryPool* pool = default_memory_pool();
  auto rand = ::arrow::random::RandomArrayGenerator(1923);

  auto random_array_1 = rand.UInt8(/*size*/ 50, /*min*/ 0, /*max*/ 100,
                                   /*null_probability*/ 0, /*alignment*/ 512, pool);
  auto random_array_2 = rand.UInt8(/*size*/ 100, /*min*/ 10, /*max*/ 50,
                                   /*null_probability*/ 0, /*alignment*/ 1024, pool);
  ASSERT_OK_AND_ASSIGN(auto sliced_array_1,
                       arrow::internal::CreateUnalignedArray(*random_array_1));
  ASSERT_OK_AND_ASSIGN(auto sliced_array_2,
                       arrow::internal::CreateUnalignedArray(*random_array_2));
  ASSERT_OK_AND_ASSIGN(
      auto chunked_array_1,
      ChunkedArray::Make({sliced_array_1, sliced_array_2}, sliced_array_1->type()));

  random_array_1 = rand.UInt8(/*size*/ 150, /*min*/ 0, /*max*/ 100,
                              /*null_probability*/ 0, /*alignment*/ 1024, pool);
  random_array_2 = rand.UInt8(/*size*/ 75, /*min*/ 10, /*max*/ 50,
                              /*null_probability*/ 0, /*alignment*/ 512, pool);
  ASSERT_OK_AND_ASSIGN(sliced_array_1,
                       arrow::internal::CreateUnalignedArray(*random_array_1));
  ASSERT_OK_AND_ASSIGN(sliced_array_2,
                       arrow::internal::CreateUnalignedArray(*random_array_2));
  ASSERT_OK_AND_ASSIGN(
      auto chunked_array_2,
      ChunkedArray::Make({sliced_array_1, sliced_array_2}, sliced_array_1->type()));

  auto f0 = field("f0", uint8());
  auto f1 = field("f1", uint8());
  std::vector<std::shared_ptr<Field>> fields = {f0, f1};
  auto schema = ::arrow::schema({f0, f1});

  auto table = Table::Make(schema, {chunked_array_1, chunked_array_2});

  std::vector<bool> needs_alignment;
  ASSERT_EQ(util::CheckAlignment(*table, 2048, &needs_alignment), false);

  ASSERT_OK_AND_ASSIGN(auto aligned_table,
                       util::EnsureAlignment(std::move(table), 2048, pool));
  ASSERT_EQ(util::CheckAlignment(*aligned_table, 2048, &needs_alignment), true);
}

}  // namespace arrow
