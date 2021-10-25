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

#include "arrow/util/byte_size.h"

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace util {

TEST(TotalBufferSize, Arrays) {
  std::shared_ptr<Array> no_nulls = ArrayFromJSON(int16(), "[1, 2, 3]");
  ASSERT_EQ(6, TotalBufferSize(*no_nulls->data()));

  std::shared_ptr<Array> with_nulls =
      ArrayFromJSON(int16(), "[1, 2, 3, 4, null, 6, 7, 8, 9]");
  ASSERT_EQ(20, TotalBufferSize(*with_nulls->data()));
}

TEST(TotalBufferSize, NestedArray) {
  std::shared_ptr<Array> array_with_children =
      ArrayFromJSON(list(int64()), "[[0, 1, 2, 3, 4], [5], null]");
  // The offsets array will have 4 4-byte offsets      (16)
  // The child array will have 6 8-byte values         (48)
  // The child array will not have a validity bitmap
  // The list array will have a 1 byte validity bitmap  (1)
  ASSERT_EQ(65, TotalBufferSize(*array_with_children));
}

TEST(TotalBufferSize, ArrayWithOffset) {
  std::shared_ptr<Array> base_array =
      ArrayFromJSON(int16(), "[1, 2, 3, 4, null, 6, 7, 8, 9]");
  std::shared_ptr<Array> sliced = base_array->Slice(8, 1);
  ASSERT_EQ(20, TotalBufferSize(*sliced));
}

TEST(TotalBufferSize, ArrayWithDict) {
  std::shared_ptr<Array> arr = ArrayFromJSON(dictionary(int32(), int8()), "[0, 0, 0]");
  ASSERT_EQ(13, TotalBufferSize(*arr));
}

TEST(TotalBufferSize, ChunkedArray) {
  ArrayVector arrays;
  for (int i = 0; i < 10; i++) {
    arrays.push_back(ConstantArrayGenerator::Zeroes(5, int32()));
  }
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> chunked_array,
                       ChunkedArray::Make(std::move(arrays)));
  ASSERT_EQ(5 * 4 * 10, TotalBufferSize(*chunked_array));
}

TEST(TotalBufferSize, RecordBatch) {
  std::shared_ptr<RecordBatch> record_batch = ConstantArrayGenerator::Zeroes(
      10, schema({field("a", int32()), field("b", int64())}));
  ASSERT_EQ(10 * 4 + 10 * 8, TotalBufferSize(*record_batch));
}

TEST(TotalBufferSize, Table) {
  ArrayVector c1_arrays, c2_arrays;
  for (int i = 0; i < 5; i++) {
    c1_arrays.push_back(ConstantArrayGenerator::Zeroes(10, int32()));
  }
  for (int i = 0; i < 10; i++) {
    c2_arrays.push_back(ConstantArrayGenerator::Zeroes(5, int64()));
  }
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> c1,
                       ChunkedArray::Make(std::move(c1_arrays)));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> c2,
                       ChunkedArray::Make(std::move(c2_arrays)));
  std::shared_ptr<Schema> schm = schema({field("a", int32()), field("b", int64())});
  std::shared_ptr<Table> table = Table::Make(std::move(schm), {c1, c2});
  ASSERT_EQ(5 * 10 * 4 + 10 * 5 * 8, TotalBufferSize(*table));
}

TEST(TotalBufferSize, SharedBuffers) {
  std::shared_ptr<Array> shared = ArrayFromJSON(int16(), "[1, 2, 3]");
  std::shared_ptr<Array> first = shared->Slice(0, 2);
  std::shared_ptr<Array> second = shared->Slice(1, 2);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> combined,
                       ChunkedArray::Make({first, second}));
  ASSERT_EQ(6, TotalBufferSize(*combined));
}

}  // namespace util
}  // namespace arrow
