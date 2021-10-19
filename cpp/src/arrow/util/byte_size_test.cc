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

TEST(TestBufferSize, Arrays) {
  MemoryPool* pool = default_memory_pool();
  // null buffer isn't counted but shouldn't interfere
  std::shared_ptr<Buffer> buf1 = std::make_shared<Buffer>(nullptr, 0);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buf2, AllocateBuffer(1000, pool));
  std::shared_ptr<ArrayData> field_one_data =
      ArrayData::Make(int32(), 250, {buf1, buf2}, {}, kUnknownNullCount, 0);
  ASSERT_EQ(1000, EstimateBufferSize(*field_one_data));
  std::shared_ptr<Array> field_one_array = MakeArray(field_one_data);
  ASSERT_EQ(1000, EstimateBufferSize(*field_one_array));

  std::shared_ptr<ArrayData> field_two_data =
      ArrayData::Make(int32(), 250, {buf1, buf2}, {}, kUnknownNullCount, 0);
  std::shared_ptr<DataType> struct_type =
      struct_({field("a", int32()), field("b", int32())});
  std::shared_ptr<ArrayData> with_children_data =
      ArrayData::Make(struct_type, 250, {std::make_shared<Buffer>(NULLPTR, 0)},
                      {field_one_data, field_two_data}, kUnknownNullCount, 0);

  // Both arrays share the same data but EstimateBufferSize doesn't currently detect that
  ASSERT_EQ(2000, EstimateBufferSize(*with_children_data));
  std::shared_ptr<Array> with_children_array = MakeArray(with_children_data);
  ASSERT_EQ(2000, EstimateBufferSize(*with_children_array));
}

TEST(TestBufferSize, ArrayWithOffset) {
  MemoryPool* pool = default_memory_pool();
  std::shared_ptr<Buffer> buf1 = std::make_shared<Buffer>(nullptr, 0);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buf2, AllocateBuffer(1000, pool));
  std::shared_ptr<ArrayData> data =
      ArrayData::Make(int32(), 200, {buf1, buf2}, {}, kUnknownNullCount, 200);
  // Offsets are not handled currently
  ASSERT_EQ(1000, EstimateBufferSize(*data));
}

TEST(TestBufferSize, ArrayWithDict) {
  std::shared_ptr<Array> arr = ArrayFromJSON(dictionary(int32(), int8()), "[0, 0, 0]");
  ASSERT_EQ(13, EstimateBufferSize(*arr));
}

TEST(TestBufferSize, ChunkedArray) {
  ArrayVector arrays;
  for (int i = 0; i < 10; i++) {
    arrays.push_back(ConstantArrayGenerator::Zeroes(5, int32()));
  }
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> chunked_array,
                       ChunkedArray::Make(std::move(arrays)));
  ASSERT_EQ(5 * 4 * 10, EstimateBufferSize(*chunked_array));
}

TEST(TestBufferSize, RecordBatch) {
  std::shared_ptr<RecordBatch> record_batch = ConstantArrayGenerator::Zeroes(
      10, schema({field("a", int32()), field("b", int64())}));
  ASSERT_EQ(10 * 4 + 10 * 8, EstimateBufferSize(*record_batch));
}

TEST(TestBufferSize, Table) {
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
  ASSERT_EQ(5 * 10 * 4 + 10 * 5 * 8, EstimateBufferSize(*table));
}

}  // namespace util
}  // namespace arrow
