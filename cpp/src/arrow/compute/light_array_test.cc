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

#include "arrow/compute/light_array.h"

#include <gtest/gtest.h>
#include <numeric>

#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/vector.h"

namespace arrow {
namespace compute {

const std::vector<std::shared_ptr<DataType>> kSampleFixedDataTypes = {
    int8(),   int16(),  int32(),  int64(),           uint8(),
    uint16(), uint32(), uint64(), decimal128(38, 6), decimal256(76, 6)};
const std::vector<std::shared_ptr<DataType>> kSampleBinaryTypes = {utf8(), binary()};

static ExecBatch JSONToExecBatch(const std::vector<TypeHolder>& types,
                                 std::string_view json) {
  auto fields = ::arrow::internal::MapVector(
      [](const TypeHolder& th) { return field("", th.GetSharedPtr()); }, types);

  ExecBatch batch{*RecordBatchFromJSON(schema(std::move(fields)), json)};

  return batch;
}

TEST(KeyColumnMetadata, FromDataType) {
  KeyColumnMetadata metadata = ColumnMetadataFromDataType(boolean()).ValueOrDie();
  ASSERT_EQ(0, metadata.fixed_length);
  ASSERT_EQ(true, metadata.is_fixed_length);
  ASSERT_EQ(false, metadata.is_null_type);

  metadata = ColumnMetadataFromDataType(null()).ValueOrDie();
  ASSERT_EQ(true, metadata.is_null_type);

  for (const auto& type : kSampleFixedDataTypes) {
    int byte_width =
        arrow::internal::checked_pointer_cast<FixedWidthType>(type)->bit_width() / 8;
    metadata = ColumnMetadataFromDataType(type).ValueOrDie();
    ASSERT_EQ(byte_width, metadata.fixed_length);
    ASSERT_EQ(true, metadata.is_fixed_length);
    ASSERT_EQ(false, metadata.is_null_type);
  }

  for (const auto& type : {binary(), utf8()}) {
    metadata = ColumnMetadataFromDataType(type).ValueOrDie();
    ASSERT_EQ(4, metadata.fixed_length);
    ASSERT_EQ(false, metadata.is_fixed_length);
    ASSERT_EQ(false, metadata.is_null_type);
  }

  for (const auto& type : {large_binary(), large_utf8()}) {
    metadata = ColumnMetadataFromDataType(type).ValueOrDie();
    ASSERT_EQ(8, metadata.fixed_length);
    ASSERT_EQ(false, metadata.is_fixed_length);
    ASSERT_EQ(false, metadata.is_null_type);
  }
}

TEST(KeyColumnArray, FromArrayData) {
  for (const auto& type : kSampleFixedDataTypes) {
    ARROW_SCOPED_TRACE("Type: ", type->ToString());
    // `array_offset` is the offset of the source array (e.g. if we are given a sliced
    // source array) while `offset` is the offset we pass when constructing the
    // KeyColumnArray
    for (auto array_offset : {0, 1}) {
      ARROW_SCOPED_TRACE("Array offset: ", array_offset);
      for (auto offset : {0, 1}) {
        ARROW_SCOPED_TRACE("Constructor offset: ", offset);
        std::shared_ptr<Array> array;
        int byte_width =
            arrow::internal::checked_pointer_cast<FixedWidthType>(type)->bit_width() / 8;
        if (is_decimal(type->id())) {
          array = ArrayFromJSON(type, R"(["1.123123", "2.123123", null])");
        } else {
          array = ArrayFromJSON(type, "[1, 2, null]");
        }
        array = array->Slice(array_offset);
        int length = static_cast<int32_t>(array->length()) - offset - array_offset;
        int buffer_offset_bytes = (offset + array_offset) * byte_width;
        KeyColumnArray kc_array =
            ColumnArrayFromArrayData(array->data(), offset, length).ValueOrDie();
        // Maximum tested offset is < 8 so validity is just bit offset
        ASSERT_EQ(offset + array_offset, kc_array.bit_offset(0));
        ASSERT_EQ(0, kc_array.bit_offset(1));
        ASSERT_EQ(array->data()->buffers[0]->data(), kc_array.data(0));
        ASSERT_EQ(array->data()->buffers[1]->data() + buffer_offset_bytes,
                  kc_array.data(1));
        ASSERT_EQ(nullptr, kc_array.data(2));
        ASSERT_EQ(length, kc_array.length());
        // When creating from ArrayData always create read-only
        ASSERT_EQ(nullptr, kc_array.mutable_data(0));
        ASSERT_EQ(nullptr, kc_array.mutable_data(1));
        ASSERT_EQ(nullptr, kc_array.mutable_data(2));
      }
    }
  }
}

TEST(KeyColumnArray, FromArrayDataBinary) {
  for (const auto& type : kSampleBinaryTypes) {
    ARROW_SCOPED_TRACE("Type: ", type->ToString());
    for (auto array_offset : {0, 1}) {
      ARROW_SCOPED_TRACE("Array offset: ", array_offset);
      for (auto offset : {0, 1}) {
        ARROW_SCOPED_TRACE("Constructor offset: ", offset);
        std::shared_ptr<Array> array = ArrayFromJSON(type, R"(["xyz", "abcabc", null])");
        int offsets_width =
            static_cast<int>(arrow::internal::checked_pointer_cast<BaseBinaryType>(type)
                                 ->layout()
                                 .buffers[1]
                                 .byte_width);
        array = array->Slice(array_offset);
        int length = static_cast<int32_t>(array->length()) - offset - array_offset;
        int buffer_offset_bytes = (offset + array_offset) * offsets_width;
        KeyColumnArray kc_array =
            ColumnArrayFromArrayData(array->data(), offset, length).ValueOrDie();
        ASSERT_EQ(offset + array_offset, kc_array.bit_offset(0));
        ASSERT_EQ(0, kc_array.bit_offset(1));
        ASSERT_EQ(array->data()->buffers[0]->data(), kc_array.data(0));
        ASSERT_EQ(array->data()->buffers[1]->data() + buffer_offset_bytes,
                  kc_array.data(1));
        ASSERT_EQ(array->data()->buffers[2]->data(), kc_array.data(2));
        ASSERT_EQ(length, kc_array.length());
        // When creating from ArrayData always create read-only
        ASSERT_EQ(nullptr, kc_array.mutable_data(0));
        ASSERT_EQ(nullptr, kc_array.mutable_data(1));
        ASSERT_EQ(nullptr, kc_array.mutable_data(2));
      }
    }
  }
}

TEST(KeyColumnArray, FromArrayDataBool) {
  for (auto array_offset : {0, 1}) {
    ARROW_SCOPED_TRACE("Array offset: ", array_offset);
    for (auto offset : {0, 1}) {
      ARROW_SCOPED_TRACE("Constructor offset: ", offset);
      std::shared_ptr<Array> array = ArrayFromJSON(boolean(), "[true, false, null]");
      array = array->Slice(array_offset);
      int length = static_cast<int32_t>(array->length()) - offset - array_offset;
      KeyColumnArray kc_array =
          ColumnArrayFromArrayData(array->data(), offset, length).ValueOrDie();
      ASSERT_EQ(offset + array_offset, kc_array.bit_offset(0));
      ASSERT_EQ(offset + array_offset, kc_array.bit_offset(1));
      ASSERT_EQ(array->data()->buffers[0]->data(), kc_array.data(0));
      ASSERT_EQ(array->data()->buffers[1]->data(), kc_array.data(1));
      ASSERT_EQ(length, kc_array.length());
      ASSERT_EQ(nullptr, kc_array.mutable_data(0));
      ASSERT_EQ(nullptr, kc_array.mutable_data(1));
    }
  }
}

TEST(KeyColumnArray, Slice) {
  constexpr int kValuesByteLength = 128;
  // Size needed for validity depends on byte_width but 16 will always be big enough
  constexpr int kValidityByteLength = 16;
  uint8_t validity_buffer[kValidityByteLength];
  uint8_t values_buffer[kValuesByteLength];
  for (auto byte_width : {2, 4}) {
    ARROW_SCOPED_TRACE("Byte Width: ", byte_width);
    int64_t length = kValuesByteLength / byte_width;
    KeyColumnMetadata metadata(true, byte_width);
    KeyColumnArray array(metadata, length, validity_buffer, values_buffer, nullptr);

    for (int offset : {0, 4, 12}) {
      ARROW_SCOPED_TRACE("Offset: ", offset);
      for (int length : {0, 4}) {
        ARROW_SCOPED_TRACE("Length: ", length);
        KeyColumnArray sliced = array.Slice(offset, length);
        int expected_validity_bit_offset = (offset == 0) ? 0 : 4;
        int expected_validity_byte_offset = (offset == 12) ? 1 : 0;
        int expected_values_byte_offset = byte_width * offset;
        ASSERT_EQ(expected_validity_bit_offset, sliced.bit_offset(0));
        ASSERT_EQ(0, sliced.bit_offset(1));
        ASSERT_EQ(validity_buffer + expected_validity_byte_offset,
                  sliced.mutable_data(0));
        ASSERT_EQ(values_buffer + expected_values_byte_offset, sliced.mutable_data(1));
      }
    }
  }
}

TEST(KeyColumnArray, SliceBool) {
  constexpr int kValuesByteLength = 2;
  constexpr int kValidityByteLength = 2;
  uint8_t validity_buffer[kValidityByteLength];
  uint8_t values_buffer[kValuesByteLength];
  int length = 16;
  KeyColumnMetadata metadata(true, /*byte_width=*/0);
  KeyColumnArray array(metadata, length, validity_buffer, values_buffer, nullptr);

  for (int offset : {0, 4, 12}) {
    ARROW_SCOPED_TRACE("Offset: ", offset);
    for (int length : {0, 4}) {
      ARROW_SCOPED_TRACE("Length: ", length);
      KeyColumnArray sliced = array.Slice(offset, length);
      int expected_bit_offset = (offset == 0) ? 0 : 4;
      int expected_byte_offset = (offset == 12) ? 1 : 0;
      ASSERT_EQ(expected_bit_offset, sliced.bit_offset(0));
      ASSERT_EQ(expected_bit_offset, sliced.bit_offset(1));
      ASSERT_EQ(validity_buffer + expected_byte_offset, sliced.mutable_data(0));
      ASSERT_EQ(values_buffer + expected_byte_offset, sliced.mutable_data(1));
    }
  }
}

TEST(ResizableArrayData, Basic) {
  std::unique_ptr<MemoryPool> pool = MemoryPool::CreateDefault();
  for (const auto& type : kSampleFixedDataTypes) {
    ARROW_SCOPED_TRACE("Type: ", type->ToString());
    int byte_width =
        arrow::internal::checked_pointer_cast<FixedWidthType>(type)->bit_width() / 8;
    {
      ResizableArrayData array;
      array.Init(type, pool.get(), /*log_num_rows_min=*/16);
      ASSERT_EQ(0, array.num_rows());
      ASSERT_OK(array.ResizeFixedLengthBuffers(2));
      ASSERT_EQ(2, array.num_rows());
      // Even though we are only asking for 2 rows we specified a rather high
      // log_num_rows_min so it should allocate at least that many rows.  Padding
      // and rounding up to a power of 2 will make the allocations larger.
      int min_bytes_needed_for_values = byte_width * (1 << 16);
      int min_bytes_needed_for_validity = (1 << 16) / 8;
      int min_bytes_needed = min_bytes_needed_for_values + min_bytes_needed_for_validity;
      ASSERT_LT(min_bytes_needed, pool->bytes_allocated());
      ASSERT_GT(min_bytes_needed * 2, pool->bytes_allocated());

      ASSERT_OK(array.ResizeFixedLengthBuffers(1 << 17));
      ASSERT_LT(min_bytes_needed * 2, pool->bytes_allocated());
      ASSERT_GT(min_bytes_needed * 4, pool->bytes_allocated());
      ASSERT_EQ(1 << 17, array.num_rows());

      // Shrinking array won't shrink allocated RAM
      ASSERT_OK(array.ResizeFixedLengthBuffers(2));
      ASSERT_LT(min_bytes_needed * 2, pool->bytes_allocated());
      ASSERT_GT(min_bytes_needed * 4, pool->bytes_allocated());
      ASSERT_EQ(2, array.num_rows());
    }
    // After array is destroyed buffers should be freed
    ASSERT_EQ(0, pool->bytes_allocated());
  }
}

TEST(ResizableArrayData, Binary) {
  std::unique_ptr<MemoryPool> pool = MemoryPool::CreateDefault();
  for (const auto& type : kSampleBinaryTypes) {
    ARROW_SCOPED_TRACE("Type: ", type->ToString());
    {
      ResizableArrayData array;
      array.Init(type, pool.get(), /*log_num_rows_min=*/4);
      ASSERT_EQ(0, array.num_rows());
      ASSERT_OK(array.ResizeFixedLengthBuffers(2));
      ASSERT_EQ(2, array.num_rows());
      // At this point the offets memory has been allocated and needs to be filled
      // in before we allocate the variable length memory
      int offsets_width =
          static_cast<int>(arrow::internal::checked_pointer_cast<BaseBinaryType>(type)
                               ->layout()
                               .buffers[1]
                               .byte_width);
      if (offsets_width == 4) {
        auto offsets = reinterpret_cast<int32_t*>(array.mutable_data(1));
        offsets[0] = 0;
        offsets[1] = 1000;
        offsets[2] = 2000;
      } else if (offsets_width == 8) {
        auto offsets = reinterpret_cast<int64_t*>(array.mutable_data(1));
        offsets[0] = 0;
        offsets[1] = 1000;
        offsets[2] = 2000;
      } else {
        FAIL() << "Unexpected offsets_width: " << offsets_width;
      }
      ASSERT_OK(array.ResizeVaryingLengthBuffer());
      // Each string is 1000 bytes.  The offsets, padding, etc. should be less than 1000
      // bytes
      ASSERT_LT(2000, pool->bytes_allocated());
      ASSERT_GT(3000, pool->bytes_allocated());
    }
    // After array is destroyed buffers should be freed
    ASSERT_EQ(0, pool->bytes_allocated());
  }
}

TEST(ExecBatchBuilder, AppendNullsBeyondLimit) {
  std::unique_ptr<MemoryPool> owned_pool = MemoryPool::CreateDefault();
  int num_rows_max = ExecBatchBuilder::num_rows_max();
  MemoryPool* pool = owned_pool.get();
  {
    ExecBatchBuilder builder;
    ASSERT_OK(builder.AppendNulls(pool, {int64(), boolean()}, 10));
    ASSERT_RAISES(CapacityError,
                  builder.AppendNulls(pool, {int64(), boolean()}, num_rows_max + 1 - 10));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(10, built.length);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  ASSERT_EQ(0, pool->bytes_allocated());
}

TEST(ExecBatchBuilder, AppendValuesBeyondLimit) {
  std::unique_ptr<MemoryPool> owned_pool = MemoryPool::CreateDefault();
  MemoryPool* pool = owned_pool.get();
  int num_rows_max = ExecBatchBuilder::num_rows_max();
  std::shared_ptr<Array> values = ConstantArrayGenerator::Int32(num_rows_max + 1);
  std::shared_ptr<Array> trimmed_values = ConstantArrayGenerator::Int32(10);
  ExecBatch batch({values}, num_rows_max + 1);
  ExecBatch trimmed_batch({trimmed_values}, 10);
  std::vector<uint16_t> first_set_row_ids(10);
  std::iota(first_set_row_ids.begin(), first_set_row_ids.end(), 0);
  std::vector<uint16_t> second_set_row_ids(num_rows_max + 1 - 10);
  std::iota(second_set_row_ids.begin(), second_set_row_ids.end(), 10);
  {
    ExecBatchBuilder builder;
    ASSERT_OK(builder.AppendSelected(pool, batch, 10, first_set_row_ids.data(),
                                     /*num_cols=*/1));
    ASSERT_RAISES(CapacityError,
                  builder.AppendSelected(pool, batch, num_rows_max + 1 - 10,
                                         second_set_row_ids.data(),
                                         /*num_cols=*/1));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(trimmed_batch, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  ASSERT_EQ(0, pool->bytes_allocated());
}

TEST(KeyColumnArray, FromExecBatch) {
  ExecBatch batch =
      JSONToExecBatch({int64(), boolean()}, "[[1, true], [2, false], [null, null]]");
  std::vector<KeyColumnArray> arrays;
  ASSERT_OK(ColumnArraysFromExecBatch(batch, &arrays));

  ASSERT_EQ(2, arrays.size());
  ASSERT_EQ(8, arrays[0].metadata().fixed_length);
  ASSERT_EQ(0, arrays[1].metadata().fixed_length);
  ASSERT_EQ(3, arrays[0].length());
  ASSERT_EQ(3, arrays[1].length());

  ASSERT_OK(ColumnArraysFromExecBatch(batch, 1, 1, &arrays));

  ASSERT_EQ(2, arrays.size());
  ASSERT_EQ(8, arrays[0].metadata().fixed_length);
  ASSERT_EQ(0, arrays[1].metadata().fixed_length);
  ASSERT_EQ(1, arrays[0].length());
  ASSERT_EQ(1, arrays[1].length());
}

TEST(ExecBatchBuilder, AppendBatches) {
  std::unique_ptr<MemoryPool> owned_pool = MemoryPool::CreateDefault();
  MemoryPool* pool = owned_pool.get();
  ExecBatch batch_one =
      JSONToExecBatch({int64(), boolean()}, "[[1, true], [2, false], [null, null]]");
  ExecBatch batch_two =
      JSONToExecBatch({int64(), boolean()}, "[[null, true], [5, true], [6, false]]");
  ExecBatch combined = JSONToExecBatch(
      {int64(), boolean()},
      "[[1, true], [2, false], [null, null], [null, true], [5, true], [6, false]]");
  {
    ExecBatchBuilder builder;
    uint16_t row_ids[3] = {0, 1, 2};
    ASSERT_OK(builder.AppendSelected(pool, batch_one, 3, row_ids, /*num_cols=*/2));
    ASSERT_OK(builder.AppendSelected(pool, batch_two, 3, row_ids, /*num_cols=*/2));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(combined, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  ASSERT_EQ(0, pool->bytes_allocated());
}

TEST(ExecBatchBuilder, AppendBatchesSomeRows) {
  std::unique_ptr<MemoryPool> owned_pool = MemoryPool::CreateDefault();
  MemoryPool* pool = owned_pool.get();
  ExecBatch batch_one =
      JSONToExecBatch({int64(), boolean()}, "[[1, true], [2, false], [null, null]]");
  ExecBatch batch_two =
      JSONToExecBatch({int64(), boolean()}, "[[null, true], [5, true], [6, false]]");
  ExecBatch combined = JSONToExecBatch(
      {int64(), boolean()}, "[[1, true], [2, false], [null, true], [5, true]]");
  {
    ExecBatchBuilder builder;
    uint16_t row_ids[2] = {0, 1};
    ASSERT_OK(builder.AppendSelected(pool, batch_one, 2, row_ids, /*num_cols=*/2));
    ASSERT_OK(builder.AppendSelected(pool, batch_two, 2, row_ids, /*num_cols=*/2));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(combined, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  ASSERT_EQ(0, pool->bytes_allocated());
}

TEST(ExecBatchBuilder, AppendBatchesSomeCols) {
  std::unique_ptr<MemoryPool> owned_pool = MemoryPool::CreateDefault();
  MemoryPool* pool = owned_pool.get();
  ExecBatch batch_one =
      JSONToExecBatch({int64(), boolean()}, "[[1, true], [2, false], [null, null]]");
  ExecBatch batch_two =
      JSONToExecBatch({int64(), boolean()}, "[[null, true], [5, true], [6, false]]");
  ExecBatch first_col_only =
      JSONToExecBatch({int64()}, "[[1], [2], [null], [null], [5], [6]]");
  ExecBatch last_col_only =
      JSONToExecBatch({boolean()}, "[[true], [false], [null], [true], [true], [false]]");
  {
    ExecBatchBuilder builder;
    uint16_t row_ids[3] = {0, 1, 2};
    int first_col_ids[1] = {0};
    ASSERT_OK(builder.AppendSelected(pool, batch_one, 3, row_ids, /*num_cols=*/1,
                                     first_col_ids));
    ASSERT_OK(builder.AppendSelected(pool, batch_two, 3, row_ids, /*num_cols=*/1,
                                     first_col_ids));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(first_col_only, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  {
    ExecBatchBuilder builder;
    uint16_t row_ids[3] = {0, 1, 2};
    // If we don't specify col_ids and num_cols is 1 it is implicitly the first col
    ASSERT_OK(builder.AppendSelected(pool, batch_one, 3, row_ids, /*num_cols=*/1));
    ASSERT_OK(builder.AppendSelected(pool, batch_two, 3, row_ids, /*num_cols=*/1));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(first_col_only, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  {
    ExecBatchBuilder builder;
    uint16_t row_ids[3] = {0, 1, 2};
    int last_col_ids[1] = {1};
    ASSERT_OK(builder.AppendSelected(pool, batch_one, 3, row_ids, /*num_cols=*/1,
                                     last_col_ids));
    ASSERT_OK(builder.AppendSelected(pool, batch_two, 3, row_ids, /*num_cols=*/1,
                                     last_col_ids));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(last_col_only, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  ASSERT_EQ(0, pool->bytes_allocated());
}

TEST(ExecBatchBuilder, AppendNulls) {
  std::unique_ptr<MemoryPool> owned_pool = MemoryPool::CreateDefault();
  MemoryPool* pool = owned_pool.get();
  ExecBatch batch_one =
      JSONToExecBatch({int64(), boolean()}, "[[1, true], [2, false], [null, null]]");
  ExecBatch combined = JSONToExecBatch(
      {int64(), boolean()},
      "[[1, true], [2, false], [null, null], [null, null], [null, null]]");
  ExecBatch just_nulls =
      JSONToExecBatch({int64(), boolean()}, "[[null, null], [null, null]]");
  {
    ExecBatchBuilder builder;
    uint16_t row_ids[3] = {0, 1, 2};
    ASSERT_OK(builder.AppendSelected(pool, batch_one, 3, row_ids, /*num_cols=*/2));
    ASSERT_OK(builder.AppendNulls(pool, {int64(), boolean()}, 2));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(combined, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  {
    ExecBatchBuilder builder;
    ASSERT_OK(builder.AppendNulls(pool, {int64(), boolean()}, 2));
    ExecBatch built = builder.Flush();
    ASSERT_EQ(just_nulls, built);
    ASSERT_NE(0, pool->bytes_allocated());
  }
  ASSERT_EQ(0, pool->bytes_allocated());
}

}  // namespace compute
}  // namespace arrow
