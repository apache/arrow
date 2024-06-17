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

#include <numeric>

#include "arrow/array/builder_binary.h"
#include "arrow/compute/row/compare_internal.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

using arrow::bit_util::BytesForBits;
using arrow::internal::CpuInfo;
using arrow::random::RandomArrayGenerator;
using arrow::util::MiniBatch;
using arrow::util::TempVectorStack;

// Specialized case for GH-39577.
TEST(KeyCompare, CompareColumnsToRowsCuriousFSB) {
  int fsb_length = 9;
  int num_rows = 7;

  MemoryPool* pool = default_memory_pool();
  TempVectorStack stack;
  ASSERT_OK(stack.Init(pool, KeyCompare::CompareColumnsToRowsTempStackUsage(num_rows)));

  auto column_right = ArrayFromJSON(fixed_size_binary(fsb_length), R"([
      "000000000",
      "111111111",
      "222222222",
      "333333333",
      "444444444",
      "555555555",
      "666666666"])");
  ExecBatch batch_right({column_right}, num_rows);

  std::vector<KeyColumnMetadata> column_metadatas_right;
  ASSERT_OK(ColumnMetadatasFromExecBatch(batch_right, &column_metadatas_right));

  RowTableMetadata table_metadata_right;
  table_metadata_right.FromColumnMetadataVector(column_metadatas_right, sizeof(uint64_t),
                                                sizeof(uint64_t));

  std::vector<KeyColumnArray> column_arrays_right;
  ASSERT_OK(ColumnArraysFromExecBatch(batch_right, &column_arrays_right));

  RowTableImpl row_table;
  ASSERT_OK(row_table.Init(pool, table_metadata_right));

  RowTableEncoder row_encoder;
  row_encoder.Init(column_metadatas_right, sizeof(uint64_t), sizeof(uint64_t));
  row_encoder.PrepareEncodeSelected(0, num_rows, column_arrays_right);

  std::vector<uint16_t> row_ids_right(num_rows);
  std::iota(row_ids_right.begin(), row_ids_right.end(), 0);
  ASSERT_OK(row_encoder.EncodeSelected(&row_table, num_rows, row_ids_right.data()));

  auto column_left = ArrayFromJSON(fixed_size_binary(fsb_length), R"([
      "000000000",
      "111111111",
      "222222222",
      "333333333",
      "444444444",
      "555555555",
      "777777777"])");
  ExecBatch batch_left({column_left}, num_rows);
  std::vector<KeyColumnArray> column_arrays_left;
  ASSERT_OK(ColumnArraysFromExecBatch(batch_left, &column_arrays_left));

  std::vector<uint32_t> row_ids_left(num_rows);
  std::iota(row_ids_left.begin(), row_ids_left.end(), 0);

  LightContext ctx{CpuInfo::GetInstance()->hardware_flags(), &stack};

  {
    uint32_t num_rows_no_match;
    std::vector<uint16_t> row_ids_out(num_rows);
    KeyCompare::CompareColumnsToRows(num_rows, NULLPTR, row_ids_left.data(), &ctx,
                                     &num_rows_no_match, row_ids_out.data(),
                                     column_arrays_left, row_table, true, NULLPTR);
    ASSERT_EQ(num_rows_no_match, 1);
    ASSERT_EQ(row_ids_out[0], 6);
  }

  {
    std::vector<uint8_t> match_bitvector(BytesForBits(num_rows));
    KeyCompare::CompareColumnsToRows(num_rows, NULLPTR, row_ids_left.data(), &ctx,
                                     NULLPTR, NULLPTR, column_arrays_left, row_table,
                                     true, match_bitvector.data());
    for (int i = 0; i < num_rows; ++i) {
      SCOPED_TRACE(i);
      ASSERT_EQ(arrow::bit_util::GetBit(match_bitvector.data(), i), i != 6);
    }
  }
}

// Make sure that KeyCompare::CompareColumnsToRows uses no more stack space than declared
// in KeyCompare::CompareColumnsToRowsTempStackUsage().
TEST(KeyCompare, CompareColumnsToRowsTempStackUsage) {
  for (auto num_rows :
       {0, 1, MiniBatch::kMiniBatchLength, MiniBatch::kMiniBatchLength * 64}) {
    SCOPED_TRACE("num_rows = " + std::to_string(num_rows));

    MemoryPool* pool = default_memory_pool();
    TempVectorStack stack;
    ASSERT_OK(stack.Init(pool, KeyCompare::CompareColumnsToRowsTempStackUsage(num_rows)));

    RandomArrayGenerator gen(42);

    auto column_right = gen.Int8(num_rows, 0, 127);
    ExecBatch batch_right({column_right}, num_rows);

    std::vector<KeyColumnMetadata> column_metadatas_right;
    ASSERT_OK(ColumnMetadatasFromExecBatch(batch_right, &column_metadatas_right));

    RowTableMetadata table_metadata_right;
    table_metadata_right.FromColumnMetadataVector(column_metadatas_right,
                                                  sizeof(uint64_t), sizeof(uint64_t));

    std::vector<KeyColumnArray> column_arrays_right;
    ASSERT_OK(ColumnArraysFromExecBatch(batch_right, &column_arrays_right));

    RowTableImpl row_table;
    ASSERT_OK(row_table.Init(pool, table_metadata_right));

    RowTableEncoder row_encoder;
    row_encoder.Init(column_metadatas_right, sizeof(uint64_t), sizeof(uint64_t));
    row_encoder.PrepareEncodeSelected(0, num_rows, column_arrays_right);

    std::vector<uint16_t> row_ids_right(num_rows);
    std::iota(row_ids_right.begin(), row_ids_right.end(), 0);
    ASSERT_OK(row_encoder.EncodeSelected(&row_table, num_rows, row_ids_right.data()));

    auto column_left = gen.Int8(num_rows, 0, 127);
    ExecBatch batch_left({column_left}, num_rows);
    std::vector<KeyColumnArray> column_arrays_left;
    ASSERT_OK(ColumnArraysFromExecBatch(batch_left, &column_arrays_left));

    std::vector<uint32_t> row_ids_left(num_rows);
    std::iota(row_ids_left.begin(), row_ids_left.end(), 0);

    LightContext ctx{CpuInfo::GetInstance()->hardware_flags(), &stack};

    uint32_t num_rows_no_match;
    std::vector<uint16_t> row_ids_out(num_rows);
    KeyCompare::CompareColumnsToRows(num_rows, NULLPTR, row_ids_left.data(), &ctx,
                                     &num_rows_no_match, row_ids_out.data(),
                                     column_arrays_left, row_table, true, NULLPTR);
  }
}

// Specialized case for GH-41813.
TEST(KeyCompare, CompareColumnsToRowsLarge) {
  if constexpr (sizeof(void*) == 4) {
    GTEST_SKIP() << "Test only works on 64-bit platforms";
  }

  constexpr auto fsb_length = 128 * 1024 * 1024;

  constexpr auto num_rows_base = 18;
  MemoryPool* pool = default_memory_pool();
  TempVectorStack stack;
  ASSERT_OK(
      stack.Init(pool, KeyCompare::CompareColumnsToRowsTempStackUsage(num_rows_base)));

  // An array containing 17 null rows and one 'X...' row.
  std::shared_ptr<FixedSizeBinaryArray> column_fsb;
  {
    FixedSizeBinaryBuilder builder(fixed_size_binary(fsb_length), pool);
    ASSERT_OK(builder.Reserve(num_rows_base));
    std::string x(fsb_length, 'X'), y(fsb_length, 'Y');
    for (int i = 0; i < num_rows_base - 1; ++i) {
      ASSERT_OK(builder.Append(x.data()));
    }
    ASSERT_OK(builder.Append(y.data()));
    ASSERT_OK(builder.Finish(&column_fsb));
  }
  std::shared_ptr<BinaryArray> column_binary;
  {
    BinaryBuilder builder(binary(), pool);
    ASSERT_OK(builder.AppendNulls(num_rows_base));
    ASSERT_OK(builder.Finish(&column_binary));
  }
  ExecBatch batch_base({column_fsb, column_binary}, num_rows_base);

  std::vector<KeyColumnMetadata> column_metadatas_base;
  ASSERT_OK(ColumnMetadatasFromExecBatch(batch_base, &column_metadatas_base));
  std::vector<KeyColumnArray> column_arrays_base;
  ASSERT_OK(ColumnArraysFromExecBatch(batch_base, &column_arrays_base));

  RowTableMetadata table_metadata_right;
  table_metadata_right.FromColumnMetadataVector(column_metadatas_base, sizeof(uint64_t),
                                                sizeof(uint64_t));

  RowTableImpl row_table;
  ASSERT_OK(row_table.Init(pool, table_metadata_right));

  // Encode row table with 18 rows, so that the last row is placed at over 2GB offset.
  constexpr auto num_rows_right = num_rows_base;
  RowTableEncoder row_encoder;
  row_encoder.Init(column_metadatas_base, sizeof(uint64_t), sizeof(uint64_t));
  row_encoder.PrepareEncodeSelected(0, num_rows_right, column_arrays_base);
  std::array<uint16_t, num_rows_right> row_ids_right;
  std::iota(row_ids_right.begin(), row_ids_right.end(), 0);
  // for (int i = 0; i < num_rows_right - 1; ++i) {
  //   row_ids_right[i] = 0;
  // }
  // row_ids_right[num_rows_right - 1] = 1;
  ASSERT_OK(row_encoder.EncodeSelected(&row_table, num_rows_right, row_ids_right.data()));

  ASSERT_GT(row_table.offsets()[num_rows_right - 1], 0x80000000u);

  constexpr auto num_rows_left = 16;
  std::vector<uint32_t> row_ids_left(num_rows_left, num_rows_base - 1);

  LightContext ctx{CpuInfo::GetInstance()->hardware_flags(), &stack};

  {
    uint32_t num_rows_no_match;
    std::vector<uint16_t> row_ids_out(num_rows_left);
    KeyCompare::CompareColumnsToRows(num_rows_left, NULLPTR, row_ids_left.data(), &ctx,
                                     &num_rows_no_match, row_ids_out.data(),
                                     column_arrays_base, row_table, true, NULLPTR);
    ASSERT_EQ(num_rows_no_match, 0);
    ASSERT_EQ(row_ids_out[0], 0);
  }

  // {
  //   std::vector<uint8_t> match_bitvector(BytesForBits(num_rows));
  //   KeyCompare::CompareColumnsToRows(num_rows, NULLPTR, row_ids_left.data(), &ctx,
  //                                    NULLPTR, NULLPTR, column_arrays_left, row_table,
  //                                    true, match_bitvector.data());
  //   for (int i = 0; i < num_rows; ++i) {
  //     SCOPED_TRACE(i);
  //     ASSERT_EQ(arrow::bit_util::GetBit(match_bitvector.data(), i), i != 6);
  //   }
  // }
}

}  // namespace compute
}  // namespace arrow
