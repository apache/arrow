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

#include "arrow/compute/row/compare_internal.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/bitmap_ops.h"

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

// Compare columns to rows at offsets over 2GB within a row table.
// Certain AVX2 instructions may behave unexpectedly causing troubles like GH-41813.
TEST(KeyCompare, CompareColumnsToRowsLarge) {
  if constexpr (sizeof(void*) == 4) {
    GTEST_SKIP() << "Test only works on 64-bit platforms";
  }

  // The idea of this case is to create a row table using several fixed length columns and
  // one var length column (so the row is hence var length and has offset buffer), with
  // the overall data size exceeding 2GB. Then compare each row with itself.
  constexpr int64_t two_gb = 2ll * 1024ll * 1024ll * 1024ll;
  // The compare function requires the row id of the left column to be uint16_t, hence the
  // number of rows.
  constexpr int64_t num_rows = std::numeric_limits<uint16_t>::max() + 1;
  const std::vector<std::shared_ptr<DataType>> fixed_length_types{uint64(), uint32()};
  // The var length column should be a little smaller than 2GB to WAR the capacity
  // limitation in the var length builder.
  constexpr int32_t var_length = two_gb / num_rows - 1;
  auto row_size =
      std::accumulate(fixed_length_types.begin(), fixed_length_types.end(), var_length,
                      [](int64_t acc, const std::shared_ptr<DataType>& type) {
                        return acc + type->byte_width();
                      });
  // The overall size should be larger than 2GB.
  ASSERT_GT(row_size * num_rows, two_gb);

  MemoryPool* pool = default_memory_pool();
  TempVectorStack stack;
  ASSERT_OK(stack.Init(pool, KeyCompare::CompareColumnsToRowsTempStackUsage(num_rows)));

  std::vector<Datum> columns;
  {
    // Several fixed length arrays containing random content.
    for (const auto& type : fixed_length_types) {
      ASSERT_OK_AND_ASSIGN(auto column, ::arrow::gen::Random(type)->Generate(num_rows));
      columns.push_back(std::move(column));
    }
    // A var length array containing 'X' repeated var_length times.
    ASSERT_OK_AND_ASSIGN(auto column_var_length,
                         ::arrow::gen::Constant(
                             std::make_shared<BinaryScalar>(std::string(var_length, 'X')))
                             ->Generate(num_rows));
    columns.push_back(std::move(column_var_length));
  }
  ExecBatch batch(std::move(columns), num_rows);

  std::vector<KeyColumnMetadata> column_metadatas;
  ASSERT_OK(ColumnMetadatasFromExecBatch(batch, &column_metadatas));
  std::vector<KeyColumnArray> column_arrays;
  ASSERT_OK(ColumnArraysFromExecBatch(batch, &column_arrays));

  // The row table (right side).
  RowTableMetadata table_metadata_right;
  table_metadata_right.FromColumnMetadataVector(column_metadatas, sizeof(uint64_t),
                                                sizeof(uint64_t));
  RowTableImpl row_table;
  ASSERT_OK(row_table.Init(pool, table_metadata_right));
  std::vector<uint16_t> row_ids_right(num_rows);
  std::iota(row_ids_right.begin(), row_ids_right.end(), 0);
  RowTableEncoder row_encoder;
  row_encoder.Init(column_metadatas, sizeof(uint64_t), sizeof(uint64_t));
  row_encoder.PrepareEncodeSelected(0, num_rows, column_arrays);
  ASSERT_OK(row_encoder.EncodeSelected(&row_table, static_cast<uint32_t>(num_rows),
                                       row_ids_right.data()));

  ASSERT_TRUE(row_table.offsets());
  // The whole point of this test.
  ASSERT_GT(row_table.offsets()[num_rows - 1], two_gb);

  // The left rows.
  std::vector<uint32_t> row_ids_left(num_rows);
  std::iota(row_ids_left.begin(), row_ids_left.end(), 0);

  LightContext ctx{CpuInfo::GetInstance()->hardware_flags(), &stack};

  {
    // No selection, output no match row ids.
    uint32_t num_rows_no_match;
    std::vector<uint16_t> row_ids_out(num_rows);
    KeyCompare::CompareColumnsToRows(
        num_rows, /*sel_left_maybe_null=*/NULLPTR, row_ids_left.data(), &ctx,
        &num_rows_no_match, row_ids_out.data(), column_arrays, row_table, true, NULLPTR);
    ASSERT_EQ(num_rows_no_match, 0);
  }

  {
    // With selection, output no match row ids.
    uint32_t num_rows_no_match;
    std::vector<uint16_t> row_ids_out(num_rows);
    std::vector<uint16_t> selection_left(num_rows);
    std::iota(selection_left.begin(), selection_left.end(), 0);
    KeyCompare::CompareColumnsToRows(num_rows, selection_left.data(), row_ids_left.data(),
                                     &ctx, &num_rows_no_match, row_ids_out.data(),
                                     column_arrays, row_table, true, NULLPTR);
    ASSERT_EQ(num_rows_no_match, 0);
  }

  {
    // No selection, output match bit vector.
    std::vector<uint8_t> match_bitvector(BytesForBits(num_rows));
    KeyCompare::CompareColumnsToRows(
        num_rows, /*sel_left_maybe_null=*/NULLPTR, row_ids_left.data(), &ctx, NULLPTR,
        NULLPTR, column_arrays, row_table, true, match_bitvector.data());
    ASSERT_EQ(arrow::internal::CountSetBits(match_bitvector.data(), 0, num_rows),
              num_rows);
  }

  {
    // With selection, output match bit vector.
    std::vector<uint8_t> match_bitvector(BytesForBits(num_rows));
    std::vector<uint16_t> selection_left(num_rows);
    std::iota(selection_left.begin(), selection_left.end(), 0);
    KeyCompare::CompareColumnsToRows(num_rows, NULLPTR, row_ids_left.data(), &ctx,
                                     NULLPTR, NULLPTR, column_arrays, row_table, true,
                                     match_bitvector.data());
    ASSERT_EQ(arrow::internal::CountSetBits(match_bitvector.data(), 0, num_rows),
              num_rows);
  }
}

}  // namespace compute
}  // namespace arrow
