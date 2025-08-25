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
using arrow::bit_util::GetBit;
using arrow::gen::Constant;
using arrow::gen::Random;
using arrow::internal::CountSetBits;
using arrow::internal::CpuInfo;
using arrow::random::kSeedMax;
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
      ASSERT_EQ(GetBit(match_bitvector.data(), i), i != 6);
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

namespace {

Result<RowTableImpl> MakeRowTableFromExecBatch(const ExecBatch& batch) {
  RowTableImpl row_table;

  std::vector<KeyColumnMetadata> column_metadatas;
  RETURN_NOT_OK(ColumnMetadatasFromExecBatch(batch, &column_metadatas));
  RowTableMetadata table_metadata;
  table_metadata.FromColumnMetadataVector(column_metadatas, sizeof(uint64_t),
                                          sizeof(uint64_t));
  RETURN_NOT_OK(row_table.Init(default_memory_pool(), table_metadata));
  std::vector<uint16_t> row_ids(batch.length);
  std::iota(row_ids.begin(), row_ids.end(), 0);
  RowTableEncoder row_encoder;
  row_encoder.Init(column_metadatas, sizeof(uint64_t), sizeof(uint64_t));
  std::vector<KeyColumnArray> column_arrays;
  RETURN_NOT_OK(ColumnArraysFromExecBatch(batch, &column_arrays));
  row_encoder.PrepareEncodeSelected(0, batch.length, column_arrays);
  RETURN_NOT_OK(row_encoder.EncodeSelected(
      &row_table, static_cast<uint32_t>(batch.length), row_ids.data()));

  return row_table;
}

Result<RowTableImpl> RepeatRowTableUntil(const RowTableImpl& seed, int64_t num_rows) {
  RowTableImpl row_table;

  RETURN_NOT_OK(row_table.Init(default_memory_pool(), seed.metadata()));
  // Append the seed row table repeatedly to grow the row table to big enough.
  while (row_table.length() < num_rows) {
    RETURN_NOT_OK(row_table.AppendSelectionFrom(seed,
                                                static_cast<uint32_t>(seed.length()),
                                                /*source_row_ids=*/NULLPTR));
  }

  return row_table;
}

void AssertCompareColumnsToRowsAllMatch(const std::vector<KeyColumnArray>& columns,
                                        const RowTableImpl& row_table,
                                        const std::vector<uint32_t>& row_ids_to_compare) {
  uint32_t num_rows_to_compare = static_cast<uint32_t>(row_ids_to_compare.size());

  TempVectorStack stack;
  ASSERT_OK(
      stack.Init(default_memory_pool(),
                 KeyCompare::CompareColumnsToRowsTempStackUsage(num_rows_to_compare)));
  LightContext ctx{CpuInfo::GetInstance()->hardware_flags(), &stack};

  {
    // No selection, output no match row ids.
    uint32_t num_rows_no_match;
    std::vector<uint16_t> row_ids_out(num_rows_to_compare);
    KeyCompare::CompareColumnsToRows(num_rows_to_compare, /*sel_left_maybe_null=*/NULLPTR,
                                     row_ids_to_compare.data(), &ctx, &num_rows_no_match,
                                     row_ids_out.data(), columns, row_table,
                                     /*are_cols_in_encoding_order=*/true,
                                     /*out_match_bitvector_maybe_null=*/NULLPTR);
    ASSERT_EQ(num_rows_no_match, 0);
  }

  {
    // No selection, output match bit vector.
    std::vector<uint8_t> match_bitvector(BytesForBits(num_rows_to_compare));
    KeyCompare::CompareColumnsToRows(
        num_rows_to_compare, /*sel_left_maybe_null=*/NULLPTR, row_ids_to_compare.data(),
        &ctx,
        /*out_num_rows=*/NULLPTR, /*out_sel_left_maybe_same=*/NULLPTR, columns, row_table,
        /*are_cols_in_encoding_order=*/true, match_bitvector.data());
    ASSERT_EQ(CountSetBits(match_bitvector.data(), 0, num_rows_to_compare),
              num_rows_to_compare);
  }

  std::vector<uint16_t> selection_left(num_rows_to_compare);
  std::iota(selection_left.begin(), selection_left.end(), 0);

  {
    // With selection, output no match row ids.
    uint32_t num_rows_no_match;
    std::vector<uint16_t> row_ids_out(num_rows_to_compare);
    KeyCompare::CompareColumnsToRows(num_rows_to_compare, selection_left.data(),
                                     row_ids_to_compare.data(), &ctx, &num_rows_no_match,
                                     row_ids_out.data(), columns, row_table,
                                     /*are_cols_in_encoding_order=*/true,
                                     /*out_match_bitvector_maybe_null=*/NULLPTR);
    ASSERT_EQ(num_rows_no_match, 0);
  }

  {
    // With selection, output match bit vector.
    std::vector<uint8_t> match_bitvector(BytesForBits(num_rows_to_compare));
    KeyCompare::CompareColumnsToRows(
        num_rows_to_compare, selection_left.data(), row_ids_to_compare.data(), &ctx,
        /*out_num_rows=*/NULLPTR, /*out_sel_left_maybe_same=*/NULLPTR, columns, row_table,
        /*are_cols_in_encoding_order=*/true, match_bitvector.data());
    ASSERT_EQ(CountSetBits(match_bitvector.data(), 0, num_rows_to_compare),
              num_rows_to_compare);
  }
}

}  // namespace

// Compare columns to rows at offsets over 2GB within a row table.
// Certain AVX2 instructions may behave unexpectedly causing troubles like GH-41813.
TEST(KeyCompare, LARGE_MEMORY_TEST(CompareColumnsToRowsOver2GB)) {
  if constexpr (sizeof(void*) == 4) {
    GTEST_SKIP() << "Test only works on 64-bit platforms";
  }

  // The idea of this case is to create a row table using several fixed length columns and
  // one var length column (so the row is hence var length and has offset buffer), with
  // the overall data size exceeding 2GB. Then compare each row with itself.
  constexpr int64_t k2GB = 2ll * 1024ll * 1024ll * 1024ll;
  // The compare function requires the row id of the left column to be uint16_t, hence the
  // number of rows.
  constexpr int64_t num_rows = std::numeric_limits<uint16_t>::max() + 1;
  const std::vector<std::shared_ptr<DataType>> fixed_length_types{uint64(), uint32()};
  // The var length column should be a little smaller than 2GB to workaround the capacity
  // limitation in the var length builder.
  constexpr int32_t var_length = k2GB / num_rows - 1;
  auto row_size = std::accumulate(fixed_length_types.begin(), fixed_length_types.end(),
                                  static_cast<int64_t>(var_length),
                                  [](int64_t acc, const std::shared_ptr<DataType>& type) {
                                    return acc + type->byte_width();
                                  });
  // The overall size should be larger than 2GB.
  ASSERT_GT(row_size * num_rows, k2GB);

  // The left side batch.
  ExecBatch batch_left;
  {
    std::vector<Datum> values;

    // Several fixed length arrays containing random content.
    for (const auto& type : fixed_length_types) {
      ASSERT_OK_AND_ASSIGN(auto value, Random(type)->Generate(num_rows));
      values.push_back(std::move(value));
    }
    // A var length array containing 'X' repeated var_length times.
    ASSERT_OK_AND_ASSIGN(
        auto value_var_length,
        Constant(std::make_shared<BinaryScalar>(std::string(var_length, 'X')))
            ->Generate(num_rows));
    values.push_back(std::move(value_var_length));

    batch_left = ExecBatch(std::move(values), num_rows);
  }

  // The left side columns.
  std::vector<KeyColumnArray> columns_left;
  ASSERT_OK(ColumnArraysFromExecBatch(batch_left, &columns_left));

  // The right side row table.
  ASSERT_OK_AND_ASSIGN(RowTableImpl row_table_right,
                       MakeRowTableFromExecBatch(batch_left));
  // The row table must contain an offset buffer.
  ASSERT_NE(row_table_right.var_length_rows(), NULLPTR);
  // The whole point of this test.
  ASSERT_GT(row_table_right.offsets()[num_rows - 1], k2GB);

  // The rows to compare.
  std::vector<uint32_t> row_ids_to_compare(num_rows);
  std::iota(row_ids_to_compare.begin(), row_ids_to_compare.end(), 0);

  AssertCompareColumnsToRowsAllMatch(columns_left, row_table_right, row_ids_to_compare);
}

// GH-43495: Compare fixed length columns to rows over 4GB within a row table.
TEST(KeyCompare, LARGE_MEMORY_TEST(CompareColumnsToRowsOver4GBFixedLength)) {
  if constexpr (sizeof(void*) == 4) {
    GTEST_SKIP() << "Test only works on 64-bit platforms";
  }

  // The idea of this case is to create a row table using one fixed length column (so the
  // row is hence fixed length), with more than 4GB data. Then compare the rows located at
  // over 4GB.

  // A small batch to append to the row table repeatedly to grow the row table to big
  // enough.
  constexpr int64_t num_rows_batch = std::numeric_limits<uint16_t>::max();
  constexpr int fixed_length = 256;

  // The size of the row table is one batch larger than 4GB, and we'll compare the last
  // num_rows_batch rows.
  constexpr int64_t k4GB = 4ll * 1024 * 1024 * 1024;
  constexpr int64_t num_rows_row_table =
      (k4GB / (fixed_length * num_rows_batch) + 1) * num_rows_batch;
  static_assert(num_rows_row_table < std::numeric_limits<uint32_t>::max(),
                "row table length must be less than uint32 max");
  static_assert(num_rows_row_table * fixed_length > k4GB,
                "row table size must be greater than 4GB");

  // The left side batch with num_rows_batch rows.
  ExecBatch batch_left;
  {
    std::vector<Datum> values;

    // A fixed length array containing random values.
    ASSERT_OK_AND_ASSIGN(
        auto value_fixed_length,
        Random(fixed_size_binary(fixed_length))->Generate(num_rows_batch));
    values.push_back(std::move(value_fixed_length));

    batch_left = ExecBatch(std::move(values), num_rows_batch);
  }

  // The left side columns with num_rows_batch rows.
  std::vector<KeyColumnArray> columns_left;
  ASSERT_OK(ColumnArraysFromExecBatch(batch_left, &columns_left));

  // The right side row table with num_rows_row_table rows.
  ASSERT_OK_AND_ASSIGN(
      RowTableImpl row_table_right,
      RepeatRowTableUntil(MakeRowTableFromExecBatch(batch_left).ValueUnsafe(),
                          num_rows_row_table));
  // The row table must be fixed length.
  ASSERT_TRUE(row_table_right.metadata().is_fixed_length);
  // The row data must be greater than 4GB.
  ASSERT_GT(row_table_right.buffer_size(1), k4GB);

  // The rows to compare: the last num_rows_batch rows in the row table VS. the whole
  // batch.
  std::vector<uint32_t> row_ids_to_compare(num_rows_batch);
  std::iota(row_ids_to_compare.begin(), row_ids_to_compare.end(),
            static_cast<uint32_t>(num_rows_row_table - num_rows_batch));

  AssertCompareColumnsToRowsAllMatch(columns_left, row_table_right, row_ids_to_compare);
}

// GH-43495: Compare var length columns to rows at offset over 4GB within a row table.
TEST(KeyCompare, LARGE_MEMORY_TEST(CompareColumnsToRowsOver4GBVarLength)) {
  if constexpr (sizeof(void*) == 4) {
    GTEST_SKIP() << "Test only works on 64-bit platforms";
  }

  // The idea of this case is to create a row table using one fixed length column and one
  // var length column (so the row is hence var length and has offset buffer), with more
  // than 4GB data. Then compare the rows located at over 4GB.

  // A small batch to append to the row table repeatedly to grow the row table to big
  // enough.
  constexpr int64_t num_rows_batch = std::numeric_limits<uint16_t>::max();
  constexpr int fixed_length = 128;
  // Involve some small randomness in the var length column.
  constexpr int var_length_min = 128;
  constexpr int var_length_max = 129;
  constexpr double null_probability = 0.01;

  // The size of the row table is one batch larger than 4GB, and we'll compare the last
  // num_rows_batch rows.
  constexpr int64_t k4GB = 4ll * 1024 * 1024 * 1024;
  constexpr int64_t size_row_min = fixed_length + var_length_min;
  constexpr int64_t num_rows_row_table =
      (k4GB / (size_row_min * num_rows_batch) + 1) * num_rows_batch;
  static_assert(num_rows_row_table < std::numeric_limits<uint32_t>::max(),
                "row table length must be less than uint32 max");
  static_assert(num_rows_row_table * size_row_min > k4GB,
                "row table size must be greater than 4GB");

  // The left side batch with num_rows_batch rows.
  ExecBatch batch_left;
  {
    std::vector<Datum> values;

    // A fixed length array containing random values.
    ASSERT_OK_AND_ASSIGN(
        auto value_fixed_length,
        Random(fixed_size_binary(fixed_length))->Generate(num_rows_batch));
    values.push_back(std::move(value_fixed_length));

    // A var length array containing random binary of 128 or 129 bytes with small portion
    // of nulls.
    auto value_var_length = RandomArrayGenerator(kSeedMax).String(
        num_rows_batch, var_length_min, var_length_max, null_probability);
    values.push_back(std::move(value_var_length));

    batch_left = ExecBatch(std::move(values), num_rows_batch);
  }

  // The left side columns with num_rows_batch rows.
  std::vector<KeyColumnArray> columns_left;
  ASSERT_OK(ColumnArraysFromExecBatch(batch_left, &columns_left));

  // The right side row table with num_rows_row_table rows.
  ASSERT_OK_AND_ASSIGN(
      RowTableImpl row_table_right,
      RepeatRowTableUntil(MakeRowTableFromExecBatch(batch_left).ValueUnsafe(),
                          num_rows_row_table));
  // The row table must contain an offset buffer.
  ASSERT_NE(row_table_right.var_length_rows(), NULLPTR);
  // At least the last row should be located at over 4GB.
  ASSERT_GT(row_table_right.offsets()[num_rows_row_table - 1], k4GB);

  // The rows to compare: the last num_rows_batch rows in the row table VS. the whole
  // batch.
  std::vector<uint32_t> row_ids_to_compare(num_rows_batch);
  std::iota(row_ids_to_compare.begin(), row_ids_to_compare.end(),
            static_cast<uint32_t>(num_rows_row_table - num_rows_batch));

  AssertCompareColumnsToRowsAllMatch(columns_left, row_table_right, row_ids_to_compare);
}

}  // namespace compute
}  // namespace arrow
