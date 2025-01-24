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

#include "arrow/compute/row/encode_internal.h"
#include "arrow/compute/row/row_internal.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace compute {

namespace {

Result<RowTableImpl> MakeRowTableFromColumn(const std::shared_ptr<Array>& column,
                                            int64_t num_rows, int row_alignment,
                                            int string_alignment) {
  DCHECK_GE(column->length(), num_rows);
  MemoryPool* pool = default_memory_pool();

  std::vector<KeyColumnArray> column_arrays;
  std::vector<Datum> values{column};
  ExecBatch batch(std::move(values), num_rows);
  RETURN_NOT_OK(ColumnArraysFromExecBatch(batch, &column_arrays));

  std::vector<KeyColumnMetadata> column_metadatas;
  RETURN_NOT_OK(ColumnMetadatasFromExecBatch(batch, &column_metadatas));
  RowTableMetadata table_metadata;
  table_metadata.FromColumnMetadataVector(column_metadatas, row_alignment,
                                          string_alignment);

  RowTableImpl row_table;
  RETURN_NOT_OK(row_table.Init(pool, table_metadata));

  RowTableEncoder row_encoder;
  row_encoder.Init(column_metadatas, row_alignment, string_alignment);
  row_encoder.PrepareEncodeSelected(0, num_rows, column_arrays);

  std::vector<uint16_t> row_ids(num_rows);
  std::iota(row_ids.begin(), row_ids.end(), 0);

  RETURN_NOT_OK(row_encoder.EncodeSelected(&row_table, static_cast<uint32_t>(num_rows),
                                           row_ids.data()));

  return row_table;
}

}  // namespace

// GH-43129: Ensure that the memory consumption of the row table is reasonable, that is,
// with the growth factor of 2, the actual memory usage does not exceed twice the amount
// of memory actually needed.
TEST(RowTableMemoryConsumption, Encode) {
  constexpr int64_t num_rows_max = 8192;
  constexpr int64_t padding_for_vectors = 64;

  std::vector<std::shared_ptr<Array>> fixed_length_columns;
  for (const auto& dt : {int8(), uint16(), int32(), uint64(), fixed_size_binary(16),
                         fixed_size_binary(32)}) {
    ASSERT_OK_AND_ASSIGN(auto fixed_length_column,
                         ::arrow::gen::Random(dt)->Generate(num_rows_max));
    fixed_length_columns.push_back(std::move(fixed_length_column));
  }

  ASSERT_OK_AND_ASSIGN(auto var_length_column,
                       ::arrow::gen::Constant(std::make_shared<BinaryScalar>("X"))
                           ->Generate(num_rows_max));

  for (int64_t num_rows : {1023, 1024, 1025, 4095, 4096, 4097}) {
    // Fixed length column.
    {
      SCOPED_TRACE("encoding fixed length column of " + std::to_string(num_rows) +
                   " rows");
      for (const auto& col : fixed_length_columns) {
        const auto& dt = col->type();
        SCOPED_TRACE("encoding fixed length column of type " + dt->ToString());
        ASSERT_OK_AND_ASSIGN(auto row_table,
                             MakeRowTableFromColumn(col, num_rows, dt->byte_width(),
                                                    /*string_alignment=*/0));
        ASSERT_NE(row_table.null_masks(/*row_id=*/0), NULLPTR);
        ASSERT_NE(row_table.fixed_length_rows(/*row_id=*/0), NULLPTR);

        int64_t actual_null_mask_size =
            num_rows * row_table.metadata().null_masks_bytes_per_row;
        ASSERT_LE(actual_null_mask_size, row_table.buffer_size(0) - padding_for_vectors);
        ASSERT_GT(actual_null_mask_size * 2,
                  row_table.buffer_size(0) - padding_for_vectors);

        int64_t actual_rows_size = num_rows * dt->byte_width();
        ASSERT_LE(actual_rows_size, row_table.buffer_size(1) - padding_for_vectors);
        ASSERT_GT(actual_rows_size * 2, row_table.buffer_size(1) - padding_for_vectors);
      }
    }

    // Var length column.
    {
      SCOPED_TRACE("encoding var length column of " + std::to_string(num_rows) + " rows");
      ASSERT_OK_AND_ASSIGN(auto row_table,
                           MakeRowTableFromColumn(var_length_column, num_rows, 4, 4));
      ASSERT_NE(row_table.null_masks(/*row_id=*/0), NULLPTR);
      ASSERT_NE(row_table.offsets(), NULLPTR);
      ASSERT_NE(row_table.var_length_rows(), NULLPTR);

      int64_t actual_null_mask_size =
          num_rows * row_table.metadata().null_masks_bytes_per_row;
      ASSERT_LE(actual_null_mask_size, row_table.buffer_size(0) - padding_for_vectors);
      ASSERT_GT(actual_null_mask_size * 2,
                row_table.buffer_size(0) - padding_for_vectors);

      int64_t actual_offset_size = num_rows * sizeof(RowTableImpl::offset_type);
      ASSERT_LE(actual_offset_size, row_table.buffer_size(1) - padding_for_vectors);
      ASSERT_GT(actual_offset_size * 2, row_table.buffer_size(1) - padding_for_vectors);

      int64_t actual_rows_size = num_rows * row_table.offsets()[1];
      ASSERT_LE(actual_rows_size, row_table.buffer_size(2) - padding_for_vectors);
      ASSERT_GT(actual_rows_size * 2, row_table.buffer_size(2) - padding_for_vectors);
    }
  }
}

// GH-43495: Ensure that we can build a row table with more than 4GB row data.
TEST(RowTableLarge, LARGE_MEMORY_TEST(Encode)) {
  if constexpr (sizeof(void*) == 4) {
    GTEST_SKIP() << "Test only works on 64-bit platforms";
  }

  // Use 9 512MB var-length rows to occupy more than 4GB memory.
  constexpr int64_t num_rows = 9;
  constexpr int64_t length_per_binary = 512 * 1024 * 1024;
  constexpr int64_t row_alignment = sizeof(uint32_t);
  constexpr int64_t var_length_alignment = sizeof(uint32_t);

  MemoryPool* pool = default_memory_pool();

  // The column to encode.
  std::vector<KeyColumnArray> columns;
  std::vector<Datum> values;
  ASSERT_OK_AND_ASSIGN(
      auto value, ::arrow::gen::Constant(
                      std::make_shared<BinaryScalar>(std::string(length_per_binary, 'X')))
                      ->Generate(1));
  values.emplace_back(std::move(value));
  ExecBatch batch = ExecBatch(std::move(values), 1);
  ASSERT_OK(ColumnArraysFromExecBatch(batch, &columns));

  // The row table.
  std::vector<KeyColumnMetadata> column_metadatas;
  ASSERT_OK(ColumnMetadatasFromExecBatch(batch, &column_metadatas));
  RowTableMetadata table_metadata;
  table_metadata.FromColumnMetadataVector(column_metadatas, row_alignment,
                                          var_length_alignment);
  RowTableImpl row_table;
  ASSERT_OK(row_table.Init(pool, table_metadata));
  RowTableEncoder row_encoder;
  row_encoder.Init(column_metadatas, row_alignment, var_length_alignment);

  // The rows to encode.
  std::vector<uint16_t> row_ids(num_rows, 0);

  // Encode num_rows rows.
  row_encoder.PrepareEncodeSelected(0, num_rows, columns);
  ASSERT_OK(row_encoder.EncodeSelected(&row_table, static_cast<uint32_t>(num_rows),
                                       row_ids.data()));

  auto encoded_row_length = table_metadata.fixed_length + length_per_binary;
  ASSERT_EQ(row_table.offsets()[num_rows - 1], encoded_row_length * (num_rows - 1));
  ASSERT_EQ(row_table.offsets()[num_rows], encoded_row_length * num_rows);
}

// GH-43495: Ensure that we can build a row table with more than 4GB row data.
TEST(RowTableLarge, LARGE_MEMORY_TEST(AppendFrom)) {
  if constexpr (sizeof(void*) == 4) {
    GTEST_SKIP() << "Test only works on 64-bit platforms";
  }

  // Use 9 512MB var-length rows to occupy more than 4GB memory.
  constexpr int64_t num_rows = 9;
  constexpr int64_t length_per_binary = 512 * 1024 * 1024;
  constexpr int64_t num_rows_seed = 1;
  constexpr int64_t row_alignment = sizeof(uint32_t);
  constexpr int64_t var_length_alignment = sizeof(uint32_t);

  MemoryPool* pool = default_memory_pool();

  // The column to encode.
  std::vector<KeyColumnArray> columns;
  std::vector<Datum> values;
  ASSERT_OK_AND_ASSIGN(
      auto value, ::arrow::gen::Constant(
                      std::make_shared<BinaryScalar>(std::string(length_per_binary, 'X')))
                      ->Generate(num_rows_seed));
  values.push_back(std::move(value));
  ExecBatch batch = ExecBatch(std::move(values), num_rows_seed);
  ASSERT_OK(ColumnArraysFromExecBatch(batch, &columns));

  // The seed row table.
  std::vector<KeyColumnMetadata> column_metadatas;
  ASSERT_OK(ColumnMetadatasFromExecBatch(batch, &column_metadatas));
  RowTableMetadata table_metadata;
  table_metadata.FromColumnMetadataVector(column_metadatas, row_alignment,
                                          var_length_alignment);
  RowTableImpl row_table_seed;
  ASSERT_OK(row_table_seed.Init(pool, table_metadata));
  RowTableEncoder row_encoder;
  row_encoder.Init(column_metadatas, row_alignment, var_length_alignment);
  row_encoder.PrepareEncodeSelected(0, num_rows_seed, columns);
  std::vector<uint16_t> row_ids(num_rows_seed, 0);
  ASSERT_OK(row_encoder.EncodeSelected(
      &row_table_seed, static_cast<uint32_t>(num_rows_seed), row_ids.data()));

  // The target row table.
  RowTableImpl row_table;
  ASSERT_OK(row_table.Init(pool, table_metadata));

  // Append seed num_rows times.
  for (int i = 0; i < num_rows; ++i) {
    ASSERT_OK(row_table.AppendSelectionFrom(row_table_seed, num_rows_seed,
                                            /*source_row_ids=*/NULLPTR));
  }

  auto encoded_row_length = table_metadata.fixed_length + length_per_binary;
  ASSERT_EQ(row_table.offsets()[num_rows - 1], encoded_row_length * (num_rows - 1));
  ASSERT_EQ(row_table.offsets()[num_rows], encoded_row_length * num_rows);
}

}  // namespace compute
}  // namespace arrow
