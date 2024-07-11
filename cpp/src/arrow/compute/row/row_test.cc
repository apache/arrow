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
// #include "arrow/testing/random.h"

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

  ASSERT_OK_AND_ASSIGN(
      auto fixed_length_column,
      ::arrow::gen::Constant(std::make_shared<UInt32Scalar>(0))->Generate(num_rows_max));
  ASSERT_OK_AND_ASSIGN(auto var_length_column,
                       ::arrow::gen::Constant(std::make_shared<BinaryScalar>("X"))
                           ->Generate(num_rows_max));

  for (int64_t num_rows : {1023, 1024, 1025, 4095, 4096, 4097}) {
    // Fixed length column.
    {
      SCOPED_TRACE("encoding fixed length column of " + std::to_string(num_rows) +
                   " rows");
      ASSERT_OK_AND_ASSIGN(auto row_table,
                           MakeRowTableFromColumn(fixed_length_column, num_rows,
                                                  uint32()->byte_width(), 0));
      ASSERT_NE(row_table.data(0), NULLPTR);
      ASSERT_NE(row_table.data(1), NULLPTR);
      ASSERT_EQ(row_table.data(2), NULLPTR);

      int64_t actual_null_mask_size =
          num_rows * row_table.metadata().null_masks_bytes_per_row;
      ASSERT_LE(actual_null_mask_size, row_table.buffer_size(0) - padding_for_vectors);
      ASSERT_GT(actual_null_mask_size * 2,
                row_table.buffer_size(0) - padding_for_vectors);

      int64_t actual_rows_size = num_rows * uint32()->byte_width();
      ASSERT_LE(actual_rows_size, row_table.buffer_size(1) - padding_for_vectors);
      ASSERT_GT(actual_rows_size * 2, row_table.buffer_size(1) - padding_for_vectors);
    }

    // Var length column.
    {
      SCOPED_TRACE("encoding var length column of " + std::to_string(num_rows) + " rows");
      ASSERT_OK_AND_ASSIGN(auto row_table,
                           MakeRowTableFromColumn(var_length_column, num_rows, 4, 4));
      ASSERT_NE(row_table.data(0), NULLPTR);
      ASSERT_NE(row_table.data(1), NULLPTR);
      ASSERT_NE(row_table.data(2), NULLPTR);

      int64_t actual_null_mask_size =
          num_rows * row_table.metadata().null_masks_bytes_per_row;
      ASSERT_LE(actual_null_mask_size, row_table.buffer_size(0) - padding_for_vectors);
      ASSERT_GT(actual_null_mask_size * 2,
                row_table.buffer_size(0) - padding_for_vectors);

      int64_t actual_offset_size = num_rows * sizeof(uint32_t);
      ASSERT_LE(actual_offset_size, row_table.buffer_size(1) - padding_for_vectors);
      ASSERT_GT(actual_offset_size * 2, row_table.buffer_size(1) - padding_for_vectors);

      int64_t actual_rows_size = num_rows * row_table.offsets()[1];
      ASSERT_LE(actual_rows_size, row_table.buffer_size(2) - padding_for_vectors);
      ASSERT_GT(actual_rows_size * 2, row_table.buffer_size(2) - padding_for_vectors);
    }
  }
}

TEST(RowTableOffsetOverflow, LARGE_MEMORY_TEST(Encode)) {
  if constexpr (sizeof(void*) == 4) {
    GTEST_SKIP() << "Test only works on 64-bit platforms";
  }

  constexpr int64_t length_per_binary = 512 * 1024 * 1024;
  constexpr int64_t num_rows = 8;

  MemoryPool* pool = default_memory_pool();

  std::vector<KeyColumnArray> columns;
  std::vector<Datum> values;
  ASSERT_OK_AND_ASSIGN(
      auto value, ::arrow::gen::Constant(
                      std::make_shared<BinaryScalar>(std::string(length_per_binary, 'X')))
                      ->Generate(1));
  values.push_back(std::move(value));
  ExecBatch batch = ExecBatch(std::move(values), 1);
  ASSERT_OK(ColumnArraysFromExecBatch(batch, &columns));

  std::vector<KeyColumnMetadata> column_metadatas;
  ASSERT_OK(ColumnMetadatasFromExecBatch(batch, &column_metadatas));
  RowTableMetadata table_metadata;
  table_metadata.FromColumnMetadataVector(column_metadatas, sizeof(uint64_t),
                                          sizeof(uint64_t));
  RowTableImpl row_table;
  ASSERT_OK(row_table.Init(pool, table_metadata));
  std::vector<uint16_t> row_ids(num_rows, 0);
  RowTableEncoder row_encoder;
  row_encoder.Init(column_metadatas, sizeof(uint64_t), sizeof(uint64_t));
  row_encoder.PrepareEncodeSelected(0, num_rows, columns);
  ASSERT_OK(row_encoder.EncodeSelected(&row_table, static_cast<uint32_t>(num_rows),
                                       row_ids.data()));
}

// TEST(RowTableOffsetOverflow, LARGE_MEMORY_TEST(Encode)) {
//   if constexpr (sizeof(void*) == 4) {
//     GTEST_SKIP() << "Test only works on 64-bit platforms";
//   }

//   // The idea of this case is to create a row table containing one fixed length column
//   and
//   // one var length column (so the row is hence var length and has offset buffer), by
//   // appending the same small batch of n rows repeatedly until it has more than 2^31
//   rows.
//   // Then compare the last n rows of the row table with the batch.
//   constexpr int64_t num_rows_batch = std::numeric_limits<uint16_t>::max();
//   constexpr int64_t num_rows_row_table = 30000 * num_rows_batch;
//   // (std::numeric_limits<int32_t>::max() + 1ll) / num_rows_batch * num_rows_batch -
//   // num_rows_batch;

//   MemoryPool* pool = default_memory_pool();

//   // The left side columns with num_rows_batch rows.
//   std::vector<KeyColumnArray> columns_left;
//   ExecBatch batch_left;
//   {
//     std::vector<Datum> values;

//     // A fixed length array containing random values.
//     ASSERT_OK_AND_ASSIGN(auto value_fixed_length,
//                          ::arrow::gen::Random(uint32())->Generate(num_rows_batch));
//     values.push_back(std::move(value_fixed_length));

//     // A var length array containing small var length values ("X").
//     ASSERT_OK_AND_ASSIGN(auto value_var_length,
//                          ::arrow::gen::Constant(std::make_shared<BinaryScalar>("X"))
//                              ->Generate(num_rows_batch));
//     values.push_back(std::move(value_var_length));

//     batch_left = ExecBatch(std::move(values), num_rows_batch);
//     ASSERT_OK(ColumnArraysFromExecBatch(batch_left, &columns_left));
//   }

//   // The right side row table with num_rows_row_table rows.
//   RowTableImpl row_table_right;
//   {
//     // Encode the row table with the left columns repeatedly.
//     std::vector<KeyColumnMetadata> column_metadatas;
//     ASSERT_OK(ColumnMetadatasFromExecBatch(batch_left, &column_metadatas));
//     RowTableMetadata table_metadata;
//     table_metadata.FromColumnMetadataVector(column_metadatas, sizeof(uint64_t),
//                                             sizeof(uint64_t));
//     ASSERT_OK(row_table_right.Init(pool, table_metadata));
//     RowTableImpl row_table_batch;
//     ASSERT_OK(row_table_batch.Init(pool, table_metadata));
//     std::vector<uint16_t> row_ids(num_rows_batch);
//     std::iota(row_ids.begin(), row_ids.end(), 0);
//     RowTableEncoder row_encoder;
//     row_encoder.Init(column_metadatas, sizeof(uint64_t), sizeof(uint64_t));
//     row_encoder.PrepareEncodeSelected(0, num_rows_batch, columns_left);
//     ASSERT_OK(row_encoder.EncodeSelected(
//         &row_table_batch, static_cast<uint32_t>(num_rows_batch), row_ids.data()));
//     for (int i = 0; i < num_rows_row_table / num_rows_batch; ++i) {
//       ASSERT_OK(row_table_right.AppendSelectionFrom(row_table_batch, num_rows_batch,
//                                                     /*source_row_ids=*/NULLPTR));
//     }

//     // The row table must contain an offset buffer.
//     ASSERT_NE(row_table_right.offsets(), NULLPTR);
//     ASSERT_EQ(row_table_right.length(), num_rows_row_table);
//   }

//   // The rows to compare: all rows in the batch to the last num_rows_batch rows of the
//   // row table.
//   std::vector<uint32_t> row_ids_to_compare(num_rows_batch);
//   std::iota(row_ids_to_compare.begin(), row_ids_to_compare.end(),
//             num_rows_row_table - num_rows_batch);

//   TempVectorStack stack;
//   ASSERT_OK(
//       stack.Init(pool,
//       KeyCompare::CompareColumnsToRowsTempStackUsage(num_rows_batch)));
//   LightContext ctx{CpuInfo::GetInstance()->hardware_flags(), &stack};

//   {
//     // No selection, output no match row ids.
//     uint32_t num_rows_no_match;
//     std::vector<uint16_t> row_ids_out(num_rows_batch);
//     KeyCompare::CompareColumnsToRows(num_rows_batch, /*sel_left_maybe_null=*/NULLPTR,
//                                      row_ids_to_compare.data(), &ctx,
//                                      &num_rows_no_match, row_ids_out.data(),
//                                      columns_left, row_table_right,
//                                      /*are_cols_in_encoding_order=*/true,
//                                      /*out_match_bitvector_maybe_null=*/NULLPTR);
//     ASSERT_EQ(num_rows_no_match, 0);
//   }

//   {
//     // No selection, output match bit vector.
//     std::vector<uint8_t> match_bitvector(BytesForBits(num_rows_batch));
//     KeyCompare::CompareColumnsToRows(
//         num_rows_batch, /*sel_left_maybe_null=*/NULLPTR, row_ids_to_compare.data(),
//         &ctx,
//         /*out_num_rows=*/NULLPTR, /*out_sel_left_maybe_same=*/NULLPTR, columns_left,
//         row_table_right,
//         /*are_cols_in_encoding_order=*/true, match_bitvector.data());
//     ASSERT_EQ(arrow::internal::CountSetBits(match_bitvector.data(), 0, num_rows_batch),
//               num_rows_batch);
//   }

//   std::vector<uint16_t> selection_left(num_rows_batch);
//   std::iota(selection_left.begin(), selection_left.end(), 0);

//   {
//     // With selection, output no match row ids.
//     uint32_t num_rows_no_match;
//     std::vector<uint16_t> row_ids_out(num_rows_batch);
//     KeyCompare::CompareColumnsToRows(num_rows_batch, selection_left.data(),
//                                      row_ids_to_compare.data(), &ctx,
//                                      &num_rows_no_match, row_ids_out.data(),
//                                      columns_left, row_table_right,
//                                      /*are_cols_in_encoding_order=*/true,
//                                      /*out_match_bitvector_maybe_null=*/NULLPTR);
//     ASSERT_EQ(num_rows_no_match, 0);
//   }

//   {
//     // With selection, output match bit vector.
//     std::vector<uint8_t> match_bitvector(BytesForBits(num_rows_batch));
//     KeyCompare::CompareColumnsToRows(
//         num_rows_batch, selection_left.data(), row_ids_to_compare.data(), &ctx,
//         /*out_num_rows=*/NULLPTR, /*out_sel_left_maybe_same=*/NULLPTR, columns_left,
//         row_table_right,
//         /*are_cols_in_encoding_order=*/true, match_bitvector.data());
//     ASSERT_EQ(arrow::internal::CountSetBits(match_bitvector.data(), 0, num_rows_batch),
//               num_rows_batch);
//   }
// }

}  // namespace compute
}  // namespace arrow
