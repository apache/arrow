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

#include "arrow/compute/row/encode.h"

#include <numeric>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/util.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/light_array.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace compute {

void CheckRowTable(const std::vector<ExecBatch>& batches, const RowTable& row_table,
                   const std::vector<uint32_t>& row_ids) {
  ASSERT_FALSE(batches.empty());
  int num_rows = 0;
  for (const auto& batch : batches) {
    num_rows += static_cast<int>(batch.length);
  }
  int num_columns = batches[0].num_values();
  ASSERT_LE(num_rows, row_table.num_rows());
  for (int i = 0; i < num_columns; i++) {
    ResizableArrayData target;
    target.Init(batches[0].values[i].type(), default_memory_pool(),
                bit_util::Log2(num_rows));
    ASSERT_OK(row_table.DecodeSelected(&target, i, num_rows, row_ids.data(),
                                       default_memory_pool()));
    std::shared_ptr<ArrayData> decoded = target.array_data();
    std::vector<std::shared_ptr<Array>> expected_arrays;
    for (const auto& batch : batches) {
      expected_arrays.push_back(batch.values[i].make_array());
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> expected_array,
                         Concatenate(expected_arrays));
    AssertArraysEqual(*expected_array, *MakeArray(decoded));
  }
}

Result<int> EncodeEntireBatches(RowTable* row_table,
                                const std::vector<ExecBatch>& batches) {
  std::vector<KeyColumnArray> temp_column_arrays;
  int num_rows_encoded = 0;
  for (const auto& batch : batches) {
    int num_rows = static_cast<int>(batch.length);
    std::vector<uint16_t> in_row_ids(num_rows);
    std::iota(in_row_ids.begin(), in_row_ids.end(), 0);
    RETURN_NOT_OK(row_table->InitIfNeeded(default_memory_pool(), batch));
    RETURN_NOT_OK(row_table->AppendBatchSelection(default_memory_pool(), batch, 0,
                                                  num_rows, num_rows, in_row_ids.data(),
                                                  temp_column_arrays));
    num_rows_encoded += num_rows;
  }
  return num_rows_encoded;
}

void CheckRoundTrip(const ExecBatch& batch, std::vector<uint16_t> row_ids = {}) {
  if (row_ids.empty()) {
    row_ids.resize(batch.length);
    std::iota(row_ids.begin(), row_ids.end(), 0);
  }
  std::vector<uint32_t> array_row_ids(row_ids.size());
  std::iota(array_row_ids.begin(), array_row_ids.end(), 0);

  RowTable row_table;
  ASSERT_OK(EncodeEntireBatches(&row_table, {batch}));
  CheckRowTable({batch}, row_table, array_row_ids);
}

TEST(RowTable, Basic) {
  // All fixed
  CheckRoundTrip(ExecBatchFromJSON({int32(), int32(), int32()}, R"([
                   [1, 4, 7],
                   [2, 5, 8],
                   [3, 6, 9]
                 ])"));
  // All varlen
  CheckRoundTrip(ExecBatchFromJSON({utf8(), utf8()}, R"([
                   ["xyz", "longer string"],
                   ["really even longer string", ""],
                   [null, "a"],
                   ["null", null],
                   ["b", "c"]
                 ])"));
  // Mixed types
  CheckRoundTrip(ExecBatchFromJSON({boolean(), int32(), utf8()}, R"([
    [true, 3, "test"],
    [null, null, null],
    [false, 0, "blah"]
  ])"));
  // No nulls
  CheckRoundTrip(ExecBatchFromJSON({int16(), utf8()}, R"([
    [10, "blahblah"],
    [8, "not-null"]
  ])"));
}

TEST(RowTable, MultiBatch) {
  constexpr int kRowsPerBatch = std::numeric_limits<uint16_t>::max();
  constexpr int num_batches = 4;
  constexpr int num_out_rows = kRowsPerBatch * num_batches;
  std::vector<uint16_t> in_row_ids(kRowsPerBatch);
  std::vector<uint32_t> out_row_ids(num_out_rows);
  std::iota(in_row_ids.begin(), in_row_ids.end(), 0);
  std::iota(out_row_ids.begin(), out_row_ids.end(), 0);
  // Should be able to encode multiple batches to something
  // greater than 2^16 rows
  BatchesWithSchema test_data = MakeRandomBatches(
      schema({field("bool", boolean()), field("i8", int8()), field("utf8", utf8())}), 4,
      kRowsPerBatch, /*add_tag=*/false);

  RowTable row_table;
  ASSERT_OK_AND_ASSIGN(int num_rows_encoded,
                       EncodeEntireBatches(&row_table, test_data.batches));
  ASSERT_EQ(num_rows_encoded, row_table.num_rows());

  CheckRowTable(test_data.batches, row_table, out_row_ids);
}

Result<ExecBatch> ColumnTake(const ExecBatch& input,
                             const std::vector<uint16_t>& row_ids) {
  UInt16Builder take_indices_builder;
  RETURN_NOT_OK(take_indices_builder.AppendValues(row_ids));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> take_indices,
                        take_indices_builder.Finish());

  std::vector<Datum> taken_arrays(input.num_values());
  for (int i = 0; i < input.num_values(); i++) {
    ARROW_ASSIGN_OR_RAISE(Datum taken_array, Take(input.values[i], take_indices));
    taken_arrays[i] = taken_array;
  }
  return ExecBatch(taken_arrays, static_cast<int64_t>(row_ids.size()));
}

TEST(RowTable, InputPartialSelection) {
  constexpr int num_rows = std::numeric_limits<uint16_t>::max();
  BatchesWithSchema test_data = MakeRandomBatches(
      schema({field("bool", boolean()), field("i8", int8()), field("utf8", utf8())}), 1,
      num_rows, /*add_tag=*/false);
  ExecBatch batch = test_data.batches[0];
  std::vector<uint16_t> row_ids;
  std::default_random_engine gen(42);
  std::uniform_int_distribution<> dist(0, 1);
  for (uint16_t i = 0; i < num_rows; i++) {
    if (dist(gen)) {
      row_ids.push_back(i);
    }
  }
  int num_selected_rows = static_cast<int>(row_ids.size());
  ASSERT_GT(num_selected_rows, 0);
  ASSERT_LT(num_selected_rows, num_rows);

  std::vector<uint32_t> array_row_ids(row_ids.size());
  std::iota(array_row_ids.begin(), array_row_ids.end(), 0);

  RowTable row_table;
  std::vector<KeyColumnArray> temp_column_arrays;
  ASSERT_OK(row_table.InitIfNeeded(default_memory_pool(), batch));
  ASSERT_OK(row_table.AppendBatchSelection(default_memory_pool(), batch, 0,
                                           num_selected_rows, num_selected_rows,
                                           row_ids.data(), temp_column_arrays));

  ASSERT_OK_AND_ASSIGN(ExecBatch expected, ColumnTake(batch, row_ids));
  CheckRowTable({expected}, row_table, array_row_ids);
}

TEST(RowTable, ThreadedDecode) {
  // Create kBatchesPerThread batches per thread.  Encode all batches from the main
  // thread. Then decode in parallel so each thread decodes it's own batches
  constexpr int kRowsPerBatch = std::numeric_limits<uint16_t>::max();
  constexpr int kNumThreads = 16;
  constexpr int kBatchesPerThread = 2;
  constexpr int kNumBatches = kNumThreads * kBatchesPerThread;
  constexpr int kNumOutRows = kRowsPerBatch * kBatchesPerThread;

  BatchesWithSchema test_data = MakeRandomBatches(
      schema({field("bool", boolean()), field("i8", int8()), field("utf8", utf8())}),
      kNumBatches, kRowsPerBatch, /*add_tag=*/false);

  RowTable row_table;
  ASSERT_OK_AND_ASSIGN(int num_rows_encoded,
                       EncodeEntireBatches(&row_table, test_data.batches));
  ASSERT_EQ(num_rows_encoded, kNumOutRows * kNumThreads);

  std::vector<std::vector<uint32_t>> row_ids_for_threads(kNumThreads);
  uint32_t row_id_offset = 0;
  for (int i = 0; i < kNumThreads; i++) {
    std::vector<uint32_t>& row_ids_for_thread = row_ids_for_threads[i];
    row_ids_for_thread.resize(kNumOutRows);
    std::iota(row_ids_for_thread.begin(), row_ids_for_thread.end(), row_id_offset);
    row_id_offset += kNumOutRows;
  }

  std::vector<std::thread> thread_tasks;
  for (int i = 0; i < kNumThreads; i++) {
    thread_tasks.emplace_back([&, i] {
      CheckRowTable({test_data.batches[i * 2], test_data.batches[i * 2 + 1]}, row_table,
                    row_ids_for_threads[i]);
    });
  }
  for (auto& thread_task : thread_tasks) {
    thread_task.join();
  }
}

void CheckComparison(const ExecBatch& left, const ExecBatch& right,
                     const std::vector<bool> expected) {
  RowTable row_table;
  ASSERT_OK(EncodeEntireBatches(&row_table, {left}));
  std::vector<uint32_t> row_ids(right.length);
  std::iota(row_ids.begin(), row_ids.end(), 0);
  uint32_t num_not_equal = 0;
  std::vector<uint16_t> not_equal_selection(right.length);
  LightContext light_context;
  light_context.hardware_flags =
      arrow::internal::CpuInfo::GetInstance()->hardware_flags();
  util::TempVectorStack temp_stack;
  ASSERT_OK(temp_stack.Init(default_memory_pool(),
                            4 * util::MiniBatch::kMiniBatchLength * sizeof(uint32_t)));
  light_context.stack = &temp_stack;
  std::vector<KeyColumnArray> temp_column_arrays;
  std::vector<uint8_t> match_bit_vector(bit_util::Log2(right.length));
  row_table.Compare(
      right, 0, static_cast<int>(right.length), static_cast<int>(right.length), nullptr,
      row_ids.data(), &num_not_equal, not_equal_selection.data(),
      light_context.hardware_flags, light_context.stack, temp_column_arrays, nullptr);

  // We over-allocated above
  not_equal_selection.resize(num_not_equal);
  std::vector<uint16_t> expected_not_equal_selection;
  for (uint16_t i = 0; i < static_cast<uint16_t>(expected.size()); i++) {
    if (!expected[i]) {
      expected_not_equal_selection.push_back(i);
    }
  }
  ASSERT_EQ(expected_not_equal_selection, not_equal_selection);

  int expected_not_equal = 0;
  for (bool val : expected) {
    if (!val) {
      expected_not_equal++;
    }
  }
  ASSERT_EQ(expected_not_equal, num_not_equal);
}

TEST(RowTable, Compare) {
  ExecBatch left = ExecBatchFromJSON({int16(), utf8()}, R"([
    [1, "blahblah"],
    [2, null],
    [3, "xyz"],
    [4, "sample"],
    [null, "5"],
    [null, null]
  ])");
  ExecBatch right = ExecBatchFromJSON({int16(), utf8()}, R"([
    [1, "blahblah"],
    [2, "not-null"],
    [3, "abc"],
    [5, "blah"],
    [null, "5"],
    [null, null]
  ])");
  std::vector<bool> expected_matches = {
      true,   // Matches exactly without nulls
      false,  // differs by null only
      false,  // differs by one value
      false,  // differs by all values
      true,   // equal but has nulls
      true    // equal-all-null
  };
  // Test in both directions
  CheckComparison(left, right, expected_matches);
  CheckComparison(right, left, expected_matches);
}

}  // namespace compute
}  // namespace arrow
