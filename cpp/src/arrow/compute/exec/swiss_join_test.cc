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

#include "arrow/compute/exec/swiss_join.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::Executor;
using internal::GetCpuThreadPool;

namespace compute {

void CheckRowArray(const std::vector<ExecBatch>& batches, const RowArray& row_array,
                   const std::vector<uint32_t>& row_ids) {
  ASSERT_FALSE(batches.empty());
  int num_rows = 0;
  for (const auto& batch : batches) {
    num_rows += static_cast<int>(batch.length);
  }
  int num_columns = batches[0].num_values();
  ASSERT_LE(num_rows, row_array.num_rows());
  for (int i = 0; i < num_columns; i++) {
    ResizableArrayData target;
    target.Init(batches[0].values[i].type(), default_memory_pool(),
                bit_util::Log2(num_rows));
    ASSERT_OK(row_array.DecodeSelected(&target, i, num_rows, row_ids.data(),
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

Result<int> EncodeEntireBatches(RowArray* row_array,
                                const std::vector<ExecBatch>& batches) {
  std::vector<KeyColumnArray> temp_column_arrays;
  int num_rows_encoded = 0;
  for (const auto& batch : batches) {
    int num_rows = static_cast<int>(batch.length);
    std::vector<uint16_t> in_row_ids(num_rows);
    std::iota(in_row_ids.begin(), in_row_ids.end(), 0);
    RETURN_NOT_OK(row_array->InitIfNeeded(default_memory_pool(), batch));
    RETURN_NOT_OK(row_array->AppendBatchSelection(default_memory_pool(), batch, 0,
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

  RowArray row_array;
  ASSERT_OK(EncodeEntireBatches(&row_array, {batch}));
  CheckRowArray({batch}, row_array, array_row_ids);
}

TEST(RowArray, Basic) {
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

TEST(RowArray, MultiBatch) {
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

  RowArray row_array;
  ASSERT_OK_AND_ASSIGN(int num_rows_encoded,
                       EncodeEntireBatches(&row_array, test_data.batches));
  ASSERT_EQ(num_rows_encoded, row_array.num_rows());

  CheckRowArray(test_data.batches, row_array, out_row_ids);
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

TEST(RowArray, InputPartialSelection) {
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

  RowArray row_array;
  std::vector<KeyColumnArray> temp_column_arrays;
  ASSERT_OK(row_array.InitIfNeeded(default_memory_pool(), batch));
  ASSERT_OK(row_array.AppendBatchSelection(default_memory_pool(), batch, 0,
                                           num_selected_rows, num_selected_rows,
                                           row_ids.data(), temp_column_arrays));

  ASSERT_OK_AND_ASSIGN(ExecBatch expected, ColumnTake(batch, row_ids));
  CheckRowArray({expected}, row_array, array_row_ids);
}

TEST(RowArray, ThreadedDecode) {
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

  RowArray row_array;
  ASSERT_OK_AND_ASSIGN(int num_rows_encoded,
                       EncodeEntireBatches(&row_array, test_data.batches));
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
      CheckRowArray({test_data.batches[i * 2], test_data.batches[i * 2 + 1]}, row_array,
                    row_ids_for_threads[i]);
    });
  }
  for (auto& thread_task : thread_tasks) {
    thread_task.join();
  }
}

void CheckComparison(const ExecBatch& left, const ExecBatch& right,
                     const std::vector<bool> expected) {
  RowArray row_array;
  ASSERT_OK(EncodeEntireBatches(&row_array, {left}));
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
  row_array.Compare(
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

TEST(RowArray, Compare) {
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

struct SwissTableTestParams {
  std::shared_ptr<Schema> schema;
  std::string name;
};

std::ostream& operator<<(std::ostream& os, const SwissTableTestParams& params) {
  return os << params.name;
}

class SwissTableWithKeysTest : public ::testing::TestWithParam<SwissTableTestParams> {
 public:
  void SetUp() {
    ASSERT_OK_AND_ASSIGN(ctx_, LightContext::Make(default_memory_pool()));
    ASSERT_OK(table_.Init(ctx_->hardware_flags, ctx_->memory_pool));
  }

  const std::shared_ptr<Schema>& test_schema() { return GetParam().schema; }

 protected:
  SwissTableWithKeys table_;
  // Scratch space to use in tests
  std::unique_ptr<LightContext, LightContext::OwningLightContextDeleter> ctx_;
  std::vector<KeyColumnArray> temp_column_arrays_;
};

TEST_P(SwissTableWithKeysTest, Hash) {
  // Basic test for the hash utility function.  If I hash the same keys I should get the
  // same hashes.  If I hash different keys I should get different hashes.
  constexpr int kBatchLength = 128;
  BatchesWithSchema batches_with_schema =
      MakeRandomBatches(test_schema(),
                        /*num_batches=*/2, kBatchLength, /*add_tag=*/false);
  SwissTableWithKeys::Input input(&batches_with_schema.batches[0], ctx_->stack,
                                  &temp_column_arrays_);

  std::vector<uint32_t> hashes_one(kBatchLength);
  SwissTableWithKeys::Hash(&input, hashes_one.data(), ctx_->hardware_flags);

  std::vector<uint32_t> hashes_two(kBatchLength);
  SwissTableWithKeys::Hash(&input, hashes_two.data(), ctx_->hardware_flags);

  ASSERT_EQ(hashes_one, hashes_two);

  SwissTableWithKeys::Input input_two(&batches_with_schema.batches[1], ctx_->stack,
                                      &temp_column_arrays_);
  std::vector<uint32_t> hashes_three(kBatchLength);
  SwissTableWithKeys::Hash(&input_two, hashes_three.data(), ctx_->hardware_flags);

  // In theory it's possible for all hashes to have collisions but it should be unlikely
  // enough for a unit test
  ASSERT_NE(hashes_one, hashes_three);

  // Original hash values should still be in there
  std::vector<uint32_t> hashes_four(kBatchLength);
  SwissTableWithKeys::Hash(&input, hashes_four.data(), ctx_->hardware_flags);

  ASSERT_EQ(hashes_one, hashes_four);
}

std::shared_ptr<Array> ReverseIndices(int64_t len) {
  Int32Builder indices_builder;
  ARROW_EXPECT_OK(indices_builder.Reserve(len));
  for (int64_t i = len - 1; i >= 0; i--) {
    indices_builder.UnsafeAppend(static_cast<int32_t>(i));
  }
  EXPECT_OK_AND_ASSIGN(std::shared_ptr<Array> indices, indices_builder.Finish());
  return indices;
}

ExecBatch ReverseBatch(ExecBatch batch) {
  ExecBatch reversed;
  std::shared_ptr<Array> reversed_indices = ReverseIndices(batch.length);
  for (const arrow::Datum& datum : batch.values) {
    EXPECT_OK_AND_ASSIGN(auto reversed_datum, Take(datum.make_array(), reversed_indices));
    reversed.values.push_back(reversed_datum);
  }
  reversed.length = batch.length;
  return reversed;
}

bool AllUnique(const std::vector<uint32_t>& values) {
  std::vector<uint32_t> values_copy(values);
  std::sort(values_copy.begin(), values_copy.end());
  std::unique(values_copy.begin(), values_copy.end());
  return values.size() == values_copy.size();
}

TEST_P(SwissTableWithKeysTest, Map) {
  constexpr int kBatchLength = 128;
  BatchesWithSchema batches_with_schema =
      MakeRandomBatches(test_schema(),
                        /*num_batches=*/2, kBatchLength, /*add_tag=*/false);
  SwissTableWithKeys::Input input(&batches_with_schema.batches[0], ctx_->stack,
                                  &temp_column_arrays_);

  std::vector<uint32_t> hashes_one(kBatchLength);
  SwissTableWithKeys::Hash(&input, hashes_one.data(), ctx_->hardware_flags);

  // Insert a bunch of keys
  std::vector<uint32_t> key_ids(kBatchLength);
  ASSERT_OK(table_.MapWithInserts(&input, hashes_one.data(), key_ids.data()));

  // We should have all unique keys but we can't make any assertions above and beyond that
  ASSERT_TRUE(AllUnique(key_ids));

  // Map the same keys, everything should match
  std::vector<uint8_t> match_bitvector(bit_util::CeilDiv(kBatchLength, 8));
  std::vector<uint32_t> new_key_ids(kBatchLength);
  table_.MapReadOnly(&input, hashes_one.data(), match_bitvector.data(),
                     new_key_ids.data());

  for (const auto& match_byte : match_bitvector) {
    ASSERT_EQ(0xFF, match_byte);
  }
  ASSERT_EQ(key_ids, new_key_ids);

  // Map the keys in reverse, should still match but key ids are reversed
  ExecBatch reversed_batch = ReverseBatch(batches_with_schema.batches[0]);
  SwissTableWithKeys::Input reversed_input(&reversed_batch, ctx_->stack,
                                           &temp_column_arrays_);

  std::vector<uint32_t> hashes_one_reverse(kBatchLength);
  SwissTableWithKeys::Hash(&reversed_input, hashes_one_reverse.data(),
                           ctx_->hardware_flags);

  new_key_ids.clear();
  new_key_ids.resize(kBatchLength);
  match_bitvector.clear();
  match_bitvector.resize(bit_util::CeilDiv(kBatchLength, 8));
  table_.MapReadOnly(&reversed_input, hashes_one_reverse.data(), match_bitvector.data(),
                     new_key_ids.data());

  for (const auto& match_byte : match_bitvector) {
    ASSERT_EQ(0xFF, match_byte);
  }
  std::reverse(new_key_ids.begin(), new_key_ids.end());
  ASSERT_EQ(key_ids, new_key_ids);

  // Map another batch of keys.  Some of them might match (since it is random data)
  // but it should be incredibly unlikely that any batch of 255 keys fully match
  SwissTableWithKeys::Input non_match_input(&batches_with_schema.batches[1], ctx_->stack,
                                            &temp_column_arrays_);
  std::vector<uint32_t> hashes_non_match(kBatchLength);
  SwissTableWithKeys::Hash(&non_match_input, hashes_non_match.data(),
                           ctx_->hardware_flags);

  new_key_ids.clear();
  new_key_ids.resize(kBatchLength);
  match_bitvector.clear();
  match_bitvector.resize(bit_util::CeilDiv(kBatchLength, 8));
  table_.MapReadOnly(&non_match_input, hashes_non_match.data(), match_bitvector.data(),
                     new_key_ids.data());

  // Can't compare directly to 0x00 since random data may allow for a few matches
  // A full matching block of 256 rows is extremely unlikely though.
  for (const auto& match_byte : match_bitvector) {
    ASSERT_NE(0xFF, match_byte);
  }
  uint32_t count_matching_key_ids = 0;
  for (const auto& key_id : new_key_ids) {
    if (key_id != 0) {
      count_matching_key_ids++;
    }
  }
  // Divide by 2 is a bit arbitrary but since this is random data some matches
  // are to be expected.
  ASSERT_LT(count_matching_key_ids, kBatchLength / 2);
}

// The table should allow parallel read access
TEST_P(SwissTableWithKeysTest, ThreadedMapReadOnly) {
  constexpr int kBatchLength = 128;
  constexpr int kBatchLengthBytes = bit_util::CeilDiv(kBatchLength, 8);
  constexpr int kNumBatches = 8;
  // These tasks will be divided across all threads
  constexpr int kNumReadTasks = 1 << 12;

  BatchesWithSchema batches_with_schema =
      MakeRandomBatches(test_schema(), kNumBatches, kBatchLength, /*add_tag=*/false);

  // Insert all batches
  // Save precomputed hashes so threads can focus on the map task
  std::vector<std::vector<uint32_t>> hashes(kBatchLength);
  std::vector<std::vector<uint32_t>> expected_key_ids(kNumBatches);
  for (std::size_t i = 0; i < kNumBatches; i++) {
    hashes[i].resize(kBatchLength);
    expected_key_ids[i].resize(kBatchLength);
    SwissTableWithKeys::Input input(&batches_with_schema.batches[i], ctx_->stack,
                                    &temp_column_arrays_);
    SwissTableWithKeys::Hash(&input, hashes[i].data(), ctx_->hardware_flags);
    ASSERT_OK(
        table_.MapWithInserts(&input, hashes[i].data(), expected_key_ids[i].data()));
  }

  struct ReadTaskThreadState {
    ReadTaskThreadState() {
      auto maybe_ctx = LightContext::Make(default_memory_pool());
      ctx = maybe_ctx.MoveValueUnsafe();
    }
    std::unique_ptr<LightContext, LightContext::OwningLightContextDeleter> ctx;
    std::vector<arrow::compute::KeyColumnArray> temp_arrays;
  };
  static std::unordered_map<std::thread::id, ReadTaskThreadState> state_map;

  struct ReadTask {
    void operator()() {
      ReadTaskThreadState& state = state_map[std::this_thread::get_id()];
      std::vector<uint8_t> match_bitvector(kBatchLengthBytes);
      std::vector<uint32_t> key_ids(kBatchLength);
      SwissTableWithKeys::Input input(&batch, state.ctx->stack, &state.temp_arrays);
      table->MapReadOnly(&input, hashes.data(), match_bitvector.data(), key_ids.data());
      for (const auto& match_byte : match_bitvector) {
        ASSERT_EQ(0xFF, match_byte);
      }
      ASSERT_EQ(expected_key_ids, key_ids);
    }
    SwissTableWithKeys* table;
    const ExecBatch& batch;
    const std::vector<uint32_t>& hashes;
    const std::vector<uint32_t>& expected_key_ids;
  };

  // Issue parallel reads
  Executor* thread_pool = GetCpuThreadPool();
  std::vector<Future<>> tasks;
  for (int i = 0; i < kNumReadTasks; i++) {
    std::size_t batch_index = i % kNumBatches;
    ReadTask read_task{&table_, batches_with_schema.batches[batch_index],
                       hashes[batch_index], expected_key_ids[batch_index]};
    tasks.push_back(DeferNotOk(thread_pool->Submit(std::move(read_task))));
  }
  for (const auto& task : tasks) {
    ASSERT_FINISHES_OK(task);
  }
}

static std::vector<SwissTableTestParams> GenerateTestParams() {
  return std::vector<SwissTableTestParams>{
      // Schema with only fixed-length fields
      {schema({field("i32", int32()), field("fixedbin", fixed_size_binary(24)),
               field("fp64", float64())}),
       "fixed"},
      // Schema with only var-length fields
      {schema({field("utf8", utf8()), field("second_utf8", utf8())}), "varlen"},
      // Schema with mix of fixed and var length fields
      {schema({field("i32", int32()), field("fixedbin", fixed_size_binary(24)),
               field("fp64", float64()), field("utf8", utf8())}),
       "mixed"}};
}

INSTANTIATE_TEST_SUITE_P(SwissTableWithKey, SwissTableWithKeysTest,
                         testing::ValuesIn(GenerateTestParams()));

}  // namespace compute
}  // namespace arrow
