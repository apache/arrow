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

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/test_util_internal.h"
#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/pretty_print.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/type.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/string.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/vector.h"

using testing::Eq;
using testing::HasSubstr;

namespace arrow {

using internal::BitmapReader;
using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::ToChars;

using compute::CallFunction;
using compute::CountOptions;
using compute::default_exec_context;
using compute::ExecSpan;
using compute::FunctionOptions;
using compute::Grouper;
using compute::RowSegmenter;
using compute::ScalarAggregateOptions;
using compute::Segment;
using compute::SortIndices;
using compute::SortKey;
using compute::SortOrder;
using compute::Take;
using compute::TDigestOptions;
using compute::VarianceOptions;

namespace acero {

namespace {

Result<Datum> MakeWindowOutput(const std::vector<ExecBatch>& output_batches,
                               const std::shared_ptr<Schema> output_schema,
                               size_t num_aggregates, size_t num_keys) {
  ArrayVector out_arrays(num_aggregates + num_keys);
  for (size_t i = 0; i < out_arrays.size(); ++i) {
    std::vector<std::shared_ptr<Array>> arrays(output_batches.size());
    for (size_t j = 0; j < output_batches.size(); ++j) {
      arrays[j] = output_batches[j].values[i].make_array();
    }
    if (arrays.empty()) {
      ARROW_ASSIGN_OR_RAISE(
          out_arrays[i],
          MakeArrayOfNull(output_schema->field(static_cast<int>(i))->type(),
                          /*length=*/0));
    } else {
      ARROW_ASSIGN_OR_RAISE(out_arrays[i], Concatenate(arrays));
    }
  }

  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<Array> struct_arr,
      StructArray::Make(std::move(out_arrays), output_schema->fields()));

  bool need_sort = true;
  for (size_t i = 0; need_sort && i < num_keys; i++) {
    if (output_schema->field(static_cast<int>(i))->type()->id() == Type::DICTIONARY) {
      need_sort = false;
    }
  }
  if (!need_sort) {
    return struct_arr;
  }

  // The exec plan may reorder the output rows.  The tests are all setup to expect ouptut
  // in ascending order of keys.  So we need to sort the result by the key columns.  To do
  // that we create a table using the key columns, calculate the sort indices from that
  // table (sorting on all fields) and then use those indices to calculate our result.
  std::vector<std::shared_ptr<Field>> key_fields;
  std::vector<std::shared_ptr<Array>> key_columns;
  std::vector<SortKey> sort_keys;
  for (std::size_t i = 0; i < num_keys; i++) {
    const std::shared_ptr<Array>& arr = out_arrays[i];
    key_columns.push_back(arr);
    key_fields.push_back(field("name_does_not_matter", arr->type()));
    sort_keys.emplace_back(static_cast<int>(i));
  }
  std::shared_ptr<Schema> key_schema = schema(std::move(key_fields));
  std::shared_ptr<Table> key_table = Table::Make(std::move(key_schema), key_columns);
  SortOptions sort_options(std::move(sort_keys));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> sort_indices,
                        SortIndices(key_table, sort_options));
  return Take(struct_arr, sort_indices);
}

Result<Datum> RunWindow(const BatchesWithSchema& input,
                        const std::vector<std::string>& key_names,
                        const std::vector<std::string>& segment_key_names,
                        const std::vector<Aggregate>& aggregates, ExecContext* ctx,
                        bool use_threads, bool segmented = false,
                        uint64_t left_boundary = 0, uint64_t right_boundary = 0,
                        bool left_inclusive = true, bool right_inclusive = true) {
  // The `use_threads` flag determines whether threads are used in generating the input to
  // the group-by.
  //
  // When segment_keys is non-empty the `segmented` flag is always true; otherwise (when
  // empty), it may still be set to true. In this case, the tester restructures (without
  // changing the data of) the result of RunGroupBy from `std::vector<ExecBatch>`
  // (output_batches) to `std::vector<ArrayVector>` (out_arrays), which have the structure
  // typical of the case of a non-empty segment_keys (with multiple arrays per column, one
  // array per segment) but only one array per column (because, technically, there is only
  // one segment in this case). Thus, this case focuses on the structure of the result.

  std::vector<FieldRef> keys(key_names.size());
  for (size_t i = 0; i < key_names.size(); ++i) {
    keys[i] = FieldRef(key_names[i]);
  }
  std::vector<FieldRef> segment_keys(segment_key_names.size());
  for (size_t i = 0; i < segment_key_names.size(); ++i) {
    segment_keys[i] = FieldRef(segment_key_names[i]);
  }

  ARROW_ASSIGN_OR_RAISE(auto plan, ExecPlan::Make(*ctx));
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;
  RETURN_NOT_OK(
      Declaration::Sequence(
          {
              {"source",
               SourceNodeOptions{input.schema, input.gen(use_threads, /*slow=*/false)}},
              {"aggregate",
               AggregateNodeOptions{
                   std::move(aggregates), std::move(keys), std::move(segment_keys),
                   WindowAggregateArgs{left_boundary, right_boundary, left_inclusive,
                                       right_inclusive}}},
              {"sink", SinkNodeOptions{&sink_gen}},
          })
          .AddToPlan(plan.get()));

  RETURN_NOT_OK(plan->Validate());
  plan->StartProducing();

  auto collected_fut = CollectAsyncGenerator(sink_gen);

  auto start_and_collect =
      AllFinished({plan->finished(), Future<>(collected_fut)})
          .Then([collected_fut]() -> Result<std::vector<ExecBatch>> {
            ARROW_ASSIGN_OR_RAISE(auto collected, collected_fut.result());
            return ::arrow::internal::MapVector(
                [](std::optional<ExecBatch> batch) {
                  return batch.value_or(ExecBatch());
                },
                std::move(collected));
          });

  ARROW_ASSIGN_OR_RAISE(std::vector<ExecBatch> output_batches,
                        start_and_collect.MoveResult());

  const auto& output_schema = plan->nodes()[0]->output()->output_schema();
  if (!segmented) {
    return MakeWindowOutput(output_batches, output_schema, aggregates.size(),
                            key_names.size());
  }

  std::vector<ArrayVector> out_arrays(aggregates.size() + key_names.size() +
                                      segment_key_names.size());
  for (size_t i = 0; i < out_arrays.size(); ++i) {
    std::vector<std::shared_ptr<Array>> arrays(output_batches.size());
    for (size_t j = 0; j < output_batches.size(); ++j) {
      auto& value = output_batches[j].values[i];
      if (value.is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(
            arrays[j], MakeArrayFromScalar(*value.scalar(), output_batches[j].length));
      } else if (value.is_array()) {
        arrays[j] = value.make_array();
      } else {
        return Status::Invalid("GroupByUsingExecPlan unsupported value kind ",
                               ToString(value.kind()));
      }
    }
    if (arrays.empty()) {
      arrays.resize(1);
      ARROW_ASSIGN_OR_RAISE(
          arrays[0], MakeArrayOfNull(output_schema->field(static_cast<int>(i))->type(),
                                     /*length=*/0));
    }
    out_arrays[i] = {std::move(arrays)};
  }

  if (segmented && segment_key_names.size() > 0) {
    ArrayVector struct_arrays;
    struct_arrays.reserve(output_batches.size());
    for (size_t j = 0; j < output_batches.size(); ++j) {
      ArrayVector struct_fields;
      struct_fields.reserve(out_arrays.size());
      for (auto out_array : out_arrays) {
        struct_fields.push_back(out_array[j]);
      }
      ARROW_ASSIGN_OR_RAISE(auto struct_array,
                            StructArray::Make(struct_fields, output_schema->fields()));
      struct_arrays.push_back(struct_array);
    }
    return ChunkedArray::Make(struct_arrays);
  } else {
    ArrayVector struct_fields(out_arrays.size());
    for (size_t i = 0; i < out_arrays.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(struct_fields[i], Concatenate(out_arrays[i]));
    }
    return StructArray::Make(std::move(struct_fields), output_schema->fields());
  }
}

Result<Datum> RunWindow(const BatchesWithSchema& input,
                        const std::vector<std::string>& key_names,
                        const std::vector<std::string>& segment_key_names,
                        const std::vector<Aggregate>& aggregates, bool use_threads,
                        bool segmented = false, uint64_t left_boundary = 0,
                        uint64_t right_boundary = 0, bool left_inclusive = true,
                        bool right_inclusive = true) {
  if (!use_threads) {
    ARROW_ASSIGN_OR_RAISE(auto thread_pool, arrow::internal::ThreadPool::Make(1));
    ExecContext seq_ctx(default_memory_pool(), thread_pool.get());
    return RunWindow(input, key_names, segment_key_names, aggregates, &seq_ctx,
                     use_threads, segmented, left_boundary, right_boundary,
                     left_inclusive, right_inclusive);
  } else {
    return RunWindow(input, key_names, segment_key_names, aggregates,
                     threaded_exec_context(), use_threads, segmented, left_boundary,
                     right_boundary, left_inclusive, right_inclusive);
  }
}

/// Simpler overload where you can give the columns as datums
Result<Datum> RunWindow(const std::vector<Datum>& arguments,
                        const std::vector<Datum>& keys,
                        const std::vector<Datum>& segment_keys,
                        const std::vector<Aggregate>& aggregates, bool use_threads,
                        bool segmented = false, uint64_t left_boundary = 0,
                        uint64_t right_boundary = 0, bool left_inclusive = true,
                        bool right_inclusive = true) {
  using arrow::compute::detail::ExecSpanIterator;

  FieldVector scan_fields(arguments.size() + keys.size() + segment_keys.size());
  std::vector<std::string> key_names(keys.size());
  std::vector<std::string> segment_key_names(segment_keys.size());
  for (size_t i = 0; i < arguments.size(); ++i) {
    auto name = std::string("agg_") + ToChars(i);
    scan_fields[i] = field(name, arguments[i].type());
  }
  size_t base = arguments.size();
  for (size_t i = 0; i < keys.size(); ++i) {
    auto name = std::string("key_") + ToChars(i);
    scan_fields[base + i] = field(name, keys[i].type());
    key_names[i] = std::move(name);
  }
  base += keys.size();
  size_t j = keys.size();
  std::string prefix("key_");
  for (size_t i = 0; i < segment_keys.size(); ++i) {
    auto name = prefix + std::to_string(j++);
    scan_fields[base + i] = field(name, segment_keys[i].type());
    segment_key_names[i] = std::move(name);
  }

  std::vector<Datum> inputs = arguments;
  inputs.reserve(inputs.size() + keys.size() + segment_keys.size());
  inputs.insert(inputs.end(), keys.begin(), keys.end());
  inputs.insert(inputs.end(), segment_keys.begin(), segment_keys.end());

  ExecSpanIterator span_iterator;
  ARROW_ASSIGN_OR_RAISE(auto batch, ExecBatch::Make(inputs));
  RETURN_NOT_OK(span_iterator.Init(batch));
  BatchesWithSchema input;
  input.schema = schema(std::move(scan_fields));
  ExecSpan span;
  while (span_iterator.Next(&span)) {
    if (span.length == 0) continue;
    input.batches.push_back(span.ToExecBatch());
  }

  return RunWindow(input, key_names, segment_key_names, aggregates, use_threads,
                   segmented, left_boundary, right_boundary, left_inclusive,
                   right_inclusive);
}

Result<std::shared_ptr<Table>> GetChunkedSegmentInput() {
  auto table =
      TableFromJSON(schema({field("argument", float64()), field("segment_key", int64())}),
                    {R"([[1,      1], [2.3,   2]])",
                     R"([[0.0,    3], [1.3,   4], [4.0,    5], [3.25,  6],
            [0.125,  7], [-0.25, 8], [0.75,   9], [4.5,  10]])",
                     R"([[1.0,   11], [3.5,   12]])",
                     R"([[0.0,   13], [2.1,   14], [4.0,  15], [3.25, 16],
            [0.125, 17], [-0.25, 18], [0.75, 19], [-0.5, 20]])"});
  return table;
}

Result<std::shared_ptr<Table>> GetCombinedSegmentInput() {
  ARROW_ASSIGN_OR_RAISE(auto table, GetChunkedSegmentInput());
  return table->CombineChunks();
}

Result<Datum> GetSumOneZeroResults() {
  return ChunkedArrayFromJSON(
      struct_({field("key_0", int64()), field("sum", float64())}),
      {R"([[1, 1], [2, 3.3], [3, 2.3], [4, 1.3], [5, 5.3], [6, 7.25], [7, 3.375], [8, -0.125], [9, 0.50]])",
       R"([[10, 5.25], [11, 5.5], [12, 4.5], [13, 3.5], [14, 2.1], [15, 6.1], [16, 7.25], [17, 3.375]])",
       R"([[18, -0.125], [19, 0.50], [20, 0.25]])"});
}

Result<Datum> GetSumZeroOneResults() {
  return ChunkedArrayFromJSON(
      struct_({field("key_0", int64()), field("sum", float64())}),
      {R"([[1, 3.3], [2, 2.3], [3, 1.3], [4, 5.3], [5, 7.25], [6, 3.375], [7, -0.125], [8, 0.50]])",
       R"([[9, 5.25], [10, 5.5], [11, 4.5], [12, 3.5], [13, 2.1], [14, 6.1], [15, 7.25], [16, 3.375]])",
       R"([[17, -0.125], [18, 0.50], [19, 0.25], [20, -0.5]])"});
}

Result<Datum> GetSumOneOneResults() {
  return ChunkedArrayFromJSON(
      struct_({field("key_0", int64()), field("sum", float64())}),
      {R"([[1, 3.3], [2, 3.3], [3, 3.6], [4, 5.3], [5, 8.55], [6, 7.375], [7, 3.125], [8, 0.625], [9, 5]])",
       R"([[10, 6.25], [11, 9], [12, 4.5], [13, 5.6], [14, 6.1], [15, 9.35], [16, 7.375], [17, 3.125]])",
       R"([[18, 0.625], [19, 0], [20, 0.25]])"});
}

Result<Datum> GetSumThreeZeroResults() {
  return ChunkedArrayFromJSON(
      struct_({field("key_0", int64()), field("sum", float64())}),
      {R"([[1, 1], [2, 3.3], [3, 3.3], [4, 4.6], [5, 7.6], [6, 8.55], [7, 8.675], [8, 7.125], [9, 3.875]])",
       R"([[10, 5.125], [11, 6.0], [12, 9.75], [13, 9.0], [14, 6.6], [15, 9.6], [16, 9.35], [17, 9.475]])",
       R"([[18, 7.125], [19, 3.875], [20, 0.125]])"});
}

Result<Datum> GetSumThreeZeroLeftOpenResults() {
  return ChunkedArrayFromJSON(
      struct_({field("key_0", int64()), field("sum", float64())}),
      {R"([[1, 1], [2, 3.3], [3, 3.3], [4, 3.6], [5, 5.3], [6, 8.55], [7, 7.375], [8, 3.125], [9, 0.625]])",
       R"([[10, 5], [11, 6.25], [12, 9], [13, 4.5], [14, 5.6], [15, 6.1], [16, 9.35], [17, 7.375]])",
       R"([[18, 3.125], [19, 0.625], [20, 0]])"});
}

Result<Datum> GetSumThreeZeroRightOpenResults() {
  return ChunkedArrayFromJSON(
      struct_({field("key_0", int64()), field("sum", float64())}),
      {R"([[1, 1], [2, 1], [3, 3.3], [4, 3.3], [5, 3.6], [6, 5.3], [7, 8.55], [8, 7.375], [9, 3.125]])",
       R"([[10, 0.625], [11, 5], [12, 6.25], [13, 9.0], [14, 4.5], [15, 5.6], [16, 6.1], [17, 9.35]])",
       R"([[18, 7.375], [19, 3.125], [20, 0.625]])"});
}

// Test [-1,0] interval on chunked input
TEST(ScalarWindowAggregation, ChunkedSumOneZero) {
  ASSERT_OK_AND_ASSIGN(auto table, GetChunkedSegmentInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetSumOneZeroResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed,
                       RunWindow({table->GetColumnByName("argument")}, {},
                                 {table->GetColumnByName("segment_key")},
                                 {{"sum", nullptr, "agg_0", "sum"}}, false, true, 1, 0));
  AssertDatumsEqual(expected, windowed, /*verbose=*/true);
}

// Test [-1,0] interval on combined input
TEST(ScalarWindowAggregation, CombinedSumOneZero) {
  ASSERT_OK_AND_ASSIGN(auto table, GetCombinedSegmentInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetSumOneZeroResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed,
                       RunWindow({table->GetColumnByName("argument")}, {},
                                 {table->GetColumnByName("segment_key")},
                                 {{"sum", nullptr, "agg_0", "sum"}}, false, true, 1, 0));
  AssertDatumsEqual(expected, windowed, /*verbose=*/true);
}

// Test [0,1] interval on chunked input
TEST(ScalarWindowAggregation, ChunkedSumZeroOne) {
  ASSERT_OK_AND_ASSIGN(auto table, GetChunkedSegmentInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetSumZeroOneResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed,
                       RunWindow({table->GetColumnByName("argument")}, {},
                                 {table->GetColumnByName("segment_key")},
                                 {{"sum", nullptr, "agg_0", "sum"}}, false, true, 0, 1));
  AssertDatumsEqual(expected, windowed, /*verbose=*/true);
}

// Test [0,1] interval on combined input
TEST(ScalarWindowAggregation, CombinedSumZeroOne) {
  ASSERT_OK_AND_ASSIGN(auto table, GetCombinedSegmentInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetSumZeroOneResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed,
                       RunWindow({table->GetColumnByName("argument")}, {},
                                 {table->GetColumnByName("segment_key")},
                                 {{"sum", nullptr, "agg_0", "sum"}}, false, true, 0, 1));
  AssertDatumsEqual(expected, windowed, /*verbose=*/true);
}

// Test [-1,1] interval on chunked input
TEST(ScalarWindowAggregation, ChunkedSumOneOne) {
  ASSERT_OK_AND_ASSIGN(auto table, GetChunkedSegmentInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetSumOneOneResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed,
                       RunWindow({table->GetColumnByName("argument")}, {},
                                 {table->GetColumnByName("segment_key")},
                                 {{"sum", nullptr, "agg_0", "sum"}}, false, true, 1, 1));
  AssertDatumsApproxEqual(expected, windowed, /*verbose=*/true);
}

// Test [-1,1] interval on combined input
TEST(ScalarWindowAggregation, CombinedSumOneOne) {
  ASSERT_OK_AND_ASSIGN(auto table, GetCombinedSegmentInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetSumOneOneResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed,
                       RunWindow({table->GetColumnByName("argument")}, {},
                                 {table->GetColumnByName("segment_key")},
                                 {{"sum", nullptr, "agg_0", "sum"}}, false, true, 1, 1));
  AssertDatumsApproxEqual(expected, windowed, /*verbose=*/true);
}

// Test [-3,0] interval on chunked input
TEST(ScalarWindowAggregation, ChunkedSumThreeZero) {
  ASSERT_OK_AND_ASSIGN(auto table, GetChunkedSegmentInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetSumThreeZeroResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed,
                       RunWindow({table->GetColumnByName("argument")}, {},
                                 {table->GetColumnByName("segment_key")},
                                 {{"sum", nullptr, "agg_0", "sum"}}, false, true, 3, 0));
  AssertDatumsApproxEqual(expected, windowed, /*verbose=*/true);
}

// Test (-3,0] interval on chunked input
TEST(ScalarWindowAggregation, ChunkedSumThreeZeroLeftOpen) {
  ASSERT_OK_AND_ASSIGN(auto table, GetChunkedSegmentInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetSumThreeZeroLeftOpenResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed, RunWindow({table->GetColumnByName("argument")}, {},
                                                 {table->GetColumnByName("segment_key")},
                                                 {{"sum", nullptr, "agg_0", "sum"}},
                                                 false, true, 3, 0, false, true));
  AssertDatumsApproxEqual(expected, windowed, /*verbose=*/true);
}

// Test [-3,0) interval on chunked input
TEST(ScalarWindowAggregation, ChunkedSumThreeZeroRightOpen) {
  ASSERT_OK_AND_ASSIGN(auto table, GetChunkedSegmentInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetSumThreeZeroRightOpenResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed, RunWindow({table->GetColumnByName("argument")}, {},
                                                 {table->GetColumnByName("segment_key")},
                                                 {{"sum", nullptr, "agg_0", "sum"}},
                                                 false, true, 3, 0, true, false));
  AssertDatumsApproxEqual(expected, windowed, /*verbose=*/true);
}

// ----------------------------------- Window Group By --------------------------

Result<std::shared_ptr<Table>> GetChunkedOneWindowInput() {
  auto table =
      TableFromJSON(schema({field("argument", float64()), field("group_key", int64()),
                            field("segment_key", int64())}),
                    {R"([[1,     1,  1], [2.3,   1,  2]])",
                     R"([[0.0,   1,  3], [1.3,   1,  4], [4.0,   1,  5], [3.25,  1,  6],
            [0.125, 1,  7], [-0.25, 1,  8], [0.75,  1,  9], [4.5,   1, 10]])",
                     R"([[1.0,   1, 11], [3.5,   1, 12]])",
                     R"([[0.0,   1, 13], [2.1,   1, 14], [4.0,   1, 15], [3.25,  1, 16],
            [0.125, 1, 17], [-0.25, 1, 18], [0.75,  1, 19], [-0.5,  1, 20]])"});
  return table;
}

Result<std::shared_ptr<Table>> GetCombinedOneWindowInput() {
  ARROW_ASSIGN_OR_RAISE(auto table, GetChunkedOneWindowInput());
  return table->CombineChunks();
}

Result<std::shared_ptr<Table>> GetChunkedFullWindowInput() {
  auto table =
      TableFromJSON(schema({field("argument", float64()), field("group_key", int64()),
                            field("segment_key", int64())}),
                    {R"([[1,     1,  1], [2.3,   2,  1]])",
                     R"([[0.0,   3,  1], [1.3,   1,  2], [4.0,   2,  2], [3.25,  3,  2],
            [0.125, 1,  3], [-0.25, 2,  3], [0.75,  3,  3], [4.5,   1, 4]])",
                     R"([[1.0,   2,  4], [3.5,   3,  4]])",
                     R"([[0.0,   1,  5], [2.1,   2,  5], [4.0,   3,  5], [3.25,  1,  6],
            [0.125, 2,  6], [-0.25, 3,  6], [0.75,  1, 7], [-0.5,  2, 7], [-0.5,  3, 7]])"});
  return table;
}

Result<std::shared_ptr<Table>> GetCombinedFullWindowInput() {
  ARROW_ASSIGN_OR_RAISE(auto table, GetChunkedFullWindowInput());
  return table->CombineChunks();
}

Result<std::shared_ptr<Table>> GetChunkedPartialWindowInput() {
  auto table =
      TableFromJSON(schema({field("argument", float64()), field("group_key", int64()),
                            field("segment_key", int64())}),
                    {R"([[1,     1,  1]])",
                     R"([[0.0,   3,  1], [1.3,   1,  2], [4.0,   2,  2], [3.25,  3,  2],
            [0.125, 1,  3], [-0.25, 2,  3], [4.5,   1, 4]])",
                     R"([[1.0,   2,  4], [3.5,   3,  4]])",
                     R"([[0.0,   1,  5], [3.25,  1,  6],
            [0.125, 2,  6], [0.75,  1, 7], [-0.5,  2, 7]])"});
  return table;
}

Result<std::shared_ptr<Table>> GetCombinedPartialWindowInput() {
  ARROW_ASSIGN_OR_RAISE(auto table, GetChunkedPartialWindowInput());
  return table->CombineChunks();
}

Result<Datum> GetGroupSumThreeZeroResults() {
  return ChunkedArrayFromJSON(
      struct_(
          {field("key_1", int64()), field("key_0", int64()), field("sum", float64())}),
      {R"([[1,  1, 1],     [2,  1, 3.3],   [3,  1, 3.3],  [4,  1, 4.6], [5,  1, 7.6], [6,  1, 8.55], [7, 1, 8.675], [8, 1, 7.125], [9, 1, 3.875]])",
       R"([[10, 1, 5.125], [11, 1, 6.0],   [12, 1, 9.75], [13, 1, 9.0], [14, 1, 6.6], [15, 1, 9.6],  [16, 1, 9.35], [17, 1, 9.475]])",
       R"([[18, 1, 7.125], [19, 1, 3.875], [20, 1, 0.125]])"});
}

Result<Datum> GetFullGroupSumThreeZeroResults() {
  return ChunkedArrayFromJSON(
      struct_(
          {field("key_1", int64()), field("key_0", int64()), field("sum", float64())}),
      {R"([[1, 1, 1],     [1, 2, 2.3],  [1, 3, 0],     [2, 1, 2.3],   [2, 2, 6.3],  [2, 3, 3.25], [3, 1, 2.425], [3, 2, 6.05], [3, 3, 4]])",
       R"([[4, 1, 6.925], [4, 2, 7.05], [4, 3, 7.5],   [5, 1, 5.925], [5, 2, 6.85], [5, 3, 11.5], [6, 1, 7.875], [6, 2, 2.975]])",
       R"([[6, 3, 8],     [7, 1, 8.5],  [7, 2, 2.725], [7, 3, 6.75]])"});
}

Result<Datum> GetPartialGroupSumThreeZeroResults() {
  return ChunkedArrayFromJSON(
      struct_(
          {field("key_1", int64()), field("key_0", int64()), field("sum", float64())}),
      {R"([[1, 1, 1],     [1, 3, 0],    [2, 1, 2.3],   [2, 3, 3.25],  [2, 2, 4],    [3, 1, 2.425], [3, 3, 3.25],  [3, 2, 3.75]])",
       R"([[4, 1, 6.925], [4, 3, 6.75], [4, 2, 4.75],  [5, 1, 5.925], [5, 2, 4.75], [5, 3, 6.75],  [6, 1, 7.875], [6, 2, 0.875]])",
       R"([[6, 3, 3.5],   [7, 1, 8.5],  [7, 2, 0.625], [7, 3, 3.5]])"});
}

// Test [-3,0] interval on chunked input with only 1 group
TEST(GroupByWindowAggregation, OneGroupChunkedSumThreeZero) {
  ASSERT_OK_AND_ASSIGN(auto table, GetChunkedOneWindowInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetGroupSumThreeZeroResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed, RunWindow({table->GetColumnByName("argument")},
                                                 {table->GetColumnByName("group_key")},
                                                 {table->GetColumnByName("segment_key")},
                                                 {{"hash_sum", nullptr, "agg_0", "sum"}},
                                                 false, true, 3, 0));
  AssertDatumsApproxEqual(expected, windowed, /*verbose=*/true);
}

// Test (-3,0] interval on chunked input with only 1 group
TEST(GroupByWindowAggregation, OneGroupCombinedSumThreeZero) {
  ASSERT_OK_AND_ASSIGN(auto table, GetCombinedOneWindowInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetGroupSumThreeZeroResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed, RunWindow({table->GetColumnByName("argument")},
                                                 {table->GetColumnByName("group_key")},
                                                 {table->GetColumnByName("segment_key")},
                                                 {{"hash_sum", nullptr, "agg_0", "sum"}},
                                                 false, true, 3, 0));
  AssertDatumsApproxEqual(expected, windowed, /*verbose=*/true);
}

// Test [-3,0] interval on chunked input with only 1 group
TEST(GroupByWindowAggregation, FullGroupsChunkedSumThreeZero) {
  ASSERT_OK_AND_ASSIGN(auto table, GetChunkedFullWindowInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetFullGroupSumThreeZeroResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed, RunWindow({table->GetColumnByName("argument")},
                                                 {table->GetColumnByName("group_key")},
                                                 {table->GetColumnByName("segment_key")},
                                                 {{"hash_sum", nullptr, "agg_0", "sum"}},
                                                 false, true, 3, 0));
  AssertDatumsApproxEqual(expected, windowed, /*verbose=*/true);
}

// Test (-3,0] interval on chunked input with only 1 group
TEST(GroupByWindowAggregation, FullGroupsChunkedSumThreeZeroLeftOpen) {
  ASSERT_OK_AND_ASSIGN(auto table, GetCombinedFullWindowInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetFullGroupSumThreeZeroResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed, RunWindow({table->GetColumnByName("argument")},
                                                 {table->GetColumnByName("group_key")},
                                                 {table->GetColumnByName("segment_key")},
                                                 {{"hash_sum", nullptr, "agg_0", "sum"}},
                                                 false, true, 3, 0));

  AssertDatumsApproxEqual(expected, windowed, /*verbose=*/true);
}

// Test [-3,0] interval on chunked input with only 1 group
TEST(GroupByWindowAggregation, PartialGroupsChunkedSumThreeZero) {
  ASSERT_OK_AND_ASSIGN(auto table, GetChunkedPartialWindowInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetPartialGroupSumThreeZeroResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed, RunWindow({table->GetColumnByName("argument")},
                                                 {table->GetColumnByName("group_key")},
                                                 {table->GetColumnByName("segment_key")},
                                                 {{"hash_sum", nullptr, "agg_0", "sum"}},
                                                 false, true, 3, 0));
  for (int i = 0; i < windowed.chunked_array()->length(); i++) {
    auto a = windowed.chunked_array()->GetScalar(i).ValueOrDie()->ToString();
    auto b = expected.chunked_array()->GetScalar(i).ValueOrDie()->ToString();
    if (a != b) {
      std::cout << a << std::endl;
      std::cout << b << std::endl;
    }
  }
  AssertDatumsApproxEqual(expected, windowed, /*verbose=*/true);
}

// Test (-3,0] interval on chunked input with only 1 group
TEST(GroupByWindowAggregation, PartialGroupsChunkedSumThreeZeroLeftOpen) {
  ASSERT_OK_AND_ASSIGN(auto table, GetCombinedPartialWindowInput());
  ASSERT_OK_AND_ASSIGN(auto expected, GetPartialGroupSumThreeZeroResults());
  ASSERT_OK_AND_ASSIGN(Datum windowed, RunWindow({table->GetColumnByName("argument")},
                                                 {table->GetColumnByName("group_key")},
                                                 {table->GetColumnByName("segment_key")},
                                                 {{"hash_sum", nullptr, "agg_0", "sum"}},
                                                 false, true, 3, 0));

  AssertDatumsApproxEqual(expected, windowed, /*verbose=*/true);
}

}  // namespace
}  // namespace acero
}  // namespace arrow
