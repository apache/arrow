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
#include <limits>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/compute/row/grouper_internal.h"
#include "arrow/table.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
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

namespace compute {
namespace {

using GroupByFunction = std::function<Result<Datum>(
    const std::vector<Datum>&, const std::vector<Datum>&, const std::vector<Datum>&,
    const std::vector<Aggregate>&, bool, bool)>;

Result<Datum> NaiveGroupBy(std::vector<Datum> arguments, std::vector<Datum> keys,
                           const std::vector<Aggregate>& aggregates) {
  ARROW_ASSIGN_OR_RAISE(auto key_batch, ExecBatch::Make(std::move(keys)));

  ARROW_ASSIGN_OR_RAISE(auto grouper, Grouper::Make(key_batch.GetTypes()));

  ARROW_ASSIGN_OR_RAISE(Datum id_batch, grouper->Consume(ExecSpan(key_batch)));

  ARROW_ASSIGN_OR_RAISE(
      auto groupings,
      Grouper::MakeGroupings(*id_batch.array_as<UInt32Array>(), grouper->num_groups()));

  ArrayVector out_columns;
  std::vector<std::string> out_names;

  for (size_t i = 0; i < arguments.size(); ++i) {
    out_names.push_back(aggregates[i].function);

    // trim "hash_" prefix
    auto scalar_agg_function = aggregates[i].function.substr(5);

    ARROW_ASSIGN_OR_RAISE(
        auto grouped_argument,
        Grouper::ApplyGroupings(*groupings, *arguments[i].make_array()));

    ScalarVector aggregated_scalars;

    for (int64_t i_group = 0; i_group < grouper->num_groups(); ++i_group) {
      auto slice = grouped_argument->value_slice(i_group);
      if (slice->length() == 0) continue;
      ARROW_ASSIGN_OR_RAISE(Datum d, CallFunction(scalar_agg_function, {slice},
                                                  aggregates[i].options.get()));
      aggregated_scalars.push_back(d.scalar());
    }

    ARROW_ASSIGN_OR_RAISE(Datum aggregated_column,
                          ScalarVectorToArray(aggregated_scalars));
    out_columns.push_back(aggregated_column.make_array());
  }

  int i = 0;
  ARROW_ASSIGN_OR_RAISE(auto uniques, grouper->GetUniques());
  std::vector<SortKey> sort_keys;
  std::vector<std::shared_ptr<Field>> sort_table_fields;
  for (const Datum& key : uniques.values) {
    out_columns.push_back(key.make_array());
    sort_keys.emplace_back(FieldRef(i));
    sort_table_fields.push_back(field("key_" + ToChars(i), key.type()));
    out_names.push_back("key_" + ToChars(i++));
  }

  // Return a struct array sorted by the keys
  SortOptions sort_options(std::move(sort_keys));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> sort_batch,
                        uniques.ToRecordBatch(schema(std::move(sort_table_fields))));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> sorted_indices,
                        SortIndices(sort_batch, sort_options));

  ARROW_ASSIGN_OR_RAISE(auto struct_arr,
                        StructArray::Make(std::move(out_columns), std::move(out_names)));
  return Take(struct_arr, sorted_indices);
}

Result<Datum> MakeGroupByOutput(const std::vector<ExecBatch>& output_batches,
                                const std::shared_ptr<Schema> output_schema,
                                size_t num_aggregates, size_t num_keys, bool naive) {
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

  bool need_sort = !naive;
  for (size_t i = num_aggregates; need_sort && i < out_arrays.size(); i++) {
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
    const std::shared_ptr<Array>& arr = out_arrays[i + num_aggregates];
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

Result<Datum> RunGroupBy(const BatchesWithSchema& input,
                         const std::vector<std::string>& key_names,
                         const std::vector<std::string>& segment_key_names,
                         const std::vector<Aggregate>& aggregates, ExecContext* ctx,
                         bool use_threads, bool segmented = false, bool naive = false) {
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
  //
  // The `naive` flag means that the output is expected to be like that of `NaiveGroupBy`,
  // which in particular doesn't require sorting. The reason for the naive flag is that
  // the expected output of some test-cases is naive and of some others it is not. The
  // current `RunGroupBy` function deals with both kinds of expected output.
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
              {"aggregate", AggregateNodeOptions{std::move(aggregates), std::move(keys),
                                                 std::move(segment_keys)}},
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
    return MakeGroupByOutput(output_batches, output_schema, aggregates.size(),
                             key_names.size(), naive);
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

Result<Datum> RunGroupBy(const BatchesWithSchema& input,
                         const std::vector<std::string>& key_names,
                         const std::vector<std::string>& segment_key_names,
                         const std::vector<Aggregate>& aggregates, bool use_threads,
                         bool segmented = false, bool naive = false) {
  if (segment_key_names.size() > 0) {
    ARROW_ASSIGN_OR_RAISE(auto thread_pool, arrow::internal::ThreadPool::Make(1));
    ExecContext seq_ctx(default_memory_pool(), thread_pool.get());
    return RunGroupBy(input, key_names, segment_key_names, aggregates, &seq_ctx,
                      use_threads, segmented, naive);
  } else {
    return RunGroupBy(input, key_names, segment_key_names, aggregates,
                      threaded_exec_context(), use_threads, segmented, naive);
  }
}

Result<Datum> RunGroupBy(const BatchesWithSchema& input,
                         const std::vector<std::string>& key_names,
                         const std::vector<Aggregate>& aggregates, bool use_threads,
                         bool segmented = false, bool naive = false) {
  return RunGroupBy(input, key_names, {}, aggregates, use_threads, segmented);
}

/// Simpler overload where you can give the columns as datums
Result<Datum> RunGroupBy(const std::vector<Datum>& arguments,
                         const std::vector<Datum>& keys,
                         const std::vector<Datum>& segment_keys,
                         const std::vector<Aggregate>& aggregates, bool use_threads,
                         bool segmented = false, bool naive = false) {
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

  return RunGroupBy(input, key_names, segment_key_names, aggregates, use_threads,
                    segmented, naive);
}

Result<Datum> RunGroupByImpl(const std::vector<Datum>& arguments,
                             const std::vector<Datum>& keys,
                             const std::vector<Datum>& segment_keys,
                             const std::vector<Aggregate>& aggregates, bool use_threads,
                             bool naive = false) {
  return RunGroupBy(arguments, keys, segment_keys, aggregates, use_threads,
                    /*segmented=*/false, naive);
}

Result<Datum> RunSegmentedGroupByImpl(const std::vector<Datum>& arguments,
                                      const std::vector<Datum>& keys,
                                      const std::vector<Datum>& segment_keys,
                                      const std::vector<Aggregate>& aggregates,
                                      bool use_threads, bool naive = false) {
  return RunGroupBy(arguments, keys, segment_keys, aggregates, use_threads,
                    /*segmented=*/true, naive);
}

void ValidateGroupBy(GroupByFunction group_by, const std::vector<Aggregate>& aggregates,
                     std::vector<Datum> arguments, std::vector<Datum> keys,
                     bool naive = true) {
  ASSERT_OK_AND_ASSIGN(Datum expected, NaiveGroupBy(arguments, keys, aggregates));

  ASSERT_OK_AND_ASSIGN(Datum actual, group_by(arguments, keys, {}, aggregates,
                                              /*use_threads=*/false, naive));

  ASSERT_OK(expected.make_array()->ValidateFull());
  ValidateOutput(actual);

  AssertDatumsEqual(expected, actual, /*verbose=*/true);
}

ExecContext* small_chunksize_context(bool use_threads = false) {
  static ExecContext ctx,
      ctx_with_threads{default_memory_pool(), arrow::internal::GetCpuThreadPool()};
  ctx.set_exec_chunksize(2);
  ctx_with_threads.set_exec_chunksize(2);
  return use_threads ? &ctx_with_threads : &ctx;
}

struct TestAggregate {
  std::string function;
  std::shared_ptr<FunctionOptions> options;
};

Result<Datum> GroupByTest(GroupByFunction group_by, const std::vector<Datum>& arguments,
                          const std::vector<Datum>& keys,
                          const std::vector<Datum>& segment_keys,
                          const std::vector<TestAggregate>& aggregates,
                          bool use_threads) {
  std::vector<Aggregate> internal_aggregates;
  int idx = 0;
  for (auto t_agg : aggregates) {
    internal_aggregates.push_back(
        {t_agg.function, t_agg.options, "agg_" + ToChars(idx), t_agg.function});
    idx = idx + 1;
  }
  return group_by(arguments, keys, segment_keys, internal_aggregates, use_threads,
                  /*naive=*/false);
}

Result<Datum> GroupByTest(GroupByFunction group_by, const std::vector<Datum>& arguments,
                          const std::vector<Datum>& keys,
                          const std::vector<TestAggregate>& aggregates,
                          bool use_threads) {
  return GroupByTest(group_by, arguments, keys, {}, aggregates, use_threads);
}

template <typename GroupClass>
void TestGroupClassSupportedKeys(
    std::function<Result<std::unique_ptr<GroupClass>>(const std::vector<TypeHolder>&)>
        make_func) {
  ASSERT_OK(make_func({boolean()}));

  ASSERT_OK(make_func({int8(), uint16(), int32(), uint64()}));

  ASSERT_OK(make_func({dictionary(int64(), utf8())}));

  ASSERT_OK(make_func({float16(), float32(), float64()}));

  ASSERT_OK(make_func({utf8(), binary(), large_utf8(), large_binary()}));

  ASSERT_OK(make_func({fixed_size_binary(16), fixed_size_binary(32)}));

  ASSERT_OK(make_func({decimal128(32, 10), decimal256(76, 20)}));

  ASSERT_OK(make_func({date32(), date64()}));

  for (auto unit : {
           TimeUnit::SECOND,
           TimeUnit::MILLI,
           TimeUnit::MICRO,
           TimeUnit::NANO,
       }) {
    ASSERT_OK(make_func({timestamp(unit), duration(unit)}));
  }

  ASSERT_OK(
      make_func({day_time_interval(), month_interval(), month_day_nano_interval()}));

  ASSERT_OK(make_func({null()}));

  ASSERT_RAISES(NotImplemented, make_func({struct_({field("", int64())})}));

  ASSERT_RAISES(NotImplemented, make_func({struct_({})}));

  ASSERT_RAISES(NotImplemented, make_func({list(int32())}));

  ASSERT_RAISES(NotImplemented, make_func({fixed_size_list(int32(), 5)}));

  ASSERT_RAISES(NotImplemented, make_func({dense_union({field("", int32())})}));
}

void TestSegments(std::unique_ptr<RowSegmenter>& segmenter, const ExecSpan& batch,
                  std::vector<Segment> expected_segments) {
  int64_t offset = 0, segment_num = 0;
  for (auto expected_segment : expected_segments) {
    SCOPED_TRACE("segment #" + ToChars(segment_num++));
    ASSERT_OK_AND_ASSIGN(auto segment, segmenter->GetNextSegment(batch, offset));
    ASSERT_EQ(expected_segment, segment);
    offset = segment.offset + segment.length;
  }
}

Result<std::unique_ptr<Grouper>> MakeGrouper(const std::vector<TypeHolder>& key_types) {
  return Grouper::Make(key_types, default_exec_context());
}

Result<std::unique_ptr<RowSegmenter>> MakeRowSegmenter(
    const std::vector<TypeHolder>& key_types) {
  return RowSegmenter::Make(key_types, /*nullable_leys=*/false, default_exec_context());
}

Result<std::unique_ptr<RowSegmenter>> MakeGenericSegmenter(
    const std::vector<TypeHolder>& key_types) {
  return MakeAnyKeysSegmenter(key_types, default_exec_context());
}

}  // namespace

TEST(RowSegmenter, SupportedKeys) {
  TestGroupClassSupportedKeys<RowSegmenter>(MakeRowSegmenter);
}

TEST(RowSegmenter, Basics) {
  std::vector<TypeHolder> bad_types2 = {int32(), float32()};
  std::vector<TypeHolder> types2 = {int32(), int32()};
  std::vector<TypeHolder> bad_types1 = {float32()};
  std::vector<TypeHolder> types1 = {int32()};
  std::vector<TypeHolder> types0 = {};
  auto batch2 = ExecBatchFromJSON(types2, "[[1, 1], [1, 2], [2, 2]]");
  auto batch1 = ExecBatchFromJSON(types1, "[[1], [1], [2]]");
  ExecBatch batch0({}, 3);
  {
    SCOPED_TRACE("offset");
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types0));
    ExecSpan span0(batch0);
    for (int64_t offset : {-1, 4}) {
      EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                      HasSubstr("invalid grouping segmenter offset"),
                                      segmenter->GetNextSegment(span0, offset));
    }
  }
  {
    SCOPED_TRACE("types0 segmenting of batch2");
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types0));
    ExecSpan span2(batch2);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("expected batch size 0 "),
                                    segmenter->GetNextSegment(span2, 0));
    ExecSpan span0(batch0);
    TestSegments(segmenter, span0, {{0, 3, true, true}, {3, 0, true, true}});
  }
  {
    SCOPED_TRACE("bad_types1 segmenting of batch1");
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(bad_types1));
    ExecSpan span1(batch1);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("expected batch value 0 of type "),
                                    segmenter->GetNextSegment(span1, 0));
  }
  {
    SCOPED_TRACE("types1 segmenting of batch2");
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types1));
    ExecSpan span2(batch2);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("expected batch size 1 "),
                                    segmenter->GetNextSegment(span2, 0));
    ExecSpan span1(batch1);
    TestSegments(segmenter, span1,
                 {{0, 2, false, true}, {2, 1, true, false}, {3, 0, true, true}});
  }
  {
    SCOPED_TRACE("bad_types2 segmenting of batch2");
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(bad_types2));
    ExecSpan span2(batch2);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("expected batch value 1 of type "),
                                    segmenter->GetNextSegment(span2, 0));
  }
  {
    SCOPED_TRACE("types2 segmenting of batch1");
    ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types2));
    ExecSpan span1(batch1);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("expected batch size 2 "),
                                    segmenter->GetNextSegment(span1, 0));
    ExecSpan span2(batch2);
    TestSegments(segmenter, span2,
                 {{0, 1, false, true},
                  {1, 1, false, false},
                  {2, 1, true, false},
                  {3, 0, true, true}});
  }
}

TEST(RowSegmenter, NonOrdered) {
  std::vector<TypeHolder> types = {int32()};
  auto batch = ExecBatchFromJSON(types, "[[1], [1], [2], [1], [2]]");
  ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types));
  TestSegments(segmenter, ExecSpan(batch),
               {{0, 2, false, true},
                {2, 1, false, false},
                {3, 1, false, false},
                {4, 1, true, false},
                {5, 0, true, true}});
}

TEST(RowSegmenter, EmptyBatches) {
  std::vector<TypeHolder> types = {int32()};
  std::vector<ExecBatch> batches = {
      ExecBatchFromJSON(types, "[]"),         ExecBatchFromJSON(types, "[]"),
      ExecBatchFromJSON(types, "[[1]]"),      ExecBatchFromJSON(types, "[]"),
      ExecBatchFromJSON(types, "[[1]]"),      ExecBatchFromJSON(types, "[]"),
      ExecBatchFromJSON(types, "[[2], [2]]"), ExecBatchFromJSON(types, "[]"),
  };
  ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types));
  TestSegments(segmenter, ExecSpan(batches[0]), {});
  TestSegments(segmenter, ExecSpan(batches[1]), {});
  TestSegments(segmenter, ExecSpan(batches[2]), {{0, 1, true, true}});
  TestSegments(segmenter, ExecSpan(batches[3]), {});
  TestSegments(segmenter, ExecSpan(batches[4]), {{0, 1, true, true}});
  TestSegments(segmenter, ExecSpan(batches[5]), {});
  TestSegments(segmenter, ExecSpan(batches[6]), {{0, 2, true, false}});
  TestSegments(segmenter, ExecSpan(batches[7]), {});
}

TEST(RowSegmenter, MultipleSegments) {
  std::vector<TypeHolder> types = {int32()};
  auto batch = ExecBatchFromJSON(types, "[[1], [1], [2], [5], [3], [3], [5], [5], [4]]");
  ASSERT_OK_AND_ASSIGN(auto segmenter, MakeRowSegmenter(types));
  TestSegments(segmenter, ExecSpan(batch),
               {{0, 2, false, true},
                {2, 1, false, false},
                {3, 1, false, false},
                {4, 2, false, false},
                {6, 2, false, false},
                {8, 1, true, false},
                {9, 0, true, true}});
}

namespace {

void TestRowSegmenterConstantBatch(
    std::function<ArgShape(size_t i)> shape_func,
    std::function<Result<std::unique_ptr<RowSegmenter>>(const std::vector<TypeHolder>&)>
        make_segmenter) {
  constexpr size_t n = 3, repetitions = 3;
  std::vector<TypeHolder> types = {int32(), int32(), int32()};
  std::vector<ArgShape> shapes(n);
  for (size_t i = 0; i < n; i++) shapes[i] = shape_func(i);
  auto full_batch = ExecBatchFromJSON(types, shapes, "[[1, 1, 1], [1, 1, 1], [1, 1, 1]]");
  auto test_by_size = [&](size_t size) -> Status {
    SCOPED_TRACE("constant-batch with " + ToChars(size) + " key(s)");
    std::vector<Datum> values(full_batch.values.begin(),
                              full_batch.values.begin() + size);
    ExecBatch batch(values, full_batch.length);
    std::vector<TypeHolder> key_types(types.begin(), types.begin() + size);
    ARROW_ASSIGN_OR_RAISE(auto segmenter, make_segmenter(key_types));
    for (size_t i = 0; i < repetitions; i++) {
      TestSegments(segmenter, ExecSpan(batch), {{0, 3, true, true}, {3, 0, true, true}});
      ARROW_RETURN_NOT_OK(segmenter->Reset());
    }
    return Status::OK();
  };
  for (size_t i = 0; i <= 3; i++) {
    ASSERT_OK(test_by_size(i));
  }
}

}  // namespace

TEST(RowSegmenter, ConstantArrayBatch) {
  TestRowSegmenterConstantBatch([](size_t i) { return ArgShape::ARRAY; },
                                MakeRowSegmenter);
}

TEST(RowSegmenter, ConstantScalarBatch) {
  TestRowSegmenterConstantBatch([](size_t i) { return ArgShape::SCALAR; },
                                MakeRowSegmenter);
}

TEST(RowSegmenter, ConstantMixedBatch) {
  TestRowSegmenterConstantBatch(
      [](size_t i) { return i % 2 == 0 ? ArgShape::SCALAR : ArgShape::ARRAY; },
      MakeRowSegmenter);
}

TEST(RowSegmenter, ConstantArrayBatchWithAnyKeysSegmenter) {
  TestRowSegmenterConstantBatch([](size_t i) { return ArgShape::ARRAY; },
                                MakeGenericSegmenter);
}

TEST(RowSegmenter, ConstantScalarBatchWithAnyKeysSegmenter) {
  TestRowSegmenterConstantBatch([](size_t i) { return ArgShape::SCALAR; },
                                MakeGenericSegmenter);
}

TEST(RowSegmenter, ConstantMixedBatchWithAnyKeysSegmenter) {
  TestRowSegmenterConstantBatch(
      [](size_t i) { return i % 2 == 0 ? ArgShape::SCALAR : ArgShape::ARRAY; },
      MakeGenericSegmenter);
}

TEST(RowSegmenter, RowConstantBatch) {
  constexpr size_t n = 3;
  std::vector<TypeHolder> types = {int32(), int32(), int32()};
  auto full_batch = ExecBatchFromJSON(types, "[[1, 1, 1], [2, 2, 2], [3, 3, 3]]");
  std::vector<Segment> expected_segments_for_size_0 = {{0, 3, true, true},
                                                       {3, 0, true, true}};
  std::vector<Segment> expected_segments = {
      {0, 1, false, true}, {1, 1, false, false}, {2, 1, true, false}, {3, 0, true, true}};
  auto test_by_size = [&](size_t size) -> Status {
    SCOPED_TRACE("constant-batch with " + ToChars(size) + " key(s)");
    std::vector<Datum> values(full_batch.values.begin(),
                              full_batch.values.begin() + size);
    ExecBatch batch(values, full_batch.length);
    std::vector<TypeHolder> key_types(types.begin(), types.begin() + size);
    ARROW_ASSIGN_OR_RAISE(auto segmenter, MakeRowSegmenter(key_types));
    TestSegments(segmenter, ExecSpan(batch),
                 size == 0 ? expected_segments_for_size_0 : expected_segments);
    return Status::OK();
  };
  for (size_t i = 0; i <= n; i++) {
    ASSERT_OK(test_by_size(i));
  }
}

TEST(Grouper, SupportedKeys) { TestGroupClassSupportedKeys<Grouper>(MakeGrouper); }

struct TestGrouper {
  explicit TestGrouper(std::vector<TypeHolder> types, std::vector<ArgShape> shapes = {})
      : types_(std::move(types)), shapes_(std::move(shapes)) {
    grouper_ = Grouper::Make(types_).ValueOrDie();

    FieldVector fields;
    for (const auto& type : types_) {
      fields.push_back(field("", type.GetSharedPtr()));
    }
    key_schema_ = schema(std::move(fields));
  }

  void ExpectConsume(const std::string& key_json, const std::string& expected) {
    auto expected_arr = ArrayFromJSON(uint32(), expected);
    if (shapes_.size() > 0) {
      ExpectConsume(ExecBatchFromJSON(types_, shapes_, key_json), expected_arr);
    } else {
      ExpectConsume(ExecBatchFromJSON(types_, key_json), expected_arr);
    }
  }

  void ExpectConsume(const std::vector<Datum>& key_values, Datum expected) {
    ASSERT_OK_AND_ASSIGN(auto key_batch, ExecBatch::Make(key_values));
    ExpectConsume(key_batch, expected);
  }

  void ExpectConsume(const ExecBatch& key_batch, Datum expected) {
    Datum ids;
    ConsumeAndValidate(key_batch, &ids);
    AssertEquivalentIds(expected, ids);
  }

  void ExpectUniques(const ExecBatch& uniques) {
    EXPECT_THAT(grouper_->GetUniques(), ResultWith(Eq(uniques)));
  }

  void ExpectUniques(const std::string& uniques_json) {
    if (shapes_.size() > 0) {
      ExpectUniques(ExecBatchFromJSON(types_, shapes_, uniques_json));
    } else {
      ExpectUniques(ExecBatchFromJSON(types_, uniques_json));
    }
  }

  void AssertEquivalentIds(const Datum& expected, const Datum& actual) {
    auto left = expected.make_array();
    auto right = actual.make_array();
    ASSERT_EQ(left->length(), right->length()) << "#ids unequal";
    int64_t num_ids = left->length();
    auto left_data = left->data();
    auto right_data = right->data();
    auto left_ids = reinterpret_cast<const uint32_t*>(left_data->buffers[1]->data());
    auto right_ids = reinterpret_cast<const uint32_t*>(right_data->buffers[1]->data());
    uint32_t max_left_id = 0;
    uint32_t max_right_id = 0;
    for (int64_t i = 0; i < num_ids; ++i) {
      if (left_ids[i] > max_left_id) {
        max_left_id = left_ids[i];
      }
      if (right_ids[i] > max_right_id) {
        max_right_id = right_ids[i];
      }
    }
    std::vector<bool> right_to_left_present(max_right_id + 1, false);
    std::vector<bool> left_to_right_present(max_left_id + 1, false);
    std::vector<uint32_t> right_to_left(max_right_id + 1);
    std::vector<uint32_t> left_to_right(max_left_id + 1);
    for (int64_t i = 0; i < num_ids; ++i) {
      uint32_t left_id = left_ids[i];
      uint32_t right_id = right_ids[i];
      if (!left_to_right_present[left_id]) {
        left_to_right[left_id] = right_id;
        left_to_right_present[left_id] = true;
      }
      if (!right_to_left_present[right_id]) {
        right_to_left[right_id] = left_id;
        right_to_left_present[right_id] = true;
      }
      ASSERT_EQ(left_id, right_to_left[right_id]);
      ASSERT_EQ(right_id, left_to_right[left_id]);
    }
  }

  void ConsumeAndValidate(const ExecBatch& key_batch, Datum* ids = nullptr) {
    ASSERT_OK_AND_ASSIGN(Datum id_batch, grouper_->Consume(ExecSpan(key_batch)));

    ValidateConsume(key_batch, id_batch);

    if (ids) {
      *ids = std::move(id_batch);
    }
  }

  void ValidateConsume(const ExecBatch& key_batch, const Datum& id_batch) {
    if (uniques_.length == -1) {
      ASSERT_OK_AND_ASSIGN(uniques_, grouper_->GetUniques());
    } else if (static_cast<int64_t>(grouper_->num_groups()) > uniques_.length) {
      ASSERT_OK_AND_ASSIGN(ExecBatch new_uniques, grouper_->GetUniques());

      // check that uniques_ are prefixes of new_uniques
      for (int i = 0; i < uniques_.num_values(); ++i) {
        auto new_unique = new_uniques[i].make_array();
        ValidateOutput(*new_unique);

        AssertDatumsEqual(uniques_[i], new_unique->Slice(0, uniques_.length),
                          /*verbose=*/true);
      }

      uniques_ = std::move(new_uniques);
    }

    // check that the ids encode an equivalent key sequence
    auto ids = id_batch.make_array();
    ValidateOutput(*ids);

    for (int i = 0; i < key_batch.num_values(); ++i) {
      SCOPED_TRACE(ToChars(i) + "th key array");
      auto original =
          key_batch[i].is_array()
              ? key_batch[i].make_array()
              : *MakeArrayFromScalar(*key_batch[i].scalar(), key_batch.length);
      ASSERT_OK_AND_ASSIGN(auto encoded, Take(*uniques_[i].make_array(), *ids));
      AssertArraysEqual(*original, *encoded, /*verbose=*/true,
                        EqualOptions().nans_equal(true));
    }
  }

  std::vector<TypeHolder> types_;
  std::vector<ArgShape> shapes_;
  std::shared_ptr<Schema> key_schema_;
  std::unique_ptr<Grouper> grouper_;
  ExecBatch uniques_ = ExecBatch({}, -1);
};

TEST(Grouper, BooleanKey) {
  TestGrouper g({boolean()});

  g.ExpectConsume("[[true], [true]]", "[0, 0]");

  g.ExpectConsume("[[true], [true]]", "[0, 0]");

  g.ExpectConsume("[[false], [null]]", "[1, 2]");

  g.ExpectConsume("[[true], [false], [true], [false], [null], [false], [null]]",
                  "[0, 1, 0, 1, 2, 1, 2]");
}

TEST(Grouper, NumericKey) {
  for (auto ty : {
           uint8(),
           int8(),
           uint16(),
           int16(),
           uint32(),
           int32(),
           uint64(),
           int64(),
           float16(),
           float32(),
           float64(),
       }) {
    SCOPED_TRACE("key type: " + ty->ToString());

    TestGrouper g({ty});

    g.ExpectConsume("[[3], [3]]", "[0, 0]");
    g.ExpectUniques("[[3]]");

    g.ExpectConsume("[[3], [3]]", "[0, 0]");
    g.ExpectUniques("[[3]]");

    g.ExpectConsume("[[27], [81], [81]]", "[1, 2, 2]");
    g.ExpectUniques("[[3], [27], [81]]");

    g.ExpectConsume("[[3], [27], [3], [27], [null], [81], [27], [81]]",
                    "[0, 1, 0, 1, 3, 2, 1, 2]");
    g.ExpectUniques("[[3], [27], [81], [null]]");
  }
}

TEST(Grouper, FloatingPointKey) {
  TestGrouper g({float32()});

  // -0.0 hashes differently from 0.0
  g.ExpectConsume("[[0.0], [-0.0]]", "[0, 1]");

  g.ExpectConsume("[[Inf], [-Inf]]", "[2, 3]");

  // assert(!(NaN == NaN)) does not cause spurious new groups
  g.ExpectConsume("[[NaN], [NaN]]", "[4, 4]");

  // TODO(bkietz) test denormal numbers, more NaNs
}

TEST(Grouper, StringKey) {
  for (auto ty : {utf8(), large_utf8(), fixed_size_binary(2)}) {
    SCOPED_TRACE("key type: " + ty->ToString());

    TestGrouper g({ty});

    g.ExpectConsume(R"([["eh"], ["eh"]])", "[0, 0]");

    g.ExpectConsume(R"([["eh"], ["eh"]])", "[0, 0]");

    g.ExpectConsume(R"([["be"], [null]])", "[1, 2]");
  }
}

TEST(Grouper, DictKey) {
  TestGrouper g({dictionary(int32(), utf8())});

  // For dictionary keys, all batches must share a single dictionary.
  // Eventually, differing dictionaries will be unified and indices transposed
  // during encoding to relieve this restriction.
  const auto dict = ArrayFromJSON(utf8(), R"(["ex", "why", "zee", null])");

  auto WithIndices = [&](const std::string& indices) {
    return Datum(*DictionaryArray::FromArrays(ArrayFromJSON(int32(), indices), dict));
  };

  // NB: null index is not considered equivalent to index=3 (which encodes null in dict)
  g.ExpectConsume({WithIndices("           [3, 1, null, 0, 2]")},
                  ArrayFromJSON(uint32(), "[0, 1, 2, 3, 4]"));

  g = TestGrouper({dictionary(int32(), utf8())});

  g.ExpectConsume({WithIndices("           [0, 1, 2, 3, null]")},
                  ArrayFromJSON(uint32(), "[0, 1, 2, 3, 4]"));

  g.ExpectConsume({WithIndices("           [3, 1, null, 0, 2]")},
                  ArrayFromJSON(uint32(), "[3, 1, 4,    0, 2]"));

  auto dict_arr = *DictionaryArray::FromArrays(
      ArrayFromJSON(int32(), "[0, 1]"),
      ArrayFromJSON(utf8(), R"(["different", "dictionary"])"));
  ExecSpan dict_span({*dict_arr->data()}, 2);
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  HasSubstr("Unifying differing dictionaries"),
                                  g.grouper_->Consume(dict_span));
}

TEST(Grouper, StringInt64Key) {
  TestGrouper g({utf8(), int64()});

  g.ExpectConsume(R"([["eh", 0], ["eh", 0]])", "[0, 0]");

  g.ExpectConsume(R"([["eh", 0], ["eh", null]])", "[0, 1]");

  g.ExpectConsume(R"([["eh", 1], ["bee", 1]])", "[2, 3]");

  g.ExpectConsume(R"([["eh", null], ["bee", 1]])", "[1, 3]");

  g = TestGrouper({utf8(), int64()});

  g.ExpectConsume(R"([
    ["ex",  0],
    ["ex",  0],
    ["why", 0],
    ["ex",  1],
    ["why", 0],
    ["ex",  1],
    ["ex",  0],
    ["why", 1]
  ])",
                  "[0, 0, 1, 2, 1, 2, 0, 3]");

  g.ExpectConsume(R"([
    ["ex",  0],
    [null,  0],
    [null,  0],
    ["ex",  1],
    [null,  null],
    ["ex",  1],
    ["ex",  0],
    ["why", null]
  ])",
                  "[0, 4, 4, 2, 5, 2, 0, 6]");
}

TEST(Grouper, DoubleStringInt64Key) {
  TestGrouper g({float64(), utf8(), int64()});

  g.ExpectConsume(R"([[1.5, "eh", 0], [1.5, "eh", 0]])", "[0, 0]");

  g.ExpectConsume(R"([[1.5, "eh", 0], [1.5, "eh", 0]])", "[0, 0]");

  g.ExpectConsume(R"([[1.0, "eh", 0], [1.0, "be", null]])", "[1, 2]");

  // note: -0 and +0 hash differently
  g.ExpectConsume(R"([[-0.0, "be", 7], [0.0, "be", 7]])", "[3, 4]");
}

TEST(Grouper, RandomInt64Keys) {
  TestGrouper g({int64()});
  for (int i = 0; i < 4; ++i) {
    SCOPED_TRACE(ToChars(i) + "th key batch");

    ExecBatch key_batch{
        *random::GenerateBatch(g.key_schema_->fields(), 1 << 12, 0xDEADBEEF)};
    g.ConsumeAndValidate(key_batch);
  }
}

TEST(Grouper, RandomStringInt64Keys) {
  TestGrouper g({utf8(), int64()});
  for (int i = 0; i < 4; ++i) {
    SCOPED_TRACE(ToChars(i) + "th key batch");

    ExecBatch key_batch{
        *random::GenerateBatch(g.key_schema_->fields(), 1 << 12, 0xDEADBEEF)};
    g.ConsumeAndValidate(key_batch);
  }
}

TEST(Grouper, RandomStringInt64DoubleInt32Keys) {
  TestGrouper g({utf8(), int64(), float64(), int32()});
  for (int i = 0; i < 4; ++i) {
    SCOPED_TRACE(ToChars(i) + "th key batch");

    ExecBatch key_batch{
        *random::GenerateBatch(g.key_schema_->fields(), 1 << 12, 0xDEADBEEF)};
    g.ConsumeAndValidate(key_batch);
  }
}

TEST(Grouper, NullKeys) {
  TestGrouper g({null()});
  g.ExpectConsume("[[null], [null]]", "[0, 0]");
}

TEST(Grouper, MultipleNullKeys) {
  TestGrouper g({null(), null(), null(), null()});
  g.ExpectConsume("[[null, null, null, null], [null, null, null, null]]", "[0, 0]");
}

TEST(Grouper, Int64NullKeys) {
  TestGrouper g({int64(), null()});
  g.ExpectConsume("[[1, null], [2, null], [1, null]]", "[0, 1, 0]");
}

TEST(Grouper, StringNullKeys) {
  TestGrouper g({utf8(), null()});
  g.ExpectConsume(R"([["be", null], ["eh", null]])", "[0, 1]");
}

TEST(Grouper, DoubleNullStringKey) {
  TestGrouper g({float64(), null(), utf8()});

  g.ExpectConsume(R"([[1.5, null, "eh"], [1.5, null, "eh"]])", "[0, 0]");
  g.ExpectConsume(R"([[null, null, "eh"], [1.0, null, null]])", "[1, 2]");
  g.ExpectConsume(R"([
    [1.0,  null, "wh"],
    [4.4,  null, null],
    [5.2,  null, "eh"],
    [6.5,  null, "be"],
    [7.3,  null, null],
    [1.0,  null, "wh"],
    [9.1,  null, "eh"],
    [10.2, null, "be"],
    [1.0, null, null]
  ])",
                  "[3, 4, 5, 6, 7, 3, 8, 9, 2]");
}

TEST(Grouper, EmptyNullKeys) {
  TestGrouper g({null()});
  g.ExpectConsume("[]", "[]");
}

TEST(Grouper, MakeGroupings) {
  auto ExpectGroupings = [](std::string ids_json, std::string expected_json) {
    auto ids = checked_pointer_cast<UInt32Array>(ArrayFromJSON(uint32(), ids_json));
    auto expected = ArrayFromJSON(list(int32()), expected_json);

    auto num_groups = static_cast<uint32_t>(expected->length());
    ASSERT_OK_AND_ASSIGN(auto actual, Grouper::MakeGroupings(*ids, num_groups));
    AssertArraysEqual(*expected, *actual, /*verbose=*/true);

    // validate ApplyGroupings
    ASSERT_OK_AND_ASSIGN(auto grouped_ids, Grouper::ApplyGroupings(*actual, *ids));

    for (uint32_t group = 0; group < num_groups; ++group) {
      auto ids_slice = checked_pointer_cast<UInt32Array>(grouped_ids->value_slice(group));
      for (auto slot : *ids_slice) {
        EXPECT_EQ(slot, group);
      }
    }
  };

  ExpectGroupings("[]", "[[]]");

  ExpectGroupings("[0, 0, 0]", "[[0, 1, 2]]");

  ExpectGroupings("[0, 0, 0, 1, 1, 2]", "[[0, 1, 2], [3, 4], [5], []]");

  ExpectGroupings("[2, 1, 2, 1, 1, 2]", "[[], [1, 3, 4], [0, 2, 5], [], []]");

  ExpectGroupings("[2, 2, 5, 5, 2, 3]", "[[], [], [0, 1, 4], [5], [], [2, 3], [], []]");

  auto ids = checked_pointer_cast<UInt32Array>(ArrayFromJSON(uint32(), "[0, null, 1]"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, HasSubstr("MakeGroupings with null ids"),
                                  Grouper::MakeGroupings(*ids, 5));
}

TEST(Grouper, ScalarValues) {
  // large_utf8 forces GrouperImpl over GrouperFastImpl
  for (const auto& str_type : {utf8(), large_utf8()}) {
    {
      TestGrouper g(
          {boolean(), int32(), decimal128(3, 2), decimal256(3, 2), fixed_size_binary(2),
           str_type, int32()},
          {ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::SCALAR,
           ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::ARRAY});
      g.ExpectConsume(
          R"([
[true, 1, "1.00", "2.00", "ab", "foo", 2],
[true, 1, "1.00", "2.00", "ab", "foo", 2],
[true, 1, "1.00", "2.00", "ab", "foo", 3]
])",
          "[0, 0, 1]");
    }
    {
      auto dict_type = dictionary(int32(), utf8());
      TestGrouper g({dict_type, str_type}, {ArgShape::SCALAR, ArgShape::SCALAR});
      const auto dict = R"(["foo", null])";
      g.ExpectConsume(
          {DictScalarFromJSON(dict_type, "0", dict), ScalarFromJSON(str_type, R"("")")},
          ArrayFromJSON(uint32(), "[0]"));
      g.ExpectConsume(
          {DictScalarFromJSON(dict_type, "1", dict), ScalarFromJSON(str_type, R"("")")},
          ArrayFromJSON(uint32(), "[1]"));
    }
  }
}

void TestSegmentKey(GroupByFunction group_by, const std::shared_ptr<Table>& table,
                    Datum output, const std::vector<Datum>& segment_keys);

class GroupBy : public ::testing::TestWithParam<GroupByFunction> {
 public:
  void ValidateGroupBy(const std::vector<Aggregate>& aggregates,
                       std::vector<Datum> arguments, std::vector<Datum> keys,
                       bool naive = true) {
    compute::ValidateGroupBy(GetParam(), aggregates, arguments, keys, naive);
  }

  Result<Datum> GroupByTest(const std::vector<Datum>& arguments,
                            const std::vector<Datum>& keys,
                            const std::vector<Datum>& segment_keys,
                            const std::vector<TestAggregate>& aggregates,
                            bool use_threads) {
    return compute::GroupByTest(GetParam(), arguments, keys, segment_keys, aggregates,
                                use_threads);
  }

  Result<Datum> GroupByTest(const std::vector<Datum>& arguments,
                            const std::vector<Datum>& keys,
                            const std::vector<TestAggregate>& aggregates,
                            bool use_threads) {
    return compute::GroupByTest(GetParam(), arguments, keys, aggregates, use_threads);
  }

  Result<Datum> AltGroupBy(const std::vector<Datum>& arguments,
                           const std::vector<Datum>& keys,
                           const std::vector<Datum>& segment_keys,
                           const std::vector<Aggregate>& aggregates,
                           bool use_threads = false) {
    return GetParam()(arguments, keys, segment_keys, aggregates, use_threads,
                      /*naive=*/false);
  }

  void TestSegmentKey(const std::shared_ptr<Table>& table, Datum output,
                      const std::vector<Datum>& segment_keys) {
    return compute::TestSegmentKey(GetParam(), table, output, segment_keys);
  }
};

TEST_P(GroupBy, Errors) {
  auto batch = RecordBatchFromJSON(
      schema({field("argument", float64()), field("group_id", uint32())}), R"([
    [1.0,   1],
    [null,  1],
    [0.0,   2],
    [null,  3],
    [4.0,   0],
    [3.25,  1],
    [0.125, 2],
    [-0.25, 2],
    [0.75,  0],
    [null,  3]
  ])");

  EXPECT_THAT(CallFunction("hash_sum", {batch->GetColumnByName("argument"),
                                        batch->GetColumnByName("group_id")}),
              Raises(StatusCode::NotImplemented,
                     HasSubstr("Direct execution of HASH_AGGREGATE functions")));
}

TEST_P(GroupBy, NoBatches) {
  // Regression test for ARROW-14583: handle when no batches are
  // passed to the group by node before finalizing
  auto table =
      TableFromJSON(schema({field("argument", float64()), field("key", int64())}), {});
  ASSERT_OK_AND_ASSIGN(
      Datum aggregated_and_grouped,
      GroupByTest({table->GetColumnByName("argument")}, {table->GetColumnByName("key")},
                  {
                      {"hash_count", nullptr},
                  },
                  /*use_threads=*/true));
  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("hash_count", int64()),
                                      field("key_0", int64()),
                                  }),
                                  R"([])"),
                    aggregated_and_grouped, /*verbose=*/true);
}

namespace {
void SortBy(std::vector<std::string> names, Datum* aggregated_and_grouped) {
  SortOptions options;
  for (auto&& name : names) {
    options.sort_keys.emplace_back(std::move(name), SortOrder::Ascending);
  }

  ASSERT_OK_AND_ASSIGN(
      auto batch, RecordBatch::FromStructArray(aggregated_and_grouped->make_array()));

  // decode any dictionary columns:
  ArrayVector cols = batch->columns();
  for (auto& col : cols) {
    if (col->type_id() != Type::DICTIONARY) continue;

    auto dict_col = checked_cast<const DictionaryArray*>(col.get());
    ASSERT_OK_AND_ASSIGN(col, Take(*dict_col->dictionary(), *dict_col->indices()));
  }
  batch = RecordBatch::Make(batch->schema(), batch->num_rows(), std::move(cols));

  ASSERT_OK_AND_ASSIGN(Datum sort_indices, SortIndices(batch, options));

  ASSERT_OK_AND_ASSIGN(*aggregated_and_grouped,
                       Take(*aggregated_and_grouped, sort_indices));
}
}  // namespace

TEST_P(GroupBy, CountOnly) {
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

    auto table =
        TableFromJSON(schema({field("argument", float64()), field("key", int64())}), {R"([
    [1.0,   1],
    [null,  1]
                        ])",
                                                                                      R"([
    [0.0,   2],
    [null,  3],
    [4.0,   null],
    [3.25,  1],
    [0.125, 2]
                        ])",
                                                                                      R"([
    [-0.25, 2],
    [0.75,  null],
    [null,  3]
                        ])"});

    ASSERT_OK_AND_ASSIGN(
        Datum aggregated_and_grouped,
        GroupByTest({table->GetColumnByName("argument")}, {table->GetColumnByName("key")},
                    {
                        {"hash_count", nullptr},
                    },
                    use_threads));
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_count", int64()),
                                        field("key_0", int64()),
                                    }),
                                    R"([
    [2,   1],
    [3,   2],
    [0,   3],
    [2,   null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, CountScalar) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON({int32(), int64()}, {ArgShape::SCALAR, ArgShape::ARRAY},
                        "[[1, 1], [1, 1], [1, 2], [1, 3]]"),
      ExecBatchFromJSON({int32(), int64()}, {ArgShape::SCALAR, ArgShape::ARRAY},
                        "[[null, 1], [null, 1], [null, 2], [null, 3]]"),
      ExecBatchFromJSON({int32(), int64()}, "[[2, 1], [3, 2], [4, 3]]"),
  };
  input.schema = schema({field("argument", int32()), field("key", int64())});

  auto skip_nulls = std::make_shared<CountOptions>(CountOptions::ONLY_VALID);
  auto keep_nulls = std::make_shared<CountOptions>(CountOptions::ONLY_NULL);
  auto count_all = std::make_shared<CountOptions>(CountOptions::ALL);
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum actual, RunGroupBy(input, {"key"},
                                 {
                                     {"hash_count", skip_nulls, "argument", "hash_count"},
                                     {"hash_count", keep_nulls, "argument", "hash_count"},
                                     {"hash_count", count_all, "argument", "hash_count"},
                                 },
                                 use_threads));

    Datum expected = ArrayFromJSON(struct_({
                                       field("hash_count", int64()),
                                       field("hash_count", int64()),
                                       field("hash_count", int64()),
                                       field("key", int64()),
                                   }),
                                   R"([
      [3, 2, 5, 1],
      [2, 1, 3, 2],
      [2, 1, 3, 3]
    ])");
    AssertDatumsApproxEqual(expected, actual, /*verbose=*/true);
  }
}

TEST_P(GroupBy, SumOnly) {
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

    auto table =
        TableFromJSON(schema({field("argument", float64()), field("key", int64())}), {R"([
    [1.0,   1],
    [null,  1]
                        ])",
                                                                                      R"([
    [0.0,   2],
    [null,  3],
    [4.0,   null],
    [3.25,  1],
    [0.125, 2]
                        ])",
                                                                                      R"([
    [-0.25, 2],
    [0.75,  null],
    [null,  3]
                        ])"});

    ASSERT_OK_AND_ASSIGN(
        Datum aggregated_and_grouped,
        GroupByTest({table->GetColumnByName("argument")}, {table->GetColumnByName("key")},
                    {
                        {"hash_sum", nullptr},
                    },
                    use_threads));
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_sum", float64()),
                                        field("key_0", int64()),
                                    }),
                                    R"([
    [4.25,   1],
    [-0.125, 2],
    [null,   3],
    [4.75,   null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, SumMeanProductDecimal) {
  auto in_schema = schema({
      field("argument0", decimal128(3, 2)),
      field("argument1", decimal256(3, 2)),
      field("key", int64()),
  });

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

    auto table = TableFromJSON(in_schema, {R"([
    ["1.00",  "1.00",  1],
    [null,    null,    1]
  ])",
                                           R"([
    ["0.00",  "0.00",  2],
    [null,    null,    3],
    ["4.00",  "4.00",  null],
    ["3.25",  "3.25",  1],
    ["0.12",  "0.12",  2]
  ])",
                                           R"([
    ["-0.25", "-0.25", 2],
    ["0.75",  "0.75",  null],
    [null,    null,    3],
    ["1.01",  "1.01",  4],
    ["1.01",  "1.01",  4],
    ["1.01",  "1.01",  4],
    ["1.02",  "1.02",  4]
  ])"});

    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 table->GetColumnByName("argument0"),
                                 table->GetColumnByName("argument1"),
                                 table->GetColumnByName("argument0"),
                                 table->GetColumnByName("argument1"),
                                 table->GetColumnByName("argument0"),
                                 table->GetColumnByName("argument1"),
                             },
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_sum", nullptr},
                                 {"hash_sum", nullptr},
                                 {"hash_mean", nullptr},
                                 {"hash_mean", nullptr},
                                 {"hash_product", nullptr},
                                 {"hash_product", nullptr},
                             },
                             use_threads));
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_sum", decimal128(3, 2)),
                                        field("hash_sum", decimal256(3, 2)),
                                        field("hash_mean", decimal128(3, 2)),
                                        field("hash_mean", decimal256(3, 2)),
                                        field("hash_product", decimal128(3, 2)),
                                        field("hash_product", decimal256(3, 2)),
                                        field("key_0", int64()),
                                    }),
                                    R"([
    ["4.25",  "4.25",  "2.13",  "2.13",  "3.25", "3.25", 1],
    ["-0.13", "-0.13", "-0.04", "-0.04", "0.00", "0.00", 2],
    [null,    null,    null,    null,    null,   null,   3],
    ["4.05",  "4.05",  "1.01",  "1.01",  "1.05", "1.05", 4],
    ["4.75",  "4.75",  "2.38",  "2.38",  "3.00", "3.00", null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, MeanOnly) {
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

    auto table =
        TableFromJSON(schema({field("argument", float64()), field("key", int64())}), {R"([
    [1.0,   1],
    [null,  1]
                        ])",
                                                                                      R"([
    [0.0,   2],
    [null,  3],
    [4.0,   null],
    [3.25,  1],
    [0.125, 2]
                        ])",
                                                                                      R"([
    [-0.25, 2],
    [0.75,  null],
    [null,  3]
                        ])"});

    auto min_count =
        std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/3);
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest({table->GetColumnByName("argument"),
                                      table->GetColumnByName("argument")},
                                     {table->GetColumnByName("key")},
                                     {
                                         {"hash_mean", nullptr},
                                         {"hash_mean", min_count},
                                     },
                                     use_threads));
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsApproxEqual(ArrayFromJSON(struct_({
                                              field("hash_mean", float64()),
                                              field("hash_mean", float64()),
                                              field("key_0", int64()),
                                          }),
                                          R"([
    [2.125,                 null,                  1],
    [-0.041666666666666664, -0.041666666666666664, 2],
    [null,                  null,                  3],
    [2.375,                 null,                  null]
  ])"),
                            aggregated_and_grouped,
                            /*verbose=*/true);
  }
}

TEST_P(GroupBy, SumMeanProductScalar) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON({int32(), int64()}, {ArgShape::SCALAR, ArgShape::ARRAY},

                        "[[1, 1], [1, 1], [1, 2], [1, 3]]"),
      ExecBatchFromJSON({int32(), int64()}, {ArgShape::SCALAR, ArgShape::ARRAY},
                        "[[null, 1], [null, 1], [null, 2], [null, 3]]"),
      ExecBatchFromJSON({int32(), int64()}, "[[2, 1], [3, 2], [4, 3]]"),
  };
  input.schema = schema({field("argument", int32()), field("key", int64())});

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum actual,
        RunGroupBy(input, {"key"},
                   {
                       {"hash_sum", nullptr, "argument", "hash_sum"},
                       {"hash_mean", nullptr, "argument", "hash_mean"},
                       {"hash_product", nullptr, "argument", "hash_product"},
                   },
                   use_threads));
    Datum expected = ArrayFromJSON(struct_({
                                       field("hash_sum", int64()),
                                       field("hash_mean", float64()),
                                       field("hash_product", int64()),
                                       field("key", int64()),
                                   }),
                                   R"([
      [4, 1.333333, 2, 1],
      [4, 2,        3, 2],
      [5, 2.5,      4, 3]
    ])");
    AssertDatumsApproxEqual(expected, actual, /*verbose=*/true);
  }
}

TEST_P(GroupBy, VarianceAndStddev) {
  auto batch = RecordBatchFromJSON(
      schema({field("argument", int32()), field("key", int64())}), R"([
    [1,   1],
    [null,  1],
    [0,   2],
    [null,  3],
    [4,   null],
    [3,  1],
    [0, 2],
    [-1, 2],
    [1,  null],
    [null,  3]
  ])");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       GroupByTest(
                           {
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                           },
                           {
                               batch->GetColumnByName("key"),
                           },
                           {},
                           {
                               {"hash_variance", nullptr},
                               {"hash_stddev", nullptr},
                           },
                           false));

  AssertDatumsApproxEqual(ArrayFromJSON(struct_({
                                            field("hash_variance", float64()),
                                            field("hash_stddev", float64()),
                                            field("key_0", int64()),
                                        }),
                                        R"([
    [1.0,                 1.0,                1],
    [0.22222222222222224, 0.4714045207910317, 2],
    [null,                null,               3],
    [2.25,                1.5,                null]
  ])"),
                          aggregated_and_grouped,
                          /*verbose=*/true);

  batch = RecordBatchFromJSON(
      schema({field("argument", float64()), field("key", int64())}), R"([
    [1.0,   1],
    [null,  1],
    [0.0,   2],
    [null,  3],
    [4.0,   null],
    [3.0,  1],
    [0.0, 2],
    [-1.0, 2],
    [1.0,  null],
    [null,  3]
  ])");

  ASSERT_OK_AND_ASSIGN(aggregated_and_grouped, GroupByTest(
                                                   {
                                                       batch->GetColumnByName("argument"),
                                                       batch->GetColumnByName("argument"),
                                                   },
                                                   {
                                                       batch->GetColumnByName("key"),
                                                   },
                                                   {},
                                                   {
                                                       {"hash_variance", nullptr},
                                                       {"hash_stddev", nullptr},
                                                   },
                                                   false));

  AssertDatumsApproxEqual(ArrayFromJSON(struct_({
                                            field("hash_variance", float64()),
                                            field("hash_stddev", float64()),
                                            field("key_0", int64()),
                                        }),
                                        R"([
    [1.0,                 1.0,                1],
    [0.22222222222222224, 0.4714045207910317, 2],
    [null,                null,               3],
    [2.25,                1.5,                null]
  ])"),
                          aggregated_and_grouped,
                          /*verbose=*/true);

  // Test ddof
  auto variance_options = std::make_shared<VarianceOptions>(/*ddof=*/2);
  ASSERT_OK_AND_ASSIGN(aggregated_and_grouped,
                       GroupByTest(
                           {
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                           },
                           {
                               batch->GetColumnByName("key"),
                           },
                           {},
                           {
                               {"hash_variance", variance_options},
                               {"hash_stddev", variance_options},
                           },
                           false));

  AssertDatumsApproxEqual(ArrayFromJSON(struct_({
                                            field("hash_variance", float64()),
                                            field("hash_stddev", float64()),
                                            field("key_0", int64()),
                                        }),
                                        R"([
    [null,                null,               1],
    [0.6666666666666667,  0.816496580927726,  2],
    [null,                null,               3],
    [null,                null,               null]
  ])"),
                          aggregated_and_grouped,
                          /*verbose=*/true);
}

TEST_P(GroupBy, VarianceAndStddevDecimal) {
  auto batch = RecordBatchFromJSON(
      schema({field("argument0", decimal128(3, 2)), field("argument1", decimal128(3, 2)),
              field("key", int64())}),
      R"([
    ["1.00",  "1.00",  1],
    [null,    null,    1],
    ["0.00",  "0.00",  2],
    ["4.00",  "4.00",  null],
    ["3.00",  "3.00",  1],
    ["0.00",  "0.00",  2],
    ["-1.00", "-1.00", 2],
    ["1.00",  "1.00",  null]
  ])");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       GroupByTest(
                           {
                               batch->GetColumnByName("argument0"),
                               batch->GetColumnByName("argument0"),
                               batch->GetColumnByName("argument1"),
                               batch->GetColumnByName("argument1"),
                           },
                           {
                               batch->GetColumnByName("key"),
                           },
                           {},
                           {
                               {"hash_variance", nullptr},
                               {"hash_stddev", nullptr},
                               {"hash_variance", nullptr},
                               {"hash_stddev", nullptr},
                           },
                           false));

  AssertDatumsApproxEqual(ArrayFromJSON(struct_({
                                            field("hash_variance", float64()),
                                            field("hash_stddev", float64()),
                                            field("hash_variance", float64()),
                                            field("hash_stddev", float64()),
                                            field("key_0", int64()),
                                        }),
                                        R"([
    [1.0,                 1.0,                1.0,                 1.0,                1],
    [0.22222222222222224, 0.4714045207910317, 0.22222222222222224, 0.4714045207910317, 2],
    [2.25,                1.5,                2.25,                1.5,                null]
  ])"),
                          aggregated_and_grouped,
                          /*verbose=*/true);
}

TEST_P(GroupBy, TDigest) {
  auto batch = RecordBatchFromJSON(
      schema({field("argument", float64()), field("key", int64())}), R"([
    [1,    1],
    [null, 1],
    [0,    2],
    [null, 3],
    [1,    4],
    [4,    null],
    [3,    1],
    [0,    2],
    [-1,   2],
    [1,    null],
    [NaN,  3],
    [1,    4],
    [1,    4],
    [null, 4]
  ])");

  auto options1 = std::make_shared<TDigestOptions>(std::vector<double>{0.5, 0.9, 0.99});
  auto options2 =
      std::make_shared<TDigestOptions>(std::vector<double>{0.5, 0.9, 0.99}, /*delta=*/50,
                                       /*buffer_size=*/1024);
  auto keep_nulls =
      std::make_shared<TDigestOptions>(/*q=*/0.5, /*delta=*/100, /*buffer_size=*/500,
                                       /*skip_nulls=*/false, /*min_count=*/0);
  auto min_count =
      std::make_shared<TDigestOptions>(/*q=*/0.5, /*delta=*/100, /*buffer_size=*/500,
                                       /*skip_nulls=*/true, /*min_count=*/3);
  auto keep_nulls_min_count =
      std::make_shared<TDigestOptions>(/*q=*/0.5, /*delta=*/100, /*buffer_size=*/500,
                                       /*skip_nulls=*/false, /*min_count=*/3);
  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       GroupByTest(
                           {
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                           },
                           {
                               batch->GetColumnByName("key"),
                           },
                           {},
                           {
                               {"hash_tdigest", nullptr},
                               {"hash_tdigest", options1},
                               {"hash_tdigest", options2},
                               {"hash_tdigest", keep_nulls},
                               {"hash_tdigest", min_count},
                               {"hash_tdigest", keep_nulls_min_count},
                           },
                           false));

  AssertDatumsApproxEqual(
      ArrayFromJSON(struct_({
                        field("hash_tdigest", fixed_size_list(float64(), 1)),
                        field("hash_tdigest", fixed_size_list(float64(), 3)),
                        field("hash_tdigest", fixed_size_list(float64(), 3)),
                        field("hash_tdigest", fixed_size_list(float64(), 1)),
                        field("hash_tdigest", fixed_size_list(float64(), 1)),
                        field("hash_tdigest", fixed_size_list(float64(), 1)),
                        field("key_0", int64()),
                    }),
                    R"([
    [[1.0],  [1.0, 3.0, 3.0],    [1.0, 3.0, 3.0],    [null], [null], [null], 1],
    [[0.0],  [0.0, 0.0, 0.0],    [0.0, 0.0, 0.0],    [0.0],  [0.0],  [0.0],  2],
    [[null], [null, null, null], [null, null, null], [null], [null], [null], 3],
    [[1.0],  [1.0, 1.0, 1.0],    [1.0, 1.0, 1.0],    [null], [1.0],  [null], 4],
    [[1.0],  [1.0, 4.0, 4.0],    [1.0, 4.0, 4.0],    [1.0],  [null], [null], null]
  ])"),
      aggregated_and_grouped,
      /*verbose=*/true);
}

TEST_P(GroupBy, TDigestDecimal) {
  auto batch = RecordBatchFromJSON(
      schema({field("argument0", decimal128(3, 2)), field("argument1", decimal256(3, 2)),
              field("key", int64())}),
      R"([
    ["1.01",  "1.01",  1],
    [null,    null,    1],
    ["0.00",  "0.00",  2],
    ["4.42",  "4.42",  null],
    ["3.86",  "3.86",  1],
    ["0.00",  "0.00",  2],
    ["-1.93", "-1.93", 2],
    ["1.85",  "1.85",  null]
  ])");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       GroupByTest(
                           {
                               batch->GetColumnByName("argument0"),
                               batch->GetColumnByName("argument1"),
                           },
                           {batch->GetColumnByName("key")},
                           {
                               {"hash_tdigest", nullptr},
                               {"hash_tdigest", nullptr},
                           },
                           false));

  AssertDatumsApproxEqual(
      ArrayFromJSON(struct_({
                        field("hash_tdigest", fixed_size_list(float64(), 1)),
                        field("hash_tdigest", fixed_size_list(float64(), 1)),
                        field("key_0", int64()),
                    }),
                    R"([
    [[1.01], [1.01], 1],
    [[0.0],  [0.0],  2],
    [[1.85], [1.85], null]
  ])"),
      aggregated_and_grouped,
      /*verbose=*/true);
}

TEST_P(GroupBy, ApproximateMedian) {
  for (const auto& type : {float64(), int8()}) {
    auto batch =
        RecordBatchFromJSON(schema({field("argument", type), field("key", int64())}), R"([
    [1,    1],
    [null, 1],
    [0,    2],
    [null, 3],
    [1,    4],
    [4,    null],
    [3,    1],
    [0,    2],
    [-1,   2],
    [1,    null],
    [null, 3],
    [1,    4],
    [1,    4],
    [null, 4]
  ])");

    std::shared_ptr<ScalarAggregateOptions> options;
    auto keep_nulls = std::make_shared<ScalarAggregateOptions>(
        /*skip_nulls=*/false, /*min_count=*/0);
    auto min_count = std::make_shared<ScalarAggregateOptions>(
        /*skip_nulls=*/true, /*min_count=*/3);
    auto keep_nulls_min_count = std::make_shared<ScalarAggregateOptions>(
        /*skip_nulls=*/false, /*min_count=*/3);
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 batch->GetColumnByName("argument"),
                                 batch->GetColumnByName("argument"),
                                 batch->GetColumnByName("argument"),
                                 batch->GetColumnByName("argument"),
                             },
                             {
                                 batch->GetColumnByName("key"),
                             },
                             {},
                             {
                                 {"hash_approximate_median", options},
                                 {"hash_approximate_median", keep_nulls},
                                 {"hash_approximate_median", min_count},
                                 {"hash_approximate_median", keep_nulls_min_count},
                             },
                             false));

    AssertDatumsApproxEqual(ArrayFromJSON(struct_({
                                              field("hash_approximate_median", float64()),
                                              field("hash_approximate_median", float64()),
                                              field("hash_approximate_median", float64()),
                                              field("hash_approximate_median", float64()),
                                              field("key_0", int64()),
                                          }),
                                          R"([
    [1.0,  null, null, null, 1],
    [0.0,  0.0,  0.0,  0.0,  2],
    [null, null, null, null, 3],
    [1.0,  null, 1.0,  null, 4],
    [1.0,  1.0,  null, null, null]
  ])"),
                            aggregated_and_grouped,
                            /*verbose=*/true);
  }
}

TEST_P(GroupBy, StddevVarianceTDigestScalar) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON({int32(), float32(), int64()},
                        {ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::ARRAY},
                        "[[1, 1.0, 1], [1, 1.0, 1], [1, 1.0, 2], [1, 1.0, 3]]"),
      ExecBatchFromJSON(
          {int32(), float32(), int64()},
          {ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::ARRAY},
          "[[null, null, 1], [null, null, 1], [null, null, 2], [null, null, 3]]"),
      ExecBatchFromJSON({int32(), float32(), int64()},
                        "[[2, 2.0, 1], [3, 3.0, 2], [4, 4.0, 3]]"),
  };
  input.schema = schema(
      {field("argument", int32()), field("argument1", float32()), field("key", int64())});

  for (bool use_threads : {false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum actual,
        RunGroupBy(input, {"key"},
                   {
                       {"hash_stddev", nullptr, "argument", "hash_stddev"},
                       {"hash_variance", nullptr, "argument", "hash_variance"},
                       {"hash_tdigest", nullptr, "argument", "hash_tdigest"},
                       {"hash_stddev", nullptr, "argument1", "hash_stddev"},
                       {"hash_variance", nullptr, "argument1", "hash_variance"},
                       {"hash_tdigest", nullptr, "argument1", "hash_tdigest"},
                   },
                   use_threads));
    Datum expected =
        ArrayFromJSON(struct_({
                          field("hash_stddev", float64()),
                          field("hash_variance", float64()),
                          field("hash_tdigest", fixed_size_list(float64(), 1)),
                          field("hash_stddev", float64()),
                          field("hash_variance", float64()),
                          field("hash_tdigest", fixed_size_list(float64(), 1)),
                          field("key", int64()),
                      }),
                      R"([
         [0.4714045, 0.222222, [1.0], 0.4714045, 0.222222, [1.0], 1],
         [1.0,       1.0,      [1.0], 1.0,       1.0,      [1.0], 2],
         [1.5,       2.25,     [1.0], 1.5,       2.25,     [1.0], 3]
       ])");
    AssertDatumsApproxEqual(expected, actual, /*verbose=*/true);
  }
}

TEST_P(GroupBy, VarianceOptions) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON(
          {int32(), float32(), int64()},
          {ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::ARRAY},
          "[[1, 1.0, 1], [1, 1.0, 1], [1, 1.0, 2], [1, 1.0, 2], [1, 1.0, 3]]"),
      ExecBatchFromJSON({int32(), float32(), int64()},
                        {ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::ARRAY},
                        "[[1, 1.0, 4], [1, 1.0, 4]]"),
      ExecBatchFromJSON({int32(), float32(), int64()},
                        {ArgShape::SCALAR, ArgShape::SCALAR, ArgShape::ARRAY},

                        "[[null, null, 1]]"),
      ExecBatchFromJSON({int32(), float32(), int64()}, "[[2, 2.0, 1], [3, 3.0, 2]]"),
      ExecBatchFromJSON({int32(), float32(), int64()}, "[[4, 4.0, 2], [2, 2.0, 4]]"),
      ExecBatchFromJSON({int32(), float32(), int64()}, "[[null, null, 4]]"),
  };
  input.schema = schema(
      {field("argument", int32()), field("argument1", float32()), field("key", int64())});

  auto keep_nulls = std::make_shared<VarianceOptions>(/*ddof=*/0, /*skip_nulls=*/false,
                                                      /*min_count=*/0);
  auto min_count =
      std::make_shared<VarianceOptions>(/*ddof=*/0, /*skip_nulls=*/true, /*min_count=*/3);
  auto keep_nulls_min_count = std::make_shared<VarianceOptions>(
      /*ddof=*/0, /*skip_nulls=*/false, /*min_count=*/3);

  for (bool use_threads : {false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum actual,
        RunGroupBy(
            input, {"key"},
            {
                {"hash_stddev", keep_nulls, "argument", "hash_stddev"},
                {"hash_stddev", min_count, "argument", "hash_stddev"},
                {"hash_stddev", keep_nulls_min_count, "argument", "hash_stddev"},
                {"hash_variance", keep_nulls, "argument", "hash_variance"},
                {"hash_variance", min_count, "argument", "hash_variance"},
                {"hash_variance", keep_nulls_min_count, "argument", "hash_variance"},
            },
            use_threads));
    Datum expected = ArrayFromJSON(struct_({
                                       field("hash_stddev", float64()),
                                       field("hash_stddev", float64()),
                                       field("hash_stddev", float64()),
                                       field("hash_variance", float64()),
                                       field("hash_variance", float64()),
                                       field("hash_variance", float64()),
                                       field("key", int64()),
                                   }),
                                   R"([
         [null,    0.471405, null,    null,   0.222222, null,   1],
         [1.29904, 1.29904,  1.29904, 1.6875, 1.6875,   1.6875, 2],
         [0.0,     null,     null,    0.0,    null,     null,   3],
         [null,    0.471405, null,    null,   0.222222, null,   4]
       ])");
    ValidateOutput(expected);
    AssertDatumsApproxEqual(expected, actual, /*verbose=*/true);

    ASSERT_OK_AND_ASSIGN(
        actual,
        RunGroupBy(
            input, {"key"},
            {
                {"hash_stddev", keep_nulls, "argument1", "hash_stddev"},
                {"hash_stddev", min_count, "argument1", "hash_stddev"},
                {"hash_stddev", keep_nulls_min_count, "argument1", "hash_stddev"},
                {"hash_variance", keep_nulls, "argument1", "hash_variance"},
                {"hash_variance", min_count, "argument1", "hash_variance"},
                {"hash_variance", keep_nulls_min_count, "argument1", "hash_variance"},
            },
            use_threads));
    expected = ArrayFromJSON(struct_({
                                 field("hash_stddev", float64()),
                                 field("hash_stddev", float64()),
                                 field("hash_stddev", float64()),
                                 field("hash_variance", float64()),
                                 field("hash_variance", float64()),
                                 field("hash_variance", float64()),
                                 field("key", int64()),
                             }),
                             R"([
         [null,    0.471405, null,    null,   0.222222, null,   1],
         [1.29904, 1.29904,  1.29904, 1.6875, 1.6875,   1.6875, 2],
         [0.0,     null,     null,    0.0,    null,     null,   3],
         [null,    0.471405, null,    null,   0.222222, null,   4]
       ])");
    ValidateOutput(expected);
    AssertDatumsApproxEqual(expected, actual, /*verbose=*/true);
  }
}

TEST_P(GroupBy, MinMaxOnly) {
  auto in_schema = schema({
      field("argument", float64()),
      field("argument1", null()),
      field("argument2", boolean()),
      field("key", int64()),
  });
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

    auto table = TableFromJSON(in_schema, {R"([
    [1.0,   null, true, 1],
    [null,  null, true, 1]
])",
                                           R"([
    [0.0,   null, false, 2],
    [null,  null, false, 3],
    [4.0,   null, null,  null],
    [3.25,  null, true,  1],
    [0.125, null, false, 2]
])",
                                           R"([
    [-0.25, null, false, 2],
    [0.75,  null, true,  null],
    [null,  null, true,  3]
])"});

    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument1"),
                                 table->GetColumnByName("argument2"),
                             },
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_min_max", nullptr},
                                 {"hash_min_max", nullptr},
                                 {"hash_min_max", nullptr},
                             },
                             use_threads));
    ValidateOutput(aggregated_and_grouped);
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_min_max", struct_({
                                                                  field("min", float64()),
                                                                  field("max", float64()),
                                                              })),
                                        field("hash_min_max", struct_({
                                                                  field("min", null()),
                                                                  field("max", null()),
                                                              })),
                                        field("hash_min_max", struct_({
                                                                  field("min", boolean()),
                                                                  field("max", boolean()),
                                                              })),
                                        field("key_0", int64()),
                                    }),
                                    R"([
    [{"min": 1.0,   "max": 3.25},  {"min": null, "max": null}, {"min": true, "max": true},   1],
    [{"min": -0.25, "max": 0.125}, {"min": null, "max": null}, {"min": false, "max": false}, 2],
    [{"min": null,  "max": null},  {"min": null, "max": null}, {"min": false, "max": true},  3],
    [{"min": 0.75,  "max": 4.0},   {"min": null, "max": null}, {"min": true, "max": true},   null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, MinMaxTypes) {
  std::vector<std::shared_ptr<DataType>> types;
  types.insert(types.end(), NumericTypes().begin(), NumericTypes().end());
  types.insert(types.end(), TemporalTypes().begin(), TemporalTypes().end());
  types.push_back(month_interval());

  const std::vector<std::string> default_table = {R"([
    [1,    1],
    [null, 1]
])",
                                                  R"([
    [0,    2],
    [null, 3],
    [3,    4],
    [5,    4],
    [4,    null],
    [3,    1],
    [0,    2]
])",
                                                  R"([
    [0,    2],
    [1,    null],
    [null, 3]
])"};

  const std::vector<std::string> date64_table = {R"([
    [86400000,    1],
    [null, 1]
])",
                                                 R"([
    [0,    2],
    [null, 3],
    [259200000,    4],
    [432000000,    4],
    [345600000,    null],
    [259200000,    1],
    [0,    2]
])",
                                                 R"([
    [0,    2],
    [86400000,    null],
    [null, 3]
])"};

  const std::string default_expected =
      R"([
    [{"min": 1, "max": 3},       1],
    [{"min": 0, "max": 0},       2],
    [{"min": null, "max": null}, 3],
    [{"min": 3, "max": 5},       4],
    [{"min": 1, "max": 4},       null]
    ])";

  const std::string date64_expected =
      R"([
    [{"min": 86400000, "max": 259200000},       1],
    [{"min": 0, "max": 0},       2],
    [{"min": null, "max": null}, 3],
    [{"min": 259200000, "max": 432000000},       4],
    [{"min": 86400000, "max": 345600000},       null]
    ])";

  for (const auto& ty : types) {
    SCOPED_TRACE(ty->ToString());
    auto in_schema = schema({field("argument0", ty), field("key", int64())});
    auto table =
        TableFromJSON(in_schema, (ty->name() == "date64") ? date64_table : default_table);

    ASSERT_OK_AND_ASSIGN(
        Datum aggregated_and_grouped,
        GroupByTest({table->GetColumnByName("argument0")},
                    {table->GetColumnByName("key")}, {{"hash_min_max", nullptr}},
                    /*use_threads=*/true));
    ValidateOutput(aggregated_and_grouped);
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(
        ArrayFromJSON(
            struct_({
                field("hash_min_max", struct_({field("min", ty), field("max", ty)})),
                field("key_0", int64()),
            }),
            (ty->name() == "date64") ? date64_expected : default_expected),
        aggregated_and_grouped,
        /*verbose=*/true);
  }
}

TEST_P(GroupBy, MinMaxDecimal) {
  auto in_schema = schema({
      field("argument0", decimal128(3, 2)),
      field("argument1", decimal256(3, 2)),
      field("key", int64()),
  });
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

    auto table = TableFromJSON(in_schema, {R"([
    ["1.01", "1.01",   1],
    [null,   null,     1]
                        ])",
                                           R"([
    ["0.00", "0.00",   2],
    [null,   null,     3],
    ["-3.25", "-3.25", 4],
    ["-5.25", "-5.25", 4],
    ["4.01", "4.01",   null],
    ["3.25", "3.25",   1],
    ["0.12", "0.12",   2]
                        ])",
                                           R"([
    ["-0.25", "-0.25", 2],
    ["0.75",  "0.75",  null],
    [null,    null,    3]
                        ])"});

    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 table->GetColumnByName("argument0"),
                                 table->GetColumnByName("argument1"),
                             },
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_min_max", nullptr},
                                 {"hash_min_max", nullptr},
                             },
                             use_threads));
    ValidateOutput(aggregated_and_grouped);
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(
        ArrayFromJSON(struct_({
                          field("hash_min_max", struct_({
                                                    field("min", decimal128(3, 2)),
                                                    field("max", decimal128(3, 2)),
                                                })),
                          field("hash_min_max", struct_({
                                                    field("min", decimal256(3, 2)),
                                                    field("max", decimal256(3, 2)),
                                                })),
                          field("key_0", int64()),
                      }),
                      R"([
    [{"min": "1.01", "max": "3.25"},   {"min": "1.01", "max": "3.25"},   1],
    [{"min": "-0.25", "max": "0.12"},  {"min": "-0.25", "max": "0.12"},  2],
    [{"min": null, "max": null},       {"min": null, "max": null},       3],
    [{"min": "-5.25", "max": "-3.25"}, {"min": "-5.25", "max": "-3.25"}, 4],
    [{"min": "0.75", "max": "4.01"},   {"min": "0.75", "max": "4.01"},   null]
  ])"),
        aggregated_and_grouped,
        /*verbose=*/true);
  }
}

TEST_P(GroupBy, MinMaxBinary) {
  for (bool use_threads : {true, false}) {
    for (const auto& ty : BaseBinaryTypes()) {
      SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

      auto table = TableFromJSON(schema({
                                     field("argument0", ty),
                                     field("key", int64()),
                                 }),
                                 {R"([
    ["aaaa", 1],
    [null,   1]
])",
                                  R"([
    ["bcd",  2],
    [null,   3],
    ["2",    null],
    ["d",    1],
    ["bc",   2]
])",
                                  R"([
    ["babcd", 2],
    ["123",   null],
    [null,    3]
])"});

      ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                           GroupByTest({table->GetColumnByName("argument0")},
                                       {table->GetColumnByName("key")},
                                       {{"hash_min_max", nullptr}}, use_threads));
      ValidateOutput(aggregated_and_grouped);
      SortBy({"key_0"}, &aggregated_and_grouped);

      AssertDatumsEqual(
          ArrayFromJSON(
              struct_({
                  field("hash_min_max", struct_({field("min", ty), field("max", ty)})),
                  field("key_0", int64()),
              }),
              R"([
    [{"min": "aaaa", "max": "d"},    1],
    [{"min": "babcd", "max": "bcd"}, 2],
    [{"min": null, "max": null},     3],
    [{"min": "123", "max": "2"},     null]
  ])"),
          aggregated_and_grouped,
          /*verbose=*/true);
    }
  }
}

TEST_P(GroupBy, MinMaxFixedSizeBinary) {
  const auto ty = fixed_size_binary(3);
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

    auto table = TableFromJSON(schema({
                                   field("argument0", ty),
                                   field("key", int64()),
                               }),
                               {R"([
    ["aaa", 1],
    [null,  1]
])",
                                R"([
    ["bac", 2],
    [null,  3],
    ["234", null],
    ["ddd", 1],
    ["bcd", 2]
])",
                                R"([
    ["bab", 2],
    ["123", null],
    [null,  3]
])"});

    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest({table->GetColumnByName("argument0")},
                                     {table->GetColumnByName("key")},
                                     {{"hash_min_max", nullptr}}, use_threads));
    ValidateOutput(aggregated_and_grouped);
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(
        ArrayFromJSON(
            struct_({
                field("hash_min_max", struct_({field("min", ty), field("max", ty)})),
                field("key_0", int64()),
            }),
            R"([
    [{"min": "aaa", "max": "ddd"}, 1],
    [{"min": "bab", "max": "bcd"}, 2],
    [{"min": null, "max": null},   3],
    [{"min": "123", "max": "234"}, null]
  ])"),
        aggregated_and_grouped,
        /*verbose=*/true);
  }
}

TEST_P(GroupBy, MinOrMax) {
  auto table =
      TableFromJSON(schema({field("argument", float64()), field("key", int64())}), {R"([
    [1.0,   1],
    [null,  1]
])",
                                                                                    R"([
    [0.0,   2],
    [null,  3],
    [4.0,   null],
    [3.25,  1],
    [0.125, 2]
])",
                                                                                    R"([
    [-0.25, 2],
    [0.75,  null],
    [null,  3]
])",
                                                                                    R"([
    [NaN,   4],
    [null,  4],
    [Inf,   4],
    [-Inf,  4],
    [0.0,   4]
])"});

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       GroupByTest({table->GetColumnByName("argument"),
                                    table->GetColumnByName("argument")},
                                   {table->GetColumnByName("key")},
                                   {
                                       {"hash_min", nullptr},
                                       {"hash_max", nullptr},
                                   },
                                   /*use_threads=*/true));
  SortBy({"key_0"}, &aggregated_and_grouped);

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("hash_min", float64()),
                                      field("hash_max", float64()),
                                      field("key_0", int64()),
                                  }),
                                  R"([
    [1.0,   3.25,  1],
    [-0.25, 0.125, 2],
    [null,  null,  3],
    [-Inf,  Inf,   4],
    [0.75,  4.0,   null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST_P(GroupBy, MinMaxScalar) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON({int32(), int64()}, {ArgShape::SCALAR, ArgShape::ARRAY},

                        "[[-1, 1], [-1, 1], [-1, 2], [-1, 3]]"),
      ExecBatchFromJSON({int32(), int64()}, {ArgShape::SCALAR, ArgShape::ARRAY},
                        "[[null, 1], [null, 1], [null, 2], [null, 3]]"),
      ExecBatchFromJSON({int32(), int64()}, "[[2, 1], [3, 2], [4, 3]]"),
  };
  input.schema = schema({field("agg_0", int32()), field("key", int64())});

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum actual,
        RunGroupBy(input, {"key"}, {{"hash_min_max", nullptr, "agg_0", "hash_min_max"}},
                   use_threads));
    Datum expected =
        ArrayFromJSON(struct_({
                          field("hash_min_max",
                                struct_({field("min", int32()), field("max", int32())})),
                          field("key", int64()),
                      }),
                      R"([
      [{"min": -1, "max": 2}, 1],
      [{"min": -1, "max": 3}, 2],
      [{"min": -1, "max": 4}, 3]
    ])");
    AssertDatumsApproxEqual(expected, actual, /*verbose=*/true);
  }
}

TEST_P(GroupBy, AnyAndAll) {
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

    auto table =
        TableFromJSON(schema({field("argument", boolean()), field("key", int64())}), {R"([
    [true,  1],
    [null,  1]
                        ])",
                                                                                      R"([
    [false, 2],
    [null,  3],
    [null,  4],
    [false, 4],
    [true,  5],
    [false, null],
    [true,  1],
    [true,  2]
                        ])",
                                                                                      R"([
    [false, 2],
    [false, null],
    [null,  3]
                        ])"});

    auto no_min =
        std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/0);
    auto min_count =
        std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/3);
    auto keep_nulls =
        std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false, /*min_count=*/0);
    auto keep_nulls_min_count =
        std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false, /*min_count=*/3);
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         AltGroupBy(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {table->GetColumnByName("key")}, {},
                             {
                                 {"hash_any", no_min, "agg_0", "hash_any"},
                                 {"hash_any", min_count, "agg_1", "hash_any"},
                                 {"hash_any", keep_nulls, "agg_2", "hash_any"},
                                 {"hash_any", keep_nulls_min_count, "agg_3", "hash_any"},
                                 {"hash_all", no_min, "agg_4", "hash_all"},
                                 {"hash_all", min_count, "agg_5", "hash_all"},
                                 {"hash_all", keep_nulls, "agg_6", "hash_all"},
                                 {"hash_all", keep_nulls_min_count, "agg_7", "hash_all"},
                             },
                             use_threads));
    SortBy({"key_0"}, &aggregated_and_grouped);

    // Group 1: trues and nulls
    // Group 2: trues and falses
    // Group 3: nulls
    // Group 4: falses and nulls
    // Group 5: trues
    // Group null: falses
    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_any", boolean()),
                                        field("hash_any", boolean()),
                                        field("hash_any", boolean()),
                                        field("hash_any", boolean()),
                                        field("hash_all", boolean()),
                                        field("hash_all", boolean()),
                                        field("hash_all", boolean()),
                                        field("hash_all", boolean()),
                                        field("key_0", int64()),
                                    }),
                                    R"([
    [true,  null, true,  null, true,  null,  null,  null,  1],
    [true,  true, true,  true, false, false, false, false, 2],
    [false, null, null,  null, true,  null,  null,  null,  3],
    [false, null, null,  null, false, null,  false, null,  4],
    [true,  null, true,  null, true,  null,  true,  null,  5],
    [false, null, false, null, false, null,  false, null,  null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, AnyAllScalar) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON({boolean(), int64()}, {ArgShape::SCALAR, ArgShape::ARRAY},

                        "[[true, 1], [true, 1], [true, 2], [true, 3]]"),
      ExecBatchFromJSON({boolean(), int64()}, {ArgShape::SCALAR, ArgShape::ARRAY},
                        "[[null, 1], [null, 1], [null, 2], [null, 3]]"),
      ExecBatchFromJSON({boolean(), int64()}, "[[true, 1], [false, 2], [null, 3]]"),
  };
  input.schema = schema({field("argument", boolean()), field("key", int64())});

  auto keep_nulls =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false, /*min_count=*/0);
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(Datum actual,
                         RunGroupBy(input, {"key"},
                                    {
                                        {"hash_any", nullptr, "argument", "hash_any"},
                                        {"hash_all", nullptr, "argument", "hash_all"},
                                        {"hash_any", keep_nulls, "argument", "hash_any"},
                                        {"hash_all", keep_nulls, "argument", "hash_all"},
                                    },
                                    use_threads));
    Datum expected = ArrayFromJSON(struct_({
                                       field("hash_any", boolean()),
                                       field("hash_all", boolean()),
                                       field("hash_any", boolean()),
                                       field("hash_all", boolean()),
                                       field("key", int64()),
                                   }),
                                   R"([
      [true, true,  true, null,  1],
      [true, false, true, false, 2],
      [true, true,  true, null,  3]
    ])");
    AssertDatumsApproxEqual(expected, actual, /*verbose=*/true);
  }
}

TEST_P(GroupBy, CountDistinct) {
  auto all = std::make_shared<CountOptions>(CountOptions::ALL);
  auto only_valid = std::make_shared<CountOptions>(CountOptions::ONLY_VALID);
  auto only_null = std::make_shared<CountOptions>(CountOptions::ONLY_NULL);
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

    auto table =
        TableFromJSON(schema({field("argument", float64()), field("key", int64())}), {R"([
    [1,    1],
    [1,    1]
])",
                                                                                      R"([
    [0,    2],
    [null, 3],
    [null, 3]
])",
                                                                                      R"([
    [null, 4],
    [null, 4]
])",
                                                                                      R"([
    [4,    null],
    [1,    3]
])",
                                                                                      R"([
    [0,    2],
    [-1,   2]
])",
                                                                                      R"([
    [1,    null],
    [NaN,  3]
  ])",
                                                                                      R"([
    [2,    null],
    [3,    null]
  ])"});

    ASSERT_OK_AND_ASSIGN(
        Datum aggregated_and_grouped,
        AltGroupBy(
            {
                table->GetColumnByName("argument"),
                table->GetColumnByName("argument"),
                table->GetColumnByName("argument"),
            },
            {
                table->GetColumnByName("key"),
            },
            {},
            {
                {"hash_count_distinct", all, "agg_0", "hash_count_distinct"},
                {"hash_count_distinct", only_valid, "agg_1", "hash_count_distinct"},
                {"hash_count_distinct", only_null, "agg_2", "hash_count_distinct"},
            },
            use_threads));
    SortBy({"key_0"}, &aggregated_and_grouped);
    ValidateOutput(aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_count_distinct", int64()),
                                        field("hash_count_distinct", int64()),
                                        field("hash_count_distinct", int64()),
                                        field("key_0", int64()),
                                    }),
                                    R"([
    [1, 1, 0, 1],
    [2, 2, 0, 2],
    [3, 2, 1, 3],
    [1, 0, 1, 4],
    [4, 4, 0, null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);

    table =
        TableFromJSON(schema({field("argument", utf8()), field("key", int64())}), {R"([
    ["foo",  1],
    ["foo",  1]
])",
                                                                                   R"([
    ["bar",  2],
    [null,   3],
    [null,   3]
])",
                                                                                   R"([
    [null, 4],
    [null, 4]
])",
                                                                                   R"([
    ["baz",  null],
    ["foo",  3]
])",
                                                                                   R"([
    ["bar",  2],
    ["spam", 2]
])",
                                                                                   R"([
    ["eggs", null],
    ["ham",  3]
  ])",
                                                                                   R"([
    ["a",    null],
    ["b",    null]
  ])"});

    ASSERT_OK_AND_ASSIGN(
        aggregated_and_grouped,
        AltGroupBy(
            {
                table->GetColumnByName("argument"),
                table->GetColumnByName("argument"),
                table->GetColumnByName("argument"),
            },
            {
                table->GetColumnByName("key"),
            },
            {},
            {
                {"hash_count_distinct", all, "agg_0", "hash_count_distinct"},
                {"hash_count_distinct", only_valid, "agg_1", "hash_count_distinct"},
                {"hash_count_distinct", only_null, "agg_2", "hash_count_distinct"},
            },
            use_threads));
    ValidateOutput(aggregated_and_grouped);
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_count_distinct", int64()),
                                        field("hash_count_distinct", int64()),
                                        field("hash_count_distinct", int64()),
                                        field("key_0", int64()),
                                    }),
                                    R"([
    [1, 1, 0, 1],
    [2, 2, 0, 2],
    [3, 2, 1, 3],
    [1, 0, 1, 4],
    [4, 4, 0, null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);

    table =
        TableFromJSON(schema({field("argument", utf8()), field("key", int64())}), {
                                                                                      R"([
    ["foo",  1],
    ["foo",  1],
    ["bar",  2],
    ["bar",  2],
    ["spam", 2]
])",
                                                                                  });

    ASSERT_OK_AND_ASSIGN(
        aggregated_and_grouped,
        AltGroupBy(
            {
                table->GetColumnByName("argument"),
                table->GetColumnByName("argument"),
                table->GetColumnByName("argument"),
            },
            {
                table->GetColumnByName("key"),
            },
            {},
            {
                {"hash_count_distinct", all, "agg_0", "hash_count_distinct"},
                {"hash_count_distinct", only_valid, "agg_1", "hash_count_distinct"},
                {"hash_count_distinct", only_null, "agg_2", "hash_count_distinct"},
            },
            use_threads));
    ValidateOutput(aggregated_and_grouped);
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_count_distinct", int64()),
                                        field("hash_count_distinct", int64()),
                                        field("hash_count_distinct", int64()),
                                        field("key_0", int64()),
                                    }),
                                    R"([
    [1, 1, 0, 1],
    [2, 2, 0, 2]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, Distinct) {
  auto all = std::make_shared<CountOptions>(CountOptions::ALL);
  auto only_valid = std::make_shared<CountOptions>(CountOptions::ONLY_VALID);
  auto only_null = std::make_shared<CountOptions>(CountOptions::ONLY_NULL);
  for (bool use_threads : {false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

    auto table =
        TableFromJSON(schema({field("argument", utf8()), field("key", int64())}), {R"([
    ["foo",  1],
    ["foo",  1]
])",
                                                                                   R"([
    ["bar",  2],
    [null,   3],
    [null,   3]
])",
                                                                                   R"([
    [null,   4],
    [null,   4]
])",
                                                                                   R"([
    ["baz",  null],
    ["foo",  3]
])",
                                                                                   R"([
    ["bar",  2],
    ["spam", 2]
])",
                                                                                   R"([
    ["eggs", null],
    ["ham",  3]
  ])",
                                                                                   R"([
    ["a",    null],
    ["b",    null]
  ])"});

    ASSERT_OK_AND_ASSIGN(auto aggregated_and_grouped,
                         AltGroupBy(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {
                                 table->GetColumnByName("key"),
                             },
                             {},
                             {
                                 {"hash_distinct", all, "agg_0", "hash_distinct"},
                                 {"hash_distinct", only_valid, "agg_1", "hash_distinct"},
                                 {"hash_distinct", only_null, "agg_2", "hash_distinct"},
                             },
                             use_threads));
    ValidateOutput(aggregated_and_grouped);
    SortBy({"key_0"}, &aggregated_and_grouped);

    // Order of sub-arrays is not stable
    auto sort = [](const Array& arr) -> std::shared_ptr<Array> {
      EXPECT_OK_AND_ASSIGN(auto indices, SortIndices(arr));
      EXPECT_OK_AND_ASSIGN(auto sorted, Take(arr, indices));
      return sorted.make_array();
    };

    auto struct_arr = aggregated_and_grouped.array_as<StructArray>();

    auto all_arr = checked_pointer_cast<ListArray>(struct_arr->field(0));
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"(["foo"])"), sort(*all_arr->value_slice(0)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"(["bar", "spam"])"),
                      sort(*all_arr->value_slice(1)), /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"(["foo", "ham", null])"),
                      sort(*all_arr->value_slice(2)), /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"([null])"), sort(*all_arr->value_slice(3)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"(["a", "b", "baz", "eggs"])"),
                      sort(*all_arr->value_slice(4)), /*verbose=*/true);

    auto valid_arr = checked_pointer_cast<ListArray>(struct_arr->field(1));
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"(["foo"])"),
                      sort(*valid_arr->value_slice(0)), /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"(["bar", "spam"])"),
                      sort(*valid_arr->value_slice(1)), /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"(["foo", "ham"])"),
                      sort(*valid_arr->value_slice(2)), /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"([])"), sort(*valid_arr->value_slice(3)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"(["a", "b", "baz", "eggs"])"),
                      sort(*valid_arr->value_slice(4)), /*verbose=*/true);

    auto null_arr = checked_pointer_cast<ListArray>(struct_arr->field(2));
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"([])"), sort(*null_arr->value_slice(0)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"([])"), sort(*null_arr->value_slice(1)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"([null])"), sort(*null_arr->value_slice(2)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"([null])"), sort(*null_arr->value_slice(3)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(utf8(), R"([])"), sort(*null_arr->value_slice(4)),
                      /*verbose=*/true);

    table =
        TableFromJSON(schema({field("argument", utf8()), field("key", int64())}), {
                                                                                      R"([
    ["foo",  1],
    ["foo",  1],
    ["bar",  2],
    ["bar",  2]
])",
                                                                                  });
    ASSERT_OK_AND_ASSIGN(aggregated_and_grouped,
                         AltGroupBy(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {
                                 table->GetColumnByName("key"),
                             },
                             {},
                             {
                                 {"hash_distinct", all, "agg_0", "hash_distinct"},
                                 {"hash_distinct", only_valid, "agg_1", "hash_distinct"},
                                 {"hash_distinct", only_null, "agg_2", "hash_distinct"},
                             },
                             use_threads));
    ValidateOutput(aggregated_and_grouped);
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(
        ArrayFromJSON(struct_({
                          field("hash_distinct", list(utf8())),
                          field("hash_distinct", list(utf8())),
                          field("hash_distinct", list(utf8())),
                          field("key_0", int64()),
                      }),
                      R"([[["foo"], ["foo"], [], 1], [["bar"], ["bar"], [], 2]])"),
        aggregated_and_grouped,
        /*verbose=*/true);
  }
}

TEST_P(GroupBy, OneMiscTypes) {
  auto in_schema = schema({
      field("floats", float64()),
      field("nulls", null()),
      field("booleans", boolean()),
      field("decimal128", decimal128(3, 2)),
      field("decimal256", decimal256(3, 2)),
      field("fixed_binary", fixed_size_binary(3)),
      field("key", int64()),
  });
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

    auto table = TableFromJSON(in_schema, {R"([
    [null, null, true,   null,    null,    null,  1],
    [1.0,  null, true,   "1.01",  "1.01",  "aaa", 1]
])",
                                           R"([
    [0.0,   null, false, "0.00",  "0.00",  "bac", 2],
    [null,  null, false, null,    null,    null,  3],
    [4.0,   null, null,  "4.01",  "4.01",  "234", null],
    [3.25,  null, true,  "3.25",  "3.25",  "ddd", 1],
    [0.125, null, false, "0.12",  "0.12",  "bcd", 2]
])",
                                           R"([
    [-0.25, null, false, "-0.25", "-0.25", "bab", 2],
    [0.75,  null, true,  "0.75",  "0.75",  "123", null],
    [null,  null, true,  null,    null,    null,  3]
])"});

    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 table->GetColumnByName("floats"),
                                 table->GetColumnByName("nulls"),
                                 table->GetColumnByName("booleans"),
                                 table->GetColumnByName("decimal128"),
                                 table->GetColumnByName("decimal256"),
                                 table->GetColumnByName("fixed_binary"),
                             },
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_one", nullptr},
                                 {"hash_one", nullptr},
                                 {"hash_one", nullptr},
                                 {"hash_one", nullptr},
                                 {"hash_one", nullptr},
                                 {"hash_one", nullptr},
                             },
                             use_threads));
    ValidateOutput(aggregated_and_grouped);
    SortBy({"key_0"}, &aggregated_and_grouped);

    const auto& struct_arr = aggregated_and_grouped.array_as<StructArray>();
    //  Check the key column
    AssertDatumsEqual(ArrayFromJSON(int64(), R"([1, 2, 3, null])"),
                      struct_arr->field(struct_arr->num_fields() - 1));

    //  Check values individually
    auto col_0_type = float64();
    const auto& col_0 = struct_arr->field(0);
    EXPECT_THAT(col_0->GetScalar(0), ResultWith(AnyOfJSON(col_0_type, R"([1.0, 3.25])")));
    EXPECT_THAT(col_0->GetScalar(1),
                ResultWith(AnyOfJSON(col_0_type, R"([0.0, 0.125, -0.25])")));
    EXPECT_THAT(col_0->GetScalar(2), ResultWith(AnyOfJSON(col_0_type, R"([null])")));
    EXPECT_THAT(col_0->GetScalar(3), ResultWith(AnyOfJSON(col_0_type, R"([4.0, 0.75])")));

    auto col_1_type = null();
    const auto& col_1 = struct_arr->field(1);
    EXPECT_THAT(col_1->GetScalar(0), ResultWith(AnyOfJSON(col_1_type, R"([null])")));
    EXPECT_THAT(col_1->GetScalar(1), ResultWith(AnyOfJSON(col_1_type, R"([null])")));
    EXPECT_THAT(col_1->GetScalar(2), ResultWith(AnyOfJSON(col_1_type, R"([null])")));
    EXPECT_THAT(col_1->GetScalar(3), ResultWith(AnyOfJSON(col_1_type, R"([null])")));

    auto col_2_type = boolean();
    const auto& col_2 = struct_arr->field(2);
    EXPECT_THAT(col_2->GetScalar(0), ResultWith(AnyOfJSON(col_2_type, R"([true])")));
    EXPECT_THAT(col_2->GetScalar(1), ResultWith(AnyOfJSON(col_2_type, R"([false])")));
    EXPECT_THAT(col_2->GetScalar(2),
                ResultWith(AnyOfJSON(col_2_type, R"([true, false])")));
    EXPECT_THAT(col_2->GetScalar(3),
                ResultWith(AnyOfJSON(col_2_type, R"([true, null])")));

    auto col_3_type = decimal128(3, 2);
    const auto& col_3 = struct_arr->field(3);
    EXPECT_THAT(col_3->GetScalar(0),
                ResultWith(AnyOfJSON(col_3_type, R"(["1.01", "3.25"])")));
    EXPECT_THAT(col_3->GetScalar(1),
                ResultWith(AnyOfJSON(col_3_type, R"(["0.00", "0.12", "-0.25"])")));
    EXPECT_THAT(col_3->GetScalar(2), ResultWith(AnyOfJSON(col_3_type, R"([null])")));
    EXPECT_THAT(col_3->GetScalar(3),
                ResultWith(AnyOfJSON(col_3_type, R"(["4.01", "0.75"])")));

    auto col_4_type = decimal256(3, 2);
    const auto& col_4 = struct_arr->field(4);
    EXPECT_THAT(col_4->GetScalar(0),
                ResultWith(AnyOfJSON(col_4_type, R"(["1.01", "3.25"])")));
    EXPECT_THAT(col_4->GetScalar(1),
                ResultWith(AnyOfJSON(col_4_type, R"(["0.00", "0.12", "-0.25"])")));
    EXPECT_THAT(col_4->GetScalar(2), ResultWith(AnyOfJSON(col_4_type, R"([null])")));
    EXPECT_THAT(col_4->GetScalar(3),
                ResultWith(AnyOfJSON(col_4_type, R"(["4.01", "0.75"])")));

    auto col_5_type = fixed_size_binary(3);
    const auto& col_5 = struct_arr->field(5);
    EXPECT_THAT(col_5->GetScalar(0),
                ResultWith(AnyOfJSON(col_5_type, R"(["aaa", "ddd"])")));
    EXPECT_THAT(col_5->GetScalar(1),
                ResultWith(AnyOfJSON(col_5_type, R"(["bab", "bcd", "bac"])")));
    EXPECT_THAT(col_5->GetScalar(2), ResultWith(AnyOfJSON(col_5_type, R"([null])")));
    EXPECT_THAT(col_5->GetScalar(3),
                ResultWith(AnyOfJSON(col_5_type, R"(["123", "234"])")));
  }
}

TEST_P(GroupBy, OneNumericTypes) {
  std::vector<std::shared_ptr<DataType>> types;
  types.insert(types.end(), NumericTypes().begin(), NumericTypes().end());
  types.insert(types.end(), TemporalTypes().begin(), TemporalTypes().end());
  types.push_back(month_interval());

  const std::vector<std::string> numeric_table_json = {R"([
      [null, 1],
      [1,    1]
    ])",
                                                       R"([
      [0,    2],
      [null, 3],
      [3,    4],
      [5,    4],
      [4,    null],
      [3,    1],
      [0,    2]
    ])",
                                                       R"([
      [0,    2],
      [1,    null],
      [null, 3]
    ])"};

  const std::vector<std::string> temporal_table_json = {R"([
      [null,      1],
      [86400000,  1]
    ])",
                                                        R"([
      [0,         2],
      [null,      3],
      [259200000, 4],
      [432000000, 4],
      [345600000, null],
      [259200000, 1],
      [0,         2]
    ])",
                                                        R"([
      [0,         2],
      [86400000,  null],
      [null,      3]
    ])"};

  for (const auto& type : types) {
    for (bool use_threads : {true, false}) {
      SCOPED_TRACE(type->ToString());
      auto in_schema = schema({field("argument0", type), field("key", int64())});
      auto table =
          TableFromJSON(in_schema, (type->name() == "date64") ? temporal_table_json
                                                              : numeric_table_json);
      ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                           GroupByTest({table->GetColumnByName("argument0")},
                                       {table->GetColumnByName("key")},
                                       {{"hash_one", nullptr}}, use_threads));
      ValidateOutput(aggregated_and_grouped);
      SortBy({"key_0"}, &aggregated_and_grouped);

      const auto& struct_arr = aggregated_and_grouped.array_as<StructArray>();
      //  Check the key column
      AssertDatumsEqual(ArrayFromJSON(int64(), R"([1, 2, 3, 4, null])"),
                        struct_arr->field(struct_arr->num_fields() - 1));

      //  Check values individually
      const auto& col = struct_arr->field(0);
      if (type->name() == "date64") {
        EXPECT_THAT(col->GetScalar(0),
                    ResultWith(AnyOfJSON(type, R"([86400000, 259200000])")));
        EXPECT_THAT(col->GetScalar(1), ResultWith(AnyOfJSON(type, R"([0])")));
        EXPECT_THAT(col->GetScalar(2), ResultWith(AnyOfJSON(type, R"([null])")));
        EXPECT_THAT(col->GetScalar(3),
                    ResultWith(AnyOfJSON(type, R"([259200000, 432000000])")));
        EXPECT_THAT(col->GetScalar(4),
                    ResultWith(AnyOfJSON(type, R"([345600000, 86400000])")));
      } else {
        EXPECT_THAT(col->GetScalar(0), ResultWith(AnyOfJSON(type, R"([1, 3])")));
        EXPECT_THAT(col->GetScalar(1), ResultWith(AnyOfJSON(type, R"([0])")));
        EXPECT_THAT(col->GetScalar(2), ResultWith(AnyOfJSON(type, R"([null])")));
        EXPECT_THAT(col->GetScalar(3), ResultWith(AnyOfJSON(type, R"([3, 5])")));
        EXPECT_THAT(col->GetScalar(4), ResultWith(AnyOfJSON(type, R"([4, 1])")));
      }
    }
  }
}

TEST_P(GroupBy, OneBinaryTypes) {
  for (bool use_threads : {true, false}) {
    for (const auto& type : BaseBinaryTypes()) {
      SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

      auto table = TableFromJSON(schema({
                                     field("argument0", type),
                                     field("key", int64()),
                                 }),
                                 {R"([
    [null,   1],
    ["aaaa", 1]
])",
                                  R"([
    ["babcd",2],
    [null,   3],
    ["2",    null],
    ["d",    1],
    ["bc",   2]
])",
                                  R"([
    ["bcd", 2],
    ["123", null],
    [null,  3]
])"});

      ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                           GroupByTest({table->GetColumnByName("argument0")},
                                       {table->GetColumnByName("key")},
                                       {{"hash_one", nullptr}}, use_threads));
      ValidateOutput(aggregated_and_grouped);
      SortBy({"key_0"}, &aggregated_and_grouped);

      const auto& struct_arr = aggregated_and_grouped.array_as<StructArray>();
      //  Check the key column
      AssertDatumsEqual(ArrayFromJSON(int64(), R"([1, 2, 3, null])"),
                        struct_arr->field(struct_arr->num_fields() - 1));

      const auto& col = struct_arr->field(0);
      EXPECT_THAT(col->GetScalar(0), ResultWith(AnyOfJSON(type, R"(["aaaa", "d"])")));
      EXPECT_THAT(col->GetScalar(1),
                  ResultWith(AnyOfJSON(type, R"(["bcd", "bc", "babcd"])")));
      EXPECT_THAT(col->GetScalar(2), ResultWith(AnyOfJSON(type, R"([null])")));
      EXPECT_THAT(col->GetScalar(3), ResultWith(AnyOfJSON(type, R"(["2", "123"])")));
    }
  }
}

TEST_P(GroupBy, OneScalar) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON({int32(), int64()}, {ArgShape::SCALAR, ArgShape::ARRAY},

                        R"([[-1, 1], [-1, 1], [-1, 1], [-1, 1]])"),
      ExecBatchFromJSON({int32(), int64()}, {ArgShape::SCALAR, ArgShape::ARRAY},
                        R"([[null, 1], [null, 1], [null, 2], [null, 3]])"),
      ExecBatchFromJSON({int32(), int64()}, R"([[22, 1], [3, 2], [4, 3]])")};
  input.schema = schema({field("argument", int32()), field("key", int64())});

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum actual,
        RunGroupBy(input, {"key"}, {{"hash_one", nullptr, "argument", "hash_one"}},
                   use_threads));

    const auto& struct_arr = actual.array_as<StructArray>();
    //  Check the key column
    AssertDatumsEqual(ArrayFromJSON(int64(), R"([1, 2, 3])"),
                      struct_arr->field(struct_arr->num_fields() - 1));

    const auto& col = struct_arr->field(0);
    EXPECT_THAT(col->GetScalar(0), ResultWith(AnyOfJSON(int32(), R"([-1, 22])")));
    EXPECT_THAT(col->GetScalar(1), ResultWith(AnyOfJSON(int32(), R"([3])")));
    EXPECT_THAT(col->GetScalar(2), ResultWith(AnyOfJSON(int32(), R"([4])")));
  }
}

TEST_P(GroupBy, ListNumeric) {
  for (const auto& type : NumericTypes()) {
    for (auto use_threads : {true, false}) {
      SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
      {
        SCOPED_TRACE("with nulls");
        auto table =
            TableFromJSON(schema({field("argument", type), field("key", int64())}), {R"([
    [99,  1],
    [99,  1]
])",
                                                                                     R"([
    [88,  2],
    [null,   3],
    [null,   3]
])",
                                                                                     R"([
    [null,   4],
    [null,   4]
])",
                                                                                     R"([
    [77,  null],
    [99,  3]
])",
                                                                                     R"([
    [88,  2],
    [66, 2]
])",
                                                                                     R"([
    [55, null],
    [44,  3]
  ])",
                                                                                     R"([
    [33,    null],
    [22,    null]
  ])"});

        ASSERT_OK_AND_ASSIGN(auto aggregated_and_grouped,
                             AltGroupBy(
                                 {
                                     table->GetColumnByName("argument"),
                                 },
                                 {
                                     table->GetColumnByName("key"),
                                 },
                                 {},
                                 {
                                     {"hash_list", nullptr, "agg_0", "hash_list"},
                                 },
                                 use_threads));
        ValidateOutput(aggregated_and_grouped);
        SortBy({"key_0"}, &aggregated_and_grouped);

        // Order of sub-arrays is not stable
        auto sort = [](const Array& arr) -> std::shared_ptr<Array> {
          EXPECT_OK_AND_ASSIGN(auto indices, SortIndices(arr));
          EXPECT_OK_AND_ASSIGN(auto sorted, Take(arr, indices));
          return sorted.make_array();
        };

        auto struct_arr = aggregated_and_grouped.array_as<StructArray>();

        auto list_arr = checked_pointer_cast<ListArray>(struct_arr->field(0));
        AssertDatumsEqual(ArrayFromJSON(type, R"([99, 99])"),
                          sort(*list_arr->value_slice(0)),
                          /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"([66, 88, 88])"),
                          sort(*list_arr->value_slice(1)), /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"([44, 99, null, null])"),
                          sort(*list_arr->value_slice(2)), /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"([null, null])"),
                          sort(*list_arr->value_slice(3)),
                          /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"([22, 33, 55, 77])"),
                          sort(*list_arr->value_slice(4)), /*verbose=*/true);
      }
      {
        SCOPED_TRACE("without nulls");
        auto table =
            TableFromJSON(schema({field("argument", type), field("key", int64())}), {R"([
    [99,  1],
    [99,  1]
])",
                                                                                     R"([
    [88,  2],
    [100,   3],
    [100,   3]
])",
                                                                                     R"([
    [86,   4],
    [86,   4]
])",
                                                                                     R"([
    [77,  null],
    [99,  3]
])",
                                                                                     R"([
    [88,  2],
    [66, 2]
])",
                                                                                     R"([
    [55, null],
    [44,  3]
  ])",
                                                                                     R"([
    [33,    null],
    [22,    null]
  ])"});

        ASSERT_OK_AND_ASSIGN(auto aggregated_and_grouped,
                             AltGroupBy(
                                 {
                                     table->GetColumnByName("argument"),
                                 },
                                 {
                                     table->GetColumnByName("key"),
                                 },
                                 {},
                                 {
                                     {"hash_list", nullptr, "agg_0", "hash_list"},
                                 },
                                 use_threads));
        ValidateOutput(aggregated_and_grouped);
        SortBy({"key_0"}, &aggregated_and_grouped);

        // Order of sub-arrays is not stable
        auto sort = [](const Array& arr) -> std::shared_ptr<Array> {
          EXPECT_OK_AND_ASSIGN(auto indices, SortIndices(arr));
          EXPECT_OK_AND_ASSIGN(auto sorted, Take(arr, indices));
          return sorted.make_array();
        };

        auto struct_arr = aggregated_and_grouped.array_as<StructArray>();

        auto list_arr = checked_pointer_cast<ListArray>(struct_arr->field(0));
        AssertDatumsEqual(ArrayFromJSON(type, R"([99, 99])"),
                          sort(*list_arr->value_slice(0)),
                          /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"([66, 88, 88])"),
                          sort(*list_arr->value_slice(1)), /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"([44, 99, 100, 100])"),
                          sort(*list_arr->value_slice(2)), /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"([86, 86])"),
                          sort(*list_arr->value_slice(3)),
                          /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"([22, 33, 55, 77])"),
                          sort(*list_arr->value_slice(4)), /*verbose=*/true);
      }
    }
  }
}

TEST_P(GroupBy, ListBinaryTypes) {
  for (bool use_threads : {true, false}) {
    for (const auto& type : BaseBinaryTypes()) {
      SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
      {
        SCOPED_TRACE("with nulls");
        auto table = TableFromJSON(schema({
                                       field("argument0", type),
                                       field("key", int64()),
                                   }),
                                   {R"([
    [null,   1],
    ["aaaa", 1]
])",
                                    R"([
    ["babcd",2],
    [null,   3],
    ["2",    null],
    ["d",    1],
    ["bc",   2]
])",
                                    R"([
    ["bcd", 2],
    ["123", null],
    [null,  3]
])"});

        ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                             AltGroupBy(
                                 {
                                     table->GetColumnByName("argument0"),
                                 },
                                 {
                                     table->GetColumnByName("key"),
                                 },
                                 {},
                                 {
                                     {"hash_list", nullptr, "agg_0", "hash_list"},
                                 },
                                 use_threads));
        ValidateOutput(aggregated_and_grouped);
        SortBy({"key_0"}, &aggregated_and_grouped);

        // Order of sub-arrays is not stable
        auto sort = [](const Array& arr) -> std::shared_ptr<Array> {
          EXPECT_OK_AND_ASSIGN(auto indices, SortIndices(arr));
          EXPECT_OK_AND_ASSIGN(auto sorted, Take(arr, indices));
          return sorted.make_array();
        };

        const auto& struct_arr = aggregated_and_grouped.array_as<StructArray>();
        // Check the key column
        AssertDatumsEqual(ArrayFromJSON(int64(), R"([1, 2, 3, null])"),
                          struct_arr->field(struct_arr->num_fields() - 1));

        auto list_arr = checked_pointer_cast<ListArray>(struct_arr->field(0));
        AssertDatumsEqual(ArrayFromJSON(type, R"(["aaaa", "d", null])"),
                          sort(*list_arr->value_slice(0)),
                          /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"(["babcd", "bc", "bcd"])"),
                          sort(*list_arr->value_slice(1)), /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"([null, null])"),
                          sort(*list_arr->value_slice(2)), /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"(["123", "2"])"),
                          sort(*list_arr->value_slice(3)),
                          /*verbose=*/true);
      }
      {
        SCOPED_TRACE("without nulls");
        auto table = TableFromJSON(schema({
                                       field("argument0", type),
                                       field("key", int64()),
                                   }),
                                   {R"([
    ["y",   1],
    ["aaaa", 1]
])",
                                    R"([
    ["babcd",2],
    ["z",   3],
    ["2",    null],
    ["d",    1],
    ["bc",   2]
])",
                                    R"([
    ["bcd", 2],
    ["123", null],
    ["z",  3]
])"});

        ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                             AltGroupBy(
                                 {
                                     table->GetColumnByName("argument0"),
                                 },
                                 {
                                     table->GetColumnByName("key"),
                                 },
                                 {},
                                 {
                                     {"hash_list", nullptr, "agg_0", "hash_list"},
                                 },
                                 use_threads));
        ValidateOutput(aggregated_and_grouped);
        SortBy({"key_0"}, &aggregated_and_grouped);

        // Order of sub-arrays is not stable
        auto sort = [](const Array& arr) -> std::shared_ptr<Array> {
          EXPECT_OK_AND_ASSIGN(auto indices, SortIndices(arr));
          EXPECT_OK_AND_ASSIGN(auto sorted, Take(arr, indices));
          return sorted.make_array();
        };

        const auto& struct_arr = aggregated_and_grouped.array_as<StructArray>();
        // Check the key column
        AssertDatumsEqual(ArrayFromJSON(int64(), R"([1, 2, 3, null])"),
                          struct_arr->field(struct_arr->num_fields() - 1));

        auto list_arr = checked_pointer_cast<ListArray>(struct_arr->field(0));
        AssertDatumsEqual(ArrayFromJSON(type, R"(["aaaa", "d", "y"])"),
                          sort(*list_arr->value_slice(0)),
                          /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"(["babcd", "bc", "bcd"])"),
                          sort(*list_arr->value_slice(1)), /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"(["z", "z"])"),
                          sort(*list_arr->value_slice(2)), /*verbose=*/true);
        AssertDatumsEqual(ArrayFromJSON(type, R"(["123", "2"])"),
                          sort(*list_arr->value_slice(3)),
                          /*verbose=*/true);
      }
    }
  }
}

TEST_P(GroupBy, ListMiscTypes) {
  auto in_schema = schema({
      field("floats", float64()),
      field("nulls", null()),
      field("booleans", boolean()),
      field("decimal128", decimal128(3, 2)),
      field("decimal256", decimal256(3, 2)),
      field("fixed_binary", fixed_size_binary(3)),
      field("key", int64()),
  });
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

    auto table = TableFromJSON(in_schema, {R"([
        [null, null, true,   null,    null,    null,  1],
        [1.0,  null, true,   "1.01",  "1.01",  "aaa", 1]
        ])",
                                           R"([
        [0.0,   null, false, "0.00",  "0.00",  "bac", 2],
        [null,  null, false, null,    null,    null,  3],
        [4.0,   null, null,  "4.01",  "4.01",  "234", null],
        [3.25,  null, true,  "3.25",  "3.25",  "ddd", 1],
        [0.125, null, false, "0.12",  "0.12",  "bcd", 2]
        ])",
                                           R"([
        [-0.25, null, false, "-0.25", "-0.25", "bab", 2],
        [0.75,  null, true,  "0.75",  "0.75",  "123", null],
        [null,  null, true,  null,    null,    null,  3]
        ])"});

    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 table->GetColumnByName("floats"),
                                 table->GetColumnByName("nulls"),
                                 table->GetColumnByName("booleans"),
                                 table->GetColumnByName("decimal128"),
                                 table->GetColumnByName("decimal256"),
                                 table->GetColumnByName("fixed_binary"),
                             },
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_list", nullptr},
                                 {"hash_list", nullptr},
                                 {"hash_list", nullptr},
                                 {"hash_list", nullptr},
                                 {"hash_list", nullptr},
                                 {"hash_list", nullptr},
                             },
                             use_threads));
    ValidateOutput(aggregated_and_grouped);
    SortBy({"key_0"}, &aggregated_and_grouped);

    // Order of sub-arrays is not stable
    auto sort = [](const Array& arr) -> std::shared_ptr<Array> {
      EXPECT_OK_AND_ASSIGN(auto indices, SortIndices(arr));
      EXPECT_OK_AND_ASSIGN(auto sorted, Take(arr, indices));
      return sorted.make_array();
    };

    const auto& struct_arr = aggregated_and_grouped.array_as<StructArray>();
    //  Check the key column
    AssertDatumsEqual(ArrayFromJSON(int64(), R"([1, 2, 3, null])"),
                      struct_arr->field(struct_arr->num_fields() - 1));

    //  Check values individually
    auto type_0 = float64();
    auto list_arr_0 = checked_pointer_cast<ListArray>(struct_arr->field(0));
    AssertDatumsEqual(ArrayFromJSON(type_0, R"([1.0, 3.25, null])"),
                      sort(*list_arr_0->value_slice(0)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_0, R"([-0.25, 0.0, 0.125])"),
                      sort(*list_arr_0->value_slice(1)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_0, R"([null, null])"),
                      sort(*list_arr_0->value_slice(2)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_0, R"([0.75, 4.0])"),
                      sort(*list_arr_0->value_slice(3)),
                      /*verbose=*/true);

    auto type_1 = null();
    auto list_arr_1 = checked_pointer_cast<ListArray>(struct_arr->field(1));
    AssertDatumsEqual(ArrayFromJSON(type_1, R"([null, null, null])"),
                      sort(*list_arr_1->value_slice(0)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_1, R"([null, null, null])"),
                      sort(*list_arr_1->value_slice(1)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_1, R"([null, null])"),
                      sort(*list_arr_1->value_slice(2)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_1, R"([null, null])"),
                      sort(*list_arr_1->value_slice(3)),
                      /*verbose=*/true);

    auto type_2 = boolean();
    auto list_arr_2 = checked_pointer_cast<ListArray>(struct_arr->field(2));
    AssertDatumsEqual(ArrayFromJSON(type_2, R"([true, true, true])"),
                      sort(*list_arr_2->value_slice(0)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_2, R"([false, false, false])"),
                      sort(*list_arr_2->value_slice(1)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_2, R"([false, true])"),
                      sort(*list_arr_2->value_slice(2)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_2, R"([true, null])"),
                      sort(*list_arr_2->value_slice(3)),
                      /*verbose=*/true);

    auto type_3 = decimal128(3, 2);
    auto list_arr_3 = checked_pointer_cast<ListArray>(struct_arr->field(3));
    AssertDatumsEqual(ArrayFromJSON(type_3, R"(["1.01", "3.25", null])"),
                      sort(*list_arr_3->value_slice(0)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_3, R"(["-0.25", "0.00", "0.12"])"),
                      sort(*list_arr_3->value_slice(1)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_3, R"([null, null])"),
                      sort(*list_arr_3->value_slice(2)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_3, R"(["0.75", "4.01"])"),
                      sort(*list_arr_3->value_slice(3)),
                      /*verbose=*/true);

    auto type_4 = decimal256(3, 2);
    auto list_arr_4 = checked_pointer_cast<ListArray>(struct_arr->field(4));
    AssertDatumsEqual(ArrayFromJSON(type_4, R"(["1.01", "3.25", null])"),
                      sort(*list_arr_4->value_slice(0)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_4, R"(["-0.25", "0.00", "0.12"])"),
                      sort(*list_arr_4->value_slice(1)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_4, R"([null, null])"),
                      sort(*list_arr_4->value_slice(2)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_4, R"(["0.75", "4.01"])"),
                      sort(*list_arr_4->value_slice(3)),
                      /*verbose=*/true);

    auto type_5 = fixed_size_binary(3);
    auto list_arr_5 = checked_pointer_cast<ListArray>(struct_arr->field(5));
    AssertDatumsEqual(ArrayFromJSON(type_5, R"(["aaa", "ddd", null])"),
                      sort(*list_arr_5->value_slice(0)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_5, R"(["bab", "bac", "bcd"])"),
                      sort(*list_arr_5->value_slice(1)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_5, R"([null, null])"),
                      sort(*list_arr_5->value_slice(2)),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(type_5, R"(["123", "234"])"),
                      sort(*list_arr_5->value_slice(3)),
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, CountAndSum) {
  auto batch = RecordBatchFromJSON(
      schema({field("argument", float64()), field("key", int64())}), R"([
    [1.0,   1],
    [null,  1],
    [0.0,   2],
    [null,  3],
    [4.0,   null],
    [3.25,  1],
    [0.125, 2],
    [-0.25, 2],
    [0.75,  null],
    [null,  3]
  ])");

  std::shared_ptr<CountOptions> count_opts;
  auto count_nulls_opts = std::make_shared<CountOptions>(CountOptions::ONLY_NULL);
  auto count_all_opts = std::make_shared<CountOptions>(CountOptions::ALL);
  auto min_count_opts =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/3);
  ASSERT_OK_AND_ASSIGN(
      Datum aggregated_and_grouped,
      AltGroupBy(
          {
              // NB: passing an argument twice or also using it as a key is legal
              batch->GetColumnByName("argument"),
              batch->GetColumnByName("argument"),
              batch->GetColumnByName("argument"),
              batch->GetColumnByName("argument"),
              batch->GetColumnByName("argument"),
              batch->GetColumnByName("key"),
          },
          {
              batch->GetColumnByName("key"),
          },
          {},
          {
              {"hash_count", count_opts, "agg_0", "hash_count"},
              {"hash_count", count_nulls_opts, "agg_1", "hash_count"},
              {"hash_count", count_all_opts, "agg_2", "hash_count"},
              {"hash_count_all", "hash_count_all"},
              {"hash_sum", "agg_3", "hash_sum"},
              {"hash_sum", min_count_opts, "agg_4", "hash_sum"},
              {"hash_sum", "agg_5", "hash_sum"},
          }));

  AssertDatumsEqual(
      ArrayFromJSON(struct_({
                        field("hash_count", int64()),
                        field("hash_count", int64()),
                        field("hash_count", int64()),
                        field("hash_count_all", int64()),
                        // NB: summing a float32 array results in float64 sums
                        field("hash_sum", float64()),
                        field("hash_sum", float64()),
                        field("hash_sum", int64()),
                        field("key_0", int64()),
                    }),
                    R"([
    [2, 1, 3, 3, 4.25,   null,   3,    1],
    [3, 0, 3, 3, -0.125, -0.125, 6,    2],
    [0, 2, 2, 2, null,   null,   6,    3],
    [2, 0, 2, 2, 4.75,   null,   null, null]
  ])"),
      aggregated_and_grouped,
      /*verbose=*/true);
}

TEST_P(GroupBy, StandAloneNullaryCount) {
  auto batch = RecordBatchFromJSON(
      schema({field("argument", float64()), field("key", int64())}), R"([
    [1.0,   1],
    [null,  1],
    [0.0,   2],
    [null,  3],
    [4.0,   null],
    [3.25,  1],
    [0.125, 2],
    [-0.25, 2],
    [0.75,  null],
    [null,  3]
  ])");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       AltGroupBy(
                           // zero arguments for aggregations because only the
                           // nullary hash_count_all aggregation is present
                           {},
                           {
                               batch->GetColumnByName("key"),
                           },
                           {},
                           {
                               {"hash_count_all", "hash_count_all"},
                           }));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("hash_count_all", int64()),
                                      field("key_0", int64()),
                                  }),
                                  R"([
    [3, 1],
    [3, 2],
    [2, 3],
    [2, null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST_P(GroupBy, Product) {
  auto batch = RecordBatchFromJSON(
      schema({field("argument", float64()), field("key", int64())}), R"([
    [-1.0,  1],
    [null,  1],
    [0.0,   2],
    [null,  3],
    [4.0,   null],
    [3.25,  1],
    [0.125, 2],
    [-0.25, 2],
    [0.75,  null],
    [null,  3]
  ])");

  auto min_count =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/3);
  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       AltGroupBy(
                           {
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("key"),
                               batch->GetColumnByName("argument"),
                           },
                           {
                               batch->GetColumnByName("key"),
                           },
                           {},
                           {
                               {"hash_product", nullptr, "agg_0", "hash_product"},
                               {"hash_product", nullptr, "agg_1", "hash_product"},
                               {"hash_product", min_count, "agg_2", "hash_product"},
                           }));

  AssertDatumsApproxEqual(ArrayFromJSON(struct_({
                                            field("hash_product", float64()),
                                            field("hash_product", int64()),
                                            field("hash_product", float64()),
                                            field("key_0", int64()),
                                        }),
                                        R"([
    [-3.25, 1,    null, 1],
    [-0.0,  8,    -0.0, 2],
    [null,  9,    null, 3],
    [3.0,   null, null, null]
  ])"),
                          aggregated_and_grouped,
                          /*verbose=*/true);

  // Overflow should wrap around
  batch = RecordBatchFromJSON(schema({field("argument", int64()), field("key", int64())}),
                              R"([
    [8589934592, 1],
    [8589934593, 1]
  ])");

  ASSERT_OK_AND_ASSIGN(aggregated_and_grouped,
                       AltGroupBy(
                           {
                               batch->GetColumnByName("argument"),
                           },
                           {
                               batch->GetColumnByName("key"),
                           },
                           {},
                           {
                               {"hash_product", nullptr, "agg_0", "hash_product"},
                           }));

  AssertDatumsApproxEqual(ArrayFromJSON(struct_({
                                            field("hash_product", int64()),
                                            field("key_0", int64()),
                                        }),
                                        R"([[8589934592, 1]])"),
                          aggregated_and_grouped,
                          /*verbose=*/true);
}

TEST_P(GroupBy, SumMeanProductKeepNulls) {
  auto batch = RecordBatchFromJSON(
      schema({field("argument", float64()), field("key", int64())}), R"([
    [-1.0,  1],
    [null,  1],
    [0.0,   2],
    [null,  3],
    [4.0,   null],
    [3.25,  1],
    [0.125, 2],
    [-0.25, 2],
    [0.75,  null],
    [null,  3]
  ])");

  auto keep_nulls = std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false);
  auto min_count =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false, /*min_count=*/3);
  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       AltGroupBy(
                           {
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                           },
                           {
                               batch->GetColumnByName("key"),
                           },
                           {},
                           {
                               {"hash_sum", keep_nulls, "agg_0", "hash_sum"},
                               {"hash_sum", min_count, "agg_1", "hash_sum"},
                               {"hash_mean", keep_nulls, "agg_2", "hash_mean"},
                               {"hash_mean", min_count, "agg_3", "hash_mean"},
                               {"hash_product", keep_nulls, "agg_4", "hash_product"},
                               {"hash_product", min_count, "agg_5", "hash_product"},
                           }));

  AssertDatumsApproxEqual(ArrayFromJSON(struct_({
                                            field("hash_sum", float64()),
                                            field("hash_sum", float64()),
                                            field("hash_mean", float64()),
                                            field("hash_mean", float64()),
                                            field("hash_product", float64()),
                                            field("hash_product", float64()),
                                            field("key_0", int64()),
                                        }),
                                        R"([
    [null,   null,   null,       null,       null, null, 1],
    [-0.125, -0.125, -0.0416667, -0.0416667, -0.0, -0.0, 2],
    [null,   null,   null,       null,       null, null, 3],
    [4.75,   null,   2.375,      null,       3.0,  null, null]
  ])"),
                          aggregated_and_grouped,
                          /*verbose=*/true);
}

TEST_P(GroupBy, SumOnlyStringAndDictKeys) {
  for (auto key_type : {utf8(), dictionary(int32(), utf8())}) {
    SCOPED_TRACE("key type: " + key_type->ToString());

    auto batch = RecordBatchFromJSON(
        schema({field("agg_0", float64()), field("key", key_type)}), R"([
      [1.0,   "alfa"],
      [null,  "alfa"],
      [0.0,   "beta"],
      [null,  "gama"],
      [4.0,    null ],
      [3.25,  "alfa"],
      [0.125, "beta"],
      [-0.25, "beta"],
      [0.75,   null ],
      [null,  "gama"]
    ])");

    ASSERT_OK_AND_ASSIGN(
        Datum aggregated_and_grouped,
        AltGroupBy({batch->GetColumnByName("agg_0")}, {batch->GetColumnByName("key")}, {},
                   {
                       {"hash_sum", nullptr, "agg_0", "hash_sum"},
                   }));
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_sum", float64()),
                                        field("key_0", key_type),
                                    }),
                                    R"([
    [4.25,   "alfa"],
    [-0.125, "beta"],
    [null,   "gama"],
    [4.75,    null ]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, ConcreteCaseWithValidateGroupBy) {
  auto batch =
      RecordBatchFromJSON(schema({field("agg_0", float64()), field("key", utf8())}), R"([
    [1.0,   "alfa"],
    [null,  "alfa"],
    [0.0,   "beta"],
    [null,  "gama"],
    [4.0,    null ],
    [3.25,  "alfa"],
    [0.125, "beta"],
    [-0.25, "beta"],
    [0.75,   null ],
    [null,  "gama"]
  ])");

  std::shared_ptr<ScalarAggregateOptions> keepna =
      std::make_shared<ScalarAggregateOptions>(false, 1);
  std::shared_ptr<CountOptions> nulls =
      std::make_shared<CountOptions>(CountOptions::ONLY_NULL);
  std::shared_ptr<CountOptions> non_null =
      std::make_shared<CountOptions>(CountOptions::ONLY_VALID);

  for (auto agg : {
           Aggregate{"hash_sum", nullptr, "agg_0", "hash_sum"},
           Aggregate{"hash_count", non_null, "agg_0", "hash_count"},
           Aggregate{"hash_count", nulls, "agg_0", "hash_count"},
           Aggregate{"hash_min_max", nullptr, "agg_0", "hash_min_max"},
           Aggregate{"hash_min_max", keepna, "agg_0", "hash_min_max"},
       }) {
    SCOPED_TRACE(agg.function);
    ValidateGroupBy({agg}, {batch->GetColumnByName("agg_0")},
                    {batch->GetColumnByName("key")});
  }
}

// Count nulls/non_nulls from record batch with no nulls
TEST_P(GroupBy, CountNull) {
  auto batch =
      RecordBatchFromJSON(schema({field("agg_0", float64()), field("key", utf8())}), R"([
    [1.0, "alfa"],
    [2.0, "beta"],
    [3.0, "gama"]
  ])");

  std::shared_ptr<CountOptions> keepna =
      std::make_shared<CountOptions>(CountOptions::ONLY_NULL);
  std::shared_ptr<CountOptions> skipna =
      std::make_shared<CountOptions>(CountOptions::ONLY_VALID);

  for (auto agg : {
           Aggregate{"hash_count", keepna, "agg_0", "hash_count"},
           Aggregate{"hash_count", skipna, "agg_0", "hash_count"},
       }) {
    SCOPED_TRACE(agg.function);
    ValidateGroupBy({agg}, {batch->GetColumnByName("agg_0")},
                    {batch->GetColumnByName("key")});
  }
}

TEST_P(GroupBy, RandomArraySum) {
  std::shared_ptr<ScalarAggregateOptions> options =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/0);
  for (int64_t length : {1 << 10, 1 << 12, 1 << 15}) {
    for (auto null_probability : {0.0, 0.01, 0.5, 1.0}) {
      auto batch = random::GenerateBatch(
          {
              field(
                  "agg_0", float32(),
                  key_value_metadata({{"null_probability", ToChars(null_probability)}})),
              field("key", int64(), key_value_metadata({{"min", "0"}, {"max", "100"}})),
          },
          length, 0xDEADBEEF);

      ValidateGroupBy(
          {
              {"hash_sum", options, "agg_0", "hash_sum"},
          },
          {batch->GetColumnByName("agg_0")}, {batch->GetColumnByName("key")},
          /*naive=*/false);
    }
  }
}

TEST_P(GroupBy, WithChunkedArray) {
  auto table =
      TableFromJSON(schema({field("argument", float64()), field("key", int64())}),
                    {R"([{"argument": 1.0,   "key": 1},
                         {"argument": null,  "key": 1}
                        ])",
                     R"([{"argument": 0.0,   "key": 2},
                         {"argument": null,  "key": 3},
                         {"argument": 4.0,   "key": null},
                         {"argument": 3.25,  "key": 1},
                         {"argument": 0.125, "key": 2},
                         {"argument": -0.25, "key": 2},
                         {"argument": 0.75,  "key": null},
                         {"argument": null,  "key": 3}
                        ])"});
  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       AltGroupBy(
                           {
                               table->GetColumnByName("argument"),
                               table->GetColumnByName("argument"),
                               table->GetColumnByName("argument"),
                           },
                           {
                               table->GetColumnByName("key"),
                           },
                           {},
                           {
                               {"hash_count", nullptr, "agg_0", "hash_count"},
                               {"hash_sum", nullptr, "agg_1", "hash_sum"},
                               {"hash_min_max", nullptr, "agg_2", "hash_min_max"},
                           }));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("hash_count", int64()),
                                      field("hash_sum", float64()),
                                      field("hash_min_max", struct_({
                                                                field("min", float64()),
                                                                field("max", float64()),
                                                            })),
                                      field("key_0", int64()),
                                  }),
                                  R"([
    [2, 4.25,   {"min": 1.0,   "max": 3.25},  1],
    [3, -0.125, {"min": -0.25, "max": 0.125}, 2],
    [0, null,   {"min": null,  "max": null},  3],
    [2, 4.75,   {"min": 0.75,  "max": 4.0},   null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST_P(GroupBy, MinMaxWithNewGroupsInChunkedArray) {
  auto table = TableFromJSON(
      schema({field("argument", int64()), field("key", int64())}),
      {R"([{"argument": 1, "key": 0}])", R"([{"argument": 0,   "key": 1}])"});
  ScalarAggregateOptions count_options;
  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       AltGroupBy(
                           {
                               table->GetColumnByName("argument"),
                           },
                           {
                               table->GetColumnByName("key"),
                           },
                           {},
                           {
                               {"hash_min_max", nullptr, "agg_0", "hash_min_max"},
                           }));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("hash_min_max", struct_({
                                                                field("min", int64()),
                                                                field("max", int64()),
                                                            })),
                                      field("key_0", int64()),
                                  }),
                                  R"([
    [{"min": 1, "max": 1}, 0],
    [{"min": 0, "max": 0}, 1]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST_P(GroupBy, SmallChunkSizeSumOnly) {
  auto batch = RecordBatchFromJSON(
      schema({field("argument", float64()), field("key", int64())}), R"([
    [1.0,   1],
    [null,  1],
    [0.0,   2],
    [null,  3],
    [4.0,   null],
    [3.25,  1],
    [0.125, 2],
    [-0.25, 2],
    [0.75,  null],
    [null,  3]
  ])");
  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       AltGroupBy({batch->GetColumnByName("argument")},
                                  {batch->GetColumnByName("key")}, {},
                                  {
                                      {"hash_sum", nullptr, "agg_0", "hash_sum"},
                                  },
                                  small_chunksize_context()));
  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("hash_sum", float64()),
                                      field("key_0", int64()),
                                  }),
                                  R"([
    [4.25,   1],
    [-0.125, 2],
    [null,   3],
    [4.75,   null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST_P(GroupBy, CountWithNullType) {
  auto table =
      TableFromJSON(schema({field("argument", null()), field("key", int64())}), {R"([
    [null,  1],
    [null,  1]
                        ])",
                                                                                 R"([
    [null, 2],
    [null, 3],
    [null, null],
    [null, 1],
    [null, 2]
                        ])",
                                                                                 R"([
    [null, 2],
    [null, null],
    [null, 3]
                        ])"});

  auto all = std::make_shared<CountOptions>(CountOptions::ALL);
  auto only_valid = std::make_shared<CountOptions>(CountOptions::ONLY_VALID);
  auto only_null = std::make_shared<CountOptions>(CountOptions::ONLY_NULL);

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_count", all},
                                 {"hash_count", only_valid},
                                 {"hash_count", only_null},
                             },
                             use_threads));
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_count", int64()),
                                        field("hash_count", int64()),
                                        field("hash_count", int64()),
                                        field("key_0", int64()),
                                    }),
                                    R"([
    [3, 0, 3, 1],
    [3, 0, 3, 2],
    [2, 0, 2, 3],
    [2, 0, 2, null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, CountWithNullTypeEmptyTable) {
  auto table = TableFromJSON(schema({field("argument", null()), field("key", int64())}),
                             {R"([])"});

  auto all = std::make_shared<CountOptions>(CountOptions::ALL);
  auto only_valid = std::make_shared<CountOptions>(CountOptions::ONLY_VALID);
  auto only_null = std::make_shared<CountOptions>(CountOptions::ONLY_NULL);

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_count", all},
                                 {"hash_count", only_valid},
                                 {"hash_count", only_null},
                             },
                             use_threads));
    auto struct_arr = aggregated_and_grouped.array_as<StructArray>();
    for (auto& field : struct_arr->fields()) {
      AssertDatumsEqual(ArrayFromJSON(int64(), "[]"), field, /*verbose=*/true);
    }
  }
}

TEST_P(GroupBy, SingleNullTypeKey) {
  auto table =
      TableFromJSON(schema({field("argument", int64()), field("key", null())}), {R"([
    [1,    null],
    [1,    null]
                        ])",
                                                                                 R"([
    [2,    null],
    [3,    null],
    [null, null],
    [1,    null],
    [2,    null]
                        ])",
                                                                                 R"([
    [2,    null],
    [null, null],
    [3,    null]
                        ])"});

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_count", nullptr},
                                 {"hash_sum", nullptr},
                                 {"hash_mean", nullptr},
                                 {"hash_min_max", nullptr},
                             },
                             use_threads));
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_count", int64()),
                                        field("hash_sum", int64()),
                                        field("hash_mean", float64()),
                                        field("hash_min_max", struct_({
                                                                  field("min", int64()),
                                                                  field("max", int64()),
                                                              })),
                                        field("key_0", null()),
                                    }),
                                    R"([
    [8, 15, 1.875, {"min": 1, "max": 3}, null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, MultipleKeysIncludesNullType) {
  auto table = TableFromJSON(schema({field("argument", float64()), field("key_0", utf8()),
                                     field("key_1", null())}),
                             {R"([
    [1.0,   "a",      null],
    [null,  "a",      null]
                        ])",
                              R"([
    [0.0,   "bcdefg", null],
    [null,  "aa",     null],
    [4.0,   null,     null],
    [3.25,  "a",      null],
    [0.125, "bcdefg", null]
                        ])",
                              R"([
    [-0.25, "bcdefg", null],
    [0.75,  null,     null],
    [null,  "aa",     null]
                        ])"});

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum aggregated_and_grouped,
        GroupByTest(
            {
                table->GetColumnByName("argument"),
                table->GetColumnByName("argument"),
                table->GetColumnByName("argument"),
            },
            {table->GetColumnByName("key_0"), table->GetColumnByName("key_1")},
            {
                {"hash_count", nullptr},
                {"hash_sum", nullptr},
                {"hash_min_max", nullptr},
            },
            use_threads));
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_count", int64()),
                                        field("hash_sum", float64()),
                                        field("hash_min_max", struct_({
                                                                  field("min", float64()),
                                                                  field("max", float64()),
                                                              })),
                                        field("key_0", utf8()),
                                        field("key_1", null()),
                                    }),
                                    R"([
    [2, 4.25,   {"min": 1,     "max": 3.25},  "a",      null],
    [0, null,   {"min": null,  "max": null},  "aa",     null],
    [3, -0.125, {"min": -0.25, "max": 0.125}, "bcdefg", null],
    [2, 4.75,   {"min": 0.75,  "max": 4},     null,     null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, SumNullType) {
  auto table =
      TableFromJSON(schema({field("argument", null()), field("key", int64())}), {R"([
    [null,  1],
    [null,  1]
                        ])",
                                                                                 R"([
    [null, 2],
    [null, 3],
    [null, null],
    [null, 1],
    [null, 2]
                        ])",
                                                                                 R"([
    [null, 2],
    [null, null],
    [null, 3]
                        ])"});

  auto no_min =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/0);
  auto min_count =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/3);
  auto keep_nulls =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false, /*min_count=*/0);
  auto keep_nulls_min_count =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false, /*min_count=*/3);

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_sum", no_min},
                                 {"hash_sum", keep_nulls},
                                 {"hash_sum", min_count},
                                 {"hash_sum", keep_nulls_min_count},
                             },
                             use_threads));
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_sum", int64()),
                                        field("hash_sum", int64()),
                                        field("hash_sum", int64()),
                                        field("hash_sum", int64()),
                                        field("key_0", int64()),
                                    }),
                                    R"([
    [0, null, null, null, 1],
    [0, null, null, null, 2],
    [0, null, null, null, 3],
    [0, null, null, null, null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, ProductNullType) {
  auto table =
      TableFromJSON(schema({field("argument", null()), field("key", int64())}), {R"([
    [null,  1],
    [null,  1]
                        ])",
                                                                                 R"([
    [null, 2],
    [null, 3],
    [null, null],
    [null, 1],
    [null, 2]
                        ])",
                                                                                 R"([
    [null, 2],
    [null, null],
    [null, 3]
                        ])"});

  auto no_min =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/0);
  auto min_count =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/3);
  auto keep_nulls =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false, /*min_count=*/0);
  auto keep_nulls_min_count =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false, /*min_count=*/3);

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_product", no_min},
                                 {"hash_product", keep_nulls},
                                 {"hash_product", min_count},
                                 {"hash_product", keep_nulls_min_count},
                             },
                             use_threads));
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_product", int64()),
                                        field("hash_product", int64()),
                                        field("hash_product", int64()),
                                        field("hash_product", int64()),
                                        field("key_0", int64()),
                                    }),
                                    R"([
    [1, null, null, null, 1],
    [1, null, null, null, 2],
    [1, null, null, null, 3],
    [1, null, null, null, null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, MeanNullType) {
  auto table =
      TableFromJSON(schema({field("argument", null()), field("key", int64())}), {R"([
    [null,  1],
    [null,  1]
                        ])",
                                                                                 R"([
    [null, 2],
    [null, 3],
    [null, null],
    [null, 1],
    [null, 2]
                        ])",
                                                                                 R"([
    [null, 2],
    [null, null],
    [null, 3]
                        ])"});

  auto no_min =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/0);
  auto min_count =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/3);
  auto keep_nulls =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false, /*min_count=*/0);
  auto keep_nulls_min_count =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false, /*min_count=*/3);

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_mean", no_min},
                                 {"hash_mean", keep_nulls},
                                 {"hash_mean", min_count},
                                 {"hash_mean", keep_nulls_min_count},
                             },
                             use_threads));
    SortBy({"key_0"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("hash_mean", float64()),
                                        field("hash_mean", float64()),
                                        field("hash_mean", float64()),
                                        field("hash_mean", float64()),
                                        field("key_0", int64()),
                                    }),
                                    R"([
    [0, null, null, null, 1],
    [0, null, null, null, 2],
    [0, null, null, null, 3],
    [0, null, null, null, null]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, NullTypeEmptyTable) {
  auto table = TableFromJSON(schema({field("argument", null()), field("key", int64())}),
                             {R"([])"});

  auto no_min =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/0);
  auto min_count =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/true, /*min_count=*/3);
  auto keep_nulls =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false, /*min_count=*/0);
  auto keep_nulls_min_count =
      std::make_shared<ScalarAggregateOptions>(/*skip_nulls=*/false, /*min_count=*/3);

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         GroupByTest(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_sum", no_min},
                                 {"hash_product", min_count},
                                 {"hash_mean", keep_nulls},
                             },
                             use_threads));
    auto struct_arr = aggregated_and_grouped.array_as<StructArray>();
    AssertDatumsEqual(ArrayFromJSON(int64(), "[]"), struct_arr->field(0),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(int64(), "[]"), struct_arr->field(1),
                      /*verbose=*/true);
    AssertDatumsEqual(ArrayFromJSON(float64(), "[]"), struct_arr->field(2),
                      /*verbose=*/true);
  }
}

TEST_P(GroupBy, OnlyKeys) {
  auto table =
      TableFromJSON(schema({field("key_0", int64()), field("key_1", utf8())}), {R"([
    [1,    "a"],
    [null, "a"]
                        ])",
                                                                                R"([
    [0,    "bcdefg"],
    [null, "aa"],
    [3,    null],
    [1,    "a"],
    [2,    "bcdefg"]
                        ])",
                                                                                R"([
    [0,    "bcdefg"],
    [1,    null],
    [null, "a"]
                        ])"});

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum aggregated_and_grouped,
        GroupByTest({},
                    {table->GetColumnByName("key_0"), table->GetColumnByName("key_1")},
                    {}, use_threads));
    SortBy({"key_0", "key_1"}, &aggregated_and_grouped);

    AssertDatumsEqual(ArrayFromJSON(struct_({
                                        field("key_0", int64()),
                                        field("key_1", utf8()),
                                    }),
                                    R"([
    [0,    "bcdefg"],
    [1,    "a"],
    [1,    null],
    [2,    "bcdefg"],
    [3,    null],
    [null, "a"],
    [null, "aa"]
  ])"),
                      aggregated_and_grouped,
                      /*verbose=*/true);
  }
}

INSTANTIATE_TEST_SUITE_P(GroupBy, GroupBy, ::testing::Values(RunGroupByImpl));

class SegmentedScalarGroupBy : public GroupBy {};

class SegmentedKeyGroupBy : public GroupBy {};

void TestSegment(GroupByFunction group_by, const std::shared_ptr<Table>& table,
                 Datum output, const std::vector<Datum>& keys,
                 const std::vector<Datum>& segment_keys, bool is_scalar_aggregate) {
  const char* names[] = {
      is_scalar_aggregate ? "count" : "hash_count",
      is_scalar_aggregate ? "sum" : "hash_sum",
      is_scalar_aggregate ? "min_max" : "hash_min_max",
  };
  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       group_by(
                           {
                               table->GetColumnByName("argument"),
                               table->GetColumnByName("argument"),
                               table->GetColumnByName("argument"),
                           },
                           keys, segment_keys,
                           {
                               {names[0], nullptr, "agg_0", names[0]},
                               {names[1], nullptr, "agg_1", names[1]},
                               {names[2], nullptr, "agg_2", names[2]},
                           },
                           /*use_threads=*/false, /*naive=*/false));

  AssertDatumsEqual(output, aggregated_and_grouped, /*verbose=*/true);
}

// test with empty keys, covering code in ScalarAggregateNode
void TestSegmentScalar(GroupByFunction group_by, const std::shared_ptr<Table>& table,
                       Datum output, const std::vector<Datum>& segment_keys) {
  TestSegment(group_by, table, output, {}, segment_keys, /*scalar=*/true);
}

// test with given segment-keys and keys set to `{"key"}`, covering code in GroupByNode
void TestSegmentKey(GroupByFunction group_by, const std::shared_ptr<Table>& table,
                    Datum output, const std::vector<Datum>& segment_keys) {
  TestSegment(group_by, table, output, {table->GetColumnByName("key")}, segment_keys,
              /*scalar=*/false);
}

Result<std::shared_ptr<Table>> GetSingleSegmentInputAsChunked() {
  auto table = TableFromJSON(schema({field("argument", float64()), field("key", int64()),
                                     field("segment_key", int64())}),
                             {R"([{"argument": 1.0,   "key": 1,    "segment_key": 1},
                         {"argument": null,  "key": 1,    "segment_key": 1}
                        ])",
                              R"([{"argument": 0.0,   "key": 2,    "segment_key": 1},
                         {"argument": null,  "key": 3,    "segment_key": 1},
                         {"argument": 4.0,   "key": null, "segment_key": 1},
                         {"argument": 3.25,  "key": 1,    "segment_key": 1},
                         {"argument": 0.125, "key": 2,    "segment_key": 1},
                         {"argument": -0.25, "key": 2,    "segment_key": 1},
                         {"argument": 0.75,  "key": null, "segment_key": 1},
                         {"argument": null,  "key": 3,    "segment_key": 1}
                        ])",
                              R"([{"argument": 1.0,   "key": 1,    "segment_key": 0},
                         {"argument": null,  "key": 1,    "segment_key": 0}
                        ])",
                              R"([{"argument": 0.0,   "key": 2,    "segment_key": 0},
                         {"argument": null,  "key": 3,    "segment_key": 0},
                         {"argument": 4.0,   "key": null, "segment_key": 0},
                         {"argument": 3.25,  "key": 1,    "segment_key": 0},
                         {"argument": 0.125, "key": 2,    "segment_key": 0},
                         {"argument": -0.25, "key": 2,    "segment_key": 0},
                         {"argument": 0.75,  "key": null, "segment_key": 0},
                         {"argument": null,  "key": 3,    "segment_key": 0}
                        ])"});
  return table;
}

Result<std::shared_ptr<Table>> GetSingleSegmentInputAsCombined() {
  ARROW_ASSIGN_OR_RAISE(auto table, GetSingleSegmentInputAsChunked());
  return table->CombineChunks();
}

Result<std::shared_ptr<ChunkedArray>> GetSingleSegmentScalarOutput() {
  return ChunkedArrayFromJSON(struct_({
                                  field("count", int64()),
                                  field("sum", float64()),
                                  field("min_max", struct_({
                                                       field("min", float64()),
                                                       field("max", float64()),
                                                   })),
                                  field("key_0", int64()),
                              }),
                              {R"([
    [7, 8.875, {"min": -0.25, "max": 4.0}, 1]
  ])",
                               R"([
    [7, 8.875, {"min": -0.25, "max": 4.0}, 0]
  ])"});
}

Result<std::shared_ptr<ChunkedArray>> GetSingleSegmentKeyOutput() {
  return ChunkedArrayFromJSON(struct_({
                                  field("hash_count", int64()),
                                  field("hash_sum", float64()),
                                  field("hash_min_max", struct_({
                                                            field("min", float64()),
                                                            field("max", float64()),
                                                        })),
                                  field("key_0", int64()),
                                  field("key_1", int64()),
                              }),
                              {R"([
    [2, 4.25,   {"min": 1.0,   "max": 3.25},  1, 1],
    [3, -0.125, {"min": -0.25, "max": 0.125}, 2, 1],
    [0, null,   {"min": null,  "max": null},  3, 1],
    [2, 4.75,   {"min": 0.75,  "max": 4.0},   null, 1]
  ])",
                               R"([
    [2, 4.25,   {"min": 1.0,   "max": 3.25},  1, 0],
    [3, -0.125, {"min": -0.25, "max": 0.125}, 2, 0],
    [0, null,   {"min": null,  "max": null},  3, 0],
    [2, 4.75,   {"min": 0.75,  "max": 4.0},   null, 0]
  ])"});
}

void TestSingleSegmentScalar(GroupByFunction group_by,
                             std::function<Result<std::shared_ptr<Table>>()> get_table) {
  ASSERT_OK_AND_ASSIGN(auto table, get_table());
  ASSERT_OK_AND_ASSIGN(auto output, GetSingleSegmentScalarOutput());
  TestSegmentScalar(group_by, table, output, {table->GetColumnByName("segment_key")});
}

void TestSingleSegmentKey(GroupByFunction group_by,
                          std::function<Result<std::shared_ptr<Table>>()> get_table) {
  ASSERT_OK_AND_ASSIGN(auto table, get_table());
  ASSERT_OK_AND_ASSIGN(auto output, GetSingleSegmentKeyOutput());
  TestSegmentKey(group_by, table, output, {table->GetColumnByName("segment_key")});
}

TEST_P(SegmentedScalarGroupBy, SingleSegmentScalarChunked) {
  TestSingleSegmentScalar(GetParam(), GetSingleSegmentInputAsChunked);
}

TEST_P(SegmentedScalarGroupBy, SingleSegmentScalarCombined) {
  TestSingleSegmentScalar(GetParam(), GetSingleSegmentInputAsCombined);
}

TEST_P(SegmentedKeyGroupBy, SingleSegmentKeyChunked) {
  TestSingleSegmentKey(GetParam(), GetSingleSegmentInputAsChunked);
}

TEST_P(SegmentedKeyGroupBy, SingleSegmentKeyCombined) {
  TestSingleSegmentKey(GetParam(), GetSingleSegmentInputAsCombined);
}

// extracts one segment of the obtained (single-segment-key) table
Result<std::shared_ptr<Table>> GetEmptySegmentKeysInput(
    std::function<Result<std::shared_ptr<Table>>()> get_table) {
  ARROW_ASSIGN_OR_RAISE(auto table, get_table());
  auto sliced = table->Slice(0, 10);
  ARROW_ASSIGN_OR_RAISE(auto batch, sliced->CombineChunksToBatch());
  ARROW_ASSIGN_OR_RAISE(auto array, batch->ToStructArray());
  ARROW_ASSIGN_OR_RAISE(auto chunked, ChunkedArray::Make({array}, array->type()));
  return Table::FromChunkedStructArray(chunked);
}

Result<std::shared_ptr<Table>> GetEmptySegmentKeysInputAsChunked() {
  return GetEmptySegmentKeysInput(GetSingleSegmentInputAsChunked);
}

Result<std::shared_ptr<Table>> GetEmptySegmentKeysInputAsCombined() {
  return GetEmptySegmentKeysInput(GetSingleSegmentInputAsCombined);
}

// extracts the expected output for one segment
Result<std::shared_ptr<Array>> GetEmptySegmentKeyOutput() {
  ARROW_ASSIGN_OR_RAISE(auto chunked, GetSingleSegmentKeyOutput());
  ARROW_ASSIGN_OR_RAISE(auto table, Table::FromChunkedStructArray(chunked));
  ARROW_ASSIGN_OR_RAISE(auto removed, table->RemoveColumn(table->num_columns() - 1));
  auto sliced = removed->Slice(0, 4);
  ARROW_ASSIGN_OR_RAISE(auto batch, sliced->CombineChunksToBatch());
  return batch->ToStructArray();
}

void TestEmptySegmentKey(GroupByFunction group_by,
                         std::function<Result<std::shared_ptr<Table>>()> get_table) {
  ASSERT_OK_AND_ASSIGN(auto table, get_table());
  ASSERT_OK_AND_ASSIGN(auto output, GetEmptySegmentKeyOutput());
  TestSegmentKey(group_by, table, output, {});
}

TEST_P(SegmentedKeyGroupBy, EmptySegmentKeyChunked) {
  TestEmptySegmentKey(GetParam(), GetEmptySegmentKeysInputAsChunked);
}

TEST_P(SegmentedKeyGroupBy, EmptySegmentKeyCombined) {
  TestEmptySegmentKey(GetParam(), GetEmptySegmentKeysInputAsCombined);
}

// adds a named copy of the last (single-segment-key) column to the obtained table
Result<std::shared_ptr<Table>> GetMultiSegmentInput(
    std::function<Result<std::shared_ptr<Table>>()> get_table,
    const std::string& add_name) {
  ARROW_ASSIGN_OR_RAISE(auto table, get_table());
  int last = table->num_columns() - 1;
  auto add_field = field(add_name, table->schema()->field(last)->type());
  return table->AddColumn(table->num_columns(), add_field, table->column(last));
}

Result<std::shared_ptr<Table>> GetMultiSegmentInputAsChunked(
    const std::string& add_name) {
  return GetMultiSegmentInput(GetSingleSegmentInputAsChunked, add_name);
}

Result<std::shared_ptr<Table>> GetMultiSegmentInputAsCombined(
    const std::string& add_name) {
  return GetMultiSegmentInput(GetSingleSegmentInputAsCombined, add_name);
}

// adds a named copy of the last (single-segment-key) column to the expected output table
Result<std::shared_ptr<ChunkedArray>> GetMultiSegmentKeyOutput(
    const std::string& add_name) {
  ARROW_ASSIGN_OR_RAISE(auto chunked, GetSingleSegmentKeyOutput());
  ARROW_ASSIGN_OR_RAISE(auto table, Table::FromChunkedStructArray(chunked));
  int last = table->num_columns() - 1;
  auto add_field = field(add_name, table->schema()->field(last)->type());
  ARROW_ASSIGN_OR_RAISE(auto added,
                        table->AddColumn(last + 1, add_field, table->column(last)));
  ARROW_ASSIGN_OR_RAISE(auto batch, added->CombineChunksToBatch());
  ARROW_ASSIGN_OR_RAISE(auto array, batch->ToStructArray());
  return ChunkedArray::Make({array->Slice(0, 4), array->Slice(4, 4)}, array->type());
}

void TestMultiSegmentKey(
    GroupByFunction group_by,
    std::function<Result<std::shared_ptr<Table>>(const std::string&)> get_table) {
  std::string add_name = "segment_key2";
  ASSERT_OK_AND_ASSIGN(auto table, get_table(add_name));
  ASSERT_OK_AND_ASSIGN(auto output, GetMultiSegmentKeyOutput("key_2"));
  TestSegmentKey(
      group_by, table, output,
      {table->GetColumnByName("segment_key"), table->GetColumnByName(add_name)});
}

TEST_P(SegmentedKeyGroupBy, MultiSegmentKeyChunked) {
  TestMultiSegmentKey(GetParam(), GetMultiSegmentInputAsChunked);
}

TEST_P(SegmentedKeyGroupBy, MultiSegmentKeyCombined) {
  TestMultiSegmentKey(GetParam(), GetMultiSegmentInputAsCombined);
}

INSTANTIATE_TEST_SUITE_P(SegmentedScalarGroupBy, SegmentedScalarGroupBy,
                         ::testing::Values(RunSegmentedGroupByImpl));

INSTANTIATE_TEST_SUITE_P(SegmentedKeyGroupBy, SegmentedKeyGroupBy,
                         ::testing::Values(RunSegmentedGroupByImpl));

}  // namespace compute
}  // namespace arrow
