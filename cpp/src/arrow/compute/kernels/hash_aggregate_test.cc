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
#include "arrow/util/int_util_internal.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/vector.h"

using testing::Eq;
using testing::HasSubstr;

namespace arrow {

using internal::BitmapReader;
using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {
namespace {

Result<Datum> NaiveGroupBy(std::vector<Datum> arguments, std::vector<Datum> keys,
                           const std::vector<internal::Aggregate>& aggregates) {
  ARROW_ASSIGN_OR_RAISE(auto key_batch, ExecBatch::Make(std::move(keys)));

  ARROW_ASSIGN_OR_RAISE(auto grouper,
                        internal::Grouper::Make(key_batch.GetDescriptors()));

  ARROW_ASSIGN_OR_RAISE(Datum id_batch, grouper->Consume(key_batch));

  ARROW_ASSIGN_OR_RAISE(
      auto groupings, internal::Grouper::MakeGroupings(*id_batch.array_as<UInt32Array>(),
                                                       grouper->num_groups()));

  ArrayVector out_columns;
  std::vector<std::string> out_names;

  for (size_t i = 0; i < arguments.size(); ++i) {
    out_names.push_back(aggregates[i].function);

    // trim "hash_" prefix
    auto scalar_agg_function = aggregates[i].function.substr(5);

    ARROW_ASSIGN_OR_RAISE(
        auto grouped_argument,
        internal::Grouper::ApplyGroupings(*groupings, *arguments[i].make_array()));

    ScalarVector aggregated_scalars;

    for (int64_t i_group = 0; i_group < grouper->num_groups(); ++i_group) {
      auto slice = grouped_argument->value_slice(i_group);
      if (slice->length() == 0) continue;
      ARROW_ASSIGN_OR_RAISE(
          Datum d, CallFunction(scalar_agg_function, {slice}, aggregates[i].options));
      aggregated_scalars.push_back(d.scalar());
    }

    ARROW_ASSIGN_OR_RAISE(Datum aggregated_column,
                          ScalarVectorToArray(aggregated_scalars));
    out_columns.push_back(aggregated_column.make_array());
  }

  int i = 0;
  ARROW_ASSIGN_OR_RAISE(auto uniques, grouper->GetUniques());
  for (const Datum& key : uniques.values) {
    out_columns.push_back(key.make_array());
    out_names.push_back("key_" + std::to_string(i++));
  }

  return StructArray::Make(std::move(out_columns), std::move(out_names));
}

Result<Datum> GroupByUsingExecPlan(const BatchesWithSchema& input,
                                   const std::vector<std::string>& key_names,
                                   const std::vector<std::string>& arg_names,
                                   const std::vector<internal::Aggregate>& aggregates,
                                   bool use_threads, ExecContext* ctx) {
  std::vector<FieldRef> keys(key_names.size());
  std::vector<FieldRef> targets(aggregates.size());
  std::vector<std::string> names(aggregates.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    names[i] = aggregates[i].function;
    targets[i] = FieldRef(arg_names[i]);
  }
  for (size_t i = 0; i < key_names.size(); ++i) {
    keys[i] = FieldRef(key_names[i]);
  }

  ARROW_ASSIGN_OR_RAISE(auto plan, ExecPlan::Make(ctx));
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;
  RETURN_NOT_OK(
      Declaration::Sequence(
          {
              {"source",
               SourceNodeOptions{input.schema, input.gen(use_threads, /*slow=*/false)}},
              {"aggregate",
               AggregateNodeOptions{std::move(aggregates), std::move(targets),
                                    std::move(names), std::move(keys)}},
              {"sink", SinkNodeOptions{&sink_gen}},
          })
          .AddToPlan(plan.get()));

  RETURN_NOT_OK(plan->Validate());
  RETURN_NOT_OK(plan->StartProducing());

  auto collected_fut = CollectAsyncGenerator(sink_gen);

  auto start_and_collect =
      AllComplete({plan->finished(), Future<>(collected_fut)})
          .Then([collected_fut]() -> Result<std::vector<ExecBatch>> {
            ARROW_ASSIGN_OR_RAISE(auto collected, collected_fut.result());
            return ::arrow::internal::MapVector(
                [](util::optional<ExecBatch> batch) { return std::move(*batch); },
                std::move(collected));
          });

  ARROW_ASSIGN_OR_RAISE(std::vector<ExecBatch> output_batches,
                        start_and_collect.MoveResult());

  ArrayVector out_arrays(aggregates.size() + key_names.size());
  const auto& output_schema = plan->sources()[0]->outputs()[0]->output_schema();
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

  return StructArray::Make(std::move(out_arrays), output_schema->fields());
}

/// Simpler overload where you can give the columns as datums
Result<Datum> GroupByUsingExecPlan(const std::vector<Datum>& arguments,
                                   const std::vector<Datum>& keys,
                                   const std::vector<internal::Aggregate>& aggregates,
                                   bool use_threads, ExecContext* ctx) {
  using arrow::compute::detail::ExecBatchIterator;

  FieldVector scan_fields(arguments.size() + keys.size());
  std::vector<std::string> key_names(keys.size());
  std::vector<std::string> arg_names(arguments.size());
  for (size_t i = 0; i < arguments.size(); ++i) {
    auto name = std::string("agg_") + std::to_string(i);
    scan_fields[i] = field(name, arguments[i].type());
    arg_names[i] = std::move(name);
  }
  for (size_t i = 0; i < keys.size(); ++i) {
    auto name = std::string("key_") + std::to_string(i);
    scan_fields[arguments.size() + i] = field(name, keys[i].type());
    key_names[i] = std::move(name);
  }

  std::vector<Datum> inputs = arguments;
  inputs.reserve(inputs.size() + keys.size());
  inputs.insert(inputs.end(), keys.begin(), keys.end());

  ARROW_ASSIGN_OR_RAISE(auto batch_iterator,
                        ExecBatchIterator::Make(inputs, ctx->exec_chunksize()));
  BatchesWithSchema input;
  input.schema = schema(std::move(scan_fields));
  ExecBatch batch;
  while (batch_iterator->Next(&batch)) {
    if (batch.length == 0) continue;
    input.batches.push_back(std::move(batch));
  }

  return GroupByUsingExecPlan(input, key_names, arg_names, aggregates, use_threads, ctx);
}

void ValidateGroupBy(const std::vector<internal::Aggregate>& aggregates,
                     std::vector<Datum> arguments, std::vector<Datum> keys) {
  ASSERT_OK_AND_ASSIGN(Datum expected, NaiveGroupBy(arguments, keys, aggregates));

  ASSERT_OK_AND_ASSIGN(Datum actual, GroupBy(arguments, keys, aggregates));

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

Result<Datum> GroupByTest(
    const std::vector<Datum>& arguments, const std::vector<Datum>& keys,
    const std::vector<::arrow::compute::internal::Aggregate>& aggregates,
    bool use_threads, bool use_exec_plan) {
  if (use_exec_plan) {
    return GroupByUsingExecPlan(arguments, keys, aggregates, use_threads,
                                small_chunksize_context(use_threads));
  } else {
    return internal::GroupBy(arguments, keys, aggregates, use_threads,
                             default_exec_context());
  }
}

}  // namespace

TEST(Grouper, SupportedKeys) {
  ASSERT_OK(internal::Grouper::Make({boolean()}));

  ASSERT_OK(internal::Grouper::Make({int8(), uint16(), int32(), uint64()}));

  ASSERT_OK(internal::Grouper::Make({dictionary(int64(), utf8())}));

  ASSERT_OK(internal::Grouper::Make({float16(), float32(), float64()}));

  ASSERT_OK(internal::Grouper::Make({utf8(), binary(), large_utf8(), large_binary()}));

  ASSERT_OK(internal::Grouper::Make({fixed_size_binary(16), fixed_size_binary(32)}));

  ASSERT_OK(internal::Grouper::Make({decimal128(32, 10), decimal256(76, 20)}));

  ASSERT_OK(internal::Grouper::Make({date32(), date64()}));

  for (auto unit : {
           TimeUnit::SECOND,
           TimeUnit::MILLI,
           TimeUnit::MICRO,
           TimeUnit::NANO,
       }) {
    ASSERT_OK(internal::Grouper::Make({timestamp(unit), duration(unit)}));
  }

  ASSERT_OK(internal::Grouper::Make(
      {day_time_interval(), month_interval(), month_day_nano_interval()}));

  ASSERT_OK(internal::Grouper::Make({null()}));

  ASSERT_RAISES(NotImplemented, internal::Grouper::Make({struct_({field("", int64())})}));

  ASSERT_RAISES(NotImplemented, internal::Grouper::Make({struct_({})}));

  ASSERT_RAISES(NotImplemented, internal::Grouper::Make({list(int32())}));

  ASSERT_RAISES(NotImplemented, internal::Grouper::Make({fixed_size_list(int32(), 5)}));

  ASSERT_RAISES(NotImplemented,
                internal::Grouper::Make({dense_union({field("", int32())})}));
}

struct TestGrouper {
  explicit TestGrouper(std::vector<ValueDescr> descrs) : descrs_(std::move(descrs)) {
    grouper_ = internal::Grouper::Make(descrs_).ValueOrDie();

    FieldVector fields;
    for (const auto& descr : descrs_) {
      fields.push_back(field("", descr.type));
    }
    key_schema_ = schema(std::move(fields));
  }

  void ExpectConsume(const std::string& key_json, const std::string& expected) {
    ExpectConsume(ExecBatchFromJSON(descrs_, key_json),
                  ArrayFromJSON(uint32(), expected));
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
    ExpectUniques(ExecBatchFromJSON(descrs_, uniques_json));
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
    ASSERT_OK_AND_ASSIGN(Datum id_batch, grouper_->Consume(key_batch));

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
      SCOPED_TRACE(std::to_string(i) + "th key array");
      auto original =
          key_batch[i].is_array()
              ? key_batch[i].make_array()
              : *MakeArrayFromScalar(*key_batch[i].scalar(), key_batch.length);
      ASSERT_OK_AND_ASSIGN(auto encoded, Take(*uniques_[i].make_array(), *ids));
      AssertArraysEqual(*original, *encoded, /*verbose=*/true,
                        EqualOptions().nans_equal(true));
    }
  }

  std::vector<ValueDescr> descrs_;
  std::shared_ptr<Schema> key_schema_;
  std::unique_ptr<internal::Grouper> grouper_;
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

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      NotImplemented, HasSubstr("Unifying differing dictionaries"),
      g.grouper_->Consume(*ExecBatch::Make({*DictionaryArray::FromArrays(
          ArrayFromJSON(int32(), "[0, 1]"),
          ArrayFromJSON(utf8(), R"(["different", "dictionary"])"))})));
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
    SCOPED_TRACE(std::to_string(i) + "th key batch");

    ExecBatch key_batch{
        *random::GenerateBatch(g.key_schema_->fields(), 1 << 12, 0xDEADBEEF)};
    g.ConsumeAndValidate(key_batch);
  }
}

TEST(Grouper, RandomStringInt64Keys) {
  TestGrouper g({utf8(), int64()});
  for (int i = 0; i < 4; ++i) {
    SCOPED_TRACE(std::to_string(i) + "th key batch");

    ExecBatch key_batch{
        *random::GenerateBatch(g.key_schema_->fields(), 1 << 12, 0xDEADBEEF)};
    g.ConsumeAndValidate(key_batch);
  }
}

TEST(Grouper, RandomStringInt64DoubleInt32Keys) {
  TestGrouper g({utf8(), int64(), float64(), int32()});
  for (int i = 0; i < 4; ++i) {
    SCOPED_TRACE(std::to_string(i) + "th key batch");

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
    ASSERT_OK_AND_ASSIGN(auto actual, internal::Grouper::MakeGroupings(*ids, num_groups));
    AssertArraysEqual(*expected, *actual, /*verbose=*/true);

    // validate ApplyGroupings
    ASSERT_OK_AND_ASSIGN(auto grouped_ids,
                         internal::Grouper::ApplyGroupings(*actual, *ids));

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
                                  internal::Grouper::MakeGroupings(*ids, 5));
}

TEST(Grouper, ScalarValues) {
  // large_utf8 forces GrouperImpl over GrouperFastImpl
  for (const auto& str_type : {utf8(), large_utf8()}) {
    {
      TestGrouper g({ValueDescr::Scalar(boolean()), ValueDescr::Scalar(int32()),
                     ValueDescr::Scalar(decimal128(3, 2)),
                     ValueDescr::Scalar(decimal256(3, 2)),
                     ValueDescr::Scalar(fixed_size_binary(2)),
                     ValueDescr::Scalar(str_type), ValueDescr::Array(int32())});
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
      TestGrouper g({ValueDescr::Scalar(dict_type), ValueDescr::Scalar(str_type)});
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

TEST(GroupBy, Errors) {
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

TEST(GroupBy, NoBatches) {
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
                  /*use_threads=*/true, /*use_exec_plan=*/true));
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

TEST(GroupBy, CountOnly) {
  for (bool use_exec_plan : {false, true}) {
    for (bool use_threads : {true, false}) {
      SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

      auto table = TableFromJSON(
          schema({field("argument", float64()), field("key", int64())}), {R"([
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

      ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                           GroupByTest({table->GetColumnByName("argument")},
                                       {table->GetColumnByName("key")},
                                       {
                                           {"hash_count", nullptr},
                                       },
                                       use_threads, use_exec_plan));
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
}

TEST(GroupBy, CountScalar) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON({ValueDescr::Scalar(int32()), int64()},
                        "[[1, 1], [1, 1], [1, 2], [1, 3]]"),
      ExecBatchFromJSON({ValueDescr::Scalar(int32()), int64()},
                        "[[null, 1], [null, 1], [null, 2], [null, 3]]"),
      ExecBatchFromJSON({int32(), int64()}, "[[2, 1], [3, 2], [4, 3]]"),
  };
  input.schema = schema({field("argument", int32()), field("key", int64())});

  CountOptions skip_nulls(CountOptions::ONLY_VALID);
  CountOptions keep_nulls(CountOptions::ONLY_NULL);
  CountOptions count_all(CountOptions::ALL);
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum actual,
        GroupByUsingExecPlan(input, {"key"}, {"argument", "argument", "argument"},
                             {
                                 {"hash_count", &skip_nulls},
                                 {"hash_count", &keep_nulls},
                                 {"hash_count", &count_all},
                             },
                             use_threads, default_exec_context()));
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

TEST(GroupBy, SumOnly) {
  for (bool use_exec_plan : {false, true}) {
    for (bool use_threads : {true, false}) {
      SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");

      auto table = TableFromJSON(
          schema({field("argument", float64()), field("key", int64())}), {R"([
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

      ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                           GroupByTest({table->GetColumnByName("argument")},
                                       {table->GetColumnByName("key")},
                                       {
                                           {"hash_sum", nullptr},
                                       },
                                       use_threads, use_exec_plan));
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
}

TEST(GroupBy, SumMeanProductDecimal) {
  auto in_schema = schema({
      field("argument0", decimal128(3, 2)),
      field("argument1", decimal256(3, 2)),
      field("key", int64()),
  });

  for (bool use_exec_plan : {false, true}) {
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
                               use_threads, use_exec_plan));
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
}

TEST(GroupBy, MeanOnly) {
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

    ScalarAggregateOptions min_count(/*skip_nulls=*/true, /*min_count=*/3);
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         internal::GroupBy({table->GetColumnByName("argument"),
                                            table->GetColumnByName("argument")},
                                           {table->GetColumnByName("key")},
                                           {
                                               {"hash_mean", nullptr},
                                               {"hash_mean", &min_count},
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

TEST(GroupBy, SumMeanProductScalar) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON({ValueDescr::Scalar(int32()), int64()},
                        "[[1, 1], [1, 1], [1, 2], [1, 3]]"),
      ExecBatchFromJSON({ValueDescr::Scalar(int32()), int64()},
                        "[[null, 1], [null, 1], [null, 2], [null, 3]]"),
      ExecBatchFromJSON({int32(), int64()}, "[[2, 1], [3, 2], [4, 3]]"),
  };
  input.schema = schema({field("argument", int32()), field("key", int64())});

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum actual,
        GroupByUsingExecPlan(input, {"key"}, {"argument", "argument", "argument"},
                             {
                                 {"hash_sum", nullptr},
                                 {"hash_mean", nullptr},
                                 {"hash_product", nullptr},
                             },
                             use_threads, default_exec_context()));
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

TEST(GroupBy, VarianceAndStddev) {
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
                       internal::GroupBy(
                           {
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                           },
                           {
                               batch->GetColumnByName("key"),
                           },
                           {
                               {"hash_variance", nullptr},
                               {"hash_stddev", nullptr},
                           }));

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

  ASSERT_OK_AND_ASSIGN(aggregated_and_grouped, internal::GroupBy(
                                                   {
                                                       batch->GetColumnByName("argument"),
                                                       batch->GetColumnByName("argument"),
                                                   },
                                                   {
                                                       batch->GetColumnByName("key"),
                                                   },
                                                   {
                                                       {"hash_variance", nullptr},
                                                       {"hash_stddev", nullptr},
                                                   }));

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
  VarianceOptions variance_options(/*ddof=*/2);
  ASSERT_OK_AND_ASSIGN(aggregated_and_grouped,
                       internal::GroupBy(
                           {
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("argument"),
                           },
                           {
                               batch->GetColumnByName("key"),
                           },
                           {
                               {"hash_variance", &variance_options},
                               {"hash_stddev", &variance_options},
                           }));

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

TEST(GroupBy, VarianceAndStddevDecimal) {
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
                       internal::GroupBy(
                           {
                               batch->GetColumnByName("argument0"),
                               batch->GetColumnByName("argument0"),
                               batch->GetColumnByName("argument1"),
                               batch->GetColumnByName("argument1"),
                           },
                           {
                               batch->GetColumnByName("key"),
                           },
                           {
                               {"hash_variance", nullptr},
                               {"hash_stddev", nullptr},
                               {"hash_variance", nullptr},
                               {"hash_stddev", nullptr},
                           }));

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

TEST(GroupBy, TDigest) {
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

  TDigestOptions options1(std::vector<double>{0.5, 0.9, 0.99});
  TDigestOptions options2(std::vector<double>{0.5, 0.9, 0.99}, /*delta=*/50,
                          /*buffer_size=*/1024);
  TDigestOptions keep_nulls(/*q=*/0.5, /*delta=*/100, /*buffer_size=*/500,
                            /*skip_nulls=*/false, /*min_count=*/0);
  TDigestOptions min_count(/*q=*/0.5, /*delta=*/100, /*buffer_size=*/500,
                           /*skip_nulls=*/true, /*min_count=*/3);
  TDigestOptions keep_nulls_min_count(/*q=*/0.5, /*delta=*/100, /*buffer_size=*/500,
                                      /*skip_nulls=*/false, /*min_count=*/3);
  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       internal::GroupBy(
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
                           {
                               {"hash_tdigest", nullptr},
                               {"hash_tdigest", &options1},
                               {"hash_tdigest", &options2},
                               {"hash_tdigest", &keep_nulls},
                               {"hash_tdigest", &min_count},
                               {"hash_tdigest", &keep_nulls_min_count},
                           }));

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

TEST(GroupBy, TDigestDecimal) {
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
                       internal::GroupBy(
                           {
                               batch->GetColumnByName("argument0"),
                               batch->GetColumnByName("argument1"),
                           },
                           {batch->GetColumnByName("key")},
                           {
                               {"hash_tdigest", nullptr},
                               {"hash_tdigest", nullptr},
                           }));

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

TEST(GroupBy, ApproximateMedian) {
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

    ScalarAggregateOptions options;
    ScalarAggregateOptions keep_nulls(
        /*skip_nulls=*/false, /*min_count=*/0);
    ScalarAggregateOptions min_count(
        /*skip_nulls=*/true, /*min_count=*/3);
    ScalarAggregateOptions keep_nulls_min_count(
        /*skip_nulls=*/false, /*min_count=*/3);
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         internal::GroupBy(
                             {
                                 batch->GetColumnByName("argument"),
                                 batch->GetColumnByName("argument"),
                                 batch->GetColumnByName("argument"),
                                 batch->GetColumnByName("argument"),
                             },
                             {
                                 batch->GetColumnByName("key"),
                             },
                             {
                                 {"hash_approximate_median", &options},
                                 {"hash_approximate_median", &keep_nulls},
                                 {"hash_approximate_median", &min_count},
                                 {"hash_approximate_median", &keep_nulls_min_count},
                             }));

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

TEST(GroupBy, StddevVarianceTDigestScalar) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON(
          {ValueDescr::Scalar(int32()), ValueDescr::Scalar(float32()), int64()},
          "[[1, 1.0, 1], [1, 1.0, 1], [1, 1.0, 2], [1, 1.0, 3]]"),
      ExecBatchFromJSON(
          {ValueDescr::Scalar(int32()), ValueDescr::Scalar(float32()), int64()},
          "[[null, null, 1], [null, null, 1], [null, null, 2], [null, null, 3]]"),
      ExecBatchFromJSON({int32(), float32(), int64()},
                        "[[2, 2.0, 1], [3, 3.0, 2], [4, 4.0, 3]]"),
  };
  input.schema = schema(
      {field("argument", int32()), field("argument1", float32()), field("key", int64())});

  for (bool use_threads : {false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(Datum actual,
                         GroupByUsingExecPlan(input, {"key"},
                                              {"argument", "argument", "argument",
                                               "argument1", "argument1", "argument1"},
                                              {
                                                  {"hash_stddev", nullptr},
                                                  {"hash_variance", nullptr},
                                                  {"hash_tdigest", nullptr},
                                                  {"hash_stddev", nullptr},
                                                  {"hash_variance", nullptr},
                                                  {"hash_tdigest", nullptr},
                                              },
                                              use_threads, default_exec_context()));
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

TEST(GroupBy, VarianceOptions) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON(
          {ValueDescr::Scalar(int32()), ValueDescr::Scalar(float32()), int64()},
          "[[1, 1.0, 1], [1, 1.0, 1], [1, 1.0, 2], [1, 1.0, 2], [1, 1.0, 3]]"),
      ExecBatchFromJSON(
          {ValueDescr::Scalar(int32()), ValueDescr::Scalar(float32()), int64()},
          "[[1, 1.0, 4], [1, 1.0, 4]]"),
      ExecBatchFromJSON(
          {ValueDescr::Scalar(int32()), ValueDescr::Scalar(float32()), int64()},
          "[[null, null, 1]]"),
      ExecBatchFromJSON({int32(), float32(), int64()}, "[[2, 2.0, 1], [3, 3.0, 2]]"),
      ExecBatchFromJSON({int32(), float32(), int64()}, "[[4, 4.0, 2], [2, 2.0, 4]]"),
      ExecBatchFromJSON({int32(), float32(), int64()}, "[[null, null, 4]]"),
  };
  input.schema = schema(
      {field("argument", int32()), field("argument1", float32()), field("key", int64())});

  VarianceOptions keep_nulls(/*ddof=*/0, /*skip_nulls=*/false, /*min_count=*/0);
  VarianceOptions min_count(/*ddof=*/0, /*skip_nulls=*/true, /*min_count=*/3);
  VarianceOptions keep_nulls_min_count(/*ddof=*/0, /*skip_nulls=*/false, /*min_count=*/3);

  for (bool use_threads : {false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum actual, GroupByUsingExecPlan(input, {"key"},
                                           {
                                               "argument",
                                               "argument",
                                               "argument",
                                               "argument",
                                               "argument",
                                               "argument",
                                           },
                                           {
                                               {"hash_stddev", &keep_nulls},
                                               {"hash_stddev", &min_count},
                                               {"hash_stddev", &keep_nulls_min_count},
                                               {"hash_variance", &keep_nulls},
                                               {"hash_variance", &min_count},
                                               {"hash_variance", &keep_nulls_min_count},
                                           },
                                           use_threads, default_exec_context()));
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
        actual, GroupByUsingExecPlan(input, {"key"},
                                     {
                                         "argument1",
                                         "argument1",
                                         "argument1",
                                         "argument1",
                                         "argument1",
                                         "argument1",
                                     },
                                     {
                                         {"hash_stddev", &keep_nulls},
                                         {"hash_stddev", &min_count},
                                         {"hash_stddev", &keep_nulls_min_count},
                                         {"hash_variance", &keep_nulls},
                                         {"hash_variance", &min_count},
                                         {"hash_variance", &keep_nulls_min_count},
                                     },
                                     use_threads, default_exec_context()));
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

TEST(GroupBy, MinMaxOnly) {
  auto in_schema = schema({
      field("argument", float64()),
      field("argument1", null()),
      field("argument2", boolean()),
      field("key", int64()),
  });
  for (bool use_exec_plan : {false, true}) {
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
                               use_threads, use_exec_plan));
      ValidateOutput(aggregated_and_grouped);
      SortBy({"key_0"}, &aggregated_and_grouped);

      AssertDatumsEqual(
          ArrayFromJSON(struct_({
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
}

TEST(GroupBy, MinMaxTypes) {
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
                    /*use_threads=*/true, /*use_exec_plan=*/true));
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

TEST(GroupBy, MinMaxDecimal) {
  auto in_schema = schema({
      field("argument0", decimal128(3, 2)),
      field("argument1", decimal256(3, 2)),
      field("key", int64()),
  });
  for (bool use_exec_plan : {false, true}) {
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
                               use_threads, use_exec_plan));
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
}

TEST(GroupBy, MinMaxBinary) {
  for (bool use_exec_plan : {false, true}) {
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

        ASSERT_OK_AND_ASSIGN(
            Datum aggregated_and_grouped,
            GroupByTest({table->GetColumnByName("argument0")},
                        {table->GetColumnByName("key")}, {{"hash_min_max", nullptr}},
                        use_threads, use_exec_plan));
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
}

TEST(GroupBy, MinMaxFixedSizeBinary) {
  const auto ty = fixed_size_binary(3);
  for (bool use_exec_plan : {false, true}) {
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

      ASSERT_OK_AND_ASSIGN(
          Datum aggregated_and_grouped,
          GroupByTest({table->GetColumnByName("argument0")},
                      {table->GetColumnByName("key")}, {{"hash_min_max", nullptr}},
                      use_threads, use_exec_plan));
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
}

TEST(GroupBy, MinOrMax) {
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
                                   /*use_threads=*/true, /*use_exec_plan=*/true));
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

TEST(GroupBy, MinMaxScalar) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON({ValueDescr::Scalar(int32()), int64()},
                        "[[-1, 1], [-1, 1], [-1, 2], [-1, 3]]"),
      ExecBatchFromJSON({ValueDescr::Scalar(int32()), int64()},
                        "[[null, 1], [null, 1], [null, 2], [null, 3]]"),
      ExecBatchFromJSON({int32(), int64()}, "[[2, 1], [3, 2], [4, 3]]"),
  };
  input.schema = schema({field("argument", int32()), field("key", int64())});

  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum actual,
        GroupByUsingExecPlan(input, {"key"}, {"argument", "argument", "argument"},
                             {{"hash_min_max", nullptr}}, use_threads,
                             default_exec_context()));
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

TEST(GroupBy, AnyAndAll) {
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

    ScalarAggregateOptions no_min(/*skip_nulls=*/true, /*min_count=*/0);
    ScalarAggregateOptions min_count(/*skip_nulls=*/true, /*min_count=*/3);
    ScalarAggregateOptions keep_nulls(/*skip_nulls=*/false, /*min_count=*/0);
    ScalarAggregateOptions keep_nulls_min_count(/*skip_nulls=*/false, /*min_count=*/3);
    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         internal::GroupBy(
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
                             {table->GetColumnByName("key")},
                             {
                                 {"hash_any", &no_min},
                                 {"hash_any", &min_count},
                                 {"hash_any", &keep_nulls},
                                 {"hash_any", &keep_nulls_min_count},
                                 {"hash_all", &no_min},
                                 {"hash_all", &min_count},
                                 {"hash_all", &keep_nulls},
                                 {"hash_all", &keep_nulls_min_count},
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

TEST(GroupBy, AnyAllScalar) {
  BatchesWithSchema input;
  input.batches = {
      ExecBatchFromJSON({ValueDescr::Scalar(boolean()), int64()},
                        "[[true, 1], [true, 1], [true, 2], [true, 3]]"),
      ExecBatchFromJSON({ValueDescr::Scalar(boolean()), int64()},
                        "[[null, 1], [null, 1], [null, 2], [null, 3]]"),
      ExecBatchFromJSON({boolean(), int64()}, "[[true, 1], [false, 2], [null, 3]]"),
  };
  input.schema = schema({field("argument", boolean()), field("key", int64())});

  ScalarAggregateOptions keep_nulls(/*skip_nulls=*/false, /*min_count=*/0);
  for (bool use_threads : {true, false}) {
    SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
    ASSERT_OK_AND_ASSIGN(
        Datum actual,
        GroupByUsingExecPlan(input, {"key"},
                             {"argument", "argument", "argument", "argument"},
                             {
                                 {"hash_any", nullptr},
                                 {"hash_all", nullptr},
                                 {"hash_any", &keep_nulls},
                                 {"hash_all", &keep_nulls},
                             },
                             use_threads, default_exec_context()));
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

TEST(GroupBy, CountDistinct) {
  CountOptions all(CountOptions::ALL);
  CountOptions only_valid(CountOptions::ONLY_VALID);
  CountOptions only_null(CountOptions::ONLY_NULL);
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

    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         internal::GroupBy(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {
                                 table->GetColumnByName("key"),
                             },
                             {
                                 {"hash_count_distinct", &all},
                                 {"hash_count_distinct", &only_valid},
                                 {"hash_count_distinct", &only_null},
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

    ASSERT_OK_AND_ASSIGN(aggregated_and_grouped,
                         internal::GroupBy(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {
                                 table->GetColumnByName("key"),
                             },
                             {
                                 {"hash_count_distinct", &all},
                                 {"hash_count_distinct", &only_valid},
                                 {"hash_count_distinct", &only_null},
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

    ASSERT_OK_AND_ASSIGN(aggregated_and_grouped,
                         internal::GroupBy(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {
                                 table->GetColumnByName("key"),
                             },
                             {
                                 {"hash_count_distinct", &all},
                                 {"hash_count_distinct", &only_valid},
                                 {"hash_count_distinct", &only_null},
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

TEST(GroupBy, Distinct) {
  CountOptions all(CountOptions::ALL);
  CountOptions only_valid(CountOptions::ONLY_VALID);
  CountOptions only_null(CountOptions::ONLY_NULL);
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
                         internal::GroupBy(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {
                                 table->GetColumnByName("key"),
                             },
                             {
                                 {"hash_distinct", &all},
                                 {"hash_distinct", &only_valid},
                                 {"hash_distinct", &only_null},
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
                         internal::GroupBy(
                             {
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                                 table->GetColumnByName("argument"),
                             },
                             {
                                 table->GetColumnByName("key"),
                             },
                             {
                                 {"hash_distinct", &all},
                                 {"hash_distinct", &only_valid},
                                 {"hash_distinct", &only_null},
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

TEST(GroupBy, CountAndSum) {
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

  CountOptions count_options;
  CountOptions count_nulls(CountOptions::ONLY_NULL);
  CountOptions count_all(CountOptions::ALL);
  ScalarAggregateOptions min_count(/*skip_nulls=*/true, /*min_count=*/3);
  ASSERT_OK_AND_ASSIGN(
      Datum aggregated_and_grouped,
      internal::GroupBy(
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
          {
              {"hash_count", &count_options},
              {"hash_count", &count_nulls},
              {"hash_count", &count_all},
              {"hash_sum", nullptr},
              {"hash_sum", &min_count},
              {"hash_sum", nullptr},
          }));

  AssertDatumsEqual(
      ArrayFromJSON(struct_({
                        field("hash_count", int64()),
                        field("hash_count", int64()),
                        field("hash_count", int64()),
                        // NB: summing a float32 array results in float64 sums
                        field("hash_sum", float64()),
                        field("hash_sum", float64()),
                        field("hash_sum", int64()),
                        field("key_0", int64()),
                    }),
                    R"([
    [2, 1, 3, 4.25,   null,   3,    1],
    [3, 0, 3, -0.125, -0.125, 6,    2],
    [0, 2, 2, null,   null,   6,    3],
    [2, 0, 2, 4.75,   null,   null, null]
  ])"),
      aggregated_and_grouped,
      /*verbose=*/true);
}

TEST(GroupBy, Product) {
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

  ScalarAggregateOptions min_count(/*skip_nulls=*/true, /*min_count=*/3);
  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       internal::GroupBy(
                           {
                               batch->GetColumnByName("argument"),
                               batch->GetColumnByName("key"),
                               batch->GetColumnByName("argument"),
                           },
                           {
                               batch->GetColumnByName("key"),
                           },
                           {
                               {"hash_product", nullptr},
                               {"hash_product", nullptr},
                               {"hash_product", &min_count},
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

  ASSERT_OK_AND_ASSIGN(aggregated_and_grouped, internal::GroupBy(
                                                   {
                                                       batch->GetColumnByName("argument"),
                                                   },
                                                   {
                                                       batch->GetColumnByName("key"),
                                                   },
                                                   {
                                                       {"hash_product", nullptr},
                                                   }));

  AssertDatumsApproxEqual(ArrayFromJSON(struct_({
                                            field("hash_product", int64()),
                                            field("key_0", int64()),
                                        }),
                                        R"([[8589934592, 1]])"),
                          aggregated_and_grouped,
                          /*verbose=*/true);
}

TEST(GroupBy, SumMeanProductKeepNulls) {
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

  ScalarAggregateOptions keep_nulls(/*skip_nulls=*/false);
  ScalarAggregateOptions min_count(/*skip_nulls=*/false, /*min_count=*/3);
  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       internal::GroupBy(
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
                           {
                               {"hash_sum", &keep_nulls},
                               {"hash_sum", &min_count},
                               {"hash_mean", &keep_nulls},
                               {"hash_mean", &min_count},
                               {"hash_product", &keep_nulls},
                               {"hash_product", &min_count},
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

TEST(GroupBy, SumOnlyStringAndDictKeys) {
  for (auto key_type : {utf8(), dictionary(int32(), utf8())}) {
    SCOPED_TRACE("key type: " + key_type->ToString());

    auto batch = RecordBatchFromJSON(
        schema({field("argument", float64()), field("key", key_type)}), R"([
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

    ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                         internal::GroupBy({batch->GetColumnByName("argument")},
                                           {batch->GetColumnByName("key")},
                                           {
                                               {"hash_sum", nullptr},
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

TEST(GroupBy, ConcreteCaseWithValidateGroupBy) {
  auto batch = RecordBatchFromJSON(
      schema({field("argument", float64()), field("key", utf8())}), R"([
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

  ScalarAggregateOptions keepna{false, 1};
  CountOptions nulls(CountOptions::ONLY_NULL);
  CountOptions non_null(CountOptions::ONLY_VALID);

  using internal::Aggregate;
  for (auto agg : {
           Aggregate{"hash_sum", nullptr},
           Aggregate{"hash_count", &non_null},
           Aggregate{"hash_count", &nulls},
           Aggregate{"hash_min_max", nullptr},
           Aggregate{"hash_min_max", &keepna},
       }) {
    SCOPED_TRACE(agg.function);
    ValidateGroupBy({agg}, {batch->GetColumnByName("argument")},
                    {batch->GetColumnByName("key")});
  }
}

// Count nulls/non_nulls from record batch with no nulls
TEST(GroupBy, CountNull) {
  auto batch = RecordBatchFromJSON(
      schema({field("argument", float64()), field("key", utf8())}), R"([
    [1.0, "alfa"],
    [2.0, "beta"],
    [3.0, "gama"]
  ])");

  CountOptions keepna{CountOptions::ONLY_NULL}, skipna{CountOptions::ONLY_VALID};

  using internal::Aggregate;
  for (auto agg : {
           Aggregate{"hash_count", &keepna},
           Aggregate{"hash_count", &skipna},
       }) {
    SCOPED_TRACE(agg.function);
    ValidateGroupBy({agg}, {batch->GetColumnByName("argument")},
                    {batch->GetColumnByName("key")});
  }
}

TEST(GroupBy, RandomArraySum) {
  ScalarAggregateOptions options(/*skip_nulls=*/true, /*min_count=*/0);
  for (int64_t length : {1 << 10, 1 << 12, 1 << 15}) {
    for (auto null_probability : {0.0, 0.01, 0.5, 1.0}) {
      auto batch = random::GenerateBatch(
          {
              field("argument", float32(),
                    key_value_metadata(
                        {{"null_probability", std::to_string(null_probability)}})),
              field("key", int64(), key_value_metadata({{"min", "0"}, {"max", "100"}})),
          },
          length, 0xDEADBEEF);

      ValidateGroupBy(
          {
              {"hash_sum", &options},
          },
          {batch->GetColumnByName("argument")}, {batch->GetColumnByName("key")});
    }
  }
}

TEST(GroupBy, WithChunkedArray) {
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
                       internal::GroupBy(
                           {
                               table->GetColumnByName("argument"),
                               table->GetColumnByName("argument"),
                               table->GetColumnByName("argument"),
                           },
                           {
                               table->GetColumnByName("key"),
                           },
                           {
                               {"hash_count", nullptr},
                               {"hash_sum", nullptr},
                               {"hash_min_max", nullptr},
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

TEST(GroupBy, MinMaxWithNewGroupsInChunkedArray) {
  auto table = TableFromJSON(
      schema({field("argument", int64()), field("key", int64())}),
      {R"([{"argument": 1, "key": 0}])", R"([{"argument": 0,   "key": 1}])"});
  ScalarAggregateOptions count_options;
  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       internal::GroupBy(
                           {
                               table->GetColumnByName("argument"),
                           },
                           {
                               table->GetColumnByName("key"),
                           },
                           {
                               {"hash_min_max", nullptr},
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

TEST(GroupBy, SmallChunkSizeSumOnly) {
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
                       internal::GroupBy({batch->GetColumnByName("argument")},
                                         {batch->GetColumnByName("key")},
                                         {
                                             {"hash_sum", nullptr},
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

TEST(GroupBy, CountWithNullType) {
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

  CountOptions all(CountOptions::ALL);
  CountOptions only_valid(CountOptions::ONLY_VALID);
  CountOptions only_null(CountOptions::ONLY_NULL);

  for (bool use_exec_plan : {false, true}) {
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
                                   {"hash_count", &all},
                                   {"hash_count", &only_valid},
                                   {"hash_count", &only_null},
                               },
                               use_threads, use_exec_plan));
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
}

TEST(GroupBy, CountWithNullTypeEmptyTable) {
  auto table = TableFromJSON(schema({field("argument", null()), field("key", int64())}),
                             {R"([])"});

  CountOptions all(CountOptions::ALL);
  CountOptions only_valid(CountOptions::ONLY_VALID);
  CountOptions only_null(CountOptions::ONLY_NULL);

  for (bool use_exec_plan : {false, true}) {
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
                                   {"hash_count", &all},
                                   {"hash_count", &only_valid},
                                   {"hash_count", &only_null},
                               },
                               use_threads, use_exec_plan));
      auto struct_arr = aggregated_and_grouped.array_as<StructArray>();
      for (auto& field : struct_arr->fields()) {
        AssertDatumsEqual(ArrayFromJSON(int64(), "[]"), field, /*verbose=*/true);
      }
    }
  }
}

TEST(GroupBy, SingleNullTypeKey) {
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

  for (bool use_exec_plan : {false, true}) {
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
                               use_threads, use_exec_plan));
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
}

TEST(GroupBy, MultipleKeysIncludesNullType) {
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

  for (bool use_exec_plan : {false, true}) {
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
              use_threads, use_exec_plan));
      SortBy({"key_0"}, &aggregated_and_grouped);

      AssertDatumsEqual(
          ArrayFromJSON(struct_({
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
}

TEST(GroupBy, SumNullType) {
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

  ScalarAggregateOptions no_min(/*skip_nulls=*/true, /*min_count=*/0);
  ScalarAggregateOptions min_count(/*skip_nulls=*/true, /*min_count=*/3);
  ScalarAggregateOptions keep_nulls(/*skip_nulls=*/false, /*min_count=*/0);
  ScalarAggregateOptions keep_nulls_min_count(/*skip_nulls=*/false, /*min_count=*/3);

  for (bool use_exec_plan : {false, true}) {
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
                                   {"hash_sum", &no_min},
                                   {"hash_sum", &keep_nulls},
                                   {"hash_sum", &min_count},
                                   {"hash_sum", &keep_nulls_min_count},
                               },
                               use_threads, use_exec_plan));
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
}

TEST(GroupBy, ProductNullType) {
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

  ScalarAggregateOptions no_min(/*skip_nulls=*/true, /*min_count=*/0);
  ScalarAggregateOptions min_count(/*skip_nulls=*/true, /*min_count=*/3);
  ScalarAggregateOptions keep_nulls(/*skip_nulls=*/false, /*min_count=*/0);
  ScalarAggregateOptions keep_nulls_min_count(/*skip_nulls=*/false, /*min_count=*/3);

  for (bool use_exec_plan : {false, true}) {
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
                                   {"hash_product", &no_min},
                                   {"hash_product", &keep_nulls},
                                   {"hash_product", &min_count},
                                   {"hash_product", &keep_nulls_min_count},
                               },
                               use_threads, use_exec_plan));
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
}

TEST(GroupBy, MeanNullType) {
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

  ScalarAggregateOptions no_min(/*skip_nulls=*/true, /*min_count=*/0);
  ScalarAggregateOptions min_count(/*skip_nulls=*/true, /*min_count=*/3);
  ScalarAggregateOptions keep_nulls(/*skip_nulls=*/false, /*min_count=*/0);
  ScalarAggregateOptions keep_nulls_min_count(/*skip_nulls=*/false, /*min_count=*/3);

  for (bool use_exec_plan : {false, true}) {
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
                                   {"hash_mean", &no_min},
                                   {"hash_mean", &keep_nulls},
                                   {"hash_mean", &min_count},
                                   {"hash_mean", &keep_nulls_min_count},
                               },
                               use_threads, use_exec_plan));
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
}

TEST(GroupBy, NullTypeEmptyTable) {
  auto table = TableFromJSON(schema({field("argument", null()), field("key", int64())}),
                             {R"([])"});

  ScalarAggregateOptions no_min(/*skip_nulls=*/true, /*min_count=*/0);
  ScalarAggregateOptions min_count(/*skip_nulls=*/true, /*min_count=*/3);
  ScalarAggregateOptions keep_nulls(/*skip_nulls=*/false, /*min_count=*/0);
  ScalarAggregateOptions keep_nulls_min_count(/*skip_nulls=*/false, /*min_count=*/3);

  for (bool use_exec_plan : {false, true}) {
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
                                   {"hash_sum", &no_min},
                                   {"hash_product", &min_count},
                                   {"hash_mean", &keep_nulls},
                               },
                               use_threads, use_exec_plan));
      auto struct_arr = aggregated_and_grouped.array_as<StructArray>();
      AssertDatumsEqual(ArrayFromJSON(int64(), "[]"), struct_arr->field(0),
                        /*verbose=*/true);
      AssertDatumsEqual(ArrayFromJSON(int64(), "[]"), struct_arr->field(1),
                        /*verbose=*/true);
      AssertDatumsEqual(ArrayFromJSON(float64(), "[]"), struct_arr->field(2),
                        /*verbose=*/true);
    }
  }
}

TEST(GroupBy, OnlyKeys) {
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

  for (bool use_exec_plan : {false, true}) {
    for (bool use_threads : {true, false}) {
      SCOPED_TRACE(use_threads ? "parallel/merged" : "serial");
      ASSERT_OK_AND_ASSIGN(
          Datum aggregated_and_grouped,
          GroupByTest({},
                      {table->GetColumnByName("key_0"), table->GetColumnByName("key_1")},
                      {}, use_threads, use_exec_plan));
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
}
}  // namespace compute
}  // namespace arrow
