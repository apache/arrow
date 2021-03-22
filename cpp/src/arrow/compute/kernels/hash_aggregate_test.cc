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

#include <algorithm>
#include <limits>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/compute/registry.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_internal.h"

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::BitmapReader;
using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {

// Copy-pasta from partition.cc
//
// In the finished product this will only be a test helper for group_by
// and partition.cc will rely on a no-aggregate call to group_by.
namespace group_helpers {
namespace {

struct ScalarVectorToArray {
  template <typename T, typename AppendScalar,
            typename BuilderType = typename TypeTraits<T>::BuilderType,
            typename ScalarType = typename TypeTraits<T>::ScalarType>
  Status UseBuilder(const AppendScalar& append) {
    BuilderType builder(type(), default_memory_pool());
    for (const auto& s : scalars_) {
      if (s->is_valid) {
        RETURN_NOT_OK(append(checked_cast<const ScalarType&>(*s), &builder));
      } else {
        RETURN_NOT_OK(builder.AppendNull());
      }
    }
    return builder.FinishInternal(&data_);
  }

  struct AppendValue {
    template <typename BuilderType, typename ScalarType>
    Status operator()(const ScalarType& s, BuilderType* builder) const {
      return builder->Append(s.value);
    }
  };

  struct AppendBuffer {
    template <typename BuilderType, typename ScalarType>
    Status operator()(const ScalarType& s, BuilderType* builder) const {
      const Buffer& buffer = *s.value;
      return builder->Append(util::string_view{buffer});
    }
  };

  template <typename T>
  enable_if_primitive_ctype<T, Status> Visit(const T&) {
    return UseBuilder<T>(AppendValue{});
  }

  template <typename T>
  enable_if_has_string_view<T, Status> Visit(const T&) {
    return UseBuilder<T>(AppendBuffer{});
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("ScalarVectorToArray for type ", type);
  }

  Result<Datum> Convert(ScalarVector scalars) && {
    if (scalars.size() == 0) {
      return Status::NotImplemented("ScalarVectorToArray with no scalars");
    }
    scalars_ = std::move(scalars);
    RETURN_NOT_OK(VisitTypeInline(*type(), this));
    return Datum(std::move(data_));
  }

  const std::shared_ptr<DataType>& type() { return scalars_[0]->type; }

  ScalarVector scalars_;
  std::shared_ptr<ArrayData> data_;
};

Result<Datum> NaiveGroupBy(std::vector<Datum> arguments, std::vector<Datum> keys,
                           const std::vector<internal::Aggregate>& aggregates) {
  ARROW_ASSIGN_OR_RAISE(auto key_batch, ExecBatch::Make(std::move(keys)));

  ARROW_ASSIGN_OR_RAISE(auto grouper,
                        internal::Grouper::Make(key_batch.GetDescriptors()));

  ARROW_ASSIGN_OR_RAISE(Datum id_batch, grouper->Consume(key_batch));

  ARROW_ASSIGN_OR_RAISE(
      auto groupings, internal::Grouper::MakeGroupings(*id_batch.array_as<UInt32Array>(),
                                                       grouper->num_groups() - 1));

  ArrayVector out_columns;

  for (size_t i = 0; i < arguments.size(); ++i) {
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
                          ScalarVectorToArray{}.Convert(std::move(aggregated_scalars)));
    out_columns.push_back(aggregated_column.make_array());
  }

  ARROW_ASSIGN_OR_RAISE(auto uniques, grouper->GetUniques());
  for (const Datum& key : uniques.values) {
    out_columns.push_back(key.make_array());
  }

  std::vector<std::string> out_names(out_columns.size(), "");
  return StructArray::Make(std::move(out_columns), std::move(out_names));
}

void ValidateGroupBy(const std::vector<internal::Aggregate>& aggregates,
                     std::vector<Datum> arguments, std::vector<Datum> keys) {
  ASSERT_OK_AND_ASSIGN(Datum expected,
                       group_helpers::NaiveGroupBy(arguments, keys, aggregates));

  ASSERT_OK_AND_ASSIGN(Datum actual, GroupBy(arguments, keys, aggregates));

  AssertDatumsEqual(expected, actual, /*verbose=*/true);
}

}  // namespace
}  // namespace group_helpers

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
    ExpectConsume(ExecBatch(*RecordBatchFromJSON(key_schema_, key_json)),
                  ArrayFromJSON(uint32(), expected));
  }

  void ExpectConsume(const std::vector<Datum>& key_batch, Datum expected) {
    ExpectConsume(*ExecBatch::Make(key_batch), expected);
  }

  void ExpectConsume(const ExecBatch& key_batch, Datum expected) {
    Datum ids;
    ConsumeAndValidate(key_batch, &ids);
    AssertDatumsEqual(expected, ids, /*verbose=*/true);
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
        auto prefix = new_uniques[i].array()->Slice(0, uniques_.length);
        AssertDatumsEqual(uniques_[i], prefix, /*verbose=*/true);
      }

      uniques_ = std::move(new_uniques);
    }

    // check that the ids encode an equivalent key sequence
    for (int i = 0; i < key_batch.num_values(); ++i) {
      SCOPED_TRACE(std::to_string(i) + "th key array");
      ASSERT_OK_AND_ASSIGN(auto expected, Take(uniques_[i], id_batch));
      AssertDatumsEqual(expected, key_batch[i], /*verbose=*/true);
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
  for (auto ty : internal::NumericTypes()) {
    SCOPED_TRACE("key type: " + ty->ToString());

    TestGrouper g({ty});

    g.ExpectConsume("[[3], [3]]", "[0, 0]");

    g.ExpectConsume("[[3], [3]]", "[0, 0]");

    g.ExpectConsume("[[27], [81]]", "[1, 2]");

    g.ExpectConsume("[[3], [27], [3], [27], [null], [81], [27], [81]]",
                    "[0, 1, 0, 1, 3, 2, 1, 2]");
  }
}

TEST(Grouper, StringKey) {
  for (auto ty : {utf8(), large_utf8()}) {
    SCOPED_TRACE("key type: " + ty->ToString());

    TestGrouper g({ty});

    g.ExpectConsume(R"([["eh"], ["eh"]])", "[0, 0]");

    g.ExpectConsume(R"([["eh"], ["eh"]])", "[0, 0]");

    g.ExpectConsume(R"([["bee"], [null]])", "[1, 2]");
  }
}

TEST(Grouper, DictKey) {
  TestGrouper g({dictionary(int32(), utf8())});

  // unification of dictionaries on encode is not yet supported
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

  ASSERT_RAISES(NotImplemented,
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

TEST(Grouper, MakeGroupings) {
  auto ExpectGroupings = [](std::string ids_json, uint32_t max_id,
                            std::string expected_json) {
    auto ids = checked_pointer_cast<UInt32Array>(ArrayFromJSON(uint32(), ids_json));
    auto expected = ArrayFromJSON(list(int32()), expected_json);

    ASSERT_OK_AND_ASSIGN(auto actual, internal::Grouper::MakeGroupings(*ids, max_id));
    AssertArraysEqual(*expected, *actual, /*verbose=*/true);
  };

  ExpectGroupings("[]", 0, "[[]]");

  ExpectGroupings("[0, 0, 0]", 0, "[[0, 1, 2]]");

  ExpectGroupings("[0, 0, 0, 1, 1, 2]", 3, "[[0, 1, 2], [3, 4], [5], []]");

  ExpectGroupings("[2, 1, 2, 1, 1, 2]", 4, "[[], [1, 3, 4], [0, 2, 5], [], []]");

  ExpectGroupings("[2, 2, 5, 5, 2, 3]", 7,
                  "[[], [], [0, 1, 4], [5], [], [2, 3], [], []]");
}

TEST(GroupBy, SumOnlyBooleanKey) {
  auto argument = ArrayFromJSON(float64(), "[1.0, 0.0, null, 3.25, 0.125, -0.25, 0.75]");
  auto key = ArrayFromJSON(boolean(), "[1, 0, 1, 0, null, 0, null]");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       internal::GroupBy({argument}, {key},
                                         {
                                             {"hash_sum", nullptr},
                                         }));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", float64()),
                                      field("", boolean()),
                                  }),
                                  R"([
    [1,     true],
    [3,    false],
    [0.875, null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, SumOnly8bitKey) {
  auto argument = ArrayFromJSON(float64(), "[1.0, 0.0, null, 3.25, 0.125, -0.25, 0.75]");
  auto key = ArrayFromJSON(int8(), "[1, 2, 3, 1, 2, 2, null]");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       internal::GroupBy({argument}, {key},
                                         {
                                             {"hash_sum", nullptr},
                                         }));

  ASSERT_OK(aggregated_and_grouped.array_as<StructArray>()->ValidateFull());

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", float64()),
                                      field("", int8()),
                                  }),
                                  R"([
    [4.25,   1],
    [-0.125, 2],
    [null,   3],
    [0.75,   null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, SumOnly32bitKey) {
  auto argument = ArrayFromJSON(float64(), "[1.0, 0.0, null, 3.25, 0.125, -0.25, 0.75]");
  auto key = ArrayFromJSON(int32(), "[1, 2, 3, 1, 2, 2, null]");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       internal::GroupBy({argument}, {key},
                                         {
                                             {"hash_sum", nullptr},
                                         }));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", float64()),
                                      field("", int32()),
                                  }),
                                  R"([
    [4.25,   1],
    [-0.125, 2],
    [null,   3],
    [0.75,   null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, SumOnly) {
  auto argument =
      ArrayFromJSON(float64(), "[1.0, 0.0, null, 4.0, 3.25, 0.125, -0.25, 0.75]");
  auto key = ArrayFromJSON(int64(), "[1, 2, 3, null, 1, 2, 2, null]");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       internal::GroupBy({argument}, {key},
                                         {
                                             {"hash_sum", nullptr},
                                         }));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", float64()),
                                      field("", int64()),
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

TEST(GroupBy, SumOnlyFloatingPointKey) {
  auto argument = ArrayFromJSON(float64(), "[1.0, 0.0, null, 3.25, 0.125, -0.25, 0.75]");
  auto key = ArrayFromJSON(float64(), "[1, 2, 3, 1, 2, 2, null]");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       internal::GroupBy({argument}, {key},
                                         {
                                             {"hash_sum", nullptr},
                                         }));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", float64()),
                                      field("", float64()),
                                  }),
                                  R"([
    [4.25,   1],
    [-0.125, 2],
    [null,   3],
    [0.75,   null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, MinMaxOnly) {
  auto argument = ArrayFromJSON(float64(), "[1.0, 0.0, null, 3.25, 0.125, -0.25, 0.75]");
  auto key = ArrayFromJSON(int64(), "[1, 2, 3, 1, 2, 2, null]");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       internal::GroupBy({argument}, {key},
                                         {
                                             {"hash_min_max", nullptr},
                                         }));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", struct_({
                                                    field("min", float64()),
                                                    field("max", float64()),
                                                })),
                                      field("", int64()),
                                  }),
                                  R"([
    [{"min": 1.0,   "max": 3.25},  1],
    [{"min": -0.25, "max": 0.125}, 2],
    [{"min": null,  "max": null},  3],
    [{"min": 0.75,  "max": 0.75},  null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, CountAndSum) {
  auto argument = ArrayFromJSON(float32(), "[1.0, 0.0, null, 3.25, 0.125, -0.25, 0.75]");
  auto key = ArrayFromJSON(int64(), "[1, 2, 1, 3, 2, 3, null]");

  CountOptions count_options;

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       // NB: passing an argument twice or also using it as a key is legal
                       internal::GroupBy({argument, argument, key}, {key},
                                         {
                                             {"hash_count", &count_options},
                                             {"hash_sum", nullptr},
                                             {"hash_sum", nullptr},
                                         }));

  AssertDatumsEqual(
      ArrayFromJSON(struct_({
                        field("", int64()),
                        // NB: summing a float32 array results in float64 sums
                        field("", float64()),
                        field("", int64()),
                        field("", int64()),
                    }),
                    R"([
    [1, 1.0,   2,    1],
    [2, 0.125, 4,    2],
    [2, 3.0,   6,    3],
    [1, 0.75,  null, null]
  ])"),
      aggregated_and_grouped,
      /*verbose=*/true);
}

TEST(GroupBy, StringKey) {
  auto argument = ArrayFromJSON(int64(), "[10, 5, 4, 2, 12, 9]");
  auto key = ArrayFromJSON(utf8(), R"(["alfa", "beta", "gamma", "gamma", null, "beta"])");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       internal::GroupBy({argument}, {key}, {{"hash_sum", nullptr}}));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", int64()),
                                      field("", utf8()),
                                  }),
                                  R"([
    [10,   "alfa"],
    [14,   "beta"],
    [6,    "gamma"],
    [12,   null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, DictKey) {
  auto argument = ArrayFromJSON(int64(), "[10, 5, 4, 2, 12, 9]");
  auto key = ArrayFromJSON(dictionary(int32(), utf8()),
                           R"(["alfa", "beta", "gamma", "gamma", null, "beta"])");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       internal::GroupBy({argument}, {key}, {{"hash_sum", nullptr}}));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", int64()),
                                      field("", dictionary(int32(), utf8())),
                                  }),
                                  R"([
    [10,   "alfa"],
    [14,   "beta"],
    [6,    "gamma"],
    [12,   null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, MultipleKeys) {
  auto argument = ArrayFromJSON(float32(), "[0.125, 0.5, -0.75, 8, 1.0, 2.0]");
  auto int_key = ArrayFromJSON(int32(), "[0, 1, 0, 1, 0, 1]");
  auto str_key =
      ArrayFromJSON(utf8(), R"(["beta", "beta", "gamma", "gamma", null, "beta"])");

  ASSERT_OK_AND_ASSIGN(
      Datum aggregated_and_grouped,
      internal::GroupBy({argument}, {int_key, str_key}, {{"hash_sum", nullptr}}));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", float64()),
                                      field("", int32()),
                                      field("", utf8()),
                                  }),
                                  R"([
    [0.125, 0, "beta"],
    [2.5,   1, "beta"],
    [-0.75, 0, "gamma"],
    [8,     1, "gamma"],
    [1.0,   0, null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, ConcreteCaseWithValidateGroupBy) {
  auto argument = ArrayFromJSON(int64(), "[10, 5, 4, 2, 12]");
  auto key = ArrayFromJSON(utf8(), R"(["alfa", "beta", "gamma", "gamma", "beta"])");

  group_helpers::ValidateGroupBy({{"hash_sum", nullptr}}, {argument}, {key});
}

TEST(GroupBy, RandomArraySum) {
  auto rand = random::RandomArrayGenerator(0xdeadbeef);

  for (int64_t length : {1 << 10, 1 << 12, 1 << 15}) {
    for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
      auto summand = rand.Float32(length, -100, 100, null_probability);
      auto key = rand.Int64(length, 0, 12);

      group_helpers::ValidateGroupBy(
          {
              {"hash_sum", nullptr},
          },
          {summand}, {key});
    }
  }
}

}  // namespace compute
}  // namespace arrow
