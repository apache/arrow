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

#include <gmock/gmock-matchers.h>

#include <unordered_set>

#include "arrow/api.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/thread_pool.h"

using testing::UnorderedElementsAreArray;

namespace arrow {
namespace compute {

BatchesWithSchema GenerateBatchesFromString(
    const std::shared_ptr<Schema>& schema,
    const std::vector<util::string_view>& json_strings, int multiplicity = 1) {
  BatchesWithSchema out_batches{{}, schema};

  std::vector<ValueDescr> descrs;
  for (auto&& field : schema->fields()) {
    descrs.emplace_back(field->type());
  }

  for (auto&& s : json_strings) {
    out_batches.batches.push_back(ExecBatchFromJSON(descrs, s));
  }

  size_t batch_count = out_batches.batches.size();
  for (int repeat = 1; repeat < multiplicity; ++repeat) {
    for (size_t i = 0; i < batch_count; ++i) {
      out_batches.batches.push_back(out_batches.batches[i]);
    }
  }

  return out_batches;
}

void CheckRunOutput(JoinType type, const BatchesWithSchema& l_batches,
                    const BatchesWithSchema& r_batches,
                    const std::vector<FieldRef>& left_keys,
                    const std::vector<FieldRef>& right_keys,
                    const BatchesWithSchema& exp_batches, bool parallel = false) {
  auto exec_ctx = arrow::internal::make_unique<ExecContext>(
      default_memory_pool(), parallel ? arrow::internal::GetCpuThreadPool() : nullptr);

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(exec_ctx.get()));

  JoinNodeOptions join_options{type, left_keys, right_keys};
  Declaration join{"hash_join", join_options};

  // add left source
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(parallel,
                                                                  /*slow=*/false)}});
  // add right source
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r_batches.schema, r_batches.gen(parallel,
                                                                  /*slow=*/false)}});
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  ASSERT_OK(Declaration::Sequence({join, {"sink", SinkNodeOptions{&sink_gen}}})
                .AddToPlan(plan.get()));

  ASSERT_THAT(StartAndCollect(plan.get(), sink_gen),
              Finishes(ResultWith(UnorderedElementsAreArray(exp_batches.batches))));
}

void RunNonEmptyTest(JoinType type, bool parallel) {
  auto l_schema = schema({field("l_i32", int32()), field("l_str", utf8())});
  auto r_schema = schema({field("r_str", utf8()), field("r_i32", int32())});
  BatchesWithSchema l_batches, r_batches, exp_batches;

  int multiplicity = parallel ? 100 : 1;

  l_batches = GenerateBatchesFromString(
      l_schema,
      {R"([[0,"d"], [1,"b"]])", R"([[2,"d"], [3,"a"], [4,"a"]])",
       R"([[5,"b"], [6,"c"], [7,"e"], [8,"e"]])"},
      multiplicity);

  r_batches = GenerateBatchesFromString(
      r_schema,
      {R"([["f", 0], ["b", 1], ["b", 2]])", R"([["c", 3], ["g", 4]])", R"([["e", 5]])"},
      multiplicity);

  switch (type) {
    case LEFT_SEMI:
      exp_batches = GenerateBatchesFromString(
          l_schema, {R"([[1,"b"]])", R"([])", R"([[5,"b"], [6,"c"], [7,"e"], [8,"e"]])"},
          multiplicity);
      break;
    case RIGHT_SEMI:
      exp_batches = GenerateBatchesFromString(
          r_schema, {R"([["b", 1], ["b", 2]])", R"([["c", 3]])", R"([["e", 5]])"},
          multiplicity);
      break;
    case LEFT_ANTI:
      exp_batches = GenerateBatchesFromString(
          l_schema, {R"([[0,"d"]])", R"([[2,"d"], [3,"a"], [4,"a"]])", R"([])"},
          multiplicity);
      break;
    case RIGHT_ANTI:
      exp_batches = GenerateBatchesFromString(
          r_schema, {R"([["f", 0]])", R"([["g", 4]])", R"([])"}, multiplicity);
      break;
    case INNER:
    case LEFT_OUTER:
    case RIGHT_OUTER:
    case FULL_OUTER:
    default:
      FAIL() << "join type not implemented!";
  }

  CheckRunOutput(type, l_batches, r_batches,
                 /*left_keys=*/{{"l_str"}}, /*right_keys=*/{{"r_str"}}, exp_batches,
                 parallel);
}

void RunEmptyTest(JoinType type, bool parallel) {
  auto l_schema = schema({field("l_i32", int32()), field("l_str", utf8())});
  auto r_schema = schema({field("r_str", utf8()), field("r_i32", int32())});

  int multiplicity = parallel ? 100 : 1;

  BatchesWithSchema l_empty, r_empty, l_n_empty, r_n_empty;

  l_empty = GenerateBatchesFromString(l_schema, {R"([])"}, multiplicity);
  r_empty = GenerateBatchesFromString(r_schema, {R"([])"}, multiplicity);

  l_n_empty =
      GenerateBatchesFromString(l_schema, {R"([[0,"d"], [1,"b"]])"}, multiplicity);
  r_n_empty = GenerateBatchesFromString(r_schema, {R"([["f", 0], ["b", 1], ["b", 2]])"},
                                        multiplicity);

  std::vector<FieldRef> l_keys{{"l_str"}};
  std::vector<FieldRef> r_keys{{"r_str"}};

  switch (type) {
    case LEFT_SEMI:
      // both empty
      CheckRunOutput(type, l_empty, r_empty, l_keys, r_keys, l_empty, parallel);
      // right empty
      CheckRunOutput(type, l_n_empty, r_empty, l_keys, r_keys, l_empty, parallel);
      // left empty
      CheckRunOutput(type, l_empty, r_n_empty, l_keys, r_keys, l_empty, parallel);
      break;
    case RIGHT_SEMI:
      // both empty
      CheckRunOutput(type, l_empty, r_empty, l_keys, r_keys, r_empty, parallel);
      // right empty
      CheckRunOutput(type, l_n_empty, r_empty, l_keys, r_keys, r_empty, parallel);
      // left empty
      CheckRunOutput(type, l_empty, r_n_empty, l_keys, r_keys, r_empty, parallel);
      break;
    case LEFT_ANTI:
      // both empty
      CheckRunOutput(type, l_empty, r_empty, l_keys, r_keys, l_empty, parallel);
      // right empty
      CheckRunOutput(type, l_n_empty, r_empty, l_keys, r_keys, l_n_empty, parallel);
      // left empty
      CheckRunOutput(type, l_empty, r_n_empty, l_keys, r_keys, l_empty, parallel);
      break;
    case RIGHT_ANTI:
      // both empty
      CheckRunOutput(type, l_empty, r_empty, l_keys, r_keys, r_empty, parallel);
      // right empty
      CheckRunOutput(type, l_n_empty, r_empty, l_keys, r_keys, r_empty, parallel);
      // left empty
      CheckRunOutput(type, l_empty, r_n_empty, l_keys, r_keys, r_n_empty, parallel);
      break;
    case INNER:
    case LEFT_OUTER:
    case RIGHT_OUTER:
    case FULL_OUTER:
    default:
      FAIL() << "join type not implemented!";
  }
}

class HashJoinTest : public testing::TestWithParam<std::tuple<JoinType, bool>> {};

INSTANTIATE_TEST_SUITE_P(
    HashJoinTest, HashJoinTest,
    ::testing::Combine(::testing::Values(JoinType::LEFT_SEMI, JoinType::RIGHT_SEMI,
                                         JoinType::LEFT_ANTI, JoinType::RIGHT_ANTI),
                       ::testing::Values(false, true)));

TEST_P(HashJoinTest, TestSemiJoins) {
  RunNonEmptyTest(std::get<0>(GetParam()), std::get<1>(GetParam()));
}

TEST_P(HashJoinTest, TestSemiJoinstEmpty) {
  RunEmptyTest(std::get<0>(GetParam()), std::get<1>(GetParam()));
}

template <typename ARROW_T, typename C_TYPE>
static Status SimpleVerifySemiJoinOutputImpl(int index_col,
                                             const std::shared_ptr<Schema>& schema,
                                             const std::vector<ExecBatch>& build_batches,
                                             const std::vector<ExecBatch>& probe_batches,
                                             const std::vector<ExecBatch>& output_batches,
                                             bool anti_join = false) {
  // populate hash set
  std::unordered_set<C_TYPE> hash_set;
  bool has_null = false;
  for (auto&& b : build_batches) {
    const std::shared_ptr<ArrayData>& arr = b[index_col].array();
    VisitArrayDataInline<ARROW_T>(
        *arr, [&](C_TYPE val) { hash_set.insert(val); }, [&]() { has_null = true; });
  }

  // probe hash set
  RecordBatchVector exp_batches;
  exp_batches.reserve(probe_batches.size());
  for (auto&& b : probe_batches) {
    const std::shared_ptr<ArrayData>& arr = b[index_col].array();

    BooleanBuilder builder;
    RETURN_NOT_OK(builder.Reserve(arr->length));
    VisitArrayDataInline<ARROW_T>(
        *arr,
        [&](C_TYPE val) {
          auto res = hash_set.find(val);
          // setting anti_join, would invert res != hash_set.end()
          builder.UnsafeAppend(anti_join != (res != hash_set.end()));
        },
        [&]() { builder.UnsafeAppend(anti_join != has_null); });

    ARROW_ASSIGN_OR_RAISE(auto filter, builder.Finish());

    ARROW_ASSIGN_OR_RAISE(auto rec_batch, b.ToRecordBatch(schema));
    ARROW_ASSIGN_OR_RAISE(auto filtered, Filter(rec_batch, filter));

    exp_batches.push_back(filtered.record_batch());
  }

  ARROW_ASSIGN_OR_RAISE(auto exp_table, Table::FromRecordBatches(exp_batches));
  std::vector<SortKey> sort_keys;
  for (auto&& f : schema->fields()) {
    sort_keys.emplace_back(f->name());
  }
  ARROW_ASSIGN_OR_RAISE(auto exp_table_sort_ids,
                        SortIndices(exp_table, SortOptions(sort_keys)));
  ARROW_ASSIGN_OR_RAISE(auto exp_table_sorted, Take(exp_table, exp_table_sort_ids));

  // create a table from output batches
  RecordBatchVector output_rbs;
  for (auto&& b : output_batches) {
    ARROW_ASSIGN_OR_RAISE(auto rb, b.ToRecordBatch(schema));
    output_rbs.push_back(std::move(rb));
  }

  ARROW_ASSIGN_OR_RAISE(auto out_table, Table::FromRecordBatches(output_rbs));
  ARROW_ASSIGN_OR_RAISE(auto out_table_sort_ids,
                        SortIndices(exp_table, SortOptions(sort_keys)));
  ARROW_ASSIGN_OR_RAISE(auto out_table_sorted, Take(exp_table, exp_table_sort_ids));

  AssertTablesEqual(*exp_table_sorted.table(), *out_table_sorted.table(),
                    /*same_chunk_layout=*/false, /*flatten=*/true);

  return Status::OK();
}

template <typename T, typename Enable = void>
struct SimpleVerifySemiJoinOutput {};

template <typename T>
struct SimpleVerifySemiJoinOutput<T, enable_if_primitive_ctype<T>> {
  static Status Verify(int index_col, const std::shared_ptr<Schema>& schema,
                       const std::vector<ExecBatch>& build_batches,
                       const std::vector<ExecBatch>& probe_batches,
                       const std::vector<ExecBatch>& output_batches,
                       bool anti_join = false) {
    return SimpleVerifySemiJoinOutputImpl<T, typename arrow::TypeTraits<T>::CType>(
        index_col, schema, build_batches, probe_batches, output_batches, anti_join);
  }
};

template <typename T>
struct SimpleVerifySemiJoinOutput<T, enable_if_base_binary<T>> {
  static Status Verify(int index_col, const std::shared_ptr<Schema>& schema,
                       const std::vector<ExecBatch>& build_batches,
                       const std::vector<ExecBatch>& probe_batches,
                       const std::vector<ExecBatch>& output_batches,
                       bool anti_join = false) {
    return SimpleVerifySemiJoinOutputImpl<T, util::string_view>(
        index_col, schema, build_batches, probe_batches, output_batches, anti_join);
  }
};

template <typename ARROW_T>
void TestSemiJoinRandom(JoinType type, bool parallel, int num_batches, int batch_size) {
  auto data_type = default_type_instance<ARROW_T>();
  auto l_schema = schema({field("l0", data_type), field("l1", data_type)});
  auto r_schema = schema({field("r0", data_type), field("r1", data_type)});

  // generate data
  auto l_batches = MakeRandomBatches(l_schema, num_batches, batch_size);
  auto r_batches = MakeRandomBatches(r_schema, num_batches, batch_size);

  std::vector<FieldRef> left_keys{{"l0"}};
  std::vector<FieldRef> right_keys{{"r1"}};

  auto exec_ctx = arrow::internal::make_unique<ExecContext>(
      default_memory_pool(), parallel ? arrow::internal::GetCpuThreadPool() : nullptr);

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(exec_ctx.get()));

  JoinNodeOptions join_options{type, left_keys, right_keys};
  Declaration join{"hash_join", join_options};

  // add left source
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(parallel,
                                                                  /*slow=*/false)}});
  // add right source
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r_batches.schema, r_batches.gen(parallel,
                                                                  /*slow=*/false)}});
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  ASSERT_OK(Declaration::Sequence({join, {"sink", SinkNodeOptions{&sink_gen}}})
                .AddToPlan(plan.get()));

  ASSERT_FINISHES_OK_AND_ASSIGN(auto res, StartAndCollect(plan.get(), sink_gen));

  // verification step for res
  switch (type) {
    case LEFT_SEMI:
      ASSERT_OK(SimpleVerifySemiJoinOutput<ARROW_T>::Verify(
          0, l_schema, r_batches.batches, l_batches.batches, res));
      return;
    case RIGHT_SEMI:
      ASSERT_OK(SimpleVerifySemiJoinOutput<ARROW_T>::Verify(
          0, r_schema, l_batches.batches, r_batches.batches, res));
      return;
    case LEFT_ANTI:
      ASSERT_OK(SimpleVerifySemiJoinOutput<ARROW_T>::Verify(
          0, l_schema, r_batches.batches, l_batches.batches, res, true));
      return;
    case RIGHT_ANTI:
      ASSERT_OK(SimpleVerifySemiJoinOutput<ARROW_T>::Verify(
          0, l_schema, r_batches.batches, l_batches.batches, res, true));
      return;
    default:
      FAIL() << "Unsupported join type";
  }
}

static constexpr int kNumBatches = 100;
static constexpr int kBatchSize = 10;

using TestingTypes = ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type,
                                      FloatType, DoubleType, StringType>;

template <typename Type>
class HashJoinTestRand : public testing::Test {};

TYPED_TEST_SUITE(HashJoinTestRand, TestingTypes);

TYPED_TEST(HashJoinTestRand, LeftSemiJoin) {
  for (bool parallel : {false, true}) {
    TestSemiJoinRandom<TypeParam>(JoinType::LEFT_SEMI, parallel, kNumBatches, kBatchSize);
  }
}

TYPED_TEST(HashJoinTestRand, RightSemiJoin) {
  for (bool parallel : {false, true}) {
    TestSemiJoinRandom<TypeParam>(JoinType::RIGHT_SEMI, parallel, kNumBatches,
                                  kBatchSize);
  }
}

TYPED_TEST(HashJoinTestRand, LeftAntiJoin) {
  for (bool parallel : {false, true}) {
    TestSemiJoinRandom<TypeParam>(JoinType::LEFT_ANTI, parallel, kNumBatches, kBatchSize);
  }
}

TYPED_TEST(HashJoinTestRand, RightAntiJoin) {
  for (bool parallel : {false, true}) {
    TestSemiJoinRandom<TypeParam>(JoinType::RIGHT_ANTI, parallel, kNumBatches,
                                  kBatchSize);
  }
}

}  // namespace compute
}  // namespace arrow
