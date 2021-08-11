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

#include "arrow/api.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

using testing::UnorderedElementsAreArray;

namespace arrow {
namespace compute {

void GenerateBatchesFromString(const std::shared_ptr<Schema>& schema,
                               const std::vector<util::string_view>& json_strings,
                               BatchesWithSchema* out_batches, int multiplicity = 1) {
  std::vector<ValueDescr> descrs;
  for (auto&& field : schema->fields()) {
    descrs.emplace_back(field->type());
  }

  for (auto&& s : json_strings) {
    out_batches->batches.push_back(ExecBatchFromJSON(descrs, s));
  }

  size_t batch_count = out_batches->batches.size();
  for (int repeat = 1; repeat < multiplicity; ++repeat) {
    for (size_t i = 0; i < batch_count; ++i) {
      out_batches->batches.push_back(out_batches->batches[i]);
    }
  }

  out_batches->schema = schema;
}

void CheckRunOutput(JoinType type, const BatchesWithSchema& l_batches,
                    const BatchesWithSchema& r_batches,
                    const std::vector<FieldRef>& left_keys,
                    const std::vector<FieldRef>& right_keys,
                    const BatchesWithSchema& exp_batches, bool parallel = false) {
  SCOPED_TRACE("serial");

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

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

  GenerateBatchesFromString(l_schema,
                            {R"([[0,"d"], [1,"b"]])", R"([[2,"d"], [3,"a"], [4,"a"]])",
                             R"([[5,"b"], [6,"c"], [7,"e"], [8,"e"]])"},
                            &l_batches, multiplicity);

  GenerateBatchesFromString(
      r_schema,
      {R"([["f", 0], ["b", 1], ["b", 2]])", R"([["c", 3], ["g", 4]])", R"([["e", 5]])"},
      &r_batches, multiplicity);

  switch (type) {
    case LEFT_SEMI:
      GenerateBatchesFromString(
          l_schema, {R"([[1,"b"]])", R"([])", R"([[5,"b"], [6,"c"], [7,"e"], [8,"e"]])"},
          &exp_batches, multiplicity);
      break;
    case RIGHT_SEMI:
      GenerateBatchesFromString(
          r_schema, {R"([["b", 1], ["b", 2]])", R"([["c", 3]])", R"([["e", 5]])"},
          &exp_batches, multiplicity);
      break;
    case LEFT_ANTI:
      GenerateBatchesFromString(
          l_schema, {R"([[0,"d"]])", R"([[2,"d"], [3,"a"], [4,"a"]])", R"([])"},
          &exp_batches, multiplicity);
      break;
    case RIGHT_ANTI:
      GenerateBatchesFromString(r_schema, {R"([["f", 0]])", R"([["g", 4]])", R"([])"},
                                &exp_batches, multiplicity);
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

  GenerateBatchesFromString(l_schema, {R"([])"}, &l_empty, multiplicity);
  GenerateBatchesFromString(r_schema, {R"([])"}, &r_empty, multiplicity);

  GenerateBatchesFromString(l_schema, {R"([[0,"d"], [1,"b"]])"}, &l_n_empty,
                            multiplicity);
  GenerateBatchesFromString(r_schema, {R"([["f", 0], ["b", 1], ["b", 2]])"}, &r_n_empty,
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

void TestJoinRandom(const std::shared_ptr<DataType>& data_type, JoinType type,
                    bool parallel, int num_batches, int batch_size) {
  auto l_schema = schema({field("l0", data_type), field("l1", data_type)});
  auto r_schema = schema({field("r0", data_type), field("r1", data_type)});

  // generate data
  auto l_batches = MakeRandomBatches(l_schema, num_batches, batch_size);
  auto r_batches = MakeRandomBatches(r_schema, num_batches, batch_size);

  std::vector<FieldRef> left_keys{{"l0"}};
  std::vector<FieldRef> right_keys{{"r1"}};

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

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

  // TODO(niranda) add a verification step for res
}

class HashJoinTestRand : public testing::TestWithParam<
                             std::tuple<std::shared_ptr<DataType>, JoinType, bool>> {};

static constexpr int kNumBatches = 1000;
static constexpr int kBatchSize = 100;

INSTANTIATE_TEST_SUITE_P(
    HashJoinTestRand, HashJoinTestRand,
    ::testing::Combine(::testing::Values(int8(), int32(), int64(), float32(), float64()),
                       ::testing::Values(JoinType::LEFT_SEMI, JoinType::RIGHT_SEMI,
                                         JoinType::LEFT_ANTI, JoinType::RIGHT_ANTI),
                       ::testing::Values(false, true)));

TEST_P(HashJoinTestRand, TestingTypes) {
  TestJoinRandom(std::get<0>(GetParam()), std::get<1>(GetParam()),
                 std::get<2>(GetParam()), kNumBatches, kBatchSize);
}

}  // namespace compute
}  // namespace arrow
