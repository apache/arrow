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

void CheckRunOutput(JoinType type, BatchesWithSchema l_batches,
                    BatchesWithSchema r_batches,
                    const std::vector<std::string>& left_keys,
                    const std::vector<std::string>& right_keys,
                    const BatchesWithSchema& exp_batches, bool parallel = false) {
  SCOPED_TRACE("serial");

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

  ASSERT_OK_AND_ASSIGN(auto l_source,
                       MakeTestSourceNode(plan.get(), "l_source", std::move(l_batches),
                                          /*parallel=*/parallel,
                                          /*slow=*/false));
  ASSERT_OK_AND_ASSIGN(auto r_source,
                       MakeTestSourceNode(plan.get(), "r_source", std::move(r_batches),
                                          /*parallel=*/parallel,
                                          /*slow=*/false));

  ASSERT_OK_AND_ASSIGN(
      auto semi_join,
      MakeHashJoinNode(type, l_source, r_source, "hash_join", left_keys, right_keys));
  auto sink_gen = MakeSinkNode(semi_join, "sink");

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

  CheckRunOutput(type, std::move(l_batches), std::move(r_batches),
                 /*left_keys=*/{"l_str"}, /*right_keys=*/{"r_str"}, exp_batches,
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

  std::vector<std::string> l_keys{"l_str"};
  std::vector<std::string> r_keys{"r_str"};

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

TEST_P(HashJoinTest, TestSemiJoinsLeftEmpty) {
  RunEmptyTest(std::get<0>(GetParam()), std::get<1>(GetParam()));
}

}  // namespace compute
}  // namespace arrow
