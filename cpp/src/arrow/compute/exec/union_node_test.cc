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

#include <iostream>

#include "arrow/api.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/pretty_print.h"
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

void CheckRunOutput(const BatchesWithSchema& l_batches,
                    const BatchesWithSchema& r_batches,
                    const BatchesWithSchema& exp_batches, bool parallel = false) {
  SCOPED_TRACE(parallel ? "parallel" : "single threaded");

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

  Declaration union_decl{"union", ExecNodeOptions{}};

  // add left source
  union_decl.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(parallel,
                                                                  /*slow=*/false)}});
  // add right source
  union_decl.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r_batches.schema, r_batches.gen(parallel,
                                                                  /*slow=*/false)}});
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  ASSERT_OK(Declaration::Sequence({union_decl, {"sink", SinkNodeOptions{&sink_gen}}})
                .AddToPlan(plan.get()));

  Future<std::vector<ExecBatch>> actual = StartAndCollect(plan.get(), sink_gen);

  auto expected_matcher =
      Finishes(ResultWith(UnorderedElementsAreArray(exp_batches.batches)));
  ASSERT_THAT(actual, expected_matcher);
}

void RunNonEmptyTest(bool parallel) {
  auto l_schema = schema({field("colum_i32", int32()), field("colum_str", utf8())});
  auto r_schema = schema({field("colum_i32", int32()), field("colum_str", utf8())});
  BatchesWithSchema l_batches, r_batches, exp_batches;

  int multiplicity = parallel ? 100 : 1;

  GenerateBatchesFromString(l_schema,
                            {
                                R"([[0,"d"], [1,"b"]])",
                                R"([[2,"d"], [3,"a"], [4,"a"]])",
                            },
                            &l_batches, multiplicity);

  GenerateBatchesFromString(r_schema,
                            {
                                R"([[10,"A"]])",
                            },
                            &r_batches, multiplicity);

  GenerateBatchesFromString(l_schema,
                            {
                                R"([[0,"d"], [1,"b"]])",
                                R"([[2,"d"], [3,"a"], [4,"a"]])",

                                R"([[10,"A"]])",
                            },
                            &exp_batches, multiplicity);
  CheckRunOutput(l_batches, r_batches, exp_batches, parallel);
}

void RunEmptyTest(bool parallel) {
  auto l_schema = schema({field("colum_i32", int32()), field("colum_str", utf8())});
  auto r_schema = schema({field("colum_i32", int32()), field("colum_str", utf8())});

  int multiplicity = parallel ? 100 : 1;

  BatchesWithSchema l_empty, r_empty, output_batches;

  GenerateBatchesFromString(l_schema, {R"([])"}, &l_empty, multiplicity);
  GenerateBatchesFromString(r_schema, {R"([])"}, &r_empty, multiplicity);

  GenerateBatchesFromString(l_schema, {R"([])", R"([])"}, &output_batches, multiplicity);

  CheckRunOutput(l_empty, r_empty, output_batches);
}

TEST(UnionTest, TestNonEmpty) {
  for (bool parallel : {false, true}) {
    RunNonEmptyTest(parallel);
  }
}

TEST(UnionTest, TestEmpty) {
  for (bool parallel : {false, true}) {
    RunEmptyTest(parallel);
  }
}

void TestUnionRandom(const std::shared_ptr<DataType>& data_type, bool parallel,
                     int num_batches, int batch_size) {
  auto l_schema = schema({field("colum0", data_type), field("colum1", data_type)});
  auto r_schema = schema({field("colum0", data_type), field("colum1", data_type)});

  // generate data
  auto l_batches = MakeRandomBatches(l_schema, num_batches, batch_size);
  auto r_batches = MakeRandomBatches(r_schema, num_batches, batch_size);

  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

  Declaration Union{"union", ExecNodeOptions{}};

  // add left source
  Union.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(parallel,
                                                                  /*slow=*/false)}});
  // add right source
  Union.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r_batches.schema, r_batches.gen(parallel,
                                                                  /*slow=*/false)}});
  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  ASSERT_OK(Declaration::Sequence({Union, {"sink", SinkNodeOptions{&sink_gen}}})
                .AddToPlan(plan.get()));

  auto actual = StartAndCollect(plan.get(), sink_gen);

  BatchesWithSchema exp_batches;
  exp_batches.schema = l_schema;
  exp_batches.batches.reserve(l_batches.batches.size() + r_batches.batches.size());

  std::copy(l_batches.batches.begin(), l_batches.batches.end(),
            std::back_inserter(exp_batches.batches));
  std::copy(r_batches.batches.begin(), r_batches.batches.end(),
            std::back_inserter(exp_batches.batches));

  auto expected_matcher =
      Finishes(ResultWith(UnorderedElementsAreArray(exp_batches.batches)));
  ASSERT_THAT(actual, expected_matcher);
}

class UnionTestRand
    : public testing::TestWithParam<std::tuple<std::shared_ptr<DataType>, bool>> {};

static constexpr int kNumBatches = 100;
static constexpr int kBatchSize = 10;

INSTANTIATE_TEST_SUITE_P(UnionTestRand, UnionTestRand,
                         ::testing::Combine(::testing::Values(int8(), int32(), int64(),
                                                              float32(), float64()),
                                            ::testing::Values(false, true)));

TEST_P(UnionTestRand, TestingTypes) {
  TestUnionRandom(std::get<0>(GetParam()), std::get<1>(GetParam()), kNumBatches,
                  kBatchSize);
}

}  // namespace compute
}  // namespace arrow