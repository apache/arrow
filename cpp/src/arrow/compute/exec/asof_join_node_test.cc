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

#include <numeric>
#include <random>
#include <unordered_set>

#include "arrow/api.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/kernels/row_encoder.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
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

void CheckRunOutput(const BatchesWithSchema& l_batches,
                    const BatchesWithSchema& r_batches,
                    const BatchesWithSchema& exp_batches, const FieldRef time,
                    const FieldRef keys, const long tolerance) {
  auto exec_ctx =
      arrow::internal::make_unique<ExecContext>(default_memory_pool(), nullptr);
  ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make(exec_ctx.get()));

  AsofJoinNodeOptions join_options(time, keys, tolerance);
  Declaration join{"asofjoin", join_options};

  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{l_batches.schema, l_batches.gen(false, false)}});
  join.inputs.emplace_back(Declaration{
      "source", SourceNodeOptions{r_batches.schema, r_batches.gen(false, false)}});

  AsyncGenerator<util::optional<ExecBatch>> sink_gen;

  ASSERT_OK(Declaration::Sequence({join, {"sink", SinkNodeOptions{&sink_gen}}})
                .AddToPlan(plan.get()));

  ASSERT_FINISHES_OK_AND_ASSIGN(auto res, StartAndCollect(plan.get(), sink_gen));

  ASSERT_OK_AND_ASSIGN(auto exp_table,
                       TableFromExecBatches(exp_batches.schema, exp_batches.batches));

  ASSERT_OK_AND_ASSIGN(auto res_table, TableFromExecBatches(exp_batches.schema, res));

  AssertTablesEqual(*exp_table, *res_table,
                    /*same_chunk_layout=*/false, /*flatten=*/true);

  std::cerr << "Result Equals"
            << "\n";
}

void RunNonEmptyTest(bool exact_matches) {
  auto l_schema =
      schema({field("time", int64()), field("key", int32()), field("l_v0", float64())});
  auto r_schema =
      schema({field("time", int64()), field("key", int32()), field("r_v0", float64())});
  auto exp_schema = schema({
      field("time", int64()),
      field("key", int32()),
      field("l_v0", float64()),
      field("r_v0", float64()),
  });

  BatchesWithSchema l_batches, r_batches, exp_batches;

  l_batches = GenerateBatchesFromString(l_schema, {R"([[1000, 1, 1.0]])"});
  r_batches = GenerateBatchesFromString(r_schema, {R"([[0, 1, 2.0]])"});
  exp_batches = GenerateBatchesFromString(exp_schema, {R"([[1000, 1, 1.0, 2.0]])"});

  CheckRunOutput(l_batches, r_batches, exp_batches, "time", "key", 1000);

  l_batches = GenerateBatchesFromString(l_schema, {R"([[1000, 1, 1.0]])"});
  r_batches = GenerateBatchesFromString(r_schema, {R"([[0, 1, 2.0]])"});
  // This is wrong
  // TODO: Fix null values in the result
  exp_batches = GenerateBatchesFromString(exp_schema, {R"([[1000, 1, 1.0, 0.0]])"});

  CheckRunOutput(l_batches, r_batches, exp_batches, "time", "key", 999);
}

class AsofJoinTest : public testing::TestWithParam<std::tuple<bool>> {};

INSTANTIATE_TEST_SUITE_P(AsofJoinTest, AsofJoinTest,
                         ::testing::Combine(::testing::Values(false, true)));

TEST_P(AsofJoinTest, TestExactMatches) { RunNonEmptyTest(std::get<0>(GetParam())); }

}  // namespace compute
}  // namespace arrow
