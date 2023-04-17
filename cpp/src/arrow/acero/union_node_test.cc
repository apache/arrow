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
#include <random>

#include "arrow/acero/options.h"
#include "arrow/acero/test_util_internal.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"

using testing::UnorderedElementsAreArray;

namespace arrow {
namespace acero {

struct TestUnionNode : public ::testing::Test {
  static constexpr int kNumBatches = 10;
  static constexpr int kBatchSize = 10;

  TestUnionNode() : rng_(0) {}

  std::shared_ptr<Schema> GenerateRandomSchema(size_t num_inputs) {
    static std::vector<std::shared_ptr<DataType>> some_arrow_types = {
        arrow::null(),    arrow::boolean(), arrow::int8(),    arrow::int16(),
        arrow::int32(),   arrow::int64(),   arrow::float16(), arrow::float32(),
        arrow::float64(), arrow::utf8(),    arrow::binary(),  arrow::date32()};

    std::vector<std::shared_ptr<Field>> fields(num_inputs);
    std::default_random_engine gen(42);
    std::uniform_int_distribution<int> types_dist(
        0, static_cast<int>(some_arrow_types.size()) - 1);
    for (size_t i = 0; i < num_inputs; i++) {
      int random_index = types_dist(gen);
      auto col_type = some_arrow_types.at(random_index);
      fields[i] =
          field("column_" + std::to_string(i) + "_" + col_type->ToString(), col_type);
    }
    return schema(fields);
  }

  void GenerateBatchesFromSchema(const std::shared_ptr<Schema>& schema,
                                 size_t num_batches, BatchesWithSchema* out_batches,
                                 int multiplicity = 1, int64_t batch_size = 4) {
    if (num_batches == 0) {
      auto empty_record_batch = ExecBatch(*rng_.BatchOf(schema->fields(), 0));
      out_batches->batches.push_back(empty_record_batch);
    } else {
      for (size_t j = 0; j < num_batches; j++) {
        out_batches->batches.push_back(
            ExecBatch(*rng_.BatchOf(schema->fields(), batch_size)));
      }
    }

    size_t batch_count = out_batches->batches.size();
    for (int repeat = 1; repeat < multiplicity; ++repeat) {
      for (size_t i = 0; i < batch_count; ++i) {
        out_batches->batches.push_back(out_batches->batches[i]);
      }
    }
    out_batches->schema = schema;
  }

  void CheckRunOutput(const std::vector<BatchesWithSchema>& batches,
                      const BatchesWithSchema& exp_batches, bool parallel = false) {
    SCOPED_TRACE(parallel ? "parallel" : "single threaded");

    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());

    Declaration union_decl{"union", ExecNodeOptions{}};

    for (const auto& batch : batches) {
      union_decl.inputs.emplace_back(Declaration{
          "source", SourceNodeOptions{batch.schema, batch.gen(parallel,
                                                              /*slow=*/false)}});
    }
    AsyncGenerator<std::optional<ExecBatch>> sink_gen;

    // Test UnionNode::Make with zero inputs
    if (batches.size() == 0) {
      ASSERT_RAISES(Invalid, Declaration::Sequence(
                                 {union_decl, {"sink", SinkNodeOptions{&sink_gen}}})
                                 .AddToPlan(plan.get()));
      return;
    } else {
      ASSERT_OK(Declaration::Sequence({union_decl, {"sink", SinkNodeOptions{&sink_gen}}})
                    .AddToPlan(plan.get()));
    }

    Future<std::vector<ExecBatch>> actual = StartAndCollect(plan.get(), sink_gen);

    auto expected_matcher =
        Finishes(ResultWith(UnorderedElementsAreArray(exp_batches.batches)));
    ASSERT_THAT(actual, expected_matcher);
  }

  void CheckUnionExecNode(size_t num_input_nodes, size_t num_batches, bool parallel) {
    auto random_schema = GenerateRandomSchema(num_input_nodes);

    int multiplicity = parallel ? 10 : 1;
    std::vector<std::shared_ptr<RecordBatch>> all_record_batches;
    std::vector<BatchesWithSchema> input_batches(num_input_nodes);
    BatchesWithSchema exp_batches;
    exp_batches.schema = random_schema;
    for (size_t i = 0; i < num_input_nodes; i++) {
      GenerateBatchesFromSchema(random_schema, num_batches, &input_batches[i],
                                multiplicity, kBatchSize);
      for (const auto& batch : input_batches[i].batches) {
        exp_batches.batches.push_back(batch);
      }
    }
    CheckRunOutput(input_batches, exp_batches, parallel);
  }

  ::arrow::random::RandomArrayGenerator rng_;
};

TEST_F(TestUnionNode, TestNonEmpty) {
  for (bool parallel : {false, true}) {
    for (int64_t num_input_nodes : {1, 2, 4, 8}) {
      this->CheckUnionExecNode(num_input_nodes, kNumBatches, parallel);
    }
  }
}
TEST_F(TestUnionNode, TestWithAnEmptyBatch) {
  this->CheckUnionExecNode(/*num_input_nodes*/ 2, /*num_batches=*/0, /*parallel=*/false);
}

TEST_F(TestUnionNode, TestEmpty) {
  this->CheckUnionExecNode(/*num_input_nodes*/ 0, /*num_batches=*/0, /*parallel=*/false);
}

}  // namespace acero
}  // namespace arrow
