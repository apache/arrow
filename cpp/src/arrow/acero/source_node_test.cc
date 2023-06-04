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

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/map_node.h"
#include "arrow/acero/options.h"
#include "arrow/acero/test_nodes.h"

namespace arrow {
namespace acero {

struct PauseThenStopNodeOptions : public ExecNodeOptions {
  explicit PauseThenStopNodeOptions(int num_pass) : num_pass(num_pass) {}

  int num_pass;
};

struct PauseThenStopNode : public MapNode {
  static constexpr const char* kKindName = "PauseThenStopNode";
  static constexpr const char* kFactoryName = "pause_then_stop";

  static void Register() {
    auto exec_reg = default_exec_factory_registry();
    if (!exec_reg->GetFactory(kFactoryName).ok()) {
      ASSERT_OK(exec_reg->AddFactory(kFactoryName, PauseThenStopNode::Make));
    }
  }

  PauseThenStopNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                    std::shared_ptr<Schema> output_schema,
                    const PauseThenStopNodeOptions& options)
      : MapNode(plan, inputs, output_schema), num_pass(options.num_pass) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, kKindName));
    auto bp_options = static_cast<const PauseThenStopNodeOptions&>(options);
    if (bp_options.num_pass < 2) {
      return Status::Invalid("num_pass must be at least 2");
    }
    return plan->EmplaceNode<PauseThenStopNode>(plan, inputs, inputs[0]->output_schema(),
                                                bp_options);
  }

  const char* kind_name() const override { return kKindName; }
  Result<ExecBatch> ProcessBatch(ExecBatch batch) override {
    if (num_pass == 2) {
      inputs()[0]->PauseProducing(this, 1);
    } else if (num_pass == 1) {
      ARROW_RETURN_NOT_OK(inputs()[0]->StopProducing());
    }
    if (num_pass > 0) --num_pass;
    return batch;
  }

  int num_pass;
};

TEST(SourceNode, PauseThenStop) {
  PauseThenStopNode::Register();

  constexpr int num_pass = 2, num_batches = 10, batch_size = 1;
  ASSERT_GE(num_pass, 2);
  ASSERT_GT(num_batches, num_pass);
  auto t_schema = schema({field("time", int32()), field("value", int32())});
  ASSERT_OK_AND_ASSIGN(auto t_batches,
                       MakeIntegerBatches({[](int row) -> int64_t { return row; },
                                           [](int row) -> int64_t { return row + 1; }},
                                          t_schema, num_batches, batch_size));

  Declaration t_src = {
      "source", SourceNodeOptions(t_batches.schema,
                                  MakeDelayedGen(t_batches, "t_src", /*delay_sec=*/0.5,
                                                 /*noisy=*/false))};
  Declaration ctrl = {
      PauseThenStopNode::kFactoryName, {t_src}, PauseThenStopNodeOptions(num_pass)};

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatchReader> batch_reader,
                       DeclarationToReader(ctrl, /*use_threads=*/false));

  int64_t total_length = 0;
  for (;;) {
    ASSERT_OK_AND_ASSIGN(auto batch, batch_reader->Next());
    if (!batch) {
      break;
    }
    total_length += batch->num_rows();
  }
  ASSERT_EQ(static_cast<int64_t>(num_pass * batch_size), total_length);
}

}  // namespace acero
}  // namespace arrow
