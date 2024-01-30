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

template <typename ThisNode>
struct PauseThenStopNode : public MapNode {
  PauseThenStopNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                    std::shared_ptr<Schema> output_schema,
                    const PauseThenStopNodeOptions& options)
      : MapNode(plan, inputs, output_schema), num_pass(options.num_pass) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, ThisNode::kKindName));
    auto bp_options = static_cast<const PauseThenStopNodeOptions&>(options);
    if (bp_options.num_pass < 2) {
      return Status::Invalid("num_pass must be at least 2");
    }
    return plan->EmplaceNode<ThisNode>(plan, inputs, inputs[0]->output_schema(),
                                       bp_options);
  }

  const char* kind_name() const override { return ThisNode::kKindName; }
  Result<ExecBatch> ProcessBatch(ExecBatch batch) override {
    if (num_pass == 1) {
      inputs()[0]->PauseProducing(this, 1);
      ARROW_RETURN_NOT_OK(static_cast<ThisNode*>(this)->DoStopProducing());
    }
    if (num_pass > 0) --num_pass;
    return batch;
  }

  int num_pass;
};

struct PauseThenStopInputNode : public PauseThenStopNode<PauseThenStopInputNode> {
  static constexpr bool kPlanExitsEarly = false;

  static constexpr const char* kKindName = "PauseThenStopInputNode";
  static constexpr const char* kFactoryName = "pause_then_stop_input";

  static Status Register() {
    auto exec_reg = default_exec_factory_registry();
    if (!exec_reg->GetFactory(kFactoryName).ok()) {
      ARROW_RETURN_NOT_OK(
          exec_reg->AddFactory(kFactoryName, PauseThenStopInputNode::Make));
    }
    return Status::OK();
  }

  using PauseThenStopNode::PauseThenStopNode;

  Status DoStopProducing() { return inputs()[0]->StopProducing(); }
};

struct PauseThenStopPlanNode : public PauseThenStopNode<PauseThenStopPlanNode> {
  static constexpr bool kPlanExitsEarly = true;

  static constexpr const char* kKindName = "PauseThenStopPlanNode";
  static constexpr const char* kFactoryName = "pause_then_stop_plan";

  static Status Register() {
    auto exec_reg = default_exec_factory_registry();
    if (!exec_reg->GetFactory(kFactoryName).ok()) {
      ARROW_RETURN_NOT_OK(
          exec_reg->AddFactory(kFactoryName, PauseThenStopPlanNode::Make));
    }
    return Status::OK();
  }

  using PauseThenStopNode::PauseThenStopNode;

  Status DoStopProducing() {
    plan()->StopProducing();
    return Status::OK();
  }
};

// GH-35837
// This tests that the plan shuts down cleanly when `StopProducing` is sent while
// backpressure is applied to the source node.
template <typename ThisNode>
void TestPauseThenStop() {
  ASSERT_OK(ThisNode::Register());

  // number of batches, number of batches to pass before pausing, batch size
  constexpr int num_batches = 10, num_pass = 2, batch_size = 1;
  // the above constants can be changed subject to the following restrictions
  // to ensure that the test works
  ASSERT_GE(num_pass, 1);            // must pass at least one batch before pausing
  ASSERT_GT(num_batches, num_pass);  // must have more batches after pausing
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
      ThisNode::kFactoryName, {t_src}, PauseThenStopNodeOptions(num_pass)};

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatchReader> batch_reader,
                       DeclarationToReader(ctrl, /*use_threads=*/false));

  if (!ThisNode::kPlanExitsEarly) {
    int64_t total_length = 0;
    for (;;) {
      ASSERT_OK_AND_ASSIGN(auto batch, batch_reader->Next());
      if (!batch) {
        break;
      }
      total_length += batch->num_rows();
    }
    ASSERT_EQ(static_cast<int64_t>(num_pass * batch_size), total_length);
  } else {
    for (int i = 0; i < num_pass - 1; i++) {
      ASSERT_OK_AND_ASSIGN(auto batch, batch_reader->Next());
      ASSERT_TRUE(batch);
    }
    ASSERT_RAISES(Cancelled, batch_reader->Next());
  }
}

TEST(SourceNode, PauseThenStopInput) { TestPauseThenStop<PauseThenStopInputNode>(); }

TEST(SourceNode, PauseThenStopPlan) { TestPauseThenStop<PauseThenStopPlanNode>(); }

}  // namespace acero
}  // namespace arrow
