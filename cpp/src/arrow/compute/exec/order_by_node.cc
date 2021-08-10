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

#include "arrow/compute/exec/exec_plan.h"

#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/table.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"

namespace arrow {

namespace compute {

using arrow::internal::checked_cast;

// Simple in-memory sort node. Accumulates all data, then sorts and
// emits output batches in order.
struct OrderByNode final : public ExecNode {
  OrderByNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
              std::shared_ptr<Schema> output_schema, SortOptions sort_options)
      : ExecNode(plan, std::move(inputs), {"target"}, std::move(output_schema),
                 /*num_outputs=*/1),
        sort_options_(std::move(sort_options)) {}

  const char* kind_name() override { return "OrderByNode"; }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "OrderByNode"));

    const auto& order_by_options = checked_cast<const OrderByNodeOptions&>(options);
    std::vector<std::string> fields;
    fields.reserve((order_by_options.sort_options.sort_keys.size()));
    for (const auto& key : order_by_options.sort_options.sort_keys)
      fields.push_back(key.name);
    auto output_schema = inputs[0]->output_schema();
    RETURN_NOT_OK(output_schema->CanReferenceFieldsByNames(fields));

    return plan->EmplaceNode<OrderByNode>(
        plan, std::move(inputs), std::move(output_schema), order_by_options.sort_options);
  }

  Status StartProducing() override {
    finished_ = Future<>::Make();
    return Status::OK();
  }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
  }

  void StopProducing() override { inputs_[0]->StopProducing(this); }

  Future<> finished() override { return finished_; }

  void InputReceived(ExecNode* input, int seq, ExecBatch batch) override {
    DCHECK_EQ(input, inputs_[0]);

    // Accumulate data
    auto maybe_batch = batch.ToRecordBatch(inputs_[0]->output_schema(),
                                           plan()->exec_context()->memory_pool());
    if (ErrorIfNotOk(maybe_batch.status())) return;
    batches_.push_back(maybe_batch.MoveValueUnsafe());

    if (input_counter_.Increment()) {
      ErrorIfNotOk(Finish());
    }
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    DCHECK_EQ(input, inputs_[0]);
    outputs_[0]->ErrorReceived(this, std::move(error));
  }

  void InputFinished(ExecNode* input, int seq_stop) override {
    if (input_counter_.SetTotal(seq_stop)) {
      ErrorIfNotOk(Finish());
    }
  }

 private:
  Status Finish() {
    ARROW_ASSIGN_OR_RAISE(
        auto table,
        Table::FromRecordBatches(inputs_[0]->output_schema(), std::move(batches_)));
    ARROW_ASSIGN_OR_RAISE(auto indices,
                          SortIndices(table, sort_options_, plan()->exec_context()));
    ARROW_ASSIGN_OR_RAISE(auto sorted, Take(table, indices, TakeOptions::NoBoundsCheck(),
                                            plan()->exec_context()));

    TableBatchReader reader(*sorted.table());
    int64_t count = 0;
    while (true) {
      std::shared_ptr<RecordBatch> batch;
      RETURN_NOT_OK(reader.ReadNext(&batch));
      if (!batch) break;
      ExecBatch exec_batch(*batch);
      exec_batch.values.emplace_back(count);
      outputs_[0]->InputReceived(this, static_cast<int>(count), std::move(exec_batch));
      count++;
    }

    outputs_[0]->InputFinished(this, static_cast<int>(count));
    finished_.MarkFinished();
    return Status::OK();
  }

  SortOptions sort_options_;
  std::vector<std::shared_ptr<RecordBatch>> batches_;
  AtomicCounter input_counter_;
  Future<> finished_;
};

ExecFactoryRegistry::AddOnLoad kRegisterOrderBy("order_by", OrderByNode::Make);

}  // namespace compute
}  // namespace arrow
