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

#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <utility>
#include <vector>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/util.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;

using compute::TakeOptions;

namespace acero {
namespace {

class OrderByNode : public ExecNode, public TracedNode {
 public:
  OrderByNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
              std::shared_ptr<Schema> output_schema, Ordering new_ordering)
      : ExecNode(plan, std::move(inputs), {"input"}, std::move(output_schema)),
        TracedNode(this),
        ordering_(std::move(new_ordering)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "FetchNode"));

    const auto& order_options = checked_cast<const OrderByNodeOptions&>(options);

    if (order_options.ordering.is_implicit() || order_options.ordering.is_unordered()) {
      return Status::Invalid("`ordering` must be an explicit non-empty ordering");
    }

    std::shared_ptr<Schema> output_schema = inputs[0]->output_schema();
    return plan->EmplaceNode<OrderByNode>(
        plan, std::move(inputs), std::move(output_schema), order_options.ordering);
  }

  const char* kind_name() const override { return "OrderByNode"; }

  const Ordering& ordering() const override { return ordering_; }

  Status InputFinished(ExecNode* input, int total_batches) override {
    DCHECK_EQ(input, inputs_[0]);
    EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
    // We can't send InputFinished downstream because we might change the # of batches
    // when we sort it.  So that happens later in DoFinish
    if (counter_.SetTotal(total_batches)) {
      return DoFinish();
    }
    return Status::OK();
  }

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra());
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->PauseProducing(this, counter);
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->ResumeProducing(this, counter);
  }

  Status StopProducingImpl() override { return Status::OK(); }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    auto scope = TraceInputReceived(batch);
    DCHECK_EQ(input, inputs_[0]);

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> record_batch,
                          batch.ToRecordBatch(output_schema_));

    {
      std::lock_guard lk(mutex_);
      accumulation_queue_.push_back(std::move(record_batch));
    }

    if (counter_.Increment()) {
      return DoFinish();
    }
    return Status::OK();
  }

  Status DoFinish() {
    ARROW_ASSIGN_OR_RAISE(
        auto table,
        Table::FromRecordBatches(output_schema_, std::move(accumulation_queue_)));
    SortOptions sort_options(ordering_.sort_keys(), ordering_.null_placement());
    ExecContext* ctx = plan_->query_context()->exec_context();
    ARROW_ASSIGN_OR_RAISE(auto indices, SortIndices(table, sort_options, ctx));
    ARROW_ASSIGN_OR_RAISE(Datum sorted,
                          Take(table, indices, TakeOptions::NoBoundsCheck(), ctx));
    const std::shared_ptr<Table>& sorted_table = sorted.table();
    TableBatchReader reader(*sorted_table);
    reader.set_chunksize(ExecPlan::kMaxBatchSize);
    int batch_index = 0;
    while (true) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> next, reader.Next());
      if (!next) {
        return output_->InputFinished(this, batch_index);
      }
      int index = batch_index++;
      plan_->query_context()->ScheduleTask(
          [this, batch = std::move(next), index]() mutable {
            ExecBatch exec_batch(*batch);
            exec_batch.index = index;
            return output_->InputReceived(this, std::move(exec_batch));
          },
          "OrderByNode::ProcessBatch");
    }
  }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
    std::stringstream ss;
    ss << "ordering=" << ordering_.ToString();
    return ss.str();
  }

 private:
  AtomicCounter counter_;
  Ordering ordering_;
  std::vector<std::shared_ptr<RecordBatch>> accumulation_queue_;
  std::mutex mutex_;
};

}  // namespace

namespace internal {

void RegisterOrderByNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(
      registry->AddFactory(std::string(OrderByNodeOptions::kName), OrderByNode::Make));
}

}  // namespace internal
}  // namespace acero
}  // namespace arrow
