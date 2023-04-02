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

#include <sstream>

#include "arrow/acero/accumulation_queue.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/map_node.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/util.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;

namespace acero {
namespace {

class FetchCounter {
 public:
  struct Page {
    int64_t to_skip;
    int64_t to_send;
    bool ended;
  };

  FetchCounter(int64_t rows_to_skip, int64_t rows_to_send)
      : rows_to_send_(rows_to_send), rows_to_skip_(rows_to_skip) {}

  Page NextPage(const ExecBatch& batch) {
    int64_t rows_in_batch_to_skip = 0;
    if (rows_to_skip_ > 0) {
      rows_in_batch_to_skip = std::min(rows_to_skip_, batch.length);
      rows_to_skip_ -= rows_in_batch_to_skip;
    }

    int64_t rows_in_batch_to_send = 0;
    if (rows_to_send_ > 0) {
      rows_in_batch_to_send =
          std::min(rows_to_send_, batch.length - rows_in_batch_to_skip);
      rows_to_send_ -= rows_in_batch_to_send;
    }
    return {rows_in_batch_to_skip, rows_in_batch_to_send, rows_to_send_ == 0};
  }

 private:
  int64_t rows_to_send_;
  int64_t rows_to_skip_;
};

class FetchNode : public ExecNode, public TracedNode, util::SequencingQueue::Processor {
 public:
  FetchNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
            std::shared_ptr<Schema> output_schema, int64_t offset, int64_t count)
      : ExecNode(plan, std::move(inputs), {"input"}, std::move(output_schema)),
        TracedNode(this),
        offset_(offset),
        count_(count),
        fetch_counter_(offset, count),
        sequencing_queue_(util::SequencingQueue::Make(this)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "FetchNode"));

    const auto& fetch_options = checked_cast<const FetchNodeOptions&>(options);

    int64_t offset = fetch_options.offset;
    int64_t count = fetch_options.count;

    if (offset < 0) {
      return Status::Invalid("`offset` must be non-negative");
    }
    if (count < 0) {
      return Status::Invalid("`count` must be non-negative");
    }

    std::shared_ptr<Schema> output_schema = inputs[0]->output_schema();
    return plan->EmplaceNode<FetchNode>(plan, std::move(inputs), std::move(output_schema),
                                        offset, count);
  }

  const char* kind_name() const override { return "FetchNode"; }

  const Ordering& ordering() const override { return inputs_[0]->ordering(); }

  Status InputFinished(ExecNode* input, int total_batches) override {
    DCHECK_EQ(input, inputs_[0]);
    EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
    // Normally we will finish in InputFinished because we sent count_ rows. However, it
    // is possible that the input does not contain count_ rows and so we have to end from
    // here
    if (in_batch_counter_.SetTotal(total_batches)) {
      if (!finished_) {
        finished_ = true;
        ARROW_RETURN_NOT_OK(inputs_[0]->StopProducing());
        ARROW_RETURN_NOT_OK(output_->InputFinished(this, out_batch_count_));
      }
    }
    return Status::OK();
  }

  Status Validate() const override {
    ARROW_RETURN_NOT_OK(ExecNode::Validate());
    if (inputs_[0]->ordering().is_unordered()) {
      return Status::Invalid(
          "Fetch node's input has no meaningful ordering and so limit/offset will be "
          "non-deterministic.  Please establish order in some way (e.g. by inserting an "
          "order_by node)");
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

    return sequencing_queue_->InsertBatch(std::move(batch));
  }

  Result<std::optional<util::SequencingQueue::Task>> Process(ExecBatch batch) override {
    if (finished_) {
      return std::nullopt;
    }
    FetchCounter::Page page = fetch_counter_.NextPage(batch);
    std::optional<util::SequencingQueue::Task> task_or_none;
    if (page.to_send > 0) {
      int new_index = out_batch_count_++;
      task_or_none = [this, to_send = page.to_send, to_skip = page.to_skip, new_index,
                      batch = std::move(batch)]() mutable {
        ExecBatch batch_to_send = std::move(batch);
        if (to_skip > 0 || to_send < batch_to_send.length) {
          batch_to_send = batch_to_send.Slice(to_skip, to_send);
        }
        batch_to_send.index = new_index;
        return output_->InputReceived(this, std::move(batch_to_send));
      };
    }
    // In the in_batch_counter_ case we've run out of data to process (count_ was
    // greater than the total # of non-skipped rows)  In the page.ended case we've
    // just hit our desired output count
    if (in_batch_counter_.Increment() || (page.ended && !finished_)) {
      finished_ = true;
      ARROW_RETURN_NOT_OK(inputs_[0]->StopProducing());
      ARROW_RETURN_NOT_OK(output_->InputFinished(this, out_batch_count_));
    }
    return task_or_none;
  }

  void Schedule(util::SequencingQueue::Task task) override {
    plan_->query_context()->ScheduleTask(std::move(task), "FetchNode::ProcessBatch");
  }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
    std::stringstream ss;
    ss << "offset=" << offset_ << " count=" << count_;
    return ss.str();
  }

 private:
  bool finished_ = false;
  int64_t offset_;
  int64_t count_;
  AtomicCounter in_batch_counter_;
  int32_t out_batch_count_ = 0;
  FetchCounter fetch_counter_;
  std::unique_ptr<util::SequencingQueue> sequencing_queue_;
};

}  // namespace

namespace internal {

void RegisterFetchNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory(std::string(FetchNodeOptions::kName), FetchNode::Make));
}

}  // namespace internal
}  // namespace acero
}  // namespace arrow
