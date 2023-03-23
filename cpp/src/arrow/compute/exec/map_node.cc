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

#include "arrow/compute/exec/map_node.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {
namespace compute {

MapNode::MapNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                 std::shared_ptr<Schema> output_schema)
    : ExecNode(plan, std::move(inputs), /*input_labels=*/{"target"},
               std::move(output_schema)),
      TracedNode(this) {}

Status MapNode::InputFinished(ExecNode* input, int total_batches) {
  DCHECK_EQ(input, inputs_[0]);
  EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
  ARROW_RETURN_NOT_OK(output_->InputFinished(this, total_batches));
  if (input_counter_.SetTotal(total_batches)) {
    this->Finish();
  }
  return Status::OK();
}

// Right now this assumes the map operation will always maintain ordering.  This
// may change in the future but is true for the current map nodes (filter/project)
const Ordering& MapNode::ordering() const { return inputs_[0]->ordering(); }

Status MapNode::StartProducing() {
  NoteStartProducing(ToStringExtra());
  return Status::OK();
}

void MapNode::PauseProducing(ExecNode* output, int32_t counter) {
  inputs_[0]->PauseProducing(this, counter);
}

void MapNode::ResumeProducing(ExecNode* output, int32_t counter) {
  inputs_[0]->ResumeProducing(this, counter);
}

Status MapNode::StopProducingImpl() { return Status::OK(); }

Status MapNode::InputReceived(ExecNode* input, ExecBatch batch) {
  auto scope = TraceInputReceived(batch);
  DCHECK_EQ(input, inputs_[0]);
  compute::Expression guarantee = batch.guarantee;
  int64_t index = batch.index;
  ARROW_ASSIGN_OR_RAISE(auto output_batch, ProcessBatch(std::move(batch)));
  output_batch.guarantee = guarantee;
  output_batch.index = index;
  ARROW_RETURN_NOT_OK(output_->InputReceived(this, std::move(output_batch)));
  if (input_counter_.Increment()) {
    this->Finish();
  }
  return Status::OK();
}

void MapNode::Finish() {}

}  // namespace compute
}  // namespace arrow
