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

#include "arrow/engine/exec_plan.h"

#include "arrow/compute/exec.h"
#include "arrow/engine/query_plan.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::checked_cast;

namespace engine {

ExecNode::InputBatch::InputBatch(std::vector<util::optional<compute::ExecBatch>> batches)
    : batches(std::move(batches)), ready_fut(Future<>::Make()) {}

ExecNode::ExecNode(ExecPlan* plan, const QueryNode* query_node)
    : plan_(plan),
      query_node_(query_node),
      num_inputs_(query_node->num_inputs()),
      finished_fut_(Future<>::Make()) {}

ExecNode::~ExecNode() = default;

void ExecNode::ReserveBatches(int32_t num_batches) {
  // Should be called with mutex locked
  if (static_cast<size_t>(num_batches) > input_batches_.size()) {
    input_batches_.resize(num_batches);
  }
}

ExecNode::InputBatch* ExecNode::EnsureBatch(int32_t batch_index) {
  // Should be called with mutex locked
  if (input_batches_[batch_index] == nullptr) {
    input_batches_[batch_index].reset(
        new InputBatch{std::vector<util::optional<compute::ExecBatch>>(num_inputs_)});
  }
  return input_batches_[batch_index].get();
}

Status ExecNode::InputReceived(int32_t input_index, int32_t batch_index,
                               compute::ExecBatch batch) {
  auto lock = mutex_.Lock();

  if (input_index >= num_inputs_) {
    return Status::Invalid("Invalid input index");
  }
  if (finish_at_ >= 0 && batch_index >= finish_at_) {
    return Status::Invalid("Input batch index out of bounds");
  }

  ReserveBatches(batch_index + 1);
  auto* input_batch = EnsureBatch(batch_index);

  if (input_batch->batches[input_index].has_value()) {
    return Status::Invalid("Batch #", batch_index, " for input #", input_index,
                           " already received");
  }
  input_batch->batches[input_index] = std::move(batch);
  if (++input_batch->num_ready == num_inputs_) {
    input_batch->ready_fut.MarkFinished();
  }
  return Status::OK();
}

Status ExecNode::InputFinished(int32_t num_batches) {
  auto lock = mutex_.Lock();

  if (finish_at_ >= 0) {
    return Status::Invalid("InputFinished already called");
  }
  finish_at_ = num_batches;
  ReserveBatches(num_batches);

  std::vector<Future<>> batch_futures;
  for (int32_t i = 0; i < num_batches; ++i) {
    auto* input_batch = EnsureBatch(i);
    batch_futures[i] = input_batch->ready_fut;
  }

  // TODO lifetime
  AllComplete(std::move(batch_futures))
      .AddCallback([this](const Result<detail::Empty>& res) {
        finished_fut_.MarkFinished(res.status());
      });
  return Status::OK();
}

Future<> ExecNode::RunAsync(int32_t batch_index, internal::Executor* executor) {
  auto lock = mutex_.Lock();

  ReserveBatches(batch_index + 1);
  auto* input_batch = EnsureBatch(batch_index);

  // TODO lifetime (take strong ref to ExecPlan?)
  return executor->Transfer(input_batch->ready_fut)
      .Then([this, batch_index](...) -> Status { return RunSyncInternal(batch_index); });
}

Future<> ExecNode::FinishAsync(internal::Executor* executor) {
  auto lock = mutex_.Lock();

  // TODO lifetime
  return executor->Transfer(finished_fut_).Then([this](...) -> Status {
    return FinishSyncInternal();
  });
}

}  // namespace engine
}  // namespace arrow
