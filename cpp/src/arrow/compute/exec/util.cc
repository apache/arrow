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

#include "arrow/compute/exec/util.h"

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/table.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace compute {

Status ValidateExecNodeInputs(ExecPlan* plan, const std::vector<ExecNode*>& inputs,
                              int expected_num_inputs, const char* kind_name) {
  if (static_cast<int>(inputs.size()) != expected_num_inputs) {
    return Status::Invalid(kind_name, " requires ", expected_num_inputs,
                           " inputs but got ", inputs.size());
  }

  for (auto input : inputs) {
    if (input->plan() != plan) {
      return Status::Invalid("Constructing a ", kind_name,
                             " node in a different plan from its input");
    }
  }

  return Status::OK();
}

Result<std::shared_ptr<Table>> TableFromExecBatches(
    const std::shared_ptr<Schema>& schema, const std::vector<ExecBatch>& exec_batches) {
  RecordBatchVector batches;
  for (const auto& batch : exec_batches) {
    ARROW_ASSIGN_OR_RAISE(auto rb, batch.ToRecordBatch(schema));
    batches.push_back(std::move(rb));
  }
  return Table::FromRecordBatches(schema, batches);
}

size_t ThreadIndexer::operator()() {
  auto id = std::this_thread::get_id();

  auto guard = mutex_.Lock();  // acquire the lock
  const auto& id_index = *id_to_index_.emplace(id, id_to_index_.size()).first;

  return Check(id_index.second);
}

size_t ThreadIndexer::Capacity() {
  static size_t max_size = GetCpuThreadPoolCapacity() + io::GetIOThreadPoolCapacity() + 1;
  return max_size;
}

size_t ThreadIndexer::Check(size_t thread_index) {
  DCHECK_LT(thread_index, Capacity())
      << "thread index " << thread_index << " is out of range [0, " << Capacity() << ")";

  return thread_index;
}

Status TableSinkNodeConsumer::Init(const std::shared_ptr<Schema>& schema,
                                   BackpressureControl* backpressure_control,
                                   ExecPlan* plan) {
  // If the user is collecting into a table then backpressure is meaningless
  ARROW_UNUSED(backpressure_control);
  schema_ = schema;
  return Status::OK();
}

Status TableSinkNodeConsumer::Consume(ExecBatch batch) {
  auto guard = consume_mutex_.Lock();
  ARROW_ASSIGN_OR_RAISE(auto rb, batch.ToRecordBatch(schema_, pool_));
  batches_.push_back(std::move(rb));
  return Status::OK();
}

Future<> TableSinkNodeConsumer::Finish() {
  ARROW_ASSIGN_OR_RAISE(*out_, Table::FromRecordBatches(schema_, batches_));
  return Status::OK();
}

[[nodiscard]] ::arrow::internal::tracing::Scope TracedNode::TraceStartProducing(
    std::string extra_details) const {
  std::string node_kind(node_->kind_name());
  util::tracing::Span span;
  return START_SCOPED_SPAN(
      span, node_kind + "::StartProducing",
      {{"node.details", extra_details}, {"node.label", node_->label()}});
}

void TracedNode::NoteStartProducing(std::string extra_details) const {
  std::string node_kind(node_->kind_name());
  EVENT_ON_CURRENT_SPAN(node_kind + "::StartProducing", {{"node.details", extra_details},
                                                         {"node.label", node_->label()}});
}

[[nodiscard]] ::arrow::internal::tracing::Scope TracedNode::TraceInputReceived(
    const ExecBatch& batch) const {
  std::string node_kind(node_->kind_name());
  util::tracing::Span span;
  return START_SCOPED_SPAN(
      span, node_kind + "::InputReceived",
      {{"node.label", node_->label()}, {"node.batch_length", batch.length}});
}

void TracedNode::NoteInputReceived(const ExecBatch& batch) const {
  std::string node_kind(node_->kind_name());
  EVENT_ON_CURRENT_SPAN(
      node_kind + "::InputReceived",
      {{"node.label", node_->label()}, {"node.batch_length", batch.length}});
}

[[nodiscard]] ::arrow::internal::tracing::Scope TracedNode::TraceFinish() const {
  std::string node_kind(node_->kind_name());
  util::tracing::Span span;
  return START_SCOPED_SPAN(span, node_kind + "::Finish",
                           {{"node.label", node_->label()}});
}

}  // namespace compute
}  // namespace arrow
