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

#include "arrow/compute/exec/test_util.h"

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"

namespace arrow {

using internal::Executor;

namespace compute {
namespace {

// TODO expose this as `static ValueDescr::FromSchemaColumns`?
std::vector<ValueDescr> DescrFromSchemaColumns(const Schema& schema) {
  std::vector<ValueDescr> descr(schema.num_fields());
  std::transform(schema.fields().begin(), schema.fields().end(), descr.begin(),
                 [](const std::shared_ptr<Field>& field) {
                   return ValueDescr::Array(field->type());
                 });
  return descr;
}

struct DummyNode : ExecNode {
  DummyNode(ExecPlan* plan, std::string label, int num_inputs, int num_outputs,
            StartProducingFunc start_producing, StopProducingFunc stop_producing)
      : ExecNode(plan, std::move(label), std::vector<BatchDescr>(num_inputs, descr()), {},
                 descr(), num_outputs),
        start_producing_(std::move(start_producing)),
        stop_producing_(std::move(stop_producing)) {
    for (int i = 0; i < num_inputs; ++i) {
      input_labels_.push_back(std::to_string(i));
    }
  }

  const char* kind_name() override { return "Dummy"; }

  void InputReceived(ExecNode* input, int seq_num, compute::ExecBatch batch) override {}

  void ErrorReceived(ExecNode* input, Status error) override {}

  void InputFinished(ExecNode* input, int seq_stop) override {}

  Status StartProducing() override {
    if (start_producing_) {
      RETURN_NOT_OK(start_producing_(this));
    }
    started_ = true;
    return Status::OK();
  }

  void PauseProducing(ExecNode* output) override {
    ASSERT_GE(num_outputs(), 0) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
  }

  void ResumeProducing(ExecNode* output) override {
    ASSERT_GE(num_outputs(), 0) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
  }

  void StopProducing(ExecNode* output) override {
    ASSERT_GE(num_outputs(), 0) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
    StopProducing();
  }

  void StopProducing() override {
    if (started_) {
      started_ = false;
      for (const auto& input : inputs_) {
        input->StopProducing(this);
      }
      if (stop_producing_) {
        stop_producing_(this);
      }
    }
  }

 private:
  void AssertIsOutput(ExecNode* output) {
    ASSERT_NE(std::find(outputs_.begin(), outputs_.end(), output), outputs_.end());
  }

  BatchDescr descr() const { return std::vector<ValueDescr>{ValueDescr(null())}; }

  StartProducingFunc start_producing_;
  StopProducingFunc stop_producing_;
  bool started_ = false;
};

struct RecordBatchReaderNode : ExecNode {
  RecordBatchReaderNode(ExecPlan* plan, std::string label,
                        std::shared_ptr<RecordBatchReader> reader, Executor* io_executor)
      : ExecNode(plan, std::move(label), {}, {},
                 DescrFromSchemaColumns(*reader->schema()), /*num_outputs=*/1),
        schema_(reader->schema()),
        reader_(std::move(reader)),
        io_executor_(io_executor) {}

  RecordBatchReaderNode(ExecPlan* plan, std::string label, std::shared_ptr<Schema> schema,
                        RecordBatchGenerator generator, Executor* io_executor)
      : ExecNode(plan, std::move(label), {}, {}, DescrFromSchemaColumns(*schema),
                 /*num_outputs=*/1),
        schema_(std::move(schema)),
        generator_(std::move(generator)),
        io_executor_(io_executor) {}

  const char* kind_name() override { return "RecordBatchReader"; }

  void InputReceived(ExecNode* input, int seq_num, compute::ExecBatch batch) override {}

  void ErrorReceived(ExecNode* input, Status error) override {}

  void InputFinished(ExecNode* input, int seq_stop) override {}

  Status StartProducing() override {
    next_batch_index_ = 0;
    if (!generator_) {
      auto it = MakeIteratorFromReader(reader_);
      ARROW_ASSIGN_OR_RAISE(generator_,
                            MakeBackgroundGenerator(std::move(it), io_executor_));
    }
    GenerateOne(std::unique_lock<std::mutex>{mutex_});
    return Status::OK();
  }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    ASSERT_EQ(output, outputs_[0]);
    std::unique_lock<std::mutex> lock(mutex_);
    generator_ = nullptr;  // null function
  }

  void StopProducing() override { StopProducing(outputs_[0]); }

 private:
  void GenerateOne(std::unique_lock<std::mutex>&& lock) {
    if (!generator_) {
      // Stopped
      return;
    }
    auto plan = this->plan()->shared_from_this();
    auto fut = generator_();
    const auto batch_index = next_batch_index_++;

    lock.unlock();
    // TODO we want to transfer always here
    io_executor_->Transfer(std::move(fut))
        .AddCallback(
            [plan, batch_index, this](const Result<std::shared_ptr<RecordBatch>>& res) {
              std::unique_lock<std::mutex> lock(mutex_);
              if (!res.ok()) {
                for (auto out : outputs_) {
                  out->ErrorReceived(this, res.status());
                }
                return;
              }
              const auto& batch = *res;
              if (IsIterationEnd(batch)) {
                lock.unlock();
                for (auto out : outputs_) {
                  out->InputFinished(this, batch_index);
                }
              } else {
                lock.unlock();
                for (auto out : outputs_) {
                  out->InputReceived(this, batch_index, compute::ExecBatch(*batch));
                }
                lock.lock();
                GenerateOne(std::move(lock));
              }
            });
  }

  std::mutex mutex_;
  const std::shared_ptr<Schema> schema_;
  const std::shared_ptr<RecordBatchReader> reader_;
  RecordBatchGenerator generator_;
  int next_batch_index_;

  Executor* const io_executor_;
};

struct RecordBatchCollectNodeImpl : public RecordBatchCollectNode {
  RecordBatchCollectNodeImpl(ExecPlan* plan, std::string label,
                             std::shared_ptr<Schema> schema)
      : RecordBatchCollectNode(plan, std::move(label), {DescrFromSchemaColumns(*schema)},
                               {"batches_to_collect"}, {}, 0),
        schema_(std::move(schema)) {}

  RecordBatchGenerator generator() override { return generator_; }

  const char* kind_name() override { return "RecordBatchReader"; }

  Status StartProducing() override {
    num_received_ = 0;
    num_emitted_ = 0;
    emit_stop_ = -1;
    stopped_ = false;
    producer_.emplace(generator_.producer());
    return Status::OK();
  }

  // sink nodes have no outputs from which to feel backpressure
  void ResumeProducing(ExecNode* output) override {
    FAIL() << "no outputs; this should never be called";
  }
  void PauseProducing(ExecNode* output) override {
    FAIL() << "no outputs; this should never be called";
  }
  void StopProducing(ExecNode* output) override {
    FAIL() << "no outputs; this should never be called";
  }

  void StopProducing() override {
    std::unique_lock<std::mutex> lock(mutex_);
    StopProducingUnlocked();
  }

  void InputReceived(ExecNode* input, int seq_num,
                     compute::ExecBatch exec_batch) override {
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopped_) {
      return;
    }
    auto maybe_batch = MakeBatch(std::move(exec_batch));
    if (!maybe_batch.ok()) {
      lock.unlock();
      producer_->Push(std::move(maybe_batch));
      return;
    }

    // TODO would be nice to factor this out in a ReorderQueue
    auto batch = *std::move(maybe_batch);
    if (seq_num <= static_cast<int>(received_batches_.size())) {
      received_batches_.resize(seq_num + 1, nullptr);
    }
    DCHECK_EQ(received_batches_[seq_num], nullptr);
    received_batches_[seq_num] = std::move(batch);
    ++num_received_;

    if (seq_num != num_emitted_) {
      // Cannot emit yet as there is a hole at `num_emitted_`
      DCHECK_GT(seq_num, num_emitted_);
      DCHECK_EQ(received_batches_[num_emitted_], nullptr);
      return;
    }
    if (num_received_ == emit_stop_) {
      StopProducingUnlocked();
    }

    // Emit batches in order as far as possible
    // First collect these batches, then unlock before producing.
    const auto seq_start = seq_num;
    while (seq_num < static_cast<int>(received_batches_.size()) &&
           received_batches_[seq_num] != nullptr) {
      ++seq_num;
    }
    DCHECK_GT(seq_num, seq_start);
    // By moving the values now, we make sure another thread won't emit the same values
    // below
    RecordBatchVector to_emit(
        std::make_move_iterator(received_batches_.begin() + seq_start),
        std::make_move_iterator(received_batches_.begin() + seq_num));

    lock.unlock();
    for (auto&& batch : to_emit) {
      producer_->Push(std::move(batch));
    }
    lock.lock();

    DCHECK_EQ(seq_start, num_emitted_);  // num_emitted_ wasn't bumped in the meantime
    num_emitted_ = seq_num;
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    // XXX do we care about properly sequencing the error?
    producer_->Push(std::move(error));
    std::unique_lock<std::mutex> lock(mutex_);
    StopProducingUnlocked();
  }

  void InputFinished(ExecNode* input, int seq_stop) override {
    std::unique_lock<std::mutex> lock(mutex_);
    DCHECK_GE(seq_stop, static_cast<int>(received_batches_.size()));
    received_batches_.reserve(seq_stop);
    emit_stop_ = seq_stop;
    if (emit_stop_ == num_received_) {
      DCHECK_EQ(emit_stop_, num_emitted_);
      StopProducingUnlocked();
    }
  }

 private:
  void StopProducingUnlocked() {
    if (!stopped_) {
      stopped_ = true;
      producer_->Close();
      inputs_[0]->StopProducing(this);
    }
  }

  // TODO factor this out as ExecBatch::ToRecordBatch()?
  Result<std::shared_ptr<RecordBatch>> MakeBatch(compute::ExecBatch&& exec_batch) {
    ArrayDataVector columns;
    columns.reserve(exec_batch.values.size());
    for (auto&& value : exec_batch.values) {
      if (!value.is_array()) {
        return Status::TypeError("Expected array input");
      }
      columns.push_back(std::move(value).array());
    }
    return RecordBatch::Make(schema_, exec_batch.length, std::move(columns));
  }

  const std::shared_ptr<Schema> schema_;

  std::mutex mutex_;
  RecordBatchVector received_batches_;
  int num_received_;
  int num_emitted_;
  int emit_stop_;
  bool stopped_;

  PushGenerator<std::shared_ptr<RecordBatch>> generator_;
  util::optional<PushGenerator<std::shared_ptr<RecordBatch>>::Producer> producer_;
};

}  // namespace

ExecNode* MakeRecordBatchReaderNode(ExecPlan* plan, std::string label,
                                    std::shared_ptr<RecordBatchReader> reader,
                                    Executor* io_executor) {
  return plan->EmplaceNode<RecordBatchReaderNode>(plan, std::move(label),
                                                  std::move(reader), io_executor);
}

ExecNode* MakeRecordBatchReaderNode(ExecPlan* plan, std::string label,
                                    std::shared_ptr<Schema> schema,
                                    RecordBatchGenerator generator,
                                    ::arrow::internal::Executor* io_executor) {
  return plan->EmplaceNode<RecordBatchReaderNode>(
      plan, std::move(label), std::move(schema), std::move(generator), io_executor);
}

ExecNode* MakeDummyNode(ExecPlan* plan, std::string label, int num_inputs,
                        int num_outputs, StartProducingFunc start_producing,
                        StopProducingFunc stop_producing) {
  return plan->EmplaceNode<DummyNode>(plan, std::move(label), num_inputs, num_outputs,
                                      std::move(start_producing),
                                      std::move(stop_producing));
}

RecordBatchCollectNode* MakeRecordBatchCollectNode(
    ExecPlan* plan, std::string label, const std::shared_ptr<Schema>& schema) {
  return internal::checked_cast<RecordBatchCollectNode*>(
      plan->EmplaceNode<RecordBatchCollectNodeImpl>(plan, std::move(label), schema));
}

}  // namespace compute
}  // namespace arrow
