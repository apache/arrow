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

#include "arrow/engine/test_util.h"

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/engine/exec_plan.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"

namespace arrow {

using internal::Executor;

namespace engine {
namespace {

// TODO expose this as `static ValueDescr::FromSchemaColumns`?
std::vector<ValueDescr> DescrFromSchemaColumns(const Schema& schema) {
  std::vector<ValueDescr> descr;
  descr.reserve(schema.num_fields());
  std::transform(schema.fields().begin(), schema.fields().end(),
                 std::back_inserter(descr), [](const std::shared_ptr<Field>& field) {
                   return ValueDescr::Array(field->type());
                 });
  return descr;
}

struct DummyNode : ExecNode {
  DummyNode(ExecPlan* plan, std::string label, int num_inputs, int num_outputs,
            StartProducingFunc start_producing, StopProducingFunc stop_producing)
      : ExecNode(plan, std::move(label)),
        num_inputs_(num_inputs),
        num_outputs_(num_outputs),
        start_producing_(std::move(start_producing)),
        stop_producing_(std::move(stop_producing)) {
    input_descrs_.assign(num_inputs, descr());
    output_descrs_.assign(num_outputs, descr());
  }

  const char* kind_name() override { return "RecordBatchReader"; }

  int num_inputs() const override { return num_inputs_; }

  int num_outputs() const override { return num_outputs_; }

  void InputReceived(int input_index, int seq_num, compute::ExecBatch batch) override {}

  void ErrorReceived(int input_index, Status error) override {}

  void InputFinished(int input_index, int seq_stop) override {}

  Status StartProducing() override {
    if (start_producing_) {
      RETURN_NOT_OK(start_producing_(this));
    }
    started_ = true;
    return Status::OK();
  }

  void StopProducing() override {
    if (started_) {
      started_ = false;
      for (const auto& input : inputs_) {
        input->StopProducing();
      }
      if (stop_producing_) {
        stop_producing_(this);
      }
    }
  }

 private:
  BatchDescr descr() const { return std::vector<ValueDescr>{ValueDescr(null())}; }

  int num_inputs_;
  int num_outputs_;
  StartProducingFunc start_producing_;
  StopProducingFunc stop_producing_;
  bool started_ = false;
};

struct RecordBatchReaderNode : ExecNode {
  RecordBatchReaderNode(ExecPlan* plan, std::string label,
                        std::shared_ptr<RecordBatchReader> reader, Executor* io_executor)
      : ExecNode(plan, std::move(label)),
        schema_(reader->schema()),
        reader_(std::move(reader)),
        io_executor_(io_executor) {
    output_descrs_.push_back(DescrFromSchemaColumns(*schema_));
  }

  RecordBatchReaderNode(ExecPlan* plan, std::string label, std::shared_ptr<Schema> schema,
                        RecordBatchGenerator generator, Executor* io_executor)
      : ExecNode(plan, std::move(label)),
        schema_(std::move(schema)),
        io_executor_(io_executor),
        generator_(std::move(generator)) {
    output_descrs_.push_back(DescrFromSchemaColumns(*schema_));
  }

  const char* kind_name() override { return "RecordBatchReader"; }

  int num_inputs() const override { return 0; }

  int num_outputs() const override { return 1; }

  void InputReceived(int input_index, int seq_num, compute::ExecBatch batch) override {}

  void ErrorReceived(int input_index, Status error) override {}

  void InputFinished(int input_index, int seq_stop) override {}

  Status StartProducing() override {
    next_batch_index_ = 0;
    if (!generator_) {
      auto it = MakePointerIterator(reader_.get());
      ARROW_ASSIGN_OR_RAISE(generator_,
                            MakeBackgroundGenerator(std::move(it), io_executor_));
    }
    GenerateOne(std::unique_lock<std::mutex>{mutex_});
    return Status::OK();
  }

  void StopProducing() override {
    std::unique_lock<std::mutex> lock(mutex_);
    generator_ = nullptr;  // null function
  }

  // TODO implement PauseProducing / ResumeProducing

 private:
  void GenerateOne(std::unique_lock<std::mutex>&& lock) {
    if (!generator_) {
      // Stopped
      return;
    }
    auto plan = plan_ref();
    auto fut = generator_();
    const auto batch_index = next_batch_index_++;

    lock.unlock();
    // TODO we want to transfer always here
    io_executor_->Transfer(std::move(fut))
        .AddCallback(
            [plan, batch_index, this](const Result<std::shared_ptr<RecordBatch>>& res) {
              std::unique_lock<std::mutex> lock(mutex_);
              DCHECK_EQ(outputs_.size(), 1);
              OutputNode* out = &outputs_[0];
              if (!res.ok()) {
                out->output->ErrorReceived(out->input_index, res.status());
                return;
              }
              const auto& batch = *res;
              if (IsIterationEnd(batch)) {
                lock.unlock();
                out->output->InputFinished(out->input_index, batch_index);
              } else {
                lock.unlock();
                out->output->InputReceived(out->input_index, batch_index,
                                           compute::ExecBatch(*batch));
                lock.lock();
                GenerateOne(std::move(lock));
              }
            });
  }

  const std::shared_ptr<Schema> schema_;
  const std::shared_ptr<RecordBatchReader> reader_;
  Executor* const io_executor_;

  std::mutex mutex_;
  RecordBatchGenerator generator_;
  int next_batch_index_;
};

struct RecordBatchCollectNodeImpl : public RecordBatchCollectNode {
  RecordBatchCollectNodeImpl(ExecPlan* plan, std::string label,
                             const std::shared_ptr<Schema>& schema)
      : RecordBatchCollectNode(plan, std::move(label)), schema_(schema) {
    input_descrs_.push_back(DescrFromSchemaColumns(*schema_));
  }

  RecordBatchGenerator generator() override { return generator_; }

  const char* kind_name() override { return "RecordBatchReader"; }

  int num_inputs() const override { return 1; }

  int num_outputs() const override { return 0; }

  Status StartProducing() override {
    num_received_ = 0;
    num_emitted_ = 0;
    emit_stop_ = -1;
    stopped_ = false;
    producer_.emplace(generator_.producer());
    return Status::OK();
  }

  void StopProducing() override {
    std::unique_lock<std::mutex> lock(mutex_);
    StopProducing(&lock);
  }

  void InputReceived(int input_index, int seq_num,
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
      StopProducing(&lock);
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

  void ErrorReceived(int input_index, Status error) override {
    // XXX do we care about properly sequencing the error?
    producer_->Push(std::move(error));
    StopProducing();
  }

  void InputFinished(int input_index, int seq_stop) override {
    std::unique_lock<std::mutex> lock(mutex_);
    DCHECK_GE(seq_stop, static_cast<int>(received_batches_.size()));
    received_batches_.reserve(seq_stop);
    emit_stop_ = seq_stop;
    if (emit_stop_ == num_received_) {
      DCHECK_EQ(emit_stop_, num_emitted_);
      StopProducing(&lock);
    }
  }

 private:
  void StopProducing(std::unique_lock<std::mutex>* lock) {
    if (!stopped_) {
      stopped_ = true;
      producer_->Close();
      inputs_[0]->StopProducing();
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
  auto ptr =
      new RecordBatchReaderNode(plan, std::move(label), std::move(reader), io_executor);
  plan->AddNode(std::unique_ptr<ExecNode>{ptr});
  return ptr;
}

ExecNode* MakeRecordBatchReaderNode(ExecPlan* plan, std::string label,
                                    std::shared_ptr<Schema> schema,
                                    RecordBatchGenerator generator,
                                    ::arrow::internal::Executor* io_executor) {
  auto ptr = new RecordBatchReaderNode(plan, std::move(label), std::move(schema),
                                       std::move(generator), io_executor);
  plan->AddNode(std::unique_ptr<ExecNode>{ptr});
  return ptr;
}

ExecNode* MakeDummyNode(ExecPlan* plan, std::string label, int num_inputs,
                        int num_outputs, StartProducingFunc start_producing,
                        StopProducingFunc stop_producing) {
  auto ptr = new DummyNode(plan, std::move(label), num_inputs, num_outputs,
                           std::move(start_producing), std::move(stop_producing));
  plan->AddNode(std::unique_ptr<ExecNode>{ptr});
  return ptr;
}

RecordBatchCollectNode* MakeRecordBatchCollectNode(
    ExecPlan* plan, std::string label, const std::shared_ptr<Schema>& schema) {
  auto ptr = new RecordBatchCollectNodeImpl(plan, std::move(label), schema);
  plan->AddNode(std::unique_ptr<ExecNode>{ptr});
  return ptr;
}

}  // namespace engine
}  // namespace arrow
