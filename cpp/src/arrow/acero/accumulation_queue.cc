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

#include "arrow/acero/accumulation_queue.h"

#include <iterator>
#include <mutex>
#include <queue>
#include <vector>

#include "arrow/acero/query_context.h"
#include "arrow/compute/exec.h"
#include "arrow/util/logging_internal.h"

namespace arrow {
namespace acero {
namespace util {
using arrow::compute::ExecBatch;
AccumulationQueue::AccumulationQueue(AccumulationQueue&& that) {
  this->batches_ = std::move(that.batches_);
  this->row_count_ = that.row_count_;
  that.Clear();
}

AccumulationQueue& AccumulationQueue::operator=(AccumulationQueue&& that) {
  this->batches_ = std::move(that.batches_);
  this->row_count_ = that.row_count_;
  that.Clear();
  return *this;
}

void AccumulationQueue::Concatenate(AccumulationQueue&& that) {
  this->batches_.reserve(this->batches_.size() + that.batches_.size());
  std::move(that.batches_.begin(), that.batches_.end(),
            std::back_inserter(this->batches_));
  this->row_count_ += that.row_count_;
  that.Clear();
}

void AccumulationQueue::InsertBatch(ExecBatch batch) {
  row_count_ += batch.length;
  batches_.emplace_back(std::move(batch));
}

void AccumulationQueue::Clear() {
  row_count_ = 0;
  batches_.clear();
}

ExecBatch& AccumulationQueue::operator[](size_t i) { return batches_[i]; }

namespace {

struct LowestBatchIndexAtTop {
  bool operator()(const ExecBatch& left, const ExecBatch& right) const {
    return left.index > right.index;
  }
};

class SequencingQueueImpl : public SequencingQueue {
 public:
  explicit SequencingQueueImpl(Processor* processor) : processor_(processor) {}

  Status InsertBatch(ExecBatch batch) override {
    DCHECK_NE(::arrow::compute::kUnsequencedIndex, batch.index);
    std::unique_lock lk(mutex_);
    if (batch.index == next_index_) {
      return DeliverNextUnlocked(std::move(batch), std::move(lk));
    }
    queue_.emplace(std::move(batch));
    return Status::OK();
  }

 private:
  Status DeliverNextUnlocked(ExecBatch batch, std::unique_lock<std::mutex>&& lk) {
    // Should be able to detect and avoid this at plan construction
    DCHECK_NE(batch.index, ::arrow::compute::kUnsequencedIndex)
        << "attempt to use a sequencing queue on an unsequenced stream of batches";
    std::vector<Task> tasks;
    next_index_++;
    ARROW_ASSIGN_OR_RAISE(std::optional<Task> this_task,
                          processor_->Process(std::move(batch)));
    while (!queue_.empty() && next_index_ == queue_.top().index) {
      ARROW_ASSIGN_OR_RAISE(std::optional<Task> task, processor_->Process(queue_.top()));
      if (task) {
        tasks.push_back(std::move(*task));
      }
      queue_.pop();
      next_index_++;
    }
    lk.unlock();
    // Schedule tasks for stale items
    for (auto& task : tasks) {
      processor_->Schedule(std::move(task));
    }
    // Run the current item immediately
    if (this_task) {
      ARROW_RETURN_NOT_OK(std::move(*this_task)());
    }
    return Status::OK();
  }

  Processor* processor_;

  std::priority_queue<ExecBatch, std::vector<ExecBatch>, LowestBatchIndexAtTop> queue_;
  int next_index_ = 0;
  std::mutex mutex_;
};

class SerialSequencingQueueImpl : public SerialSequencingQueue {
 public:
  explicit SerialSequencingQueueImpl(Processor* processor) : processor_(processor) {}

  Status InsertBatch(ExecBatch batch) override {
    DCHECK_NE(::arrow::compute::kUnsequencedIndex, batch.index);
    std::unique_lock lk(mutex_);
    queue_.push(std::move(batch));
    if (queue_.top().index == next_index_ && !is_processing_) {
      is_processing_ = true;
      return DoProcess(std::move(lk));
    }
    return Status::OK();
  }

 private:
  Status DoProcess(std::unique_lock<std::mutex>&& lk) {
    while (!queue_.empty() && queue_.top().index == next_index_) {
      ExecBatch next(queue_.top());
      queue_.pop();
      next_index_++;
      lk.unlock();
      // ARROW_RETURN_NOT_OK may return early here.  In that case  is_processing_ will
      // never switch to false so no other threads can process but that should be ok
      // since we failed anyways.  It is important however, that we do not hold the lock.
      ARROW_RETURN_NOT_OK(processor_->Process(std::move(next)));
      lk.lock();
    }
    is_processing_ = false;
    return Status::OK();
  }

  Processor* processor_;

  std::mutex mutex_;
  std::priority_queue<ExecBatch, std::vector<ExecBatch>, LowestBatchIndexAtTop> queue_;
  int next_index_ = 0;
  bool is_processing_ = false;
};

class BackpressureProcessor : public SerialSequencingQueue::Processor {
 private:
  struct DoHandle {
    explicit DoHandle(BackpressureProcessor& queue)
        : queue_(queue), start_size_(queue_.SizeUnlocked()) {}

    ~DoHandle() {
      // unsynced access is safe since DoHandle is internally only used when the
      // lock is held
      size_t end_size = queue_.SizeUnlocked();
      queue_.handler_.Handle(start_size_, end_size);
    }

    BackpressureProcessor& queue_;
    size_t start_size_;
  };

 public:
  explicit BackpressureProcessor(SerialSequencingQueue::Processor* processor,
                                 BackpressureHandler handler, ExecPlan* plan,
                                 bool requires_io = true)
      : processor_(processor),
        handler_(std::move(handler)),
        plan_(plan),
        requires_io_(requires_io) {}

  void Schedule() {
    if (requires_io_) {
      plan_->query_context()->ScheduleIOTask([this]() { return DoProcess(); },
                                             "BackpressureProcessor::DoProcessIO");
    } else {
      plan_->query_context()->ScheduleTask([this]() { return DoProcess(); },
                                           "BackpressureProcessor::DoProcess");
    }
  }

  Status Process(ExecBatch batch) override {
    std::unique_lock lk(mutex_);
    {
      DoHandle do_handle(*this);
      sequenced_queue_.push(batch);
    }
    if (!is_processing_) {
      is_processing_ = true;
      Schedule();
    }
    return Status::OK();
  }

 private:
  Status DoProcess() {
    std::unique_lock lk(mutex_);
    while (!sequenced_queue_.empty()) {
      ExecBatch next(sequenced_queue_.front());
      {
        DoHandle do_handle(*this);
        sequenced_queue_.pop();
      }
      lk.unlock();
      ARROW_RETURN_NOT_OK(processor_->Process(std::move(next)));
      lk.lock();
    }
    is_processing_ = false;
    return Status::OK();
  }
  size_t SizeUnlocked() const { return sequenced_queue_.size(); }

  Processor* processor_;
  BackpressureHandler handler_;
  ExecPlan* plan_;
  bool requires_io_;
  std::mutex mutex_;
  std::queue<ExecBatch> sequenced_queue_;
  bool is_processing_ = false;
};

}  // namespace

std::unique_ptr<SequencingQueue> SequencingQueue::Make(Processor* processor) {
  return std::make_unique<SequencingQueueImpl>(processor);
}

std::unique_ptr<SerialSequencingQueue> SerialSequencingQueue::Make(Processor* processor) {
  return std::make_unique<SerialSequencingQueueImpl>(processor);
}

std::unique_ptr<SerialSequencingQueue::Processor>
SerialSequencingQueue::Processor::MakeBackpressureWrapper(Processor* processor,
                                                          BackpressureHandler handler,
                                                          ExecPlan* plan,
                                                          bool requires_io) {
  return std::make_unique<util::BackpressureProcessor>(processor, std::move(handler),
                                                       plan);
}

}  // namespace util
}  // namespace acero
}  // namespace arrow
