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

#include "arrow/compute/exec/accumulation_queue.h"

#include "arrow/util/future.h"
#include "arrow/util/logging.h"

#include <iterator>
#include <mutex>
#include <optional>
#include <queue>

namespace arrow {
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

struct ExecBatchCmp {
  bool operator()(const ExecBatch& left, const ExecBatch& right) {
    return left.index > right.index;
  }
};

class OrderedAccumulationQueueImpl : public OrderedAccumulationQueue {
 public:
  OrderedAccumulationQueueImpl(TaskFactoryCallback create_task, ScheduleCallback schedule)
      : create_task_(std::move(create_task)), schedule_(std::move(schedule)) {}

  ~OrderedAccumulationQueueImpl() override = default;

  Status InsertBatch(ExecBatch batch) override {
    DCHECK_GE(batch.index, 0);
    std::unique_lock<std::mutex> lk(mutex_);
    if (!processing_ && batch.index == next_index_) {
      std::vector<ExecBatch> next_batch = PopUnlocked(std::move(batch));
      processing_ = true;
      lk.unlock();
      return Deliver(std::move(next_batch));
    }
    batches_.push(std::move(batch));
    return Status::OK();
  }

  Status CheckDrained() const override {
    if (!batches_.empty()) {
      return Status::UnknownError(
          "Ordered accumulation queue has data remaining after finish");
    }
    return Status::OK();
  }

 private:
  std::vector<ExecBatch> PopUnlocked(std::optional<ExecBatch> batch) {
    std::vector<ExecBatch> popped;
    if (batch.has_value()) {
      popped.push_back(std::move(*batch));
      next_index_++;
    }
    while (!batches_.empty() && batches_.top().index == next_index_) {
      popped.push_back(std::move(batches_.top()));
      batches_.pop();
      next_index_++;
    }
    return popped;
  }

  Status Deliver(std::vector<ExecBatch> batches) {
    ARROW_ASSIGN_OR_RAISE(Task task, create_task_(std::move(batches)));
    Task wrapped_task = [this, task = std::move(task)] {
      ARROW_RETURN_NOT_OK(task());
      std::unique_lock<std::mutex> lk(mutex_);
      if (!batches_.empty() && batches_.top().index == next_index_) {
        std::vector<ExecBatch> next_batches = PopUnlocked(std::nullopt);
        lk.unlock();
        ARROW_RETURN_NOT_OK(Deliver(std::move(next_batches)));
      } else {
        processing_ = false;
      }
      return Status::OK();
    };
    return schedule_(std::move(wrapped_task));
  }

  TaskFactoryCallback create_task_;
  ScheduleCallback schedule_;
  std::priority_queue<ExecBatch, std::vector<ExecBatch>, ExecBatchCmp> batches_;
  int next_index_ = 0;
  bool processing_ = false;
  std::mutex mutex_;
};

std::unique_ptr<OrderedAccumulationQueue> OrderedAccumulationQueue::Make(
    TaskFactoryCallback create_task, ScheduleCallback schedule) {
  return std::make_unique<OrderedAccumulationQueueImpl>(std::move(create_task),
                                                        std::move(schedule));
}

}  // namespace util
}  // namespace arrow
