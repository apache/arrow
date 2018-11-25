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

#include "arrow/util/task-group.h"

#include <condition_variable>
#include <cstdint>
#include <mutex>

#include "arrow/util/logging.h"
#include "arrow/util/thread-pool.h"

namespace arrow {
namespace internal {

////////////////////////////////////////////////////////////////////////
// Serial TaskGroup implementation

class SerialTaskGroup : public TaskGroup {
 public:
  void AppendReal(std::function<Status()> task) override {
    DCHECK(!finished_);
    if (status_.ok()) {
      status_ &= task();
    }
  }

  Status current_status() override { return status_; }

  Status Finish() override {
    if (!finished_) {
      finished_ = true;
      if (parent_) {
        parent_->status_ &= status_;
      }
    }
    return status_;
  }

  int parallelism() override { return 1; }

  std::shared_ptr<TaskGroup> MakeSubGroup() override {
    auto child = new SerialTaskGroup();
    child->parent_ = this;
    return std::shared_ptr<TaskGroup>(child);
  }

 protected:
  Status status_;
  bool finished_ = false;
  SerialTaskGroup* parent_ = nullptr;
};

////////////////////////////////////////////////////////////////////////
// Threaded TaskGroup implementation

class ThreadedTaskGroup : public TaskGroup {
 public:
  explicit ThreadedTaskGroup(ThreadPool* thread_pool) : thread_pool_(thread_pool) {}

  ~ThreadedTaskGroup() override {
    // Make sure all pending tasks are finished, so that dangling references
    // to this don't persist.
    ARROW_UNUSED(Finish());
  }

  void AppendReal(std::function<Status()> task) override {
    std::lock_guard<std::mutex> lock(mutex_);
    DCHECK(!finished_);

    if (status_.ok()) {
      ++nremaining_;
      status_ = thread_pool_->Spawn([&, task]() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (status_.ok()) {
          lock.unlock();
          // XXX what about exceptions?
          Status st = task();
          lock.lock();
          status_ &= st;
        }
        OneTaskDone();
      });
    }
  }

  Status current_status() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return status_;
  }

  Status Finish() override {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!finished_) {
      cv_.wait(lock, [&]() { return nremaining_ == 0; });
      // Current tasks may start other tasks, so only set this when done
      finished_ = true;
      if (parent_) {
        // Need to lock parent
        std::lock_guard<std::mutex> parent_lock(parent_->mutex_);
        parent_->OneTaskDone();
      }
    }
    return status_;
  }

  int parallelism() override { return thread_pool_->GetCapacity(); }

  std::shared_ptr<TaskGroup> MakeSubGroup() override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto child = new ThreadedTaskGroup(thread_pool_);
    child->parent_ = this;
    nremaining_++;
    return std::shared_ptr<TaskGroup>(child);
  }

 protected:
  void OneTaskDone() {
    // We are locked
    --nremaining_;
    DCHECK_GE(nremaining_, 0);
    if (nremaining_ == 0) {
      cv_.notify_one();
    }
  }

  ThreadPool* thread_pool_;
  std::mutex mutex_;
  std::condition_variable cv_;
  Status status_;
  bool finished_ = false;
  int32_t nremaining_ = 0;
  ThreadedTaskGroup* parent_ = nullptr;
};

std::shared_ptr<TaskGroup> TaskGroup::MakeSerial() {
  return std::shared_ptr<TaskGroup>(new SerialTaskGroup);
}

std::shared_ptr<TaskGroup> TaskGroup::MakeThreaded(ThreadPool* thread_pool) {
  return std::shared_ptr<TaskGroup>(new ThreadedTaskGroup(thread_pool));
}

}  // namespace internal
}  // namespace arrow
