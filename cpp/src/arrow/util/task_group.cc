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

#include "arrow/util/task_group.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <utility>

#include "arrow/util/checked_cast.h"
#include "arrow/util/config.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace internal {

namespace {

////////////////////////////////////////////////////////////////////////
// Serial TaskGroup implementation

class SerialTaskGroup : public TaskGroup {
 public:
  explicit SerialTaskGroup(StopToken stop_token) : stop_token_(std::move(stop_token)) {}

  void AppendReal(FnOnce<Status()> task) override {
    DCHECK(!finished_);
    if (stop_token_.IsStopRequested()) {
      status_ &= stop_token_.Poll();
      return;
    }
    if (status_.ok()) {
      status_ &= std::move(task)();
    }
  }

  Status current_status() override { return status_; }

  bool ok() const override { return status_.ok(); }

  Status Finish() override {
    if (!finished_) {
      finished_ = true;
    }
    return status_;
  }

  Future<> FinishAsync() override { return Future<>::MakeFinished(Finish()); }

  int parallelism() override { return 1; }

  StopToken stop_token_;
  Status status_;
  bool finished_ = false;
};

////////////////////////////////////////////////////////////////////////
// Threaded TaskGroup implementation

class ThreadedTaskGroup : public TaskGroup {
 public:
  ThreadedTaskGroup(Executor* executor, StopToken stop_token)
      : executor_(executor),
        stop_token_(std::move(stop_token)),
        nremaining_(0),
        ok_(true),
        finished_(false) {}

  ~ThreadedTaskGroup() override {
    // Make sure all pending tasks are finished, so that dangling references
    // to this don't persist.
    ARROW_UNUSED(Finish());
  }

  void AppendReal(FnOnce<Status()> task) override {
    DCHECK(!finished_);
    if (stop_token_.IsStopRequested()) {
      UpdateStatus(stop_token_.Poll());
      return;
    }

    // The hot path is unlocked thanks to atomics
    // Only if an error occurs is the lock taken
    if (ok_.load(std::memory_order_acquire)) {
      nremaining_.fetch_add(1, std::memory_order_acquire);

      auto self = checked_pointer_cast<ThreadedTaskGroup>(shared_from_this());

      auto callable = [self = std::move(self), task = std::move(task),
                       stop_token = stop_token_]() mutable {
        if (self->ok_.load(std::memory_order_acquire)) {
          Status st;
          if (stop_token.IsStopRequested()) {
            st = stop_token.Poll();
          } else {
            // XXX what about exceptions?
            st = std::move(task)();
          }
          self->UpdateStatus(std::move(st));
        }
        self->OneTaskDone();
      };
      UpdateStatus(executor_->Spawn(std::move(callable)));
    }
  }

  Status current_status() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return status_;
  }

  bool ok() const override { return ok_.load(); }

  Status Finish() override {
#ifdef ARROW_ENABLE_THREADING
    std::unique_lock<std::mutex> lock(mutex_);
    if (!finished_) {
      cv_.wait(lock, [&]() { return nremaining_.load() == 0; });
      // Current tasks may start other tasks, so only set this when done
      finished_ = true;
    }
#else
    while (!finished_ && nremaining_.load() != 0) {
      arrow::internal::SerialExecutor::RunTasksOnAllExecutors();
    }
    finished_ = true;
#endif
    return status_;
  }

  Future<> FinishAsync() override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!completion_future_.has_value()) {
      if (nremaining_.load() == 0) {
        completion_future_ = Future<>::MakeFinished(status_);
      } else {
        completion_future_ = Future<>::Make();
      }
    }
    return *completion_future_;
  }

  int parallelism() override { return executor_->GetCapacity(); }

 protected:
  void UpdateStatus(Status&& st) {
    // Must be called unlocked, only locks on error
    if (ARROW_PREDICT_FALSE(!st.ok())) {
      std::lock_guard<std::mutex> lock(mutex_);
      ok_.store(false, std::memory_order_release);
      status_ &= std::move(st);
    }
  }

  void OneTaskDone() {
    // Can be called unlocked thanks to atomics
    auto nremaining = nremaining_.fetch_sub(1, std::memory_order_release) - 1;
    DCHECK_GE(nremaining, 0);
    if (nremaining == 0) {
      // Take the lock so that ~ThreadedTaskGroup cannot destroy cv
      // before cv.notify_one() has returned
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.notify_one();
      if (completion_future_.has_value()) {
        // MarkFinished could be slow.  We don't want to call it while we are holding
        // the lock.
        auto& future = *completion_future_;
        const auto finished = completion_future_->is_finished();
        const auto& status = status_;
        // This will be redundant if the user calls Finish and not FinishAsync
        if (!finished && !finished_) {
          finished_ = true;
          lock.unlock();
          future.MarkFinished(status);
        } else {
          lock.unlock();
        }
      }
    }
  }

  // These members are usable unlocked
  Executor* executor_;
  StopToken stop_token_;
  std::atomic<int32_t> nremaining_;
  std::atomic<bool> ok_;
  std::atomic<bool> finished_;

  // These members use locking
  std::mutex mutex_;
  std::condition_variable cv_;
  Status status_;
  std::optional<Future<>> completion_future_;
};

}  // namespace

std::shared_ptr<TaskGroup> TaskGroup::MakeSerial(StopToken stop_token) {
  return std::shared_ptr<TaskGroup>(new SerialTaskGroup{stop_token});
}

std::shared_ptr<TaskGroup> TaskGroup::MakeThreaded(Executor* thread_pool,
                                                   StopToken stop_token) {
  return std::shared_ptr<TaskGroup>(new ThreadedTaskGroup{thread_pool, stop_token});
}

}  // namespace internal
}  // namespace arrow
