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

#include "arrow/util/async_util.h"

#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"

#include <deque>
#include <iostream>
#include <list>
#include <mutex>

namespace arrow {

namespace util {

class ThrottleImpl : public AsyncTaskScheduler::Throttle {
 public:
  explicit ThrottleImpl(int max_concurrent_cost) : available_cost_(max_concurrent_cost) {}

  std::optional<Future<>> TryAcquire(int amt) override {
    std::lock_guard<std::mutex> lk(mutex_);
    if (backoff_.is_valid()) {
      return backoff_;
    }
    if (amt <= available_cost_) {
      available_cost_ -= amt;
      return std::nullopt;
    }
    backoff_ = Future<>::Make();
    return backoff_;
  }

  void Release(int amt) override {
    Future<> backoff_to_fulfill;
    {
      std::lock_guard<std::mutex> lk(mutex_);
      available_cost_ += amt;
      if (backoff_.is_valid()) {
        backoff_to_fulfill = std::move(backoff_);
      }
    }
    if (backoff_to_fulfill.is_valid()) {
      backoff_to_fulfill.MarkFinished();
    }
  }

 private:
  std::mutex mutex_;
  int available_cost_;
  Future<> backoff_;
};

std::unique_ptr<AsyncTaskScheduler::Throttle> AsyncTaskScheduler::MakeThrottle(
    int max_concurrent_cost) {
  return ::arrow::internal::make_unique<ThrottleImpl>(max_concurrent_cost);
}

namespace {

// Very basic FIFO queue
class FifoQueue : public AsyncTaskScheduler::Queue {
  using Task = AsyncTaskScheduler::Task;
  void Push(std::unique_ptr<Task> task) override { tasks_.push_back(std::move(task)); }

  std::unique_ptr<Task> Pop() override {
    std::unique_ptr<Task> task = std::move(tasks_.front());
    tasks_.pop_front();
    return task;
  }

  const Task& Peek() override { return *tasks_.front(); }

  bool Empty() override { return tasks_.empty(); }

  void Purge() override { tasks_.clear(); }

 private:
  std::list<std::unique_ptr<Task>> tasks_;
};

class AsyncTaskSchedulerImpl : public AsyncTaskScheduler {
 public:
  using Task = AsyncTaskScheduler::Task;
  using Queue = AsyncTaskScheduler::Queue;

  AsyncTaskSchedulerImpl(AsyncTaskSchedulerImpl* parent, std::unique_ptr<Queue> queue,
                         Throttle* throttle, FnOnce<Status()> finish_callback)
      : AsyncTaskScheduler(),
        queue_(std::move(queue)),
        throttle_(throttle),
        finish_callback_(std::move(finish_callback)) {
    if (parent == nullptr) {
      owned_global_abort_ = ::arrow::internal::make_unique<std::atomic<bool>>(0);
      global_abort_ = owned_global_abort_.get();
    } else {
      global_abort_ = parent->global_abort_;
    }
    if (throttle != nullptr && !queue_) {
      queue_ = ::arrow::internal::make_unique<FifoQueue>();
    }
  }

  ~AsyncTaskSchedulerImpl() {
    {
      std::unique_lock<std::mutex> lk(mutex_);
      if (state_ == State::kRunning) {
        AbortUnlocked(
            Status::UnknownError("AsyncTaskScheduler abandoned before completion"),
            std::move(lk));
      }
    }
    finished_.Wait();
  }

  bool AddTask(std::unique_ptr<Task> task) override {
    std::unique_lock<std::mutex> lk(mutex_);
    // When a scheduler has been ended that usually signals to the caller that the
    // scheduler is free to be deleted (and any associated resources).  In this case the
    // task likely has dangling pointers/references and would be unsafe to execute.
    DCHECK_NE(state_, State::kEnded)
        << "Attempt to add a task to a scheduler after it had ended.";
    if (state_ == State::kAborted) {
      return false;
    }
    if (global_abort_->load()) {
      AbortUnlocked(Status::Cancelled("Another scheduler aborted"), std::move(lk));
      return false;
    }
    if (throttle_) {
      // If the queue isn't empty then don't even try and acquire the throttle
      // We can safely assume it is either blocked or in the middle of trying to
      // alert a queued task.
      if (!queue_->Empty()) {
        queue_->Push(std::move(task));
        return true;
      }
      std::optional<Future<>> maybe_backoff = throttle_->TryAcquire(task->cost());
      if (maybe_backoff) {
        queue_->Push(std::move(task));
        lk.unlock();
        maybe_backoff->AddCallback([this](const Status&) {
          std::unique_lock<std::mutex> lk2(mutex_);
          ContinueTasksUnlocked(std::move(lk2));
        });
      } else {
        SubmitTaskUnlocked(std::move(task), std::move(lk));
      }
    } else {
      SubmitTaskUnlocked(std::move(task), std::move(lk));
    }
    return true;
  }

  AsyncTaskScheduler* MakeSubScheduler(FnOnce<Status()> finish_callback,
                                       Throttle* throttle,
                                       std::unique_ptr<Queue> queue) override {
    std::unique_ptr<AsyncTaskSchedulerImpl> owned_child =
        ::arrow::internal::make_unique<AsyncTaskSchedulerImpl>(
            this, std::move(queue), throttle, std::move(finish_callback));
    AsyncTaskScheduler* child = owned_child.get();
    std::list<std::unique_ptr<AsyncTaskSchedulerImpl>>::iterator child_itr;
    {
      std::lock_guard<std::mutex> lk(mutex_);
      running_tasks_++;
      sub_schedulers_.push_back(std::move(owned_child));
      child_itr = --sub_schedulers_.end();
    }

    struct Finalizer {
      void operator()(const Status& st) {
        std::unique_lock<std::mutex> lk(self->mutex_);
        FnOnce<Status()> finish_callback;
        if (!st.ok()) {
          self->running_tasks_--;
          self->AbortUnlocked(st, std::move(lk));
          return;
        } else {
          // We only eagerly erase the sub-scheduler on a successful completion.  This is
          // because, if the sub-scheduler aborted, then the caller of MakeSubScheduler
          // might still be planning to call End
          finish_callback = std::move((*child_itr)->finish_callback_);
          self->sub_schedulers_.erase(child_itr);
        }
        lk.unlock();
        Status finish_st = std::move(finish_callback)();
        lk.lock();
        self->running_tasks_--;
        if (!finish_st.ok()) {
          self->AbortUnlocked(finish_st, std::move(lk));
          return;
        }
        if (self->IsFullyFinished()) {
          lk.unlock();
          self->finished_.MarkFinished(self->maybe_error_);
        }
      }

      AsyncTaskSchedulerImpl* self;
      std::list<std::unique_ptr<AsyncTaskSchedulerImpl>>::iterator child_itr;
    };

    child->OnFinished().AddCallback(Finalizer{this, child_itr});
    return child;
  }

  void End() override {
    std::unique_lock<std::mutex> lk(mutex_);
    if (state_ == State::kAborted) {
      return;
    }
    state_ = State::kEnded;
    if (running_tasks_ == 0 && (!queue_ || queue_->Empty())) {
      lk.unlock();
      finished_.MarkFinished(std::move(maybe_error_));
    }
  }

  Future<> OnFinished() const override { return finished_; }

 private:
  void ContinueTasksUnlocked(std::unique_lock<std::mutex>&& lk) {
    while (!queue_->Empty()) {
      int next_cost = queue_->Peek().cost();
      std::optional<Future<>> maybe_backoff = throttle_->TryAcquire(next_cost);
      if (maybe_backoff) {
        lk.unlock();
        if (!maybe_backoff->TryAddCallback([this] {
              return [this](const Status&) {
                std::unique_lock<std::mutex> lk2(mutex_);
                ContinueTasksUnlocked(std::move(lk2));
              };
            })) {
          lk.lock();
          continue;
        }
        return;
      } else {
        std::unique_ptr<Task> next_task = queue_->Pop();
        SubmitTaskUnlocked(std::move(next_task), std::move(lk));
        lk.lock();
      }
    }
  }

  bool IsFullyFinished() {
    return state_ != State::kRunning && (!queue_ || queue_->Empty()) &&
           running_tasks_ == 0;
  }

  void DoSubmitTask(std::unique_ptr<Task> task) {
    int cost = task->cost();
    Result<Future<>> submit_result = (*task)(this);
    if (!submit_result.ok()) {
      global_abort_->store(true);
      std::unique_lock<std::mutex> lk(mutex_);
      running_tasks_--;
      AbortUnlocked(submit_result.status(), std::move(lk));
      return;
    }
    submit_result->AddCallback([this, cost](const Status& st) {
      std::unique_lock<std::mutex> lk(mutex_);
      if (!st.ok()) {
        running_tasks_--;
        AbortUnlocked(st, std::move(lk));
        return;
      }
      if (global_abort_->load()) {
        running_tasks_--;
        AbortUnlocked(Status::Cancelled("Another scheduler aborted"), std::move(lk));
        return;
      }
      // It's perhaps a little odd to release the throttle here instead of at the end of
      // this method.  However, once we decrement running_tasks_ and release the lock we
      // are eligible for deletion and throttle_ would become invalid.
      lk.unlock();
      if (throttle_) {
        throttle_->Release(cost);
      }
      lk.lock();
      running_tasks_--;
      if (IsFullyFinished()) {
        lk.unlock();
        finished_.MarkFinished(maybe_error_);
      }
    });
  }

  void AbortUnlocked(const Status& st, std::unique_lock<std::mutex>&& lk) {
    if (state_ == State::kRunning) {
      maybe_error_ = st;
      state_ = State::kAborted;
      if (queue_) {
        queue_->Purge();
      }
    } else if (state_ == State::kEnded) {
      if (maybe_error_.ok()) {
        maybe_error_ = st;
      }
    }
    if (running_tasks_ == 0) {
      lk.unlock();
      finished_.MarkFinished(std::move(maybe_error_));
    }
  }

  void SubmitTaskUnlocked(std::unique_ptr<Task> task, std::unique_lock<std::mutex>&& lk) {
    running_tasks_++;
    lk.unlock();
    DoSubmitTask(std::move(task));
  }

  enum State { kRunning, kAborted, kEnded };

  std::unique_ptr<Queue> queue_;
  Throttle* throttle_;
  FnOnce<Status()> finish_callback_;

  Future<> finished_ = Future<>::Make();
  int running_tasks_ = 0;
  // Starts as running, then transitions to either aborted or ended
  State state_ = State::kRunning;
  // Starts as ok but may transition to an error if aborted.  Will be the first
  // error that caused the abort.  If multiple errors occur, only the first is captured.
  Status maybe_error_;
  std::mutex mutex_;

  std::list<std::unique_ptr<AsyncTaskSchedulerImpl>> sub_schedulers_;

  std::unique_ptr<std::atomic<bool>> owned_global_abort_ = nullptr;
  std::atomic<bool>* global_abort_;
};

}  // namespace

std::unique_ptr<AsyncTaskScheduler> AsyncTaskScheduler::Make(
    Throttle* throttle, std::unique_ptr<Queue> queue) {
  return ::arrow::internal::make_unique<AsyncTaskSchedulerImpl>(
      nullptr, std::move(queue), throttle, FnOnce<Status()>());
}

}  // namespace util
}  // namespace arrow
