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

#include <deque>
#include <list>
#include <memory>
#include <mutex>

namespace arrow {

namespace util {

class ThrottleImpl : public AsyncTaskScheduler::Throttle {
 public:
  explicit ThrottleImpl(int max_concurrent_cost)
      : max_concurrent_cost_(max_concurrent_cost), available_cost_(max_concurrent_cost) {}

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

  int Capacity() override { return max_concurrent_cost_; }

 private:
  std::mutex mutex_;
  int max_concurrent_cost_;
  int available_cost_;
  Future<> backoff_;
};

std::unique_ptr<AsyncTaskScheduler::Throttle> AsyncTaskScheduler::MakeThrottle(
    int max_concurrent_cost) {
  return std::make_unique<ThrottleImpl>(max_concurrent_cost);
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

class AlreadyFailedScheduler : public AsyncTaskScheduler {
 public:
  explicit AlreadyFailedScheduler(Status failure_reason,
                                  FnOnce<Status(Status)> finish_callback)
      : failure_reason_(std::move(failure_reason)),
        finish_callback_(std::move(finish_callback)) {}
  ~AlreadyFailedScheduler() override {
    std::ignore = std::move(finish_callback_)(failure_reason_);
  }
  bool AddTask(std::unique_ptr<Task> task) override { return false; }
  std::unique_ptr<AsyncTaskScheduler::Holder> MakeSubScheduler(
      FnOnce<Status(AsyncTaskScheduler*)> initial_task,
      FnOnce<Status(Status)> finish_callback, Throttle* throttle,
      std::unique_ptr<Queue> queue) override {
    return AlreadyFailedScheduler::Make(failure_reason_, std::move(initial_task),
                                        std::move(finish_callback));
  }
  static std::unique_ptr<AsyncTaskScheduler::Holder> Make(
      Status failure, FnOnce<Status(AsyncTaskScheduler*)> initial_task,
      FnOnce<Status(Status)> finish_callback) {
    DCHECK(!failure.ok());
    auto failed_scheduler = std::make_unique<AlreadyFailedScheduler>(
        std::move(failure), std::move(finish_callback));
    // Don't care if initial_task fails as we've already failed
    std::ignore = std::move(initial_task)(failed_scheduler.get());
    return std::make_unique<HolderImpl>(std::move(failed_scheduler));
  }

 private:
  class HolderImpl : public AsyncTaskScheduler::Holder {
   public:
    explicit HolderImpl(std::unique_ptr<AlreadyFailedScheduler> scheduler)
        : scheduler_(std::move(scheduler)) {}
    ~HolderImpl() override { scheduler_.reset(); }
    void Reset() override { scheduler_.reset(); }
    AsyncTaskScheduler* Get() override { return scheduler_.get(); }

   private:
    std::unique_ptr<AlreadyFailedScheduler> scheduler_;
  };

  Status failure_reason_;
  FnOnce<Status(Status)> finish_callback_;
};

class AsyncTaskSchedulerImpl : public AsyncTaskScheduler {
 public:
  using Task = AsyncTaskScheduler::Task;
  using Queue = AsyncTaskScheduler::Queue;

  AsyncTaskSchedulerImpl(AsyncTaskSchedulerImpl* parent, std::unique_ptr<Queue> queue,
                         Throttle* throttle, FnOnce<Status(Status)> finish_callback,
                         StopToken stop_token)
      : AsyncTaskScheduler(),
        queue_(std::move(queue)),
        throttle_(throttle),
        finish_callback_(std::move(finish_callback)),
        stop_token_(std::move(stop_token)) {
    if (parent == nullptr) {
      owned_global_abort_ = std::make_unique<std::atomic<bool>>(0);
      global_abort_ = owned_global_abort_.get();
    } else {
      global_abort_ = parent->global_abort_;
    }
    if (throttle != nullptr && !queue_) {
      queue_ = std::make_unique<FifoQueue>();
    }
  }

  ~AsyncTaskSchedulerImpl() {
    std::unique_lock<std::mutex> lk(mutex_);
    DCHECK(!queue_ || queue_->Empty())
        << " scheduler destroyed before all queued tasks finished";
    DCHECK_EQ(running_tasks_, 0) << " scheduler destroyed while tasks still running";
    DCHECK_EQ(state_, State::kEnded);
  }

  bool AddTask(std::unique_ptr<Task> task) override {
    std::unique_lock<std::mutex> lk(mutex_);
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
      int latched_cost = std::min(task->cost(), throttle_->Capacity());
      std::optional<Future<>> maybe_backoff = throttle_->TryAcquire(latched_cost);
      if (maybe_backoff) {
        queue_->Push(std::move(task));
        lk.unlock();
        maybe_backoff->AddCallback([this](const Status&) {
          std::unique_lock<std::mutex> lk2(mutex_);
          ContinueTasksUnlocked(&lk2);
        });
      } else {
        SubmitTaskUnlocked(std::move(task), &lk);
      }
    } else {
      SubmitTaskUnlocked(std::move(task), &lk);
    }
    return true;
  }

  std::unique_ptr<Holder> MakeSubScheduler(
      FnOnce<Status(AsyncTaskScheduler*)> initial_task,
      FnOnce<Status(Status)> finish_callback, Throttle* throttle,
      std::unique_ptr<Queue> queue) override {
    AsyncTaskSchedulerImpl* child;
    std::list<std::shared_ptr<AsyncTaskSchedulerImpl>>::iterator child_itr;
    std::shared_ptr<AsyncTaskSchedulerImpl> owned_child;
    {
      std::lock_guard<std::mutex> lk(mutex_);
      if (state_ == State::kAborted) {
        return AlreadyFailedScheduler::Make(maybe_error_, std::move(initial_task),
                                            std::move(finish_callback));
      }
      owned_child = std::make_shared<AsyncTaskSchedulerImpl>(
          this, std::move(queue), throttle, std::move(finish_callback), stop_token_);
      child = owned_child.get();
      running_tasks_++;
      sub_schedulers_.push_back(owned_child);
      child_itr = --sub_schedulers_.end();
    }

    struct Finalizer {
      void operator()(const Status& st) {
        std::unique_lock<std::mutex> lk(self->mutex_);
        FnOnce<Status(Status)> finish_callback = std::move(child->finish_callback_);
        self->sub_schedulers_.erase(child_itr);
        lk.unlock();
        Status finish_st = std::move(finish_callback)(st);
        lk.lock();
        self->running_tasks_--;
        if (!st.ok()) {
          self->AbortUnlocked(st, std::move(lk));
          return;
        }
        if (!finish_st.ok()) {
          self->AbortUnlocked(finish_st, std::move(lk));
          return;
        }
        self->MaybeEndUnlocked(std::move(lk));
      }

      AsyncTaskSchedulerImpl* self;
      AsyncTaskSchedulerImpl* child;
      std::list<std::shared_ptr<AsyncTaskSchedulerImpl>>::iterator child_itr;
    };

    Status initial_task_st = std::move(initial_task)(child);
    child->OnTaskFinished(initial_task_st, 0);
    child->OnFinished().AddCallback(Finalizer{this, child, child_itr});
    return std::make_unique<HolderImpl>(std::move(owned_child));
  }

  void End() {
    std::unique_lock<std::mutex> lk(mutex_);
    if (state_ == State::kEnded) {
      return;
    }
    state_ = State::kEnded;
    MaybeEndUnlocked(std::move(lk));
  }

  Future<> OnFinished() const { return finished_; }

 private:
  void ContinueTasksUnlocked(std::unique_lock<std::mutex>* lk) {
    while (!queue_->Empty()) {
      int next_cost = std::min(queue_->Peek().cost(), throttle_->Capacity());
      std::optional<Future<>> maybe_backoff = throttle_->TryAcquire(next_cost);
      if (maybe_backoff) {
        lk->unlock();
        if (!maybe_backoff->TryAddCallback([this] {
              return [this](const Status&) {
                std::unique_lock<std::mutex> lk2(mutex_);
                ContinueTasksUnlocked(&lk2);
              };
            })) {
          lk->lock();
          continue;
        }
        return;
      } else {
        std::unique_ptr<Task> next_task = queue_->Pop();
        if (!SubmitTaskUnlocked(std::move(next_task), lk)) {
          // We reached a terminal condition and there is no need to further continue
          return;
        }
        lk->lock();
      }
    }
  }

  bool IsFullyFinished() {
    return state_ == State::kEnded && (!queue_ || queue_->Empty()) && running_tasks_ == 0;
  }

  bool OnTaskFinished(const Status& st, int task_cost) {
    std::unique_lock<std::mutex> lk(mutex_);
    if (!st.ok()) {
      running_tasks_--;
      AbortUnlocked(st, std::move(lk));
      return false;
    }
    if (global_abort_->load()) {
      running_tasks_--;
      AbortUnlocked(Status::Cancelled("Another scheduler aborted"), std::move(lk));
      return false;
    }
    // It's perhaps a little odd to release the throttle here instead of at the end of
    // this method.  However, once we decrement running_tasks_ and release the lock we
    // are eligible for deletion and throttle_ would become invalid.
    lk.unlock();
    if (throttle_ && task_cost > 0) {
      throttle_->Release(task_cost);
    }
    lk.lock();
    running_tasks_--;
    return MaybeEndUnlocked(std::move(lk));
  }

  bool DoSubmitTask(std::unique_ptr<Task> task) {
    int cost = task->cost();
    if (throttle_) {
      cost = std::min(cost, throttle_->Capacity());
    }
    Result<Future<>> submit_result = (*task)(this);
    if (!submit_result.ok()) {
      std::unique_lock<std::mutex> lk(mutex_);
      global_abort_->store(true);
      running_tasks_--;
      AbortUnlocked(submit_result.status(), std::move(lk));
      return false;
    }
    // Capture `task` to keep it alive until finished
    if (!submit_result->TryAddCallback(
            [this, cost, task_inner = std::move(task)]() mutable {
              return [this, cost, task_inner2 = std::move(task_inner)](const Status& st) {
                OnTaskFinished(st, cost);
              };
            })) {
      return OnTaskFinished(submit_result->status(), cost);
    }
    return true;
  }

  bool MaybeEndUnlocked(std::unique_lock<std::mutex>&& lk) {
    if (running_tasks_ > 0 &&
        running_tasks_ <= static_cast<int>(sub_schedulers_.size())) {
      // parent is out of tasks and so sub schedulers should be ended
      std::vector<std::weak_ptr<AsyncTaskSchedulerImpl>> to_reset;
      for (const auto& sub_scheduler : sub_schedulers_) {
        to_reset.emplace_back(sub_scheduler);
      }
      lk.unlock();
      for (const auto& weak_scheduler : to_reset) {
        std::shared_ptr<AsyncTaskSchedulerImpl> scheduler = weak_scheduler.lock();
        if (scheduler) {
          scheduler->End();
        }
      }
      return true;
    }
    if (IsFullyFinished()) {
      lk.unlock();
      finished_.MarkFinished(maybe_error_);
      return false;
    } else {
      lk.unlock();
      return true;
    }
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
      if (queue_) {
        queue_->Purge();
      }
    }
    MaybeEndUnlocked(std::move(lk));
  }

  bool SubmitTaskUnlocked(std::unique_ptr<Task> task, std::unique_lock<std::mutex>* lk) {
    running_tasks_++;
    lk->unlock();
    return DoSubmitTask(std::move(task));
  }

  class HolderImpl : public AsyncTaskScheduler::Holder {
   public:
    explicit HolderImpl(std::weak_ptr<AsyncTaskSchedulerImpl> scheduler)
        : scheduler_(std::move(scheduler)) {}
    ~HolderImpl() override { EndIfNeeded(); }
    void Reset() override { EndIfNeeded(); }
    AsyncTaskScheduler* Get() override {
      std::shared_ptr<AsyncTaskSchedulerImpl> scheduler = scheduler_.lock();
      DCHECK(!!scheduler);
      return scheduler.get();
    }
    void EndIfNeeded() {
      if (destroyed_) {
        return;
      }
      destroyed_ = true;
      std::shared_ptr<AsyncTaskSchedulerImpl> scheduler = scheduler_.lock();
      if (scheduler) {
        scheduler->End();
      }
    }

   private:
    std::weak_ptr<AsyncTaskSchedulerImpl> scheduler_;
    bool destroyed_ = false;
  };

  enum State { kRunning, kAborted, kEnded };

  std::unique_ptr<Queue> queue_;
  Throttle* throttle_;
  FnOnce<Status(Status)> finish_callback_;

  Future<> finished_ = Future<>::Make();
  // Always starts at 1 to represent the initial task
  int running_tasks_ = 1;
  State state_ = State::kRunning;
  // Starts as ok but may transition to an error if aborted.  Will be the first
  // error that caused the abort.  If multiple errors occur, only the first is captured.
  Status maybe_error_;
  std::mutex mutex_;

  std::list<std::shared_ptr<AsyncTaskSchedulerImpl>> sub_schedulers_;

  std::unique_ptr<std::atomic<bool>> owned_global_abort_ = nullptr;
  std::atomic<bool>* global_abort_;
  StopToken stop_token_;

  // Allows AsyncTaskScheduler::Make to call OnTaskFinished
  friend AsyncTaskScheduler;
};

}  // namespace

Future<> AsyncTaskScheduler::Make(FnOnce<Status(AsyncTaskScheduler*)> initial_task,
                                  Throttle* throttle, std::unique_ptr<Queue> queue,
                                  StopToken stop_token) {
  auto scheduler = std::make_unique<AsyncTaskSchedulerImpl>(
      nullptr, std::move(queue), throttle, FnOnce<Status(Status)>(),
      std::move(stop_token));
  Status initial_task_st = std::move(initial_task)(scheduler.get());
  scheduler->OnTaskFinished(initial_task_st, 0);
  // Top-level schedulers are pre-ended.
  scheduler->End();
  // Keep scheduler alive until finished
  return scheduler->OnFinished().Then([scheduler = std::move(scheduler)] {});
}

}  // namespace util
}  // namespace arrow
