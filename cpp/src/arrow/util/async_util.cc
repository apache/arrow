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
#include "arrow/util/string.h"
#include "arrow/util/tracing_internal.h"

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>

using namespace std::string_view_literals;  // NOLINT

namespace arrow {

namespace util {

class ThrottleImpl : public ThrottledAsyncTaskScheduler::Throttle {
 public:
  explicit ThrottleImpl(int max_concurrent_cost)
      : max_concurrent_cost_(max_concurrent_cost), available_cost_(max_concurrent_cost) {}

  ~ThrottleImpl() {
    if (backoff_.is_valid()) {
      backoff_.MarkFinished(Status::Cancelled("Throttle destroyed while paused"));
    }
  }

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
    std::unique_lock lk(mutex_);
    available_cost_ += amt;
    NotifyUnlocked(std::move(lk));
  }

  void Pause() override {
    std::lock_guard lg(mutex_);
    paused_ = true;
    if (!backoff_.is_valid()) {
      backoff_ = Future<>::Make();
    }
  }

  void Resume() override {
    std::unique_lock lk(mutex_);
    paused_ = false;
    // Might be a useless notification if our current cost is full
    // or no one is waiting but it should be ok.
    NotifyUnlocked(std::move(lk));
  }

  int Capacity() override { return max_concurrent_cost_; }

 private:
  void NotifyUnlocked(std::unique_lock<std::mutex>&& lk) {
    if (backoff_.is_valid()) {
      Future<> backoff_to_fulfill = std::move(backoff_);
      lk.unlock();
      backoff_to_fulfill.MarkFinished();
    } else {
      lk.unlock();
    }
  }

  std::mutex mutex_;
  int max_concurrent_cost_;
  int available_cost_;
  bool paused_ = false;
  Future<> backoff_;
};

namespace {

// Very basic FIFO queue
class FifoQueue : public ThrottledAsyncTaskScheduler::Queue {
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

#ifdef ARROW_WITH_OPENTELEMETRY
::arrow::internal::tracing::Scope TraceTaskSubmitted(AsyncTaskScheduler::Task* task,
                                                     const util::tracing::Span& parent) {
  if (task->span.valid()) {
    EVENT(task->span, "task submitted");
    return ACTIVATE_SPAN(task->span);
  }

  return START_SCOPED_SPAN_WITH_PARENT_SV(task->span, parent, task->name(),
                                          {{"task.cost", task->cost()}});
}

void TraceTaskQueued(AsyncTaskScheduler::Task* task, const util::tracing::Span& parent) {
  START_SCOPED_SPAN_WITH_PARENT_SV(task->span, parent, task->name(),
                                   {{"task.cost", task->cost()}});
}

void TraceTaskFinished(AsyncTaskScheduler::Task* task) { END_SPAN(task->span); }

void TraceSchedulerAbort(const Status& error) { EVENT_ON_CURRENT_SPAN(error.ToString()); }
#endif

class AsyncTaskSchedulerImpl : public AsyncTaskScheduler {
 public:
  using Task = AsyncTaskScheduler::Task;

  explicit AsyncTaskSchedulerImpl(StopToken stop_token,
                                  FnOnce<void(const Status&)> abort_callback)
      : AsyncTaskScheduler(),
        stop_token_(std::move(stop_token)),
        abort_callback_(std::move(abort_callback)) {
    START_SCOPED_SPAN(span_, "AsyncTaskScheduler");
  }

  ~AsyncTaskSchedulerImpl() {
    DCHECK_EQ(running_tasks_, 0) << " scheduler destroyed while tasks still running";
  }

  bool AddTask(std::unique_ptr<Task> task) override {
    std::unique_lock<std::mutex> lk(mutex_);
    if (stop_token_.IsStopRequested()) {
      AbortUnlocked(stop_token_.Poll(), std::move(lk));
    }
    if (IsAborted()) {
      return false;
    }
    SubmitTaskUnlocked(std::move(task), std::move(lk));
    return true;
  }

  Future<> OnFinished() const { return finished_; }
  const tracing::Span& span() const override { return span_; }

 private:
  bool IsAborted() { return !maybe_error_.ok(); }

  bool IsFullyFinished() { return running_tasks_ == 0; }

  void OnTaskFinished(const Status& st) {
    std::unique_lock<std::mutex> lk(mutex_);
    if (!st.ok()) {
      running_tasks_--;
      AbortUnlocked(st, std::move(lk));
      return;
    }
    running_tasks_--;
    return MaybeEndUnlocked(std::move(lk));
  }

  void DoSubmitTask(std::unique_ptr<Task> task) {
    Result<Future<>> submit_result = (*task)();
    if (!submit_result.ok()) {
      std::unique_lock<std::mutex> lk(mutex_);
      running_tasks_--;
      AbortUnlocked(submit_result.status(), std::move(lk));
      return;
    }
    // Capture `task` to keep it alive until finished
    if (!submit_result->TryAddCallback([this, task_inner = std::move(task)]() mutable {
          return [this, task_inner2 = std::move(task_inner)](const Status& st) {
#ifdef ARROW_WITH_OPENTELEMETRY
            TraceTaskFinished(task_inner2.get());
#endif
            OnTaskFinished(st);
          };
        })) {
      return OnTaskFinished(submit_result->status());
    }
  }

  void MaybeEndUnlocked(std::unique_lock<std::mutex>&& lk) {
    if (IsFullyFinished()) {
      lk.unlock();
      finished_.MarkFinished(maybe_error_);
    } else {
      // Always unlock for consistency's sake
      lk.unlock();
    }
  }

  void AbortUnlocked(const Status& st, std::unique_lock<std::mutex>&& lk) {
    DCHECK(!st.ok());
    bool aborted = false;
    if (!IsAborted()) {
      maybe_error_ = st;
#ifdef ARROW_WITH_OPENTELEMETRY
      TraceSchedulerAbort(st);
#endif
      // Add one more "task" to represent running the abort callback.  This
      // will prevent any other task finishing and marking the scheduler finished
      // while we are running the abort callback.
      running_tasks_++;
      aborted = true;
    }
    if (aborted) {
      lk.unlock();
      std::move(abort_callback_)(st);
      lk.lock();
      running_tasks_--;
    }
    MaybeEndUnlocked(std::move(lk));
  }

  void SubmitTaskUnlocked(std::unique_ptr<Task> task, std::unique_lock<std::mutex>&& lk) {
    if (stop_token_.IsStopRequested()) {
      AbortUnlocked(stop_token_.Poll(), std::move(lk));
      return;
    }
#ifdef ARROW_WITH_OPENTELEMETRY
    // It's important that the task's span be active while we run the submit function.
    // Normally the submit function should transfer the span to the thread task as the
    // active span.
    auto scope = TraceTaskSubmitted(task.get(), span_);
#endif
    running_tasks_++;
    lk.unlock();
    return DoSubmitTask(std::move(task));
  }

  Future<> finished_ = Future<>::Make();
  // The initial task is our first task
  int running_tasks_ = 1;
  // Starts as ok but may transition to an error if aborted.  Will be the first
  // error that caused the abort.  If multiple errors occur, only the first is captured.
  Status maybe_error_;
  std::mutex mutex_;
  StopToken stop_token_;
  FnOnce<void(const Status&)> abort_callback_;
  util::tracing::Span span_;

  // Allows AsyncTaskScheduler::Make to call OnTaskFinished
  friend AsyncTaskScheduler;
};

class ThrottledAsyncTaskSchedulerImpl
    : public ThrottledAsyncTaskScheduler,
      public std::enable_shared_from_this<ThrottledAsyncTaskSchedulerImpl> {
 public:
  using Queue = ThrottledAsyncTaskScheduler::Queue;
  using Throttle = ThrottledAsyncTaskScheduler::Throttle;

  ThrottledAsyncTaskSchedulerImpl(AsyncTaskScheduler* target,
                                  std::unique_ptr<Throttle> throttle,
                                  std::unique_ptr<Queue> queue)
      : target_(target), throttle_(std::move(throttle)), queue_(std::move(queue)) {}

  ~ThrottledAsyncTaskSchedulerImpl() {
    // There can be tasks left in the queue in the event of an abort
    queue_->Purge();
  }

  bool AddTask(std::unique_ptr<Task> task) override {
    std::unique_lock lk(mutex_);
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
#ifdef ARROW_WITH_OPENTELEMETRY
      TraceTaskQueued(task.get(), span());
#endif
      queue_->Push(std::move(task));
      lk.unlock();
      maybe_backoff->AddCallback(
          [weak_self = std::weak_ptr<ThrottledAsyncTaskSchedulerImpl>(
               shared_from_this())](const Status& st) {
            if (st.ok()) {
              if (auto self = weak_self.lock()) {
                self->ContinueTasks();
              }
            }
          });
      return true;
    } else {
      lk.unlock();
      return SubmitTask(std::move(task), latched_cost);
    }
  }

  void Pause() override { throttle_->Pause(); }
  void Resume() override { throttle_->Resume(); }
  const util::tracing::Span& span() const override { return target_->span(); }

 private:
  bool SubmitTask(std::unique_ptr<Task> task, int latched_cost) {
    // Wrap the task with a wrapper that runs it and then checks to see if there are any
    // queued tasks
    std::string_view name = task->name();
    return target_->AddSimpleTask(
        [latched_cost, inner_task = std::move(task),
         self = shared_from_this()]() mutable -> Result<Future<>> {
          ARROW_ASSIGN_OR_RAISE(Future<> inner_fut, (*inner_task)());
          return inner_fut.Then([latched_cost, self = std::move(self)] {
            self->throttle_->Release(latched_cost);
            self->ContinueTasks();
          });
        },
        name);
  }

  void ContinueTasks() {
    std::unique_lock lk(mutex_);
    while (!queue_->Empty()) {
      int next_cost = std::min(queue_->Peek().cost(), throttle_->Capacity());
      std::optional<Future<>> maybe_backoff = throttle_->TryAcquire(next_cost);
      if (maybe_backoff) {
        lk.unlock();
        if (!maybe_backoff->TryAddCallback([&] {
              return [self = shared_from_this()](const Status& st) {
                if (st.ok()) {
                  self->ContinueTasks();
                }
              };
            })) {
          if (!maybe_backoff->status().ok()) {
            return;
          }
          lk.lock();
          continue;
        }
        return;
      } else {
        std::unique_ptr<Task> next_task = queue_->Pop();
        lk.unlock();
        if (!SubmitTask(std::move(next_task), next_cost)) {
          return;
        }
        lk.lock();
      }
    }
  }

  AsyncTaskScheduler* target_;
  std::unique_ptr<Throttle> throttle_;
  std::unique_ptr<Queue> queue_;
  std::mutex mutex_;
};

class AsyncTaskGroupImpl : public AsyncTaskGroup {
 public:
  AsyncTaskGroupImpl(AsyncTaskScheduler* target, FnOnce<Status()> finish_cb)
      : target_(target), state_(std::make_shared<State>(std::move(finish_cb))) {}

  ~AsyncTaskGroupImpl() {
    if (--state_->task_count == 0) {
      Status st = std::move(state_->finish_cb)();
      if (!st.ok()) {
        // We can't return an invalid status from the destructor so we schedule a dummy
        // failing task
        target_->AddSimpleTask([st = std::move(st)]() { return st; },
                               "failed_task_reporter"sv);
      }
    }
  }

  bool AddTask(std::unique_ptr<Task> task) override {
    state_->task_count++;
    struct WrapperTask : public Task {
      WrapperTask(std::unique_ptr<Task> target, std::shared_ptr<State> state)
          : target(std::move(target)), state(std::move(state)) {}
      Result<Future<>> operator()() override {
        ARROW_ASSIGN_OR_RAISE(Future<> inner_fut, (*target)());
        return inner_fut.Then([state = std::move(state)]() {
          if (--state->task_count == 0) {
            return std::move(state->finish_cb)();
          }
          return Status::OK();
        });
      }
      int cost() const override { return target->cost(); }
      std::string_view name() const override { return target->name(); }
      std::unique_ptr<Task> target;
      std::shared_ptr<State> state;
    };
    return target_->AddTask(std::make_unique<WrapperTask>(std::move(task), state_));
  }

  const util::tracing::Span& span() const override { return target_->span(); }

 private:
  struct State {
    explicit State(FnOnce<Status()> finish_cb)
        : task_count(1), finish_cb(std::move(finish_cb)) {}
    std::atomic<int> task_count;
    FnOnce<Status()> finish_cb;
  };
  AsyncTaskScheduler* target_;
  std::shared_ptr<State> state_;
};

}  // namespace

Future<> AsyncTaskScheduler::Make(FnOnce<Status(AsyncTaskScheduler*)> initial_task,
                                  FnOnce<void(const Status&)> abort_callback,
                                  StopToken stop_token) {
  util::tracing::Span span;
  auto scope = START_SCOPED_SPAN_SV(span, "AsyncTaskScheduler::InitialTask"sv);
  auto scheduler = std::make_unique<AsyncTaskSchedulerImpl>(std::move(stop_token),
                                                            std::move(abort_callback));
  Status initial_task_st = std::move(initial_task)(scheduler.get());
  scheduler->OnTaskFinished(std::move(initial_task_st));
  // Keep scheduler alive until finished
  return scheduler->OnFinished().Then([scheduler = std::move(scheduler)] {});
}

std::shared_ptr<ThrottledAsyncTaskScheduler> ThrottledAsyncTaskScheduler::Make(
    AsyncTaskScheduler* target, int max_concurrent_cost,
    std::unique_ptr<ThrottledAsyncTaskScheduler::Queue> maybe_queue) {
  std::unique_ptr<ThrottledAsyncTaskScheduler::Queue> queue =
      (maybe_queue) ? std::move(maybe_queue) : std::make_unique<FifoQueue>();
  return std::make_shared<ThrottledAsyncTaskSchedulerImpl>(
      target, std::make_unique<ThrottleImpl>(max_concurrent_cost), std::move(queue));
}

std::shared_ptr<ThrottledAsyncTaskScheduler>
ThrottledAsyncTaskScheduler::MakeWithCustomThrottle(
    AsyncTaskScheduler* target,
    std::unique_ptr<ThrottledAsyncTaskScheduler::Throttle> throttle,
    std::unique_ptr<ThrottledAsyncTaskScheduler::Queue> maybe_queue) {
  std::unique_ptr<ThrottledAsyncTaskScheduler::Queue> queue =
      (maybe_queue) ? std::move(maybe_queue) : std::make_unique<FifoQueue>();
  return std::make_shared<ThrottledAsyncTaskSchedulerImpl>(target, std::move(throttle),
                                                           std::move(queue));
}
std::unique_ptr<AsyncTaskGroup> AsyncTaskGroup::Make(AsyncTaskScheduler* target,
                                                     FnOnce<Status()> finish_cb) {
  return std::make_unique<AsyncTaskGroupImpl>(target, std::move(finish_cb));
}

class ThrottledAsyncTaskGroup : public ThrottledAsyncTaskScheduler {
 public:
  ThrottledAsyncTaskGroup(std::shared_ptr<ThrottledAsyncTaskScheduler> throttle,
                          std::unique_ptr<AsyncTaskGroup> task_group)
      : throttle_(std::move(throttle)), task_group_(std::move(task_group)) {}
  void Pause() override { throttle_->Pause(); }
  void Resume() override { throttle_->Resume(); }
  const util::tracing::Span& span() const override { return task_group_->span(); }
  bool AddTask(std::unique_ptr<Task> task) override {
    return task_group_->AddTask(std::move(task));
  }

 private:
  std::shared_ptr<ThrottledAsyncTaskScheduler> throttle_;
  std::unique_ptr<AsyncTaskGroup> task_group_;
};

std::unique_ptr<ThrottledAsyncTaskScheduler> MakeThrottledAsyncTaskGroup(
    AsyncTaskScheduler* target, int max_concurrent_cost,
    std::unique_ptr<ThrottledAsyncTaskScheduler::Queue> maybe_queue,
    FnOnce<Status()> finish_cb) {
  std::shared_ptr<ThrottledAsyncTaskScheduler> throttle =
      ThrottledAsyncTaskScheduler::Make(target, max_concurrent_cost,
                                        std::move(maybe_queue));
  std::unique_ptr<AsyncTaskGroup> task_group =
      AsyncTaskGroup::Make(throttle.get(), std::move(finish_cb));
  return std::make_unique<ThrottledAsyncTaskGroup>(std::move(throttle),
                                                   std::move(task_group));
}

}  // namespace util
}  // namespace arrow
