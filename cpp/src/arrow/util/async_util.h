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

#pragma once

#include <atomic>
#include <functional>
#include <memory>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/cancel.h"
#include "arrow/util/functional.h"
#include "arrow/util/future.h"
#include "arrow/util/iterator.h"
#include "arrow/util/mutex.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::FnOnce;

namespace util {

/// A utility which keeps tracks of, and schedules, asynchronous tasks
///
/// An asynchronous task has a synchronous component and an asynchronous component.
/// The synchronous component typically schedules some kind of work on an external
/// resource (e.g. the I/O thread pool or some kind of kernel-based asynchronous
/// resource like io_uring).  The asynchronous part represents the work
/// done on that external resource.  Executing the synchronous part will be referred
/// to as "submitting the task" since this usually includes submitting the asynchronous
/// portion to the external thread pool.
///
/// By default the scheduler will submit the task (execute the synchronous part) as
/// soon as it is added, assuming the underlying thread pool hasn't terminated or the
/// scheduler hasn't aborted.  In this mode, the scheduler is simply acting as
/// a task group (keeping track of the ongoing work).
///
/// This can be used to provide structured concurrency for asynchronous development.
/// A task group created at a high level can be distributed amongst low level components
/// which register work to be completed.  The high level job can then wait for all work
/// to be completed before cleaning up.
///
/// A task scheduler starts with an initial task.  That task, and all subsequent tasks
/// are free to add subtasks.  Once all submitted tasks finsih the scheduler will
/// finish.  Note, it is not an error to add additional tasks after a scheduler has
/// aborted. These tasks will be ignored and never submitted.  The scheduler has a future
/// which will complete all submitted tasks have finished executing.  Once all tasks have
/// been finsihed the scheduler should no longer be used in any capacity.
///
/// Task failure (either the synchronous portion or the asynchronous portion) will cause
/// the scheduler to enter an aborted state.  The first such failure will be reported in
/// the final task future.
///
/// It is also possible to limit the number of concurrent tasks the scheduler will
/// execute. This is done by setting a throttle.  The throttle initially assumes all
/// tasks are equal but a custom cost can be supplied when scheduling a task (e.g. based
/// on the total I/O cost of the task, or the expected RAM utilization of the task)
///
/// When the total number of running tasks is limited then scheduler priority may also
/// become a consideration.  By default the scheduler runs with a FIFO queue but a custom
/// task queue can be provided.  One could, for example, use a priority queue to control
/// the order in which tasks are executed.
///
/// It is common to have multiple stages of execution.  For example, when scanning, we
/// first inspect each fragment (the inspect stage) to figure out the row groups and then
/// we scan row groups (the scan stage) to read in the data.  This sort of multi-stage
/// execution should be represented as two seperate task groups.  The first task group can
/// then have a custom finish callback which ends the second task group.
class ARROW_EXPORT AsyncTaskScheduler {
 public:
  /// Destructor for AsyncTaskScheduler
  ///
  /// The lifetime of the task scheduled is managed automatically.  The scheduler
  /// will remain valid while any tasks are running (and can always be safely accessed)
  /// within tasks) and will be destroyed as soon as all tasks have finsihed.
  virtual ~AsyncTaskScheduler() = default;
  /// An interface for a task
  ///
  /// Users may want to override this, for example, to add priority
  /// information for use by a queue.
  class Task {
   public:
    virtual ~Task() = default;
    /// Submit the task
    ///
    /// This will be called by the scheduler at most once when there
    /// is space to run the task.  This is expected to be a fairly quick
    /// function that simply submits the actual task work to an external
    /// resource (e.g. I/O thread pool).
    ///
    /// If this call fails then the scheduler will enter an aborted state.
    virtual Result<Future<>> operator()(AsyncTaskScheduler* scheduler) = 0;
    /// The cost of the task
    ///
    /// The scheduler limits the total number of concurrent tasks based
    /// on cost.  A custom cost may be used, for example, if you would like
    /// to limit the number of tasks based on the total expected RAM usage of
    /// the tasks (this is done in the scanner)
    virtual int cost() const { return 1; }
  };

  /// An interface for a task queue
  ///
  /// A queue's methods will not be called concurrently
  class Queue {
   public:
    virtual ~Queue() = default;
    /// Push a task to the queue
    virtual void Push(std::unique_ptr<Task> task) = 0;
    /// Pop the next task from the queue
    virtual std::unique_ptr<Task> Pop() = 0;
    /// Peek the next task in the queue
    virtual const Task& Peek() = 0;
    /// Check if the queue is empty
    virtual bool Empty() = 0;
    /// Purge the queue of all items
    virtual void Purge() = 0;
  };

  class Throttle {
   public:
    virtual ~Throttle() = default;
    /// Acquire amt permits
    ///
    /// If nullopt is returned then the permits were immediately
    /// acquired and the caller can proceed.  If a future is returned then the caller
    /// should wait for the future to complete first.  When the returned future completes
    /// the permits have NOT been acquired and the caller must call Acquire again
    virtual std::optional<Future<>> TryAcquire(int amt) = 0;
    /// Release amt permits
    ///
    /// This will possibly complete waiting futures and should probably not be
    /// called while holding locks.
    virtual void Release(int amt) = 0;

    /// The size of the largest task that can run
    ///
    /// Incoming tasks will have their cost latched to this value to ensure
    /// they can still run (although they will generally be the only thing allowed to
    /// run at that time).
    virtual int Capacity() = 0;
  };
  /// Create a throttle
  ///
  /// This throttle is used to limit how many tasks can run at once.  The
  /// user should keep the throttle alive for the lifetime of the scheduler.
  /// The same throttle can be used in multiple schedulers.
  static std::unique_ptr<Throttle> MakeThrottle(int max_concurrent_cost);

  /// Add a task to the scheduler
  ///
  /// If the scheduler is in an aborted state this call will return false and the task
  /// will never be run.  This is harmless and does not need to be guarded against.
  ///
  /// If there are no limits on the number of concurrent tasks then the submit function
  /// will be run immediately.
  ///
  /// Otherwise, if there is a throttle, and it is full, then this task will be inserted
  /// into the scheduler's queue and submitted later when there is space.
  ///
  /// The return value for this call can usually be ignored.  There is little harm in
  /// attempting to add tasks to an aborted scheduler.  It is only included for callers
  /// that want to avoid future task generation.
  ///
  /// \return true if the task was submitted or queued, false if the task was ignored
  virtual bool AddTask(std::unique_ptr<Task> task) = 0;

  /// Adds an async generator to the scheduler
  ///
  /// The async generator will be visited, one item at a time.  Submitting a task
  /// will consist of polling the generator for the next future.  The generator's future
  /// will then represent the task itself.
  ///
  /// This visits the task serially without readahead.  If readahead or parallelism
  /// is desired then it should be added in the generator itself.
  ///
  /// The tasks will be submitted to a subscheduler which will be ended when the generator
  /// is exhausted.
  ///
  /// The generator itself will be kept alive until all tasks have been completed.
  /// However, if the scheduler is aborted, the generator will be destroyed as soon as the
  /// next item would be requested.
  template <typename T>
  bool AddAsyncGenerator(std::function<Future<T>()> generator,
                         std::function<Status(const T&)> visitor,
                         FnOnce<Status(Status)> finish_callback) {
    struct State {
      State(std::function<Future<T>()> generator, std::function<Status(const T&)> visitor,
            AsyncTaskScheduler* scheduler)
          : generator(std::move(generator)),
            visitor(std::move(visitor)),
            scheduler(scheduler) {}
      std::function<Future<T>()> generator;
      std::function<Status(const T&)> visitor;
      AsyncTaskScheduler* scheduler;
    };
    struct SubmitTask : public Task {
      explicit SubmitTask(std::unique_ptr<State> state_holder)
          : state_holder(std::move(state_holder)) {}

      struct SubmitTaskCallback {
        SubmitTaskCallback(std::unique_ptr<State> state_holder, Future<> task_completion)
            : state_holder(std::move(state_holder)),
              task_completion(std::move(task_completion)) {}
        void operator()(const Result<T>& maybe_item) {
          if (!maybe_item.ok()) {
            task_completion.MarkFinished(maybe_item.status());
            return;
          }
          const auto& item = *maybe_item;
          if (IsIterationEnd(item)) {
            task_completion.MarkFinished();
            return;
          }
          Status visit_st = state_holder->visitor(item);
          if (!visit_st.ok()) {
            task_completion.MarkFinished(std::move(visit_st));
            return;
          }
          state_holder->scheduler->AddTask(
              std::make_unique<SubmitTask>(std::move(state_holder)));
          task_completion.MarkFinished();
        }
        std::unique_ptr<State> state_holder;
        Future<> task_completion;
      };

      Result<Future<>> operator()(AsyncTaskScheduler* scheduler) {
        Future<> task = Future<>::Make();
        // Consume as many items as we can (those that are already finished)
        // synchronously to avoid recursion / stack overflow.
        while (true) {
          Future<T> next = state_holder->generator();
          if (next.TryAddCallback(
                  [&] { return SubmitTaskCallback(std::move(state_holder), task); })) {
            return task;
          }
          ARROW_ASSIGN_OR_RAISE(T item, next.result());
          if (IsIterationEnd(item)) {
            task.MarkFinished();
            return task;
          }
          ARROW_RETURN_NOT_OK(state_holder->visitor(item));
        }
      }
      std::unique_ptr<State> state_holder;
    };
    MakeSubScheduler(
        [&](AsyncTaskScheduler* generator_scheduler) {
          std::unique_ptr<State> state_holder = std::make_unique<State>(
              std::move(generator), std::move(visitor), generator_scheduler);
          generator_scheduler->AddTask(
              std::make_unique<SubmitTask>(std::move(state_holder)));
          return Status::OK();
        },
        std::move(finish_callback));
    return true;
  }

  template <typename Callable>
  struct SimpleTask : public Task {
    explicit SimpleTask(Callable callable) : callable(std::move(callable)) {}
    Result<Future<>> operator()(AsyncTaskScheduler* scheduler) override {
      return callable();
    }
    Callable callable;
  };

  template <typename Callable>
  bool AddSimpleTask(Callable callable) {
    return AddTask(std::make_unique<SimpleTask<Callable>>(std::move(callable)));
  }

  class Holder {
   public:
    Holder() = default;
    virtual ~Holder() = default;
    ARROW_DEFAULT_MOVE_AND_ASSIGN(Holder);
    ARROW_DISALLOW_COPY_AND_ASSIGN(Holder);
    virtual void Reset() = 0;
    virtual AsyncTaskScheduler* Get() = 0;
  };

  /// Create a sub-scheduler for tracking a subset of tasks
  ///
  /// The parent scheduler will manage the lifetime of the sub-scheduler.  It will
  /// be destroyed once it is finished.
  ///
  /// Often some state needs to be associated with a subset of tasks.
  /// For example, when scanning a dataset we need to keep a file reader
  /// alive for all of the read tasks for each file. A sub-scheduler can be used to do
  /// this.
  ///
  /// This method returns a control that can be used to manage the lifetime of the
  /// sub-scheduler.  Callers are free to ignore this return value and the sub-scheduler
  /// will end when it runs out of tasks.  However, there are times when the full set of
  /// tasks is not known in advance.  For example, when writing a dataset to a file we
  /// create a sub-scheduler for the file.  We do not know how many batches will be
  /// written to a file and there may be long gaps when we have no batches to write to the
  /// file (and thus no tasks in the scheduler).  In this case a reference to the
  /// holder can be used to keep the sub-scheduler alive.
  ///
  /// Eagerly resetting the returned holder is always optional.  The returned holder will
  /// not keep the sub-scheduler alive longer than the parent scheduler.  Once the parent
  /// and child scheduler have run out of tasks the sub-scheduler will be destroyed (the
  /// holder will be invalidated and can still safely be destroyed after this).
  ///
  /// To summarize:
  ///
  ///   Ignored holder: The sub-scheduler will end as soon as the initial task (and all
  ///                   subsequent child tasks) finish.  Use this if you know all the work
  ///                   that needs to be done up front.
  ///   Holder kept "forever": The sub-scheduler will be destroyed when the parent
  ///                   scheduler is destroyed.  Do this for a "peer" scheduler whose
  ///                   lifetime is the same as the parent but has a special queue or
  ///                   throttle.
  ///   Holder eagerly reset: The sub-scheduler will be destroyed as soon as it runs out
  ///                   of tasks after the holder is reset.  This is done in the file
  ///                   writing example above when we realize we have finished with a file
  ///                   because we hit the max rows limit (which might happen well before
  ///                   the plan ends) so that we can close the file once all remaining
  ///                   write tasks wrap up.
  ///
  /// If either the parent scheduler or the sub-scheduler encounter an error
  /// then they will both enter an aborted state (this is a shared state).
  /// Finish callbacks will always be run only when the sub-scheduler's tasks
  /// have all been completed.
  ///
  /// The parent scheduler will not complete until the sub-scheduler's
  /// tasks (and finish callback) have all executed.
  ///
  /// A sub-scheduler can share the same throttle as its parent but it
  /// can also have its own unique throttle or no throttle
  virtual std::unique_ptr<Holder> MakeSubScheduler(
      FnOnce<Status(AsyncTaskScheduler*)> initial_task,
      FnOnce<Status(Status)> finish_callback, Throttle* throttle = NULLPTR,
      std::unique_ptr<Queue> queue = NULLPTR) = 0;

  /// Construct a scheduler
  ///
  /// \param initial_task The initial task which is responsible for adding
  ///        the first subtasks to the scheduler.
  /// \param throttle A throttle to control how many tasks will be submitted at one time.
  ///        The default (nullptr) will always submit tasks when they are added.
  /// \param queue A queue to control task order.  Only used if throttle != nullptr.
  ///        The default (nullptr) will use a FIFO queue if there is a throttle.
  /// \param stop_token An optional stop token that will allow cancellation of the
  ///        scheduler.  This will be checked before each task is submitted and, in the
  ///        event of a cancellation, the scheduler will enter an aborted state. This is
  ///        a graceful cancellation and submitted tasks will still complete.
  /// \return A future that will be completed when the initial task and all subtasks have
  ///         finished.
  static Future<> Make(FnOnce<Status(AsyncTaskScheduler*)> initial_task,
                       Throttle* throttle = NULLPTR,
                       std::unique_ptr<Queue> queue = NULLPTR,
                       StopToken stop_token = StopToken::Unstoppable());
};

}  // namespace util
}  // namespace arrow
