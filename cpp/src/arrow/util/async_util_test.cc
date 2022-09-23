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

#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_set>

#include <gtest/gtest.h>

#include "arrow/result.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/future.h"

namespace arrow {
namespace util {

TEST(AsyncTaskScheduler, ShouldScheduleConcurrentTasks) {
  constexpr int kMaxConcurrentTasks = 2;
  constexpr int kTotalNumTasks = kMaxConcurrentTasks + 1;
  std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
      AsyncTaskScheduler::MakeThrottle(kMaxConcurrentTasks);
  // A basic test to make sure we schedule the right number of concurrent tasks
  std::unique_ptr<AsyncTaskScheduler> task_group =
      AsyncTaskScheduler::Make(throttle.get());
  Future<> futures[kTotalNumTasks];
  bool submitted[kTotalNumTasks];
  for (int i = 0; i < kTotalNumTasks; i++) {
    futures[i] = Future<>::Make();
    submitted[i] = false;
    task_group->AddSimpleTask([&, i] {
      submitted[i] = true;
      return futures[i];
    });
  }
  task_group->End();
  AssertNotFinished(task_group->OnFinished());
  for (int i = 0; i < kTotalNumTasks; i++) {
    if (i < kMaxConcurrentTasks) {
      ASSERT_TRUE(submitted[i]);
    } else {
      ASSERT_FALSE(submitted[i]);
    }
  }

  for (int j = 0; j < kTotalNumTasks; j++) {
    futures[j].MarkFinished();
    if (j + kMaxConcurrentTasks < kTotalNumTasks) {
      ASSERT_TRUE(submitted[j + kMaxConcurrentTasks]);
    }
  }
  ASSERT_FINISHES_OK(task_group->OnFinished());
}

TEST(AsyncTaskScheduler, Abandoned) {
  // The goal here is to ensure that an abandoned scheduler aborts.
  // It should block until all submitted tasks finish.  It should not
  // submit any pending tasks.
  bool submitted_task_finished = false;
  bool pending_task_submitted = false;
  std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
      AsyncTaskScheduler::MakeThrottle(1);
  Future<> finished_fut;
  Future<> first_task = Future<>::Make();
  AsyncTaskScheduler* weak_scheduler;
  std::thread delete_scheduler_thread;
  {
    std::unique_ptr<AsyncTaskScheduler> scheduler =
        AsyncTaskScheduler::Make(throttle.get());
    weak_scheduler = scheduler.get();
    finished_fut = scheduler->OnFinished();
    // This task will start and should finish
    scheduler->AddSimpleTask([&, first_task] {
      return first_task.Then([&] { submitted_task_finished = true; });
    });
    // This task will never be submitted
    scheduler->AddSimpleTask([&] {
      pending_task_submitted = true;
      return Future<>::MakeFinished();
    });
    // We don't want to finish the first task until after the scheduler has been abandoned
    // and entered an aborted state.  However, deleting the scheduler blocks until the
    // first task is finished.  So we trigger the delete on a separate thread.
    struct DeleteSchedulerTask {
      void operator()() { scheduler.reset(); }
      std::unique_ptr<AsyncTaskScheduler> scheduler;
    };
    delete_scheduler_thread = std::thread(DeleteSchedulerTask{std::move(scheduler)});
  }
  // Here we are waiting for the scheduler to enter aborted state.  Once aborted the
  // scheduler will no longer accept new tasks and will return false.
  BusyWait(10, [&] {
    SleepABit();
    return !weak_scheduler->AddSimpleTask([] { return Future<>::MakeFinished(); });
  });
  // Now that the scheduler deletion is in progress we should be able to finish the
  // first task and be confident the second task should not be submitted.
  first_task.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(UnknownError, finished_fut);
  delete_scheduler_thread.join();
  ASSERT_TRUE(submitted_task_finished);
  ASSERT_FALSE(pending_task_submitted);
}

TEST(AsyncTaskScheduler, TaskFailsAfterEnd) {
  std::unique_ptr<AsyncTaskScheduler> scheduler = AsyncTaskScheduler::Make();
  Future<> task = Future<>::Make();
  scheduler->AddSimpleTask([task] { return task; });
  scheduler->End();
  AssertNotFinished(scheduler->OnFinished());
  task.MarkFinished(Status::Invalid("XYZ"));
  ASSERT_FINISHES_AND_RAISES(Invalid, scheduler->OnFinished());
}

TEST(AsyncTaskScheduler, SubSchedulerFinishCallback) {
  bool finish_callback_ran = false;
  std::unique_ptr<AsyncTaskScheduler> scheduler = AsyncTaskScheduler::Make();
  AsyncTaskScheduler* sub_scheduler = scheduler->MakeSubScheduler([&] {
    finish_callback_ran = true;
    return Status::OK();
  });
  ASSERT_FALSE(finish_callback_ran);
  sub_scheduler->End();
  ASSERT_TRUE(finish_callback_ran);
  scheduler->End();
  ASSERT_FINISHES_OK(scheduler->OnFinished());
}

TEST(AsyncTaskScheduler, SubSchedulerFinishAbort) {
  bool finish_callback_ran = false;
  std::unique_ptr<AsyncTaskScheduler> scheduler = AsyncTaskScheduler::Make();
  AsyncTaskScheduler* sub_scheduler = scheduler->MakeSubScheduler([&] {
    finish_callback_ran = true;
    return Status::Invalid("XYZ");
  });
  ASSERT_FALSE(finish_callback_ran);
  sub_scheduler->End();
  ASSERT_TRUE(finish_callback_ran);
  scheduler->End();
  ASSERT_FINISHES_AND_RAISES(Invalid, scheduler->OnFinished());
}

FnOnce<Status()> EmptyFinishCallback() {
  return [] { return Status::OK(); };
}

TEST(AsyncTaskScheduler, SubSchedulerNoticesParentAbort) {
  std::unique_ptr<AsyncTaskScheduler> parent = AsyncTaskScheduler::Make();
  std::unique_ptr<AsyncTaskScheduler::Throttle> child_throttle =
      AsyncTaskScheduler::MakeThrottle(1);
  AsyncTaskScheduler* child =
      parent->MakeSubScheduler(EmptyFinishCallback(), child_throttle.get());

  Future<> task = Future<>::Make();
  bool was_submitted = false;
  ASSERT_TRUE(child->AddSimpleTask([task] { return task; }));
  ASSERT_TRUE(child->AddSimpleTask([&was_submitted] {
    was_submitted = true;
    return Future<>::MakeFinished();
  }));
  ASSERT_TRUE(parent->AddSimpleTask([] { return Status::Invalid("XYZ"); }));
  ASSERT_FALSE(child->AddSimpleTask([task] { return task; }));
  task.MarkFinished();
  child->End();
  parent->End();
  ASSERT_FINISHES_AND_RAISES(Invalid, parent->OnFinished());
}

TEST(AsyncTaskScheduler, SubSchedulerNoTasks) {
  // An unended sub-scheduler should keep the parent scheduler unfinished even if there
  // there are no tasks.
  std::unique_ptr<AsyncTaskScheduler> parent = AsyncTaskScheduler::Make();
  AsyncTaskScheduler* child = parent->MakeSubScheduler(EmptyFinishCallback());
  parent->End();
  AssertNotFinished(parent->OnFinished());
  child->End();
  ASSERT_FINISHES_OK(parent->OnFinished());
}

class CustomThrottle : public AsyncTaskScheduler::Throttle {
 public:
  virtual std::optional<Future<>> TryAcquire(int amt) {
    if (gate_.is_finished()) {
      return std::nullopt;
    } else {
      return gate_;
    }
  }
  virtual void Release(int amt) {}
  void Unlock() { gate_.MarkFinished(); }

 private:
  Future<> gate_ = Future<>::Make();
};

TEST(AsyncTaskScheduler, EndWaitsForAddedButNotSubmittedTasks) {
  /// If a scheduler ends then unsubmitted tasks should still be executed
  std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
      AsyncTaskScheduler::MakeThrottle(1);
  std::unique_ptr<AsyncTaskScheduler> task_group =
      AsyncTaskScheduler::Make(throttle.get());
  Future<> slow_task = Future<>::Make();
  bool was_run = false;
  ASSERT_TRUE(task_group->AddSimpleTask([slow_task] { return slow_task; }));
  ASSERT_TRUE(task_group->AddSimpleTask([&was_run] {
    was_run = true;
    return Future<>::MakeFinished();
  }));
  ASSERT_FALSE(was_run);
  task_group->End();
  slow_task.MarkFinished();
  ASSERT_FINISHES_OK(task_group->OnFinished());
  ASSERT_TRUE(was_run);

  /// Same test but block task by custom throttle
  auto custom_throttle = std::make_unique<CustomThrottle>();
  task_group = AsyncTaskScheduler::Make(custom_throttle.get());
  was_run = false;
  ASSERT_TRUE(task_group->AddSimpleTask([&was_run] {
    was_run = true;
    return Future<>::MakeFinished();
  }));
  ASSERT_FALSE(was_run);
  task_group->End();
  custom_throttle->Unlock();
  ASSERT_FINISHES_OK(task_group->OnFinished());
  ASSERT_TRUE(was_run);
}

TEST(AsyncTaskScheduler, TaskFinishesAfterError) {
  /// If a task fails it shouldn't impact previously submitted tasks
  std::unique_ptr<AsyncTaskScheduler> task_group = AsyncTaskScheduler::Make();
  Future<> fut1 = Future<>::Make();
  ASSERT_TRUE(task_group->AddSimpleTask([fut1] { return fut1; }));
  ASSERT_TRUE(task_group->AddSimpleTask(
      [] { return Future<>::MakeFinished(Status::Invalid("XYZ")); }));
  task_group->End();
  Future<> finished_fut = task_group->OnFinished();
  AssertNotFinished(finished_fut);
  fut1.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Invalid, finished_fut);
}

TEST(AsyncTaskScheduler, FailAfterAdd) {
  /// If a task fails it shouldn't impact tasks that have been submitted
  /// even if they were submitted later
  std::unique_ptr<AsyncTaskScheduler> task_group = AsyncTaskScheduler::Make();
  Future<> will_fail = Future<>::Make();
  ASSERT_TRUE(task_group->AddSimpleTask([will_fail] { return will_fail; }));
  Future<> added_later_and_passes = Future<>::Make();
  ASSERT_TRUE(task_group->AddSimpleTask(
      [added_later_and_passes] { return added_later_and_passes; }));
  will_fail.MarkFinished(Status::Invalid("XYZ"));
  ASSERT_FALSE(task_group->AddSimpleTask([] { return Future<>::Make(); }));
  task_group->End();
  Future<> finished_fut = task_group->OnFinished();
  AssertNotFinished(finished_fut);
  added_later_and_passes.MarkFinished();
  AssertFinished(finished_fut);
  ASSERT_FINISHES_AND_RAISES(Invalid, finished_fut);
}

TEST(AsyncTaskScheduler, PurgeUnsubmitted) {
  /// If a task fails then unsubmitted tasks should not be executed
  std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
      AsyncTaskScheduler::MakeThrottle(1);
  std::unique_ptr<AsyncTaskScheduler> task_group =
      AsyncTaskScheduler::Make(throttle.get());
  Future<> will_fail = Future<>::Make();
  ASSERT_TRUE(task_group->AddSimpleTask([will_fail] { return will_fail; }));
  bool was_submitted = false;
  ASSERT_TRUE(task_group->AddSimpleTask([&was_submitted] {
    was_submitted = false;
    return Future<>::MakeFinished();
  }));
  will_fail.MarkFinished(Status::Invalid("XYZ"));
  task_group->End();
  Future<> finished_fut = task_group->OnFinished();
  ASSERT_FINISHES_AND_RAISES(Invalid, finished_fut);
  ASSERT_FALSE(was_submitted);
}

TEST(AsyncTaskScheduler, FifoStress) {
  // Regresses an issue where adding a task, when the throttle was
  // just cleared, could lead to the added task being run immediately,
  // even though there were queued tasks.
  constexpr int kNumIters = 100;
  for (int i = 0; i < kNumIters; i++) {
    std::atomic<bool> middle_task_run{false};
    std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
        AsyncTaskScheduler::MakeThrottle(1);
    std::unique_ptr<AsyncTaskScheduler> task_group =
        AsyncTaskScheduler::Make(throttle.get());
    task_group->AddSimpleTask([] { return SleepABitAsync(); });
    task_group->AddSimpleTask([&] {
      middle_task_run = true;
      return Future<>::MakeFinished();
    });
    SleepABit();
    task_group->AddSimpleTask([&] {
      EXPECT_TRUE(middle_task_run);
      return Future<>::MakeFinished();
    });
  }
}

TEST(AsyncTaskScheduler, MaxConcurrentTasksStress) {
  constexpr int kNumIters = 100;
  constexpr int kNumTasks = 32;
  constexpr int kNumConcurrentTasks = 8;
  for (int i = 0; i < kNumIters; i++) {
    std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
        AsyncTaskScheduler::MakeThrottle(kNumConcurrentTasks);
    std::unique_ptr<AsyncTaskScheduler> task_group =
        AsyncTaskScheduler::Make(throttle.get());
    std::atomic<int> num_tasks_running{0};
    for (int task_idx = 0; task_idx < kNumTasks; task_idx++) {
      task_group->AddSimpleTask([&num_tasks_running, kNumConcurrentTasks] {
        if (num_tasks_running.fetch_add(1) > kNumConcurrentTasks) {
          ADD_FAILURE() << "More than " << kNumConcurrentTasks
                        << " tasks were allowed to run concurrently";
        }
        return SleepABitAsync().Then(
            [&num_tasks_running] { num_tasks_running.fetch_sub(1); });
      });
    }
    task_group->End();
    ASSERT_FINISHES_OK(task_group->OnFinished());
  }
}

TEST(AsyncTaskScheduler, ScanningStress) {
  // Simulates the scanner's use of the scheduler
  // The top level scheduler scans over fragments and
  // for each fragment a sub-scheduler is created that scans
  // that fragment.  The sub-schedulers all share a common throttle
  /// If a task fails then unsubmitted tasks should not be executed
  constexpr int kNumIters = 16;
  constexpr int kNumFragments = 16;
  constexpr int kBatchesPerFragment = 8;
  constexpr int kNumConcurrentTasks = 4;
  constexpr int kExpectedBatchesScanned = kNumFragments * kBatchesPerFragment;

  for (int i = 0; i < kNumIters; i++) {
    std::unique_ptr<AsyncTaskScheduler::Throttle> batch_limit =
        AsyncTaskScheduler::MakeThrottle(kNumConcurrentTasks);
    std::unique_ptr<AsyncTaskScheduler> listing_scheduler = AsyncTaskScheduler::Make();
    std::atomic<int> batches_scanned{0};
    std::atomic<int> fragments_scanned{0};
    auto scan_batch = [&] { batches_scanned++; };
    auto submit_scan = [&]() { return SleepABitAsync().Then(scan_batch); };
    auto list_fragment = [&]() {
      AsyncTaskScheduler* batch_scheduler =
          listing_scheduler->MakeSubScheduler(EmptyFinishCallback(), batch_limit.get());
      for (int i = 0; i < kBatchesPerFragment; i++) {
        ASSERT_TRUE(batch_scheduler->AddSimpleTask(submit_scan));
      }
      batch_scheduler->End();
      if (++fragments_scanned == kNumFragments) {
        listing_scheduler->End();
      }
    };
    auto submit_list_fragment = [&]() { return SleepABitAsync().Then(list_fragment); };
    for (int frag_idx = 0; frag_idx < kNumFragments; frag_idx++) {
      ASSERT_TRUE(listing_scheduler->AddSimpleTask(submit_list_fragment));
    }
    ASSERT_FINISHES_OK(listing_scheduler->OnFinished());
    ASSERT_EQ(kExpectedBatchesScanned, batches_scanned.load());
  }
}

class TaskWithPriority : public AsyncTaskScheduler::Task {
 public:
  TaskWithPriority(std::function<Result<Future<>>()> task, int priority)
      : task(std::move(task)), priority(priority) {}
  Result<Future<>> operator()(AsyncTaskScheduler* scheduler) override { return task(); }

  std::function<Result<Future<>>()> task;
  int priority;
};

struct TaskWithPriorityCompare {
  bool operator()(TaskWithPriority* left, TaskWithPriority* right) {
    return left->priority < right->priority;
  }
};

// A priority queue that prefers tasks with higher priority
class PriorityQueue : public AsyncTaskScheduler::Queue {
 public:
  using Task = AsyncTaskScheduler::Task;
  void Push(std::unique_ptr<Task> task) {
    queue_.push(static_cast<TaskWithPriority*>(task.release()));
  }
  std::unique_ptr<Task> Pop() {
    TaskWithPriority* top = queue_.top();
    queue_.pop();
    return std::unique_ptr<Task>(top);
  }
  const Task& Peek() { return *queue_.top(); }
  bool Empty() { return queue_.empty(); }
  void Purge() {
    while (!queue_.empty()) {
      queue_.pop();
    }
  }

 private:
  std::priority_queue<TaskWithPriority*, std::vector<TaskWithPriority*>,
                      TaskWithPriorityCompare>
      queue_;
};

TEST(AsyncTaskScheduler, Priority) {
  constexpr int kNumTasks = 32;
  constexpr int kNumConcurrentTasks = 8;
  std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
      AsyncTaskScheduler::MakeThrottle(kNumConcurrentTasks);
  std::unique_ptr<AsyncTaskScheduler> task_group = AsyncTaskScheduler::Make(
      throttle.get(), std::make_unique<PriorityQueue>());

  std::shared_ptr<GatingTask> gate = GatingTask::Make();
  int submit_order[kNumTasks];
  std::atomic<int> order_index{0};

  for (int task_idx = 0; task_idx < kNumTasks; task_idx++) {
    int priority = task_idx;
    std::function<Result<Future<>>()> task_exec = [&, priority]() -> Result<Future<>> {
      submit_order[order_index++] = priority;
      return gate->AsyncTask();
    };
    auto task = std::make_unique<TaskWithPriority>(task_exec, priority);
    task_group->AddTask(std::move(task));
  }
  task_group->End();
  AssertNotFinished(task_group->OnFinished());

  ASSERT_OK(gate->WaitForRunning(kNumConcurrentTasks));
  ASSERT_OK(gate->Unlock());

  for (int i = 0; i < kNumConcurrentTasks; i++) {
    // The first tasks will be submitted immediately since the queue is empty
    ASSERT_EQ(submit_order[i], i);
  }
  // After that the remaining tasks will run in LIFO order because of the priority
  for (int i = kNumConcurrentTasks; i < kNumTasks; i++) {
    ASSERT_EQ(submit_order[i], kNumTasks - i - 1 + kNumConcurrentTasks);
  }
}

}  // namespace util
}  // namespace arrow
