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
#include "arrow/testing/async_test_util.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/future.h"
#include "arrow/util/test_common.h"

namespace arrow {
namespace util {

constexpr std::string_view kDummyName = "unit test";

TEST(AsyncTaskScheduler, ShouldScheduleConcurrentTasks) {
  // A basic test to make sure we schedule the right number of concurrent tasks
  constexpr int kMaxConcurrentTasks = 2;
  constexpr int kTotalNumTasks = kMaxConcurrentTasks + 1;
  Future<> futures[kTotalNumTasks];
  bool submitted[kTotalNumTasks];
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    std::shared_ptr<ThrottledAsyncTaskScheduler> throttled =
        ThrottledAsyncTaskScheduler::Make(scheduler, kMaxConcurrentTasks);
    for (int i = 0; i < kTotalNumTasks; i++) {
      futures[i] = Future<>::Make();
      submitted[i] = false;
      throttled->AddSimpleTask(
          [&, i] {
            submitted[i] = true;
            return futures[i];
          },
          kDummyName);
    }
    return Status::OK();
  });
  AssertNotFinished(finished);
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
  ASSERT_FINISHES_OK(finished);
}

TEST(AsyncTaskScheduler, CancelWaitsForTasksToFinish) {
  StopSource stop_source;
  Future<> task = Future<>::Make();
  Future<> finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        scheduler->AddSimpleTask([&] { return task; }, kDummyName);
        return Status::OK();
      },
      /*abort_callback=*/[](const Status&) {}, stop_source.token());
  stop_source.RequestStop();
  AssertNotFinished(finished);
  task.MarkFinished();
  // We don't get a cancel error here which is ok because
  // we did ran all the tasks.
  ASSERT_FINISHES_OK(finished);
}

TEST(AsyncTaskScheduler, CancelPurgesQueuedTasks) {
  StopSource stop_source;
  Future<> task = Future<>::Make();
  bool second_task_submitted = false;
  Future<> finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        std::shared_ptr<ThrottledAsyncTaskScheduler> throttled =
            ThrottledAsyncTaskScheduler::Make(scheduler, 1);
        throttled->AddSimpleTask([&] { return task; }, kDummyName);
        throttled->AddSimpleTask(
            [&] {
              second_task_submitted = true;
              return Future<>::MakeFinished();
            },
            kDummyName);
        return Status::OK();
      },
      /*abort_callback=*/[](const Status&) {}, stop_source.token());
  stop_source.RequestStop();
  task.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Cancelled, finished);
  ASSERT_FALSE(second_task_submitted);
}

TEST(AsyncTaskScheduler, CancelPreventsAdditionalTasks) {
  StopSource stop_source;
  Future<> task = Future<>::Make();
  bool second_task_submitted = false;
  Future<> finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        scheduler->AddSimpleTask([&] { return task; }, kDummyName);
        stop_source.RequestStop();
        scheduler->AddSimpleTask(
            [&] {
              second_task_submitted = true;
              return task;
            },
            kDummyName);
        return Status::OK();
      },
      /*abort_callback=*/[](const Status&) {}, stop_source.token());
  task.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Cancelled, finished);
  ASSERT_FALSE(second_task_submitted);
}

TEST(AsyncTaskScheduler, AbortCallback) {
  // `task` simulates a long running task that will not end for a while.  The abort
  // callback ends the task early.
  Future<> task = Future<>::Make();
  Future<> finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        scheduler->AddSimpleTask([&] { return task; }, kDummyName);
        scheduler->AddSimpleTask([] { return Status::Invalid("XYZ"); }, kDummyName);
        return Status::OK();
      },
      [&](const Status& st) {
        ASSERT_TRUE(st.IsInvalid());
        task.MarkFinished();
      });
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
}

TEST(AsyncTaskScheduler, TaskStaysAliveUntilFinished) {
  bool my_task_destroyed = false;
  Future<> task = Future<>::Make();
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    struct MyTask : public AsyncTaskScheduler::Task {
      MyTask(bool* my_task_destroyed_ptr, Future<> task_fut)
          : my_task_destroyed_ptr(my_task_destroyed_ptr), task_fut(std::move(task_fut)) {}
      ~MyTask() { *my_task_destroyed_ptr = true; }
      Result<Future<>> operator()() override { return task_fut; }
      std::string_view name() const override { return kDummyName; }
      bool* my_task_destroyed_ptr;
      Future<> task_fut;
    };
    scheduler->AddTask(std::make_unique<MyTask>(&my_task_destroyed, task));
    return Status::OK();
  });
  SleepABit();
  ASSERT_FALSE(my_task_destroyed);
  task.MarkFinished();
  ASSERT_TRUE(my_task_destroyed);
  ASSERT_FINISHES_OK(finished);
}

TEST(AsyncTaskScheduler, InitialTaskAddsNothing) {
  Future<> finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) { return Status::OK(); });
  ASSERT_FINISHES_OK(finished);
}

TEST(AsyncTaskScheduler, InitialTaskFails) {
  Future<> task = Future<>::Make();
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    EXPECT_TRUE(scheduler->AddSimpleTask([&]() { return task; }, kDummyName));
    return Status::Invalid("XYZ");
  });
  AssertNotFinished(finished);
  task.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);

  finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) { return Status::Invalid("XYZ"); });
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
}

TEST(AsyncTaskScheduler, TaskGroup) {
  Future<> task = Future<>::Make();
  bool finish_callback_ran = false;
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    std::unique_ptr<AsyncTaskGroup> task_group = AsyncTaskGroup::Make(scheduler, [&] {
      finish_callback_ran = true;
      return Status::OK();
    });
    EXPECT_TRUE(task_group->AddSimpleTask([&]() { return task; }, kDummyName));
    return Status::OK();
  });
  ASSERT_FALSE(finish_callback_ran);
  AssertNotFinished(finished);
  task.MarkFinished();
  ASSERT_FINISHES_OK(finished);
  ASSERT_TRUE(finish_callback_ran);
}

TEST(AsyncTaskScheduler, TaskGroupLifetime) {
  Future<> task = Future<>::Make();
  bool finish_callback_ran = false;
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    std::unique_ptr<AsyncTaskGroup> task_group = AsyncTaskGroup::Make(scheduler, [&] {
      finish_callback_ran = true;
      return Status::OK();
    });
    EXPECT_TRUE(task_group->AddSimpleTask([&]() { return task; }, kDummyName));
    // Last task in group is finished but we still have a reference to the group (and
    // could still add tasks) so the finish callback does not run
    task.MarkFinished();
    EXPECT_FALSE(finish_callback_ran);
    return Status::OK();
  });
  ASSERT_FINISHES_OK(finished);
  ASSERT_TRUE(finish_callback_ran);
}

TEST(AsyncTaskScheduler, TaskGroupNoTasks) {
  Future<> task = Future<>::Make();
  bool finish_callback_ran = false;
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    std::unique_ptr<AsyncTaskGroup> task_group = AsyncTaskGroup::Make(scheduler, [&] {
      finish_callback_ran = true;
      return Status::OK();
    });
    EXPECT_FALSE(finish_callback_ran);
    return Status::OK();
  });
  ASSERT_FINISHES_OK(finished);
  ASSERT_TRUE(finish_callback_ran);
}

TEST(AsyncTaskScheduler, TaskGroupFinishCallbackFails) {
  Future<> task = Future<>::Make();
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    std::unique_ptr<AsyncTaskGroup> task_group =
        AsyncTaskGroup::Make(scheduler, [&] { return Status::Invalid("XYZ"); });
    EXPECT_TRUE(task_group->AddSimpleTask([&]() { return task; }, kDummyName));
    // Last task in group is finished but we still have a reference to the group (and
    // could still add tasks) so the finish callback does not run
    return Status::OK();
  });
  AssertNotFinished(finished);
  task.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
}

#ifndef ARROW_VALGRIND
TEST(AsyncTaskScheduler, FailingTaskStress) {
  // Test many tasks failing at the same time
  constexpr int kNumTasks = 256;
  for (int i = 0; i < kNumTasks; i++) {
    Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
      EXPECT_TRUE(scheduler->AddSimpleTask([] { return SleepABitAsync(); }, kDummyName));
      EXPECT_TRUE(scheduler->AddSimpleTask(
          [] { return SleepABitAsync().Then([]() { return Status::Invalid("XYZ"); }); },
          kDummyName));
      return Status::OK();
    });
    ASSERT_FINISHES_AND_RAISES(Invalid, finished);
  }
  for (int i = 0; i < kNumTasks; i++) {
    Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
      std::unique_ptr<AsyncTaskGroup> task_group =
          AsyncTaskGroup::Make(scheduler, [] { return Status::OK(); });
      EXPECT_TRUE(task_group->AddSimpleTask([] { return SleepABitAsync(); }, kDummyName));
      EXPECT_TRUE(task_group->AddSimpleTask(
          [] { return SleepABitAsync().Then([]() { return Status::Invalid("XYZ"); }); },
          kDummyName));
      return Status::OK();
    });
    ASSERT_FINISHES_AND_RAISES(Invalid, finished);
  }
}
#endif

TEST(AsyncTaskScheduler, AsyncGenerator) {
  for (bool slow : {false, true}) {
    std::vector<TestInt> values{1, 2, 3};
    std::vector<TestInt> seen_values{};
    ARROW_SCOPED_TRACE("Slow: ", slow);
    Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
      AsyncGenerator<TestInt> generator = MakeVectorGenerator<TestInt>(values);
      if (slow) {
        generator = util::SlowdownABit(generator);
      }
      std::function<Status(const TestInt&)> visitor = [&](const TestInt& val) {
        seen_values.push_back(val);
        return Status::OK();
      };
      scheduler->AddAsyncGenerator(std::move(generator), std::move(visitor), kDummyName);
      return Status::OK();
    });
    ASSERT_FINISHES_OK(finished);
    ASSERT_EQ(seen_values, values);
  }
}

class CustomThrottle : public ThrottledAsyncTaskScheduler::Throttle {
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
  int Capacity() { return std::numeric_limits<int>::max(); }
  virtual void Pause() { FAIL() << "Should not get here."; }
  virtual void Resume() { FAIL() << "Should not get here."; }

 private:
  Future<> gate_ = Future<>::Make();
};

TEST(AsyncTaskScheduler, Throttle) {
  // Queued tasks should still be executed and should block completion of the scheduler
  Future<> slow_task = Future<>::Make();
  bool was_run = false;
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    std::shared_ptr<ThrottledAsyncTaskScheduler> throttled =
        ThrottledAsyncTaskScheduler::Make(scheduler, 1);
    EXPECT_TRUE(throttled->AddSimpleTask([slow_task] { return slow_task; }, kDummyName));
    EXPECT_TRUE(throttled->AddSimpleTask(
        [&was_run] {
          was_run = true;
          return Future<>::MakeFinished();
        },
        kDummyName));
    EXPECT_FALSE(was_run);
    return Status::OK();
  });
  slow_task.MarkFinished();
  ASSERT_FINISHES_OK(finished);
  ASSERT_TRUE(was_run);

  /// Same test but block task by custom throttle
  was_run = false;
  auto custom_throttle = std::make_unique<CustomThrottle>();
  CustomThrottle* custom_throttle_view = custom_throttle.get();
  finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    std::shared_ptr<ThrottledAsyncTaskScheduler> throttled =
        ThrottledAsyncTaskScheduler::MakeWithCustomThrottle(scheduler,
                                                            std::move(custom_throttle));
    EXPECT_TRUE(throttled->AddSimpleTask(
        [&was_run] {
          was_run = true;
          return Future<>::MakeFinished();
        },
        kDummyName));
    EXPECT_FALSE(was_run);
    custom_throttle_view->Unlock();
    return Status::OK();
  });
  ASSERT_FINISHES_OK(finished);
  ASSERT_TRUE(was_run);
}

TEST(AsyncTaskScheduler, TaskWithCostBiggerThanThrottle) {
  // It can be difficult to know the maximum cost a task may have.  In
  // scanning this is the maximum size of a batch stored on disk which we
  // cannot know ahead of time.  So a task may have a cost greater than the
  // size of the throttle.  In that case we simply drop the cost to the
  // capacity of the throttle.
  constexpr int kThrottleCapacity = 5;
  bool task_submitted = false;
  Future<> blocking_task = Future<>::Make();
  Future<> task = Future<>::Make();
  struct ExpensiveTask : AsyncTaskScheduler::Task {
    ExpensiveTask(bool* task_submitted, Future<> task)
        : task_submitted(task_submitted), task(std::move(task)) {}
    Result<Future<>> operator()() override {
      *task_submitted = true;
      return task;
    }
    int cost() const override { return kThrottleCapacity * 50; }
    std::string_view name() const override { return kDummyName; }
    bool* task_submitted;
    Future<> task;
  };
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    std::shared_ptr<ThrottledAsyncTaskScheduler> throttled =
        ThrottledAsyncTaskScheduler::Make(scheduler, kThrottleCapacity);
    EXPECT_TRUE(throttled->AddSimpleTask([&] { return blocking_task; }, kDummyName));
    EXPECT_TRUE(
        throttled->AddTask(std::make_unique<ExpensiveTask>(&task_submitted, task)));
    return Status::OK();
  });

  // Task should not be submitted initially because blocking_task (even though
  // it has a cost of 1) is preventing it.
  ASSERT_FALSE(task_submitted);
  blocking_task.MarkFinished();
  // One blocking_task is out of the way the task is free to run
  ASSERT_TRUE(task_submitted);
  task.MarkFinished();
  ASSERT_FINISHES_OK(finished);
}

TEST(AsyncTaskScheduler, TaskFinishesAfterError) {
  /// If a task fails it shouldn't impact previously submitted tasks
  Future<> fut1 = Future<>::Make();
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    EXPECT_TRUE(scheduler->AddSimpleTask([fut1] { return fut1; }, kDummyName));
    EXPECT_TRUE(scheduler->AddSimpleTask(
        [] { return Future<>::MakeFinished(Status::Invalid("XYZ")); }, kDummyName));
    return Status::OK();
  });
  AssertNotFinished(finished);
  fut1.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
}

TEST(AsyncTaskScheduler, FailAfterAdd) {
  /// If a task fails it shouldn't impact tasks that have been submitted
  /// even if they were submitted later
  Future<> will_fail = Future<>::Make();
  Future<> added_later_and_passes = Future<>::Make();
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    EXPECT_TRUE(scheduler->AddSimpleTask([will_fail] { return will_fail; }, kDummyName));
    EXPECT_TRUE(scheduler->AddSimpleTask(
        [added_later_and_passes] { return added_later_and_passes; }, kDummyName));
    will_fail.MarkFinished(Status::Invalid("XYZ"));
    EXPECT_FALSE(scheduler->AddSimpleTask([] { return Future<>::Make(); }, kDummyName));
    return Status::OK();
  });
  AssertNotFinished(finished);
  added_later_and_passes.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
}

TEST(AsyncTaskScheduler, PurgeUnsubmitted) {
  // If a task fails then unsubmitted tasks should not be executed
  Future<> will_fail = Future<>::Make();
  bool was_submitted = false;
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    std::shared_ptr<ThrottledAsyncTaskScheduler> throttled =
        ThrottledAsyncTaskScheduler::Make(scheduler, 1);
    EXPECT_TRUE(throttled->AddSimpleTask([will_fail] { return will_fail; }, kDummyName));
    EXPECT_TRUE(throttled->AddSimpleTask(
        [&was_submitted] {
          was_submitted = true;
          return Future<>::MakeFinished();
        },
        kDummyName));
    will_fail.MarkFinished(Status::Invalid("XYZ"));
    return Status::OK();
  });
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
  ASSERT_FALSE(was_submitted);

  // Purge might still be needed when done with initial task too
  will_fail = Future<>::Make();
  Future<> slow_task_that_passes = Future<>::Make();
  was_submitted = false;
  finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    std::shared_ptr<ThrottledAsyncTaskScheduler> throttled =
        ThrottledAsyncTaskScheduler::Make(scheduler, 2);
    EXPECT_TRUE(throttled->AddSimpleTask([will_fail] { return will_fail; }, kDummyName));
    EXPECT_TRUE(throttled->AddSimpleTask(
        [slow_task_that_passes] { return slow_task_that_passes; }, kDummyName));
    EXPECT_TRUE(throttled->AddSimpleTask(
        [&was_submitted] {
          was_submitted = true;
          return Future<>::MakeFinished();
        },
        kDummyName));
    return Status::OK();
  });
  will_fail.MarkFinished(Status::Invalid("XYZ"));
  slow_task_that_passes.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
  ASSERT_FALSE(was_submitted);
}

#ifndef ARROW_VALGRIND
TEST(AsyncTaskScheduler, FifoStress) {
  // Regresses an issue where adding a task, when the throttle was
  // just cleared, could lead to the added task being run immediately,
  // even though there were queued tasks.
  constexpr int kNumIters = 100;
  for (int i = 0; i < kNumIters; i++) {
    std::atomic<bool> middle_task_run{false};
    Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
      std::shared_ptr<ThrottledAsyncTaskScheduler> throttled =
          ThrottledAsyncTaskScheduler::Make(scheduler, 1);
      throttled->AddSimpleTask([] { return SleepABitAsync(); }, kDummyName);
      throttled->AddSimpleTask(
          [&] {
            middle_task_run = true;
            return Future<>::MakeFinished();
          },
          kDummyName);
      SleepABit();
      throttled->AddSimpleTask(
          [&] {
            EXPECT_TRUE(middle_task_run);
            return Future<>::MakeFinished();
          },
          kDummyName);
      return Status::OK();
    });
    ASSERT_FINISHES_OK(finished);
  }
}

TEST(AsyncTaskScheduler, MaxConcurrentTasksStress) {
  constexpr int kNumIters = 100;
  constexpr int kNumTasks = 32;
  constexpr int kNumConcurrentTasks = 8;
  for (int i = 0; i < kNumIters; i++) {
    std::atomic<int> num_tasks_running{0};
    Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
      std::shared_ptr<ThrottledAsyncTaskScheduler> throttled =
          ThrottledAsyncTaskScheduler::Make(scheduler, kNumConcurrentTasks);
      for (int task_idx = 0; task_idx < kNumTasks; task_idx++) {
        throttled->AddSimpleTask(
            [&num_tasks_running, kNumConcurrentTasks] {
              if (num_tasks_running.fetch_add(1) > kNumConcurrentTasks) {
                ADD_FAILURE() << "More than " << kNumConcurrentTasks
                              << " tasks were allowed to run concurrently";
              }
              return SleepABitAsync().Then(
                  [&num_tasks_running] { num_tasks_running.fetch_sub(1); });
            },
            kDummyName);
      }
      return Status::OK();
    });
    ASSERT_FINISHES_OK(finished);
  }
}

TEST(AsyncTaskScheduler, ScanningStress) {
  // Simulates the scanner's use of the scheduler
  // The top level scheduler scans over fragments and
  // for each fragment a task group is created that scans
  // that fragment.  The task groups all share a common throttle
  constexpr int kNumIters = 16;
  constexpr int kNumFragments = 16;
  constexpr int kBatchesPerFragment = 8;
  constexpr int kNumConcurrentTasks = 4;
  constexpr int kExpectedBatchesScanned = kNumFragments * kBatchesPerFragment;

  for (int i = 0; i < kNumIters; i++) {
    std::atomic<int> batches_scanned{0};
    auto scan_batch = [&] { batches_scanned++; };
    auto submit_scan = [&]() { return SleepABitAsync().Then(scan_batch); };
    Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
      std::shared_ptr<ThrottledAsyncTaskScheduler> throttled =
          ThrottledAsyncTaskScheduler::Make(scheduler, kNumConcurrentTasks);
      auto list_fragment = [&, throttled]() {
        std::unique_ptr<AsyncTaskGroup> task_group =
            AsyncTaskGroup::Make(throttled.get(), [] { return Status::OK(); });
        for (int i = 0; i < kBatchesPerFragment; i++) {
          EXPECT_TRUE(task_group->AddSimpleTask(submit_scan, kDummyName));
        }
        return Status::OK();
      };
      auto submit_list_fragment = [&]() { return SleepABitAsync().Then(list_fragment); };
      for (int frag_idx = 0; frag_idx < kNumFragments; frag_idx++) {
        EXPECT_TRUE(scheduler->AddSimpleTask(submit_list_fragment, kDummyName));
      }
      return Status::OK();
    });
    ASSERT_FINISHES_OK(finished);
    ASSERT_EQ(kExpectedBatchesScanned, batches_scanned.load());
  }
}
#endif

class TaskWithPriority : public AsyncTaskScheduler::Task {
 public:
  TaskWithPriority(std::function<Result<Future<>>()> task, int priority)
      : task(std::move(task)), priority(priority) {}
  Result<Future<>> operator()() override { return task(); }
  std::string_view name() const override { return kDummyName; }

  std::function<Result<Future<>>()> task;
  int priority;
};

struct TaskWithPriorityCompare {
  bool operator()(TaskWithPriority* left, TaskWithPriority* right) {
    return left->priority < right->priority;
  }
};

// A priority queue that prefers tasks with higher priority
class PriorityQueue : public ThrottledAsyncTaskScheduler::Queue {
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

  std::shared_ptr<GatingTask> gate = GatingTask::Make();
  int submit_order[kNumTasks];
  std::atomic<int> order_index{0};

  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    std::shared_ptr<ThrottledAsyncTaskScheduler> throttled =
        ThrottledAsyncTaskScheduler::Make(scheduler, kNumConcurrentTasks,
                                          std::make_unique<PriorityQueue>());
    for (int task_idx = 0; task_idx < kNumTasks; task_idx++) {
      int priority = task_idx;
      std::function<Result<Future<>>()> task_exec = [&, priority]() -> Result<Future<>> {
        submit_order[order_index++] = priority;
        return gate->AsyncTask();
      };
      auto task = std::make_unique<TaskWithPriority>(task_exec, priority);
      throttled->AddTask(std::move(task));
    }
    return Status::OK();
  });

  AssertNotFinished(finished);

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
