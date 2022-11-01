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

FnOnce<Status(Status)> EmptyFinishCallback() {
  return [](Status) { return Status::OK(); };
}

TEST(AsyncTaskScheduler, ShouldScheduleConcurrentTasks) {
  // A basic test to make sure we schedule the right number of concurrent tasks
  constexpr int kMaxConcurrentTasks = 2;
  constexpr int kTotalNumTasks = kMaxConcurrentTasks + 1;
  std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
      AsyncTaskScheduler::MakeThrottle(kMaxConcurrentTasks);
  Future<> futures[kTotalNumTasks];
  bool submitted[kTotalNumTasks];
  Future<> finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        for (int i = 0; i < kTotalNumTasks; i++) {
          futures[i] = Future<>::Make();
          submitted[i] = false;
          scheduler->AddSimpleTask([&, i] {
            submitted[i] = true;
            return futures[i];
          });
        }
        return Status::OK();
      },
      throttle.get());
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

TEST(AsyncTaskScheduler, SubSchedulerHolderLeftForever) {
  // This tests the "peer scheduler" case where a sub-scheduler
  // holder is left forever (and thus should end when the parent
  // scheduler ends)
  Future<> parent_scheduler_task = Future<>::Make();
  Future<> child_scheduler_task = Future<>::Make();
  // Holder kept alive outside of any task scheduler.
  std::unique_ptr<AsyncTaskScheduler::Holder> holder;
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    scheduler->AddSimpleTask([&] { return parent_scheduler_task; });
    holder = scheduler->MakeSubScheduler(
        [&](AsyncTaskScheduler* sub_scheduler) {
          sub_scheduler->AddSimpleTask([&] { return child_scheduler_task; });
          return Status::OK();
        },
        EmptyFinishCallback());
    return Status::OK();
  });
  parent_scheduler_task.MarkFinished();
  child_scheduler_task.MarkFinished();
  AssertNotFinished(finished);
  // If this holder were never destroyed then it would deadlock.  Care
  // should be taken to ensure the holder is destroyed.
  holder->Reset();
  ASSERT_FINISHES_OK(finished);

  // Holder destroyed when parent scheduler destroyed
  parent_scheduler_task = Future<>::Make();
  child_scheduler_task = Future<>::Make();
  finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    scheduler->AddSimpleTask([&] { return parent_scheduler_task; });
    std::shared_ptr<AsyncTaskScheduler::Holder> inner_holder =
        scheduler->MakeSubScheduler(
            [&](AsyncTaskScheduler* sub_scheduler) {
              sub_scheduler->AddSimpleTask([&] { return child_scheduler_task; });
              return Status::OK();
            },
            EmptyFinishCallback());
    return Status::OK();
  });
  child_scheduler_task.MarkFinished();
  parent_scheduler_task.MarkFinished();
  ASSERT_FINISHES_OK(finished);

  // Holders should also be kept alive by grandparent tasks (and great-grandparent, etc.)
  Future<> grandparent_scheduler_task = Future<>::Make();
  parent_scheduler_task = Future<>::Make();
  bool child_scheduler_finished = false;
  {
    std::unique_ptr<AsyncTaskScheduler::Holder> scoped_holder;
    finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
      scheduler->AddSimpleTask([&] { return grandparent_scheduler_task; });
      scheduler->MakeSubScheduler(
          [&](AsyncTaskScheduler* mid_scheduler) {
            mid_scheduler->AddSimpleTask([&] { return parent_scheduler_task; });
            scoped_holder = mid_scheduler->MakeSubScheduler(
                [&](AsyncTaskScheduler* sub_scheduler) { return Status::OK(); },
                [&](const Status& st) {
                  child_scheduler_finished = true;
                  return Status::OK();
                });
            return Status::OK();
          },
          EmptyFinishCallback());
      return Status::OK();
    });
    parent_scheduler_task.MarkFinished();
    ASSERT_FALSE(child_scheduler_finished);
    grandparent_scheduler_task.MarkFinished();
    AssertNotFinished(finished);
  }
  ASSERT_FINISHES_OK(finished);
}

TEST(AsyncTaskScheduler, CancelWaitsForTasksToFinish) {
  StopSource stop_source;
  Future<> task = Future<>::Make();
  Future<> finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        scheduler->AddSimpleTask([&] { return task; });
        return Status::OK();
      },
      nullptr, nullptr, stop_source.token());
  stop_source.RequestStop();
  AssertNotFinished(finished);
  task.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Cancelled, finished);
}

TEST(AsyncTaskScheduler, CancelPurgesQueuedTasks) {
  StopSource stop_source;
  Future<> task = Future<>::Make();
  std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
      AsyncTaskScheduler::MakeThrottle(1);
  bool second_task_submitted = false;
  Future<> finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        scheduler->AddSimpleTask([&] { return task; });
        scheduler->AddSimpleTask([&] {
          second_task_submitted = true;
          return Future<>::MakeFinished();
        });
        return Status::OK();
      },
      throttle.get(), nullptr, stop_source.token());
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
        scheduler->AddSimpleTask([&] { return task; });
        stop_source.RequestStop();
        scheduler->AddSimpleTask([&] {
          second_task_submitted = true;
          return task;
        });
        return Status::OK();
      },
      nullptr, nullptr, stop_source.token());
  task.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Cancelled, finished);
  ASSERT_FALSE(second_task_submitted);
}

TEST(AsyncTaskScheduler, TaskStaysAliveUntilFinished) {
  bool my_task_destroyed = false;
  Future<> task = Future<>::Make();
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    struct MyTask : public AsyncTaskScheduler::Task {
      MyTask(bool* my_task_destroyed_ptr, Future<> task_fut)
          : my_task_destroyed_ptr(my_task_destroyed_ptr), task_fut(std::move(task_fut)) {}
      ~MyTask() { *my_task_destroyed_ptr = true; }
      Result<Future<>> operator()(AsyncTaskScheduler*) { return task_fut; }
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
    EXPECT_TRUE(scheduler->AddSimpleTask([&]() { return task; }));
    return Status::Invalid("XYZ");
  });
  AssertNotFinished(finished);
  task.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);

  finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) { return Status::Invalid("XYZ"); });
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
}

#ifndef ARROW_VALGRIND
TEST(AsyncTaskScheduler, FailingTaskStress) {
  // Test many tasks failing at the same time
  constexpr int kNumTasks = 256;
  for (int i = 0; i < kNumTasks; i++) {
    Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
      EXPECT_TRUE(scheduler->AddSimpleTask([] { return SleepABitAsync(); }));
      EXPECT_TRUE(scheduler->AddSimpleTask(
          [] { return SleepABitAsync().Then([]() { return Status::Invalid("XYZ"); }); }));
      return Status::OK();
    });
    ASSERT_FINISHES_AND_RAISES(Invalid, finished);
  }
  for (int i = 0; i < kNumTasks; i++) {
    Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
      scheduler->MakeSubScheduler(
          [&](AsyncTaskScheduler* sub_scheduler) {
            EXPECT_TRUE(sub_scheduler->AddSimpleTask([] { return SleepABitAsync(); }));
            EXPECT_TRUE(sub_scheduler->AddSimpleTask([] {
              return SleepABitAsync().Then([]() { return Status::Invalid("XYZ"); });
            }));
            return Status::OK();
          },
          EmptyFinishCallback());
      return Status::OK();
    });
    ASSERT_FINISHES_AND_RAISES(Invalid, finished);
  }
  // Test many schedulers failing at the same time (also a mixture of failing due
  // to global abort racing with local task failure)
  constexpr int kNumSchedulers = 32;
  for (int i = 0; i < kNumTasks; i++) {
    Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
      std::vector<Future<>> tests;
      for (int i = 0; i < kNumSchedulers; i++) {
        tests.push_back(SleepABitAsync().Then([&] {
          scheduler->MakeSubScheduler(
              [&](AsyncTaskScheduler* sub_scheduler) {
                std::ignore = sub_scheduler->AddSimpleTask([] {
                  return SleepABitAsync().Then([]() { return Status::Invalid("XYZ"); });
                });
                return Status::OK();
              },
              EmptyFinishCallback());
        }));
      }
      AllComplete(std::move(tests)).Wait();
      return Status::OK();
    });
    ASSERT_FINISHES_AND_RAISES(Invalid, finished);
  }
}
#endif

TEST(AsyncTaskScheduler, SubSchedulerFinishCallback) {
  bool finish_callback_ran = false;
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    scheduler->MakeSubScheduler(
        [&](AsyncTaskScheduler* sub_scheduler) {
          EXPECT_FALSE(finish_callback_ran);
          return Status::OK();
        },
        [&](Status) {
          finish_callback_ran = true;
          return Status::OK();
        });
    return Status::OK();
  });
  EXPECT_TRUE(finish_callback_ran);
  ASSERT_FINISHES_OK(finished);

  // Finish callback should run even if sub scheduler never starts any tasks
  // because the parent scheduler was already aborted
  finish_callback_ran = false;
  finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    EXPECT_TRUE(scheduler->AddSimpleTask([] { return Status::Invalid("XYZ"); }));
    {
      scheduler->MakeSubScheduler(
          [&](AsyncTaskScheduler* sub_scheduler) {
            EXPECT_FALSE(finish_callback_ran);
            return Status::OK();
          },
          [&](Status) {
            finish_callback_ran = true;
            return Status::OK();
          });
    }
    return Status::OK();
  });
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
  ASSERT_TRUE(finish_callback_ran);

  // Finish callback should run even if scheduler is in aborted state when the
  // sub-scheduler finishes
  finish_callback_ran = false;
  finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* scheduler) {
    {
      Future<> task = Future<>::Make();
      scheduler->AddSimpleTask([&] { return task; });
      scheduler->MakeSubScheduler(
          [&](AsyncTaskScheduler* sub_scheduler) mutable {
            task.MarkFinished(Status::Invalid("XYZ"));
            EXPECT_FALSE(finish_callback_ran);
            return Status::OK();
          },
          [&](Status) {
            finish_callback_ran = true;
            return Status::OK();
          });
    }
    EXPECT_TRUE(finish_callback_ran);
    return Status::OK();
  });
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
}

TEST(AsyncTaskScheduler, SubSchedulerNoticesParentAbort) {
  Future<> finished = AsyncTaskScheduler::Make([&](AsyncTaskScheduler* parent) {
    std::unique_ptr<AsyncTaskScheduler::Throttle> child_throttle =
        AsyncTaskScheduler::MakeThrottle(1);
    parent->MakeSubScheduler(
        [&](AsyncTaskScheduler* child) {
          Future<> task = Future<>::Make();
          bool was_submitted = false;
          EXPECT_TRUE(child->AddSimpleTask([task] { return task; }));
          EXPECT_TRUE(child->AddSimpleTask([&was_submitted] {
            was_submitted = true;
            return Future<>::MakeFinished();
          }));
          EXPECT_TRUE(parent->AddSimpleTask([] { return Status::Invalid("XYZ"); }));
          EXPECT_FALSE(child->AddSimpleTask([task] { return task; }));
          task.MarkFinished();
          return Status::OK();
        },
        EmptyFinishCallback(), child_throttle.get());
    return Status::OK();
  });
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
}

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
      scheduler->AddAsyncGenerator(std::move(generator), std::move(visitor),
                                   EmptyFinishCallback());
      return Status::OK();
    });
    ASSERT_FINISHES_OK(finished);
    ASSERT_EQ(seen_values, values);
  }
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
  int Capacity() { return std::numeric_limits<int>::max(); }

 private:
  Future<> gate_ = Future<>::Make();
};

TEST(AsyncTaskScheduler, EndWaitsForAddedButNotSubmittedTasks) {
  /// If a scheduler ends then unsubmitted tasks should still be executed
  std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
      AsyncTaskScheduler::MakeThrottle(1);
  Future<> slow_task = Future<>::Make();
  bool was_run = false;
  Future<> finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        EXPECT_TRUE(scheduler->AddSimpleTask([slow_task] { return slow_task; }));
        EXPECT_TRUE(scheduler->AddSimpleTask([&was_run] {
          was_run = true;
          return Future<>::MakeFinished();
        }));
        EXPECT_FALSE(was_run);
        return Status::OK();
      },
      throttle.get());
  slow_task.MarkFinished();
  ASSERT_FINISHES_OK(finished);
  ASSERT_TRUE(was_run);

  /// Same test but block task by custom throttle
  auto custom_throttle = std::make_unique<CustomThrottle>();
  was_run = false;
  finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        EXPECT_TRUE(scheduler->AddSimpleTask([&was_run] {
          was_run = true;
          return Future<>::MakeFinished();
        }));
        EXPECT_FALSE(was_run);
        return Status::OK();
      },
      custom_throttle.get());
  custom_throttle->Unlock();
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
  std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
      AsyncTaskScheduler::MakeThrottle(kThrottleCapacity);
  bool task_submitted = false;
  Future<> blocking_task = Future<>::Make();
  Future<> task = Future<>::Make();
  struct ExpensiveTask : AsyncTaskScheduler::Task {
    ExpensiveTask(bool* task_submitted, Future<> task)
        : task_submitted(task_submitted), task(std::move(task)) {}
    Result<Future<>> operator()(AsyncTaskScheduler*) override {
      *task_submitted = true;
      return task;
    }
    int cost() const override { return kThrottleCapacity * 50; }
    bool* task_submitted;
    Future<> task;
  };
  Future<> finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        EXPECT_TRUE(scheduler->AddSimpleTask([&] { return blocking_task; }));
        EXPECT_TRUE(
            scheduler->AddTask(std::make_unique<ExpensiveTask>(&task_submitted, task)));
        return Status::OK();
      },
      throttle.get());

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
    EXPECT_TRUE(scheduler->AddSimpleTask([fut1] { return fut1; }));
    EXPECT_TRUE(scheduler->AddSimpleTask(
        [] { return Future<>::MakeFinished(Status::Invalid("XYZ")); }));
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
    EXPECT_TRUE(scheduler->AddSimpleTask([will_fail] { return will_fail; }));
    EXPECT_TRUE(scheduler->AddSimpleTask(
        [added_later_and_passes] { return added_later_and_passes; }));
    will_fail.MarkFinished(Status::Invalid("XYZ"));
    EXPECT_FALSE(scheduler->AddSimpleTask([] { return Future<>::Make(); }));
    return Status::OK();
  });
  AssertNotFinished(finished);
  added_later_and_passes.MarkFinished();
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
}

TEST(AsyncTaskScheduler, PurgeUnsubmitted) {
  // If a task fails then unsubmitted tasks should not be executed
  std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
      AsyncTaskScheduler::MakeThrottle(1);
  Future<> will_fail = Future<>::Make();
  bool was_submitted = false;
  Future<> finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        EXPECT_TRUE(scheduler->AddSimpleTask([will_fail] { return will_fail; }));
        EXPECT_TRUE(scheduler->AddSimpleTask([&was_submitted] {
          was_submitted = true;
          return Future<>::MakeFinished();
        }));
        will_fail.MarkFinished(Status::Invalid("XYZ"));
        return Status::OK();
      },
      throttle.get());
  ASSERT_FINISHES_AND_RAISES(Invalid, finished);
  ASSERT_FALSE(was_submitted);

  // Purge might still be needed when done with initial task too
  throttle = AsyncTaskScheduler::MakeThrottle(2);
  will_fail = Future<>::Make();
  Future<> slow_task_that_passes = Future<>::Make();
  was_submitted = false;
  finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        EXPECT_TRUE(scheduler->AddSimpleTask([will_fail] { return will_fail; }));
        EXPECT_TRUE(scheduler->AddSimpleTask(
            [slow_task_that_passes] { return slow_task_that_passes; }));
        EXPECT_TRUE(scheduler->AddSimpleTask([&was_submitted] {
          was_submitted = true;
          return Future<>::MakeFinished();
        }));
        return Status::OK();
      },
      throttle.get());
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
    std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
        AsyncTaskScheduler::MakeThrottle(1);
    Future<> finished = AsyncTaskScheduler::Make(
        [&](AsyncTaskScheduler* scheduler) {
          scheduler->AddSimpleTask([] { return SleepABitAsync(); });
          scheduler->AddSimpleTask([&] {
            middle_task_run = true;
            return Future<>::MakeFinished();
          });
          SleepABit();
          scheduler->AddSimpleTask([&] {
            EXPECT_TRUE(middle_task_run);
            return Future<>::MakeFinished();
          });
          return Status::OK();
        },
        throttle.get());
    ASSERT_FINISHES_OK(finished);
  }
}

TEST(AsyncTaskScheduler, MaxConcurrentTasksStress) {
  constexpr int kNumIters = 100;
  constexpr int kNumTasks = 32;
  constexpr int kNumConcurrentTasks = 8;
  for (int i = 0; i < kNumIters; i++) {
    std::unique_ptr<AsyncTaskScheduler::Throttle> throttle =
        AsyncTaskScheduler::MakeThrottle(kNumConcurrentTasks);
    std::atomic<int> num_tasks_running{0};
    Future<> finished = AsyncTaskScheduler::Make(
        [&](AsyncTaskScheduler* scheduler) {
          for (int task_idx = 0; task_idx < kNumTasks; task_idx++) {
            scheduler->AddSimpleTask([&num_tasks_running, kNumConcurrentTasks] {
              if (num_tasks_running.fetch_add(1) > kNumConcurrentTasks) {
                ADD_FAILURE() << "More than " << kNumConcurrentTasks
                              << " tasks were allowed to run concurrently";
              }
              return SleepABitAsync().Then(
                  [&num_tasks_running] { num_tasks_running.fetch_sub(1); });
            });
          }
          return Status::OK();
        },
        throttle.get());
    ASSERT_FINISHES_OK(finished);
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
    std::atomic<int> batches_scanned{0};
    auto scan_batch = [&] { batches_scanned++; };
    auto submit_scan = [&]() { return SleepABitAsync().Then(scan_batch); };
    std::unique_ptr<AsyncTaskScheduler::Throttle> batch_limit =
        AsyncTaskScheduler::MakeThrottle(kNumConcurrentTasks);
    Future<> finished =
        AsyncTaskScheduler::Make([&](AsyncTaskScheduler* listing_scheduler) {
          auto list_fragment = [&, listing_scheduler]() {
            listing_scheduler->MakeSubScheduler(
                [&](AsyncTaskScheduler* batch_scheduler) {
                  for (int i = 0; i < kBatchesPerFragment; i++) {
                    EXPECT_TRUE(batch_scheduler->AddSimpleTask(submit_scan));
                  }
                  return Status::OK();
                },
                EmptyFinishCallback(), batch_limit.get());
          };
          auto submit_list_fragment = [&]() {
            return SleepABitAsync().Then(list_fragment);
          };
          for (int frag_idx = 0; frag_idx < kNumFragments; frag_idx++) {
            EXPECT_TRUE(listing_scheduler->AddSimpleTask(submit_list_fragment));
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

  std::shared_ptr<GatingTask> gate = GatingTask::Make();
  int submit_order[kNumTasks];
  std::atomic<int> order_index{0};

  Future<> finished = AsyncTaskScheduler::Make(
      [&](AsyncTaskScheduler* scheduler) {
        for (int task_idx = 0; task_idx < kNumTasks; task_idx++) {
          int priority = task_idx;
          std::function<Result<Future<>>()> task_exec = [&,
                                                         priority]() -> Result<Future<>> {
            submit_order[order_index++] = priority;
            return gate->AsyncTask();
          };
          auto task = std::make_unique<TaskWithPriority>(task_exec, priority);
          scheduler->AddTask(std::move(task));
        }
        return Status::OK();
      },
      throttle.get(), std::make_unique<PriorityQueue>());

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
