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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace internal {

// Generate random sleep durations
static std::vector<double> RandomSleepDurations(int nsleeps, double min_seconds,
                                                double max_seconds) {
  std::vector<double> sleeps;
  std::default_random_engine engine;
  std::uniform_real_distribution<> sleep_dist(min_seconds, max_seconds);
  for (int i = 0; i < nsleeps; ++i) {
    sleeps.push_back(sleep_dist(engine));
  }
  return sleeps;
}

// Check TaskGroup behaviour with a bunch of all-successful tasks
void TestTaskGroupSuccess(std::shared_ptr<TaskGroup> task_group) {
  const int NTASKS = 10;
  auto sleeps = RandomSleepDurations(NTASKS, 1e-3, 4e-3);

  // Add NTASKS sleeps
  std::atomic<int> count(0);
  for (int i = 0; i < NTASKS; ++i) {
    task_group->Append([&, i]() {
      SleepFor(sleeps[i]);
      count += i;
      return Status::OK();
    });
  }
  ASSERT_TRUE(task_group->ok());

  ASSERT_OK(task_group->Finish());
  ASSERT_TRUE(task_group->ok());
  ASSERT_EQ(count.load(), NTASKS * (NTASKS - 1) / 2);
  // Finish() is idempotent
  ASSERT_OK(task_group->Finish());
}

// Check TaskGroup behaviour with some successful and some failing tasks
void TestTaskGroupErrors(std::shared_ptr<TaskGroup> task_group) {
  const int NSUCCESSES = 2;
  const int NERRORS = 20;

  std::atomic<int> count(0);

  auto task_group_was_ok = false;
  task_group->Append([&]() -> Status {
    for (int i = 0; i < NSUCCESSES; ++i) {
      task_group->Append([&]() {
        count++;
        return Status::OK();
      });
    }
    task_group_was_ok = task_group->ok();
    for (int i = 0; i < NERRORS; ++i) {
      task_group->Append([&]() {
        SleepFor(1e-2);
        count++;
        return Status::Invalid("some message");
      });
    }

    return Status::OK();
  });

  // Task error is propagated
  ASSERT_RAISES(Invalid, task_group->Finish());
  ASSERT_TRUE(task_group_was_ok);
  ASSERT_FALSE(task_group->ok());
  if (task_group->parallelism() == 1) {
    // Serial: exactly two successes and an error
    ASSERT_EQ(count.load(), 3);
  } else {
    // Parallel: at least two successes and an error
    ASSERT_GE(count.load(), 3);
    ASSERT_LE(count.load(), 2 * task_group->parallelism());
  }
  // Finish() is idempotent
  ASSERT_RAISES(Invalid, task_group->Finish());
}

void TestTaskGroupCancel(std::shared_ptr<TaskGroup> task_group, StopSource* stop_source) {
  const int NSUCCESSES = 2;
  const int NCANCELS = 20;

  std::atomic<int> count(0);

  auto task_group_was_ok = false;
  task_group->Append([&]() -> Status {
    for (int i = 0; i < NSUCCESSES; ++i) {
      task_group->Append([&]() {
        count++;
        return Status::OK();
      });
    }
    task_group_was_ok = task_group->ok();
    for (int i = 0; i < NCANCELS; ++i) {
      task_group->Append([&]() {
        SleepFor(1e-2);
        stop_source->RequestStop();
        count++;
        return Status::OK();
      });
    }

    return Status::OK();
  });

  // Cancellation is propagated
  ASSERT_RAISES(Cancelled, task_group->Finish());
  ASSERT_TRUE(task_group_was_ok);
  ASSERT_FALSE(task_group->ok());
  if (task_group->parallelism() == 1) {
    // Serial: exactly three successes
    ASSERT_EQ(count.load(), NSUCCESSES + 1);
  } else {
    // Parallel: at least three successes
    ASSERT_GE(count.load(), NSUCCESSES + 1);
    ASSERT_LE(count.load(), NSUCCESSES * task_group->parallelism());
  }
  // Finish() is idempotent
  ASSERT_RAISES(Cancelled, task_group->Finish());
}

class CopyCountingTask {
 public:
  explicit CopyCountingTask(std::shared_ptr<uint8_t> target)
      : counter(0), target(std::move(target)) {}

  CopyCountingTask(const CopyCountingTask& other)
      : counter(other.counter + 1), target(other.target) {}

  CopyCountingTask& operator=(const CopyCountingTask& other) {
    counter = other.counter + 1;
    target = other.target;
    return *this;
  }

  CopyCountingTask(CopyCountingTask&& other) = default;
  CopyCountingTask& operator=(CopyCountingTask&& other) = default;

  Status operator()() {
    *target = counter;
    return Status::OK();
  }

 private:
  uint8_t counter;
  std::shared_ptr<uint8_t> target;
};

// Check TaskGroup behaviour with tasks spawning other tasks
void TestTasksSpawnTasks(std::shared_ptr<TaskGroup> task_group) {
  const int N = 6;

  std::atomic<int> count(0);
  // Make a task that recursively spawns itself
  std::function<std::function<Status()>(int)> make_task = [&](int i) {
    return [&, i]() {
      count++;
      if (i > 0) {
        // Exercise parallelism by spawning two tasks at once and then sleeping
        task_group->Append(make_task(i - 1));
        task_group->Append(make_task(i - 1));
        SleepFor(1e-3);
      }
      return Status::OK();
    };
  };

  task_group->Append(make_task(N));

  ASSERT_OK(task_group->Finish());
  ASSERT_TRUE(task_group->ok());
  ASSERT_EQ(count.load(), (1 << (N + 1)) - 1);
}

// A task that keeps recursing until a barrier is set.
// Using a lambda for this doesn't play well with Thread Sanitizer.
struct BarrierTask {
  std::atomic<bool>* barrier_;
  std::weak_ptr<TaskGroup> weak_group_ptr_;
  Status final_status_;

  Status operator()() {
    if (!barrier_->load()) {
      SleepFor(1e-5);
      // Note the TaskGroup should be kept alive by the fact this task
      // is still running...
      weak_group_ptr_.lock()->Append(*this);
    }
    return final_status_;
  }
};

// Try to replicate subtle lifetime issues when destroying a TaskGroup
// where all tasks may not have finished running.
void StressTaskGroupLifetime(std::function<std::shared_ptr<TaskGroup>()> factory) {
  const int NTASKS = 100;
  auto task_group = factory();
  auto weak_group_ptr = std::weak_ptr<TaskGroup>(task_group);

  std::atomic<bool> barrier(false);

  BarrierTask task{&barrier, weak_group_ptr, Status::OK()};

  for (int i = 0; i < NTASKS; ++i) {
    task_group->Append(task);
  }

  // Lose strong reference
  barrier.store(true);
  task_group.reset();

  // Wait for finish
  while (!weak_group_ptr.expired()) {
    SleepFor(1e-5);
  }
}

// Same, but with also a failing task
void StressFailingTaskGroupLifetime(std::function<std::shared_ptr<TaskGroup>()> factory) {
  const int NTASKS = 100;
  auto task_group = factory();
  auto weak_group_ptr = std::weak_ptr<TaskGroup>(task_group);

  std::atomic<bool> barrier(false);

  BarrierTask task{&barrier, weak_group_ptr, Status::OK()};
  BarrierTask failing_task{&barrier, weak_group_ptr, Status::Invalid("XXX")};

  for (int i = 0; i < NTASKS; ++i) {
    task_group->Append(task);
  }
  task_group->Append(failing_task);

  // Lose strong reference
  barrier.store(true);
  task_group.reset();

  // Wait for finish
  while (!weak_group_ptr.expired()) {
    SleepFor(1e-5);
  }
}

void TestNoCopyTask(std::shared_ptr<TaskGroup> task_group) {
  auto counter = std::make_shared<uint8_t>(0);
  CopyCountingTask task(counter);
  task_group->Append(std::move(task));
  ASSERT_OK(task_group->Finish());
  ASSERT_EQ(0, *counter);
}

void TestFinishNotSticky(std::function<std::shared_ptr<TaskGroup>()> factory) {
  // If a task is added that runs very quickly it might decrement the task counter back
  // down to 0 and mark the completion future as complete before all tasks are added.
  // The "finished future" of the task group could get stuck to complete.
  //
  // Instead the task group should not allow the finished future to be marked complete
  // until after FinishAsync has been called.
  const int NTASKS = 100;
  for (int i = 0; i < NTASKS; ++i) {
    auto task_group = factory();
    // Add a task and let it complete
    task_group->Append([] { return Status::OK(); });
    // Wait a little bit, if the task group was going to lock the finish hopefully it
    // would do so here while we wait
    SleepFor(1e-2);

    // Add a new task that will still be running
    std::atomic<bool> ready(false);
    std::mutex m;
    std::condition_variable cv;
    task_group->Append([&m, &cv, &ready] {
      std::unique_lock<std::mutex> lk(m);
      cv.wait(lk, [&ready] { return ready.load(); });
      return Status::OK();
    });

    // Ensure task group not finished already
    auto finished = task_group->FinishAsync();
    ASSERT_FALSE(finished.is_finished());

    std::unique_lock<std::mutex> lk(m);
    ready = true;
    lk.unlock();
    cv.notify_one();

    ASSERT_FINISHES_OK(finished);
  }
}

void TestFinishNeverStarted(std::shared_ptr<TaskGroup> task_group) {
  // If we call FinishAsync we are done adding tasks so if we never added any it should be
  // completed
  auto finished = task_group->FinishAsync();
  ASSERT_TRUE(finished.Wait(1));
}

void TestFinishAlreadyCompleted(std::function<std::shared_ptr<TaskGroup>()> factory) {
  // If we call FinishAsync we are done adding tasks so even if no tasks are running we
  // should still be completed
  const int NTASKS = 100;
  for (int i = 0; i < NTASKS; ++i) {
    auto task_group = factory();
    // Add a task and let it complete
    task_group->Append([] { return Status::OK(); });
    // Wait a little bit, hopefully enough time for the task to finish on one of these
    // iterations
    SleepFor(1e-2);
    auto finished = task_group->FinishAsync();
    ASSERT_FINISHES_OK(finished);
  }
}

TEST(SerialTaskGroup, Success) { TestTaskGroupSuccess(TaskGroup::MakeSerial()); }

TEST(SerialTaskGroup, Errors) { TestTaskGroupErrors(TaskGroup::MakeSerial()); }

TEST(SerialTaskGroup, Cancel) {
  StopSource stop_source;
  TestTaskGroupCancel(TaskGroup::MakeSerial(stop_source.token()), &stop_source);
}

TEST(SerialTaskGroup, TasksSpawnTasks) { TestTasksSpawnTasks(TaskGroup::MakeSerial()); }

TEST(SerialTaskGroup, NoCopyTask) { TestNoCopyTask(TaskGroup::MakeSerial()); }

TEST(SerialTaskGroup, FinishNeverStarted) {
  TestFinishNeverStarted(TaskGroup::MakeSerial());
}

TEST(SerialTaskGroup, FinishAlreadyCompleted) {
  TestFinishAlreadyCompleted([] { return TaskGroup::MakeSerial(); });
}

TEST(ThreadedTaskGroup, Success) {
  auto task_group = TaskGroup::MakeThreaded(GetCpuThreadPool());
  TestTaskGroupSuccess(task_group);
}

TEST(ThreadedTaskGroup, Errors) {
  // Limit parallelism to ensure some tasks don't get started
  // after the first failing ones
  std::shared_ptr<ThreadPool> thread_pool;
  ASSERT_OK_AND_ASSIGN(thread_pool, ThreadPool::Make(4));

  TestTaskGroupErrors(TaskGroup::MakeThreaded(thread_pool.get()));
}

TEST(ThreadedTaskGroup, Cancel) {
  std::shared_ptr<ThreadPool> thread_pool;
  ASSERT_OK_AND_ASSIGN(thread_pool, ThreadPool::Make(4));

  StopSource stop_source;
  TestTaskGroupCancel(TaskGroup::MakeThreaded(thread_pool.get(), stop_source.token()),
                      &stop_source);
}

TEST(ThreadedTaskGroup, TasksSpawnTasks) {
  auto task_group = TaskGroup::MakeThreaded(GetCpuThreadPool());
  TestTasksSpawnTasks(task_group);
}

TEST(ThreadedTaskGroup, NoCopyTask) {
  std::shared_ptr<ThreadPool> thread_pool;
  ASSERT_OK_AND_ASSIGN(thread_pool, ThreadPool::Make(4));
  TestNoCopyTask(TaskGroup::MakeThreaded(thread_pool.get()));
}

TEST(ThreadedTaskGroup, StressTaskGroupLifetime) {
  std::shared_ptr<ThreadPool> thread_pool;
  ASSERT_OK_AND_ASSIGN(thread_pool, ThreadPool::Make(16));

  StressTaskGroupLifetime([&] { return TaskGroup::MakeThreaded(thread_pool.get()); });
}

TEST(ThreadedTaskGroup, StressFailingTaskGroupLifetime) {
  std::shared_ptr<ThreadPool> thread_pool;
  ASSERT_OK_AND_ASSIGN(thread_pool, ThreadPool::Make(16));

  StressFailingTaskGroupLifetime(
      [&] { return TaskGroup::MakeThreaded(thread_pool.get()); });
}

TEST(ThreadedTaskGroup, FinishNotSticky) {
  std::shared_ptr<ThreadPool> thread_pool;
  ASSERT_OK_AND_ASSIGN(thread_pool, ThreadPool::Make(16));

  TestFinishNotSticky([&] { return TaskGroup::MakeThreaded(thread_pool.get()); });
}

TEST(ThreadedTaskGroup, FinishNeverStarted) {
  std::shared_ptr<ThreadPool> thread_pool;
  ASSERT_OK_AND_ASSIGN(thread_pool, ThreadPool::Make(4));
  TestFinishNeverStarted(TaskGroup::MakeThreaded(thread_pool.get()));
}

TEST(ThreadedTaskGroup, FinishAlreadyCompleted) {
  std::shared_ptr<ThreadPool> thread_pool;
  ASSERT_OK_AND_ASSIGN(thread_pool, ThreadPool::Make(16));

  TestFinishAlreadyCompleted([&] { return TaskGroup::MakeThreaded(thread_pool.get()); });
}

}  // namespace internal
}  // namespace arrow
