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

#include "arrow/acero/task_util.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>

#include <gtest/gtest.h>

#include "arrow/acero/util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::ThreadPool;

namespace acero {

/// \brief Create a thread pool and start all threads
///
/// By default a thread pool will not create threads until they
/// are actually needed.  This can make it a bit difficult to
/// reproduce certain issues.  This creates a thread pool and
/// then makes sure the threads are actually created before
/// returning it.
Result<std::shared_ptr<ThreadPool>> MakePrimedThreadPool(int num_threads) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ThreadPool> thread_pool,
                        ThreadPool::Make(num_threads));
  int num_threads_running = 0;
  std::mutex mutex;
  std::condition_variable thread_gate;
  std::condition_variable primer_gate;
  for (int i = 0; i < num_threads; i++) {
    // This shouldn't fail and, if it fails midway, we will have some threads
    // still running if we do RETURN_NOT_OK so lets do ABORT_NOT_OK
    ABORT_NOT_OK(thread_pool->Spawn([&] {
      std::unique_lock<std::mutex> lk(mutex);
      num_threads_running++;
      primer_gate.notify_one();
      thread_gate.wait(lk);
    }));
  }
  std::unique_lock<std::mutex> primer_lock(mutex);
  primer_gate.wait(primer_lock, [&] { return num_threads_running == num_threads; });
  thread_gate.notify_all();
  primer_lock.unlock();
  thread_pool->WaitForIdle();
  return thread_pool;
}

Status SlowTaskImpl(std::size_t, int64_t) {
  SleepABit();
  return Status::OK();
}
Status FastTaskImpl(std::size_t, int64_t) { return Status::OK(); }
// If this is the last task group then start the next stage
TaskScheduler::TaskGroupContinuationImpl MakeContinuation(
    std::atomic<int>* counter, std::function<void(std::size_t, int)> start_next_stage,
    int next_stage) {
  return [counter, start_next_stage, next_stage](std::size_t thread_id) {
    if (counter->fetch_sub(1) == 1) {
      start_next_stage(thread_id, next_stage);
    }
    return Status::OK();
  };
}
// Signal the cv if this is the last group
TaskScheduler::TaskGroupContinuationImpl MakeFinalContinuation(
    std::atomic<int>* counter, std::mutex* mutex, std::condition_variable* finish) {
  return [=](std::size_t thread_id) {
    if (counter->fetch_sub(1) == 1) {
      std::lock_guard<std::mutex> lg(*mutex);
      finish->notify_one();
    }
    return Status::OK();
  };
}

// This test simulates one of the current use patterns of the
// task scheduler.  There are a number of groups.  The groups
// are allocated to stages.  All groups in a stage execute
// concurrently.  When all groups in that stage finish the next
// stage is started.
TEST(TaskScheduler, Stress) {
  constexpr int kNumThreads = 8;
  constexpr int kNumGroups = 8;
  constexpr int kGroupsPerStage = 3;
  constexpr int kTasksPerGroup = 32;
  constexpr int kNumStages = (kNumGroups % kGroupsPerStage == 0)
                                 ? (kNumGroups / kGroupsPerStage)
                                 : (kNumGroups / kGroupsPerStage) + 1;
  constexpr int kTrailingGroups = (kNumGroups % kGroupsPerStage == 0)
                                      ? kGroupsPerStage
                                      : kNumGroups % kGroupsPerStage;

  ThreadIndexer thread_indexer;
  int num_threads = std::min(static_cast<int>(thread_indexer.Capacity()), kNumThreads);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ThreadPool> thread_pool,
                       ThreadPool::Make(num_threads));

  std::array<std::atomic<int>, kNumStages - 1> stage_counters;
  for (int i = 0; i < kNumStages - 1; i++) {
    stage_counters[i].store(kGroupsPerStage);
  }
  std::atomic<int> final_counter(kTrailingGroups);
  std::mutex mutex;
  std::condition_variable finish_cv;

  std::vector<int> group_ids;
  auto scheduler = TaskScheduler::Make();

  std::function<void(std::size_t, int)> start_next_stage = [&](std::size_t thread_id,
                                                               int stage_index) {
    int start = stage_index * kGroupsPerStage;
    int end = std::min(kNumGroups, start + kGroupsPerStage);
    for (int i = start; i < end; i++) {
      ASSERT_OK(thread_pool->Spawn([&, i] {
        std::size_t my_thread_id = thread_indexer();
        SleepABit();
        ASSERT_OK(scheduler->StartTaskGroup(my_thread_id, group_ids[i], kTasksPerGroup));
      }));
    }
  };

  for (auto i = 0; i < kNumGroups; i++) {
    int next_stage = (i / kGroupsPerStage) + 1;
    TaskScheduler::TaskGroupContinuationImpl finish =
        MakeFinalContinuation(&final_counter, &mutex, &finish_cv);
    if (next_stage < kNumStages) {
      finish =
          MakeContinuation(&stage_counters[next_stage - 1], start_next_stage, next_stage);
    }
    group_ids.push_back(scheduler->RegisterTaskGroup(SlowTaskImpl, finish));
  }
  scheduler->RegisterEnd();

  TaskScheduler::AbortContinuationImpl abort = [] { FAIL() << "Unexpected abort"; };
  TaskScheduler::ScheduleImpl schedule =
      [&](TaskScheduler::TaskGroupContinuationImpl task) {
        return thread_pool->Spawn([&, task] {
          std::size_t thread_id = thread_indexer();
          ASSERT_OK(task(thread_id));
        });
      };
  std::unique_lock<std::mutex> lock(mutex);
  ASSERT_OK(thread_pool->Spawn([&] {
    std::size_t thread_id = thread_indexer();
    ASSERT_OK(scheduler->StartScheduling(thread_id, schedule, num_threads * 4, false));
    start_next_stage(thread_id, 0);
  }));

  finish_cv.wait(lock);
  thread_pool->WaitForIdle();
}

// This is a reproducer for a bug that was encountered when one
// thread starts a task group while another thread is finishing
// the last of its tasks.
TEST(TaskScheduler, StressTwo) {
  constexpr int kNumThreads = 16;
  constexpr int kNumGroups = 8;
  constexpr int kTasksPerGroup = 1;
  constexpr int kIterations = 1000;

  ThreadIndexer thread_indexer;
  int num_threads = std::min(static_cast<int>(thread_indexer.Capacity()), kNumThreads);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ThreadPool> thread_pool,
                       MakePrimedThreadPool(num_threads));

  for (int i = 0; i < kIterations; i++) {
    std::atomic<int> final_counter(kNumGroups);
    std::mutex mutex;
    std::condition_variable finish_cv;

    std::vector<int> group_ids;
    auto scheduler = TaskScheduler::Make();

    for (auto i = 0; i < kNumGroups; i++) {
      TaskScheduler::TaskGroupContinuationImpl finish =
          MakeFinalContinuation(&final_counter, &mutex, &finish_cv);
      group_ids.push_back(scheduler->RegisterTaskGroup(FastTaskImpl, finish));
    }
    scheduler->RegisterEnd();

    TaskScheduler::AbortContinuationImpl abort = [] { FAIL() << "Unexpected abort"; };
    TaskScheduler::ScheduleImpl schedule =
        [&](TaskScheduler::TaskGroupContinuationImpl task) {
          return thread_pool->Spawn([&, task] {
            std::size_t thread_id = thread_indexer();
            ASSERT_OK(task(thread_id));
          });
        };

    ASSERT_OK(scheduler->StartScheduling(0, schedule, num_threads * 4, false));
    std::unique_lock<std::mutex> lock(mutex);
    for (int i = 0; i < kNumGroups; i++) {
      ASSERT_OK(thread_pool->Spawn([&, i] {
        std::size_t thread_id = thread_indexer();
        ASSERT_OK(scheduler->StartTaskGroup(thread_id, i, kTasksPerGroup));
      }));
    }

    finish_cv.wait(lock);
    thread_pool->WaitForIdle();
  }
}

}  // namespace acero
}  // namespace arrow
