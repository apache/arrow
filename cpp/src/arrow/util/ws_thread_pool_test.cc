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

/// Task queue tests

#include <iostream>
#include <thread>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/ws_thread_pool.h"

namespace arrow {
namespace internal {

using Task = ThreadPool::Task;

class WorkStealingTest : public ::testing::Test {
 protected:
  template <int Size>
  struct Tasks {
    Tasks() { Reset(); }

    Task* GetTask(int i) { return tasks[i].get(); }

    // Used when the task finish order is known, verifies the correct task finished
    void VerifyTask(Task* task, int expected_index) {
      ASSERT_FALSE(finished[expected_index])
          << " expected task " << expected_index
          << " just finished now but it was finished already";
      std::move(task->callable)();
      ASSERT_TRUE(finished[expected_index])
          << " expected task " << expected_index
          << " to finish but some other task was given instead";
    }

    // FinishTask + VerifyAllTasksFinished is used when the task finish order is
    // not known
    void FinishTask(Task* task) { std::move(task->callable)(); }

    void VerifyAllTasksFinished(int expected_num_tasks) {
      for (int i = 0; i < expected_num_tasks; i++) {
        ASSERT_TRUE(finished[i])
            << "Expected task " << i << " to be finished but it was not";
      }
    }

    void Reset() {
      for (int i = 0; i < Size; i++) {
        finished[i] = false;
        tasks[i] = std::make_shared<Task>(MakeTask(i));
      }
    }

    Task MakeTask(int index) {
      bool* finished_flag = finished.data() + index;
      FnOnce<void()> cb = [finished_flag] { *finished_flag = true; };
      return Task{std::move(cb), StopToken::Unstoppable(), {}};
    }

    std::array<bool, Size> finished;
    std::array<std::shared_ptr<Task>, Size> tasks;
  };
};

class ResizableRingBufferTest : public WorkStealingTest {};

TEST_F(ResizableRingBufferTest, Basic) {
  Tasks<18> tasks;
  ResizableRingBuffer buffer(8);
  ASSERT_EQ(8, buffer.size());

  for (int i = 0; i < 6; i++) {
    buffer.Put(i, tasks.GetTask(i));
  }

  for (int i = 0; i < 2; i++) {
    tasks.VerifyTask(buffer.Get(i), i);
  }

  auto resized = buffer.Resize(2, 6);
  ASSERT_EQ(16, resized.size());

  for (int i = 6; i < 18; i++) {
    resized.Put(i, tasks.GetTask(i));
  }

  for (int i = 2; i < 16; i++) {
    tasks.VerifyTask(resized.Get(i), i);
  }

  for (int i = 0; i < 2; i++) {
    tasks.VerifyTask(resized.Get(i), 16 + i);
  }
}

class WorkQueueTest : public WorkStealingTest {};

TEST_F(WorkQueueTest, PushThenPop) {
  Tasks<18> tasks;
  WorkQueue work_queue(8);

  for (int i = 0; i < 6; i++) {
    // Add 0-5
    work_queue.Push(tasks.GetTask(i));
  }

  for (int i = 0; i < 2; i++) {
    // Pop 4,5
    tasks.VerifyTask(work_queue.Pop(), 5 - i);
  }

  for (int i = 6; i < 18; i++) {
    // Add 6-17
    work_queue.Push(tasks.GetTask(i));
  }

  for (int i = 0; i < 12; i++) {
    // Pop 17-6
    tasks.VerifyTask(work_queue.Pop(), 17 - i);
  }

  for (int i = 0; i < 4; i++) {
    // Pop 0-3
    tasks.VerifyTask(work_queue.Pop(), 3 - i);
  }
}

TEST_F(WorkQueueTest, PushThenSteal) {
  Tasks<18> tasks;
  WorkQueue work_queue(8);

  for (int i = 0; i < 6; i++) {
    work_queue.Push(tasks.GetTask(i));
  }

  for (int i = 0; i < 2; i++) {
    tasks.VerifyTask(work_queue.Steal(), i);
  }

  for (int i = 6; i < 18; i++) {
    work_queue.Push(tasks.GetTask(i));
  }

  for (int i = 2; i < 18; i++) {
    tasks.VerifyTask(work_queue.Steal(), i);
  }
}

class WorkQueueStressTest : public WorkStealingTest,
                            public ::testing::WithParamInterface<bool> {};

TEST_P(WorkQueueStressTest, StressSteal) {
  // Tests stealing from the queue while the producer adds tasks
  bool slow_consumer = GetParam();
  constexpr int MAX_NTASKS = 10000;
  int ntasks = MAX_NTASKS;
  if (slow_consumer) {
    ntasks = 100;
  }
  int iterations = 100;
  if (slow_consumer) {
    iterations = 10;
  }
  Tasks<MAX_NTASKS> tasks;

  for (int i = 0; i < iterations; i++) {
    // The slow_consumer test case should cover steal while resizing
    WorkQueue work_queue(2);
    std::thread producer([&work_queue, &tasks, ntasks] {
      for (int i = 0; i < ntasks; i++) {
        work_queue.Push(tasks.GetTask(i));
      }
    });

    std::thread consumer([&work_queue, &tasks, ntasks, slow_consumer] {
      int tasks_consumed = 0;
      while (tasks_consumed < ntasks) {
        auto next_task = work_queue.Steal();
        if (next_task) {
          tasks.FinishTask(next_task);
          tasks_consumed++;
          if (slow_consumer) {
            SleepABit();
          }
        }
      }
    });

    producer.join();
    consumer.join();
    tasks.VerifyAllTasksFinished(ntasks);
    tasks.Reset();
  }
}

TEST_P(WorkQueueStressTest, PopSteal) {
  // Builds up a queue of tasks and then tests the owning producer
  // working through the queue at the same time another thread steal
  constexpr int NTASKS = 10000;
  constexpr int ITERATIONS = 100;

  bool multiple_theives = GetParam();
  int num_thieves = (multiple_theives) ? 8 : 1;

  Tasks<NTASKS> tasks;

  for (int i = 0; i < ITERATIONS; i++) {
    WorkQueue work_queue(32);
    for (int i = 0; i < NTASKS; i++) {
      work_queue.Push(tasks.GetTask(i));
    }

    std::function<Task*()> pop = [&work_queue] { return work_queue.Pop(); };
    std::function<Task*()> steal = [&work_queue] { return work_queue.Steal(); };

    std::atomic<int> tasks_consumed(0);
    // The logic for the consumers is the same. The only difference is if they call
    // pop or steal.
    auto consume_factory = [&tasks, &tasks_consumed](std::function<Task*()> consume_fn) {
      return [&tasks, &tasks_consumed, consume_fn] {
        while (true) {
          auto next_task = consume_fn();
          if (next_task) {
            tasks.FinishTask(next_task);
            tasks_consumed.fetch_add(1, std::memory_order_relaxed);
          } else {
            int consumed = tasks_consumed.load(std::memory_order_acquire);
            if (consumed >= NTASKS) {
              break;
            }
          }
        }
      };
    };

    std::vector<std::thread> threads;
    threads.emplace_back(consume_factory(pop));
    for (int i = 0; i < num_thieves; i++) {
      threads.emplace_back(consume_factory(steal));
    }

    for (auto& thread : threads) {
      thread.join();
    }

    tasks.VerifyAllTasksFinished(NTASKS);
    tasks.Reset();
  }
}

TEST_P(WorkQueueStressTest, PopAddSteal) {
  // Producer alternates between adding and popping a task while other
  // threads compete to steal tasks.  The pop may fail (because it was
  // already stolen) and that's ok.
  constexpr int NTASKS = 10000;
  constexpr int ITERATIONS = 100;

  bool multiple_theives = GetParam();
  int num_thieves = (multiple_theives) ? 8 : 1;

  Tasks<NTASKS> tasks;
  std::atomic<int> tasks_consumed(0);

  for (int i = 0; i < ITERATIONS; i++) {
    WorkQueue work_queue(32);

    auto producer_fn = [&tasks, &work_queue, &tasks_consumed] {
      for (int i = 0; i < NTASKS; i++) {
        work_queue.Push(tasks.GetTask(i));
        auto task = work_queue.Pop();
        if (task) {
          tasks.FinishTask(task);
          tasks_consumed.fetch_add(1, std::memory_order_relaxed);
        }
      }
    };

    auto consumer_fn = [&tasks, &work_queue, &tasks_consumed] {
      while (true) {
        auto next_task = work_queue.Steal();
        if (next_task) {
          tasks.FinishTask(next_task);
          tasks_consumed.fetch_add(1, std::memory_order_relaxed);
        } else {
          int consumed = tasks_consumed.load(std::memory_order_acquire);
          if (consumed >= NTASKS) {
            break;
          }
        }
      }
    };

    std::vector<std::thread> threads;
    threads.emplace_back(producer_fn);
    for (int i = 0; i < num_thieves; i++) {
      threads.emplace_back(consumer_fn);
    }

    for (auto& thread : threads) {
      thread.join();
    }

    tasks.VerifyAllTasksFinished(NTASKS);
    tasks.Reset();
  }
}

INSTANTIATE_TEST_SUITE_P(TestWorkQueueStress, WorkQueueStressTest,
                         ::testing::Values(false, true));

TEST(WorkStealingThreadPool, StealsProperlyFromOutside) {
  constexpr int NTHREADS = 4;
  ASSERT_OK_AND_ASSIGN(auto thread_pool, WorkStealingThreadPool::Make(NTHREADS));

  auto gating_task = GatingTask::Make();
  for (int i = 0; i < NTHREADS; i++) {
    thread_pool->Submit(gating_task->Task());
  }

  ASSERT_OK(gating_task->WaitForRunning(4));
  ASSERT_OK(gating_task->Unlock());
  ASSERT_OK(thread_pool->Shutdown());
}

TEST(WorkStealingThreadPool, StealsProperlyFromInside) {
  constexpr int NTHREADS = 4;
  ASSERT_OK_AND_ASSIGN(auto thread_pool, WorkStealingThreadPool::Make(NTHREADS));

  auto gating_task = GatingTask::Make();
  thread_pool->Submit([&gating_task, &thread_pool] {
    // The WS loop will want to run its own tasks but can't because they are gating so the
    // other threads should be able to steal
    for (int i = 0; i < NTHREADS; i++) {
      thread_pool->Submit(gating_task->Task());
    }
  });

  ASSERT_OK(gating_task->WaitForRunning(4));
  ASSERT_OK(gating_task->Unlock());
  ASSERT_OK(thread_pool->Shutdown());
}

}  // namespace internal
}  // namespace arrow

/// Thread pool tests