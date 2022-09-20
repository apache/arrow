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

#include "arrow/compute/exec/accumulation_queue.h"

#include <gtest/gtest.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace util {

TEST(AccumulationQueue, Basic) {
  constexpr int kNumBatches = 1000;
  constexpr int kNumIters = 100;
  constexpr int kRandomSeed = 42;

  for (int i = 0; i < kNumIters; i++) {
    std::vector<ExecBatch> collected(kNumBatches);
    int num_seen = 0;
    int num_active_tasks = 0;
    std::mutex task_counter_mutex;
    std::condition_variable task_counter_cv;

    OrderedAccumulationQueue::TaskFactoryCallback create_collect_task =
        [&](std::vector<ExecBatch> batches) {
          int start = num_seen;
          num_seen += static_cast<int>(batches.size());
          return [&, start, batches = std::move(batches)]() {
            std::move(batches.begin(), batches.end(), collected.begin() + start);
            return Status::OK();
          };
        };

    std::vector<std::thread> threads;

    OrderedAccumulationQueue::ScheduleCallback schedule =
        [&](OrderedAccumulationQueue::Task task) {
          std::lock_guard<std::mutex> lk(task_counter_mutex);
          num_active_tasks++;
          threads.emplace_back([&, task = std::move(task)] {
            ASSERT_OK(task());
            std::lock_guard<std::mutex> lk(task_counter_mutex);
            if (--num_active_tasks == 0) {
              task_counter_cv.notify_one();
            }
          });
          return Status::OK();
        };

    std::unique_ptr<OrderedAccumulationQueue> ordered_queue =
        OrderedAccumulationQueue::Make(std::move(create_collect_task),
                                       std::move(schedule));

    std::vector<ExecBatch> test_batches(kNumBatches);
    for (int i = 0; i < kNumBatches; i++) {
      test_batches[i].index = i;
    }

    std::default_random_engine gen(kRandomSeed);
    std::shuffle(test_batches.begin(), test_batches.end(), gen);

    for (auto& batch : test_batches) {
      ASSERT_OK(ordered_queue->InsertBatch(std::move(batch)));
    }

    std::unique_lock<std::mutex> lk(task_counter_mutex);
    task_counter_cv.wait(lk, [&] { return num_active_tasks == 0; });

    for (auto& thread : threads) {
      thread.join();
    }

    ASSERT_OK(ordered_queue->CheckDrained());
    ASSERT_EQ(kNumBatches, static_cast<int>(collected.size()));

    for (int i = 0; i < kNumBatches; i++) {
      ASSERT_EQ(i, collected[i].index);
    }
  }
}

}  // namespace util
}  // namespace arrow
