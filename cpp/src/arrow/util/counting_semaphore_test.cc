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

#include "arrow/util/counting_semaphore.h"

#include <atomic>
#include <thread>
#include <vector>

#include "arrow/testing/gtest_util.h"
#include "gtest/gtest.h"

namespace arrow {
namespace util {

TEST(CountingSemaphore, Basic) {
  CountingSemaphore semaphore;
  std::atomic<bool> acquired{false};
  std::atomic<bool> started{false};
  std::thread acquirer([&] {
    started.store(true);
    ASSERT_OK(semaphore.Acquire(3));
    acquired = true;
  });
  ASSERT_OK(semaphore.WaitForWaiters(1));
  ASSERT_TRUE(started.load());
  ASSERT_FALSE(acquired.load());
  ASSERT_OK(semaphore.Release(2));
  SleepABit();
  ASSERT_FALSE(acquired.load());
  ASSERT_OK(semaphore.Release(1));
  BusyWait(10, [&] { return acquired.load(); });
  ASSERT_TRUE(acquired.load());
  ASSERT_OK(semaphore.Close());
  acquirer.join();
}

TEST(CountingSemaphore, CloseAborts) {
  CountingSemaphore semaphore;
  std::atomic<bool> cleanup{false};
  std::thread acquirer([&] {
    ASSERT_RAISES(Invalid, semaphore.Acquire(1));
    cleanup = true;
  });
  ASSERT_OK(semaphore.WaitForWaiters(1));
  ASSERT_FALSE(cleanup.load());
  ASSERT_RAISES(Invalid, semaphore.Close());
  BusyWait(10, [&] { return cleanup.load(); });
  acquirer.join();
}

TEST(CountingSemaphore, Stress) {
  constexpr uint32_t NTHREADS = 10;
  CountingSemaphore semaphore;
  std::vector<uint32_t> max_allowed_cases = {1, 3};
  std::atomic<uint32_t> count{0};
  std::atomic<bool> max_exceeded{false};
  std::vector<std::thread> threads;
  for (uint32_t max_allowed : max_allowed_cases) {
    ASSERT_OK(semaphore.Release(max_allowed));
    for (uint32_t i = 0; i < NTHREADS; i++) {
      threads.emplace_back([&] {
        ASSERT_OK(semaphore.Acquire(1));
        uint32_t last_count = count.fetch_add(1);
        if (last_count >= max_allowed) {
          max_exceeded.store(true);
        }
        SleepABit();
        count.fetch_sub(1);
        ASSERT_OK(semaphore.Release(1));
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    threads.clear();
    ASSERT_OK(semaphore.Acquire(max_allowed));
  }
  ASSERT_OK(semaphore.Close());
  ASSERT_FALSE(max_exceeded.load());
}

}  // namespace util
}  // namespace arrow
