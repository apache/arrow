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
#include <functional>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/cancel.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"

namespace arrow {

static constexpr double kLongWait = 5;  // seconds

class CancelTest : public ::testing::Test {};

TEST_F(CancelTest, TokenBasics) {
  {
    StopToken token;
    ASSERT_FALSE(token.IsStopRequested());
    ASSERT_OK(token.Poll());

    token.RequestStop();
    ASSERT_TRUE(token.IsStopRequested());
    ASSERT_RAISES(Cancelled, token.Poll());
  }
  {
    StopToken token;
    token.RequestStop(Status::IOError("Operation cancelled"));
    ASSERT_TRUE(token.IsStopRequested());
    ASSERT_RAISES(IOError, token.Poll());
  }
}

TEST_F(CancelTest, RequestStopTwice) {
  StopToken token;
  token.RequestStop();
  // Second RequestStop() call is ignored
  token.RequestStop(Status::IOError("Operation cancelled"));
  ASSERT_TRUE(token.IsStopRequested());
  ASSERT_RAISES(Cancelled, token.Poll());
}

TEST_F(CancelTest, SetCallback) {
  std::vector<int> results;
  StopToken token;
  {
    const auto cb = token.SetCallback([&](const Status& st) { results.push_back(1); });
    ASSERT_EQ(results.size(), 0);
  }
  {
    const auto cb = token.SetCallback([&](const Status& st) { results.push_back(1); });
    ASSERT_EQ(results.size(), 0);
    token.RequestStop();
    ASSERT_EQ(results, std::vector<int>{1});
    token.RequestStop();
    ASSERT_EQ(results, std::vector<int>{1});
  }
  {
    const auto cb = token.SetCallback([&](const Status& st) { results.push_back(2); });
    ASSERT_EQ(results, std::vector<int>({1, 2}));
    token.RequestStop();
    ASSERT_EQ(results, std::vector<int>({1, 2}));
  }
}

TEST_F(CancelTest, StopCallbackMove) {
  std::vector<int> results;
  StopToken token;

  StopCallback cb1(&token, [&](const Status& st) { results.push_back(1); });
  const auto cb2 = std::move(cb1);

  ASSERT_EQ(results.size(), 0);
  token.RequestStop();
  ASSERT_EQ(results, std::vector<int>{1});
}

TEST_F(CancelTest, ThreadedPollSuccess) {
  constexpr int kNumThreads = 10;

  std::vector<Status> results(kNumThreads);
  std::vector<std::thread> threads;

  StopToken token;
  std::atomic<bool> terminate_flag{false};

  const auto worker_func = [&](int thread_num) {
    while (token.Poll().ok() && !terminate_flag) {
    }
    results[thread_num] = token.Poll();
  };
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(std::bind(worker_func, i));
  }

  // Let the threads start and hammer on Poll() for a while
  SleepFor(1e-2);
  // Tell threads to stop
  terminate_flag = true;
  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& st : results) {
    ASSERT_OK(st);
  }
}

TEST_F(CancelTest, ThreadedPollCancel) {
  constexpr int kNumThreads = 10;

  std::vector<Status> results(kNumThreads);
  std::vector<std::thread> threads;

  StopToken token;
  std::atomic<bool> terminate_flag{false};
  const auto stop_error = Status::IOError("Operation cancelled");

  const auto worker_func = [&](int thread_num) {
    while (token.Poll().ok() && !terminate_flag) {
    }
    results[thread_num] = token.Poll();
  };

  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(std::bind(worker_func, i));
  }
  // Let the threads start
  SleepFor(1e-2);
  // Cancel token while threads are hammering on Poll()
  token.RequestStop(stop_error);
  // Tell threads to stop
  terminate_flag = true;
  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& st : results) {
    ASSERT_EQ(st, stop_error);
  }
}

TEST_F(CancelTest, ThreadedSetCallbackCancel) {
  constexpr int kIterations = 100;
  constexpr double kMaxWait = 1e-3;

  std::default_random_engine gen(42);
  std::uniform_real_distribution<double> wait_dist(0.0, kMaxWait);

  for (int i = 0; i < kIterations; ++i) {
    Status result;

    StopToken token;
    auto barrier = Future<>::Make();
    const auto stop_error = Status::IOError("Operation cancelled");

    const auto worker_func = [&]() {
      ARROW_CHECK(barrier.Wait(kLongWait));
      token.RequestStop(stop_error);
    };
    std::thread thread(worker_func);

    // Unblock thread
    barrier.MarkFinished();
    // Use a variable wait time to maximize potential synchronization issues
    const auto wait_time = wait_dist(gen);
    if (wait_time > kMaxWait * 0.5) {
      SleepFor(wait_time);
    }

    // Register callback while thread might be cancelling
    StopCallback stop_cb(&token, [&](const Status& st) { result = st; });
    thread.join();

    ASSERT_EQ(result, stop_error);
  }
}

}  // namespace arrow
