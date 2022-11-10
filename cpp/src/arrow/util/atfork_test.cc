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
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/atfork_internal.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace internal {

using testing::ElementsAre;
using testing::IsSubsetOf;
using testing::UnorderedElementsAreArray;

class TestAtFork : public ::testing::Test {
 public:
  using CallbackBefore = typename AtForkHandler::CallbackBefore;
  using CallbackAfter = typename AtForkHandler::CallbackAfter;

  CallbackBefore PushBefore(int v) {
    return [this, v]() {
      std::lock_guard<std::mutex> lock(mutex_);
      before_.push_back(v);
      return v;
    };
  }

  CallbackAfter PushParentAfter(int w) {
    return [this, w](std::any token) {
      const int* v = std::any_cast<int>(&token);
      ASSERT_NE(v, nullptr);
      std::lock_guard<std::mutex> lock(mutex_);
      parent_after_.emplace_back(*v + w);
    };
  }

  CallbackAfter PushChildAfter(int w) {
    return [this, w](std::any token) {
      const int* v = std::any_cast<int>(&token);
      ASSERT_NE(v, nullptr);
      // Mutex may be invalid and child is single-thread anyway
      child_after_.push_back(*v + w);
    };
  }

  void Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    before_.clear();
    parent_after_.clear();
    child_after_.clear();
  }

#ifndef _WIN32
  void RunInChild(std::function<void()> func) {
    auto child_pid = fork();
    if (child_pid == -1) {
      ASSERT_OK(IOErrorFromErrno(errno, "Error calling fork(): "));
    }
    if (child_pid == 0) {
      // Child
      ASSERT_NO_FATAL_FAILURE(func()) << "Failure in child process";
      std::exit(0);
    } else {
      // Parent
      AssertChildExit(child_pid);
    }
  }
#endif

  std::mutex mutex_;
  std::vector<int> before_;
  std::vector<int> parent_after_;
  std::vector<int> child_after_;
};

#ifndef _WIN32

TEST_F(TestAtFork, EmptyHandlers) {
  auto handlers = std::make_shared<AtForkHandler>();

  RegisterAtFork(handlers);
  RegisterAtFork(handlers);

  RunInChild([&]() {
    ASSERT_TRUE(before_.empty());
    ASSERT_TRUE(parent_after_.empty());
    ASSERT_TRUE(child_after_.empty());
  });

  ASSERT_TRUE(before_.empty());
  ASSERT_TRUE(parent_after_.empty());
  ASSERT_TRUE(child_after_.empty());

  handlers.reset();

  RunInChild([]() {});
}

TEST_F(TestAtFork, SingleThread) {
  auto handlers1 = std::make_shared<AtForkHandler>(PushBefore(1), PushParentAfter(11),
                                                   PushChildAfter(21));
  auto handlers2 = std::make_shared<AtForkHandler>(PushBefore(2), PushParentAfter(12),
                                                   PushChildAfter(22));

  RegisterAtFork(handlers1);
  RegisterAtFork(handlers2);

  RunInChild([&]() {
    ASSERT_THAT(before_, ElementsAre(1, 2));
    ASSERT_THAT(parent_after_, ElementsAre());
    ASSERT_THAT(child_after_, ElementsAre(2 + 22, 1 + 21));
  });
  ASSERT_THAT(before_, ElementsAre(1, 2));
  ASSERT_THAT(parent_after_, ElementsAre(2 + 12, 1 + 11));
  ASSERT_THAT(child_after_, ElementsAre());
  Reset();

  // Destroy one handler
  handlers1.reset();

  RunInChild([&]() {
    ASSERT_THAT(before_, ElementsAre(2));
    ASSERT_THAT(parent_after_, ElementsAre());
    ASSERT_THAT(child_after_, ElementsAre(2 + 22));
  });
  ASSERT_THAT(before_, ElementsAre(2));
  ASSERT_THAT(parent_after_, ElementsAre(2 + 12));
  ASSERT_THAT(child_after_, ElementsAre());
  Reset();

  // Destroy other handler, create new ones
  auto handlers3 = std::make_shared<AtForkHandler>(PushBefore(3), PushParentAfter(13),
                                                   PushChildAfter(23));
  auto handlers4 = std::make_shared<AtForkHandler>(PushBefore(4), PushParentAfter(14),
                                                   PushChildAfter(24));

  RegisterAtFork(handlers3);
  RegisterAtFork(handlers4);
  handlers2.reset();

  RunInChild([&]() {
    ASSERT_THAT(before_, ElementsAre(3, 4));
    ASSERT_THAT(parent_after_, ElementsAre());
    ASSERT_THAT(child_after_, ElementsAre(4 + 24, 3 + 23));
  });
  ASSERT_THAT(before_, ElementsAre(3, 4));
  ASSERT_THAT(parent_after_, ElementsAre(4 + 14, 3 + 13));
  ASSERT_THAT(child_after_, ElementsAre());
}

#if !(defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER))

// The two following tests would seem to leak for various reasons.
// Also, Thread Sanitizer would fail with the same error message as in
// https://github.com/google/sanitizers/issues/950.

TEST_F(TestAtFork, MultipleThreads) {
  const int kNumThreads = 5;
  const int kNumIterations = 40;
  const int kParentAfterAddend = 10000;
  const int kChildAfterAddend = 20000;
  std::atomic<int> seed = 12345;

  auto check_values_in_child = [&]() {
    std::vector<int> expected_child;
    for (const auto v : before_) {
      expected_child.push_back(v + v + kChildAfterAddend);
    }
    // The handlers that were alive on this fork() are a subset of the handlers
    // that were called at any point in the parent.
    ASSERT_THAT(child_after_, IsSubsetOf(expected_child));
  };

  auto run_in_thread = [&](int index) {
    std::default_random_engine engine(++seed);
    std::uniform_int_distribution<int> value_dist(index * 100, (index + 1) * 100 - 1);
    std::bernoulli_distribution fork_dist(0.1);

    for (int i = 0; i < kNumIterations; ++i) {
      int value = value_dist(engine);
      auto handlers = std::make_shared<AtForkHandler>(
          PushBefore(value), PushParentAfter(value + kParentAfterAddend),
          PushChildAfter(value + kChildAfterAddend));
      RegisterAtFork(handlers);
      if (fork_dist(engine)) {
        RunInChild(check_values_in_child);
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(run_in_thread, i);
  }
  for (auto&& thread : threads) {
    thread.join();
  }

  std::vector<int> expected_parent;
  for (const auto v : before_) {
    expected_parent.push_back(v + v + kParentAfterAddend);
  }
  // The handlers that were called after fork are the same that were called
  // before fork; however, their overall order is undefined as multiple fork()
  // calls were made and multiple handlers may have been alive during
  // each fork() called.
  ASSERT_THAT(parent_after_, UnorderedElementsAreArray(expected_parent));
  ASSERT_TRUE(child_after_.empty());
}

TEST_F(TestAtFork, NestedChild) {
#ifdef __APPLE__
  GTEST_SKIP() << "Nested fork is not supported on macOS";
#endif

  auto handlers1 = std::make_shared<AtForkHandler>(PushBefore(1), PushParentAfter(11),
                                                   PushChildAfter(21));
  auto handlers2 = std::make_shared<AtForkHandler>(PushBefore(2), PushParentAfter(12),
                                                   PushChildAfter(22));

  RegisterAtFork(handlers1);
  RegisterAtFork(handlers2);

  RunInChild([&]() {
    Reset();

    // Add a new handler, destroy one of the parent handlers
    auto handlers3 = std::make_shared<AtForkHandler>(PushBefore(3), PushParentAfter(13),
                                                     PushChildAfter(23));
    RegisterAtFork(handlers3);
    handlers2.reset();

    RunInChild([&]() {
      ASSERT_THAT(before_, ElementsAre(1, 3));
      ASSERT_THAT(parent_after_, ElementsAre());
      ASSERT_THAT(child_after_, ElementsAre(3 + 23, 1 + 21));
    });

    ASSERT_THAT(before_, ElementsAre(1, 3));
    ASSERT_THAT(parent_after_, ElementsAre(3 + 13, 1 + 11));
    ASSERT_THAT(child_after_, ElementsAre());
  });

  ASSERT_THAT(before_, ElementsAre(1, 2));
  ASSERT_THAT(parent_after_, ElementsAre(2 + 12, 1 + 11));
  ASSERT_THAT(child_after_, ElementsAre());
}

#endif  // !(defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER) ||
        //   defined(THREAD_SANITIZER))

#endif  // !defined(_WIN32)

#ifdef _WIN32
TEST_F(TestAtFork, NoOp) {
  auto handlers = std::make_shared<AtForkHandler>(PushBefore(1), PushParentAfter(11),
                                                  PushChildAfter(21));

  RegisterAtFork(handlers);

  ASSERT_TRUE(before_.empty());
  ASSERT_TRUE(parent_after_.empty());
  ASSERT_TRUE(child_after_.empty());
}
#endif

}  // namespace internal
}  // namespace arrow
