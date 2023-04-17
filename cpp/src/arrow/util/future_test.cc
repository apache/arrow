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

#include "arrow/util/future.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <numeric>
#include <ostream>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/testing/executor_util.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::ThreadPool;

int ToInt(int x) { return x; }

// A data type without a default constructor.
struct Foo {
  int bar;
  std::string baz;

  explicit Foo(int value) : bar(value), baz(std::to_string(value)) {}

  int ToInt() const { return bar; }

  bool operator==(int other) const { return bar == other; }
  bool operator==(const Foo& other) const { return bar == other.bar; }
  friend bool operator==(int left, const Foo& right) { return right == left; }

  friend std::ostream& operator<<(std::ostream& os, const Foo& foo) {
    return os << "Foo(" << foo.bar << ")";
  }
};

template <>
struct IterationTraits<Foo> {
  static Foo End() { return Foo(-1); }
};

template <>
struct IterationTraits<MoveOnlyDataType> {
  static MoveOnlyDataType End() { return MoveOnlyDataType(-1); }
};

template <typename T>
struct IteratorResults {
  std::vector<T> values;
  std::vector<Status> errors;
};

template <typename T>
IteratorResults<T> IteratorToResults(Iterator<T> iterator) {
  IteratorResults<T> results;

  while (true) {
    auto res = iterator.Next();
    if (res == IterationTraits<T>::End()) {
      break;
    }
    if (res.ok()) {
      results.values.push_back(*std::move(res));
    } else {
      results.errors.push_back(res.status());
    }
  }
  return results;
}

// So that main thread may wait a bit for a future to be finished
constexpr auto kYieldDuration = std::chrono::microseconds(50);
constexpr double kTinyWait = 1e-5;  // seconds
constexpr double kLargeWait = 5.0;  // seconds

template <typename T>
class SimpleExecutor {
 public:
  explicit SimpleExecutor(int nfutures)
      : pool_(ThreadPool::Make(/*threads=*/4).ValueOrDie()) {
    for (int i = 0; i < nfutures; ++i) {
      futures_.push_back(Future<T>::Make());
    }
  }

  std::vector<Future<T>>& futures() { return futures_; }

  void SetFinished(const std::vector<std::pair<int, bool>>& pairs) {
    for (const auto& pair : pairs) {
      const int fut_index = pair.first;
      if (pair.second) {
        futures_[fut_index].MarkFinished(T(fut_index));
      } else {
        futures_[fut_index].MarkFinished(Status::UnknownError("xxx"));
      }
    }
  }

  void SetFinishedDeferred(std::vector<std::pair<int, bool>> pairs) {
    std::this_thread::sleep_for(kYieldDuration);
    ABORT_NOT_OK(
        pool_->Spawn([this, pairs = std::move(pairs)]() { SetFinished(pairs); }));
  }

  // Mark future successful
  void SetFinished(int fut_index) { futures_[fut_index].MarkFinished(T(fut_index)); }

  void SetFinishedDeferred(int fut_index) {
    std::this_thread::sleep_for(kYieldDuration);
    ABORT_NOT_OK(pool_->Spawn([this, fut_index]() { SetFinished(fut_index); }));
  }

  // Mark all futures in [start, stop) successful
  void SetFinished(int start, int stop) {
    for (int fut_index = start; fut_index < stop; ++fut_index) {
      futures_[fut_index].MarkFinished(T(fut_index));
    }
  }

  void SetFinishedDeferred(int start, int stop) {
    std::this_thread::sleep_for(kYieldDuration);
    ABORT_NOT_OK(pool_->Spawn([this, start, stop]() { SetFinished(start, stop); }));
  }

 protected:
  std::vector<Future<T>> futures_;
  std::shared_ptr<ThreadPool> pool_;
};

// --------------------------------------------------------------------
// Simple in-thread tests

TEST(FutureSyncTest, Int) {
  {
    // MarkFinished(int)
    auto fut = Future<int>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished(42);
    AssertSuccessful(fut);
    auto res = fut.result();
    ASSERT_OK(res);
    ASSERT_EQ(*res, 42);
    res = std::move(fut).result();
    ASSERT_OK(res);
    ASSERT_EQ(*res, 42);
  }
  {
    // MakeFinished(int)
    auto fut = Future<int>::MakeFinished(42);
    AssertSuccessful(fut);
    auto res = fut.result();
    ASSERT_OK(res);
    ASSERT_EQ(*res, 42);
    res = std::move(fut.result());
    ASSERT_OK(res);
    ASSERT_EQ(*res, 42);
  }
  {
    // MarkFinished(Result<int>)
    auto fut = Future<int>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished(Result<int>(43));
    AssertSuccessful(fut);
    ASSERT_OK_AND_ASSIGN(auto value, fut.result());
    ASSERT_EQ(value, 43);
  }
  {
    // MarkFinished(failed Result<int>)
    auto fut = Future<int>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished(Result<int>(Status::IOError("xxx")));
    AssertFailed(fut);
    ASSERT_RAISES(IOError, fut.result());
  }
  {
    // MakeFinished(Status)
    auto fut = Future<int>::MakeFinished(Status::IOError("xxx"));
    AssertFailed(fut);
    ASSERT_RAISES(IOError, fut.result());
  }
  {
    // MarkFinished(Status)
    auto fut = Future<int>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut);
    ASSERT_RAISES(IOError, fut.result());
  }
}

TEST(FutureSyncTest, Foo) {
  {
    auto fut = Future<Foo>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished(Foo(42));
    AssertSuccessful(fut);
    auto res = fut.result();
    ASSERT_OK(res);
    Foo value = *res;
    ASSERT_EQ(value, 42);
    ASSERT_OK(fut.status());
    res = std::move(fut).result();
    ASSERT_OK(res);
    value = *res;
    ASSERT_EQ(value, 42);
  }
  {
    // MarkFinished(Result<Foo>)
    auto fut = Future<Foo>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished(Result<Foo>(Foo(42)));
    AssertSuccessful(fut);
    ASSERT_OK_AND_ASSIGN(Foo value, fut.result());
    ASSERT_EQ(value, 42);
  }
  {
    // MarkFinished(failed Result<Foo>)
    auto fut = Future<Foo>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished(Result<Foo>(Status::IOError("xxx")));
    AssertFailed(fut);
    ASSERT_RAISES(IOError, fut.result());
  }
}

TEST(FutureSyncTest, Empty) {
  {
    // MarkFinished()
    auto fut = Future<>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished();
    AssertSuccessful(fut);
  }
  {
    // MakeFinished()
    auto fut = Future<>::MakeFinished();
    AssertSuccessful(fut);
  }
  {
    // MarkFinished(Status)
    auto fut = Future<>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished();
    AssertSuccessful(fut);
  }
  {
    // MakeFinished(Status)
    auto fut = Future<>::MakeFinished();
    AssertSuccessful(fut);
    fut = Future<>::MakeFinished(Status::IOError("xxx"));
    AssertFailed(fut);
  }
  {
    // MarkFinished(Status)
    auto fut = Future<>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut);
    ASSERT_RAISES(IOError, fut.status());
  }
}

TEST(FutureSyncTest, GetStatusFuture) {
  {
    auto fut = Future<MoveOnlyDataType>::Make();
    Future<> status_future(fut);

    AssertNotFinished(fut);
    AssertNotFinished(status_future);

    fut.MarkFinished(MoveOnlyDataType(42));
    AssertSuccessful(fut);
    AssertSuccessful(status_future);
    ASSERT_EQ(&fut.status(), &status_future.status());
  }
  {
    auto fut = Future<MoveOnlyDataType>::Make();
    Future<> status_future(fut);

    AssertNotFinished(fut);
    AssertNotFinished(status_future);

    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut);
    AssertFailed(status_future);
    ASSERT_EQ(&fut.status(), &status_future.status());
  }
}

// Ensure the implicit convenience constructors behave as desired.
TEST(FutureSyncTest, ImplicitConstructors) {
  {
    auto fut = ([]() -> Future<MoveOnlyDataType> {
      return arrow::Status::Invalid("Invalid");
    })();
    AssertFailed(fut);
    ASSERT_RAISES(Invalid, fut.result());
  }
  {
    auto fut = ([]() -> Future<MoveOnlyDataType> {
      return arrow::Result<MoveOnlyDataType>(arrow::Status::Invalid("Invalid"));
    })();
    AssertFailed(fut);
    ASSERT_RAISES(Invalid, fut.result());
  }
  {
    auto fut = ([]() -> Future<MoveOnlyDataType> { return MoveOnlyDataType(42); })();
    AssertSuccessful(fut);
  }
  {
    auto fut = ([]() -> Future<MoveOnlyDataType> {
      return arrow::Result<MoveOnlyDataType>(MoveOnlyDataType(42));
    })();
    AssertSuccessful(fut);
  }
}

TEST(FutureRefTest, ChainRemoved) {
  // Creating a future chain should not prevent the futures from being deleted if the
  // entire chain is deleted
  std::weak_ptr<FutureImpl> ref;
  std::weak_ptr<FutureImpl> ref2;
  {
    auto fut = Future<>::Make();
    auto fut2 = fut.Then([]() { return Status::OK(); });
    ref = fut.impl_;
    ref2 = fut2.impl_;
  }
  ASSERT_TRUE(ref.expired());
  ASSERT_TRUE(ref2.expired());

  {
    auto fut = Future<>::Make();
    auto fut2 = fut.Then([]() { return Future<>::Make(); });
    ref = fut.impl_;
    ref2 = fut2.impl_;
  }
  ASSERT_TRUE(ref.expired());
  ASSERT_TRUE(ref2.expired());
}

TEST(FutureRefTest, TailRemoved) {
  // Keeping the head of the future chain should keep the entire chain alive
  std::shared_ptr<Future<>> ref;
  std::weak_ptr<FutureImpl> ref2;
  bool side_effect_run = false;
  {
    ref = std::make_shared<Future<>>(Future<>::Make());
    auto fut2 = ref->Then([&side_effect_run]() {
      side_effect_run = true;
      return Status::OK();
    });
    ref2 = fut2.impl_;
  }
  ASSERT_FALSE(ref2.expired());

  ref->MarkFinished();
  ASSERT_TRUE(side_effect_run);
  ASSERT_TRUE(ref2.expired());
}

TEST(FutureRefTest, HeadRemoved) {
  // Keeping the tail of the future chain should not keep the entire chain alive.  If no
  // one has a reference to the head then the future is abandoned.  TODO (ARROW-12207):
  // detect abandonment.
  std::weak_ptr<FutureImpl> ref;
  std::shared_ptr<Future<>> ref2;
  {
    auto fut = std::make_shared<Future<>>(Future<>::Make());
    ref = fut->impl_;
    ref2 = std::make_shared<Future<>>(fut->Then([]() {}));
  }
  ASSERT_TRUE(ref.expired());

  {
    auto fut = Future<>::Make();
    ref2 = std::make_shared<Future<>>(fut.Then([&]() {
      auto intermediate = Future<>::Make();
      ref = intermediate.impl_;
      return intermediate;
    }));
    fut.MarkFinished();
  }
  ASSERT_TRUE(ref.expired());
}

TEST(FutureStressTest, Callback) {
#ifdef ARROW_VALGRIND
  const int NITERS = 2;
#else
  const int NITERS = 1000;
#endif
  for (unsigned int n = 0; n < NITERS; n++) {
    auto fut = Future<>::Make();
    std::atomic<unsigned int> count_finished_immediately(0);
    std::atomic<unsigned int> count_finished_deferred(0);
    std::atomic<unsigned int> callbacks_added(0);
    std::atomic<bool> finished(false);

    std::thread callback_adder([&] {
      auto test_thread = std::this_thread::get_id();
      while (!finished.load()) {
        fut.AddCallback([&test_thread, &count_finished_immediately,
                         &count_finished_deferred](const Status& status) {
          ARROW_EXPECT_OK(status);
          if (std::this_thread::get_id() == test_thread) {
            count_finished_immediately++;
          } else {
            count_finished_deferred++;
          }
        });
        callbacks_added++;
        if (callbacks_added.load() > 10000) {
          // If we've added many callbacks already and the main thread hasn't noticed yet,
          // help it a bit (this seems especially useful in Valgrind).
          SleepABit();
        }
      }
    });

    while (callbacks_added.load() == 0) {
      // Spin until the callback_adder has started running
    }

    ASSERT_EQ(0, count_finished_deferred.load());
    ASSERT_EQ(0, count_finished_immediately.load());

    fut.MarkFinished();

    while (count_finished_immediately.load() == 0) {
      // Spin until the callback_adder has added at least one post-future
    }

    finished.store(true);
    callback_adder.join();
    auto total_added = callbacks_added.load();
    auto total_immediate = count_finished_immediately.load();
    auto total_deferred = count_finished_deferred.load();
    ASSERT_EQ(total_added, total_immediate + total_deferred);
  }
}

TEST(FutureStressTest, TryAddCallback) {
  for (unsigned int n = 0; n < 1; n++) {
    auto fut = Future<>::Make();
    std::atomic<unsigned int> callbacks_added(0);
    std::atomic<bool> finished(false);
    std::mutex mutex;
    std::condition_variable cv;
    std::thread::id callback_adder_thread_id;

    std::thread callback_adder([&] {
      callback_adder_thread_id = std::this_thread::get_id();
      std::function<void(const Status&)> callback =
          [&callback_adder_thread_id](const Status& st) {
            ARROW_EXPECT_OK(st);
            if (std::this_thread::get_id() == callback_adder_thread_id) {
              FAIL() << "TryAddCallback allowed a callback to be run synchronously";
            }
          };
      std::function<std::function<void(const Status&)>()> callback_factory =
          [&callback]() { return callback; };
      while (true) {
        auto callback_added = fut.TryAddCallback(callback_factory);
        if (callback_added) {
          callbacks_added++;
          if (callbacks_added.load() > 10000) {
            // If we've added many callbacks already and the main thread hasn't
            // noticed yet, help it a bit (this seems especially useful in Valgrind).
            SleepABit();
          }
        } else {
          break;
        }
      }
      {
        std::lock_guard<std::mutex> lg(mutex);
        finished.store(true);
      }
      cv.notify_one();
    });

    while (callbacks_added.load() == 0) {
      // Spin until the callback_adder has started running
    }

    fut.MarkFinished();

    std::unique_lock<std::mutex> lk(mutex);
    cv.wait_for(lk, std::chrono::duration<double>(0.5),
                [&finished] { return finished.load(); });
    lk.unlock();

    ASSERT_TRUE(finished);
    callback_adder.join();
  }
}

TEST(FutureStressTest, DeleteAfterWait) {
  constexpr int kNumTasks = 100;
  for (int i = 0; i < kNumTasks; i++) {
    {
      auto future = std::make_unique<Future<>>(Future<>::Make());
      std::thread t([&]() {
        SleepABit();
        future->MarkFinished();
      });
      ASSERT_TRUE(future->Wait(arrow::kDefaultAssertFinishesWaitSeconds));
      future.reset();
      t.join();
    }
  }
}

TEST(FutureCompletionTest, Void) {
  {
    // Simple callback
    auto fut = Future<int>::Make();
    int passed_in_result = 0;
    auto fut2 =
        fut.Then([&passed_in_result](const int& result) { passed_in_result = result; });
    fut.MarkFinished(42);
    AssertSuccessful(fut2);
    ASSERT_EQ(passed_in_result, 42);
  }
  {
    // Propagate failure by returning it from on_failure
    auto fut = Future<int>::Make();
    auto fut2 = fut.Then([](const int&) {}, [](const Status& s) { return s; });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut2);
    ASSERT_TRUE(fut2.status().IsIOError());
  }
  {
    // From void
    auto fut = Future<>::Make();
    auto fut2 = fut.Then([]() {});
    fut.MarkFinished();
    AssertSuccessful(fut2);
  }
  {
    // Propagate failure by not having on_failure
    auto fut = Future<>::Make();
    auto cb_was_run = false;
    auto fut2 = fut.Then([&cb_was_run]() {
      cb_was_run = true;
      return Status::OK();
    });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut2);
    ASSERT_FALSE(cb_was_run);
  }
  {
    // Swallow failure by catching in on_failure
    auto fut = Future<>::Make();
    Status status_seen = Status::OK();
    auto fut2 = fut.Then([]() {},
                         [&status_seen](const Status& s) {
                           status_seen = s;
                           return Status::OK();
                         });
    ASSERT_TRUE(status_seen.ok());
    fut.MarkFinished(Status::IOError("xxx"));
    ASSERT_TRUE(status_seen.IsIOError());
    AssertSuccessful(fut2);
  }
}

TEST(FutureCompletionTest, NonVoid) {
  {
    // Simple callback
    auto fut = Future<int>::Make();
    auto fut2 = fut.Then([](int result) {
      auto passed_in_result = result;
      return passed_in_result * passed_in_result;
    });
    fut.MarkFinished(42);
    AssertSuccessful(fut2);
    auto result = *fut2.result();
    ASSERT_EQ(result, 42 * 42);
  }
  {
    // Propagate failure by not having on_failure
    auto fut = Future<int>::Make();
    auto cb_was_run = false;
    auto fut2 = fut.Then([&cb_was_run](int result) {
      cb_was_run = true;
      auto passed_in_result = result;
      return passed_in_result * passed_in_result;
    });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut2);
    ASSERT_TRUE(fut2.status().IsIOError());
    ASSERT_FALSE(cb_was_run);
  }
  {
    // Swallow failure by catching in on_failure
    auto fut = Future<int>::Make();
    bool was_io_error = false;
    auto fut2 = fut.Then([](int) { return 99; },
                         [&was_io_error](const Status& s) {
                           was_io_error = s.IsIOError();
                           return 100;
                         });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertSuccessful(fut2);
    auto result = *fut2.result();
    ASSERT_EQ(result, 100);
    ASSERT_TRUE(was_io_error);
  }
  {
    // From void
    auto fut = Future<>::Make();
    auto fut2 = fut.Then([]() { return 42; });
    fut.MarkFinished();
    AssertSuccessful(fut2);
    auto result = *fut2.result();
    ASSERT_EQ(result, 42);
  }
  {
    // Propagate failure by returning failure

    // Cannot do this.  Must return Result<int> because
    // both callbacks must return the same thing and you can't
    // return an int from the second callback if you're trying
    // to propagate a failure
  }
}

TEST(FutureCompletionTest, FutureNonVoid) {
  {
    // Simple callback
    auto fut = Future<int>::Make();
    auto innerFut = Future<std::string>::Make();
    int passed_in_result = 0;
    auto fut2 = fut.Then([&passed_in_result, innerFut](int result) {
      passed_in_result = result;
      return innerFut;
    });
    fut.MarkFinished(42);
    ASSERT_EQ(passed_in_result, 42);
    AssertNotFinished(fut2);
    innerFut.MarkFinished("hello");
    AssertSuccessful(fut2);
    auto result = *fut2.result();
    ASSERT_EQ(result, "hello");
  }
  {
    // Propagate failure by not having on_failure
    auto fut = Future<int>::Make();
    auto innerFut = Future<std::string>::Make();
    auto was_cb_run = false;
    auto fut2 = fut.Then([innerFut, &was_cb_run](int) {
      was_cb_run = true;
      return innerFut;
    });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut2);
    ASSERT_TRUE(fut2.status().IsIOError());
    ASSERT_FALSE(was_cb_run);
  }
  {
    // Swallow failure by catching in on_failure
    auto fut = Future<int>::Make();
    auto innerFut = Future<std::string>::Make();
    bool was_io_error = false;
    auto was_cb_run = false;
    auto fut2 = fut.Then(
        [innerFut, &was_cb_run](int) {
          was_cb_run = true;
          return innerFut;
        },
        [&was_io_error, innerFut](const Status& s) {
          was_io_error = s.IsIOError();
          return innerFut;
        });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertNotFinished(fut2);
    innerFut.MarkFinished("hello");
    AssertSuccessful(fut2);
    auto result = *fut2.result();
    ASSERT_EQ(result, "hello");
    ASSERT_TRUE(was_io_error);
    ASSERT_FALSE(was_cb_run);
  }
  {
    // From void
    auto fut = Future<>::Make();
    auto innerFut = Future<std::string>::Make();
    auto fut2 = fut.Then([&innerFut]() { return innerFut; });
    fut.MarkFinished();
    AssertNotFinished(fut2);
    innerFut.MarkFinished("hello");
    AssertSuccessful(fut2);
    auto result = *fut2.result();
    ASSERT_EQ(result, "hello");
  }
  {
    // Propagate failure by returning failure
    auto fut = Future<>::Make();
    auto innerFut = Future<std::string>::Make();
    auto was_cb_run = false;
    auto fut2 = fut.Then(
        [&innerFut, &was_cb_run]() {
          was_cb_run = true;
          return Result<Future<std::string>>(innerFut);
        },
        [](const Status& status) { return Result<Future<std::string>>(status); });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut2);
    ASSERT_FALSE(was_cb_run);
  }
}

TEST(FutureCompletionTest, Status) {
  {
    // Simple callback
    auto fut = Future<int>::Make();
    int passed_in_result = 0;
    Future<> fut2 = fut.Then([&passed_in_result](int result) {
      passed_in_result = result;
      return Status::OK();
    });
    fut.MarkFinished(42);
    ASSERT_EQ(passed_in_result, 42);
    AssertSuccessful(fut2);
  }
  {
    // Propagate failure by not having on_failure
    auto fut = Future<int>::Make();
    auto was_cb_run = false;
    auto fut2 = fut.Then([&was_cb_run](int) {
      was_cb_run = true;
      return Status::OK();
    });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut2);
    ASSERT_TRUE(fut2.status().IsIOError());
    ASSERT_FALSE(was_cb_run);
  }
  {
    // Swallow failure by catching in on_failure
    auto fut = Future<int>::Make();
    bool was_io_error = false;
    auto was_cb_run = false;
    auto fut2 = fut.Then(
        [&was_cb_run](int i) {
          was_cb_run = true;
          return Status::OK();
        },
        [&was_io_error](const Status& s) {
          was_io_error = s.IsIOError();
          return Status::OK();
        });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertSuccessful(fut2);
    ASSERT_TRUE(was_io_error);
    ASSERT_FALSE(was_cb_run);
  }
  {
    // From void
    auto fut = Future<>::Make();
    auto fut2 = fut.Then([]() { return Status::OK(); });
    fut.MarkFinished();
    AssertSuccessful(fut2);
  }
  {
    // Propagate failure by returning failure
    auto fut = Future<>::Make();
    auto was_cb_run = false;
    auto fut2 = fut.Then(
        [&was_cb_run]() {
          was_cb_run = true;
          return Status::OK();
        },
        [](const Status& s) { return s; });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut2);
    ASSERT_FALSE(was_cb_run);
  }
}

TEST(FutureCompletionTest, Result) {
  {
    // Simple callback
    auto fut = Future<int>::Make();
    Future<int> fut2 = fut.Then([](const int& i) {
      auto passed_in_result = i;
      return Result<int>(passed_in_result * passed_in_result);
    });
    fut.MarkFinished(42);
    AssertSuccessful(fut2);
    auto result = *fut2.result();
    ASSERT_EQ(result, 42 * 42);
  }
  {
    // Propagate failure by not having on_failure
    auto fut = Future<int>::Make();
    auto was_cb_run = false;
    auto fut2 = fut.Then([&was_cb_run](const int& i) {
      was_cb_run = true;
      auto passed_in_result = i;
      return Result<int>(passed_in_result * passed_in_result);
    });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut2);
    ASSERT_TRUE(fut2.status().IsIOError());
    ASSERT_FALSE(was_cb_run);
  }
  {
    // Swallow failure by catching in on_failure
    auto fut = Future<int>::Make();
    bool was_io_error = false;
    bool was_cb_run = false;
    auto fut2 = fut.Then(
        [&was_cb_run](const int& i) {
          was_cb_run = true;
          return Result<int>(100);
        },
        [&was_io_error](const Status& s) {
          was_io_error = s.IsIOError();
          return Result<int>(100);
        });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertSuccessful(fut2);
    auto result = *fut2.result();
    ASSERT_EQ(result, 100);
    ASSERT_TRUE(was_io_error);
    ASSERT_FALSE(was_cb_run);
  }
  {
    // From void
    auto fut = Future<>::Make();
    auto fut2 = fut.Then([]() { return Result<int>(42); });
    fut.MarkFinished();
    AssertSuccessful(fut2);
    auto result = *fut2.result();
    ASSERT_EQ(result, 42);
  }
  {
    // Propagate failure by returning failure
    auto fut = Future<>::Make();
    auto was_cb_run = false;
    auto fut2 = fut.Then(
        [&was_cb_run]() {
          was_cb_run = true;
          return Result<int>(42);
        },
        [](const Status& s) { return Result<int>(s); });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut2);
    ASSERT_FALSE(was_cb_run);
  }
}

TEST(FutureCompletionTest, FutureVoid) {
  {
    // Simple callback
    auto fut = Future<int>::Make();
    auto innerFut = Future<>::Make();
    int passed_in_result = 0;
    auto fut2 = fut.Then([&passed_in_result, innerFut](int i) {
      passed_in_result = i;
      return innerFut;
    });
    fut.MarkFinished(42);
    AssertNotFinished(fut2);
    innerFut.MarkFinished();
    AssertSuccessful(fut2);
    auto res = fut2.status();
    ASSERT_OK(res);
    ASSERT_EQ(passed_in_result, 42);
  }
  {
    // Precompleted future
    auto fut = Future<int>::Make();
    auto innerFut = Future<>::Make();
    innerFut.MarkFinished();
    int passed_in_result = 0;
    auto fut2 = fut.Then([&passed_in_result, innerFut](int i) {
      passed_in_result = i;
      return innerFut;
    });
    AssertNotFinished(fut2);
    fut.MarkFinished(42);
    AssertSuccessful(fut2);
    ASSERT_EQ(passed_in_result, 42);
  }
  {
    // Propagate failure by not having on_failure
    auto fut = Future<int>::Make();
    auto innerFut = Future<>::Make();
    auto was_cb_run = false;
    auto fut2 = fut.Then([innerFut, &was_cb_run](int) {
      was_cb_run = true;
      return innerFut;
    });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut2);
    if (IsFutureFinished(fut2.state())) {
      ASSERT_TRUE(fut2.status().IsIOError());
    }
    ASSERT_FALSE(was_cb_run);
  }
  {
    // Swallow failure by catching in on_failure
    auto fut = Future<int>::Make();
    auto innerFut = Future<>::Make();
    auto was_cb_run = false;
    auto fut2 = fut.Then(
        [innerFut, &was_cb_run](int) {
          was_cb_run = true;
          return innerFut;
        },
        [innerFut](const Status& s) { return innerFut; });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertNotFinished(fut2);
    innerFut.MarkFinished();
    AssertSuccessful(fut2);
    ASSERT_FALSE(was_cb_run);
  }
  {
    // From void
    auto fut = Future<>::Make();
    auto innerFut = Future<>::Make();
    auto fut2 = fut.Then([&innerFut]() { return innerFut; });
    fut.MarkFinished();
    AssertNotFinished(fut2);
    innerFut.MarkFinished();
    AssertSuccessful(fut2);
  }
  {
    // Propagate failure by returning failure
    auto fut = Future<>::Make();
    auto innerFut = Future<>::Make();
    auto fut2 = fut.Then([&innerFut]() { return innerFut; },
                         [](const Status& s) { return Future<>::MakeFinished(s); });
    fut.MarkFinished(Status::IOError("xxx"));
    AssertFailed(fut2);
  }
}

class FutureSchedulingTest : public testing::Test {
 public:
  internal::Executor* executor() { return mock_executor.get(); }

  int spawn_count() { return static_cast<int>(mock_executor->captured_tasks.size()); }

  void AssertRunSynchronously(const std::vector<int>& ids) { AssertIds(ids, true); }

  void AssertScheduled(const std::vector<int>& ids) { AssertIds(ids, false); }

  void AssertIds(const std::vector<int>& ids, bool should_be_synchronous) {
    for (auto id : ids) {
      ASSERT_EQ(should_be_synchronous, callbacks_run_synchronously.find(id) !=
                                           callbacks_run_synchronously.end());
    }
  }

  std::function<void(const Status&)> callback(int id) {
    return [this, id](const Status&) { callbacks_run_synchronously.insert(id); };
  }

  std::shared_ptr<DelayedExecutor> mock_executor = std::make_shared<DelayedExecutor>();
  std::unordered_set<int> callbacks_run_synchronously;
};

TEST_F(FutureSchedulingTest, ScheduleNever) {
  CallbackOptions options;
  options.should_schedule = ShouldSchedule::Never;
  options.executor = executor();
  // Successful future
  {
    auto fut = Future<>::Make();
    fut.AddCallback(callback(1), options);
    fut.MarkFinished();
    fut.AddCallback(callback(2), options);
    ASSERT_EQ(0, spawn_count());
    AssertRunSynchronously({1, 2});
  }
  // Failing future
  {
    auto fut = Future<>::Make();
    fut.AddCallback(callback(3), options);
    fut.MarkFinished(Status::Invalid("XYZ"));
    fut.AddCallback(callback(4), options);
    ASSERT_EQ(0, spawn_count());
    AssertRunSynchronously({3, 4});
  }
}

TEST_F(FutureSchedulingTest, ScheduleAlways) {
  CallbackOptions options;
  options.should_schedule = ShouldSchedule::Always;
  options.executor = executor();
  // Successful future
  {
    auto fut = Future<>::Make();
    fut.AddCallback(callback(1), options);
    fut.MarkFinished();
    fut.AddCallback(callback(2), options);
    ASSERT_EQ(2, spawn_count());
    AssertScheduled({1, 2});
  }
  // Failing future
  {
    auto fut = Future<>::Make();
    fut.AddCallback(callback(3), options);
    fut.MarkFinished(Status::Invalid("XYZ"));
    fut.AddCallback(callback(4), options);
    ASSERT_EQ(4, spawn_count());
    AssertScheduled({3, 4});
  }
}

TEST_F(FutureSchedulingTest, ScheduleIfUnfinished) {
  CallbackOptions options;
  options.should_schedule = ShouldSchedule::IfUnfinished;
  options.executor = executor();
  // Successful future
  {
    auto fut = Future<>::Make();
    fut.AddCallback(callback(1), options);
    fut.MarkFinished();
    fut.AddCallback(callback(2), options);
    ASSERT_EQ(1, spawn_count());
    AssertRunSynchronously({2});
    AssertScheduled({1});
  }
  // Failing future
  {
    auto fut = Future<>::Make();
    fut.AddCallback(callback(3), options);
    fut.MarkFinished(Status::Invalid("XYZ"));
    fut.AddCallback(callback(4), options);
    ASSERT_EQ(2, spawn_count());
    AssertRunSynchronously({4});
    AssertScheduled({3});
  }
}

TEST_F(FutureSchedulingTest, ScheduleIfDifferentExecutor) {
  struct : internal::Executor {
    int GetCapacity() override { return pool_->GetCapacity(); }

    bool OwnsThisThread() override { return pool_->OwnsThisThread(); }

    Status SpawnReal(internal::TaskHints hints, internal::FnOnce<void()> task,
                     StopToken stop_token, StopCallback&& stop_callback) override {
      ++spawn_count;
      return pool_->Spawn(hints, std::move(task), std::move(stop_token),
                          std::move(stop_callback));
    }

    std::atomic<int> spawn_count{0};
    internal::Executor* pool_ = internal::GetCpuThreadPool();
  } executor;

  CallbackOptions options;
  options.executor = &executor;
  options.should_schedule = ShouldSchedule::IfDifferentExecutor;
  auto pass_err = [](const Status& s) { return s; };

  std::atomic<bool> fut0_on_executor{false};
  std::atomic<bool> fut1_on_executor{false};

  auto fut0 = Future<>::Make();
  auto fut1 = Future<>::Make();

  auto fut0_done = fut0.Then(
      [&] {
        // marked finished on main thread -> must be scheduled to executor
        fut0_on_executor.store(executor.OwnsThisThread());

        fut1.MarkFinished();
      },
      pass_err, options);

  auto fut1_done = fut1.Then(
      [&] {
        // marked finished on executor -> no need to schedule
        fut1_on_executor.store(executor.OwnsThisThread());
      },
      pass_err, options);

  fut0.MarkFinished();

  AllComplete({fut0_done, fut1_done}).Wait();

  ASSERT_EQ(executor.spawn_count, 1);
  ASSERT_TRUE(fut0_on_executor);
  ASSERT_TRUE(fut1_on_executor);
}

TEST_F(FutureSchedulingTest, ScheduleAlwaysKeepsFutureAliveUntilCallback) {
  CallbackOptions options;
  options.should_schedule = ShouldSchedule::Always;
  options.executor = executor();
  {
    auto fut = Future<int>::Make();
    fut.AddCallback([](const Result<int> val) { ASSERT_EQ(7, *val); }, options);
    fut.MarkFinished(7);
  }
  std::move(mock_executor->captured_tasks[0])();
}

TEST(FutureAllTest, Empty) {
  auto combined = arrow::All(std::vector<Future<int>>{});
  auto after_assert = combined.Then(
      [](std::vector<Result<int>> results) { ASSERT_EQ(0, results.size()); });
  AssertSuccessful(after_assert);
}

TEST(FutureAllTest, Simple) {
  auto f1 = Future<int>::Make();
  auto f2 = Future<int>::Make();
  std::vector<Future<int>> futures = {f1, f2};
  auto combined = arrow::All(futures);

  auto after_assert = combined.Then([](std::vector<Result<int>> results) {
    ASSERT_EQ(2, results.size());
    ASSERT_EQ(1, *results[0]);
    ASSERT_EQ(2, *results[1]);
  });

  // Finish in reverse order, results should still be delivered in proper order
  AssertNotFinished(after_assert);
  f2.MarkFinished(2);
  AssertNotFinished(after_assert);
  f1.MarkFinished(1);
  AssertSuccessful(after_assert);
}

TEST(FutureAllTest, Failure) {
  auto f1 = Future<int>::Make();
  auto f2 = Future<int>::Make();
  auto f3 = Future<int>::Make();
  std::vector<Future<int>> futures = {f1, f2, f3};
  auto combined = arrow::All(futures);

  auto after_assert = combined.Then([](std::vector<Result<int>> results) {
    ASSERT_EQ(3, results.size());
    ASSERT_EQ(1, *results[0]);
    ASSERT_EQ(Status::IOError("XYZ"), results[1].status());
    ASSERT_EQ(3, *results[2]);
  });

  f1.MarkFinished(1);
  f2.MarkFinished(Status::IOError("XYZ"));
  f3.MarkFinished(3);

  AssertFinished(after_assert);
}

TEST(FutureAllCompleteTest, Empty) {
  Future<> combined = AllComplete(std::vector<Future<>>{});
  AssertSuccessful(combined);
}

TEST(FutureAllCompleteTest, Simple) {
  auto f1 = Future<int>::Make();
  auto f2 = Future<int>::Make();
  std::vector<Future<>> futures = {Future<>(f1), Future<>(f2)};
  auto combined = AllComplete(futures);
  AssertNotFinished(combined);
  f2.MarkFinished(2);
  AssertNotFinished(combined);
  f1.MarkFinished(1);
  AssertSuccessful(combined);
}

TEST(FutureAllCompleteTest, Failure) {
  auto f1 = Future<int>::Make();
  auto f2 = Future<int>::Make();
  auto f3 = Future<int>::Make();
  std::vector<Future<>> futures = {Future<>(f1), Future<>(f2), Future<>(f3)};
  auto combined = AllComplete(futures);
  AssertNotFinished(combined);
  f1.MarkFinished(1);
  AssertNotFinished(combined);
  f2.MarkFinished(Status::IOError("XYZ"));
  AssertFinished(combined);
  f3.MarkFinished(3);
  AssertFinished(combined);
  ASSERT_EQ(Status::IOError("XYZ"), combined.status());
}

TEST(FutureLoopTest, Sync) {
  struct {
    int i = 0;
    Future<int> Get() { return Future<int>::MakeFinished(i++); }
  } IntSource;

  bool do_fail = false;
  std::vector<int> ints;
  auto loop_body = [&] {
    return IntSource.Get().Then([&](int i) -> Result<ControlFlow<int>> {
      if (do_fail && i == 3) {
        return Status::IOError("xxx");
      }

      if (i == 5) {
        int sum = 0;
        for (int i : ints) sum += i;
        return Break(sum);
      }

      ints.push_back(i);
      return Continue();
    });
  };

  {
    auto sum_fut = Loop(loop_body);
    AssertSuccessful(sum_fut);

    ASSERT_OK_AND_ASSIGN(auto sum, sum_fut.result());
    ASSERT_EQ(sum, 0 + 1 + 2 + 3 + 4);
  }

  {
    do_fail = true;
    IntSource.i = 0;
    auto sum_fut = Loop(loop_body);
    AssertFailed(sum_fut);
    ASSERT_RAISES(IOError, sum_fut.result());
  }
}

TEST(FutureLoopTest, EmptyBreakValue) {
  Future<> none_fut =
      Loop([&] { return Future<>::MakeFinished().Then([&]() { return Break(); }); });
  AssertSuccessful(none_fut);
}

TEST(FutureLoopTest, EmptyLoop) {
  auto loop_body = []() -> Future<ControlFlow<int>> {
    return Future<ControlFlow<int>>::MakeFinished(Break(0));
  };
  auto loop_fut = Loop(loop_body);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto loop_res, loop_fut);
  ASSERT_EQ(loop_res, 0);
}

// TODO - Test provided by Ben but I don't understand how it can pass legitimately.
// Any future result will be passed by reference to the callbacks (as there can be
// multiple callbacks).  In the Loop construct it takes the break and forwards it
// on to the outer future.  Since there is no way to move a reference this can only
// be done by copying.
//
// In theory it should be safe since Loop is guaranteed to be the last callback added
// to the control future and so the value can be safely moved at that point.  However,
// I'm unable to reproduce whatever trick you had in ControlFlow to make this work.
// If we want to formalize this "last callback can steal" concept then we could add
// a "last callback" to Future which gets called with an rvalue instead of an lvalue
// reference but that seems overly complicated.
//
// Ben, can you recreate whatever trick you had in place before that allowed this to
// pass?  Perhaps some kind of cast.  Worst case, I can move back to using
// ControlFlow instead of std::optional
//
// TEST(FutureLoopTest, MoveOnlyBreakValue) {
//   Future<MoveOnlyDataType> one_fut = Loop([&] {
//     return Future<int>::MakeFinished(1).Then(
//         [&](int i) { return Break(MoveOnlyDataType(i)); });
//   });
//   AssertSuccessful(one_fut);
//   ASSERT_OK_AND_ASSIGN(auto one, std::move(one_fut).result());
//   ASSERT_EQ(one, 1);
// }

TEST(FutureLoopTest, StackOverflow) {
  // Looping over futures is normally a rather recursive task.  If the futures complete
  // synchronously (because they are already finished) it could lead to a stack overflow
  // if care is not taken.
  int counter = 0;
  auto loop_body = [&counter]() -> Future<ControlFlow<int>> {
    while (counter < 1000000) {
      counter++;
      return Future<ControlFlow<int>>::MakeFinished(Continue());
    }
    return Future<ControlFlow<int>>::MakeFinished(Break(-1));
  };
  auto loop_fut = Loop(loop_body);
  ASSERT_TRUE(loop_fut.Wait(0.1));
}

TEST(FutureLoopTest, AllowsBreakFutToBeDiscarded) {
  int counter = 0;
  auto loop_body = [&counter]() -> Future<ControlFlow<int>> {
    while (counter < 10) {
      counter++;
      return Future<ControlFlow<int>>::MakeFinished(Continue());
    }
    return Future<ControlFlow<int>>::MakeFinished(Break(-1));
  };
  auto loop_fut = Loop(loop_body).Then([](const int&) { return Status::OK(); });
  ASSERT_TRUE(loop_fut.Wait(0.1));
}

class MoveTrackingCallable {
 public:
  MoveTrackingCallable() {}

  ~MoveTrackingCallable() { valid_ = false; }

  MoveTrackingCallable(const MoveTrackingCallable& other) {}

  MoveTrackingCallable(MoveTrackingCallable&& other) { other.valid_ = false; }

  MoveTrackingCallable& operator=(const MoveTrackingCallable& other) { return *this; }

  MoveTrackingCallable& operator=(MoveTrackingCallable&& other) {
    other.valid_ = false;
    return *this;
  }

  Status operator()() {
    if (valid_) {
      return Status::OK();
    } else {
      return Status::Invalid("Invalid callback triggered");
    }
  }

 private:
  bool valid_ = true;
};

TEST(FutureCompletionTest, ReuseCallback) {
  auto fut = Future<>::Make();

  Future<> continuation;
  {
    MoveTrackingCallable callback;
    continuation = fut.Then(callback);
  }

  fut.MarkFinished();

  ASSERT_TRUE(continuation.is_finished());
  if (continuation.is_finished()) {
    ASSERT_OK(continuation.status());
  }
}

// --------------------------------------------------------------------
// Tests with an executor

template <typename T>
class FutureTestBase : public ::testing::Test {
 public:
  using ExecutorType = SimpleExecutor<T>;

  void MakeExecutor(int nfutures) { executor_.reset(new ExecutorType(nfutures)); }

  void MakeExecutor(int nfutures, std::vector<std::pair<int, bool>> immediate) {
    MakeExecutor(nfutures);
    executor_->SetFinished(std::move(immediate));
  }

  template <typename U>
  void RandomShuffle(std::vector<U>* values) {
    std::default_random_engine gen(seed_++);
    std::shuffle(values->begin(), values->end(), gen);
  }

  // Generate a sequence of randomly-sized ordered spans covering exactly [0, size).
  // Returns a vector of (start, stop) pairs.
  std::vector<std::pair<int, int>> RandomSequenceSpans(int size) {
    std::default_random_engine gen(seed_++);
    // The distribution of span sizes
    std::poisson_distribution<int> dist(5);
    std::vector<std::pair<int, int>> spans;
    int start = 0;
    while (start < size) {
      int stop = std::min(start + dist(gen), size);
      spans.emplace_back(start, stop);
      start = stop;
    }
    return spans;
  }

  void AssertAllNotFinished(const std::vector<int>& future_indices) {
    const auto& futures = executor_->futures();
    for (const auto fut_index : future_indices) {
      AssertNotFinished(futures[fut_index]);
    }
  }

  // Assert the given futures are *eventually* successful
  void AssertAllSuccessful(const std::vector<int>& future_indices) {
    const auto& futures = executor_->futures();
    for (const auto fut_index : future_indices) {
      ASSERT_OK(futures[fut_index].status());
      ASSERT_EQ(*futures[fut_index].result(), fut_index);
    }
  }

  // Assert the given futures are *eventually* failed
  void AssertAllFailed(const std::vector<int>& future_indices) {
    const auto& futures = executor_->futures();
    for (const auto fut_index : future_indices) {
      ASSERT_RAISES(UnknownError, futures[fut_index].status());
    }
  }

  // Assert the given futures are *eventually* successful
  void AssertSpanSuccessful(int start, int stop) {
    const auto& futures = executor_->futures();
    for (int fut_index = start; fut_index < stop; ++fut_index) {
      ASSERT_OK(futures[fut_index].status());
      ASSERT_EQ(*futures[fut_index].result(), fut_index);
    }
  }

  void AssertAllSuccessful() {
    AssertSpanSuccessful(0, static_cast<int>(executor_->futures().size()));
  }

  // Assert the given futures are successful *now*
  void AssertSpanSuccessfulNow(int start, int stop) {
    const auto& futures = executor_->futures();
    for (int fut_index = start; fut_index < stop; ++fut_index) {
      ASSERT_TRUE(IsFutureFinished(futures[fut_index].state()));
    }
  }

  void AssertAllSuccessfulNow() {
    AssertSpanSuccessfulNow(0, static_cast<int>(executor_->futures().size()));
  }

  void TestBasicWait() {
    MakeExecutor(4, {{1, true}, {2, false}});
    AssertAllNotFinished({0, 3});
    AssertAllSuccessful({1});
    AssertAllFailed({2});
    AssertAllNotFinished({0, 3});
    executor_->SetFinishedDeferred({{0, true}, {3, true}});
    AssertAllSuccessful({0, 1, 3});
  }

  void TestTimedWait() {
    MakeExecutor(2);
    const auto& futures = executor_->futures();
    ASSERT_FALSE(futures[0].Wait(kTinyWait));
    ASSERT_FALSE(futures[1].Wait(kTinyWait));
    AssertAllNotFinished({0, 1});
    executor_->SetFinishedDeferred({{0, true}, {1, true}});
    ASSERT_TRUE(futures[0].Wait(kLargeWait));
    ASSERT_TRUE(futures[1].Wait(kLargeWait));
    AssertAllSuccessfulNow();
  }

  void TestStressWait() {
#ifdef ARROW_VALGRIND
    const int N = 20;
#else
    const int N = 2000;
#endif
    MakeExecutor(N);
    const auto& futures = executor_->futures();
    const auto spans = RandomSequenceSpans(N);
    for (const auto& span : spans) {
      int start = span.first, stop = span.second;
      executor_->SetFinishedDeferred(start, stop);
      AssertSpanSuccessful(start, stop);
      if (stop < N) {
        AssertNotFinished(futures[stop]);
      }
    }
    AssertAllSuccessful();
  }

 protected:
  std::unique_ptr<ExecutorType> executor_;
  int seed_ = 42;
};

template <typename T>
class FutureWaitTest : public FutureTestBase<T> {};

using FutureWaitTestTypes = ::testing::Types<int, Foo, MoveOnlyDataType>;

TYPED_TEST_SUITE(FutureWaitTest, FutureWaitTestTypes);

TYPED_TEST(FutureWaitTest, BasicWait) { this->TestBasicWait(); }

TYPED_TEST(FutureWaitTest, TimedWait) { this->TestTimedWait(); }

TYPED_TEST(FutureWaitTest, StressWait) { this->TestStressWait(); }

namespace internal {
TEST(FnOnceTest, MoveOnlyDataType) {
  // ensuring this is valid guarantees we are making no unnecessary copies
  FnOnce<int(const MoveOnlyDataType&, MoveOnlyDataType, std::string)> fn =
      [](const MoveOnlyDataType& i0, MoveOnlyDataType i1, std::string copyable) {
        return *i0.data + *i1.data + (i0.moves * 1000) + (i1.moves * 100);
      };

  using arg0 = call_traits::argument_type<0, decltype(fn)>;
  using arg1 = call_traits::argument_type<1, decltype(fn)>;
  using arg2 = call_traits::argument_type<2, decltype(fn)>;
  static_assert(std::is_same<arg0, const MoveOnlyDataType&>::value, "");
  static_assert(std::is_same<arg1, MoveOnlyDataType>::value, "");
  static_assert(std::is_same<arg2, std::string>::value,
                "should not add a && to the call type (demanding rvalue unnecessarily)");

  MoveOnlyDataType i0{1}, i1{41};
  std::string copyable = "";
  ASSERT_EQ(std::move(fn)(i0, std::move(i1), copyable), 242);
  ASSERT_EQ(i0.moves, 0);
  ASSERT_EQ(i1.moves, 0);
}

TEST(FutureTest, MatcherExamples) {
  EXPECT_THAT(Future<int>::MakeFinished(Status::Invalid("arbitrary error")),
              Finishes(Raises(StatusCode::Invalid)));

  EXPECT_THAT(Future<int>::MakeFinished(Status::Invalid("arbitrary error")),
              Finishes(Raises(StatusCode::Invalid, testing::HasSubstr("arbitrary"))));

  // message doesn't match, so no match
  EXPECT_THAT(Future<int>::MakeFinished(Status::Invalid("arbitrary error")),
              Finishes(testing::Not(
                  Raises(StatusCode::Invalid, testing::HasSubstr("reasonable")))));

  // different error code, so no match
  EXPECT_THAT(Future<int>::MakeFinished(Status::TypeError("arbitrary error")),
              Finishes(testing::Not(Raises(StatusCode::Invalid))));

  // not an error, so no match
  EXPECT_THAT(Future<int>::MakeFinished(333),
              Finishes(testing::Not(Raises(StatusCode::Invalid))));

  EXPECT_THAT(Future<std::string>::MakeFinished("hello world"),
              Finishes(ResultWith(testing::HasSubstr("hello"))));

  // Matcher waits on Futures
  auto string_fut = Future<std::string>::Make();
  auto finisher = std::thread([&] {
    SleepABit();
    string_fut.MarkFinished("hello world");
  });
  EXPECT_THAT(string_fut, Finishes(ResultWith(testing::HasSubstr("hello"))));
  finisher.join();

  EXPECT_THAT(Future<std::string>::MakeFinished(Status::Invalid("XXX")),
              Finishes(testing::Not(ResultWith(testing::HasSubstr("hello")))));

  // holds a value, but that value doesn't match the given pattern
  EXPECT_THAT(Future<std::string>::MakeFinished("foo bar"),
              Finishes(testing::Not(ResultWith(testing::HasSubstr("hello")))));
}

}  // namespace internal
}  // namespace arrow
