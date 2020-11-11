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
#include "arrow/util/future_iterator.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
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

// A data type with only move constructors.
struct MoveOnlyDataType {
  explicit MoveOnlyDataType(int x) : data(new int(x)) {}

  MoveOnlyDataType(const MoveOnlyDataType& other) = delete;
  MoveOnlyDataType& operator=(const MoveOnlyDataType& other) = delete;

  MoveOnlyDataType(MoveOnlyDataType&& other) { MoveFrom(&other); }
  MoveOnlyDataType& operator=(MoveOnlyDataType&& other) {
    MoveFrom(&other);
    return *this;
  }

  ~MoveOnlyDataType() { Destroy(); }

  void Destroy() {
    if (data != nullptr) {
      delete data;
      data = nullptr;
    }
  }

  void MoveFrom(MoveOnlyDataType* other) {
    Destroy();
    data = other->data;
    other->data = nullptr;
  }

  int ToInt() const { return data == nullptr ? -42 : *data; }

  bool operator==(int other) const { return data != nullptr && *data == other; }
  bool operator==(const MoveOnlyDataType& other) const {
    return data != nullptr && other.data != nullptr && *data == *other.data;
  }
  friend bool operator==(int left, const MoveOnlyDataType& right) {
    return right == left;
  }

  int* data = nullptr;
};

template <>
struct IterationTraits<MoveOnlyDataType> {
  static MoveOnlyDataType End() { return MoveOnlyDataType(-1); }
};

template <typename T>
void AssertNotFinished(const Future<T>& fut) {
  ASSERT_FALSE(IsFutureFinished(fut.state()));
}

template <typename T>
void AssertFinished(const Future<T>& fut) {
  ASSERT_TRUE(IsFutureFinished(fut.state()));
}

// Assert the future is successful *now*
template <typename T>
void AssertSuccessful(const Future<T>& fut) {
  ASSERT_EQ(fut.state(), FutureState::SUCCESS);
  ASSERT_OK(fut.status());
}

// Assert the future is failed *now*
template <typename T>
void AssertFailed(const Future<T>& fut) {
  ASSERT_EQ(fut.state(), FutureState::FAILURE);
  ASSERT_FALSE(fut.status().ok());
}

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
    ABORT_NOT_OK(pool_->Spawn([=]() { SetFinished(pairs); }));
  }

  // Mark future successful
  void SetFinished(int fut_index) { futures_[fut_index].MarkFinished(T(fut_index)); }

  void SetFinishedDeferred(int fut_index) {
    std::this_thread::sleep_for(kYieldDuration);
    ABORT_NOT_OK(pool_->Spawn([=]() { SetFinished(fut_index); }));
  }

  // Mark all futures in [start, stop) successful
  void SetFinished(int start, int stop) {
    for (int fut_index = start; fut_index < stop; ++fut_index) {
      futures_[fut_index].MarkFinished(T(fut_index));
    }
  }

  void SetFinishedDeferred(int start, int stop) {
    std::this_thread::sleep_for(kYieldDuration);
    ABORT_NOT_OK(pool_->Spawn([=]() { SetFinished(start, stop); }));
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
    // MarkFinished(Foo)
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

TEST(FutureSyncTest, MoveOnlyDataType) {
  {
    // MarkFinished(MoveOnlyDataType)
    auto fut = Future<MoveOnlyDataType>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished(MoveOnlyDataType(42));
    AssertSuccessful(fut);
    const auto& res = fut.result();
    ASSERT_TRUE(res.ok());
    ASSERT_EQ(*res, 42);
    ASSERT_OK_AND_ASSIGN(MoveOnlyDataType value, std::move(fut).result());
    ASSERT_EQ(value, 42);
  }
  {
    // MarkFinished(Result<MoveOnlyDataType>)
    auto fut = Future<MoveOnlyDataType>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished(Result<MoveOnlyDataType>(MoveOnlyDataType(43)));
    AssertSuccessful(fut);
    ASSERT_OK_AND_ASSIGN(MoveOnlyDataType value, std::move(fut).result());
    ASSERT_EQ(value, 43);
  }
  {
    // MarkFinished(failed Result<MoveOnlyDataType>)
    auto fut = Future<MoveOnlyDataType>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished(Result<MoveOnlyDataType>(Status::IOError("xxx")));
    AssertFailed(fut);
    ASSERT_RAISES(IOError, fut.status());
    const auto& res = fut.result();
    ASSERT_TRUE(res.status().IsIOError());
    ASSERT_RAISES(IOError, std::move(fut).result());
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
    auto res = fut.result();
    ASSERT_OK(res);
    res = std::move(fut.result());
    ASSERT_OK(res);
  }
  {
    // MarkFinished(Status)
    auto fut = Future<>::Make();
    AssertNotFinished(fut);
    fut.MarkFinished(Status::OK());
    AssertSuccessful(fut);
  }
  {
    // MakeFinished(Status)
    auto fut = Future<>::MakeFinished(Status::OK());
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
    const int N = 2000;
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

  void TestBasicWaitForAny() {
    MakeExecutor(4, {{1, true}, {2, false}});
    auto& futures = executor_->futures();

    std::vector<Future<T>*> wait_on = {&futures[0], &futures[1]};
    auto finished = WaitForAny(wait_on);
    ASSERT_THAT(finished, testing::ElementsAre(1));

    wait_on = {&futures[1], &futures[2], &futures[3]};
    while (finished.size() < 2) {
      finished = WaitForAny(wait_on);
    }
    ASSERT_THAT(finished, testing::UnorderedElementsAre(0, 1));

    executor_->SetFinished(3);
    finished = WaitForAny(futures);
    ASSERT_THAT(finished, testing::UnorderedElementsAre(1, 2, 3));

    executor_->SetFinishedDeferred(0);
    // Busy wait until the state change is done
    while (finished.size() < 4) {
      finished = WaitForAny(futures);
    }
    ASSERT_THAT(finished, testing::UnorderedElementsAre(0, 1, 2, 3));
  }

  void TestTimedWaitForAny() {
    MakeExecutor(4, {{1, true}, {2, false}});
    auto& futures = executor_->futures();

    std::vector<int> finished;
    std::vector<Future<T>*> wait_on = {&futures[0], &futures[3]};
    finished = WaitForAny(wait_on, kTinyWait);
    ASSERT_EQ(finished.size(), 0);

    executor_->SetFinished(3);
    finished = WaitForAny(wait_on, kLargeWait);
    ASSERT_THAT(finished, testing::ElementsAre(1));

    executor_->SetFinished(0);
    while (finished.size() < 2) {
      finished = WaitForAny(wait_on, kTinyWait);
    }
    ASSERT_THAT(finished, testing::UnorderedElementsAre(0, 1));

    while (finished.size() < 4) {
      finished = WaitForAny(futures, kTinyWait);
    }
    ASSERT_THAT(finished, testing::UnorderedElementsAre(0, 1, 2, 3));
  }

  void TestBasicWaitForAll() {
    MakeExecutor(4, {{1, true}, {2, false}});
    auto& futures = executor_->futures();

    std::vector<Future<T>*> wait_on = {&futures[1], &futures[2]};
    WaitForAll(wait_on);
    AssertSpanSuccessfulNow(1, 3);

    executor_->SetFinishedDeferred({{0, true}, {3, false}});
    WaitForAll(futures);
    AssertAllSuccessfulNow();
    WaitForAll(futures);
  }

  void TestTimedWaitForAll() {
    MakeExecutor(4, {{1, true}, {2, false}});
    auto& futures = executor_->futures();

    ASSERT_FALSE(WaitForAll(futures, kTinyWait));

    executor_->SetFinishedDeferred({{0, true}, {3, false}});
    ASSERT_TRUE(WaitForAll(futures, kLargeWait));
    AssertAllSuccessfulNow();
  }

  void TestStressWaitForAny() {
    const int N = 300;
    MakeExecutor(N);
    const auto& futures = executor_->futures();
    const auto spans = RandomSequenceSpans(N);
    std::vector<int> finished;
    // Note this loop is potentially O(N**2), because we're copying
    // O(N)-sized vector when waiting.
    for (const auto& span : spans) {
      int start = span.first, stop = span.second;
      executor_->SetFinishedDeferred(start, stop);
      size_t last_finished_size = finished.size();
      finished = WaitForAny(futures);
      ASSERT_GE(finished.size(), last_finished_size);
      // The spans are contiguous and ordered, so `stop` is also the number
      // of futures for which SetFinishedDeferred() was called.
      ASSERT_LE(finished.size(), static_cast<size_t>(stop));
    }
    // Semi-busy wait for all futures to be finished
    while (finished.size() < static_cast<size_t>(N)) {
      finished = WaitForAny(futures);
    }
    AssertAllSuccessfulNow();
  }

  void TestStressWaitForAll() {
    const int N = 300;
    MakeExecutor(N);
    const auto& futures = executor_->futures();
    const auto spans = RandomSequenceSpans(N);
    // Note this loop is potentially O(N**2), because we're copying
    // O(N)-sized vector when waiting.
    for (const auto& span : spans) {
      int start = span.first, stop = span.second;
      executor_->SetFinishedDeferred(start, stop);
      bool finished = WaitForAll(futures, kTinyWait);
      if (stop < N) {
        ASSERT_FALSE(finished);
      }
    }
    ASSERT_TRUE(WaitForAll(futures, kLargeWait));
    AssertAllSuccessfulNow();
  }

  void TestBasicAsCompleted() {
    {
      MakeExecutor(4, {{1, true}, {2, true}});
      executor_->SetFinishedDeferred({{0, true}, {3, true}});
      auto it = MakeAsCompletedIterator(executor_->futures());
      std::vector<T> values = IteratorToVector(std::move(it));
      ASSERT_THAT(values, testing::UnorderedElementsAre(0, 1, 2, 3));
    }
    {
      // Check that AsCompleted is opportunistic, it yields elements in order
      // of completion.
      MakeExecutor(4, {{2, true}});
      auto it = MakeAsCompletedIterator(executor_->futures());
      ASSERT_OK_AND_EQ(2, it.Next());
      executor_->SetFinishedDeferred({{3, true}});
      ASSERT_OK_AND_EQ(3, it.Next());
      executor_->SetFinishedDeferred({{0, true}});
      ASSERT_OK_AND_EQ(0, it.Next());
      executor_->SetFinishedDeferred({{1, true}});
      ASSERT_OK_AND_EQ(1, it.Next());
      ASSERT_OK_AND_EQ(IterationTraits<T>::End(), it.Next());
      ASSERT_OK_AND_EQ(IterationTraits<T>::End(), it.Next());  // idempotent
    }
  }

  void TestErrorsAsCompleted() {
    MakeExecutor(4, {{1, true}, {2, false}});
    executor_->SetFinishedDeferred({{0, true}, {3, false}});
    auto it = MakeAsCompletedIterator(executor_->futures());
    auto results = IteratorToResults(std::move(it));
    ASSERT_THAT(results.values, testing::UnorderedElementsAre(0, 1));
    ASSERT_EQ(results.errors.size(), 2);
    ASSERT_RAISES(UnknownError, results.errors[0]);
    ASSERT_RAISES(UnknownError, results.errors[1]);
  }

  void TestStressAsCompleted() {
    const int N = 1000;
    MakeExecutor(N);

    // Launch a worker thread that will finish random spans of futures,
    // in random order.
    auto spans = RandomSequenceSpans(N);
    RandomShuffle(&spans);
    auto feed_iterator = [&]() {
      for (const auto& span : spans) {
        int start = span.first, stop = span.second;
        executor_->SetFinishedDeferred(start, stop);  // will sleep a bit
      }
    };
    auto worker = std::thread(std::move(feed_iterator));
    auto it = MakeAsCompletedIterator(executor_->futures());
    auto results = IteratorToResults(std::move(it));
    worker.join();

    ASSERT_EQ(results.values.size(), static_cast<size_t>(N));
    ASSERT_EQ(results.errors.size(), 0);
    std::vector<int> expected(N);
    std::iota(expected.begin(), expected.end(), 0);
    std::vector<int> actual(N);
    std::transform(results.values.begin(), results.values.end(), actual.begin(),
                   [](const T& value) { return value.ToInt(); });
    std::sort(actual.begin(), actual.end());
    ASSERT_EQ(expected, actual);
  }

 protected:
  std::unique_ptr<ExecutorType> executor_;
  int seed_ = 42;
};

template <typename T>
class FutureTest : public FutureTestBase<T> {};

using FutureTestTypes = ::testing::Types<int, Foo, MoveOnlyDataType>;

TYPED_TEST_SUITE(FutureTest, FutureTestTypes);

TYPED_TEST(FutureTest, BasicWait) { this->TestBasicWait(); }

TYPED_TEST(FutureTest, TimedWait) { this->TestTimedWait(); }

TYPED_TEST(FutureTest, StressWait) { this->TestStressWait(); }

TYPED_TEST(FutureTest, BasicWaitForAny) { this->TestBasicWaitForAny(); }

TYPED_TEST(FutureTest, TimedWaitForAny) { this->TestTimedWaitForAny(); }

TYPED_TEST(FutureTest, StressWaitForAny) { this->TestStressWaitForAny(); }

TYPED_TEST(FutureTest, BasicWaitForAll) { this->TestBasicWaitForAll(); }

TYPED_TEST(FutureTest, TimedWaitForAll) { this->TestTimedWaitForAll(); }

TYPED_TEST(FutureTest, StressWaitForAll) { this->TestStressWaitForAll(); }

template <typename T>
class FutureIteratorTest : public FutureTestBase<T> {};

using FutureIteratorTestTypes = ::testing::Types<Foo, MoveOnlyDataType>;

TYPED_TEST_SUITE(FutureIteratorTest, FutureIteratorTestTypes);

TYPED_TEST(FutureIteratorTest, BasicAsCompleted) { this->TestBasicAsCompleted(); }

TYPED_TEST(FutureIteratorTest, ErrorsAsCompleted) { this->TestErrorsAsCompleted(); }

TYPED_TEST(FutureIteratorTest, StressAsCompleted) { this->TestStressAsCompleted(); }

}  // namespace arrow
