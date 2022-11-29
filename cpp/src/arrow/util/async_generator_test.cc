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
#include <mutex>
#include <optional>
#include <random>
#include <thread>
#include <unordered_set>
#include <utility>

#include "arrow/io/slow.h"
#include "arrow/testing/async_test_util.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/async_util.h"
#include "arrow/util/test_common.h"
#include "arrow/util/vector.h"

namespace arrow {

template <typename T>
AsyncGenerator<T> MakeJittery(AsyncGenerator<T> source) {
  auto latency_generator = arrow::io::LatencyGenerator::Make(0.01);
  return MakeMappedGenerator(std::move(source), [latency_generator](const T& res) {
    auto out = Future<T>::Make();
    std::thread([out, res, latency_generator]() mutable {
      latency_generator->Sleep();
      out.MarkFinished(res);
    }).detach();
    return out;
  });
}

// Yields items with a small pause between each one from a background thread
std::function<Future<TestInt>()> BackgroundAsyncVectorIt(
    std::vector<TestInt> v, bool sleep = true, int max_q = kDefaultBackgroundMaxQ,
    int q_restart = kDefaultBackgroundQRestart) {
  auto pool = internal::GetCpuThreadPool();
  auto slow_iterator = PossiblySlowVectorIt(v, sleep);
  EXPECT_OK_AND_ASSIGN(
      auto background,
      MakeBackgroundGenerator<TestInt>(std::move(slow_iterator), pool, max_q, q_restart));
  return MakeTransferredGenerator(background, pool);
}

std::function<Future<TestInt>()> NewBackgroundAsyncVectorIt(std::vector<TestInt> v,
                                                            bool sleep = true) {
  auto pool = internal::GetCpuThreadPool();
  auto iterator = VectorIt(v);
  auto slow_iterator = MakeTransformedIterator<TestInt, TestInt>(
      std::move(iterator), [sleep](TestInt item) -> Result<TransformFlow<TestInt>> {
        if (sleep) {
          SleepABit();
        }
        return TransformYield(item);
      });

  EXPECT_OK_AND_ASSIGN(auto background,
                       MakeBackgroundGenerator<TestInt>(std::move(slow_iterator), pool));
  return MakeTransferredGenerator(background, pool);
}

template <typename T>
void AssertAsyncGeneratorMatch(std::vector<T> expected, AsyncGenerator<T> actual) {
  auto vec_future = CollectAsyncGenerator(std::move(actual));
  EXPECT_OK_AND_ASSIGN(auto vec, vec_future.result());
  EXPECT_EQ(expected, vec);
}

template <typename T>
void AssertGeneratorExhausted(AsyncGenerator<T>& gen) {
  ASSERT_FINISHES_OK_AND_ASSIGN(auto next, gen());
  ASSERT_TRUE(IsIterationEnd(next));
}

// --------------------------------------------------------------------
// Asynchronous iterator tests

template <typename T>
class ReentrantCheckerGuard;

template <typename T>
ReentrantCheckerGuard<T> ExpectNotAccessedReentrantly(AsyncGenerator<T>* generator);

template <typename T>
class ReentrantChecker {
 public:
  Future<T> operator()() {
    if (state_->generated_unfinished_future.load()) {
      state_->valid.store(false);
    }
    state_->generated_unfinished_future.store(true);
    auto result = state_->source();
    return result.Then(Callback{state_});
  }

  bool valid() { return state_->valid.load(); }

 private:
  explicit ReentrantChecker(AsyncGenerator<T> source)
      : state_(std::make_shared<State>(std::move(source))) {}

  friend ReentrantCheckerGuard<T> ExpectNotAccessedReentrantly<T>(
      AsyncGenerator<T>* generator);

  struct State {
    explicit State(AsyncGenerator<T> source_)
        : source(std::move(source_)), generated_unfinished_future(false), valid(true) {}

    AsyncGenerator<T> source;
    std::atomic<bool> generated_unfinished_future;
    std::atomic<bool> valid;
  };
  struct Callback {
    Future<T> operator()(const T& result) {
      state_->generated_unfinished_future.store(false);
      return result;
    }
    std::shared_ptr<State> state_;
  };

  std::shared_ptr<State> state_;
};

template <typename T>
class ReentrantCheckerGuard {
 public:
  explicit ReentrantCheckerGuard(ReentrantChecker<T> checker)
      : checker_(std::move(checker)) {}

  ARROW_DISALLOW_COPY_AND_ASSIGN(ReentrantCheckerGuard);
  ReentrantCheckerGuard(ReentrantCheckerGuard&& other) : checker_(other.checker_) {
    if (other.owner_) {
      other.owner_ = false;
      owner_ = true;
    } else {
      owner_ = false;
    }
  }
  ReentrantCheckerGuard& operator=(ReentrantCheckerGuard&& other) {
    checker_ = other.checker_;
    if (other.owner_) {
      other.owner_ = false;
      owner_ = true;
    } else {
      owner_ = false;
    }
    return *this;
  }

  ~ReentrantCheckerGuard() {
    if (owner_ && !checker_.valid()) {
      ADD_FAILURE() << "A generator was accessed reentrantly when the test asserted it "
                       "should not be.";
    }
  }

 private:
  ReentrantChecker<T> checker_;
  bool owner_ = true;
};

template <typename T>
ReentrantCheckerGuard<T> ExpectNotAccessedReentrantly(AsyncGenerator<T>* generator) {
  auto reentrant_checker = ReentrantChecker<T>(*generator);
  *generator = reentrant_checker;
  return ReentrantCheckerGuard<T>(reentrant_checker);
}

class GeneratorTestFixture : public ::testing::TestWithParam<bool> {
 public:
  ~GeneratorTestFixture() override = default;

 protected:
  AsyncGenerator<TestInt> MakeSource(const std::vector<TestInt>& items) {
    std::vector<TestInt> wrapped(items.begin(), items.end());
    auto gen = util::AsyncVectorIt(std::move(wrapped));
    if (IsSlow()) {
      return util::SlowdownABit(std::move(gen));
    }
    return gen;
  }

  AsyncGenerator<TestInt> MakeEmptySource() { return MakeSource({}); }

  AsyncGenerator<TestInt> MakeFailingSource() {
    AsyncGenerator<TestInt> gen = [] {
      return Future<TestInt>::MakeFinished(Status::Invalid("XYZ"));
    };
    if (IsSlow()) {
      return util::SlowdownABit(std::move(gen));
    }
    return gen;
  }

  int GetNumItersForStress() {
    // Run fewer trials for the slow case since they take longer
    if (IsSlow()) {
      return 10;
    } else {
      return 100;
    }
  }

  bool IsSlow() { return GetParam(); }
};

template <typename T>
class ManualIteratorControl {
 public:
  virtual ~ManualIteratorControl() {}
  virtual void Push(Result<T> result) = 0;
  virtual uint32_t times_polled() = 0;
};

template <typename T>
class PushIterator : public ManualIteratorControl<T> {
 public:
  PushIterator() : state_(std::make_shared<State>()) {}
  virtual ~PushIterator() {}

  Result<T> Next() {
    std::unique_lock<std::mutex> lk(state_->mx);
    state_->times_polled++;
    if (!state_->cv.wait_for(lk, std::chrono::seconds(300),
                             [&] { return !state_->items.empty(); })) {
      return Status::Invalid("Timed out waiting for PushIterator");
    }
    auto next = std::move(state_->items.front());
    state_->items.pop();
    return next;
  }

  void Push(Result<T> result) override {
    {
      std::lock_guard<std::mutex> lg(state_->mx);
      state_->items.push(std::move(result));
    }
    state_->cv.notify_one();
  }

  uint32_t times_polled() override {
    std::lock_guard<std::mutex> lg(state_->mx);
    return state_->times_polled;
  }

 private:
  struct State {
    uint32_t times_polled = 0;
    std::mutex mx;
    std::condition_variable cv;
    std::queue<Result<T>> items;
  };

  std::shared_ptr<State> state_;
};

template <typename T>
Iterator<T> MakePushIterator(std::shared_ptr<ManualIteratorControl<T>>* out) {
  auto iter = std::make_shared<PushIterator<T>>();
  *out = iter;
  return Iterator<T>(*iter);
}

template <typename T>
class ManualGenerator {
 public:
  ManualGenerator() : times_polled_(std::make_shared<uint32_t>()) {}

  Future<T> operator()() {
    (*times_polled_)++;
    return source_();
  }

  uint32_t times_polled() const { return *times_polled_; }
  typename PushGenerator<T>::Producer producer() { return source_.producer(); }

 private:
  PushGenerator<T> source_;
  std::shared_ptr<uint32_t> times_polled_;
};

TEST(TestAsyncUtil, Visit) {
  auto generator = util::AsyncVectorIt<TestInt>({1, 2, 3});
  unsigned int sum = 0;
  auto sum_future = VisitAsyncGenerator<TestInt>(generator, [&sum](TestInt item) {
    sum += item.value;
    return Status::OK();
  });
  ASSERT_TRUE(sum_future.is_finished());
  ASSERT_EQ(6, sum);
}

TEST(TestAsyncUtil, Collect) {
  std::vector<TestInt> expected = {1, 2, 3};
  auto generator = util::AsyncVectorIt(expected);
  auto collected = CollectAsyncGenerator(generator);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto collected_val, collected);
  ASSERT_EQ(expected, collected_val);
}

TEST(TestAsyncUtil, Map) {
  std::vector<TestInt> input = {1, 2, 3};
  auto generator = util::AsyncVectorIt(input);
  std::function<TestStr(const TestInt&)> mapper = [](const TestInt& in) {
    return std::to_string(in.value);
  };
  auto mapped = MakeMappedGenerator(std::move(generator), mapper);
  std::vector<TestStr> expected{"1", "2", "3"};
  AssertAsyncGeneratorMatch(expected, mapped);
}

TEST(TestAsyncUtil, MapAsync) {
  std::vector<TestInt> input = {1, 2, 3};
  auto generator = util::AsyncVectorIt(input);
  std::function<Future<TestStr>(const TestInt&)> mapper = [](const TestInt& in) {
    return SleepABitAsync().Then([in]() { return TestStr(std::to_string(in.value)); });
  };
  auto mapped = MakeMappedGenerator(std::move(generator), mapper);
  std::vector<TestStr> expected{"1", "2", "3"};
  AssertAsyncGeneratorMatch(expected, mapped);
}

TEST(TestAsyncUtil, MapReentrant) {
  std::vector<TestInt> input = {1, 2};
  auto source = util::AsyncVectorIt(input);
  util::TrackingGenerator<TestInt> tracker(std::move(source));
  source = MakeTransferredGenerator(AsyncGenerator<TestInt>(tracker),
                                    internal::GetCpuThreadPool());

  std::atomic<int> map_tasks_running(0);
  // Mapper blocks until can_proceed is marked finished, should start multiple map tasks
  Future<> can_proceed = Future<>::Make();
  std::function<Future<TestStr>(const TestInt&)> mapper = [&](const TestInt& in) {
    map_tasks_running.fetch_add(1);
    return can_proceed.Then([in]() { return TestStr(std::to_string(in.value)); });
  };
  auto mapped = MakeMappedGenerator(std::move(source), mapper);

  EXPECT_EQ(0, tracker.num_read());

  auto one = mapped();
  auto two = mapped();

  BusyWait(10, [&] { return map_tasks_running.load() == 2; });
  EXPECT_EQ(2, map_tasks_running.load());
  EXPECT_EQ(2, tracker.num_read());

  auto end_one = mapped();
  auto end_two = mapped();

  can_proceed.MarkFinished();
  ASSERT_FINISHES_OK_AND_ASSIGN(auto oneval, one);
  EXPECT_EQ("1", oneval.value);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto twoval, two);
  EXPECT_EQ("2", twoval.value);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto end, end_one);
  ASSERT_EQ(IterationTraits<TestStr>::End(), end);
  ASSERT_FINISHES_OK_AND_ASSIGN(end, end_two);
  ASSERT_EQ(IterationTraits<TestStr>::End(), end);
}

TEST(TestAsyncUtil, MapParallelStress) {
  constexpr int NTASKS = 10;
  constexpr int NITEMS = 10;
  for (int i = 0; i < NTASKS; i++) {
    auto gen = MakeVectorGenerator(RangeVector(NITEMS));
    gen = util::SlowdownABit(std::move(gen));
    auto guard = ExpectNotAccessedReentrantly(&gen);
    std::function<TestStr(const TestInt&)> mapper = [](const TestInt& in) {
      SleepABit();
      return std::to_string(in.value);
    };
    auto mapped = MakeMappedGenerator(std::move(gen), mapper);
    mapped = MakeReadaheadGenerator(mapped, 8);
    ASSERT_FINISHES_OK_AND_ASSIGN(auto collected, CollectAsyncGenerator(mapped));
    ASSERT_EQ(NITEMS, collected.size());
  }
}

TEST(TestAsyncUtil, MapQueuingFailStress) {
  constexpr int NTASKS = 10;
  constexpr int NITEMS = 10;
  for (bool slow : {true, false}) {
    for (int i = 0; i < NTASKS; i++) {
      std::shared_ptr<std::atomic<bool>> done = std::make_shared<std::atomic<bool>>();
      auto inner = util::AsyncVectorIt(RangeVector(NITEMS));
      if (slow) inner = MakeJittery(inner);
      auto gen = util::FailAt(inner, NITEMS / 2);
      std::function<TestStr(const TestInt&)> mapper = [done](const TestInt& in) {
        if (done->load()) {
          ADD_FAILURE() << "Callback called after generator sent end signal";
        }
        return std::to_string(in.value);
      };
      auto mapped = MakeMappedGenerator(std::move(gen), mapper);
      auto readahead = MakeReadaheadGenerator(std::move(mapped), 8);
      ASSERT_FINISHES_AND_RAISES(Invalid, CollectAsyncGenerator(std::move(readahead)));
      done->store(true);
    }
  }
}

TEST(TestAsyncUtil, MapTaskFail) {
  std::vector<TestInt> input = {1, 2, 3};
  auto generator = util::AsyncVectorIt(input);
  std::function<Result<TestStr>(const TestInt&)> mapper =
      [](const TestInt& in) -> Result<TestStr> {
    if (in.value == 2) {
      return Status::Invalid("XYZ");
    }
    return TestStr(std::to_string(in.value));
  };
  auto mapped = MakeMappedGenerator(std::move(generator), mapper);
  ASSERT_FINISHES_AND_RAISES(Invalid, CollectAsyncGenerator(mapped));
}

TEST(TestAsyncUtil, MapTaskDelayedFail) {
  // Regression test for an edge case in MappingGenerator
  auto push = PushGenerator<TestInt>();
  auto producer = push.producer();
  AsyncGenerator<TestInt> generator = push;

  auto delayed = Future<TestStr>::Make();
  std::function<Future<TestStr>(const TestInt&)> mapper =
      [=](const TestInt& in) -> Future<TestStr> {
    if (in.value == 1) return delayed;
    return TestStr(std::to_string(in.value));
  };
  auto mapped = MakeMappedGenerator(std::move(generator), mapper);

  producer.Push(TestInt(1));
  auto fut = mapped();
  SleepABit();
  ASSERT_FALSE(fut.is_finished());
  // At this point there should be nothing in waiting_jobs, so the
  // next call will push something to the queue and schedule Callback
  auto fut2 = mapped();
  // There's now one job in waiting_jobs. Failing the original task will
  // purge the queue.
  delayed.MarkFinished(Status::Invalid("XYZ"));
  ASSERT_FINISHES_AND_RAISES(Invalid, fut);
  // However, Callback can still run once we fulfill the remaining
  // request. Callback needs to see that the generator is finished and
  // bail out, instead of trying to manipulate waiting_jobs.
  producer.Push(TestInt(2));
  ASSERT_FINISHES_OK_AND_EQ(TestStr(), fut2);
}

TEST(TestAsyncUtil, MapSourceFail) {
  std::vector<TestInt> input = {1, 2, 3};
  auto generator = util::FailAt(util::AsyncVectorIt(input), 1);
  std::function<Result<TestStr>(const TestInt&)> mapper =
      [](const TestInt& in) -> Result<TestStr> {
    return TestStr(std::to_string(in.value));
  };
  auto mapped = MakeMappedGenerator(std::move(generator), mapper);
  ASSERT_FINISHES_AND_RAISES(Invalid, CollectAsyncGenerator(mapped));
}

TEST(TestAsyncUtil, Concatenated) {
  std::vector<TestInt> inputOne{1, 2, 3};
  std::vector<TestInt> inputTwo{4, 5, 6};
  std::vector<TestInt> expected{1, 2, 3, 4, 5, 6};
  auto gen = util::AsyncVectorIt<AsyncGenerator<TestInt>>(
      {util::AsyncVectorIt<TestInt>(inputOne), util::AsyncVectorIt<TestInt>(inputTwo)});
  auto concat = MakeConcatenatedGenerator(gen);
  AssertAsyncGeneratorMatch(expected, concat);
}

class FromFutureFixture : public GeneratorTestFixture {};

TEST_P(FromFutureFixture, Basic) {
  auto source = Future<std::vector<TestInt>>::MakeFinished(RangeVector(3));
  if (IsSlow()) {
    source = SleepABitAsync().Then(
        []() -> Result<std::vector<TestInt>> { return RangeVector(3); });
  }
  auto slow = IsSlow();
  auto to_gen = source.Then([slow](const std::vector<TestInt>& vec) {
    auto vec_gen = MakeVectorGenerator(vec);
    if (slow) {
      return util::SlowdownABit(std::move(vec_gen));
    }
    return vec_gen;
  });
  auto gen = MakeFromFuture(std::move(to_gen));
  auto collected = CollectAsyncGenerator(std::move(gen));
  ASSERT_FINISHES_OK_AND_EQ(RangeVector(3), collected);
}

INSTANTIATE_TEST_SUITE_P(FromFutureTests, FromFutureFixture,
                         ::testing::Values(false, true));

class MergedGeneratorTestFixture : public GeneratorTestFixture {};

TEST_P(MergedGeneratorTestFixture, Merged) {
  auto gen = util::AsyncVectorIt<AsyncGenerator<TestInt>>(
      {MakeSource({1, 2, 3}), MakeSource({4, 5, 6})});

  auto concat_gen = MakeMergedGenerator(gen, 10);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto concat, CollectAsyncGenerator(concat_gen));
  auto concat_ints =
      internal::MapVector([](const TestInt& val) { return val.value; }, concat);
  std::set<int> concat_set(concat_ints.begin(), concat_ints.end());

  std::set<int> expected{1, 2, 4, 3, 5, 6};
  ASSERT_EQ(expected, concat_set);
}

TEST_P(MergedGeneratorTestFixture, OuterSubscriptionEmpty) {
  auto gen = util::AsyncVectorIt<AsyncGenerator<TestInt>>({});
  if (IsSlow()) {
    gen = util::SlowdownABit(gen);
  }
  auto merged_gen = MakeMergedGenerator(gen, 10);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto collected,
                                CollectAsyncGenerator(std::move(merged_gen)));
  ASSERT_TRUE(collected.empty());
}

TEST_P(MergedGeneratorTestFixture, MergedInnerFail) {
  auto gen = util::AsyncVectorIt<AsyncGenerator<TestInt>>(
      {MakeSource({1, 2, 3}), util::FailAt(MakeSource({1, 2, 3}), 1),
       MakeSource({1, 2, 3})});
  auto merged_gen = MakeMergedGenerator(gen, 10);
  // Merged generator can be pulled async-reentrantly and we need to make
  // sure, if it is, that all futures are marked complete, even if there is an error
  std::vector<Future<TestInt>> futures;
  for (int i = 0; i < 20; i++) {
    futures.push_back(merged_gen());
  }
  // Items could come in any order so the only guarantee is that we see at least
  // one item before the failure.  After the failure the behavior is undefined
  // except that we know the futures must complete.
  bool error_seen = false;
  for (int i = 0; i < 20; i++) {
    Future<TestInt> fut = futures[i];
    ASSERT_TRUE(fut.Wait(arrow::kDefaultAssertFinishesWaitSeconds));
    Status status = futures[i].status();
    if (!status.ok()) {
      ASSERT_GT(i, 0);
      if (!error_seen) {
        error_seen = true;
        ASSERT_TRUE(status.IsInvalid());
      }
    }
  }
}

TEST_P(MergedGeneratorTestFixture, MergedInnerFailCleanup) {
  // The purpose of this test is to ensure we do not emit an error until all outstanding
  // futures have completed.  This is part of the AsyncGenerator contract
  std::shared_ptr<GatingTask> failing_task_gate = GatingTask::Make();
  std::shared_ptr<GatingTask> passing_task_gate = GatingTask::Make();
  // A passing inner source emits one item and then waits on a gate and then
  // emits a terminal item.
  //
  // A failing inner source emits one item and then waits on a gate and then
  // emits an error.
  auto make_source = [&](bool fails) -> AsyncGenerator<TestInt> {
    std::shared_ptr<std::atomic<int>> count = std::make_shared<std::atomic<int>>(0);
    return [&, fails, count]() -> Future<TestInt> {
      int my_count = (*count)++;
      if (my_count == 1) {
        if (fails) {
          return failing_task_gate->AsyncTask().Then(
              []() -> Result<TestInt> { return Status::Invalid("XYZ"); });
        } else {
          return passing_task_gate->AsyncTask().Then(
              []() -> Result<TestInt> { return IterationEnd<TestInt>(); });
        }
      } else {
        return SleepABitAsync().Then([] { return TestInt(0); });
      }
    };
  };
  auto outer = MakeVectorGenerator<AsyncGenerator<TestInt>>(
      {make_source(false), make_source(true), make_source(false)});
  auto merged_gen = MakeMergedGenerator(outer, 10);

  constexpr int NUM_FUTURES = 20;
  std::vector<Future<TestInt>> futures;
  for (int i = 0; i < NUM_FUTURES; i++) {
    futures.push_back(merged_gen());
  }

  auto count_completed_futures = [&] {
    int count = 0;
    for (const auto& future : futures) {
      if (future.is_finished()) {
        count++;
      }
    }
    return count;
  };

  // The first future from each source can be emitted.  The second from
  // each source should be blocked by the gates.
  ASSERT_OK(passing_task_gate->WaitForRunning(2));
  ASSERT_OK(failing_task_gate->WaitForRunning(1));
  ASSERT_EQ(count_completed_futures(), 3);
  // We will unlock the error now but it should not be emitted because
  // the other futures are blocked
  // std::cout << "Unlocking failing gate\n";
  ASSERT_OK(failing_task_gate->Unlock());
  SleepABit();
  ASSERT_EQ(count_completed_futures(), 3);
  // Now we will unlock the in-progress futures and everything should complete
  // We don't know exactly what order things will emit in but after the failure
  // we should only see terminal items
  // std::cout << "Unlocking passing gate\n";
  ASSERT_OK(passing_task_gate->Unlock());

  bool error_seen = false;
  for (const auto& fut : futures) {
    ASSERT_TRUE(fut.Wait(arrow::kDefaultAssertFinishesWaitSeconds));
    if (fut.status().ok()) {
      if (error_seen) {
        ASSERT_TRUE(IsIterationEnd(*fut.result()));
      }
    } else {
      // We should only see one error
      ASSERT_FALSE(error_seen);
      error_seen = true;
      ASSERT_TRUE(fut.status().IsInvalid());
    }
  }
}

TEST_P(MergedGeneratorTestFixture, FinishesQuickly) {
  // Testing a source that finishes on the first pull
  auto source = util::AsyncVectorIt<AsyncGenerator<TestInt>>({MakeSource({1})});
  auto merged = MakeMergedGenerator(std::move(source), 10);
  ASSERT_FINISHES_OK_AND_EQ(TestInt(1), merged());
  AssertGeneratorExhausted(merged);
}

TEST_P(MergedGeneratorTestFixture, MergedOuterFail) {
  auto gen = util::FailAt(
      util::AsyncVectorIt<AsyncGenerator<TestInt>>(
          {MakeSource({1, 2, 3}), MakeSource({1, 2, 3}), MakeSource({1, 2, 3})}),
      1);
  auto merged_gen = MakeMergedGenerator(gen, 10);
  ASSERT_FINISHES_AND_RAISES(Invalid, CollectAsyncGenerator(merged_gen));
}

TEST_P(MergedGeneratorTestFixture, MergedLimitedSubscriptions) {
  auto gen = util::AsyncVectorIt<AsyncGenerator<TestInt>>(
      {MakeSource({1, 2}), MakeSource({3, 4}), MakeSource({5, 6, 7, 8}),
       MakeSource({9, 10, 11, 12})});
  util::TrackingGenerator<AsyncGenerator<TestInt>> tracker(std::move(gen));
  auto merged = MakeMergedGenerator(AsyncGenerator<AsyncGenerator<TestInt>>(tracker), 2);

  SleepABit();
  // Lazy pull, should not start until first pull
  ASSERT_EQ(0, tracker.num_read());

  ASSERT_FINISHES_OK_AND_ASSIGN(auto next, merged());
  ASSERT_TRUE(next.value == 1 || next.value == 3);

  // First 2 values have to come from one of the first 2 sources
  ASSERT_EQ(2, tracker.num_read());
  ASSERT_FINISHES_OK_AND_ASSIGN(next, merged());
  ASSERT_LT(next.value, 5);
  ASSERT_GT(next.value, 0);

  // By the time five values have been read we should have exhausted at
  // least one source
  for (int i = 0; i < 3; i++) {
    ASSERT_FINISHES_OK_AND_ASSIGN(next, merged());
    // 9 is possible if we read 1,2,3,4 and then grab 9 while 5 is running slow
    ASSERT_LT(next.value, 10);
    ASSERT_GT(next.value, 0);
  }
  ASSERT_GT(tracker.num_read(), 2);
  ASSERT_LT(tracker.num_read(), 5);

  // Read remaining values
  for (int i = 0; i < 7; i++) {
    ASSERT_FINISHES_OK_AND_ASSIGN(next, merged());
    ASSERT_LT(next.value, 13);
    ASSERT_GT(next.value, 0);
  }

  AssertGeneratorExhausted(merged);
}

#ifndef ARROW_VALGRIND
TEST_P(MergedGeneratorTestFixture, MergedStress) {
  constexpr int NGENERATORS = 10;
  constexpr int NITEMS = 10;
  for (int i = 0; i < GetNumItersForStress(); i++) {
    std::vector<AsyncGenerator<TestInt>> sources;
    std::vector<ReentrantCheckerGuard<TestInt>> guards;
    for (int j = 0; j < NGENERATORS; j++) {
      auto source = MakeSource(RangeVector(NITEMS));
      guards.push_back(ExpectNotAccessedReentrantly(&source));
      sources.push_back(source);
    }
    AsyncGenerator<AsyncGenerator<TestInt>> source_gen = util::AsyncVectorIt(sources);
    auto outer_gaurd = ExpectNotAccessedReentrantly(&source_gen);

    auto merged = MakeMergedGenerator(source_gen, 4);
    ASSERT_FINISHES_OK_AND_ASSIGN(auto items, CollectAsyncGenerator(merged));
    ASSERT_EQ(NITEMS * NGENERATORS, items.size());
  }
}

TEST_P(MergedGeneratorTestFixture, MergedParallelStress) {
  constexpr int NGENERATORS = 10;
  constexpr int NITEMS = 10;
  for (int i = 0; i < GetNumItersForStress(); i++) {
    std::vector<AsyncGenerator<TestInt>> sources;
    for (int j = 0; j < NGENERATORS; j++) {
      sources.push_back(MakeSource(RangeVector(NITEMS)));
    }
    auto merged = MakeMergedGenerator(util::AsyncVectorIt(sources), 4);
    merged = MakeReadaheadGenerator(merged, 4);
    ASSERT_FINISHES_OK_AND_ASSIGN(auto items, CollectAsyncGenerator(merged));
    ASSERT_EQ(NITEMS * NGENERATORS, items.size());
  }
}
#endif

TEST_P(MergedGeneratorTestFixture, MergedRecursion) {
  // Regression test for an edge case in MergedGenerator. Ensure if
  // the source generator returns already-completed futures and there
  // are many queued pulls (or, the consumer pulls again as part of
  // the callback), we don't recurse due to AddCallback (leading to an
  // eventual stack overflow).
  const int kNumItems = IsSlow() ? 128 : 4096;
  std::vector<TestInt> items(kNumItems, TestInt(42));
  auto generator = MakeSource(items);
  PushGenerator<AsyncGenerator<TestInt>> sources;
  auto merged = MakeMergedGenerator(AsyncGenerator<AsyncGenerator<TestInt>>(sources), 1);
  std::vector<Future<TestInt>> pulls;
  for (int i = 0; i < kNumItems; i++) {
    pulls.push_back(merged());
  }
  sources.producer().Push(generator);
  sources.producer().Close();
  for (const auto& fut : pulls) {
    ASSERT_FINISHES_OK_AND_EQ(TestInt(42), fut);
  }
}

TEST_P(MergedGeneratorTestFixture, DeepOuterGeneratorStackOverflow) {
  // Simulate a very deep and very quick outer generator that yields simple
  // inner generators.  Everything completes synchronously.  This is to
  // try and provoke a stack overflow the simulates ARROW-16692
  constexpr int kNumItems = 10000;
  constexpr int kMaxSubscriptions = 8;
  std::vector<AsyncGenerator<TestInt>> inner_generators;
  for (int i = 0; i < kNumItems; i++) {
    inner_generators.push_back(MakeVectorGenerator<TestInt>({}));
  }
  AsyncGenerator<AsyncGenerator<TestInt>> outer_generator =
      MakeVectorGenerator(inner_generators);
  AsyncGenerator<TestInt> merged =
      MakeMergedGenerator(outer_generator, kMaxSubscriptions);
  ASSERT_FINISHES_OK_AND_ASSIGN(std::vector<TestInt> collected,
                                CollectAsyncGenerator(std::move(merged)));
  ASSERT_TRUE(collected.empty());
}

INSTANTIATE_TEST_SUITE_P(MergedGeneratorTests, MergedGeneratorTestFixture,
                         ::testing::Values(false, true));

class AutoStartingGeneratorTestFixture : public GeneratorTestFixture {};

TEST_P(AutoStartingGeneratorTestFixture, Basic) {
  AsyncGenerator<TestInt> source = MakeSource({1, 2, 3});
  util::TrackingGenerator<TestInt> tracked(source);
  AsyncGenerator<TestInt> gen =
      MakeAutoStartingGenerator(static_cast<AsyncGenerator<TestInt>>(tracked));
  ASSERT_EQ(1, tracked.num_read());
  ASSERT_FINISHES_OK_AND_EQ(TestInt(1), gen());
  ASSERT_EQ(1, tracked.num_read());
  ASSERT_FINISHES_OK_AND_EQ(TestInt(2), gen());
  ASSERT_EQ(2, tracked.num_read());
  ASSERT_FINISHES_OK_AND_EQ(TestInt(3), gen());
  ASSERT_EQ(3, tracked.num_read());
  AssertGeneratorExhausted(gen);
}

TEST_P(AutoStartingGeneratorTestFixture, CopySafe) {
  AsyncGenerator<TestInt> source = MakeSource({1, 2, 3});
  AsyncGenerator<TestInt> gen = MakeAutoStartingGenerator(std::move(source));
  AsyncGenerator<TestInt> copy = gen;
  ASSERT_FINISHES_OK_AND_EQ(TestInt(1), gen());
  ASSERT_FINISHES_OK_AND_EQ(TestInt(2), copy());
  ASSERT_FINISHES_OK_AND_EQ(TestInt(3), gen());
  AssertGeneratorExhausted(gen);
  AssertGeneratorExhausted(copy);
}

INSTANTIATE_TEST_SUITE_P(AutoStartingGeneratorTests, AutoStartingGeneratorTestFixture,
                         ::testing::Values(false, true));

class SeqMergedGeneratorTestFixture : public ::testing::Test {
 protected:
  SeqMergedGeneratorTestFixture() : tracked_source_(push_source_) {}

  void BeginCaptureOutput(AsyncGenerator<TestInt> gen) {
    finished_ = VisitAsyncGenerator(std::move(gen), [this](TestInt val) {
      sink_.push_back(val.value);
      return Status::OK();
    });
  }

  void EmitItem(int sub_index, int value) {
    EXPECT_LT(sub_index, push_subs_.size());
    push_subs_[sub_index].producer().Push(value);
  }

  void EmitErrorItem(int sub_index) {
    EXPECT_LT(sub_index, push_subs_.size());
    push_subs_[sub_index].producer().Push(Status::Invalid("XYZ"));
  }

  void EmitSub() {
    PushGenerator<TestInt> sub;
    util::TrackingGenerator<TestInt> tracked_sub(sub);
    tracked_subs_.push_back(tracked_sub);
    push_subs_.push_back(std::move(sub));
    push_source_.producer().Push(std::move(tracked_sub));
  }

  void EmitErrorSub() { push_source_.producer().Push(Status::Invalid("XYZ")); }

  void FinishSub(int sub_index) {
    EXPECT_LT(sub_index, tracked_subs_.size());
    push_subs_[sub_index].producer().Close();
  }

  void FinishSubs() { push_source_.producer().Close(); }

  void AssertFinishedOk() { ASSERT_FINISHES_OK(finished_); }

  void AssertFailed() { ASSERT_FINISHES_AND_RAISES(Invalid, finished_); }

  int NumItemsAskedFor(int sub_index) {
    EXPECT_LT(sub_index, tracked_subs_.size());
    return tracked_subs_[sub_index].num_read();
  }

  int NumSubsAskedFor() { return tracked_source_.num_read(); }

  void AssertRead(std::vector<int> values) {
    ASSERT_EQ(values.size(), sink_.size());
    for (std::size_t i = 0; i < sink_.size(); i++) {
      ASSERT_EQ(values[i], sink_[i]);
    }
  }

  PushGenerator<AsyncGenerator<TestInt>> push_source_;
  std::vector<PushGenerator<TestInt>> push_subs_;
  std::vector<util::TrackingGenerator<TestInt>> tracked_subs_;
  util::TrackingGenerator<AsyncGenerator<TestInt>> tracked_source_;
  Future<> finished_;
  std::vector<int> sink_;
};

TEST_F(SeqMergedGeneratorTestFixture, Basic) {
  ASSERT_OK_AND_ASSIGN(
      AsyncGenerator<TestInt> gen,
      MakeSequencedMergedGenerator(
          static_cast<AsyncGenerator<AsyncGenerator<TestInt>>>(tracked_source_), 4));
  // Should not initially ask for anything
  ASSERT_EQ(0, NumSubsAskedFor());
  BeginCaptureOutput(gen);
  // Should not read ahead async-reentrantly from source
  ASSERT_EQ(1, NumSubsAskedFor());
  EmitSub();
  ASSERT_EQ(2, NumSubsAskedFor());
  // Should immediately start polling
  ASSERT_EQ(1, NumItemsAskedFor(0));
  EmitSub();
  EmitSub();
  EmitSub();
  EmitSub();
  // Should limit how many subs it reads ahead
  ASSERT_EQ(4, NumSubsAskedFor());
  // Should immediately start polling subs even if they aren't yet active
  ASSERT_EQ(1, NumItemsAskedFor(1));
  ASSERT_EQ(1, NumItemsAskedFor(2));
  ASSERT_EQ(1, NumItemsAskedFor(3));
  // Items emitted on non-active subs should not be delivered and should not trigger
  // further polling on the inactive sub
  EmitItem(1, 0);
  ASSERT_EQ(1, NumItemsAskedFor(1));
  AssertRead({});
  EmitItem(0, 1);
  AssertRead({1});
  ASSERT_EQ(2, NumItemsAskedFor(0));
  EmitItem(0, 2);
  AssertRead({1, 2});
  ASSERT_EQ(3, NumItemsAskedFor(0));
  // On finish it should move to the next sub and pull 1 item
  FinishSub(0);
  ASSERT_EQ(5, NumSubsAskedFor());
  ASSERT_EQ(2, NumItemsAskedFor(1));
  AssertRead({1, 2, 0});
  // Now finish all the subs and make sure an empty sub is ok
  FinishSub(1);
  FinishSub(2);
  FinishSub(3);
  FinishSub(4);
  ASSERT_EQ(6, NumSubsAskedFor());
  FinishSubs();
  AssertFinishedOk();
}

TEST_F(SeqMergedGeneratorTestFixture, ErrorItem) {
  ASSERT_OK_AND_ASSIGN(
      AsyncGenerator<TestInt> gen,
      MakeSequencedMergedGenerator(
          static_cast<AsyncGenerator<AsyncGenerator<TestInt>>>(tracked_source_), 4));
  BeginCaptureOutput(gen);
  EmitSub();
  EmitSub();
  EmitErrorItem(1);
  // It will still read from the active sub and won't notice the error until it switches
  // to the failing sub
  EmitItem(0, 0);
  AssertRead({0});
  FinishSub(0);
  AssertFailed();
  FinishSub(1);
  FinishSubs();
}

TEST_F(SeqMergedGeneratorTestFixture, ErrorSub) {
  ASSERT_OK_AND_ASSIGN(
      AsyncGenerator<TestInt> gen,
      MakeSequencedMergedGenerator(
          static_cast<AsyncGenerator<AsyncGenerator<TestInt>>>(tracked_source_), 4));
  BeginCaptureOutput(gen);
  EmitSub();
  EmitErrorSub();
  FinishSub(0);
  AssertFailed();
}

TEST(TestAsyncUtil, FromVector) {
  AsyncGenerator<TestInt> gen;
  {
    std::vector<TestInt> input = {1, 2, 3};
    gen = MakeVectorGenerator(std::move(input));
  }
  std::vector<TestInt> expected = {1, 2, 3};
  AssertAsyncGeneratorMatch(expected, gen);
}

TEST(TestAsyncUtil, SynchronousFinish) {
  AsyncGenerator<TestInt> generator = []() {
    return Future<TestInt>::MakeFinished(IterationTraits<TestInt>::End());
  };
  Transformer<TestInt, TestStr> skip_all = [](TestInt value) { return TransformSkip(); };
  auto transformed = MakeTransformedGenerator(generator, skip_all);
  auto future = CollectAsyncGenerator(transformed);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto actual, future);
  ASSERT_EQ(std::vector<TestStr>(), actual);
}

TEST(TestAsyncUtil, GeneratorIterator) {
  auto generator = BackgroundAsyncVectorIt({1, 2, 3});
  auto iterator = MakeGeneratorIterator(std::move(generator));
  ASSERT_OK_AND_EQ(TestInt(1), iterator.Next());
  ASSERT_OK_AND_EQ(TestInt(2), iterator.Next());
  ASSERT_OK_AND_EQ(TestInt(3), iterator.Next());
  AssertIteratorExhausted(iterator);
  AssertIteratorExhausted(iterator);
}

TEST(TestAsyncUtil, MakeTransferredGenerator) {
  std::mutex mutex;
  std::condition_variable cv;
  std::atomic<bool> finished(false);

  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));

  // Needs to be a slow source to ensure we don't call Then on a completed
  AsyncGenerator<TestInt> slow_generator = [&]() {
    return thread_pool
        ->Submit([&] {
          std::unique_lock<std::mutex> lock(mutex);
          cv.wait_for(lock, std::chrono::duration<double>(30),
                      [&] { return finished.load(); });
          return IterationTraits<TestInt>::End();
        })
        .ValueOrDie();
  };

  auto transferred =
      MakeTransferredGenerator<TestInt>(std::move(slow_generator), thread_pool.get());

  auto current_thread_id = std::this_thread::get_id();
  auto fut = transferred().Then([&current_thread_id](const TestInt&) {
    ASSERT_NE(current_thread_id, std::this_thread::get_id());
  });

  {
    std::lock_guard<std::mutex> lg(mutex);
    finished.store(true);
  }
  cv.notify_one();
  ASSERT_FINISHES_OK(fut);
}

// This test is too slow for valgrind
#if !(defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER))

TEST(TestAsyncUtil, StackOverflow) {
  int counter = 0;
  AsyncGenerator<TestInt> generator = [&counter]() {
    if (counter < 10000) {
      return Future<TestInt>::MakeFinished(counter++);
    } else {
      return Future<TestInt>::MakeFinished(IterationTraits<TestInt>::End());
    }
  };
  Transformer<TestInt, TestStr> discard =
      [](TestInt next) -> Result<TransformFlow<TestStr>> { return TransformSkip(); };
  auto transformed = MakeTransformedGenerator(generator, discard);
  auto collected_future = CollectAsyncGenerator(transformed);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto collected, collected_future);
  ASSERT_EQ(0, collected.size());
}

#endif

class BackgroundGeneratorTestFixture : public GeneratorTestFixture {
 protected:
  AsyncGenerator<TestInt> Make(const std::vector<TestInt>& it,
                               int max_q = kDefaultBackgroundMaxQ,
                               int q_restart = kDefaultBackgroundQRestart) {
    bool slow = GetParam();
    return BackgroundAsyncVectorIt(it, slow, max_q, q_restart);
  }
};

TEST_P(BackgroundGeneratorTestFixture, Empty) {
  auto background = Make({});
  AssertGeneratorExhausted(background);
}

TEST_P(BackgroundGeneratorTestFixture, Basic) {
  std::vector<TestInt> expected = {1, 2, 3};
  auto background = Make(expected);
  auto future = CollectAsyncGenerator(background);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto collected, future);
  ASSERT_EQ(expected, collected);
}

TEST_P(BackgroundGeneratorTestFixture, BadResult) {
  std::shared_ptr<ManualIteratorControl<TestInt>> iterator_control;
  auto iterator = MakePushIterator<TestInt>(&iterator_control);
  // Enough valid items to fill the queue and then some
  for (int i = 0; i < 5; i++) {
    iterator_control->Push(i);
  }
  // Next fail
  iterator_control->Push(Status::Invalid("XYZ"));
  ASSERT_OK_AND_ASSIGN(
      auto generator,
      MakeBackgroundGenerator(std::move(iterator), internal::GetCpuThreadPool(), 4, 2));

  ASSERT_FINISHES_OK_AND_EQ(TestInt(0), generator());
  // Have not yet restarted so next results should always be valid
  ASSERT_FINISHES_OK_AND_EQ(TestInt(1), generator());
  // Next three results may or may not be valid.
  // The typical case is the call for TestInt(2) restarts a full queue and then maybe
  // TestInt(3) and TestInt(4) arrive quickly enough to not get pre-empted or maybe
  // they don't.
  //
  // A more bizarre, but possible, case is the checking thread falls behind the producer
  // thread just so and TestInt(1) arrives and is delivered but before the call for
  // TestInt(2) happens the background reader reads 2, 3, 4, and 5[err] into the queue so
  // the queue never fills up and even TestInt(2) is preempted.
  bool invalid_encountered = false;
  for (int i = 0; i < 3; i++) {
    auto next_fut = generator();
    auto next_result = next_fut.result();
    if (next_result.ok()) {
      ASSERT_EQ(TestInt(i + 2), next_result.ValueUnsafe());
    } else {
      invalid_encountered = true;
      break;
    }
  }
  // If both of the next two results are valid then this one will surely be invalid
  if (!invalid_encountered) {
    ASSERT_FINISHES_AND_RAISES(Invalid, generator());
  }
  AssertGeneratorExhausted(generator);
}

TEST_P(BackgroundGeneratorTestFixture, InvalidExecutor) {
  std::vector<TestInt> expected = {1, 2, 3, 4, 5, 6, 7, 8};
  // Case 1: waiting future
  auto slow = GetParam();
  auto it = PossiblySlowVectorIt(expected, slow);
  ASSERT_OK_AND_ASSIGN(auto invalid_executor, internal::ThreadPool::Make(1));
  ASSERT_OK(invalid_executor->Shutdown());
  ASSERT_OK_AND_ASSIGN(auto background, MakeBackgroundGenerator(
                                            std::move(it), invalid_executor.get(), 4, 2));
  ASSERT_FINISHES_AND_RAISES(Invalid, background());

  // Case 2: Queue bad result
  it = PossiblySlowVectorIt(expected, slow);
  ASSERT_OK_AND_ASSIGN(invalid_executor, internal::ThreadPool::Make(1));
  ASSERT_OK_AND_ASSIGN(
      background, MakeBackgroundGenerator(std::move(it), invalid_executor.get(), 4, 2));
  ASSERT_FINISHES_OK_AND_EQ(TestInt(1), background());
  ASSERT_OK(invalid_executor->Shutdown());
  // Next two are ok because queue is shutdown
  ASSERT_FINISHES_OK_AND_EQ(TestInt(2), background());
  ASSERT_FINISHES_OK_AND_EQ(TestInt(3), background());
  // Now the queue should have tried (and failed) to start back up
  ASSERT_FINISHES_AND_RAISES(Invalid, background());
}

TEST_P(BackgroundGeneratorTestFixture, StopAndRestart) {
  std::shared_ptr<ManualIteratorControl<TestInt>> iterator_control;
  auto iterator = MakePushIterator<TestInt>(&iterator_control);
  // Start with 6 items in the source
  for (int i = 0; i < 6; i++) {
    iterator_control->Push(i);
  }
  iterator_control->Push(IterationEnd<TestInt>());

  ASSERT_OK_AND_ASSIGN(
      auto generator,
      MakeBackgroundGenerator(std::move(iterator), internal::GetCpuThreadPool(), 4, 2));
  SleepABit();
  // Lazy, should not start until polled once
  ASSERT_EQ(iterator_control->times_polled(), 0);
  // First poll should trigger 5 reads (1 for the polled value, 4 for the queue)
  auto next = generator();
  BusyWait(10, [&] { return iterator_control->times_polled() >= 5; });
  // And then stop and not read any more
  SleepABit();
  ASSERT_EQ(iterator_control->times_polled(), 5);

  ASSERT_FINISHES_OK_AND_EQ(TestInt(0), next);
  // One more read should bring q down to 3 and should not restart
  ASSERT_FINISHES_OK_AND_EQ(TestInt(1), generator());
  SleepABit();
  ASSERT_EQ(iterator_control->times_polled(), 5);

  // One more read should bring q down to 2 and that should restart
  // but it will only read up to 6 because we hit end of stream
  ASSERT_FINISHES_OK_AND_EQ(TestInt(2), generator());
  BusyWait(10, [&] { return iterator_control->times_polled() >= 7; });
  ASSERT_EQ(iterator_control->times_polled(), 7);

  for (int i = 3; i < 6; i++) {
    ASSERT_FINISHES_OK_AND_EQ(TestInt(i), generator());
  }

  AssertGeneratorExhausted(generator);
}

struct TrackingIterator {
  explicit TrackingIterator(bool slow)
      : token(std::make_shared<bool>(false)), slow(slow) {}

  Result<TestInt> Next() {
    if (slow) {
      SleepABit();
    }
    return TestInt(0);
  }
  std::weak_ptr<bool> GetWeakTargetRef() { return std::weak_ptr<bool>(token); }

  std::shared_ptr<bool> token;
  bool slow;
};

TEST_P(BackgroundGeneratorTestFixture, AbortReading) {
  // If there is an error downstream then it is likely the chain will abort and the
  // background generator will lose all references and should abandon reading
  TrackingIterator source(IsSlow());
  auto tracker = source.GetWeakTargetRef();
  auto iter = Iterator<TestInt>(std::move(source));
  std::shared_ptr<AsyncGenerator<TestInt>> generator;
  {
    ASSERT_OK_AND_ASSIGN(
        auto gen, MakeBackgroundGenerator(std::move(iter), internal::GetCpuThreadPool()));
    generator = std::make_shared<AsyncGenerator<TestInt>>(gen);
  }

  // Poll one item to start it up
  ASSERT_FINISHES_OK_AND_EQ(TestInt(0), (*generator)());
  ASSERT_FALSE(tracker.expired());
  // Remove last reference to generator, should trigger and wait for cleanup
  generator.reset();
  // Cleanup should have ensured no more reference to the source.  It may take a moment
  // to expire because the background thread has to destruct itself
  BusyWait(10, [&tracker] { return tracker.expired(); });
}

TEST_P(BackgroundGeneratorTestFixture, AbortOnIdleBackground) {
  // Tests what happens when the downstream aborts while the background thread is idle
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));

  auto source = PossiblySlowVectorIt(RangeVector(100), IsSlow());
  std::shared_ptr<AsyncGenerator<TestInt>> generator;
  {
    ASSERT_OK_AND_ASSIGN(auto gen,
                         MakeBackgroundGenerator(std::move(source), thread_pool.get()));
    generator = std::make_shared<AsyncGenerator<TestInt>>(gen);
  }
  ASSERT_FINISHES_OK_AND_EQ(TestInt(0), (*generator)());

  // The generator should pretty quickly fill up the queue and idle
  BusyWait(10, [&thread_pool] { return thread_pool->GetNumTasks() == 0; });

  // Now delete the generator and hope we don't deadlock
  generator.reset();
}

struct SlowEmptyIterator {
  Result<TestInt> Next() {
    if (called_) {
      return Status::Invalid("Should not have been called twice");
    }
    SleepFor(0.1);
    return IterationTraits<TestInt>::End();
  }

 private:
  bool called_ = false;
};

TEST_P(BackgroundGeneratorTestFixture, BackgroundRepeatEnd) {
  // Ensure that the background generator properly fulfills the asyncgenerator contract
  // and can be called after it ends.
  ASSERT_OK_AND_ASSIGN(auto io_pool, internal::ThreadPool::Make(1));

  bool slow = GetParam();
  Iterator<TestInt> iterator;
  if (slow) {
    iterator = Iterator<TestInt>(SlowEmptyIterator());
  } else {
    iterator = MakeEmptyIterator<TestInt>();
  }
  ASSERT_OK_AND_ASSIGN(auto background_gen,
                       MakeBackgroundGenerator(std::move(iterator), io_pool.get()));

  background_gen =
      MakeTransferredGenerator(std::move(background_gen), internal::GetCpuThreadPool());

  auto one = background_gen();
  ASSERT_FINISHES_OK_AND_ASSIGN(auto one_fin, one);
  ASSERT_TRUE(IsIterationEnd(one_fin));

  auto two = background_gen();
  ASSERT_FINISHES_OK_AND_ASSIGN(auto two_fin, two);
  ASSERT_TRUE(IsIterationEnd(two_fin));
}

TEST_P(BackgroundGeneratorTestFixture, Stress) {
  constexpr int NTASKS = 20;
  constexpr int NITEMS = 20;
  auto expected = RangeVector(NITEMS);
  std::vector<Future<std::vector<TestInt>>> futures;
  for (unsigned int i = 0; i < NTASKS; i++) {
    auto background = Make(expected, /*max_q=*/4, /*q_restart=*/2);
    futures.push_back(CollectAsyncGenerator(background));
  }
  auto combined = All(futures);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto completed_vectors, combined);
  for (std::size_t i = 0; i < completed_vectors.size(); i++) {
    ASSERT_OK_AND_ASSIGN(auto vector, completed_vectors[i]);
    ASSERT_EQ(vector, expected);
  }
}

INSTANTIATE_TEST_SUITE_P(BackgroundGeneratorTests, BackgroundGeneratorTestFixture,
                         ::testing::Values(false, true));

TEST(TestAsyncUtil, SerialReadaheadSlowProducer) {
  AsyncGenerator<TestInt> gen = BackgroundAsyncVectorIt({1, 2, 3, 4, 5});
  auto guard = ExpectNotAccessedReentrantly(&gen);
  SerialReadaheadGenerator<TestInt> serial_readahead(gen, 2);
  AssertAsyncGeneratorMatch({1, 2, 3, 4, 5},
                            static_cast<AsyncGenerator<TestInt>>(serial_readahead));
}

TEST(TestAsyncUtil, SerialReadaheadSlowConsumer) {
  int num_delivered = 0;
  auto source = [&num_delivered]() {
    if (num_delivered < 5) {
      return Future<TestInt>::MakeFinished(num_delivered++);
    } else {
      return Future<TestInt>::MakeFinished(IterationTraits<TestInt>::End());
    }
  };
  AsyncGenerator<TestInt> serial_readahead = SerialReadaheadGenerator<TestInt>(source, 3);
  SleepABit();
  ASSERT_EQ(0, num_delivered);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto next, serial_readahead());
  ASSERT_EQ(0, next.value);
  ASSERT_EQ(4, num_delivered);
  AssertAsyncGeneratorMatch({1, 2, 3, 4}, serial_readahead);

  // Ensure still reads ahead with just 1 slot
  num_delivered = 0;
  serial_readahead = SerialReadaheadGenerator<TestInt>(source, 1);
  ASSERT_FINISHES_OK_AND_ASSIGN(next, serial_readahead());
  ASSERT_EQ(0, next.value);
  ASSERT_EQ(2, num_delivered);
  AssertAsyncGeneratorMatch({1, 2, 3, 4}, serial_readahead);
}

TEST(TestAsyncUtil, SerialReadaheadStress) {
  constexpr int NTASKS = 20;
  constexpr int NITEMS = 50;
  for (int i = 0; i < NTASKS; i++) {
    AsyncGenerator<TestInt> gen = BackgroundAsyncVectorIt(RangeVector(NITEMS));
    auto guard = ExpectNotAccessedReentrantly(&gen);
    SerialReadaheadGenerator<TestInt> serial_readahead(gen, 2);
    auto visit_fut =
        VisitAsyncGenerator<TestInt>(serial_readahead, [](TestInt test_int) -> Status {
          // Normally sleeping in a visit function would be a faux-pas but we want to slow
          // the reader down to match the producer to maximize the stress
          SleepABit();
          return Status::OK();
        });
    ASSERT_FINISHES_OK(visit_fut);
  }
}

TEST(TestAsyncUtil, SerialReadaheadStressFast) {
  constexpr int NTASKS = 20;
  constexpr int NITEMS = 50;
  for (int i = 0; i < NTASKS; i++) {
    AsyncGenerator<TestInt> gen = BackgroundAsyncVectorIt(RangeVector(NITEMS), false);
    auto guard = ExpectNotAccessedReentrantly(&gen);
    SerialReadaheadGenerator<TestInt> serial_readahead(gen, 2);
    auto visit_fut = VisitAsyncGenerator<TestInt>(
        serial_readahead, [](TestInt test_int) -> Status { return Status::OK(); });
    ASSERT_FINISHES_OK(visit_fut);
  }
}

TEST(TestAsyncUtil, SerialReadaheadStressFailing) {
  constexpr int NTASKS = 20;
  constexpr int NITEMS = 50;
  constexpr int EXPECTED_SUM = 45;
  for (int i = 0; i < NTASKS; i++) {
    AsyncGenerator<TestInt> it = BackgroundAsyncVectorIt(RangeVector(NITEMS));
    AsyncGenerator<TestInt> fails_at_ten = [&it]() {
      auto next = it();
      return next.Then([](const TestInt& item) -> Result<TestInt> {
        if (item.value >= 10) {
          return Status::Invalid("XYZ");
        } else {
          return item;
        }
      });
    };
    SerialReadaheadGenerator<TestInt> serial_readahead(fails_at_ten, 2);
    unsigned int sum = 0;
    auto visit_fut = VisitAsyncGenerator<TestInt>(serial_readahead,
                                                  [&sum](TestInt test_int) -> Status {
                                                    sum += test_int.value;
                                                    // Sleep to maximize stress
                                                    SleepABit();
                                                    return Status::OK();
                                                  });
    ASSERT_FINISHES_AND_RAISES(Invalid, visit_fut);
    ASSERT_EQ(EXPECTED_SUM, sum);
  }
}

TEST(TestAsyncUtil, Readahead) {
  int num_delivered = 0;
  auto source = [&num_delivered]() {
    if (num_delivered < 5) {
      return Future<TestInt>::MakeFinished(num_delivered++);
    } else {
      return Future<TestInt>::MakeFinished(IterationTraits<TestInt>::End());
    }
  };
  auto readahead = MakeReadaheadGenerator<TestInt>(source, 10);
  // Should not pump until first item requested
  ASSERT_EQ(0, num_delivered);

  auto first = readahead();
  // At this point the pumping should have happened
  ASSERT_EQ(5, num_delivered);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto first_val, first);
  ASSERT_EQ(TestInt(0), first_val);

  // Read the rest
  for (int i = 0; i < 4; i++) {
    auto next = readahead();
    ASSERT_FINISHES_OK_AND_ASSIGN(auto next_val, next);
    ASSERT_EQ(TestInt(i + 1), next_val);
  }

  // Next should be end
  auto last = readahead();
  ASSERT_FINISHES_OK_AND_ASSIGN(auto last_val, last);
  ASSERT_TRUE(IsIterationEnd(last_val));
}

TEST(TestAsyncUtil, ReadaheadOneItem) {
  bool delivered = false;
  auto source = [&delivered]() {
    if (!delivered) {
      delivered = true;
      return Future<TestInt>::MakeFinished(0);
    } else {
      return Future<TestInt>::MakeFinished(IterationTraits<TestInt>::End());
    }
  };
  auto readahead = MakeReadaheadGenerator<TestInt>(source, 10);
  auto collected = CollectAsyncGenerator(std::move(readahead));
  ASSERT_FINISHES_OK_AND_ASSIGN(auto actual, collected);
  ASSERT_EQ(1, actual.size());
  ASSERT_EQ(TestInt(0), actual[0]);
}

TEST(TestAsyncUtil, ReadaheadCopy) {
  auto source = util::AsyncVectorIt<TestInt>(RangeVector(6));
  auto gen = MakeReadaheadGenerator(std::move(source), 2);

  for (int i = 0; i < 2; i++) {
    ASSERT_FINISHES_OK_AND_EQ(TestInt(i), gen());
  }
  auto gen_copy = gen;
  for (int i = 0; i < 2; i++) {
    ASSERT_FINISHES_OK_AND_EQ(TestInt(i + 2), gen_copy());
  }
  for (int i = 0; i < 2; i++) {
    ASSERT_FINISHES_OK_AND_EQ(TestInt(i + 4), gen());
  }
  AssertGeneratorExhausted(gen);
  AssertGeneratorExhausted(gen_copy);
}

TEST(TestAsyncUtil, ReadaheadMove) {
  auto source = util::AsyncVectorIt<TestInt>(RangeVector(6));
  auto gen = MakeReadaheadGenerator(std::move(source), 2);

  for (int i = 0; i < 2; i++) {
    ASSERT_FINISHES_OK_AND_EQ(TestInt(i), gen());
  }
  auto gen_copy = std::move(gen);
  for (int i = 0; i < 4; i++) {
    ASSERT_FINISHES_OK_AND_EQ(TestInt(i + 2), gen_copy());
  }
  AssertGeneratorExhausted(gen_copy);
}

TEST(TestAsyncUtil, ReadaheadFailed) {
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(20));
  std::atomic<int32_t> counter(0);
  auto gating_task = GatingTask::Make();
  // All tasks are a little slow.  The first task fails.
  // The readahead will have spawned 9 more tasks and they
  // should all pass
  auto source = [&]() -> Future<TestInt> {
    auto count = counter++;
    return DeferNotOk(thread_pool->Submit([&, count]() -> Result<TestInt> {
      gating_task->Task()();
      if (count == 0) {
        return Status::Invalid("X");
      }
      return TestInt(count);
    }));
  };
  auto readahead = MakeReadaheadGenerator<TestInt>(source, 10);
  auto should_be_invalid = readahead();
  // Polling once should allow 10 additional calls to start
  ASSERT_OK(gating_task->WaitForRunning(11));
  ASSERT_OK(gating_task->Unlock());

  // Once unlocked the error task should always be the first.  Some number of successful
  // tasks may follow until the end.
  ASSERT_FINISHES_AND_RAISES(Invalid, should_be_invalid);

  ASSERT_FINISHES_OK_AND_ASSIGN(auto remaining_results, CollectAsyncGenerator(readahead));
  // Don't need to know the exact number of successful tasks (and it may vary)
  for (std::size_t i = 0; i < remaining_results.size(); i++) {
    ASSERT_EQ(TestInt(static_cast<int>(i) + 1), remaining_results[i]);
  }
}

TEST(TestAsyncUtil, ReadaheadFailedWaitForInFlight) {
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(20));
  // If a failure causes an early end then we should not emit that failure
  // until all in-flight futures have completed.  This is to prevent tasks from
  // outliving the generator
  std::atomic<int32_t> counter(0);
  auto failure_gating_task = GatingTask::Make();
  auto in_flight_gating_task = GatingTask::Make();
  auto source = [&]() -> Future<TestInt> {
    auto count = counter++;
    return DeferNotOk(thread_pool->Submit([&, count]() -> Result<TestInt> {
      if (count == 0) {
        failure_gating_task->Task()();
        return Status::Invalid("X");
      }
      in_flight_gating_task->Task()();
      // These are our in-flight tasks
      return TestInt(0);
    }));
  };
  auto readahead = MakeReadaheadGenerator<TestInt>(source, 10);
  auto should_be_invalid = readahead();
  ASSERT_OK(in_flight_gating_task->WaitForRunning(10));
  ASSERT_OK(failure_gating_task->Unlock());
  SleepABit();
  // Can't be finished because in-flight tasks are still running
  AssertNotFinished(should_be_invalid);
  ASSERT_OK(in_flight_gating_task->Unlock());
  ASSERT_FINISHES_AND_RAISES(Invalid, should_be_invalid);
}

TEST(TestAsyncUtil, ReadaheadFailedStress) {
  constexpr int NTASKS = 10;
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(20));
  for (int i = 0; i < NTASKS; i++) {
    std::atomic<int32_t> counter(0);
    std::atomic<bool> finished(false);
    AsyncGenerator<TestInt> source = [&]() -> Future<TestInt> {
      auto count = counter++;
      return DeferNotOk(thread_pool->Submit([&, count]() -> Result<TestInt> {
        SleepABit();
        if (count == 5) {
          return Status::Invalid("X");
        }
        // Generator should not have been finished at this point
        EXPECT_FALSE(finished);
        return TestInt(0);
      }));
    };
    ASSERT_FINISHES_AND_RAISES(Invalid, CollectAsyncGenerator(source));
    finished.store(false);
  }
}

class EnumeratorTestFixture : public GeneratorTestFixture {
 protected:
  void AssertEnumeratedCorrectly(AsyncGenerator<Enumerated<TestInt>>& gen,
                                 int num_items) {
    auto collected = CollectAsyncGenerator(gen);
    ASSERT_FINISHES_OK_AND_ASSIGN(auto items, collected);
    EXPECT_EQ(num_items, items.size());

    for (const auto& item : items) {
      ASSERT_EQ(item.index, item.value.value);
      bool last = item.index == num_items - 1;
      ASSERT_EQ(last, item.last);
    }
    AssertGeneratorExhausted(gen);
  }
};

TEST_P(EnumeratorTestFixture, Basic) {
  constexpr int NITEMS = 100;

  auto source = MakeSource(RangeVector(NITEMS));
  auto enumerated = MakeEnumeratedGenerator(std::move(source));

  AssertEnumeratedCorrectly(enumerated, NITEMS);
}

TEST_P(EnumeratorTestFixture, Empty) {
  auto source = MakeEmptySource();
  auto enumerated = MakeEnumeratedGenerator(std::move(source));
  AssertGeneratorExhausted(enumerated);
}

TEST_P(EnumeratorTestFixture, Error) {
  auto source = util::FailAt(MakeSource({1, 2, 3}), 1);
  auto enumerated = MakeEnumeratedGenerator(std::move(source));

  // Even though the first item finishes ok the enumerator buffers it.  The error then
  // takes priority over the buffered result.
  ASSERT_FINISHES_AND_RAISES(Invalid, enumerated());
}

INSTANTIATE_TEST_SUITE_P(EnumeratedTests, EnumeratorTestFixture,
                         ::testing::Values(false, true));

class SequencerTestFixture : public GeneratorTestFixture {
 protected:
  void RandomShuffle(std::vector<TestInt>& values) {
    std::default_random_engine gen(seed_++);
    std::shuffle(values.begin(), values.end(), gen);
  }

  int seed_ = 42;
  std::function<bool(const TestInt&, const TestInt&)> cmp_ =
      [](const TestInt& left, const TestInt& right) { return left.value > right.value; };
  // Let's increment by 2's to make it interesting
  std::function<bool(const TestInt&, const TestInt&)> is_next_ =
      [](const TestInt& left, const TestInt& right) {
        return left.value + 2 == right.value;
      };
};

TEST_P(SequencerTestFixture, SequenceBasic) {
  // Basic sequencing
  auto original = MakeSource({6, 4, 2});
  auto sequenced = MakeSequencingGenerator(original, cmp_, is_next_, TestInt(0));
  AssertAsyncGeneratorMatch({2, 4, 6}, sequenced);

  // From ordered input
  original = MakeSource({2, 4, 6});
  sequenced = MakeSequencingGenerator(original, cmp_, is_next_, TestInt(0));
  AssertAsyncGeneratorMatch({2, 4, 6}, sequenced);
}

TEST_P(SequencerTestFixture, SequenceLambda) {
  auto cmp = [](const TestInt& left, const TestInt& right) {
    return left.value > right.value;
  };
  auto is_next = [](const TestInt& left, const TestInt& right) {
    return left.value + 2 == right.value;
  };
  // Basic sequencing
  auto original = MakeSource({6, 4, 2});
  auto sequenced = MakeSequencingGenerator(original, cmp, is_next, TestInt(0));
  AssertAsyncGeneratorMatch({2, 4, 6}, sequenced);
}

TEST_P(SequencerTestFixture, SequenceError) {
  {
    auto original = MakeSource({6, 4, 2});
    original = util::FailAt(original, 1);
    auto sequenced = MakeSequencingGenerator(original, cmp_, is_next_, TestInt(0));
    auto collected = CollectAsyncGenerator(sequenced);
    ASSERT_FINISHES_AND_RAISES(Invalid, collected);
  }
  {
    // Failure should clear old items out of the queue immediately
    // shared_ptr versions of cmp_ and is_next_
    auto cmp = cmp_;
    std::function<bool(const std::shared_ptr<TestInt>&, const std::shared_ptr<TestInt>&)>
        ptr_cmp =
            [cmp](const std::shared_ptr<TestInt>& left,
                  const std::shared_ptr<TestInt>& right) { return cmp(*left, *right); };
    auto is_next = is_next_;
    std::function<bool(const std::shared_ptr<TestInt>&, const std::shared_ptr<TestInt>&)>
        ptr_is_next = [is_next](const std::shared_ptr<TestInt>& left,
                                const std::shared_ptr<TestInt>& right) {
          return is_next(*left, *right);
        };

    PushGenerator<std::shared_ptr<TestInt>> source;
    auto sequenced = MakeSequencingGenerator(
        static_cast<AsyncGenerator<std::shared_ptr<TestInt>>>(source), ptr_cmp,
        ptr_is_next, std::make_shared<TestInt>(0));

    auto should_be_cleared = std::make_shared<TestInt>(4);
    std::weak_ptr<TestInt> ref = should_be_cleared;
    auto producer = source.producer();
    auto next_fut = sequenced();
    producer.Push(std::move(should_be_cleared));
    producer.Push(Status::Invalid("XYZ"));
    ASSERT_TRUE(ref.expired());

    ASSERT_FINISHES_AND_RAISES(Invalid, next_fut);
  }
  {
    // Failure should interrupt pumping
    PushGenerator<TestInt> source;
    auto sequenced = MakeSequencingGenerator(static_cast<AsyncGenerator<TestInt>>(source),
                                             cmp_, is_next_, TestInt(0));

    auto producer = source.producer();
    auto next_fut = sequenced();
    producer.Push(TestInt(4));
    producer.Push(Status::Invalid("XYZ"));
    producer.Push(TestInt(2));
    ASSERT_FINISHES_AND_RAISES(Invalid, next_fut);
    // The sequencer should not have pulled the 2 out of the source because it should
    // have stopped pumping on error
    ASSERT_FINISHES_OK_AND_EQ(TestInt(2), source());
  }
}

TEST_P(SequencerTestFixture, Readahead) {
  AsyncGenerator<TestInt> original = MakeSource({4, 2, 0, 6});
  util::TrackingGenerator<TestInt> tracker(original);
  AsyncGenerator<TestInt> sequenced = MakeSequencingGenerator(
      static_cast<AsyncGenerator<TestInt>>(tracker), cmp_, is_next_, TestInt(-2));
  ASSERT_FINISHES_OK_AND_EQ(TestInt(0), sequenced());
  ASSERT_EQ(3, tracker.num_read());
  ASSERT_FINISHES_OK_AND_EQ(TestInt(2), sequenced());
  ASSERT_EQ(3, tracker.num_read());
  ASSERT_FINISHES_OK_AND_EQ(TestInt(4), sequenced());
  ASSERT_EQ(3, tracker.num_read());
  ASSERT_FINISHES_OK_AND_EQ(TestInt(6), sequenced());
  ASSERT_EQ(4, tracker.num_read());
}

TEST_P(SequencerTestFixture, SequenceStress) {
  constexpr int NITEMS = 100;
  for (auto task_index = 0; task_index < GetNumItersForStress(); task_index++) {
    auto input = RangeVector(NITEMS, 2);
    RandomShuffle(input);
    auto original = MakeSource(input);
    auto sequenced = MakeSequencingGenerator(original, cmp_, is_next_, TestInt(-2));
    AssertAsyncGeneratorMatch(RangeVector(NITEMS, 2), sequenced);
  }
}

INSTANTIATE_TEST_SUITE_P(SequencerTests, SequencerTestFixture,
                         ::testing::Values(false, true));

TEST(TestAsyncIteratorTransform, SkipSome) {
  auto original = util::AsyncVectorIt<TestInt>({1, 2, 3});
  auto filter = MakeFilter([](TestInt& t) { return t.value != 2; });
  auto filtered = MakeTransformedGenerator(std::move(original), filter);
  AssertAsyncGeneratorMatch({"1", "3"}, std::move(filtered));
}

TEST(PushGenerator, Empty) {
  PushGenerator<TestInt> gen;
  auto producer = gen.producer();

  auto fut = gen();
  AssertNotFinished(fut);
  ASSERT_FALSE(producer.is_closed());
  ASSERT_TRUE(producer.Close());
  ASSERT_TRUE(producer.is_closed());
  ASSERT_FINISHES_OK_AND_EQ(IterationTraits<TestInt>::End(), fut);
  ASSERT_FINISHES_OK_AND_EQ(IterationTraits<TestInt>::End(), gen());

  // Close idempotent
  fut = gen();
  ASSERT_FALSE(producer.Close());
  ASSERT_TRUE(producer.is_closed());
  ASSERT_FINISHES_OK_AND_EQ(IterationTraits<TestInt>::End(), fut);
  ASSERT_FINISHES_OK_AND_EQ(IterationTraits<TestInt>::End(), gen());
  ASSERT_FINISHES_OK_AND_EQ(IterationTraits<TestInt>::End(), gen());
}

TEST(PushGenerator, Success) {
  PushGenerator<TestInt> gen;
  auto producer = gen.producer();
  std::vector<Future<TestInt>> futures;

  ASSERT_TRUE(producer.Push(TestInt{1}));
  ASSERT_TRUE(producer.Push(TestInt{2}));
  for (int i = 0; i < 3; ++i) {
    futures.push_back(gen());
  }
  ASSERT_FINISHES_OK_AND_EQ(TestInt{1}, futures[0]);
  ASSERT_FINISHES_OK_AND_EQ(TestInt{2}, futures[1]);
  AssertNotFinished(futures[2]);

  ASSERT_TRUE(producer.Push(TestInt{3}));
  ASSERT_FINISHES_OK_AND_EQ(TestInt{3}, futures[2]);
  ASSERT_TRUE(producer.Push(TestInt{4}));
  futures.push_back(gen());
  ASSERT_FINISHES_OK_AND_EQ(TestInt{4}, futures[3]);
  ASSERT_TRUE(producer.Push(TestInt{5}));

  ASSERT_FALSE(producer.is_closed());
  ASSERT_TRUE(producer.Close());
  ASSERT_TRUE(producer.is_closed());
  for (int i = 0; i < 4; ++i) {
    futures.push_back(gen());
  }
  ASSERT_FINISHES_OK_AND_EQ(TestInt{5}, futures[4]);
  for (int i = 5; i < 8; ++i) {
    ASSERT_FINISHES_OK_AND_EQ(IterationTraits<TestInt>::End(), futures[i]);
  }
  ASSERT_FINISHES_OK_AND_EQ(IterationTraits<TestInt>::End(), gen());
}

TEST(PushGenerator, Errors) {
  PushGenerator<TestInt> gen;
  auto producer = gen.producer();
  std::vector<Future<TestInt>> futures;

  ASSERT_TRUE(producer.Push(TestInt{1}));
  ASSERT_TRUE(producer.Push(Status::Invalid("2")));
  for (int i = 0; i < 3; ++i) {
    futures.push_back(gen());
  }
  ASSERT_FINISHES_OK_AND_EQ(TestInt{1}, futures[0]);
  ASSERT_FINISHES_AND_RAISES(Invalid, futures[1]);
  AssertNotFinished(futures[2]);

  ASSERT_TRUE(producer.Push(Status::IOError("3")));
  ASSERT_TRUE(producer.Push(TestInt{4}));
  ASSERT_FINISHES_AND_RAISES(IOError, futures[2]);
  futures.push_back(gen());
  ASSERT_FINISHES_OK_AND_EQ(TestInt{4}, futures[3]);

  ASSERT_FALSE(producer.is_closed());
  ASSERT_TRUE(producer.Close());
  ASSERT_TRUE(producer.is_closed());
  ASSERT_FINISHES_OK_AND_EQ(IterationTraits<TestInt>::End(), gen());
}

TEST(PushGenerator, CloseEarly) {
  PushGenerator<TestInt> gen;
  auto producer = gen.producer();
  std::vector<Future<TestInt>> futures;

  ASSERT_TRUE(producer.Push(TestInt{1}));
  ASSERT_TRUE(producer.Push(TestInt{2}));
  for (int i = 0; i < 3; ++i) {
    futures.push_back(gen());
  }
  ASSERT_FALSE(producer.is_closed());
  ASSERT_TRUE(producer.Close());
  ASSERT_TRUE(producer.is_closed());
  ASSERT_FALSE(producer.Push(TestInt{3}));
  ASSERT_FALSE(producer.Close());
  ASSERT_TRUE(producer.is_closed());

  ASSERT_FINISHES_OK_AND_EQ(TestInt{1}, futures[0]);
  ASSERT_FINISHES_OK_AND_EQ(TestInt{2}, futures[1]);
  ASSERT_FINISHES_OK_AND_EQ(IterationTraits<TestInt>::End(), futures[2]);
  ASSERT_FINISHES_OK_AND_EQ(IterationTraits<TestInt>::End(), gen());
}

TEST(PushGenerator, DanglingProducer) {
  std::optional<PushGenerator<TestInt>> gen;
  gen.emplace();
  auto producer = gen->producer();

  ASSERT_TRUE(producer.Push(TestInt{1}));
  ASSERT_FALSE(producer.is_closed());
  gen.reset();
  ASSERT_TRUE(producer.is_closed());
  ASSERT_FALSE(producer.Push(TestInt{2}));
  ASSERT_FALSE(producer.Close());
}

TEST(PushGenerator, Stress) {
  const int NTHREADS = 20;
  const int NVALUES = 2000;
  const int NFUTURES = NVALUES + 100;

  PushGenerator<TestInt> gen;
  auto producer = gen.producer();

  std::atomic<int> next_value{0};

  auto producer_worker = [&]() {
    while (true) {
      int v = next_value.fetch_add(1);
      if (v >= NVALUES) {
        break;
      }
      producer.Push(v);
    }
  };

  auto producer_main = [&]() {
    std::vector<std::thread> threads;
    for (int i = 0; i < NTHREADS; ++i) {
      threads.emplace_back(producer_worker);
    }
    for (auto& thread : threads) {
      thread.join();
    }
    producer.Close();
  };

  std::vector<Result<TestInt>> results;
  std::thread thread(producer_main);
  for (int i = 0; i < NFUTURES; ++i) {
    results.push_back(gen().result());
  }
  thread.join();

  std::unordered_set<int> seen_values;
  for (int i = 0; i < NVALUES; ++i) {
    ASSERT_OK_AND_ASSIGN(auto v, results[i]);
    ASSERT_EQ(seen_values.count(v.value), 0);
    seen_values.insert(v.value);
  }
  for (int i = NVALUES; i < NFUTURES; ++i) {
    ASSERT_OK_AND_EQ(IterationTraits<TestInt>::End(), results[i]);
  }
}

TEST(SingleFutureGenerator, Basics) {
  auto fut = Future<TestInt>::Make();
  auto gen = MakeSingleFutureGenerator(fut);
  auto collect_fut = CollectAsyncGenerator(gen);
  AssertNotFinished(collect_fut);
  fut.MarkFinished(TestInt{42});
  ASSERT_FINISHES_OK_AND_ASSIGN(auto collected, collect_fut);
  ASSERT_EQ(collected, std::vector<TestInt>{42});
  // Generator exhausted
  collect_fut = CollectAsyncGenerator(gen);
  ASSERT_FINISHES_OK_AND_EQ(std::vector<TestInt>{}, collect_fut);
}

TEST(FailingGenerator, Basics) {
  auto gen = MakeFailingGenerator<TestInt>(Status::IOError("zzz"));
  auto collect_fut = CollectAsyncGenerator(gen);
  ASSERT_FINISHES_AND_RAISES(IOError, collect_fut);
  // Generator exhausted
  collect_fut = CollectAsyncGenerator(gen);
  ASSERT_FINISHES_OK_AND_EQ(std::vector<TestInt>{}, collect_fut);
}

TEST(DefaultIfEmptyGenerator, Basics) {
  std::vector<TestInt> values{1, 2, 3, 4};
  auto gen = MakeVectorGenerator(values);
  ASSERT_FINISHES_OK_AND_ASSIGN(
      auto actual, CollectAsyncGenerator(MakeDefaultIfEmptyGenerator(gen, TestInt(42))));
  EXPECT_EQ(values, actual);

  gen = MakeVectorGenerator<TestInt>({});
  ASSERT_FINISHES_OK_AND_ASSIGN(
      actual, CollectAsyncGenerator(MakeDefaultIfEmptyGenerator(gen, TestInt(42))));
  EXPECT_EQ(std::vector<TestInt>{42}, actual);
}
}  // namespace arrow
