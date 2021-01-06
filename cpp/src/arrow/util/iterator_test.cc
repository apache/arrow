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

#include "arrow/util/iterator.h"
#include "arrow/util/async_iterator.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <ostream>
#include <thread>
#include <unordered_set>
#include <vector>

#include "arrow/testing/gtest_util.h"

namespace arrow {

struct TestInt {
  TestInt() : value(-999) {}
  TestInt(int i) : value(i) {}  // NOLINT runtime/explicit
  int value;

  bool operator==(const TestInt& other) const { return value == other.value; }

  friend std::ostream& operator<<(std::ostream& os, const TestInt& v) {
    os << "{" << v.value << "}";
    return os;
  }
};

template <>
struct IterationTraits<TestInt> {
  static TestInt End() { return TestInt(); }
};

template <typename T>
class TracingIterator {
 public:
  explicit TracingIterator(Iterator<T> it) : it_(std::move(it)), state_(new State) {}

  Result<T> Next() {
    auto lock = state_->Lock();
    state_->thread_ids_.insert(std::this_thread::get_id());

    RETURN_NOT_OK(state_->GetNextStatus());

    ARROW_ASSIGN_OR_RAISE(auto out, it_.Next());
    state_->values_.push_back(out);

    state_->cv_.notify_one();
    return out;
  }

  class State {
   public:
    const std::vector<T>& values() { return values_; }

    const std::unordered_set<std::thread::id>& thread_ids() { return thread_ids_; }

    void InsertFailure(Status st) {
      auto lock = Lock();
      next_status_ = std::move(st);
    }

    // Wait until the iterator has emitted at least `size` values
    void WaitForValues(int size) {
      auto lock = Lock();
      cv_.wait(lock, [&]() { return values_.size() >= static_cast<size_t>(size); });
    }

    void AssertValuesEqual(const std::vector<T>& expected) {
      auto lock = Lock();
      ASSERT_EQ(values_, expected);
    }

    void AssertValuesStartwith(const std::vector<T>& expected) {
      auto lock = Lock();
      ASSERT_TRUE(std::equal(expected.begin(), expected.end(), values_.begin()));
    }

    std::unique_lock<std::mutex> Lock() { return std::unique_lock<std::mutex>(mutex_); }

   private:
    friend TracingIterator;

    Status GetNextStatus() {
      if (next_status_.ok()) {
        return Status::OK();
      }

      Status st = std::move(next_status_);
      next_status_ = Status::OK();
      return st;
    }

    Status next_status_;
    std::vector<T> values_;
    std::unordered_set<std::thread::id> thread_ids_;

    std::mutex mutex_;
    std::condition_variable cv_;
  };

  const std::shared_ptr<State>& state() const { return state_; }

 private:
  Iterator<T> it_;

  std::shared_ptr<State> state_;
};

template <typename T>
inline Iterator<T> EmptyIt() {
  return MakeEmptyIterator<T>();
}
inline Iterator<TestInt> VectorIt(std::vector<TestInt> v) {
  return MakeVectorIterator<TestInt>(std::move(v));
}

std::function<Future<TestInt>()> AsyncVectorIt(std::vector<TestInt> v) {
  auto index = std::make_shared<size_t>(0);
  auto vec = std::make_shared<std::vector<TestInt>>(std::move(v));
  return [index, vec]() -> Future<TestInt> {
    if (*index >= vec->size()) {
      return Future<TestInt>::MakeFinished(IterationTraits<TestInt>::End());
    }
    auto next = (*vec)[*index];
    (*index)++;
    return Future<TestInt>::MakeFinished(next);
  };
}

constexpr auto kYieldDuration = std::chrono::microseconds(50);

// Yields items with a small pause between each one from a background thread
std::function<Future<TestInt>()> BackgroundAsyncVectorIt(std::vector<TestInt> v) {
  auto pool = internal::GetCpuThreadPool();
  auto iterator = VectorIt(v);
  auto slow_iterator = MakeTransformedIterator<TestInt, TestInt>(
      std::move(iterator), [](TestInt item) -> TransformFlow<TestInt> {
        std::this_thread::sleep_for(kYieldDuration);
        return TransformYield(item);
      });
  EXPECT_OK_AND_ASSIGN(auto background,
                       MakeBackgroundIterator<TestInt>(std::move(slow_iterator), pool));
  return background;
}

std::vector<TestInt> RangeVector(int max) {
  std::vector<TestInt> range(max);
  for (unsigned int i = 0; i < max; i++) {
    range[i] = i;
  }
  return range;
}

template <typename T>
inline Iterator<T> VectorIt(std::vector<T> v) {
  return MakeVectorIterator<T>(std::move(v));
}

template <typename Fn, typename T>
inline Iterator<T> FilterIt(Iterator<T> it, Fn&& fn) {
  return MakeFilterIterator(std::forward<Fn>(fn), std::move(it));
}

template <typename T>
inline Iterator<T> FlattenIt(Iterator<Iterator<T>> its) {
  return MakeFlattenIterator(std::move(its));
}

template <typename T>
void AssertIteratorMatch(std::vector<T> expected, Iterator<T> actual) {
  EXPECT_EQ(expected, IteratorToVector(std::move(actual)));
}

template <typename T>
void AssertAsyncGeneratorMatch(std::vector<T> expected, AsyncGenerator<T> actual) {
  auto vec_future = CollectAsyncGenerator(std::move(actual));
  EXPECT_OK_AND_ASSIGN(auto vec, vec_future.result());
  EXPECT_EQ(expected, vec);
}

template <typename T>
void AssertIteratorNoMatch(std::vector<T> expected, Iterator<T> actual) {
  EXPECT_NE(expected, IteratorToVector(std::move(actual)));
}

template <typename T>
void AssertIteratorNext(T expected, Iterator<T>& it) {
  ASSERT_OK_AND_ASSIGN(T actual, it.Next());
  ASSERT_EQ(expected, actual);
}

template <typename T>
void AssertIteratorExhausted(Iterator<T>& it) {
  AssertIteratorNext(IterationTraits<T>::End(), it);
}

TEST(TestEmptyIterator, Basic) { AssertIteratorMatch({}, EmptyIt<TestInt>()); }

TEST(TestVectorIterator, Basic) {
  AssertIteratorMatch({}, VectorIt({}));
  AssertIteratorMatch({1, 2, 3}, VectorIt({1, 2, 3}));

  AssertIteratorNoMatch({1}, VectorIt({}));
  AssertIteratorNoMatch({}, VectorIt({1, 2, 3}));
  AssertIteratorNoMatch({1, 2, 2}, VectorIt({1, 2, 3}));
  AssertIteratorNoMatch({1, 2, 3, 1}, VectorIt({1, 2, 3}));

  // int does not have specialized IterationTraits
  std::vector<int> elements = {0, 1, 2, 3, 4, 5};
  std::vector<int*> expected;
  for (int& element : elements) {
    expected.push_back(&element);
  }
  AssertIteratorMatch(expected, MakeVectorPointingIterator(std::move(elements)));
}

TEST(TestVectorIterator, RangeForLoop) {
  std::vector<TestInt> ints = {1, 2, 3, 4};

  auto ints_it = ints.begin();
  for (auto maybe_i : VectorIt(ints)) {
    ASSERT_OK_AND_ASSIGN(TestInt i, maybe_i);
    ASSERT_EQ(i, *ints_it++);
  }
  ASSERT_EQ(ints_it, ints.end()) << *ints_it << "@" << (ints_it - ints.begin());

  std::vector<std::unique_ptr<TestInt>> intptrs;
  for (TestInt i : ints) {
    intptrs.emplace_back(new TestInt(i));
  }

  // also works with move only types
  ints_it = ints.begin();
  for (auto maybe_i_ptr : MakeVectorIterator(std::move(intptrs))) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestInt> i_ptr, maybe_i_ptr);
    ASSERT_EQ(*i_ptr, *ints_it++);
  }
  ASSERT_EQ(ints_it, ints.end());
}

template <typename T>
std::function<TransformFlow<T>(T)> MakeFirstN(int n) {
  auto remaining = std::make_shared<int>(n);
  return [remaining](T next) -> TransformFlow<T> {
    if (*remaining > 0) {
      *remaining = *remaining - 1;
      return TransformYield(next);
    }
    return TransformFinish();
  };
}

TEST(TestIteratorTransform, Truncating) {
  auto original = VectorIt({1, 2, 3});
  auto truncated = MakeTransformedIterator(std::move(original), MakeFirstN<TestInt>(2));
  AssertIteratorMatch({1, 2}, std::move(truncated));
}

TEST(TestIteratorTransform, TestPointer) {
  auto original = VectorIt<std::shared_ptr<int>>(
      {std::make_shared<int>(1), std::make_shared<int>(2), std::make_shared<int>(3)});
  auto truncated =
      MakeTransformedIterator(std::move(original), MakeFirstN<std::shared_ptr<int>>(2));
  ASSERT_OK_AND_ASSIGN(auto result, truncated.ToVector());
  ASSERT_EQ(2, result.size());
}

TEST(TestIteratorTransform, TruncatingShort) {
  // Tests the failsafe case where we never call Finish
  auto original = VectorIt({1});
  auto truncated = MakeTransformedIterator<TestInt, TestInt>(std::move(original),
                                                             MakeFirstN<TestInt>(2));
  AssertIteratorMatch({1}, std::move(truncated));
}

TEST(TestAsyncUtil, Background) {
  std::vector<TestInt> expected = {1, 2, 3};
  auto background = BackgroundAsyncVectorIt(expected);
  auto future = CollectAsyncGenerator(background);
  ASSERT_FALSE(future.is_finished());
  future.Wait();
  ASSERT_TRUE(future.is_finished());
  ASSERT_EQ(expected, *future.result());
}

// TEST(TestAsyncUtil, CompleteBackgroundStressTest) {
//   auto expected = RangeVector(1000);
//   std::vector<Future<std::vector<TestInt>>> futures;
//   for (unsigned int i = 0; i < 1000; i++) {
//     auto background = BackgroundAsyncVectorIt(expected);
//     futures.push_back(CollectAsyncGenerator(background));
//   }
//   auto combined = All(futures);
//   combined.Wait(2);
//   if (combined.is_finished()) {
//     ASSERT_OK_AND_ASSIGN(auto completed_vectors, combined.result());
//     for (auto&& vector : completed_vectors) {
//       ASSERT_EQ(vector, expected);
//     }
//   } else {
//     FAIL() << "After 2 seconds all background iterators had not finished collecting";
//   }
// }

TEST(TestAsyncUtil, Visit) {
  auto generator = AsyncVectorIt({1, 2, 3});
  auto sum = std::make_shared<unsigned int>();
  auto sum_future = VisitAsyncGenerator<TestInt>(generator, [sum](TestInt item) {
    (*sum) += item.value;
    return Status::OK();
  });
  // Should be superfluous
  sum_future.Wait();
  ASSERT_EQ(6, *sum);
}

TEST(TestAsyncUtil, Collect) {
  std::vector<TestInt> expected = {1, 2, 3};
  auto generator = AsyncVectorIt(expected);
  auto collected = CollectAsyncGenerator(generator);
  ASSERT_EQ(expected, *collected.result());
}

template <typename T>
std::function<TransformFlow<T>(T)> MakeRepeatN(int repeat_count) {
  auto current_repeat = std::make_shared<int>(0);
  return [repeat_count, current_repeat](T next) -> TransformFlow<T> {
    (*current_repeat) += 1;
    bool ready_for_next = false;
    if (*current_repeat == repeat_count) {
      *current_repeat = 0;
      ready_for_next = true;
    }
    return TransformYield(next, ready_for_next);
  };
}

TEST(TestIteratorTransform, Repeating) {
  auto original = VectorIt({1, 2, 3});
  auto repeated = MakeTransformedIterator<TestInt, TestInt>(std::move(original),
                                                            MakeRepeatN<TestInt>(2));
  AssertIteratorMatch({1, 1, 2, 2, 3, 3}, std::move(repeated));
}

template <typename T>
std::function<TransformFlow<T>(T)> MakeFilter(std::function<bool(T&)> filter) {
  return [filter](T next) -> TransformFlow<T> {
    if (filter(next)) {
      return TransformYield(next);
    } else {
      return TransformSkip();
    }
  };
}

TEST(TestIteratorTransform, Filter) {
  // Exercises TransformSkip
  auto original = VectorIt({1, 2, 3});
  auto filter = MakeFilter<TestInt>([](TestInt& t) { return t.value != 2; });
  auto filtered = MakeTransformedIterator(std::move(original), filter);
  AssertIteratorMatch({1, 3}, std::move(filtered));
}

TEST(TestAsyncIteratorTransform, Filter) {
  auto original = AsyncVectorIt({1, 2, 3});
  auto filter = MakeFilter<TestInt>([](TestInt& t) { return t.value != 2; });
  auto filtered = TransformAsyncGenerator(std::move(original), filter);
  AssertAsyncGeneratorMatch({1, 3}, std::move(filtered));
}

TEST(TestFunctionIterator, RangeForLoop) {
  int i = 0;
  auto fails_at_3 = MakeFunctionIterator([&]() -> Result<TestInt> {
    if (i >= 3) {
      return Status::IndexError("fails at 3");
    }
    return i++;
  });

  int expected_i = 0;
  for (auto maybe_i : fails_at_3) {
    if (expected_i < 3) {
      ASSERT_OK(maybe_i.status());
      ASSERT_EQ(*maybe_i, expected_i);
    } else if (expected_i == 3) {
      ASSERT_RAISES(IndexError, maybe_i.status());
    }
    ASSERT_LE(expected_i, 3) << "iteration stops after an error is encountered";
    ++expected_i;
  }
}

TEST(FilterIterator, Basic) {
  AssertIteratorMatch({1, 2, 3, 4}, FilterIt(VectorIt({1, 2, 3, 4}), [](TestInt i) {
                        return FilterIterator::Accept(std::move(i));
                      }));

  AssertIteratorMatch({}, FilterIt(VectorIt({1, 2, 3, 4}), [](TestInt i) {
                        return FilterIterator::Reject<TestInt>();
                      }));

  AssertIteratorMatch({2, 4}, FilterIt(VectorIt({1, 2, 3, 4}), [](TestInt i) {
                        return i.value % 2 == 0 ? FilterIterator::Accept(std::move(i))
                                                : FilterIterator::Reject<TestInt>();
                      }));
}

TEST(FlattenVectorIterator, Basic) {
  // Flatten expects to consume Iterator<Iterator<T>>
  AssertIteratorMatch({}, FlattenIt(EmptyIt<Iterator<TestInt>>()));

  std::vector<Iterator<TestInt>> ok;
  ok.push_back(VectorIt({1}));
  ok.push_back(VectorIt({2}));
  ok.push_back(VectorIt({3}));
  AssertIteratorMatch({1, 2, 3}, FlattenIt(VectorIt(std::move(ok))));

  std::vector<Iterator<TestInt>> not_enough;
  not_enough.push_back(VectorIt({1}));
  not_enough.push_back(VectorIt({2}));
  AssertIteratorNoMatch({1, 2, 3}, FlattenIt(VectorIt(std::move(not_enough))));

  std::vector<Iterator<TestInt>> too_much;
  too_much.push_back(VectorIt({1}));
  too_much.push_back(VectorIt({2}));
  too_much.push_back(VectorIt({3}));
  too_much.push_back(VectorIt({2}));
  AssertIteratorNoMatch({1, 2, 3}, FlattenIt(VectorIt(std::move(too_much))));
}

Iterator<TestInt> Join(TestInt a, TestInt b) {
  std::vector<Iterator<TestInt>> joined{2};
  joined[0] = VectorIt({a});
  joined[1] = VectorIt({b});

  return FlattenIt(VectorIt(std::move(joined)));
}

Iterator<TestInt> Join(TestInt a, Iterator<TestInt> b) {
  std::vector<Iterator<TestInt>> joined{2};
  joined[0] = VectorIt(std::vector<TestInt>{a});
  joined[1] = std::move(b);

  return FlattenIt(VectorIt(std::move(joined)));
}

TEST(FlattenVectorIterator, Pyramid) {
  auto it = Join(1, Join(2, Join(2, Join(3, Join(3, 3)))));
  AssertIteratorMatch({1, 2, 2, 3, 3, 3}, std::move(it));
}

TEST(ReadaheadIterator, DefaultConstructor) {
  ReadaheadIterator<TestInt> it;
  TestInt v{42};
  ASSERT_OK_AND_ASSIGN(v, it.Next());
  ASSERT_EQ(v, TestInt());
}

TEST(ReadaheadIterator, Empty) {
  ASSERT_OK_AND_ASSIGN(auto it, MakeReadaheadIterator(VectorIt({}), 2));
  AssertIteratorMatch({}, std::move(it));
}

TEST(ReadaheadIterator, Basic) {
  ASSERT_OK_AND_ASSIGN(auto it, MakeReadaheadIterator(VectorIt({1, 2, 3, 4, 5}), 2));
  AssertIteratorMatch({1, 2, 3, 4, 5}, std::move(it));
}

TEST(ReadaheadIterator, NotExhausted) {
  ASSERT_OK_AND_ASSIGN(auto it, MakeReadaheadIterator(VectorIt({1, 2, 3, 4, 5}), 2));
  AssertIteratorNext({1}, it);
  AssertIteratorNext({2}, it);
}

void SleepABit(double seconds = 1e-3) {
  std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
}

TEST(ReadaheadIterator, Trace) {
  TracingIterator<TestInt> tracing_it(VectorIt({1, 2, 3, 4, 5, 6, 7, 8}));
  auto tracing = tracing_it.state();
  ASSERT_EQ(tracing->values().size(), 0);

  ASSERT_OK_AND_ASSIGN(
      auto it, MakeReadaheadIterator(Iterator<TestInt>(std::move(tracing_it)), 2));
  tracing->WaitForValues(2);
  SleepABit();  // check no further value is emitted
  tracing->AssertValuesEqual({1, 2});

  AssertIteratorNext({1}, it);
  tracing->WaitForValues(3);
  SleepABit();
  tracing->AssertValuesEqual({1, 2, 3});

  AssertIteratorNext({2}, it);
  AssertIteratorNext({3}, it);
  AssertIteratorNext({4}, it);
  tracing->WaitForValues(6);
  SleepABit();
  tracing->AssertValuesEqual({1, 2, 3, 4, 5, 6});

  AssertIteratorNext({5}, it);
  AssertIteratorNext({6}, it);
  AssertIteratorNext({7}, it);
  tracing->WaitForValues(9);
  SleepABit();
  tracing->AssertValuesEqual({1, 2, 3, 4, 5, 6, 7, 8, {}});

  AssertIteratorNext({8}, it);
  AssertIteratorExhausted(it);
  AssertIteratorExhausted(it);  // Again
  tracing->WaitForValues(9);
  SleepABit();
  tracing->AssertValuesStartwith({1, 2, 3, 4, 5, 6, 7, 8, {}});
  // A couple more EOF values may have been emitted
  const auto& values = tracing->values();
  ASSERT_LE(values.size(), 11);
  for (size_t i = 9; i < values.size(); ++i) {
    ASSERT_EQ(values[i], TestInt());
  }

  // Values were all emitted from the same thread, and it's not this thread
  const auto& thread_ids = tracing->thread_ids();
  ASSERT_EQ(thread_ids.size(), 1);
  ASSERT_NE(*thread_ids.begin(), std::this_thread::get_id());
}

TEST(ReadaheadIterator, NextError) {
  TracingIterator<TestInt> tracing_it((VectorIt({1, 2, 3})));
  auto tracing = tracing_it.state();
  ASSERT_EQ(tracing->values().size(), 0);

  tracing->InsertFailure(Status::IOError("xxx"));

  ASSERT_OK_AND_ASSIGN(
      auto it, MakeReadaheadIterator(Iterator<TestInt>(std::move(tracing_it)), 2));

  ASSERT_RAISES(IOError, it.Next().status());

  AssertIteratorNext({1}, it);
  tracing->WaitForValues(3);
  SleepABit();
  tracing->AssertValuesEqual({1, 2, 3});
  AssertIteratorNext({2}, it);
  AssertIteratorNext({3}, it);
  AssertIteratorExhausted(it);
}

}  // namespace arrow
