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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <ostream>
#include <thread>
#include <unordered_set>
#include <vector>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/test_common.h"
#include "arrow/util/vector.h"

namespace arrow {

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

// Non-templated version of VectorIt<T> to allow better type deduction
inline Iterator<TestInt> VectorIt(std::vector<TestInt> v) {
  return MakeVectorIterator<TestInt>(std::move(v));
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
void AssertIteratorNoMatch(std::vector<T> expected, Iterator<T> actual) {
  EXPECT_NE(expected, IteratorToVector(std::move(actual)));
}

template <typename T>
void AssertIteratorNext(T expected, Iterator<T>& it) {
  ASSERT_OK_AND_ASSIGN(T actual, it.Next());
  ASSERT_EQ(expected, actual);
}

template <typename T>
class DeleteDetectableIterator {
 public:
  explicit DeleteDetectableIterator(std::vector<T> values, bool* deleted)
      : values_(std::move(values)), i_(0), deleted_(deleted) {}

  DeleteDetectableIterator(DeleteDetectableIterator&& source)
      : values_(std::move(source.values_)), i_(source.i_), deleted_(source.deleted_) {
    source.deleted_ = nullptr;
  }

  ~DeleteDetectableIterator() {
    if (deleted_) {
      *deleted_ = true;
    }
  }

  Result<T> Next() {
    if (i_ == values_.size()) {
      return IterationTraits<T>::End();
    }
    return std::move(values_[i_++]);
  }

 private:
  std::vector<T> values_;
  size_t i_;
  bool* deleted_;
};

// Generic iterator tests

TEST(TestIterator, DeleteOnEnd) {
  bool deleted = false;
  Iterator<TestInt> it(DeleteDetectableIterator<TestInt>({1}, &deleted));
  ASSERT_FALSE(deleted);
  AssertIteratorNext({1}, it);
  ASSERT_FALSE(deleted);
  ASSERT_OK_AND_ASSIGN(auto value, it.Next());
  ASSERT_TRUE(IsIterationEnd(value));
  ASSERT_TRUE(deleted);
}

// --------------------------------------------------------------------
// Synchronous iterator tests

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

Transformer<TestInt, TestStr> MakeFirstN(int n) {
  int remaining = n;
  return [remaining](TestInt next) mutable -> Result<TransformFlow<TestStr>> {
    if (remaining > 0) {
      remaining--;
      return TransformYield(TestStr(next));
    }
    return TransformFinish();
  };
}

template <typename T>
Transformer<T, T> MakeFirstNGeneric(int n) {
  int remaining = n;
  return [remaining](T next) mutable -> Result<TransformFlow<T>> {
    if (remaining > 0) {
      remaining--;
      return TransformYield(next);
    }
    return TransformFinish();
  };
}

TEST(TestIteratorTransform, Truncating) {
  auto original = VectorIt({1, 2, 3});
  auto truncated = MakeTransformedIterator(std::move(original), MakeFirstN(2));
  AssertIteratorMatch({"1", "2"}, std::move(truncated));
}

TEST(TestIteratorTransform, TestPointer) {
  auto original = VectorIt<std::shared_ptr<int>>(
      {std::make_shared<int>(1), std::make_shared<int>(2), std::make_shared<int>(3)});
  auto truncated = MakeTransformedIterator(std::move(original),
                                           MakeFirstNGeneric<std::shared_ptr<int>>(2));
  ASSERT_OK_AND_ASSIGN(auto result, truncated.ToVector());
  ASSERT_EQ(2, result.size());
}

TEST(TestIteratorTransform, TruncatingShort) {
  // Tests the failsafe case where we never call Finish
  auto original = VectorIt({1});
  auto truncated =
      MakeTransformedIterator<TestInt, TestStr>(std::move(original), MakeFirstN(2));
  AssertIteratorMatch({"1"}, std::move(truncated));
}

TEST(TestIteratorTransform, SkipSome) {
  // Exercises TransformSkip
  auto original = VectorIt({1, 2, 3});
  auto filter = MakeFilter([](TestInt& t) { return t.value != 2; });
  auto filtered = MakeTransformedIterator(std::move(original), filter);
  AssertIteratorMatch({"1", "3"}, std::move(filtered));
}

TEST(TestIteratorTransform, SkipAll) {
  // Exercises TransformSkip
  auto original = VectorIt({1, 2, 3});
  auto filter = MakeFilter([](TestInt& t) { return false; });
  auto filtered = MakeTransformedIterator(std::move(original), filter);
  AssertIteratorMatch({}, std::move(filtered));
}

Transformer<TestInt, TestStr> MakeAbortOnSecond() {
  int counter = 0;
  return [counter](TestInt next) mutable -> Result<TransformFlow<TestStr>> {
    if (counter++ == 1) {
      return Status::Invalid("X");
    }
    return TransformYield(TestStr(next));
  };
}

TEST(TestIteratorTransform, Abort) {
  auto original = VectorIt({1, 2, 3});
  auto transformed = MakeTransformedIterator(std::move(original), MakeAbortOnSecond());
  ASSERT_OK(transformed.Next());
  ASSERT_RAISES(Invalid, transformed.Next());
  ASSERT_OK_AND_ASSIGN(auto third, transformed.Next());
  ASSERT_TRUE(IsIterationEnd(third));
}

template <typename T>
Transformer<T, T> MakeRepeatN(int repeat_count) {
  int current_repeat = 0;
  return [repeat_count, current_repeat](T next) mutable -> Result<TransformFlow<T>> {
    current_repeat++;
    bool ready_for_next = false;
    if (current_repeat == repeat_count) {
      current_repeat = 0;
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
      ASSERT_OK(maybe_i);
      ASSERT_EQ(*maybe_i, expected_i);
    } else if (expected_i == 3) {
      ASSERT_RAISES(IndexError, maybe_i);
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

TEST(ReadaheadIterator, Trace) {
  TracingIterator<TestInt> tracing_it(VectorIt({1, 2, 3, 4, 5, 6, 7, 8}));
  auto tracing = tracing_it.state();
  ASSERT_EQ(tracing->values().size(), 0);

  ASSERT_OK_AND_ASSIGN(
      auto it, MakeReadaheadIterator(Iterator<TestInt>(std::move(tracing_it)), 2));
  SleepABit();  // Background iterator won't start pumping until first request comes in
  ASSERT_EQ(tracing->values().size(), 0);

  AssertIteratorNext({1}, it);  // Once we ask for one value we should get that one value
                                // as well as 2 read ahead

  tracing->WaitForValues(3);
  tracing->AssertValuesEqual({1, 2, 3});

  SleepABit();  // No further values should be fetched
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

#ifdef ARROW_ENABLE_THREADING
  // Values were all emitted from the same thread, and it's not this thread
  const auto& thread_ids = tracing->thread_ids();
  ASSERT_EQ(thread_ids.size(), 1);
  ASSERT_NE(*thread_ids.begin(), std::this_thread::get_id());
#endif
}

TEST(ReadaheadIterator, NextError) {
  TracingIterator<TestInt> tracing_it((VectorIt({1, 2, 3})));
  auto tracing = tracing_it.state();
  ASSERT_EQ(tracing->values().size(), 0);

  tracing->InsertFailure(Status::IOError("xxx"));

  ASSERT_OK_AND_ASSIGN(
      auto it, MakeReadaheadIterator(Iterator<TestInt>(std::move(tracing_it)), 2));

  ASSERT_RAISES(IOError, it.Next());

  AssertIteratorExhausted(it);
  SleepABit();
  tracing->AssertValuesEqual({});
  AssertIteratorExhausted(it);
}

}  // namespace arrow
