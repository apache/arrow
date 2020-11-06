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
class ManualIteratorBase {
 public:
  Result<T> Next(std::size_t index) {
    std::unique_lock<std::mutex> lock(mutex_);
    last_asked_for_index_ = index;
    waiting_count_++;
    if (cv_.wait_for(lock, std::chrono::milliseconds(3000),
                     [this, index] { return finished_ || results_.size() > index; })) {
      waiting_count_--;
      if (finished_ && index >= results_.size()) {
        return Result<T>(IterationTraits<T>::End());
      }
      return Result<T>(results_[index]);
    } else {
      waiting_count_--;
      return Result<T>(
          Status::Invalid("Timed out waiting for someone to deliver a value"));
    }
  }

  void Deliver(const T& value) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      results_.push_back(value);
    }
    cv_.notify_all();
  }

  void MarkFinished() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      finished_ = true;
    }
    cv_.notify_all();
  }

  std::size_t LastAskedForIndex() const { return last_asked_for_index_; }

  Status WaitForWaiters(unsigned int num_waiters, unsigned int last_asked_for_index) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (cv_.wait_for(lock, std::chrono::milliseconds(100),
                     [this, num_waiters, last_asked_for_index] {
                       return waiting_count_ >= num_waiters &&
                              last_asked_for_index == last_asked_for_index_;
                     })) {
      return Status::OK();
    } else {
      return Status::Invalid("Timed out waiting for waiters to show up at iterator");
    }
  }

  Status WaitForGteWaiters(unsigned int num_waiters) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (cv_.wait_for(lock, std::chrono::milliseconds(100),
                     [this, num_waiters] { return waiting_count_ >= num_waiters; })) {
      return Status::OK();
    } else {
      return Status::Invalid("Timed out waiting for waiters to show up at iterator");
    }
  }

  Status WaitForLteWaiters(unsigned int num_waiters) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (cv_.wait_for(lock, std::chrono::milliseconds(100),
                     [this, num_waiters] { return waiting_count_ <= num_waiters; })) {
      return Status::OK();
    } else {
      return Status::Invalid("Timed out waiting for waiters to show up at iterator");
    }
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::vector<T> results_;
  bool finished_;
  unsigned int waiting_count_;
  std::size_t last_asked_for_index_ = -1;
};

template <typename T>
class ManualIterator {
 public:
  explicit ManualIterator(std::shared_ptr<ManualIteratorBase<T>> base)
      : base_(base), i(0) {}
  Result<T> Next() { return base_->Next(i++); }

 private:
  std::shared_ptr<ManualIteratorBase<T>> base_;
  std::size_t i;
};

template <typename T>
Iterator<T> MakeManual(std::shared_ptr<ManualIteratorBase<T>> base) {
  return Iterator<T>(ManualIterator<T>(base));
}

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

TEST(AsyncReadaheadIterator, BasicIteration) {
  auto base_it = VectorIt({1, 2});
  ASSERT_OK_AND_ASSIGN(auto readahead_it,
                       MakeAsyncReadaheadIterator(std::move(base_it), 1));
  auto future_one = readahead_it.NextFuture();
  auto future_two = readahead_it.NextFuture();
  auto future_three = readahead_it.NextFuture();
  future_three.Wait();
  ASSERT_EQ(TestInt(1), *future_one.result());
  ASSERT_EQ(TestInt(2), *future_two.result());
  ASSERT_EQ(TestInt(), *future_three.result());
}

TEST(AsyncReadaheadIterator, FastConsumer) {
  auto base = std::make_shared<ManualIteratorBase<TestInt>>();
  auto base_it = MakeManual<TestInt>(base);
  ASSERT_OK_AND_ASSIGN(auto readahead_it,
                       MakeAsyncReadaheadIterator(std::move(base_it), 2));
  auto future_one = readahead_it.NextFuture();
  auto future_two = readahead_it.NextFuture();
  auto future_three = readahead_it.NextFuture();
  // The readahead should ask for item 0 and wait
  ASSERT_OK(base->WaitForWaiters(1, 0));
  // Even though readahead is 2 there should only be one thread so one waiter.  TODO:
  // Optimize 100ms wait
  ASSERT_TRUE(base->WaitForGteWaiters(2).IsInvalid());
  ASSERT_EQ(FutureState::PENDING, future_one.state());

  base->Deliver(1);
  ASSERT_OK(base->WaitForWaiters(1, 1));
  ASSERT_TRUE(future_one.is_valid());
  if (future_one.is_valid()) {
    ASSERT_EQ(future_one.result(), 1);
  }
  ASSERT_EQ(FutureState::PENDING, future_two.state());

  base->MarkFinished();
  ASSERT_TRUE(future_two.Wait(0.1));
  if (future_two.is_valid()) {
    ASSERT_EQ(IterationTraits<TestInt>::End(), future_two.result().ValueOrDie());
  }

  ASSERT_TRUE(future_three.Wait(0.1));
  ASSERT_EQ(IterationTraits<TestInt>::End(), future_three.result().ValueOrDie());
}

TEST(AsyncReadaheadIterator, FastProducer) {
  auto base = std::make_shared<ManualIteratorBase<TestInt>>();
  auto base_it = MakeManual<TestInt>(base);
  ASSERT_OK_AND_ASSIGN(auto readahead_it,
                       MakeAsyncReadaheadIterator(std::move(base_it), 2));
  base->Deliver(1);
  base->Deliver(2);
  base->MarkFinished();
  // There should be no one waiting for values.
  ASSERT_OK(base->WaitForLteWaiters(
      0));  // First the thread finishes reading and blocks on its own queue
  ASSERT_TRUE(base->WaitForGteWaiters(1)
                  .IsInvalid());  // Then the thread remains blocked and doesn't call Next

  auto future_one = readahead_it.NextFuture();
  // Future should be returned immediately completed
  ASSERT_EQ(TestInt(1), future_one.result().ValueOrDie());

  auto future_two = readahead_it.NextFuture();
  ASSERT_EQ(TestInt(2), future_two.result().ValueOrDie());

  auto future_three = readahead_it.NextFuture();
  ASSERT_EQ(IterationTraits<TestInt>::End(), future_three.result().ValueOrDie());
}

TEST(AsyncReadaheadIterator, ForEach) {
  auto it = VectorIt({1, 2, 3, 4});
  ASSERT_OK_AND_ASSIGN(auto readahead_it, MakeAsyncReadaheadIterator(std::move(it), 1));
  std::atomic<uint32_t> sum(0);
  auto for_each_future = async::AsyncForEach<TestInt>(
      std::move(readahead_it),
      [&sum](const TestInt& r) {
        sum.fetch_add(r.value);
        return Status::OK();
      },
      1);
  for_each_future.Wait();
  ASSERT_OK(for_each_future.status());
  ASSERT_EQ(10, sum.load());
}

}  // namespace arrow
