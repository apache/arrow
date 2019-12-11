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

template <typename T>
std::vector<T> IteratorToVector(Iterator<T> iterator) {
  std::vector<T> out;

  auto fn = [&out](T value) -> Status {
    out.emplace_back(std::move(value));
    return Status::OK();
  };

  ARROW_EXPECT_OK(iterator.Visit(fn));

  return out;
}

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
  explicit TracingIterator(Iterator<T> it) : it_(std::move(it)) {}

  Status Next(T* out) {
    std::unique_lock<std::mutex> lock(mutex_);
    thread_ids_.insert(std::this_thread::get_id());
    if (!next_status_.ok()) {
      Status st = std::move(next_status_);
      next_status_ = Status::OK();
      return st;
    }
    RETURN_NOT_OK(it_.Next(out));
    values_.push_back(*out);

    cv_.notify_one();
    return Status::OK();
  }

  const std::vector<T>& values() { return values_; }

  const std::unordered_set<std::thread::id>& thread_ids() { return thread_ids_; }

  void InsertFailure(Status st) {
    std::unique_lock<std::mutex> lock(mutex_);
    next_status_ = std::move(st);
  }

  // Wait until the iterator has emitted at least `size` values
  void WaitForValues(int size) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&]() { return values_.size() >= static_cast<size_t>(size); });
  }

  void AssertValuesEqual(const std::vector<T>& expected) {
    std::unique_lock<std::mutex> lock(mutex_);
    ASSERT_EQ(values_, expected);
  }

  void AssertValuesStartwith(const std::vector<T>& expected) {
    std::unique_lock<std::mutex> lock(mutex_);
    ASSERT_TRUE(std::equal(expected.begin(), expected.end(), values_.begin()));
  }

 private:
  Iterator<T> it_;
  Status next_status_;
  std::vector<T> values_;
  std::unordered_set<std::thread::id> thread_ids_;

  std::mutex mutex_;
  std::condition_variable cv_;
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
  T actual;
  ASSERT_OK(it.Next(&actual));
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
  AssertIteratorMatch(std::vector<util::optional<int>>{0, 1, 2, 3, 4, 5},
                      MakeVectorOptionalIterator(std::vector<int>{0, 1, 2, 3, 4, 5}));
}

TEST(TestVectorIterator, RangeForLoop) {
  std::vector<TestInt> ints = {1, 2, 3, 4};

  auto ints_it = ints.begin();
  for (auto v_result : VectorIt(ints)) {
    ASSERT_OK_AND_ASSIGN(TestInt v, v_result);
    ASSERT_EQ(v, *ints_it++);
  }
  ASSERT_EQ(ints_it, ints.end());

  std::vector<std::unique_ptr<TestInt>> vec;
  for (TestInt i : ints) {
    vec.emplace_back(new TestInt(i));
  }

  // also works with move only types
  ints_it = ints.begin();
  for (auto v_result : MakeVectorIterator(std::move(vec))) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestInt> v, std::move(v_result));
    ASSERT_EQ(*v, *ints_it++);
  }
  ASSERT_EQ(ints_it, ints.end());
}

TEST(TestFunctionIterator, RangeForLoop) {
  int i = 0;
  auto fails_at_3 = MakeFunctionIterator([&](TestInt* out) {
    if (i >= 3) {
      return Status::IndexError("fails at 3");
    }
    out->value = i++;
    return Status::OK();
  });

  std::vector<Result<TestInt>> actual,
      expected{0, 1, 2, Status::IndexError("fails at 3")};
  for (auto r : fails_at_3) {
    actual.push_back(std::move(r));
  }
  ASSERT_EQ(actual, expected);
}

TEST(FilterIterator, Basic) {
  AssertIteratorMatch({1, 2, 3, 4}, FilterIt(VectorIt({1, 2, 3, 4}),
                                             [](TestInt i, TestInt* o, bool* accept) {
                                               *o = std::move(i);
                                               return Status::OK();
                                             }));

  AssertIteratorMatch(
      {}, FilterIt(VectorIt({1, 2, 3, 4}), [](TestInt i, TestInt* o, bool* accept) {
        *o = std::move(i);
        *accept = false;
        return Status::OK();
      }));

  AssertIteratorMatch(
      {2, 4}, FilterIt(VectorIt({1, 2, 3, 4}), [](TestInt i, TestInt* o, bool* accept) {
        *o = std::move(i);
        *accept = (i.value % 2 == 0);
        return Status::OK();
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
  ASSERT_OK(it.Next(&v));
  ASSERT_EQ(v, TestInt());
}

TEST(ReadaheadIterator, Empty) {
  Iterator<TestInt> it;
  ASSERT_OK(MakeReadaheadIterator(VectorIt({}), 2, &it));
  AssertIteratorMatch({}, std::move(it));
}

TEST(ReadaheadIterator, Basic) {
  Iterator<TestInt> it;
  ASSERT_OK(MakeReadaheadIterator(VectorIt({1, 2, 3, 4, 5}), 2, &it));
  AssertIteratorMatch({1, 2, 3, 4, 5}, std::move(it));
}

TEST(ReadaheadIterator, NotExhausted) {
  Iterator<TestInt> it;
  ASSERT_OK(MakeReadaheadIterator(VectorIt({1, 2, 3, 4, 5}), 2, &it));
  AssertIteratorNext({1}, it);
  AssertIteratorNext({2}, it);
}

void SleepABit(double seconds = 1e-3) {
  std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
}

TEST(ReadaheadIterator, Trace) {
  auto tracing =
      std::make_shared<TracingIterator<TestInt>>(VectorIt({1, 2, 3, 4, 5, 6, 7, 8}));
  ASSERT_EQ(tracing->values().size(), 0);

  Iterator<TestInt> it;
  ASSERT_OK(MakeReadaheadIterator(MakePointerIterator(tracing), 2, &it));
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
  auto tracing = std::make_shared<TracingIterator<TestInt>>(VectorIt({1, 2, 3}));
  ASSERT_EQ(tracing->values().size(), 0);
  tracing->InsertFailure(Status::IOError("xxx"));

  Iterator<TestInt> it;
  ASSERT_OK(MakeReadaheadIterator(MakePointerIterator(tracing), 2, &it));
  TestInt v;
  ASSERT_RAISES(IOError, it.Next(&v));

  AssertIteratorNext({1}, it);
  tracing->WaitForValues(3);
  SleepABit();
  tracing->AssertValuesEqual({1, 2, 3});
  AssertIteratorNext({2}, it);
  AssertIteratorNext({3}, it);
  AssertIteratorExhausted(it);
}

}  // namespace arrow
