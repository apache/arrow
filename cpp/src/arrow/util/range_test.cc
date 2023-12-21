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
#include <cstddef>
#include <cstdint>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/util/range.h"

using testing::ElementsAre;

namespace arrow::internal {

class TestLazyIter : public ::testing::Test {
 public:
  int64_t kSize = 1000;
  void SetUp() override {
    randint(kSize, 0, 1000000, &source_);
    target_.resize(kSize);
  }

 protected:
  std::vector<int> source_;
  std::vector<int> target_;
};

TEST_F(TestLazyIter, TestIncrementCopy) {
  auto add_one = [this](int64_t index) { return source_[index] + 1; };
  auto lazy_range = MakeLazyRange(add_one, kSize);
  std::copy(lazy_range.begin(), lazy_range.end(), target_.begin());

  for (int64_t index = 0; index < kSize; ++index) {
    ASSERT_EQ(source_[index] + 1, target_[index]);
  }
}

TEST_F(TestLazyIter, TestPostIncrementCopy) {
  auto add_one = [this](int64_t index) { return source_[index] + 1; };
  auto lazy_range = MakeLazyRange(add_one, kSize);
  auto iter = lazy_range.begin();
  auto end = lazy_range.end();
  auto target_iter = target_.begin();

  while (iter != end) {
    *(target_iter++) = *(iter++);
  }

  for (size_t index = 0, limit = source_.size(); index != limit; ++index) {
    ASSERT_EQ(source_[index] + 1, target_[index]);
  }
}

TEST(Zip, TupleTypes) {
  char arr[3];
  const std::string const_arr[3];
  for (auto tuple :
       Zip(arr,                   // 1. mutable lvalue range
           const_arr,             // 2. const lvalue range
           std::vector<float>{},  // 3. rvalue range
           std::vector<bool>{},   // 4. rvalue range dereferencing to non ref
           Enumerate<int>)) {     // 6. Enumerate
                                  //    (const lvalue range dereferencing to non ref)
    static_assert(
        std::is_same_v<decltype(tuple),
                       std::tuple<char&,               // 1. mutable lvalue ref binding
                                  const std::string&,  // 2. const lvalue ref binding
                                  float&,              // 3. mutable lvalue ref binding
                                  std::vector<bool>::reference,  // 4. by-value non ref
                                                                 // binding (thanks STL)
                                  int  // 5. by-value non ref binding
                                       //    (that's fine they're just ints)
                                  >>);
  }

  static size_t max_count;
  static size_t count = 0;

  struct Counted {
    static void increment_count() {
      ++count;
      EXPECT_LE(count, max_count);
    }

    Counted() { increment_count(); }
    Counted(Counted&&) { increment_count(); }

    ~Counted() { --count; }

    Counted(const Counted&) = delete;
    Counted& operator=(const Counted&) = delete;
    Counted& operator=(Counted&&) = delete;
  };

  {
    max_count = 3;
    const Counted const_arr[3];
    EXPECT_EQ(count, 3);

    for (auto [e] : Zip(const_arr)) {
      // Putting a const reference to range into Zip results in no copies and the
      // corresponding tuple element will also be a const reference
      EXPECT_EQ(count, 3);
      static_assert(std::is_same_v<decltype(e), const Counted&>);
    }
    EXPECT_EQ(count, 3);
  }

  {
    max_count = 3;
    Counted arr[3];
    EXPECT_EQ(count, 3);

    for (auto [e] : Zip(arr)) {
      // Putting a mutable reference to range into Zip results in no copies and the
      // corresponding tuple element will also be a mutable reference
      EXPECT_EQ(count, 3);
      static_assert(std::is_same_v<decltype(e), Counted&>);
    }
    EXPECT_EQ(count, 3);
  }

  {
    max_count = 3;
    EXPECT_EQ(count, 0);
    for (auto [e] : Zip(std::vector<Counted>(3))) {
      // Putting a prvalue vector into Zip results in no copies and keeps the temporary
      // alive as a mutable vector so that we can move out of it if we might reuse the
      // elements:
      EXPECT_EQ(count, 3);
      static_assert(std::is_same_v<decltype(e), Counted&>);
    }
    EXPECT_EQ(count, 0);
  }

  {
    std::vector<bool> v{false, false, false, false};
    for (auto [i, e] : Zip(Enumerate<int>, v)) {
      // Testing with a range whose references aren't actually references
      static_assert(std::is_same_v<decltype(e), decltype(v)::reference>);
      static_assert(std::is_same_v<decltype(e), decltype(v[0])>);
      static_assert(!std::is_reference_v<decltype(e)>);
      e = (i % 2 == 0);
    }

    EXPECT_THAT(v, ElementsAre(true, false, true, false));
  }
}

TEST(Zip, EndAfterShortestEnds) {
  std::vector<int> shorter{0, 0, 0}, longer{9, 9, 9, 9, 9, 9};

  for (auto [s, l] : Zip(shorter, longer)) {
    std::swap(s, l);
  }

  EXPECT_THAT(longer, ElementsAre(0, 0, 0, 9, 9, 9));
}

TEST(Zip, Enumerate) {
  std::vector<std::string> vec(3);

  for (auto [i, s] : Zip(Enumerate<>, vec)) {
    static_assert(std::is_same_v<decltype(s), std::string&>);
    s = std::to_string(i + 7);
  }

  EXPECT_THAT(vec, ElementsAre("7", "8", "9"));
}
}  // namespace arrow::internal
