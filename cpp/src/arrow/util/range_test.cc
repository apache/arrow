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
  static size_t count = 0;
  struct Counted {
    Counted() { ++count; }
    ~Counted() { --count; }

    Counted(const Counted&) { ++count; }
    Counted& operator=(const Counted&) = delete;
  };

  {
    const Counted const_arr[] = {{}, {}, {}};
    EXPECT_EQ(count, std::size(const_arr));

    for (auto [e] : Zip(const_arr)) {
      // Putting a const reference to range into Zip results in no copies and the
      // corresponding tuple element will also be a const reference
      EXPECT_EQ(count, std::size(const_arr));
      static_assert(std::is_same_v<decltype(e), const Counted&>);
    }
  }

  {
    Counted arr[] = {{}, {}, {}};
    EXPECT_EQ(count, std::size(arr));

    for (auto [e] : Zip(arr)) {
      // Putting a mutable reference to range into Zip results in no copies and the
      // corresponding tuple element will also be a mutable reference
      EXPECT_EQ(count, std::size(arr));
      static_assert(std::is_same_v<decltype(e), Counted&>);
    }
  }

  for (auto [e] : Zip(std::vector<Counted>{{}, {}, {}})) {
    // Putting a prvalue vector into Zip results in no copies and keeps the temporary
    // alive as a mutable vector so that we can move out of it if we might reuse the
    // elements:
    EXPECT_EQ(count, 3);
    static_assert(std::is_same_v<decltype(e), Counted&>);
  }

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
