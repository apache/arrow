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

#include <cstddef>
#include <iostream>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/span.h"

using testing::ElementsAre;
using testing::ElementsAreArray;
using testing::PrintToString;

namespace arrow::util {

template <typename T>
std::ostream& operator<<(std::ostream& os, const span<T>& span) {
  // Inefficient but good enough for testing
  os << PrintToString(std::vector(span.begin(), span.end()));
  return os;
}

TEST(Span, Construction) {
  // const spans may be constructed from mutable spans
  static_assert(std::is_constructible_v<span<const int>, span<int>>);
  // ... but mutable spans may be constructed from const spans
  static_assert(!std::is_constructible_v<span<int>, span<const int>>);

  int arr[] = {1, 2, 3};
  constexpr int const_arr[] = {7, 8, 9};

  static_assert(std::is_constructible_v<span<int>, decltype(arr)&>);
  static_assert(!std::is_constructible_v<span<int>, decltype(const_arr)&>);

  static_assert(std::is_constructible_v<span<const int>, decltype(arr)&>);
  static_assert(std::is_constructible_v<span<const int>, decltype(const_arr)&>);
  static_assert(std::is_constructible_v<span<const int>, span<int>>);

  static_assert(std::is_constructible_v<span<int>, std::vector<int>&>);
  static_assert(!std::is_constructible_v<span<int>, const std::vector<int>&>);
  static_assert(!std::is_constructible_v<span<int>, std::vector<int>&&>);

  static_assert(std::is_constructible_v<span<const int>, std::vector<int>&>);
  static_assert(std::is_constructible_v<span<const int>, const std::vector<int>&>);
  // const spans may even be constructed from rvalue ranges
  static_assert(std::is_constructible_v<span<const int>, std::vector<int>&&>);

  EXPECT_THAT(span<const int>(const_arr), ElementsAreArray(const_arr));
  EXPECT_THAT(span<int>(arr), ElementsAreArray(arr));

  static_assert(!std::is_constructible_v<span<const unsigned>, decltype(const_arr)&>);
  static_assert(!std::is_constructible_v<span<const std::byte>, decltype(const_arr)&>);
}

TEST(Span, TemplateArgumentDeduction) {
  int arr[3];
  const int const_arr[] = {1, 2, 3};
  std::vector<int> vec;
  const std::vector<int> const_vec;
  static_assert(std::is_same_v<decltype(span(arr)), span<int>>);
  static_assert(std::is_same_v<decltype(span(vec)), span<int>>);
  static_assert(std::is_same_v<decltype(span(const_arr)), span<const int>>);
  static_assert(std::is_same_v<decltype(span(const_vec)), span<const int>>);
}

TEST(Span, Size) {
  int arr[] = {1, 2, 3};
  EXPECT_EQ(span(arr).size(), 3);
  EXPECT_EQ(span(arr).size_bytes(), sizeof(int) * 3);

  std::vector<int> vec;
  EXPECT_TRUE(span(vec).empty());
  EXPECT_EQ(span(vec).size(), 0);
  EXPECT_EQ(span(vec).size_bytes(), 0);

  vec.resize(999);
  EXPECT_FALSE(span(vec).empty());
  EXPECT_EQ(span(vec).size(), 999);
  EXPECT_EQ(span(vec).size_bytes(), sizeof(int) * 999);
}

TEST(Span, Equality) {
  auto check_eq = [](auto l, auto r) {
    ARROW_SCOPED_TRACE("l = ", l, ", r = ", r);
    EXPECT_TRUE(l == r);
    EXPECT_FALSE(l != r);
  };
  auto check_ne = [](auto l, auto r) {
    ARROW_SCOPED_TRACE("l = ", l, ", r = ", r);
    EXPECT_TRUE(l != r);
    EXPECT_FALSE(l == r);
  };

  {
    // exercise integral branch with memcmp
    check_eq(span<int>(), span<int>());

    int arr[] = {1, 2, 3};
    check_eq(span(arr), span(arr));
    check_eq(span(arr).subspan(1), span(arr).subspan(1));
    check_ne(span(arr).subspan(1), span(arr).subspan(2));

    std::vector<int> vec{1, 2, 3};
    check_eq(span(vec), span(arr));
    check_eq(span(vec).subspan(1), span(arr).subspan(1));

    vec = {2, 3, 4};
    check_ne(span(vec), span(arr));
    check_eq(span(vec).subspan(0, 2), span(arr).subspan(1));

    // 0-sized
    vec = {};
    check_ne(span(vec), span(arr));
    check_eq(span(vec), span(arr).subspan(3));
  }
  {
    // exercise non-integral branch with for loop
    check_eq(span<std::string>(), span<std::string>());

    std::string arr[] = {"a", "b", "c"};
    check_eq(span(arr), span(arr));
    check_eq(span(arr).subspan(1), span(arr).subspan(1));

    std::vector<std::string> vec{"a", "b", "c"};
    check_eq(span(vec), span(arr));
    check_eq(span(vec).subspan(1), span(arr).subspan(1));

    vec = {"b", "c", "d"};
    check_ne(span(vec), span(arr));
    check_eq(span(vec).subspan(0, 2), span(arr).subspan(1));

    // 0-sized
    vec = {};
    check_ne(span(vec), span(arr));
    check_eq(span(vec), span(arr).subspan(3));
  }
}

TEST(Span, SubSpan) {
  int arr[] = {1, 2, 3};
  span s(arr);

  auto ExpectIdentical = [](span<int> l, span<int> r) {
    EXPECT_EQ(l.data(), r.data());
    EXPECT_EQ(l.size(), r.size());
  };

  ExpectIdentical(s.subspan(0), s);
  ExpectIdentical(s.subspan(0, s.size()), s);

  for (size_t offset = 0; offset < s.size(); ++offset) {
    span expected(arr + offset, s.size() - offset);
    ExpectIdentical(s.subspan(offset), expected);
    ExpectIdentical(s.subspan(offset, s.size() * 3), expected);
  }
  EXPECT_TRUE(s.subspan(s.size()).empty());
  EXPECT_TRUE(s.subspan(s.size() * 3).empty());

  for (size_t length = 0; length < s.size(); ++length) {
    span expected(arr, length);
    ExpectIdentical(s.subspan(0, length), expected);
  }

  ExpectIdentical(s.subspan(1, 1), span(arr + 1, 1));
}

TEST(Span, Mutation) {
  size_t arr[] = {9, 9, 9, 9, 9};

  span s(arr);
  for (size_t i = 0; i < s.size(); ++i) {
    s[i] = i * i;
  }

  EXPECT_THAT(arr, ElementsAre(0, 1, 4, 9, 16));

  auto set = [](span<size_t> lhs, size_t rhs) {
    for (size_t& i : lhs) {
      i = rhs;
    }
  };
  set(span(arr), 0);
  set(span(arr).subspan(1), 1);
  set(span(arr).subspan(2, 2), 23);
  EXPECT_THAT(arr, ElementsAre(0, 1, 23, 23, 1));
}

}  // namespace arrow::util
