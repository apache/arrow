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
#include <limits>
#include <string>
#include <type_traits>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/small_vector.h"

using testing::ElementsAre;

namespace arrow {
namespace internal {

template <typename Vector>
bool UsesStaticStorage(const Vector& v) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(v.data());
  if (p == nullptr) {
    return true;
  }
  const uint8_t* v_start = reinterpret_cast<const uint8_t*>(&v);
  return (p >= v_start && p < v_start + sizeof(v));
}

struct StaticVectorTraits {
  template <typename T, size_t N>
  using VectorType = StaticVector<T, N>;

  static bool CanOverflow() { return false; }

  static constexpr size_t MaxSizeFor(size_t n) { return n; }

  static constexpr size_t TestSizeFor(size_t max_size) { return max_size; }
};

struct SmallVectorTraits {
  template <typename T, size_t N>
  using VectorType = SmallVector<T, N>;

  static bool CanOverflow() { return true; }

  static constexpr size_t MaxSizeFor(size_t n) {
    return std::numeric_limits<size_t>::max();
  }

  static constexpr size_t TestSizeFor(size_t max_size) {
    return max_size > 6 ? max_size / 3 : 2;
  }
};

using VectorTraits = ::testing::Types<StaticVectorTraits, SmallVectorTraits>;

template <typename T, typename I>
struct VectorIntLikeParam {
  using Traits = T;
  using IntLike = I;

  static constexpr bool IsMoveOnly() { return !std::is_copy_constructible<I>::value; }
};

using VectorIntLikeParams =
    ::testing::Types<VectorIntLikeParam<StaticVectorTraits, int>,
                     VectorIntLikeParam<SmallVectorTraits, int>,
                     VectorIntLikeParam<StaticVectorTraits, MoveOnlyDataType>,
                     VectorIntLikeParam<SmallVectorTraits, MoveOnlyDataType>>;

template <typename Param>
class TestSmallStaticVector : public ::testing::Test {
  template <bool B, typename T = void>
  using enable_if_t = typename std::enable_if<B, T>::type;

  template <typename P>
  using enable_if_move_only = enable_if_t<P::IsMoveOnly(), int>;

  template <typename P>
  using enable_if_not_move_only = enable_if_t<!P::IsMoveOnly(), int>;

 public:
  using Traits = typename Param::Traits;
  using IntLike = typename Param::IntLike;

  template <typename T, size_t N>
  using VectorType = typename Traits::template VectorType<T, N>;

  template <size_t N>
  using IntVectorType = VectorType<IntLike, N>;

  template <size_t N>
  IntVectorType<N> CheckFourValues() {
    IntVectorType<N> ints;
    EXPECT_EQ(ints.size(), 0);
    EXPECT_EQ(ints.capacity(), N);
    EXPECT_EQ(ints.max_size(), Traits::MaxSizeFor(N));
    EXPECT_TRUE(UsesStaticStorage(ints));

    ints.emplace_back(3);
    ints.emplace_back(42);
    EXPECT_EQ(ints.size(), 2);
    EXPECT_EQ(ints.capacity(), N);
    EXPECT_EQ(ints[0], 3);
    EXPECT_EQ(ints[1], 42);
    EXPECT_TRUE(UsesStaticStorage(ints));

    ints.push_back(IntLike(5));
    ints.emplace_back(false);
    EXPECT_EQ(ints.size(), 4);
    EXPECT_EQ(ints[2], 5);
    EXPECT_EQ(ints[3], 0);

    ints[3] = IntLike(8);
    EXPECT_EQ(ints[3], 8);
    EXPECT_EQ(ints.back(), 8);
    ints.front() = IntLike(-1);
    EXPECT_EQ(ints[0], -1);
    EXPECT_EQ(ints.front(), -1);

    return ints;
  }

  void TestBasics() {
    constexpr size_t N = Traits::TestSizeFor(4);
    const auto ints = CheckFourValues<N>();
    EXPECT_EQ(UsesStaticStorage(ints), !Traits::CanOverflow());
  }

  void TestAlwaysStatic() {
    const auto ints = CheckFourValues<4>();
    EXPECT_TRUE(UsesStaticStorage(ints));
  }

  template <size_t N>
  void CheckReserve(size_t max_size, bool expect_overflow) {
    IntVectorType<N> ints;
    ints.emplace_back(123);

    size_t orig_capacity = ints.capacity();

    ints.reserve(max_size / 3);
    ASSERT_EQ(ints.capacity(), std::max(max_size / 3, orig_capacity));
    ASSERT_EQ(ints.size(), 1);
    ASSERT_EQ(ints[0], 123);

    ints.reserve(4 * max_size / 5);
    ASSERT_EQ(ints.capacity(), std::max(4 * max_size / 5, orig_capacity));
    ASSERT_EQ(ints.size(), 1);
    ASSERT_EQ(ints[0], 123);
    ASSERT_EQ(UsesStaticStorage(ints), !expect_overflow);

    size_t old_capacity = ints.capacity();
    ints.reserve(max_size / 5);  // no-op
    ASSERT_EQ(ints.capacity(), old_capacity);
    ASSERT_EQ(ints.size(), 1);
    ASSERT_EQ(ints[0], 123);

    ints.reserve(1);  // no-op
    ASSERT_EQ(ints.capacity(), old_capacity);
    ASSERT_EQ(ints.size(), 1);
    ASSERT_EQ(ints[0], 123);
  }

  void TestReserve() {
    CheckReserve<Traits::TestSizeFor(12)>(12, /*expect_overflow=*/Traits::CanOverflow());
    CheckReserve<12>(12, /*expect_overflow=*/false);
  }

  template <size_t N>
  void CheckClear(bool expect_overflow) {
    IntVectorType<N> ints;
    for (int v : {5, 6, 7, 8, 9}) {
      ints.emplace_back(v);
    }
    ASSERT_EQ(ints.size(), 5);
    size_t capacity = ints.capacity();

    ints.clear();
    ASSERT_EQ(ints.size(), 0);
    ASSERT_EQ(ints.capacity(), capacity);
    ASSERT_EQ(UsesStaticStorage(ints), !expect_overflow);
  }

  void TestClear() {
    CheckClear<Traits::TestSizeFor(5)>(/*expect_overflow=*/Traits::CanOverflow());
    CheckClear<6>(/*expect_overflow=*/false);
  }

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void TestConstructWithCount(enable_if_t<!IsMoveOnly>* = 0) {
    constexpr size_t N = Traits::TestSizeFor(4);
    {
      const IntVectorType<N> ints(3);
      ASSERT_EQ(ints.size(), 3);
      ASSERT_EQ(ints.capacity(), std::max<size_t>(N, 3));
      for (int i = 0; i < 3; ++i) {
        ASSERT_EQ(ints[i], 0);
      }
      EXPECT_THAT(ints, ElementsAre(0, 0, 0));
    }
  }

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void TestConstructWithCount(enable_if_t<IsMoveOnly>* = 0) {
    GTEST_SKIP() << "Cannot construct vector of move-only type with value count";
  }

  template <size_t N>
  void CheckConstructWithValues() {
    {
      const IntVectorType<N> ints{};
      ASSERT_EQ(ints.size(), 0);
      ASSERT_EQ(ints.capacity(), N);
    }
    {
      const IntVectorType<N> ints{IntLike(4), IntLike(5), IntLike(6)};
      ASSERT_EQ(ints.size(), 3);
      ASSERT_EQ(ints.capacity(), std::max<size_t>(N, 3));
      ASSERT_EQ(ints[0], 4);
      ASSERT_EQ(ints[1], 5);
      ASSERT_EQ(ints[2], 6);
      ASSERT_EQ(ints.front(), 4);
      ASSERT_EQ(ints.back(), 6);
    }
  }

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void TestConstructWithValues(enable_if_t<!IsMoveOnly>* = 0) {
    CheckConstructWithValues<Traits::TestSizeFor(4)>();
    CheckConstructWithValues<5>();
  }

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void TestConstructWithValues(enable_if_t<IsMoveOnly>* = 0) {
    GTEST_SKIP() << "Cannot construct vector of move-only type with explicit values";
  }

  template <size_t N>
  void CheckMove(bool expect_overflow) {
    IntVectorType<N> ints;
    ints.emplace_back(4);
    ints.emplace_back(5);
    ints.emplace_back(6);
    ints.emplace_back(7);
    ints.emplace_back(8);

    IntVectorType<N> moved_ints(std::move(ints));
    ASSERT_EQ(moved_ints.size(), 5);
    ASSERT_EQ(ints.size(), 0);
    EXPECT_THAT(moved_ints, ElementsAre(4, 5, 6, 7, 8));
    ASSERT_EQ(UsesStaticStorage(moved_ints), !expect_overflow);

    IntVectorType<N> moved_moved_ints = std::move(moved_ints);
    ASSERT_EQ(moved_moved_ints.size(), 5);
    ASSERT_EQ(moved_ints.size(), 0);
    EXPECT_THAT(moved_moved_ints, ElementsAre(4, 5, 6, 7, 8));

    // Move into itself
    moved_moved_ints = std::move(moved_moved_ints);
    ASSERT_EQ(moved_moved_ints.size(), 5);
    EXPECT_THAT(moved_moved_ints, ElementsAre(4, 5, 6, 7, 8));
  }

  void TestMove() {
    CheckMove<Traits::TestSizeFor(5)>(/*expect_overflow=*/Traits::CanOverflow());
    CheckMove<5>(/*expect_overflow=*/false);
  }

  template <size_t N>
  void CheckCopy(bool expect_overflow) {
    IntVectorType<N> ints;
    ints.emplace_back(4);
    ints.emplace_back(5);
    ints.emplace_back(6);
    ints.emplace_back(7);
    ints.emplace_back(8);

    IntVectorType<N> copied_ints(ints);
    ASSERT_EQ(copied_ints.size(), 5);
    ASSERT_EQ(ints.size(), 5);
    EXPECT_THAT(copied_ints, ElementsAre(4, 5, 6, 7, 8));
    EXPECT_THAT(ints, ElementsAre(4, 5, 6, 7, 8));
    ASSERT_EQ(UsesStaticStorage(copied_ints), !expect_overflow);

    IntVectorType<N> copied_copied_ints = copied_ints;
    ASSERT_EQ(copied_copied_ints.size(), 5);
    ASSERT_EQ(copied_ints.size(), 5);
    EXPECT_THAT(copied_copied_ints, ElementsAre(4, 5, 6, 7, 8));
    EXPECT_THAT(copied_ints, ElementsAre(4, 5, 6, 7, 8));

    auto copy_into = [](const IntVectorType<N>& src, IntVectorType<N>* dest) {
      *dest = src;
    };

    // Copy into itself
    // (avoiding the trivial form `copied_copied_ints = copied_copied_ints`
    //  that would produce a clang warning)
    copy_into(copied_copied_ints, &copied_copied_ints);
    ASSERT_EQ(copied_copied_ints.size(), 5);
    EXPECT_THAT(copied_copied_ints, ElementsAre(4, 5, 6, 7, 8));
  }

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void TestCopy(enable_if_t<!IsMoveOnly>* = 0) {
    CheckCopy<Traits::TestSizeFor(5)>(/*expect_overflow=*/Traits::CanOverflow());
    CheckCopy<5>(/*expect_overflow=*/false);
  }

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void TestCopy(enable_if_t<IsMoveOnly>* = 0) {
    GTEST_SKIP() << "Cannot copy vector of move-only type";
  }

  template <size_t N>
  void CheckSort() {
    IntVectorType<N> ints;
    for (int v : {42, 2, 123, -5, 6, 12, 8, 13}) {
      ints.emplace_back(v);
    }
    std::sort(ints.begin(), ints.end());
    EXPECT_THAT(ints, ElementsAre(-5, 2, 6, 8, 12, 13, 42, 123));
  }

  void TestSort() {
    CheckSort<Traits::TestSizeFor(8)>();
    CheckSort<8>();
  }

  void TestIterators() {
    constexpr size_t N = Traits::TestSizeFor(5);
    IntVectorType<N> ints;
    ASSERT_EQ(ints.begin(), ints.end());

    for (int v : {5, 6, 7, 8, 42}) {
      ints.emplace_back(v);
    }
    auto it = ints.begin();
    ASSERT_NE(it, ints.end());
    ASSERT_EQ(*it++, 5);
    ASSERT_EQ(ints.end() - it, 4);

    auto it2 = ++it;
    ASSERT_EQ(*it, 7);
    ASSERT_EQ(*it2, 7);
    ASSERT_EQ(it, it2);

    ASSERT_EQ(ints.end() - it, 3);
    ASSERT_EQ(*it--, 7);
    ASSERT_NE(it, it2);

    ASSERT_EQ(ints.end() - it, 4);
    ASSERT_NE(it, ints.end());
    ASSERT_EQ(*--it, 5);
    ASSERT_EQ(*it, 5);
    ASSERT_EQ(ints.end() - it, 5);
    it += 4;
    ASSERT_EQ(*it, 42);
    ASSERT_EQ(ints.end() - it, 1);
    ASSERT_NE(it, ints.end());
    ASSERT_EQ(*(it - 3), 6);
    ASSERT_EQ(++it, ints.end());
  }

  void TestConstIterators() {
    constexpr size_t N = Traits::TestSizeFor(5);
    {
      const IntVectorType<N> ints{};
      ASSERT_EQ(ints.begin(), ints.end());
    }
    {
      IntVectorType<N> underlying_ints;
      for (int v : {5, 6, 7, 8, 42}) {
        underlying_ints.emplace_back(v);
      }
      const IntVectorType<N>& ints = underlying_ints;
      auto it = ints.begin();
      ASSERT_NE(it, ints.end());
      ASSERT_EQ(*it++, 5);
      auto it2 = it++;
      ASSERT_EQ(*it2, 6);
      ASSERT_EQ(*it, 7);
      ASSERT_NE(it, it2);
      ASSERT_EQ(*++it2, 7);
      ASSERT_EQ(it, it2);
    }
  }
};

TYPED_TEST_SUITE(TestSmallStaticVector, VectorIntLikeParams);

TYPED_TEST(TestSmallStaticVector, Basics) { this->TestBasics(); }

TYPED_TEST(TestSmallStaticVector, AlwaysStatic) { this->TestAlwaysStatic(); }

TYPED_TEST(TestSmallStaticVector, Reserve) { this->TestReserve(); }

TYPED_TEST(TestSmallStaticVector, Clear) { this->TestClear(); }

TYPED_TEST(TestSmallStaticVector, ConstructWithCount) { this->TestConstructWithCount(); }

TYPED_TEST(TestSmallStaticVector, ConstructWithValues) {
  this->TestConstructWithValues();
}

TYPED_TEST(TestSmallStaticVector, Move) { this->TestMove(); }

TYPED_TEST(TestSmallStaticVector, Copy) { this->TestCopy(); }

TYPED_TEST(TestSmallStaticVector, Sort) { this->TestSort(); }

TYPED_TEST(TestSmallStaticVector, Iterators) { this->TestIterators(); }

TYPED_TEST(TestSmallStaticVector, ConstIterators) { this->TestConstIterators(); }

TEST(StaticVector, Traits) {
  ASSERT_TRUE((std::is_trivially_destructible<StaticVector<int, 4>>::value));
  ASSERT_FALSE((std::is_trivially_destructible<StaticVector<std::string, 4>>::value));
}

TEST(SmallVector, Traits) {
  ASSERT_FALSE((std::is_trivially_destructible<SmallVector<int, 4>>::value));
}

}  // namespace internal
}  // namespace arrow
