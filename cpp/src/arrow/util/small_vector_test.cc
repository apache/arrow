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
#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/small_vector.h"

using testing::ElementsAre;
using testing::ElementsAreArray;

namespace arrow {
namespace internal {

struct HeapInt {
  HeapInt() : HeapInt(0) {}

  explicit HeapInt(int x) : ptr(new int(x)) {}

  HeapInt& operator=(int x) {
    ptr.reset(new int(x));
    return *this;
  }

  HeapInt(const HeapInt& other) : HeapInt(other.ToInt()) {}

  HeapInt& operator=(const HeapInt& other) {
    *this = other.ToInt();
    return *this;
  }

  int ToInt() const { return ptr == nullptr ? -98 : *ptr; }

  bool operator==(const HeapInt& other) const {
    return ptr != nullptr && other.ptr != nullptr && *ptr == *other.ptr;
  }
  bool operator<(const HeapInt& other) const {
    return ptr == nullptr || (other.ptr != nullptr && *ptr < *other.ptr);
  }

  bool operator==(int other) const { return ptr != nullptr && *ptr == other; }
  friend bool operator==(int left, const HeapInt& right) { return right == left; }

  std::unique_ptr<int> ptr;
};

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
                     VectorIntLikeParam<StaticVectorTraits, HeapInt>,
                     VectorIntLikeParam<SmallVectorTraits, HeapInt>,
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
  IntVectorType<N> MakeVector(const std::vector<int>& init_values) {
    IntVectorType<N> ints;
    for (auto v : init_values) {
      ints.emplace_back(v);
    }
    return ints;
  }

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
    IntVectorType<N> ints = MakeVector<N>({5, 6, 7, 8, 9});
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
  void TestConstructFromCount(enable_if_t<!IsMoveOnly>* = 0) {
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
  void TestConstructFromCount(enable_if_t<IsMoveOnly>* = 0) {
    GTEST_SKIP() << "Cannot construct vector of move-only type with value count";
  }

  template <size_t N>
  void CheckConstructFromValues() {
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
  void TestConstructFromValues(enable_if_t<!IsMoveOnly>* = 0) {
    CheckConstructFromValues<Traits::TestSizeFor(4)>();
    CheckConstructFromValues<5>();
  }

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void TestConstructFromValues(enable_if_t<IsMoveOnly>* = 0) {
    GTEST_SKIP() << "Cannot construct vector of move-only type with explicit values";
  }

  void CheckConstructFromMovedStdVector() {
    constexpr size_t N = Traits::TestSizeFor(6);
    {
      std::vector<IntLike> src;
      const IntVectorType<N> ints(std::move(src));
      ASSERT_EQ(ints.size(), 0);
      ASSERT_EQ(ints.capacity(), N);
    }
    {
      std::vector<IntLike> src;
      for (int i = 0; i < 6; ++i) {
        src.emplace_back(i + 4);
      }
      const IntVectorType<N> ints(std::move(src));
      ASSERT_EQ(ints.size(), 6);
      ASSERT_EQ(ints.capacity(), std::max<size_t>(N, 6));
      EXPECT_THAT(ints, ElementsAre(4, 5, 6, 7, 8, 9));
    }
  }

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void CheckConstructFromCopiedStdVector(enable_if_t<!IsMoveOnly>* = 0) {
    constexpr size_t N = Traits::TestSizeFor(6);
    {
      const std::vector<IntLike> src;
      const IntVectorType<N> ints(src);
      ASSERT_EQ(ints.size(), 0);
      ASSERT_EQ(ints.capacity(), N);
    }
    {
      std::vector<IntLike> values;
      for (int i = 0; i < 6; ++i) {
        values.emplace_back(i + 4);
      }
      const auto& src = values;
      const IntVectorType<N> ints(src);
      ASSERT_EQ(ints.size(), 6);
      ASSERT_EQ(ints.capacity(), std::max<size_t>(N, 6));
      EXPECT_THAT(ints, ElementsAre(4, 5, 6, 7, 8, 9));
    }
  }

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void CheckConstructFromCopiedStdVector(enable_if_t<IsMoveOnly>* = 0) {}

  void TestConstructFromStdVector() {
    CheckConstructFromMovedStdVector();
    CheckConstructFromCopiedStdVector();
  }

  void CheckAssignFromMovedStdVector() {
    constexpr size_t N = Traits::TestSizeFor(6);
    {
      std::vector<IntLike> src;
      IntVectorType<N> ints = MakeVector<N>({42});
      ints = std::move(src);
      ASSERT_EQ(ints.size(), 0);
      ASSERT_EQ(ints.capacity(), N);
    }
    {
      std::vector<IntLike> src;
      for (int i = 0; i < 6; ++i) {
        src.emplace_back(i + 4);
      }
      IntVectorType<N> ints = MakeVector<N>({42});
      ints = std::move(src);
      ASSERT_EQ(ints.size(), 6);
      ASSERT_EQ(ints.capacity(), std::max<size_t>(N, 6));
      EXPECT_THAT(ints, ElementsAre(4, 5, 6, 7, 8, 9));
    }
  }

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void CheckAssignFromCopiedStdVector(enable_if_t<!IsMoveOnly>* = 0) {
    constexpr size_t N = Traits::TestSizeFor(6);
    {
      const std::vector<IntLike> src;
      IntVectorType<N> ints = MakeVector<N>({42});
      ints = src;
      ASSERT_EQ(ints.size(), 0);
      ASSERT_EQ(ints.capacity(), N);
    }
    {
      std::vector<IntLike> values;
      for (int i = 0; i < 6; ++i) {
        values.emplace_back(i + 4);
      }
      const auto& src = values;
      IntVectorType<N> ints = MakeVector<N>({42});
      ints = src;
      ASSERT_EQ(ints.size(), 6);
      ASSERT_EQ(ints.capacity(), std::max<size_t>(N, 6));
      EXPECT_THAT(ints, ElementsAre(4, 5, 6, 7, 8, 9));
    }
  }

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void CheckAssignFromCopiedStdVector(enable_if_t<IsMoveOnly>* = 0) {}

  void TestAssignFromStdVector() {
    CheckAssignFromMovedStdVector();
    CheckAssignFromCopiedStdVector();
  }

  template <size_t N>
  void CheckMove(bool expect_overflow) {
    IntVectorType<N> ints = MakeVector<N>({4, 5, 6, 7, 8});

    IntVectorType<N> moved_ints(std::move(ints));
    ASSERT_EQ(moved_ints.size(), 5);
    EXPECT_THAT(moved_ints, ElementsAre(4, 5, 6, 7, 8));
    ASSERT_EQ(UsesStaticStorage(moved_ints), !expect_overflow);
    ASSERT_TRUE(UsesStaticStorage(ints));

    IntVectorType<N> moved_moved_ints = std::move(moved_ints);
    ASSERT_EQ(moved_moved_ints.size(), 5);
    EXPECT_THAT(moved_moved_ints, ElementsAre(4, 5, 6, 7, 8));

#ifndef __MINGW32__
    // Move into itself
    moved_moved_ints = std::move(moved_moved_ints);
    ASSERT_EQ(moved_moved_ints.size(), 5);
    EXPECT_THAT(moved_moved_ints, ElementsAre(4, 5, 6, 7, 8));
#endif
  }

  void TestMove() {
    CheckMove<Traits::TestSizeFor(5)>(/*expect_overflow=*/Traits::CanOverflow());
    CheckMove<5>(/*expect_overflow=*/false);
  }

  template <size_t N>
  void CheckCopy(bool expect_overflow) {
    IntVectorType<N> ints = MakeVector<N>({4, 5, 6, 7, 8});

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

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void TestResize(enable_if_t<!IsMoveOnly>* = 0) {
    constexpr size_t N = Traits::TestSizeFor(8);
    {
      IntVectorType<N> ints;
      ints.resize(2);
      ASSERT_GE(ints.capacity(), 2);
      EXPECT_THAT(ints, ElementsAreArray(std::vector<int>(2, 0)));
      ints.resize(3);
      ASSERT_GE(ints.capacity(), 3);
      EXPECT_THAT(ints, ElementsAreArray(std::vector<int>(3, 0)));
      ints.resize(8);
      ASSERT_GE(ints.capacity(), 8);
      EXPECT_THAT(ints, ElementsAreArray(std::vector<int>(8, 0)));
      ints.resize(6);
      ints.resize(6);  // no-op
      ASSERT_GE(ints.capacity(), 8);
      EXPECT_THAT(ints, ElementsAreArray(std::vector<int>(6, 0)));
      ints.resize(0);
      ASSERT_GE(ints.capacity(), 8);
      EXPECT_THAT(ints, ElementsAreArray(std::vector<int>(0, 0)));
      ints.resize(5);
      ASSERT_GE(ints.capacity(), 8);
      EXPECT_THAT(ints, ElementsAreArray(std::vector<int>(5, 0)));
      ints.resize(7);
      ASSERT_GE(ints.capacity(), 8);
      EXPECT_THAT(ints, ElementsAreArray(std::vector<int>(7, 0)));
    }
    {
      IntVectorType<N> ints;
      ints.resize(2, IntLike(2));
      ASSERT_GE(ints.capacity(), 2);
      EXPECT_THAT(ints, ElementsAre(2, 2));
      ints.resize(3, IntLike(3));
      ASSERT_GE(ints.capacity(), 3);
      EXPECT_THAT(ints, ElementsAre(2, 2, 3));
      ints.resize(8, IntLike(8));
      ASSERT_GE(ints.capacity(), 8);
      EXPECT_THAT(ints, ElementsAre(2, 2, 3, 8, 8, 8, 8, 8));
      ints.resize(6, IntLike(6));
      ints.resize(6, IntLike(6));  // no-op
      ASSERT_GE(ints.capacity(), 8);
      EXPECT_THAT(ints, ElementsAre(2, 2, 3, 8, 8, 8));
      ints.resize(0, IntLike(0));
      ASSERT_GE(ints.capacity(), 8);
      EXPECT_THAT(ints, ElementsAre());
      ints.resize(5, IntLike(5));
      ASSERT_GE(ints.capacity(), 8);
      EXPECT_THAT(ints, ElementsAre(5, 5, 5, 5, 5));
      ints.resize(7, IntLike(7));
      ASSERT_GE(ints.capacity(), 8);
      EXPECT_THAT(ints, ElementsAre(5, 5, 5, 5, 5, 7, 7));
    }
  }

  template <bool IsMoveOnly = Param::IsMoveOnly()>
  void TestResize(enable_if_t<IsMoveOnly>* = 0) {
    GTEST_SKIP() << "Cannot resize vector of move-only type";
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
    {
      // Forward iterators
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
    {
      // Reverse iterators
      IntVectorType<N> ints;
      ASSERT_EQ(ints.rbegin(), ints.rend());

      for (int v : {42, 8, 7, 6, 5}) {
        ints.emplace_back(v);
      }

      auto it = ints.rbegin();
      ASSERT_NE(it, ints.rend());
      ASSERT_EQ(*it++, 5);
      ASSERT_EQ(ints.rend() - it, 4);

      auto it2 = ++it;
      ASSERT_EQ(*it, 7);
      ASSERT_EQ(*it2, 7);
      ASSERT_EQ(it, it2);

      ASSERT_EQ(ints.rend() - it, 3);
      ASSERT_EQ(*it--, 7);
      ASSERT_NE(it, it2);

      ASSERT_EQ(ints.rend() - it, 4);
      ASSERT_NE(it, ints.rend());
      ASSERT_EQ(*--it, 5);
      ASSERT_EQ(*it, 5);
      ASSERT_EQ(ints.rend() - it, 5);
      it += 4;
      ASSERT_EQ(*it, 42);
      ASSERT_EQ(ints.rend() - it, 1);
      ASSERT_NE(it, ints.rend());
      ASSERT_EQ(*(it - 3), 6);
      ASSERT_EQ(++it, ints.rend());
    }
  }

  void TestConstIterators() {
    constexpr size_t N = Traits::TestSizeFor(5);
    {
      const IntVectorType<N> ints{};
      ASSERT_EQ(ints.begin(), ints.end());
      ASSERT_EQ(ints.rbegin(), ints.rend());
    }
    {
      // Forward iterators
      IntVectorType<N> underlying_ints = MakeVector<N>({5, 6, 7, 8, 42});
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

      // Conversion from non-const iterator
      it = underlying_ints.begin() + 1;
      ASSERT_NE(it, underlying_ints.end());
      ASSERT_EQ(*it, 6);
      it += underlying_ints.end() - it;
      ASSERT_EQ(it, underlying_ints.end());
    }
    {
      // Reverse iterators
      IntVectorType<N> underlying_ints = MakeVector<N>({42, 8, 7, 6, 5});
      const IntVectorType<N>& ints = underlying_ints;

      auto it = ints.rbegin();
      ASSERT_NE(it, ints.rend());
      ASSERT_EQ(*it++, 5);
      auto it2 = it++;
      ASSERT_EQ(*it2, 6);
      ASSERT_EQ(*it, 7);
      ASSERT_NE(it, it2);
      ASSERT_EQ(*++it2, 7);
      ASSERT_EQ(it, it2);

      // Conversion from non-const iterator
      it = underlying_ints.rbegin() + 1;
      ASSERT_NE(it, underlying_ints.rend());
      ASSERT_EQ(*it, 6);
      it += underlying_ints.rend() - it;
      ASSERT_EQ(it, underlying_ints.rend());
    }
  }

  void TestInsertIteratorPair() {
    // insert(const_iterator, InputIt first, InputIt last)
    constexpr size_t N = Traits::TestSizeFor(10);
    {
      // empty source and destination
      const std::vector<int> src{};
      IntVectorType<N> ints;
      ints.insert(ints.begin(), src.begin(), src.end());
      ASSERT_EQ(ints.size(), 0);

      ints.emplace_back(42);
      ints.insert(ints.begin(), src.begin(), src.end());
      ints.insert(ints.end(), src.begin(), src.end());
      EXPECT_THAT(ints, ElementsAre(42));
    }
    const std::vector<int> src{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    {
      // insert at start
      IntVectorType<N> ints;
      ints.insert(ints.begin(), src.begin() + 4, src.begin() + 7);
      EXPECT_THAT(ints, ElementsAre(4, 5, 6));
      ints.insert(ints.begin(), src.begin() + 1, src.begin() + 4);
      EXPECT_THAT(ints, ElementsAre(1, 2, 3, 4, 5, 6));
      ints.insert(ints.begin(), src.begin(), src.begin() + 1);
      EXPECT_THAT(ints, ElementsAre(0, 1, 2, 3, 4, 5, 6));
      ints.insert(ints.begin(), src.begin() + 7, src.begin() + 10);
      EXPECT_THAT(ints, ElementsAre(7, 8, 9, 0, 1, 2, 3, 4, 5, 6));
    }
    {
      // insert at end
      IntVectorType<N> ints;
      ints.insert(ints.end(), src.begin() + 4, src.begin() + 7);
      EXPECT_THAT(ints, ElementsAre(4, 5, 6));
      ints.insert(ints.end(), src.begin() + 1, src.begin() + 4);
      EXPECT_THAT(ints, ElementsAre(4, 5, 6, 1, 2, 3));
      ints.insert(ints.end(), src.begin(), src.begin() + 1);
      EXPECT_THAT(ints, ElementsAre(4, 5, 6, 1, 2, 3, 0));
      ints.insert(ints.end(), src.begin() + 7, src.begin() + 10);
      EXPECT_THAT(ints, ElementsAre(4, 5, 6, 1, 2, 3, 0, 7, 8, 9));
    }
    {
      // insert at some point inside
      IntVectorType<N> ints;
      ints.insert(ints.begin(), src.begin() + 4, src.begin() + 7);
      EXPECT_THAT(ints, ElementsAre(4, 5, 6));
      ints.insert(ints.begin() + 2, src.begin() + 1, src.begin() + 4);
      EXPECT_THAT(ints, ElementsAre(4, 5, 1, 2, 3, 6));
      ints.insert(ints.begin() + 2, src.begin(), src.begin() + 1);
      EXPECT_THAT(ints, ElementsAre(4, 5, 0, 1, 2, 3, 6));
      ints.insert(ints.begin() + 2, src.begin() + 7, src.begin() + 10);
      EXPECT_THAT(ints, ElementsAre(4, 5, 7, 8, 9, 0, 1, 2, 3, 6));
    }
    {
      // insert from a std::move_iterator (potentially move-only)
      IntVectorType<N> src = MakeVector<N>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
      IntVectorType<N> ints;
      auto move_it = [&](size_t i) { return std::make_move_iterator(src.begin() + i); };
      ints.insert(ints.begin(), move_it(4), move_it(7));
      EXPECT_THAT(ints, ElementsAre(4, 5, 6));
      ints.insert(ints.begin() + 2, move_it(1), move_it(4));
      EXPECT_THAT(ints, ElementsAre(4, 5, 1, 2, 3, 6));
      ints.insert(ints.begin() + 2, move_it(0), move_it(1));
      EXPECT_THAT(ints, ElementsAre(4, 5, 0, 1, 2, 3, 6));
      ints.insert(ints.begin() + 2, move_it(7), move_it(10));
      EXPECT_THAT(ints, ElementsAre(4, 5, 7, 8, 9, 0, 1, 2, 3, 6));
    }
  }
};

TYPED_TEST_SUITE(TestSmallStaticVector, VectorIntLikeParams);

TYPED_TEST(TestSmallStaticVector, Basics) { this->TestBasics(); }

TYPED_TEST(TestSmallStaticVector, AlwaysStatic) { this->TestAlwaysStatic(); }

TYPED_TEST(TestSmallStaticVector, Reserve) { this->TestReserve(); }

TYPED_TEST(TestSmallStaticVector, Clear) { this->TestClear(); }

TYPED_TEST(TestSmallStaticVector, ConstructFromCount) { this->TestConstructFromCount(); }

TYPED_TEST(TestSmallStaticVector, ConstructFromValues) {
  this->TestConstructFromValues();
}

TYPED_TEST(TestSmallStaticVector, ConstructFromStdVector) {
  this->TestConstructFromStdVector();
}

TYPED_TEST(TestSmallStaticVector, AssignFromStdVector) {
  this->TestAssignFromStdVector();
}

TYPED_TEST(TestSmallStaticVector, Move) { this->TestMove(); }

TYPED_TEST(TestSmallStaticVector, Copy) { this->TestCopy(); }

TYPED_TEST(TestSmallStaticVector, Resize) { this->TestResize(); }

TYPED_TEST(TestSmallStaticVector, Sort) { this->TestSort(); }

TYPED_TEST(TestSmallStaticVector, Iterators) { this->TestIterators(); }

TYPED_TEST(TestSmallStaticVector, ConstIterators) { this->TestConstIterators(); }

TYPED_TEST(TestSmallStaticVector, InsertIteratorPair) { this->TestInsertIteratorPair(); }

TEST(StaticVector, Traits) {
  ASSERT_TRUE((std::is_trivially_destructible<StaticVector<int, 4>>::value));
  ASSERT_FALSE((std::is_trivially_destructible<StaticVector<std::string, 4>>::value));
}

TEST(SmallVector, Traits) {
  ASSERT_FALSE((std::is_trivially_destructible<SmallVector<int, 4>>::value));
}

}  // namespace internal
}  // namespace arrow
