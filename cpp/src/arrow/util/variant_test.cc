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

#include "arrow/util/variant.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/testing/gtest_compat.h"

namespace arrow {

namespace util {
namespace {

using ::testing::Eq;

template <typename H, typename... T>
void AssertDefaultConstruction() {
  using variant_type = Variant<H, T...>;

  static_assert(std::is_nothrow_default_constructible<variant_type>::value, "");

  variant_type v;
  EXPECT_EQ(v.index(), 0);
  EXPECT_EQ(get<H>(v), H{});
}

TEST(Variant, DefaultConstruction) {
  AssertDefaultConstruction<int>();
  AssertDefaultConstruction<int, std::string>();
  AssertDefaultConstruction<std::string, int>();
  AssertDefaultConstruction<std::unique_ptr<int>>();
  AssertDefaultConstruction<std::vector<int>, int>();
  AssertDefaultConstruction<bool, std::string, std::unique_ptr<int>, void*,
                            std::true_type>();
  AssertDefaultConstruction<std::nullptr_t, std::unique_ptr<int>, void*, bool,
                            std::string, std::true_type>();
}

template <typename V, typename T>
struct AssertCopyConstructionOne {
  void operator()(uint8_t index) {
    V v{member_};
    EXPECT_EQ(v.index(), index);
    EXPECT_EQ(get<T>(v), member_);

    V copy{v};
    EXPECT_EQ(copy.index(), v.index());
    EXPECT_EQ(get<T>(copy), get<T>(v));
    EXPECT_EQ(copy, v);

    V assigned;
    assigned = member_;
    EXPECT_EQ(assigned.index(), index);
    EXPECT_EQ(get<T>(assigned), member_);

    assigned = v;
    EXPECT_EQ(assigned.index(), v.index());
    EXPECT_EQ(get<T>(assigned), get<T>(v));
    EXPECT_EQ(assigned, v);
  }

  const T& member_;
};

template <typename... T>
void AssertCopyConstruction(T... member) {
  uint8_t index = 0;
  for (auto Assert : {std::function<void(uint8_t)>(
           AssertCopyConstructionOne<Variant<T...>, T>{member})...}) {
    Assert(index++);
  }
}

template <typename... T>
void AssertCopyConstructionDisabled() {
  static_assert(!std::is_copy_constructible<Variant<T...>>::value,
                "copy construction was not disabled");
}

TEST(Variant, CopyConstruction) {
  // if any member is not copy constructible then Variant is not copy constructible
  AssertCopyConstructionDisabled<std::unique_ptr<int>>();
  AssertCopyConstructionDisabled<std::unique_ptr<int>, std::string>();
  AssertCopyConstructionDisabled<std::string, int, bool, std::unique_ptr<int>>();

  AssertCopyConstruction(32, std::string("hello"), true);
  AssertCopyConstruction(std::string("world"), false, 53);
  AssertCopyConstruction(nullptr, std::true_type{}, std::string("!"));
  AssertCopyConstruction(std::vector<int>{1, 3, 3, 7}, "C string");

  // copy assignment operator is not used
  struct CopyAssignThrows {
    CopyAssignThrows() = default;
    CopyAssignThrows(const CopyAssignThrows&) = default;

    CopyAssignThrows& operator=(const CopyAssignThrows&) { throw 42; }

    CopyAssignThrows(CopyAssignThrows&&) = default;
    CopyAssignThrows& operator=(CopyAssignThrows&&) = default;

    bool operator==(const CopyAssignThrows&) const { return true; }
  };
  EXPECT_NO_THROW(AssertCopyConstruction(CopyAssignThrows{}));
}

TEST(Variant, Emplace) {
  using variant_type = Variant<std::string, std::vector<int>, int>;
  variant_type v;

  v.emplace<int>();
  EXPECT_EQ(v, variant_type{int{}});

  v.emplace<std::string>("hello");
  EXPECT_EQ(v, variant_type{std::string("hello")});

  v.emplace<std::vector<int>>({1, 3, 3, 7});
  EXPECT_EQ(v, variant_type{std::vector<int>({1, 3, 3, 7})});
}

TEST(Variant, MoveConstruction) {
  struct noop_delete {
    void operator()(...) const {}
  };
  using ptr = std::unique_ptr<int, noop_delete>;
  static_assert(!std::is_copy_constructible<ptr>::value, "");

  using variant_type = Variant<int, ptr>;

  int tag = 42;
  auto ExpectIsTag = [&](const variant_type& v) {
    EXPECT_EQ(v.index(), 1);
    EXPECT_EQ(get<ptr>(v).get(), &tag);
  };

  ptr p;

  // move construction from member
  p.reset(&tag);
  variant_type v0{std::move(p)};
  ExpectIsTag(v0);

  // move assignment from member
  p.reset(&tag);
  v0 = std::move(p);
  ExpectIsTag(v0);

  // move construction from other variant
  variant_type v1{std::move(v0)};
  ExpectIsTag(v1);

  // move assignment from other variant
  p.reset(&tag);
  variant_type v2{std::move(p)};
  v1 = std::move(v2);
  ExpectIsTag(v1);

  // type changing move assignment from member
  variant_type v3;
  EXPECT_NE(v3.index(), 1);
  p.reset(&tag);
  v3 = std::move(p);
  ExpectIsTag(v3);

  // type changing move assignment from other variant
  variant_type v4;
  EXPECT_NE(v4.index(), 1);
  v4 = std::move(v3);
  ExpectIsTag(v4);
}

TEST(Variant, ExceptionSafety) {
  struct {
  } actually_throw;

  struct {
  } dont_throw;

  struct ConstructorThrows {
    explicit ConstructorThrows(decltype(actually_throw)) { throw 42; }
    explicit ConstructorThrows(decltype(dont_throw)) {}

    ConstructorThrows(const ConstructorThrows&) { throw 42; }

    ConstructorThrows& operator=(const ConstructorThrows&) = default;
    ConstructorThrows(ConstructorThrows&&) = default;
    ConstructorThrows& operator=(ConstructorThrows&&) = default;
  };

  Variant<int, ConstructorThrows> v;

  // constructor throws during emplacement
  EXPECT_THROW(v.emplace<ConstructorThrows>(actually_throw), int);
  // safely returned to the default state
  EXPECT_EQ(v.index(), 0);

  // constructor throws during copy assignment from member
  EXPECT_THROW(
      {
        const ConstructorThrows throws(dont_throw);
        v = throws;
      },
      int);
  // safely returned to the default state
  EXPECT_EQ(v.index(), 0);
}

// XXX GTest 1.11 exposes a `using std::visit` in its headers which
// somehow gets preferred to `arrow::util::visit`, even if there is
// a using clause (perhaps because of macros such as EXPECT_EQ).
template <typename... Args>
void DoVisit(Args&&... args) {
  return ::arrow::util::visit(std::forward<Args>(args)...);
}

template <typename T, typename... Args>
void AssertVisitedEquals(const T& expected, Args&&... args) {
  const auto actual = ::arrow::util::visit(std::forward<Args>(args)...);
  EXPECT_EQ(expected, actual);
}

template <typename V, typename T>
struct AssertVisitOne {
  void operator()(const T& actual) { EXPECT_EQ(&actual, expected_); }

  void operator()(T* actual) { EXPECT_EQ(actual, expected_); }

  template <typename U>
  void operator()(const U&) {
    FAIL() << "the expected type was not visited.";
  }

  template <typename U>
  void operator()(U*) {
    FAIL() << "the expected type was not visited.";
  }

  explicit AssertVisitOne(T member) : member_(std::move(member)) {}

  void operator()() {
    V v{member_};
    expected_ = &get<T>(v);
    DoVisit(*this, v);
    DoVisit(*this, &v);
  }

  T member_;
  const T* expected_;
};

// Try visiting all alternatives on a Variant<T...>
template <typename... T>
void AssertVisitAll(T... member) {
  for (auto Assert :
       {std::function<void()>(AssertVisitOne<Variant<T...>, T>{member})...}) {
    Assert();
  }
}

TEST(VariantTest, Visit) {
  AssertVisitAll(32, std::string("hello"), true);
  AssertVisitAll(std::string("world"), false, 53);
  AssertVisitAll(nullptr, std::true_type{}, std::string("!"));
  AssertVisitAll(std::vector<int>{1, 3, 3, 7}, "C string");

  using int_or_string = Variant<int, std::string>;
  int_or_string v;

  // value returning visit:
  struct {
    int_or_string operator()(int i) { return int_or_string{i * 2}; }
    int_or_string operator()(const std::string& s) { return int_or_string{s + s}; }
  } Double;

  v = 7;
  AssertVisitedEquals(int_or_string{14}, Double, v);

  v = "lolol";
  AssertVisitedEquals(int_or_string{"lolollolol"}, Double, v);

  // mutating visit:
  struct {
    void operator()(int* i) { *i *= 2; }
    void operator()(std::string* s) { *s += *s; }
  } DoubleInplace;

  v = 7;
  DoVisit(DoubleInplace, &v);
  EXPECT_EQ(v, int_or_string{14});

  v = "lolol";
  DoVisit(DoubleInplace, &v);
  EXPECT_EQ(v, int_or_string{"lolollolol"});
}

TEST(VariantTest, Equality) {
  using int_or_double = Variant<int, double>;

  auto eq = [](const int_or_double& a, const int_or_double& b) {
    EXPECT_TRUE(a == b);
    EXPECT_FALSE(a != b);
  };
  auto ne = [](const int_or_double& a, const int_or_double& b) {
    EXPECT_TRUE(a != b);
    EXPECT_FALSE(a == b);
  };

  int_or_double u, v;
  u.emplace<int>(1);
  v.emplace<int>(1);
  eq(u, v);
  v.emplace<int>(2);
  ne(u, v);
  v.emplace<double>(1.0);
  ne(u, v);
  u.emplace<double>(1.0);
  eq(u, v);
  u.emplace<double>(2.0);
  ne(u, v);
}

}  // namespace
}  // namespace util
}  // namespace arrow
