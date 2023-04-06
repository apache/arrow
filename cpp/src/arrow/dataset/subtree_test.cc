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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/dataset/forest_internal.h"
#include "arrow/dataset/subtree_internal.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/string.h"

namespace arrow {

using internal::StartsWith;

using compute::field_ref;
using compute::literal;

namespace dataset {

using testing::ContainerEq;

// Tests of subtree pruning

// Don't depend on FileSystem - port just enough to be useful here
struct FileInfo {
  bool is_dir;
  std::string path;

  bool operator==(const FileInfo& other) const {
    return is_dir == other.is_dir && path == other.path;
  }

  static FileInfo Dir(std::string path) { return FileInfo{true, std::move(path)}; }

  static FileInfo File(std::string path) { return FileInfo{false, std::move(path)}; }

  static bool ByPath(const FileInfo& l, const FileInfo& r) { return l.path < r.path; }
};

struct TestPathTree {
  FileInfo info;
  std::vector<TestPathTree> subtrees;

  explicit TestPathTree(std::string file_path)
      : info(FileInfo::File(std::move(file_path))) {}

  TestPathTree(std::string dir_path, std::vector<TestPathTree> subtrees)
      : info(FileInfo::Dir(std::move(dir_path))), subtrees(std::move(subtrees)) {}

  TestPathTree(Forest::Ref ref, const std::vector<FileInfo>& infos) : info(infos[ref.i]) {
    const Forest& forest = *ref.forest;

    int begin = ref.i + 1;
    int end = begin + ref.num_descendants();

    for (int i = begin; i < end; ++i) {
      subtrees.emplace_back(forest[i], infos);
      i += forest[i].num_descendants();
    }
  }

  bool operator==(const TestPathTree& other) const {
    return info == other.info && subtrees == other.subtrees;
  }

  std::string ToString() const {
    auto out = "\n" + info.path;
    if (info.is_dir) out += "/";

    for (const auto& subtree : subtrees) {
      out += subtree.ToString();
    }
    return out;
  }

  friend std::ostream& operator<<(std::ostream& os, const TestPathTree& tree) {
    return os << tree.ToString();
  }
};

using PT = TestPathTree;

std::string_view RemoveTrailingSlash(std::string_view key) {
  while (!key.empty() && key.back() == '/') {
    key.remove_suffix(1);
  }
  return key;
}
bool IsAncestorOf(std::string_view ancestor, std::string_view descendant) {
  // See filesystem/path_util.h
  ancestor = RemoveTrailingSlash(ancestor);
  if (ancestor == "") return true;
  descendant = RemoveTrailingSlash(descendant);
  if (!StartsWith(descendant, ancestor)) return false;
  descendant.remove_prefix(ancestor.size());
  if (descendant.empty()) return true;
  return descendant.front() == '/';
}

Forest MakeForest(std::vector<FileInfo>* infos) {
  std::sort(infos->begin(), infos->end(), FileInfo::ByPath);

  return Forest(static_cast<int>(infos->size()), [&](int i, int j) {
    return IsAncestorOf(infos->at(i).path, infos->at(j).path);
  });
}

void ExpectForestIs(std::vector<FileInfo> infos, std::vector<PT> expected_roots) {
  auto forest = MakeForest(&infos);

  std::vector<PT> actual_roots;
  ASSERT_OK(forest.Visit(
      [&](Forest::Ref ref) -> Result<bool> {
        actual_roots.emplace_back(ref, infos);
        return false;  // only vist roots
      },
      [](Forest::Ref) {}));

  // visit expected and assert equality
  EXPECT_THAT(actual_roots, ContainerEq(expected_roots));
}

TEST(Forest, Basic) {
  ExpectForestIs({}, {});

  ExpectForestIs({FileInfo::File("aa")}, {PT("aa")});
  ExpectForestIs({FileInfo::Dir("AA")}, {PT("AA", {})});
  ExpectForestIs({FileInfo::Dir("AA"), FileInfo::File("AA/aa")},
                 {PT("AA", {PT("AA/aa")})});
  ExpectForestIs({FileInfo::Dir("AA"), FileInfo::Dir("AA/BB"), FileInfo::File("AA/BB/0")},
                 {PT("AA", {PT("AA/BB", {PT("AA/BB/0")})})});

  // Missing parent can still find ancestor.
  ExpectForestIs({FileInfo::Dir("AA"), FileInfo::File("AA/BB/bb")},
                 {PT("AA", {PT("AA/BB/bb")})});

  // Ancestors should link to parent regardless of ordering.
  ExpectForestIs({FileInfo::File("AA/aa"), FileInfo::Dir("AA")},
                 {PT("AA", {PT("AA/aa")})});

  // Multiple roots are supported.
  ExpectForestIs({FileInfo::File("aa"), FileInfo::File("bb")}, {PT("aa"), PT("bb")});
  ExpectForestIs({FileInfo::File("00"), FileInfo::Dir("AA"), FileInfo::File("AA/aa"),
                  FileInfo::File("BB/bb")},
                 {PT("00"), PT("AA", {PT("AA/aa")}), PT("BB/bb")});
  ExpectForestIs({FileInfo::Dir("AA"), FileInfo::Dir("AA/BB"), FileInfo::File("AA/BB/0"),
                  FileInfo::Dir("CC"), FileInfo::Dir("CC/BB"), FileInfo::File("CC/BB/0")},
                 {PT("AA", {PT("AA/BB", {PT("AA/BB/0")})}),
                  PT("CC", {PT("CC/BB", {PT("CC/BB/0")})})});
}

TEST(Forest, HourlyETL) {
  // This test mimics a scenario where an ETL dumps hourly files in a structure
  // `$year/$month/$day/$hour/*.parquet`.
  constexpr int64_t kYears = 3;
  constexpr int64_t kMonthsPerYear = 12;
  constexpr int64_t kDaysPerMonth = 31;
  constexpr int64_t kHoursPerDay = 24;
  constexpr int64_t kFilesPerHour = 2;

  // Avoid constructing strings
  std::vector<std::string> numbers{kDaysPerMonth + 1};
  for (size_t i = 0; i < numbers.size(); i++) {
    numbers[i] = std::to_string(i);
    if (numbers[i].size() == 1) {
      numbers[i] = "0" + numbers[i];
    }
  }

  auto join = [](const std::vector<std::string>& path) {
    if (path.empty()) return std::string("");
    std::string result = path[0];
    for (const auto& part : path) {
      result += '/';
      result += part;
    }
    return result;
  };

  std::vector<FileInfo> infos;

  std::vector<PT> forest;
  for (int64_t year = 0; year < kYears; year++) {
    auto year_str = std::to_string(year + 2000);
    auto year_dir = FileInfo::Dir(year_str);
    infos.push_back(year_dir);

    std::vector<PT> months;
    for (int64_t month = 0; month < kMonthsPerYear; month++) {
      auto month_str = join({year_str, numbers[month + 1]});
      auto month_dir = FileInfo::Dir(month_str);
      infos.push_back(month_dir);

      std::vector<PT> days;
      for (int64_t day = 0; day < kDaysPerMonth; day++) {
        auto day_str = join({month_str, numbers[day + 1]});
        auto day_dir = FileInfo::Dir(day_str);
        infos.push_back(day_dir);

        std::vector<PT> hours;
        for (int64_t hour = 0; hour < kHoursPerDay; hour++) {
          auto hour_str = join({day_str, numbers[hour]});
          auto hour_dir = FileInfo::Dir(hour_str);
          infos.push_back(hour_dir);

          std::vector<PT> files;
          for (int64_t file = 0; file < kFilesPerHour; file++) {
            auto file_str = join({hour_str, numbers[file] + ".parquet"});
            auto file_fd = FileInfo::File(file_str);
            infos.push_back(file_fd);
            files.emplace_back(file_str);
          }

          auto hour_pt = PT(hour_str, std::move(files));
          hours.push_back(hour_pt);
        }

        auto day_pt = PT(day_str, std::move(hours));
        days.push_back(day_pt);
      }

      auto month_pt = PT(month_str, std::move(days));
      months.push_back(month_pt);
    }

    auto year_pt = PT(year_str, std::move(months));
    forest.push_back(year_pt);
  }

  ExpectForestIs(infos, forest);
}

TEST(Forest, Visit) {
  using Infos = std::vector<FileInfo>;

  for (auto infos :
       {Infos{}, Infos{FileInfo::Dir("A"), FileInfo::File("A/a")},
        Infos{FileInfo::Dir("AA"), FileInfo::Dir("AA/BB"), FileInfo::File("AA/BB/0"),
              FileInfo::Dir("CC"), FileInfo::Dir("CC/BB"), FileInfo::File("CC/BB/0")}}) {
    ASSERT_TRUE(std::is_sorted(infos.begin(), infos.end(), FileInfo::ByPath));

    auto forest = MakeForest(&infos);

    auto ignore_post = [](Forest::Ref) {};

    // noop is fine
    ASSERT_OK(
        forest.Visit([](Forest::Ref) -> Result<bool> { return false; }, ignore_post));

    // Should propagate failure
    if (forest.size() != 0) {
      ASSERT_RAISES(
          Invalid,
          forest.Visit([](Forest::Ref) -> Result<bool> { return Status::Invalid(""); },
                       ignore_post));
    }

    // Ensure basic visit of all nodes
    int i = 0;
    ASSERT_OK(forest.Visit(
        [&](Forest::Ref ref) -> Result<bool> {
          EXPECT_EQ(ref.i, i);
          ++i;
          return true;
        },
        ignore_post));

    // Visit only directories
    Infos actual_dirs;
    ASSERT_OK(forest.Visit(
        [&](Forest::Ref ref) -> Result<bool> {
          if (!infos[ref.i].is_dir) {
            return false;
          }
          actual_dirs.push_back(infos[ref.i]);
          return true;
        },
        ignore_post));

    Infos expected_dirs;
    for (const auto& info : infos) {
      if (info.is_dir) {
        expected_dirs.push_back(info);
      }
    }
    EXPECT_THAT(actual_dirs, ContainerEq(expected_dirs));
  }
}

TEST(Subtree, EncodeExpression) {
  SubtreeImpl tree;
  ASSERT_EQ(0, tree.GetOrInsert(equal(field_ref("a"), literal("1"))));
  // Should be idempotent
  ASSERT_EQ(0, tree.GetOrInsert(equal(field_ref("a"), literal("1"))));
  ASSERT_EQ(equal(field_ref("a"), literal("1")), tree.code_to_expr_[0]);

  SubtreeImpl::expression_codes codes;
  auto conj =
      and_(equal(field_ref("a"), literal("1")), equal(field_ref("b"), literal("2")));
  tree.EncodeConjunctionMembers(conj, &codes);
  ASSERT_EQ(SubtreeImpl::expression_codes({0, 1}), codes);

  codes.clear();
  conj = or_(equal(field_ref("a"), literal("1")), equal(field_ref("b"), literal("2")));
  tree.EncodeConjunctionMembers(conj, &codes);
  ASSERT_EQ(SubtreeImpl::expression_codes({2}), codes);
}

TEST(Subtree, GetSubtreeExpression) {
  SubtreeImpl tree;
  const auto expr_a = equal(field_ref("a"), literal("1"));
  const auto expr_b = equal(field_ref("b"), literal("2"));
  const auto code_a = tree.GetOrInsert(expr_a);
  const auto code_b = tree.GetOrInsert(expr_b);
  ASSERT_EQ(expr_a,
            tree.GetSubtreeExpression(SubtreeImpl::Encoded{std::nullopt, {code_a}}));
  ASSERT_EQ(expr_b, tree.GetSubtreeExpression(
                        SubtreeImpl::Encoded{std::nullopt, {code_a, code_b}}));
}

class FakeFragment {
 public:
  explicit FakeFragment(Expression partition_expression)
      : partition_expression_(partition_expression) {}
  const Expression& partition_expression() const { return partition_expression_; }

 private:
  Expression partition_expression_;
};

TEST(Subtree, EncodeFragments) {
  const auto expr_a =
      and_(equal(field_ref("a"), literal("1")), equal(field_ref("b"), literal("2")));
  const auto expr_b =
      and_(equal(field_ref("a"), literal("2")), equal(field_ref("b"), literal("3")));
  std::vector<std::shared_ptr<FakeFragment>> fragments;
  fragments.push_back(std::make_shared<FakeFragment>(expr_a));
  fragments.push_back(std::make_shared<FakeFragment>(expr_b));

  SubtreeImpl tree;
  auto encoded = tree.EncodeGuarantees(
      [&](int index) { return fragments[index]->partition_expression(); },
      static_cast<int>(fragments.size()));
  EXPECT_THAT(
      tree.code_to_expr_,
      ContainerEq(std::vector<compute::Expression>{
          equal(field_ref("a"), literal("1")), equal(field_ref("b"), literal("2")),
          equal(field_ref("a"), literal("2")), equal(field_ref("b"), literal("3"))}));
  EXPECT_THAT(
      encoded,
      testing::UnorderedElementsAreArray({
          SubtreeImpl::Encoded{std::make_optional<int>(0),
                               SubtreeImpl::expression_codes({0, 1})},
          SubtreeImpl::Encoded{std::make_optional<int>(1),
                               SubtreeImpl::expression_codes({2, 3})},
          SubtreeImpl::Encoded{std::nullopt, SubtreeImpl::expression_codes({0})},
          SubtreeImpl::Encoded{std::nullopt, SubtreeImpl::expression_codes({2})},
          SubtreeImpl::Encoded{std::nullopt, SubtreeImpl::expression_codes({0, 1})},
          SubtreeImpl::Encoded{std::nullopt, SubtreeImpl::expression_codes({2, 3})},
      }));
}
}  // namespace dataset
}  // namespace arrow
