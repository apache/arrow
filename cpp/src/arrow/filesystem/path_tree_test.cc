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

#include "arrow/filesystem/path_tree.h"

#include <memory>
#include <string>
#include <utility>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/testing/gtest_util.h"

using testing::ContainerEq;

namespace arrow {
namespace fs {

static PathTree PT(FileStats stats) { return PathTree(std::move(stats)); }

static PathTree PT(std::vector<FileStats> stats) {
  auto maybe_forest = PathTree::Make(stats);
  ARROW_EXPECT_OK(maybe_forest.status());
  auto forest = std::move(maybe_forest).ValueOrDie();
  EXPECT_EQ(forest.size(), 1);
  return std::move(forest[0]);
}

void AssertMakePathTree(std::vector<FileStats> stats, PathForest expected) {
  ASSERT_OK_AND_ASSIGN(PathForest actual, PathTree::Make(stats));
  EXPECT_THAT(actual, ContainerEq(expected));
}

void AssertSubtreesAre(PathTree tree, PathForest expected_subtrees) {
  EXPECT_THAT(tree.subtrees(), ContainerEq(expected_subtrees));
}

TEST(TestPathTree, Basic) {
  AssertMakePathTree({}, {});

  AssertMakePathTree({File("aa")}, {PT(File("aa"))});
  AssertMakePathTree({Dir("AA")}, {PT(Dir("AA"))});
  AssertMakePathTree({Dir("AA"), File("AA/aa")}, {PT({Dir("AA"), File("AA/aa")})});

  // Missing parent can still find ancestor.
  AssertMakePathTree({Dir("AA"), File("AA/BB/bb")}, {PT({Dir("AA"), File("AA/BB/bb")})});

  // Ancestors should link to parent regardless of ordering.
  AssertMakePathTree({File("AA/aa"), Dir("AA")}, {PT({Dir("AA"), File("AA/aa")})});

  // Multiple roots are supported.
  AssertMakePathTree({File("aa"), File("bb")}, {PT(File("aa")), PT(File("bb"))});
  AssertMakePathTree(
      {File("00"), Dir("AA"), File("AA/aa"), File("BB/bb")},
      {PT({File("00")}), PT({Dir("AA"), File("AA/aa")}), PT({File("BB/bb")})});
}

TEST(TestPathTree, Subtrees) {
  // one subtree containing each child of the root directory
  AssertSubtreesAre(PT({Dir("AA"), File("AA/aa"), File("AA/bb"), File("AA/cc")}),
                    {PT(File("AA/aa")), PT(File("AA/bb")), PT(File("AA/cc"))});

  // one subtree rooted on the only child of the root directory
  AssertSubtreesAre(PT({Dir("AA"), Dir("AA/aa"), Dir("AA/aa/00"), File("AA/aa/00/bb")}),
                    {PT({Dir("AA/aa"), Dir("AA/aa/00"), File("AA/aa/00/bb")})});

  // multiple subtrees
  AssertSubtreesAre(PT({Dir("AA"), Dir("AA/aa"), File("AA/aa/0"), File("AA/bb"),
                        Dir("AA/cc"), File("AA/cc/0")}),
                    {PT({Dir("AA/aa"), File("AA/aa/0")}), PT({File("AA/bb")}),
                     PT({Dir("AA/cc"), File("AA/cc/0")})});
}

TEST(TestPathTree, HourlyETL) {
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

  auto join = [](const FileStats& dir, const std::string& basename) {
    return internal::JoinAbstractPath(std::vector<std::string>{dir.path(), basename});
  };

  std::vector<FileStats> stats;

  for (int64_t year = 0; year < kYears; year++) {
    auto year_dir = Dir(std::to_string(year + 2000));
    stats.push_back(year_dir);

    for (int64_t month = 0; month < kMonthsPerYear; month++) {
      auto month_dir = Dir(join(year_dir, numbers[month + 1]));
      stats.push_back(month_dir);

      for (int64_t day = 0; day < kDaysPerMonth; day++) {
        auto day_dir = Dir(join(month_dir, numbers[day + 1]));
        stats.push_back(day_dir);

        for (int64_t hour = 0; hour < kHoursPerDay; hour++) {
          auto hour_dir = Dir(join(day_dir, numbers[hour]));
          stats.push_back(hour_dir);

          for (int64_t file = 0; file < kFilesPerHour; file++) {
            auto file_fd = File(join(hour_dir, numbers[file] + ".parquet"));
            stats.push_back(file_fd);
          }
        }
      }
    }
  }

  ASSERT_OK_AND_ASSIGN(auto forest, PathTree::Make(stats));
  int64_t year = 0;
  for (const auto& year_tree : forest) {
    auto year_dir = Dir(std::to_string(year + 2000));
    EXPECT_EQ(year_tree.stats(), year_dir);

    int64_t month = 0;
    for (const auto& month_tree : year_tree.subtrees()) {
      auto month_dir = Dir(join(year_dir, numbers[month + 1]));
      EXPECT_EQ(month_tree.stats(), month_dir);

      int64_t day = 0;
      for (const auto& day_tree : month_tree.subtrees()) {
        auto day_dir = Dir(join(month_dir, numbers[day + 1]));
        EXPECT_EQ(day_tree.stats(), day_dir);

        int64_t hour = 0;
        for (const auto& hour_tree : day_tree.subtrees()) {
          auto hour_dir = Dir(join(day_dir, numbers[hour]));
          EXPECT_EQ(hour_tree.stats(), hour_dir);

          int64_t file = 0;
          for (const auto& file_tree : hour_tree.subtrees()) {
            auto file_fd = File(join(hour_dir, numbers[file] + ".parquet"));
            EXPECT_EQ(file_tree.stats(), file_fd);
            ++file;
          }
          ++hour;
        }
        ++day;
      }
      ++month;
    }
    ++year;
  }
}

TEST(TestPathTree, Visit) {
  auto tree = PT({Dir("A"), File("A/a")});

  // Should propagate failure
  auto visit_noop = [](const FileStats&) { return Status::OK(); };
  ASSERT_OK(tree.Visit(visit_noop));
  auto visit_fail = [](const FileStats&) { return Status::Invalid(""); };
  ASSERT_RAISES(Invalid, tree.Visit(visit_fail));

  std::vector<FileStats> collection;
  auto visit_collect = [&collection](const FileStats& f) {
    collection.push_back(f);
    return Status::OK();
  };
  ASSERT_OK(tree.Visit(visit_collect));

  // Ensure basic visit of all nodes
  EXPECT_THAT(collection, ContainerEq(std::vector<FileStats>{Dir("A"), File("A/a")}));

  // Matcher should be evaluated on all nodes
  collection = {};
  auto visit_collect_directories =
      [&collection](const FileStats& f) -> PathTree::MaybePrune {
    if (!f.IsDirectory()) {
      return PathTree::Prune;
    }
    collection.push_back(f);
    return PathTree::Continue;
  };
  ASSERT_OK(tree.Visit(visit_collect_directories));
  EXPECT_THAT(collection, ContainerEq(std::vector<FileStats>{Dir("A")}));
}

}  // namespace fs
}  // namespace arrow
