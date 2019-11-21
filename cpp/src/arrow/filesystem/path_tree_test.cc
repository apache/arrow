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

static std::shared_ptr<PathTree> PT(FileStats stats) {
  return std::make_shared<PathTree>(std::move(stats));
}

static std::shared_ptr<PathTree> PT(FileStats stats,
                                    std::vector<std::shared_ptr<PathTree>> subtrees) {
  return std::make_shared<PathTree>(std::move(stats), std::move(subtrees));
}

void AssertMakePathTree(std::vector<FileStats> stats,
                        std::vector<std::shared_ptr<PathTree>> expected) {
  std::vector<std::shared_ptr<PathTree>> actual;

  ASSERT_OK(PathTree::Make(stats, &actual));
  EXPECT_THAT(actual, ContainerEq(expected));
}

TEST(TestPathTree, Basic) {
  AssertMakePathTree({}, {});

  AssertMakePathTree({File("aa")}, {PT(File("aa"))});
  AssertMakePathTree({Dir("AA")}, {PT(Dir("AA"))});
  AssertMakePathTree({Dir("AA"), File("AA/aa")}, {PT(Dir("AA"), {PT(File("AA/aa"))})});

  // Missing parent can still find ancestor.
  AssertMakePathTree({Dir("AA"), File("AA/BB/bb")},
                     {PT(Dir("AA"), {PT(File("AA/BB/bb"))})});

  // Ancestors should link to parent irregardless of the ordering
  AssertMakePathTree({File("AA/aa"), Dir("AA")}, {PT(Dir("AA"), {PT(File("AA/aa"))})});

  // Multiple roots are supported.
  AssertMakePathTree({File("aa"), File("bb")}, {PT(File("aa")), PT(File("bb"))});
  AssertMakePathTree(
      {File("00"), Dir("AA"), File("AA/aa"), File("BB/bb")},
      {PT(File("00")), PT(Dir("AA"), {PT(File("AA/aa"))}), PT(File("BB/bb"))});
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
  }

  auto join = [](const std::vector<std::string>& path) {
    return internal::JoinAbstractPath(path);
  };

  std::vector<FileStats> stats;

  PathForest forest;
  for (int64_t year = 0; year < kYears; year++) {
    auto year_str = std::to_string(year + 2000);
    auto year_dir = Dir(year_str);
    stats.push_back(year_dir);

    PathForest months;
    for (int64_t month = 0; month < kMonthsPerYear; month++) {
      auto month_str = join({year_str, numbers[month + 1]});
      auto month_dir = Dir(month_str);
      stats.push_back(month_dir);

      PathForest days;
      for (int64_t day = 0; day < kDaysPerMonth; day++) {
        auto day_str = join({month_str, numbers[day + 1]});
        auto day_dir = Dir(day_str);
        stats.push_back(day_dir);

        PathForest hours;
        for (int64_t hour = 0; hour < kHoursPerDay; hour++) {
          auto hour_str = join({day_str, numbers[hour]});
          auto hour_dir = Dir(hour_str);
          stats.push_back(hour_dir);

          PathForest files;
          for (int64_t file = 0; file < kFilesPerHour; file++) {
            auto file_str = join({hour_str, numbers[file] + ".parquet"});
            auto file_fd = File(file_str);
            stats.push_back(file_fd);
            files.push_back(PT(file_fd));
          }

          auto hour_pt = PT(hour_dir, std::move(files));
          hours.push_back(hour_pt);
        }

        auto day_pt = PT(day_dir, std::move(hours));
        days.push_back(day_pt);
      }

      auto month_pt = PT(month_dir, std::move(days));
      months.push_back(month_pt);
    }

    auto year_pt = PT(year_dir, std::move(months));
    forest.push_back(year_pt);
  }

  AssertMakePathTree(stats, forest);
}

TEST(TestPathTree, Visit) {
  std::shared_ptr<PathTree> tree;
  ASSERT_OK(PathTree::Make({Dir("A"), File("A/a")}, &tree));

  // Should propagate failure
  auto visit_noop = [](const FileStats&) { return Status::OK(); };
  ASSERT_OK(tree->Visit(visit_noop));
  auto visit_fail = [](const FileStats&) { return Status::Invalid(""); };
  ASSERT_RAISES(Invalid, tree->Visit(visit_fail));
  auto match_fail = [](const FileStats&, bool* match) { return Status::Invalid(""); };
  ASSERT_RAISES(Invalid, tree->Visit(visit_noop, match_fail));

  // Ensure basic visit of all nodes
  std::vector<FileStats> collect;
  auto visit = [&collect](const FileStats& f) {
    collect.push_back(f);
    return Status::OK();
  };
  ASSERT_OK(tree->Visit(visit));
  EXPECT_THAT(collect, ContainerEq(std::vector<FileStats>{Dir("A"), File("A/a")}));

  // Matcher should be evaluated on all nodes
  collect.resize(0);
  auto match_dir = [](const FileStats& s, bool* m) {
    *m = s.IsDirectory();
    return Status::OK();
  };
  ASSERT_OK(tree->Visit(visit, match_dir));
  EXPECT_THAT(collect, ContainerEq(std::vector<FileStats>{Dir("A")}));
}

}  // namespace fs
}  // namespace arrow
