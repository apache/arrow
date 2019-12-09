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

#include "arrow/filesystem/path_forest.h"

#include <algorithm>
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

struct TestPathTree;
bool operator==(const std::vector<PathForest::Ref>& roots,
                const std::vector<TestPathTree>& test_trees);

struct TestPathTree {
  FileStats stats;
  std::vector<TestPathTree> subtrees;

  explicit TestPathTree(std::string file_path) : stats(File(std::move(file_path))) {}

  TestPathTree(std::string dir_path, std::vector<TestPathTree> subtrees)
      : stats(Dir(std::move(dir_path))), subtrees(std::move(subtrees)) {}

  template <typename Visitor>
  void Visit(const Visitor& visitor) const {
    visitor(stats);
    for (const auto& tree : subtrees) {
      tree.Visit(visitor);
    }
  }

  friend std::ostream& operator<<(std::ostream& os, const TestPathTree& tree) {
    os << "TestPathTree:";

    auto visitor = [&](const fs::FileStats& stats) {
      auto segments = fs::internal::SplitAbstractPath(stats.path());
      os << "\n" << stats.path();
    };

    tree.Visit(visitor);
    return os;
  }

  friend bool operator==(PathForest::Ref actual, const TestPathTree& expected) {
    if (actual.stats() != expected.stats) {
      return false;
    }

    return actual.descendants().roots() == expected.subtrees;
  }
};

bool operator==(const std::vector<PathForest::Ref>& roots,
                const std::vector<TestPathTree>& test_trees) {
  return roots.size() == test_trees.size() &&
         std::equal(roots.begin(), roots.end(), test_trees.begin());
}

using PT = TestPathTree;

void AssertMakePathTree(std::vector<FileStats> stats, std::vector<PT> expected) {
  ASSERT_OK_AND_ASSIGN(auto forest, PathForest::Make(stats));
  EXPECT_EQ(forest.roots(), expected);
  ASSERT_OK(forest.Visit([](PathForest::Ref ref) {
    auto p = ref.parent();
    if (p.forest == nullptr) {
      return Status::OK();
    }

    EXPECT_GE(p.i + p.num_descendants(), ref.i);
    return Status::OK();
  }));
}

TEST(TestPathForest, Basic) {
  AssertMakePathTree({}, {});

  AssertMakePathTree({File("aa")}, {PT("aa")});
  AssertMakePathTree({Dir("AA")}, {PT("AA", {})});
  AssertMakePathTree({Dir("AA"), File("AA/aa")}, {PT("AA", {PT("AA/aa")})});
  AssertMakePathTree({Dir("AA"), Dir("AA/BB"), File("AA/BB/0")},
                     {PT("AA", {PT("AA/BB", {PT("AA/BB/0")})})});

  // Missing parent can still find ancestor.
  AssertMakePathTree({Dir("AA"), File("AA/BB/bb")}, {PT("AA", {PT("AA/BB/bb")})});

  // Ancestors should link to parent regardless of ordering.
  AssertMakePathTree({File("AA/aa"), Dir("AA")}, {PT("AA", {PT("AA/aa")})});

  // Multiple roots are supported.
  AssertMakePathTree({File("aa"), File("bb")}, {PT("aa"), PT("bb")});
  AssertMakePathTree({File("00"), Dir("AA"), File("AA/aa"), File("BB/bb")},
                     {PT("00"), PT("AA", {PT("AA/aa")}), PT("BB/bb")});
  AssertMakePathTree({Dir("AA"), Dir("AA/BB"), File("AA/BB/0"), Dir("CC"), Dir("CC/BB"),
                      File("CC/BB/0")},
                     {PT("AA", {PT("AA/BB", {PT("AA/BB/0")})}),
                      PT("CC", {PT("CC/BB", {PT("CC/BB/0")})})});
}

TEST(TestPathForest, AssociatedObjects) {
  std::vector<FileStats> stats = {File("aa/1"), File("bb/2"), File("aa/3"), File("bb/4")};
  std::vector<std::string> dirnames = {"aa", "bb", "aa", "bb"};
  std::vector<std::string> basenames = {"1", "2", "3", "4"};

  ASSERT_OK_AND_ASSIGN(auto forest, PathForest::Make(stats, &dirnames, &basenames));

  ASSERT_OK(forest.Visit([&](PathForest::Ref ref) {
    EXPECT_THAT(ref.stats().path(), ::testing::StartsWith(dirnames[ref.i]));
    EXPECT_THAT(ref.stats().path(), ::testing::EndsWith(basenames[ref.i]));
    return Status::OK();
  }));
}

TEST(TestPathForest, HourlyETL) {
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
    return internal::JoinAbstractPath(path);
  };

  std::vector<FileStats> stats;

  std::vector<PT> forest;
  for (int64_t year = 0; year < kYears; year++) {
    auto year_str = std::to_string(year + 2000);
    auto year_dir = Dir(year_str);
    stats.push_back(year_dir);

    std::vector<PT> months;
    for (int64_t month = 0; month < kMonthsPerYear; month++) {
      auto month_str = join({year_str, numbers[month + 1]});
      auto month_dir = Dir(month_str);
      stats.push_back(month_dir);

      std::vector<PT> days;
      for (int64_t day = 0; day < kDaysPerMonth; day++) {
        auto day_str = join({month_str, numbers[day + 1]});
        auto day_dir = Dir(day_str);
        stats.push_back(day_dir);

        std::vector<PT> hours;
        for (int64_t hour = 0; hour < kHoursPerDay; hour++) {
          auto hour_str = join({day_str, numbers[hour]});
          auto hour_dir = Dir(hour_str);
          stats.push_back(hour_dir);

          std::vector<PT> files;
          for (int64_t file = 0; file < kFilesPerHour; file++) {
            auto file_str = join({hour_str, numbers[file] + ".parquet"});
            auto file_fd = File(file_str);
            stats.push_back(file_fd);
            files.push_back(PT(file_str));
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

  AssertMakePathTree(stats, forest);
}

TEST(TestPathForest, Visit) {
  ASSERT_OK_AND_ASSIGN(auto forest, PathForest::Make({Dir("A"), File("A/a")}));

  // Should propagate failure
  auto visit_noop = [](PathForest::Ref) { return Status::OK(); };
  ASSERT_OK(forest.Visit(visit_noop));
  auto visit_fail = [](PathForest::Ref) { return Status::Invalid(""); };
  ASSERT_RAISES(Invalid, forest.Visit(visit_fail));

  std::vector<FileStats> collection;
  auto visit_collect = [&collection](PathForest::Ref ref) {
    collection.push_back(ref.stats());
    return Status::OK();
  };
  ASSERT_OK(forest.Visit(visit_collect));

  // Ensure basic visit of all nodes
  EXPECT_THAT(collection, ContainerEq(std::vector<FileStats>{Dir("A"), File("A/a")}));

  collection = {};
  auto visit_collect_directories =
      [&collection](PathForest::Ref ref) -> PathForest::MaybePrune {
    if (!ref.stats().IsDirectory()) {
      return PathForest::Prune;
    }
    collection.push_back(ref.stats());
    return PathForest::Continue;
  };
  ASSERT_OK(forest.Visit(visit_collect_directories));
  EXPECT_THAT(collection, ContainerEq(std::vector<FileStats>{Dir("A")}));
}

}  // namespace fs
}  // namespace arrow
