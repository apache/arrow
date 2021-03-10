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
#include "arrow/util/range.h"

using testing::ContainerEq;

namespace arrow {
namespace fs {

struct TestPathTree {
  FileInfo info;
  std::vector<TestPathTree> subtrees;

  explicit TestPathTree(std::string file_path) : info(File(std::move(file_path))) {}

  TestPathTree(std::string dir_path, std::vector<TestPathTree> subtrees)
      : info(Dir(std::move(dir_path))), subtrees(std::move(subtrees)) {}

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
    auto out = "\n" + info.path();
    if (info.IsDirectory()) out += "/";

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

Forest MakeForest(std::vector<FileInfo>* infos) {
  std::sort(infos->begin(), infos->end(), FileInfo::ByPath{});

  return Forest(static_cast<int>(infos->size()), [&](int i, int j) {
    return internal::IsAncestorOf(infos->at(i).path(), infos->at(j).path());
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

  ExpectForestIs({File("aa")}, {PT("aa")});
  ExpectForestIs({Dir("AA")}, {PT("AA", {})});
  ExpectForestIs({Dir("AA"), File("AA/aa")}, {PT("AA", {PT("AA/aa")})});
  ExpectForestIs({Dir("AA"), Dir("AA/BB"), File("AA/BB/0")},
                 {PT("AA", {PT("AA/BB", {PT("AA/BB/0")})})});

  // Missing parent can still find ancestor.
  ExpectForestIs({Dir("AA"), File("AA/BB/bb")}, {PT("AA", {PT("AA/BB/bb")})});

  // Ancestors should link to parent regardless of ordering.
  ExpectForestIs({File("AA/aa"), Dir("AA")}, {PT("AA", {PT("AA/aa")})});

  // Multiple roots are supported.
  ExpectForestIs({File("aa"), File("bb")}, {PT("aa"), PT("bb")});
  ExpectForestIs({File("00"), Dir("AA"), File("AA/aa"), File("BB/bb")},
                 {PT("00"), PT("AA", {PT("AA/aa")}), PT("BB/bb")});
  ExpectForestIs({Dir("AA"), Dir("AA/BB"), File("AA/BB/0"), Dir("CC"), Dir("CC/BB"),
                  File("CC/BB/0")},
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
    return internal::JoinAbstractPath(path);
  };

  std::vector<FileInfo> infos;

  std::vector<PT> forest;
  for (int64_t year = 0; year < kYears; year++) {
    auto year_str = std::to_string(year + 2000);
    auto year_dir = Dir(year_str);
    infos.push_back(year_dir);

    std::vector<PT> months;
    for (int64_t month = 0; month < kMonthsPerYear; month++) {
      auto month_str = join({year_str, numbers[month + 1]});
      auto month_dir = Dir(month_str);
      infos.push_back(month_dir);

      std::vector<PT> days;
      for (int64_t day = 0; day < kDaysPerMonth; day++) {
        auto day_str = join({month_str, numbers[day + 1]});
        auto day_dir = Dir(day_str);
        infos.push_back(day_dir);

        std::vector<PT> hours;
        for (int64_t hour = 0; hour < kHoursPerDay; hour++) {
          auto hour_str = join({day_str, numbers[hour]});
          auto hour_dir = Dir(hour_str);
          infos.push_back(hour_dir);

          std::vector<PT> files;
          for (int64_t file = 0; file < kFilesPerHour; file++) {
            auto file_str = join({hour_str, numbers[file] + ".parquet"});
            auto file_fd = File(file_str);
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
  for (auto infos : {Infos{}, Infos{Dir("A"), File("A/a")},
                     Infos{Dir("AA"), Dir("AA/BB"), File("AA/BB/0"), Dir("CC"),
                           Dir("CC/BB"), File("CC/BB/0")}}) {
    ASSERT_TRUE(std::is_sorted(infos.begin(), infos.end(), FileInfo::ByPath{}));

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
          if (!infos[ref.i].IsDirectory()) {
            return false;
          }
          actual_dirs.push_back(infos[ref.i]);
          return true;
        },
        ignore_post));

    Infos expected_dirs;
    for (const auto& info : infos) {
      if (info.IsDirectory()) {
        expected_dirs.push_back(info);
      }
    }
    EXPECT_THAT(actual_dirs, ContainerEq(expected_dirs));
  }
}

}  // namespace fs
}  // namespace arrow
