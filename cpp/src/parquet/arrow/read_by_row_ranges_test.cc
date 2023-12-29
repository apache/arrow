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

#include "gtest/gtest.h"

#include <memory>
#include <optional>
#include <random>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/io/memory.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/range.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/test_util.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_writer.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/row_ranges.h"

using arrow::TimeUnit;
using arrow::internal::Iota;
using arrow::io::BufferReader;
using parquet::schema::GroupNode;

namespace parquet::arrow::relyt {

struct ReadStatesParam {
  ReadStatesParam() = default;

  ReadStatesParam(RowRanges ranges, std::shared_ptr<PageReadStates> states)
      : row_ranges(std::move(ranges)), read_states(std::move(states)){};

  RowRanges row_ranges;
  std::shared_ptr<PageReadStates> read_states;
};

class TestBuildPageReadStates : public ::testing::TestWithParam<ReadStatesParam> {
 protected:
  void SetUp() override {
    // clang-format off
    //
    // Column chunk data layout:
    //  |-----------------------------------------------------------------------------------------|
    //  | dict_page | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
    //      -            215K         300K         512K         268K         435K         355K
    //      -           [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
    //
    // clang-format on
    page_locations_.push_back(PageLocation{1048626, 220160, 0});     // page0
    page_locations_.push_back(PageLocation{1268786, 307200, 440});   // page1
    page_locations_.push_back(PageLocation{1575986, 524288, 998});   // page2
    page_locations_.push_back(PageLocation{2100274, 274432, 1346});  // page3
    page_locations_.push_back(PageLocation{2374706, 445440, 1835});  // page4
    page_locations_.push_back(PageLocation{2820146, 363520, 2177});  // page5

    param_ = GetParam();
  }

  void BuildAndVerifyPageReadStates() {
    ASSERT_OK_AND_ASSIGN(
        auto read_states,
        BuildPageReadStates(num_rows_, chunk_range_, page_locations_, param_.row_ranges));
    ASSERT_TRUE((read_states == nullptr && param_.read_states == nullptr) ||
                (*param_.read_states == *read_states));
  }

 private:
  const int64_t num_rows_ = 2704;
  const ::arrow::io::ReadRange chunk_range_{50, 3183666};
  std::vector<PageLocation> page_locations_;

  ReadStatesParam param_;
};

std::vector<ReadStatesParam> GenerateTestCases() {
  std::vector<ReadStatesParam> params;

  // clang-format off
  //
  //  0         350
  //  |<--read-->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{0, 350}}};

    ReadRanges read_ranges =
        ReadRanges{{{50, 1048576} /*dict page*/, {1048626, 220160} /*data_page0*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{0, 350}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{350}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //
  //  0           439
  //  |<-- read -->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{0, 439}}};

    ReadRanges read_ranges =
        ReadRanges{{{50, 1048576} /*dict page*/, {1048626, 220160} /*data_page0*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{0, 439}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{439}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //
  //  0             503
  //  |<--- read --->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{0, 503}}};

    ReadRanges read_ranges = ReadRanges{{{50, 1048576} /*dict page*/,
                                         {1048626, 220160} /*data_page0*/,
                                         {1268786, 307200} /*data_page1*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{0, 439}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{439}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{440, 503}},
                                      std::vector<int64_t>{0}, std::vector<int64_t>{63}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //
  //  0                        997
  //  |<--------- read -------->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{0, 997}}};

    ReadRanges read_ranges = ReadRanges{{{50, 1048576} /*dict page*/,
                                         {1048626, 220160} /*data_page0*/,
                                         {1268786, 307200} /*data_page1*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{0, 439}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{439}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{440, 997}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{557}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //
  //  0                          1105
  //  |<---------- read ---------->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{0, 1105}}};

    ReadRanges read_ranges = ReadRanges{{{50, 1048576} /*dict page*/,
                                         {1048626, 220160} /*data_page0*/,
                                         {1268786, 307200} /*data_page1*/,
                                         {1575986, 524288} /*data_page2*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{0, 439}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{439}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{440, 997}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{557}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{998, 1105}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{107}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //
  //   57       402
  //    |<-read->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{57, 402}}};

    ReadRanges read_ranges =
        ReadRanges{{{50, 1048576} /*dict page*/, {1048626, 220160} /*data_page0*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{57, 402}},
                                      std::vector<int64_t>{57},
                                      std::vector<int64_t>{402}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //
  //   57         439
  //    |<--read-->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{57, 439}}};

    ReadRanges read_ranges =
        ReadRanges{{{50, 1048576} /*dict page*/, {1048626, 220160} /*data_page0*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{57, 439}},
                                      std::vector<int64_t>{57},
                                      std::vector<int64_t>{439}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //
  //   57               637
  //    |<---- read ---->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{57, 637}}};

    ReadRanges read_ranges = ReadRanges{{{50, 1048576} /*dict page*/,
                                         {1048626, 220160} /*data_page0*/,
                                         {1268786, 307200} /*data_page1*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{57, 439}},
                                      std::vector<int64_t>{57},
                                      std::vector<int64_t>{439}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{440, 637}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{197}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //
  //   57                      997
  //    |<-------- read ------->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{57, 997}}};

    ReadRanges read_ranges = ReadRanges{{{50, 1048576} /*dict page*/,
                                         {1048626, 220160} /*data_page0*/,
                                         {1268786, 307200} /*data_page1*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{57, 439}},
                                      std::vector<int64_t>{57},
                                      std::vector<int64_t>{439}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{440, 997}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{557}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //
  //   57                        1104
  //    |<--------- read --------->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{57, 1104}}};

    ReadRanges read_ranges = ReadRanges{{{50, 1048576} /*dict page*/,
                                         {1048626, 220160} /*data_page0*/,
                                         {1268786, 307200} /*data_page1*/,
                                         {1575986, 524288} /*data_page2*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{57, 439}},
                                      std::vector<int64_t>{57},
                                      std::vector<int64_t>{439}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{440, 997}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{557}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{998, 1104}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{106}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //                                1197 1246
  //   57                       1104 |   | 1311 1450
  //    |<--------- read --------->| |<->| |<-->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{57, 1104}, {1197, 1246}, {1311, 1450}}};

    ReadRanges read_ranges = ReadRanges{{{50, 1048576} /*dict page*/,
                                         {1048626, 220160} /*data_page0*/,
                                         {1268786, 307200} /*data_page1*/,
                                         {1575986, 524288} /*data_page2*/,
                                         {2100274, 274432} /*data_page3*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{57, 439}},
                                      std::vector<int64_t>{57},
                                      std::vector<int64_t>{439}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{440, 997}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{557}});
    skip_infos.push_back(PageSkipInfo{
        std::vector<RowRanges::Range>{{998, 1104}, {1197, 1246}, {1311, 1345}},
        std::vector<int64_t>{0, 92, 64}, std::vector<int64_t>{106, 248, 347}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{1346, 1450}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{104}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //
  //   57       403 475 763 859        1259  1403    1679
  //    |<-read->|   |<-->|  |<---read-->|     |<----->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{57, 403}, {475, 763}, {859, 1259}, {1403, 1679}}};

    ReadRanges read_ranges = ReadRanges{{{50, 1048576} /*dict page*/,
                                         {1048626, 220160} /*data_page0*/,
                                         {1268786, 307200} /*data_page1*/,
                                         {1575986, 524288} /*data_page2*/,
                                         {2100274, 274432} /*data_page3*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{57, 403}},
                                      std::vector<int64_t>{57},
                                      std::vector<int64_t>{403}});
    skip_infos.push_back(
        PageSkipInfo{std::vector<RowRanges::Range>{{475, 763}, {859, 997}},
                     std::vector<int64_t>{35, 95}, std::vector<int64_t>{323, 557}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{998, 1259}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{261}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{1403, 1679}},
                                      std::vector<int64_t>{57},
                                      std::vector<int64_t>{333}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //
  //       257         666     1004  1117 1289   1599      2001           2433
  //        |           |        |<--->|  |<------>|         |              |
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{
        {{257, 257}, {666, 666}, {1004, 1117}, {1289, 1599}, {2001, 2001}, {2433, 2433}}};

    ReadRanges read_ranges = ReadRanges{{{50, 1048576} /*dict page*/,
                                         {1048626, 220160} /*data_page0*/,
                                         {1268786, 307200} /*data_page1*/,
                                         {1575986, 524288} /*data_page2*/,
                                         {2100274, 274432} /*data_page3*/,
                                         {2374706, 445440} /*data_page4*/,
                                         {2820146, 363520} /*data_page5*/}};
    std::vector<PageSkipInfo> skip_infos;
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{257, 257}},
                                      std::vector<int64_t>{257},
                                      std::vector<int64_t>{257}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{666, 666}},
                                      std::vector<int64_t>{226},
                                      std::vector<int64_t>{226}});
    skip_infos.push_back(
        PageSkipInfo{std::vector<RowRanges::Range>{{1004, 1117}, {1289, 1345}},
                     std::vector<int64_t>{6, 171}, std::vector<int64_t>{119, 347}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{1346, 1599}},
                                      std::vector<int64_t>{0},
                                      std::vector<int64_t>{253}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{2001, 2001}},
                                      std::vector<int64_t>{166},
                                      std::vector<int64_t>{166}});
    skip_infos.push_back(PageSkipInfo{std::vector<RowRanges::Range>{{2433, 2433}},
                                      std::vector<int64_t>{256},
                                      std::vector<int64_t>{256}});
    auto read_states = std::make_shared<PageReadStates>(ranges, read_ranges, skip_infos);

    params.emplace_back(ranges, std::move(read_states));
  }

  // clang-format off
  //
  //  0                                                                           2703
  //  |<--------------------------------------------------------------------------->|
  //  |-----------------------------------------------------------------------------|
  //  | data_page0 | data_page1 | data_page2 | data_page3 | data_page4 | data_page5 |
  //       215K         300K         512K         268K         435K         355K
  //      [0,439]    [440,997]    [998,1345]   [1346,1834]  [1835,2176]  [2177,2703]
  //
  // clang-format on
  {
    RowRanges ranges = RowRanges{{{0, 2703}}};

    params.emplace_back(ranges, nullptr);
  }

  return params;
}

INSTANTIATE_TEST_SUITE_P(BuildPageReadStates, TestBuildPageReadStates,
                         ::testing::ValuesIn(GenerateTestCases()));

TEST_P(TestBuildPageReadStates, BuildPageReadStates) { BuildAndVerifyPageReadStates(); }

void CompareResult(const std::shared_ptr<::arrow::Table>& table_expected,
                   const std::shared_ptr<::arrow::Table>& table_read,
                   const std::unordered_map<int, RowRanges>& row_ranges,
                   int64_t max_row_group_length) {
  std::vector<::arrow::io::ReadRange> expected_rows;
  std::vector<::arrow::io::ReadRange> actually_rows;

  std::vector<int> row_group_indices;
  for (const auto& kv : row_ranges) {
    row_group_indices.push_back(kv.first);
  }
  ASSERT_EQ(row_group_indices.size(), row_ranges.size());

  // Calculate expected row ranges in original table and corresponding
  // ranges in read result table.
  std::sort(row_group_indices.begin(), row_group_indices.end());
  int64_t cumulative_rows = 0;
  for (int row_group : row_group_indices) {
    auto it = row_ranges.find(row_group);
    if (it == row_ranges.end()) {
      ASSERT_TRUE(false);
    }
    const auto& ranges = it->second.GetRanges();
    int64_t base = max_row_group_length * row_group;
    for (const auto& range : ranges) {
      expected_rows.emplace_back(
          ::arrow::io::ReadRange{base + range.from, (range.to - range.from + 1)});
      actually_rows.emplace_back(
          ::arrow::io::ReadRange{cumulative_rows, (range.to - range.from + 1)});
      cumulative_rows += (range.to - range.from + 1);
    }
  }
  ASSERT_EQ(expected_rows.size(), actually_rows.size());

  // Check total row num
  int64_t total_rows_expected = 0;
  int64_t total_rows_actually = 0;
  for (const auto& range : expected_rows) {
    total_rows_expected += range.length;
  }
  for (const auto& range : actually_rows) {
    total_rows_actually += range.length;
  }
  ASSERT_EQ(total_rows_expected, total_rows_actually);
  ASSERT_EQ(total_rows_expected, table_read->num_rows());

  // Check data is consistent for every row range
  for (size_t i = 0; i < expected_rows.size(); i++) {
    auto expected =
        table_expected->Slice(expected_rows[i].offset, expected_rows[i].length);
    auto actually = table_read->Slice(actually_rows[i].offset, actually_rows[i].length);
    EXPECT_TRUE(expected->Equals(*actually));
  }
}

void CompareResult(const std::shared_ptr<::arrow::Schema>& schema,
                   const std::shared_ptr<::arrow::RecordBatch>& record_batch_expected,
                   const std::shared_ptr<::arrow::RecordBatch>& record_batch_read,
                   const std::unordered_map<int, RowRanges>& row_ranges,
                   int64_t max_row_group_length) {
  ASSERT_OK_AND_ASSIGN(auto table_read,
                       ::arrow::Table::FromRecordBatches(schema, {record_batch_read}));
  ASSERT_OK_AND_ASSIGN(auto table_expected, ::arrow::Table::FromRecordBatches(
                                                schema, {record_batch_expected}));
  CompareResult(std::move(table_expected), std::move(table_read), row_ranges,
                max_row_group_length);
}

class TestReadByRowRangesSimple : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = ::arrow::default_memory_pool();
    auto sink = CreateOutputStream();
    max_row_group_length_ = 10;
    // Limit the max number of rows in a row group to 10
    auto writer_properties = WriterProperties::Builder()
                                 .max_row_group_length(max_row_group_length_)
                                 ->enable_write_page_index()
                                 ->build();
    auto arrow_writer_properties = default_arrow_writer_properties();

    // Prepare schema
    schema_ = ::arrow::schema(
        {::arrow::field("a", ::arrow::int64()),
         ::arrow::field("b", ::arrow::struct_({::arrow::field("b1", ::arrow::int64()),
                                               ::arrow::field("b2", ::arrow::utf8())})),
         ::arrow::field("c", ::arrow::utf8())});
    std::shared_ptr<SchemaDescriptor> parquet_schema;
    ASSERT_OK_NO_THROW(ToParquetSchema(schema_.get(), *writer_properties,
                                       *arrow_writer_properties, &parquet_schema));
    auto schema_node = std::static_pointer_cast<GroupNode>(parquet_schema->schema_root());

    // Prepare data
    record_batch_ = ::arrow::RecordBatchFromJSON(schema_, R"([
      [1,    {"b1": -3,   "b2": "1"   }, "alfa"],
      [null, {"b1": null, "b2": "22"  }, "alfa"],
      [3,    {"b1": -2,   "b2": "333" }, "beta"],
      [null, {"b1": null, "b2": null  }, "gama"],
      [5,    {"b1": -1,   "b2": "-333"}, null  ],
      [6,    {"b1": null, "b2": "-22" }, "alfa"],
      [7,    {"b1": 0,    "b2": "-1"  }, "beta"],
      [8,    {"b1": null, "b2": null  }, "beta"],
      [9,    {"b1": 1,    "b2": "0"   }, null  ],
      [null, {"b1": null, "b2": ""    }, "gama"],
      [11,   {"b1": 2,    "b2": "1234"}, "foo" ],
      [12,   {"b1": null, "b2": "4321"}, "bar" ]
    ])");
    num_rows_ = record_batch_->num_rows();

    // Create writer to write data via RecordBatch.
    auto writer = ParquetFileWriter::Open(sink, schema_node, writer_properties);
    std::unique_ptr<FileWriter> arrow_writer;
    ASSERT_OK(FileWriter::Make(pool_, std::move(writer), schema_, arrow_writer_properties,
                               &arrow_writer));
    // NewBufferedRowGroup() is not called explicitly and it will be called
    // inside WriteRecordBatch().
    ASSERT_OK_NO_THROW(arrow_writer->WriteRecordBatch(*record_batch_));
    ASSERT_OK_NO_THROW(arrow_writer->Close());
    ASSERT_OK_AND_ASSIGN(buffer_, sink->Finish());
  }

  void ReadAllData(int64_t batch_size) {
    std::vector<RowRanges::Range> rg0_row_ranges;
    std::vector<RowRanges::Range> rg1_row_ranges;
    rg0_row_ranges.push_back({0, 9});
    rg1_row_ranges.push_back({0, 1});

    std::unordered_map<int, RowRanges> row_ranges;
    row_ranges[0] = RowRanges(std::move(rg0_row_ranges));
    row_ranges[1] = RowRanges(std::move(rg1_row_ranges));

    reader_properties_.set_batch_size(batch_size);
    auto reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer_));
    std::unique_ptr<FileReader> arrow_reader;
    ASSERT_OK(
        FileReader::Make(pool_, std::move(reader), reader_properties_, &arrow_reader));
    const int num_row_groups =
        arrow_reader->parquet_reader()->metadata()->num_row_groups();
    std::unique_ptr<::arrow::RecordBatchReader> batch_reader;
    ASSERT_OK_NO_THROW(arrow_reader->GetRecordBatchReader(
        Iota(num_row_groups), column_vectors_, std::make_optional(row_ranges),
        &batch_reader));

    const int iterate_num = static_cast<int>((num_rows_ + batch_size - 1) / batch_size);
    const int64_t last_batch_size =
        num_rows_ % batch_size == 0 ? batch_size : num_rows_ % batch_size;
    std::shared_ptr<::arrow::RecordBatch> record_batch_read;
    for (int i = 0; i < iterate_num; i++) {
      ASSERT_OK(batch_reader->ReadNext(&record_batch_read));
      if (i != iterate_num - 1) {
        const auto& expected = record_batch_->Slice(i * batch_size, batch_size);
        EXPECT_TRUE(expected->Equals(*record_batch_read));
      } else {
        const auto& expected = record_batch_->Slice(i * batch_size, last_batch_size);
        EXPECT_TRUE(expected->Equals(*record_batch_read));
      }
    }
    ASSERT_OK(batch_reader->ReadNext(&record_batch_read));
    ASSERT_TRUE(record_batch_read == nullptr);
  }

  void ReadPartialDataWithOneRangeEachPage(int64_t offset) {
    auto reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer_));
    const int64_t row_group_length = reader->metadata()->RowGroup(0)->num_rows();
    for (int row_num = 1; row_num < num_rows_ - offset; row_num++) {
      std::vector<int> row_group_indices;
      std::unordered_map<int, RowRanges> row_ranges;
      if (offset + row_num <= row_group_length) {
        row_group_indices.push_back(0);

        row_ranges[0] = RowRanges({RowRanges::Range{offset, offset + row_num - 1}});
      } else {
        row_group_indices.push_back(0);
        row_group_indices.push_back(1);

        row_ranges[0] = RowRanges({RowRanges::Range{offset, row_group_length - 1}});
        row_ranges[1] =
            RowRanges({RowRanges::Range{0, row_num - (row_group_length - offset + 1)}});
      }

      reader_properties_.set_batch_size(15);
      reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer_));
      std::unique_ptr<FileReader> arrow_reader;
      ASSERT_OK(
          FileReader::Make(pool_, std::move(reader), reader_properties_, &arrow_reader));
      std::unique_ptr<::arrow::RecordBatchReader> batch_reader;
      ASSERT_OK_NO_THROW(arrow_reader->GetRecordBatchReader(
          row_group_indices, column_vectors_, std::make_optional(row_ranges),
          &batch_reader));

      std::shared_ptr<::arrow::RecordBatch> record_batch_read;
      ASSERT_OK(batch_reader->ReadNext(&record_batch_read));
      EXPECT_TRUE(record_batch_->Slice(offset, row_num)->Equals(*record_batch_read));
      ASSERT_OK(batch_reader->ReadNext(&record_batch_read));
      ASSERT_TRUE(record_batch_read == nullptr);
    }
  }

  std::vector<RowRanges::Range> SplitRanges(int64_t from, int64_t to) {
    std::vector<RowRanges::Range> splits;
    if (from == to) {
      splits.push_back(RowRanges::Range{from, to});
    } else {
      std::random_device rd;
      std::mt19937_64 gen(rd());
      std::uniform_int_distribution<int64_t> d(from, to);
      const int64_t point0 = d(gen);
      const int64_t point1 = d(gen);

      RowRanges::Range first_part, second_part;
      if (point0 < point1) {
        first_part = {from, point0};
        second_part = {point1, to};
      } else if (point0 == point1) {
        if (from < point0) {
          first_part = {from, point0 - 1};
          second_part = {point1, to};
        } else {
          first_part = {from, point0};
          second_part = {point1 + 1, to};
        }
      } else {
        first_part = {from, point1};
        second_part = {point0, to};
      }
      splits.push_back(std::move(first_part));
      splits.push_back(std::move(second_part));
    }

    return splits;
  }

  void ReadPartialDataWithMultiRangesEachPage(int64_t offset) {
    auto reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer_));
    const int64_t row_group_length = reader->metadata()->RowGroup(0)->num_rows();
    for (int row_num = 1; row_num < num_rows_ - offset; row_num++) {
      std::vector<int> row_group_indices;
      std::unordered_map<int, RowRanges> row_ranges;
      if (offset + row_num <= row_group_length) {
        row_group_indices.push_back(0);

        row_ranges[0] = RowRanges(SplitRanges(offset, offset + row_num - 1));
      } else {
        row_group_indices.push_back(0);
        row_group_indices.push_back(1);

        row_ranges[0] = RowRanges(SplitRanges(offset, row_group_length - 1));
        row_ranges[1] =
            RowRanges(SplitRanges(0, row_num - (row_group_length - offset + 1)));
      }

      reader_properties_.set_batch_size(15);
      reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer_));
      std::unique_ptr<FileReader> arrow_reader;
      ASSERT_OK(
          FileReader::Make(pool_, std::move(reader), reader_properties_, &arrow_reader));
      std::unique_ptr<::arrow::RecordBatchReader> batch_reader;
      ASSERT_OK_NO_THROW(arrow_reader->GetRecordBatchReader(
          row_group_indices, column_vectors_, std::make_optional(row_ranges),
          &batch_reader));

      std::shared_ptr<::arrow::RecordBatch> record_batch_read;
      ASSERT_OK(batch_reader->ReadNext(&record_batch_read));
      CompareResult(schema_, record_batch_, record_batch_read, row_ranges,
                    max_row_group_length_);
      ASSERT_OK(batch_reader->ReadNext(&record_batch_read));
      ASSERT_TRUE(record_batch_read == nullptr);
    }
  }

  void RandomReadOneRow() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int64_t> d(0, num_rows_ - 1);
    const int64_t row_offset = d(gen);

    auto reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer_));
    const int64_t row_group_length = reader->metadata()->RowGroup(0)->num_rows();
    std::vector<int> row_group_indices;
    const int row_group_index = static_cast<int>(row_offset / row_group_length);
    row_group_indices.push_back(row_group_index);
    int64_t row_offset_in_rg = row_offset % row_group_length;
    std::unordered_map<int, RowRanges> row_ranges;
    row_ranges[row_group_index] =
        RowRanges({RowRanges::Range{row_offset_in_rg, row_offset_in_rg}});

    std::unique_ptr<FileReader> arrow_reader;
    ASSERT_OK(
        FileReader::Make(pool_, std::move(reader), reader_properties_, &arrow_reader));
    std::unique_ptr<::arrow::RecordBatchReader> batch_reader;
    ASSERT_OK_NO_THROW(arrow_reader->GetRecordBatchReader(
        row_group_indices, column_vectors_, std::make_optional(row_ranges),
        &batch_reader));

    std::shared_ptr<::arrow::RecordBatch> record_batch_read;
    ASSERT_OK(batch_reader->ReadNext(&record_batch_read));
    CompareResult(schema_, record_batch_, record_batch_read, row_ranges,
                  max_row_group_length_);
  }

  void DynamicReadAsDict() {
    reader_properties_.set_batch_size(num_rows_);

    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int64_t> d(0, num_rows_ - 1);
    const int64_t point0 = d(gen);
    const int64_t point1 = d(gen);

    int64_t from, to;
    if (point0 <= point1) {
      from = point0;
      to = point1;
    } else {
      from = point1;
      to = point0;
    }

    std::vector<int> row_group_indices;
    std::unordered_map<int, RowRanges> row_ranges;
    if (to >= max_row_group_length_) {
      if (from >= max_row_group_length_) {
        row_group_indices.push_back(1);
        row_ranges[1] =
            RowRanges({{from - max_row_group_length_, to - max_row_group_length_}});
      } else {
        row_group_indices.push_back(0);
        row_group_indices.push_back(1);

        row_ranges[0] = RowRanges({{from, max_row_group_length_ - 1}});
        row_ranges[1] = RowRanges({{0, to - max_row_group_length_}});
      }
    } else {
      row_group_indices.push_back(0);
      row_ranges[0] = RowRanges({{from, to}});
    }

    int64_t num_rows_expected = 0;
    for (const auto& kv : row_ranges) {
      num_rows_expected += kv.second.GetRowCount();
    }

    auto reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer_));
    ASSERT_EQ(2, reader->metadata()->num_row_groups());
    for (int i = 0; i < reader->metadata()->num_columns(); i++) {
      reader_properties_.set_read_dictionary(i, true);
    }

    std::unique_ptr<FileReader> arrow_reader;
    ASSERT_OK(
        FileReader::Make(pool_, std::move(reader), reader_properties_, &arrow_reader));
    std::unique_ptr<::arrow::RecordBatchReader> batch_reader_raw;
    std::unique_ptr<::arrow::RecordBatchReader> batch_reader;

    // Primitive types support read multiple dicts in one batch and cache result in
    // iterator to make sure return only one dict in each invocation
    {
      ASSERT_OK_NO_THROW(arrow_reader->GetRecordBatchReader(row_group_indices, {0, 3},
                                                            &batch_reader_raw));
      ASSERT_OK_NO_THROW(arrow_reader->GetRecordBatchReader(
          row_group_indices, {0, 3}, std::make_optional(row_ranges), &batch_reader));

      int64_t num_rows_read = 0;
      std::shared_ptr<::arrow::RecordBatch> record_batch_raw;
      std::shared_ptr<::arrow::RecordBatch> record_batch_read;
      for (int row_group_index : row_group_indices) {
        ASSERT_OK(batch_reader_raw->ReadNext(&record_batch_raw));
        ASSERT_OK(batch_reader->ReadNext(&record_batch_read));

        ASSERT_TRUE(record_batch_raw != nullptr);
        ASSERT_TRUE(record_batch_read != nullptr);

        EXPECT_TRUE(record_batch_read->column(0)->type_id() == ::arrow::Type::INT64);
        EXPECT_TRUE(record_batch_read->column(1)->type_id() == ::arrow::Type::DICTIONARY);
        auto dict2 = std::dynamic_pointer_cast<::arrow::DictionaryArray>(
            record_batch_read->column(1));
        EXPECT_TRUE(dict2->dictionary() != nullptr);

        // Compare read result
        const auto& ranges = row_ranges[row_group_index].GetRanges();
        auto record_batch_expected =
            record_batch_raw->Slice(ranges[0].from, (ranges[0].to - ranges[0].from + 1));
        ASSERT_TRUE(record_batch_expected->Equals(*record_batch_read));

        num_rows_read += record_batch_read->num_rows();
      }

      ASSERT_EQ(num_rows_expected, num_rows_read);
    }

    // Nested types do not support read multiple dicts in one batch
    {
      ASSERT_OK_NO_THROW(arrow_reader->GetRecordBatchReader(
          row_group_indices, column_vectors_, &batch_reader_raw));
      ASSERT_OK_NO_THROW(arrow_reader->GetRecordBatchReader(
          row_group_indices, column_vectors_, std::make_optional(row_ranges),
          &batch_reader));

      if (row_ranges.size() == 1) {
        // Read only one dict in this invocation
        std::shared_ptr<::arrow::RecordBatch> record_batch_raw;
        std::shared_ptr<::arrow::RecordBatch> record_batch_read;
        ASSERT_OK(batch_reader_raw->ReadNext(&record_batch_raw));
        ASSERT_OK(batch_reader->ReadNext(&record_batch_read));

        ASSERT_TRUE(record_batch_raw != nullptr);
        ASSERT_TRUE(record_batch_read != nullptr);

        EXPECT_TRUE(record_batch_read->column(0)->type_id() == ::arrow::Type::INT64);
        EXPECT_TRUE(record_batch_read->column(1)->type_id() == ::arrow::Type::STRUCT);
        const auto array =
            std::dynamic_pointer_cast<::arrow::StructArray>(record_batch_read->column(1));
        EXPECT_TRUE(array->field(0)->type_id() == ::arrow::Type::INT64);
        EXPECT_TRUE(array->field(1)->type_id() == ::arrow::Type::DICTIONARY);
        auto dict1 = std::dynamic_pointer_cast<::arrow::DictionaryArray>(array->field(1));
        EXPECT_TRUE(dict1->dictionary() != nullptr);
        EXPECT_TRUE(record_batch_read->column(2)->type_id() == ::arrow::Type::DICTIONARY);
        auto dict2 = std::dynamic_pointer_cast<::arrow::DictionaryArray>(
            record_batch_read->column(2));
        EXPECT_TRUE(dict2->dictionary() != nullptr);

        // Compare read result
        const int row_group_index = row_group_indices[0];
        const auto& ranges = row_ranges[row_group_index].GetRanges();
        auto record_batch_expected =
            record_batch_raw->Slice(ranges[0].from, (ranges[0].to - ranges[0].from + 1));
        ASSERT_TRUE(record_batch_expected->Equals(*record_batch_read));

        ASSERT_EQ(num_rows_expected, record_batch_read->num_rows());

        record_batch_read.reset();
        ASSERT_OK(batch_reader->ReadNext(&record_batch_read));
        ASSERT_TRUE(record_batch_read == nullptr);
      } else {
        // Read multiple dicts in this invocation
        std::shared_ptr<::arrow::RecordBatch> record_batch_read;
        ASSERT_RAISES_WITH_MESSAGE(NotImplemented,
                                   "NotImplemented: Nested data conversions not "
                                   "implemented for chunked array outputs",
                                   batch_reader->ReadNext(&record_batch_read));
      }
    }
  }

  void ReadWithInvalidRowRange() {
    auto reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer_));
    const int num_row_groups = reader->metadata()->num_row_groups();
    std::unique_ptr<FileReader> arrow_reader;
    ASSERT_OK(
        FileReader::Make(pool_, std::move(reader), reader_properties_, &arrow_reader));

    // Random generate a point within file row range
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int64_t> d(0, num_rows_ - 1);
    const int64_t row_offset = d(gen);

    std::vector<int> row_group_indices;
    const int row_group_index = static_cast<int>(row_offset / max_row_group_length_);
    row_group_indices.push_back(row_group_index);

    // First row index is too small
    {
      int64_t end_row_offset;
      RowRanges::Range allowable_range;

      if (row_group_index == num_row_groups - 1) {
        end_row_offset = num_rows_ - max_row_group_length_ - 1;
        allowable_range = {0, num_rows_ % max_row_group_length_ - 1};
      } else {
        end_row_offset = max_row_group_length_ - 1;
        allowable_range = {0, max_row_group_length_ - 1};
      }

      std::unordered_map<int, RowRanges> row_ranges;
      row_ranges[row_group_index] = RowRanges({RowRanges::Range{-1, end_row_offset}});

      std::stringstream ss;
      ss << "Invalid: Row range exceeded, allowable: " << allowable_range.ToString()
         << ", actually: " << row_ranges[row_group_index].ToString();
      std::unique_ptr<::arrow::RecordBatchReader> batch_reader;

      ASSERT_RAISES_WITH_MESSAGE(Invalid, ss.str(),
                                 arrow_reader->GetRecordBatchReader(
                                     row_group_indices, column_vectors_,
                                     std::make_optional(row_ranges), &batch_reader));
    }

    // Last row index is too big
    {
      int64_t start_row_offset = row_offset % max_row_group_length_;
      int64_t end_row_offset;
      RowRanges::Range allowable_range;

      if (row_group_index == num_row_groups - 1) {
        end_row_offset = num_rows_ - max_row_group_length_;
        allowable_range = {0, num_rows_ % max_row_group_length_ - 1};
      } else {
        end_row_offset = max_row_group_length_;
        allowable_range = {0, max_row_group_length_ - 1};
      }

      std::unordered_map<int, RowRanges> row_ranges;
      row_ranges[row_group_index] =
          RowRanges({RowRanges::Range{start_row_offset, end_row_offset}});

      std::stringstream ss;
      ss << "Invalid: Row range exceeded, allowable: " << allowable_range.ToString()
         << ", actually: " << row_ranges[row_group_index].ToString();
      std::unique_ptr<::arrow::RecordBatchReader> batch_reader;

      ASSERT_RAISES_WITH_MESSAGE(Invalid, ss.str(),
                                 arrow_reader->GetRecordBatchReader(
                                     row_group_indices, column_vectors_,
                                     std::make_optional(row_ranges), &batch_reader));
    }

    // First row index and last row index are illegal
    {
      int64_t end_row_offset;
      RowRanges::Range allowable_range;

      if (row_group_index == num_row_groups - 1) {
        end_row_offset = num_rows_ - max_row_group_length_;
        allowable_range = {0, num_rows_ % max_row_group_length_ - 1};
      } else {
        end_row_offset = max_row_group_length_;
        allowable_range = {0, max_row_group_length_ - 1};
      }

      std::unordered_map<int, RowRanges> row_ranges;
      row_ranges[row_group_index] = RowRanges({RowRanges::Range{-1, end_row_offset}});

      std::stringstream ss;
      ss << "Invalid: Row range exceeded, allowable: " << allowable_range.ToString()
         << ", actually: " << row_ranges[row_group_index].ToString();
      std::unique_ptr<::arrow::RecordBatchReader> batch_reader;

      ASSERT_RAISES_WITH_MESSAGE(Invalid, ss.str(),
                                 arrow_reader->GetRecordBatchReader(
                                     row_group_indices, column_vectors_,
                                     std::make_optional(row_ranges), &batch_reader));
    }
  }

  // @formatter:off
  const std::vector<int> column_vectors_ = {0, 1, 2, 3};
  // @formatter:on

  MemoryPool* pool_;

  int64_t max_row_group_length_{-1};
  std::shared_ptr<::arrow::Schema> schema_;

  std::shared_ptr<Buffer> buffer_;
  std::shared_ptr<::arrow::RecordBatch> record_batch_;
  int64_t num_rows_;

  ArrowReaderProperties reader_properties_;
};

TEST_F(TestReadByRowRangesSimple, ReadAllData) {
  // Test cases:
  //   1. Batch size bigger than row group size
  //   2. Batch size equals to row group size
  //   3. Batch size smaller than row group size
  //   4. Read all data into 4 record batches,
  for (int64_t batch_size : {15, 10, 7, 3}) {
    ReadAllData(batch_size);
  }
}

TEST_F(TestReadByRowRangesSimple, ReadPartialDataWithOneRangeEachPage) {
  for (int64_t offset : Iota(9)) {
    ReadPartialDataWithOneRangeEachPage(offset);
  }
}

TEST_F(TestReadByRowRangesSimple, ReadPartialDataWithMultiRangesEachPage) {
  for (int64_t offset : Iota(9)) {
    ReadPartialDataWithMultiRangesEachPage(offset);
  }
}

TEST_F(TestReadByRowRangesSimple, RandomReadOneRow) {
  for (int i = 0; i < 10; i++) {
    RandomReadOneRow();
  }
}

TEST_F(TestReadByRowRangesSimple, DynamicReadAsDict) {
  for (int i = 0; i < 10; i++) {
    DynamicReadAsDict();
  }
}

TEST_F(TestReadByRowRangesSimple, ReadWithInvalidRowRanges) { ReadWithInvalidRowRange(); }

class TestReadByRowRangesBase {
 protected:
  virtual void PrepareDataAndReader(bool nullable = false) = 0;

  void ReadNonNullData() {
    PrepareDataAndReader(false);
    for (int i = 0; i < 10; i++) {
      RandomReadOneRow();
    }
    for (int num_ranges : {1, 5, 10, 20}) {
      ReadDataWithRandomRanges(num_ranges);
    }
  }

  void ReadNullableData() {
    PrepareDataAndReader(true);
    for (int i = 0; i < 10; i++) {
      RandomReadOneRow();
    }
    for (int num_ranges : {1, 5, 10, 20}) {
      ReadDataWithRandomRanges(num_ranges);
    }
  }

  MemoryPool* pool_ = ::arrow::default_memory_pool();

  int64_t max_data_page_size_{-1};
  int64_t write_batch_size_{-1};
  int64_t max_row_group_length_{-1};
  std::shared_ptr<WriterProperties> writer_properties_;

  std::shared_ptr<::arrow::Schema> schema_;
  std::shared_ptr<::arrow::Table> table_;

  int64_t num_rows_{-1};
  std::unique_ptr<FileReader> reader_;

  std::vector<int> column_vectors_;

 private:
  void CheckPrepared() const {
    ASSERT_TRUE(num_rows_ > 0);
    ASSERT_TRUE(schema_ != nullptr);
    ASSERT_TRUE(table_ != nullptr);
    ASSERT_TRUE(reader_ != nullptr);
  }

  std::vector<RowRanges::Range> GenerateRowRanges(int num_ranges) const {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int64_t> d(0, num_rows_ - 1);

    std::vector<int64_t> points;
    for (int i = 0; i < num_ranges * 2; i++) {
      int64_t point = d(gen);
      auto it = std::find(points.begin(), points.end(), point);

      // Try to generate a value that is not in the points vector
      if (it != points.end()) {
        while (true) {
          point = d(gen);
          it = std::find(points.begin(), points.end(), point);
          if (it == points.end()) {
            break;
          }
        }
      }
      points.push_back(d(gen));
    }
    std::sort(points.begin(), points.end());

    std::vector<RowRanges::Range> ranges;
    for (int i = 0; i < num_ranges; i++) {
      ranges.push_back(RowRanges::Range{points[2 * i], points[2 * i + 1]});
    }
    return ranges;
  }

  std::unordered_map<int, RowRanges> SplitRowRangesByRowGroup(
      const std::vector<RowRanges::Range>& ranges) const {
    const auto metadata = reader_->parquet_reader()->metadata();
    const auto num_row_groups = metadata->num_row_groups();

    int64_t from = 0;
    int64_t cumulative_row = 0;
    std::vector<int64_t> row_nums;
    std::vector<int64_t> cumulative_rows;
    std::vector<RowRanges::Range> rg_row_ranges;
    for (int i = 0; i < num_row_groups; i++) {
      const int64_t row_num = metadata->RowGroup(i)->num_rows();
      row_nums.push_back(row_num);
      cumulative_rows.push_back(cumulative_row);
      cumulative_row += row_num;
      rg_row_ranges.push_back(RowRanges::Range{from, cumulative_row - 1});
      from = cumulative_row;
    }

    std::unordered_map<int, RowRanges> row_ranges;
    std::vector<RowRanges::Range> rg_ranges;

    int i = 0;
    for (auto range : ranges) {
      while (i < num_row_groups) {
        auto res = RowRanges::Range::Intersection(rg_row_ranges[i], range);
        if (res == RowRanges::EMPTY_RANGE) {
          if (range.from > rg_row_ranges[i].to) {
            if (!rg_ranges.empty()) {
              row_ranges[i] = RowRanges(rg_ranges);
              rg_ranges.clear();
            }
            // move to next i
            i++;
          } else {
            // range.to < rg_row_ranges[i].from
            // move to next range
            break;
          }
        } else {
          RowRanges::Range rg_range{res.from - cumulative_rows[i],
                                    res.to - cumulative_rows[i]};
          rg_ranges.push_back(std::move(rg_range));
          if (range.to > rg_row_ranges[i].to) {
            row_ranges[i] = RowRanges(rg_ranges);
            rg_ranges.clear();
            // move to next i
            i++;
          } else {
            // range.to <= rg_row_ranges[i].to
            // move to next range
            break;
          }
        }
      }
    }

    if (!rg_ranges.empty()) {
      row_ranges[i] = RowRanges(rg_ranges);
      rg_ranges.clear();
    }

    return row_ranges;
  }

  void ReadDataWithRandomRanges(int num_ranges) {
    CheckPrepared();

    const std::vector<RowRanges::Range> ranges = GenerateRowRanges(num_ranges);
    const std::unordered_map<int, RowRanges> row_ranges =
        SplitRowRangesByRowGroup(ranges);

    std::vector<int> row_group_indices;
    for (const auto& kv : row_ranges) {
      row_group_indices.push_back(kv.first);
    }

    // Although file reader doesn't care whether passed-in row group indices
    // are in asc order, we sort it to make easier to compare read result
    // with original data table.
    std::sort(row_group_indices.begin(), row_group_indices.end());

    std::unique_ptr<::arrow::RecordBatchReader> batch_reader;
    ASSERT_OK_NO_THROW(reader_->GetRecordBatchReader(row_group_indices, column_vectors_,
                                                     std::make_optional(row_ranges),
                                                     &batch_reader));
    std::shared_ptr<::arrow::RecordBatch> record_batch_read;
    ASSERT_OK(batch_reader->ReadNext(&record_batch_read));
    ASSERT_OK_AND_ASSIGN(auto table_read,
                         ::arrow::Table::FromRecordBatches(schema_, {record_batch_read}));
    CompareResult(table_, table_read, row_ranges, max_row_group_length_);
    ASSERT_OK(batch_reader->ReadNext(&record_batch_read));
    ASSERT_TRUE(record_batch_read == nullptr);
  }

  void RandomReadOneRow() {
    CheckPrepared();

    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int64_t> d(0, num_rows_ - 1);
    const int64_t row_offset = d(gen);

    std::vector<int> row_group_indices;
    const int row_group_index = static_cast<int>(row_offset / max_row_group_length_);
    row_group_indices.push_back(row_group_index);
    int64_t row_offset_in_rg = row_offset % max_row_group_length_;
    std::unordered_map<int, RowRanges> row_ranges;
    row_ranges[row_group_index] =
        RowRanges({RowRanges::Range{row_offset_in_rg, row_offset_in_rg}});

    std::unique_ptr<::arrow::RecordBatchReader> batch_reader;
    ASSERT_OK_NO_THROW(reader_->GetRecordBatchReader(row_group_indices, column_vectors_,
                                                     std::make_optional(row_ranges),
                                                     &batch_reader));

    std::shared_ptr<::arrow::RecordBatch> record_batch_read;
    ASSERT_OK(batch_reader->ReadNext(&record_batch_read));
    ASSERT_OK_AND_ASSIGN(auto table_read,
                         ::arrow::Table::FromRecordBatches(schema_, {record_batch_read}));
    CompareResult(table_, table_read, row_ranges, max_row_group_length_);
    ASSERT_OK(batch_reader->ReadNext(&record_batch_read));
    ASSERT_TRUE(record_batch_read == nullptr);
  }
};

template <typename TestType>
class TestReadSingleColumnByRowRanges : public TestReadByRowRangesBase,
                                        public ::testing::Test {
 protected:
  void SetUp() override {
    max_data_page_size_ = 1;
    write_batch_size_ = 256;
    max_row_group_length_ = 1024;

    WriterProperties::Builder builder;
    writer_properties_ = builder.max_row_group_length(max_row_group_length_)
                             ->data_pagesize(max_data_page_size_)
                             ->write_batch_size(write_batch_size_)
                             ->enable_write_page_index()
                             ->build();
  }

  void PrepareDataAndReader(bool nullable) override {
    auto sink = CreateOutputStream();

    std::shared_ptr<::arrow::Array> values;
    const int64_t total_rows = 10240;
    if (nullable) {
      ASSERT_OK(NullableArray<TestType>(total_rows, total_rows / 2, 0, &values));
      table_ = MakeSimpleTable(values, true);
    } else {
      ASSERT_OK(NonNullArray<TestType>(total_rows, &values));
      table_ = MakeSimpleTable(values, false);
    }
    schema_ = table_->schema();
    num_rows_ = table_->num_rows();
    column_vectors_.push_back(0);

    ASSERT_OK_NO_THROW(WriteTable(*table_, ::arrow::default_memory_pool(), sink,
                                  max_row_group_length_, writer_properties_));
    ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

    auto arrow_reader_properties = ::parquet::default_arrow_reader_properties();
    arrow_reader_properties.set_batch_size(num_rows_);

    FileReaderBuilder file_reader_builder;
    ASSERT_OK_NO_THROW(
        file_reader_builder.Open(std::make_shared<BufferReader>(std::move(buffer))));
    ASSERT_OK_NO_THROW(file_reader_builder.memory_pool(pool_)
                           ->properties(arrow_reader_properties)
                           ->Build(&reader_));
  }
};

using TestTypes = ::testing::Types<
    ::arrow::BooleanType, ::arrow::UInt8Type, ::arrow::Int8Type, ::arrow::UInt16Type,
    ::arrow::Int16Type, ::arrow::Int32Type, ::arrow::UInt64Type, ::arrow::Int64Type,
    ::arrow::Date32Type, ::arrow::FloatType, ::arrow::DoubleType, ::arrow::StringType,
    ::arrow::BinaryType, ::arrow::FixedSizeBinaryType, DecimalWithPrecisionAndScale<19>,
    DecimalWithPrecisionAndScale<20>, DecimalWithPrecisionAndScale<25>,
    DecimalWithPrecisionAndScale<27>, DecimalWithPrecisionAndScale<29>,
    DecimalWithPrecisionAndScale<33>, DecimalWithPrecisionAndScale<38>>;

TYPED_TEST_SUITE(TestReadSingleColumnByRowRanges, TestTypes);

TYPED_TEST(TestReadSingleColumnByRowRanges, ReadNonNullColumn) {
  this->ReadNonNullData();
}

TYPED_TEST(TestReadSingleColumnByRowRanges, ReadNullableColumn) {
  this->ReadNullableData();
}

class TestReadTableByRowRanges : public TestReadByRowRangesBase, public ::testing::Test {
 protected:
  void SetUp() override {
    max_data_page_size_ = 32 * 1024;  // 32KB
    max_row_group_length_ = 150000;

    WriterProperties::Builder builder;
    writer_properties_ = builder.max_row_group_length(max_row_group_length_)
                             ->data_pagesize(max_data_page_size_)
                             ->enable_write_page_index()
                             ->build();

    num_rows_ = 1000000;
  }

  void PrepareDataAndReader(bool) override {
    auto sink = CreateOutputStream();

    using ColumnTypes =
        TypeList<::arrow::BooleanType, ::arrow::UInt8Type, ::arrow::Int8Type,
                 ::arrow::UInt16Type, ::arrow::Int16Type, ::arrow::Int32Type,
                 ::arrow::UInt64Type, ::arrow::Int64Type, ::arrow::Date32Type,
                 ::arrow::FloatType, ::arrow::DoubleType, ::arrow::StringType,
                 ::arrow::BinaryType, ::arrow::FixedSizeBinaryType,
                 DecimalWithPrecisionAndScale<19>, DecimalWithPrecisionAndScale<20>,
                 DecimalWithPrecisionAndScale<25>, DecimalWithPrecisionAndScale<27>,
                 DecimalWithPrecisionAndScale<29>, DecimalWithPrecisionAndScale<33>,
                 DecimalWithPrecisionAndScale<38>>;

    // Prepare data and schema
    columns_.clear();
    MakeColumns(ColumnTypes{});
    ASSERT_EQ(21, columns_.size());

    std::vector<std::shared_ptr<::arrow::Field>> fields;
    for (size_t i = 0; i < columns_.size(); i++) {
      const bool nullable = columns_[i]->null_count() > 0;
      fields.push_back(
          ::arrow::field("col" + std::to_string(i), columns_[i]->type(), nullable));
    }
    schema_ = ::arrow::schema(std::move(fields));
    table_ = ::arrow::Table::Make(schema_, columns_, num_rows_);

    // Write data to sink
    ASSERT_OK_NO_THROW(WriteTable(*table_, ::arrow::default_memory_pool(), sink,
                                  max_row_group_length_, writer_properties_));
    ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

    // Prepare relyt schema and missing values
    auto arrow_reader_properties = ::parquet::default_arrow_reader_properties();
    std::vector<std::shared_ptr<::arrow::Field>> relyt_fields;
    for (int i = 0; i < schema_->num_fields(); i++) {
      column_vectors_.push_back({i});
    }

    arrow_reader_properties.set_batch_size(num_rows_);

    FileReaderBuilder file_reader_builder;
    ASSERT_OK_NO_THROW(
        file_reader_builder.Open(std::make_shared<BufferReader>(std::move(buffer))));
    ASSERT_OK_NO_THROW(file_reader_builder.memory_pool(pool_)
                           ->properties(arrow_reader_properties)
                           ->Build(&reader_));
  }

 private:
  template <typename... Types>
  struct TypeList {};

  template <typename T>
  void MakeColumn() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> d(0, 1);
    bool nullable = d(gen) == 1;

    std::shared_ptr<::arrow::Array> values;
    if (nullable) {
      ASSERT_OK(NullableArray<T>(num_rows_, num_rows_ / 2, 0, &values));
    } else {
      ASSERT_OK(NonNullArray<T>(num_rows_, &values));
    }

    columns_.push_back(std::move(values));
  }

  template <typename... Types>
  void MakeColumns(TypeList<Types...>) {}

  template <typename T, typename... Types>
  void MakeColumns(TypeList<T, Types...>) {
    MakeColumn<T>();
    MakeColumns(TypeList<Types...>{});
  }

  void MakeColumns() {}

  std::vector<std::shared_ptr<::arrow::Array>> columns_;
};

TEST_F(TestReadTableByRowRanges, RandomReadByRowRanges) { this->ReadNullableData(); }

}  // namespace parquet::arrow::relyt
