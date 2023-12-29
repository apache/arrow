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

#include "parquet/exception.h"
#include "parquet/row_ranges.h"

namespace parquet {

TEST(TestRowRanges, NormalRowRanges) {
  // Construct with given ranges and continues ranges will be merged
  {
    std::vector<RowRanges::Range> ranges;
    ranges.push_back({0, 0});
    ranges.push_back({1, 1});
    ranges.push_back({3, 5});
    ranges.push_back({9, 15});

    RowRanges row_ranges{std::move(ranges)};

    ranges.clear();
    ranges.push_back({0, 1});
    ranges.push_back({3, 5});
    ranges.push_back({9, 15});
    ASSERT_TRUE(ranges == row_ranges.GetRanges());
    ASSERT_EQ(12, row_ranges.GetRowCount());
  }

  // Construct with add method and continues ranges will be merged
  {
    RowRanges row_ranges;
    row_ranges.Add(RowRanges::Range{0, 0});
    row_ranges.Add(RowRanges::Range{1, 1});
    row_ranges.Add(RowRanges::Range{3, 7});
    row_ranges.Add(RowRanges::Range{9, 11});

    std::vector<RowRanges::Range> ranges;
    ranges.push_back({0, 1});
    ranges.push_back({3, 7});
    ranges.push_back({9, 11});
    ASSERT_EQ(10, row_ranges.GetRowCount());
    ASSERT_TRUE(ranges == row_ranges.GetRanges());
  }

  // Construct with add method and continues ranges will be merged
  {
    RowRanges row_ranges;
    row_ranges.Add(RowRanges::Range{0, 0});
    row_ranges.Add(RowRanges::Range{1, 1});
    row_ranges.Add(RowRanges::Range{3, 7});
    row_ranges.Add(RowRanges::Range{8, 11});
    row_ranges.Add(RowRanges::Range{12, 15});
    row_ranges.Add(RowRanges::Range{16, 23});
    ASSERT_EQ(23, row_ranges.GetRowCount());

    std::vector<RowRanges::Range> ranges;
    ranges.push_back({0, 1});
    ranges.push_back({3, 23});
    ASSERT_TRUE(ranges == row_ranges.GetRanges());
  }

  // Add same ranges
  {
    RowRanges row_ranges;

    RowRanges::Range range = {2, 3};
    row_ranges.Add(range);
    row_ranges.Add(range);
    ASSERT_EQ(1, row_ranges.GetRanges().size());
    ASSERT_TRUE(range == row_ranges.GetRanges()[0]);
  }

  // Add overlapped ranges
  {
    RowRanges row_ranges;
    row_ranges.Add(RowRanges::Range{0, 1});
    row_ranges.Add(RowRanges::Range{9, 25});
    row_ranges.Add(RowRanges::Range{14, 33});
    ASSERT_EQ(2, row_ranges.GetRanges().size());

    RowRanges::Range range0{0, 1};
    RowRanges::Range range1{9, 33};
    ASSERT_TRUE(range0 == row_ranges.GetRanges()[0]);
    ASSERT_TRUE(range1 == row_ranges.GetRanges()[1]);
  }

  // Add overlapped ranges
  {
    RowRanges row_ranges;
    row_ranges.Add(RowRanges::Range{0, 1});
    row_ranges.Add(RowRanges::Range{9, 25});
    row_ranges.Add(RowRanges::Range{12, 15});
    ASSERT_EQ(2, row_ranges.GetRanges().size());

    RowRanges::Range range0{0, 1};
    RowRanges::Range range1{9, 25};
    ASSERT_TRUE(range0 == row_ranges.GetRanges()[0]);
    ASSERT_TRUE(range1 == row_ranges.GetRanges()[1]);
  }
}

TEST(TestRowRanges, InvalidRowRanges) {
  // Construct with empty ranges
  {
    std::vector<RowRanges::Range> ranges;
    ASSERT_THROW(RowRanges{ranges}, ParquetException);
  }

  // Ranges are not in ascending order
  {
    std::vector<RowRanges::Range> ranges;
    ranges.push_back({0, 1});
    ranges.push_back({3, 9});
    ranges.push_back({2, 2});
    ASSERT_THROW(RowRanges{ranges}, ParquetException);
  }

  // Construct with illegal range
  {
    std::vector<RowRanges::Range> ranges;
    ranges.push_back({7, 3});
    ASSERT_THROW(RowRanges{ranges}, ParquetException);
  }

  // Add small range
  {
    RowRanges row_ranges;
    row_ranges.Add(RowRanges::Range{1, 3});
    row_ranges.Add(RowRanges::Range{9, 15});
    ASSERT_THROW(row_ranges.Add(RowRanges::Range{4, 7}), ParquetException);
  }

  // Add illegal range
  {
    RowRanges row_ranges;
    ASSERT_THROW(row_ranges.Add(RowRanges::Range{7, 3}), ParquetException);
  }
}

TEST(TestRowRanges, RangeUnion) {
  {
    RowRanges::Range range0{10, 30};
    RowRanges::Range range1{20, 45};

    RowRanges::Range result{10, 45};
    ASSERT_TRUE(result == RowRanges::Range::Union(range0, range1));
  }

  {
    RowRanges::Range range0{10, 30};
    RowRanges::Range range1{35, 45};

    ASSERT_TRUE(RowRanges::EMPTY_RANGE == RowRanges::Range::Union(range0, range1));
  }
}

TEST(TestRowRanges, RangeIntersection) {
  {
    RowRanges::Range range0{10, 30};
    RowRanges::Range range1{20, 45};

    RowRanges::Range result{20, 30};
    ASSERT_TRUE(result == RowRanges::Range::Intersection(range0, range1));
  }

  {
    RowRanges::Range range0{10, 30};
    RowRanges::Range range1{35, 45};

    ASSERT_TRUE(RowRanges::EMPTY_RANGE == RowRanges::Range::Intersection(range0, range1));
  }
}

TEST(TestRowRanges, IsOverlapping) {
  std::vector<RowRanges::Range> ranges;
  ranges.push_back({1, 15});
  ranges.push_back({23, 39});
  ranges.push_back({29, 44});

  // {1, 15}, {23, 44}
  RowRanges row_ranges{std::move(ranges)};

  ASSERT_TRUE(row_ranges.IsOverlapping(1, 1));
  ASSERT_TRUE(row_ranges.IsOverlapping(15, 15));
  ASSERT_TRUE(row_ranges.IsOverlapping(23, 23));
  ASSERT_TRUE(row_ranges.IsOverlapping(44, 44));

  ASSERT_TRUE(row_ranges.IsOverlapping(10, 10));
  ASSERT_TRUE(row_ranges.IsOverlapping(30, 30));

  ASSERT_TRUE(row_ranges.IsOverlapping(3, 15));
  ASSERT_TRUE(row_ranges.IsOverlapping(15, 23));
  ASSERT_TRUE(row_ranges.IsOverlapping(5, 20));
  ASSERT_TRUE(row_ranges.IsOverlapping(20, 25));
  ASSERT_TRUE(row_ranges.IsOverlapping(20, 50));
  ASSERT_TRUE(row_ranges.IsOverlapping(10, 50));
  ASSERT_TRUE(row_ranges.IsOverlapping(40, 56));
  ASSERT_TRUE(row_ranges.IsOverlapping(30, 38));

  ASSERT_FALSE(row_ranges.IsOverlapping(0, 0));
  ASSERT_FALSE(row_ranges.IsOverlapping(16, 16));
  ASSERT_FALSE(row_ranges.IsOverlapping(22, 22));
  ASSERT_FALSE(row_ranges.IsOverlapping(45, 45));
  ASSERT_FALSE(row_ranges.IsOverlapping(16, 22));
  ASSERT_FALSE(row_ranges.IsOverlapping(17, 17));
  ASSERT_FALSE(row_ranges.IsOverlapping(45, 100));
}

TEST(TestRowRanges, Intersection) {
  RowRanges row_ranges0;
  row_ranges0.Add({113, 241});
  row_ranges0.Add({550, 667});

  {
    RowRanges row_ranges1;
    row_ranges1.Add({101, 113});
    row_ranges1.Add({241, 333});

    RowRanges result;
    result.Add({113, 113});
    result.Add({241, 241});
    ASSERT_TRUE(result == RowRanges::Intersection(row_ranges0, row_ranges1));
  }

  {
    RowRanges row_ranges1;
    row_ranges1.Add({101, 113});
    row_ranges1.Add({309, 550});

    RowRanges result;
    result.Add({113, 113});
    result.Add({550, 550});
    ASSERT_TRUE(result == RowRanges::Intersection(row_ranges0, row_ranges1));
  }

  {
    RowRanges row_ranges1;
    row_ranges1.Add({241, 289});
    row_ranges1.Add({309, 550});

    RowRanges result;
    result.Add({241, 241});
    result.Add({550, 550});
    ASSERT_TRUE(result == RowRanges::Intersection(row_ranges0, row_ranges1));
  }

  {
    RowRanges row_ranges1;
    row_ranges1.Add({309, 550});
    row_ranges1.Add({667, 706});

    RowRanges result;
    result.Add({550, 550});
    result.Add({667, 667});
    ASSERT_TRUE(result == RowRanges::Intersection(row_ranges0, row_ranges1));
  }

  {
    RowRanges row_ranges1;
    row_ranges1.Add({667, 706});
    row_ranges1.Add({997, 1023});

    RowRanges result;
    result.Add({667, 667});
    ASSERT_TRUE(result == RowRanges::Intersection(row_ranges0, row_ranges1));
  }

  {
    RowRanges row_ranges1;
    row_ranges1.Add({10, 197});
    row_ranges1.Add({278, 403});

    RowRanges result;
    result.Add({113, 197});
    ASSERT_TRUE(result == RowRanges::Intersection(row_ranges0, row_ranges1));
  }

  {
    RowRanges row_ranges1;
    row_ranges1.Add({10, 197});
    row_ranges1.Add({278, 601});

    RowRanges result;
    result.Add({113, 197});
    result.Add({550, 601});
    ASSERT_TRUE(result == RowRanges::Intersection(row_ranges0, row_ranges1));
  }

  {
    RowRanges row_ranges1;
    row_ranges1.Add({10, 1003});

    RowRanges result;
    result.Add({113, 241});
    result.Add({550, 667});
    ASSERT_TRUE(result == RowRanges::Intersection(row_ranges0, row_ranges1));
  }

  {
    RowRanges row_ranges1;
    row_ranges1.Add({273, 498});
    row_ranges1.Add({703, 1021});
    ASSERT_TRUE(
        0 == RowRanges::Intersection(row_ranges0, row_ranges1).GetRanges().size());
  }
}

} // namespace parquet
