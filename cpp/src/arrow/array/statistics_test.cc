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
#include <memory>

#include <gtest/gtest.h>

#include "arrow/array/statistics.h"
#include "arrow/compare.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

TEST(ArrayStatisticsTest, TestNullCount) {
  ArrayStatistics statistics;
  ASSERT_FALSE(statistics.null_count.has_value());
  statistics.null_count = 29;
  ASSERT_TRUE(statistics.null_count.has_value());
  ASSERT_EQ(29, statistics.null_count.value());
}

TEST(ArrayStatisticsTest, TestDistinctCount) {
  ArrayStatistics statistics;
  ASSERT_FALSE(statistics.distinct_count.has_value());
  statistics.distinct_count = 29;
  ASSERT_TRUE(statistics.distinct_count.has_value());
  ASSERT_EQ(29, statistics.distinct_count.value());
}

TEST(ArrayStatisticsTest, TestMin) {
  ArrayStatistics statistics;
  ASSERT_FALSE(statistics.min.has_value());
  ASSERT_FALSE(statistics.is_min_exact);
  statistics.min = static_cast<uint64_t>(29);
  statistics.is_min_exact = true;
  ASSERT_TRUE(statistics.min.has_value());
  ASSERT_TRUE(std::holds_alternative<uint64_t>(statistics.min.value()));
  ASSERT_EQ(29, std::get<uint64_t>(statistics.min.value()));
  ASSERT_TRUE(statistics.is_min_exact);
}

TEST(ArrayStatisticsTest, TestMax) {
  ArrayStatistics statistics;
  ASSERT_FALSE(statistics.max.has_value());
  ASSERT_FALSE(statistics.is_max_exact);
  statistics.max = std::string("hello");
  statistics.is_max_exact = false;
  ASSERT_TRUE(statistics.max.has_value());
  ASSERT_TRUE(std::holds_alternative<std::string>(statistics.max.value()));
  ASSERT_EQ("hello", std::get<std::string>(statistics.max.value()));
  ASSERT_FALSE(statistics.is_max_exact);
}

TEST(ArrayStatisticsTest, TestEquality) {
  ArrayStatistics statistics1;
  ArrayStatistics statistics2;

  ASSERT_EQ(statistics1, statistics2);
  statistics1.null_count = 29;
  ASSERT_NE(statistics1, statistics2);
  statistics2.null_count = 29;
  ASSERT_EQ(statistics1, statistics2);

  statistics1.distinct_count = 2929;
  ASSERT_NE(statistics1, statistics2);
  statistics2.distinct_count = 2929;
  ASSERT_EQ(statistics1, statistics2);

  statistics1.min = std::string("world");
  ASSERT_NE(statistics1, statistics2);
  statistics2.min = std::string("world");
  ASSERT_EQ(statistics1, statistics2);

  statistics1.is_min_exact = true;
  ASSERT_NE(statistics1, statistics2);
  statistics2.is_min_exact = true;
  ASSERT_EQ(statistics1, statistics2);

  statistics1.max = static_cast<int64_t>(-29);
  ASSERT_NE(statistics1, statistics2);
  statistics2.max = static_cast<int64_t>(-29);
  ASSERT_EQ(statistics1, statistics2);
  statistics2.max = static_cast<int64_t>(2);
  ASSERT_NE(statistics1, statistics2);

  statistics1.max = std::nullopt;
  statistics2.max = std::nullopt;
  // the state of both of them are std::nullopt
  ASSERT_EQ(statistics1.max, statistics2.max);
  // the state of one of them is std::nullopt
  statistics1.max = std::shared_ptr<Scalar>();
  ASSERT_NE(statistics1, statistics2);
  // the state of both of them are empty shared_ptr
  statistics2.max = std::shared_ptr<Scalar>();
  ASSERT_EQ(statistics1, statistics2);
  ASSERT_OK_AND_ASSIGN(statistics1.max, MakeScalar(int64(), 5));
  // the state of one of them is empty shared_ptr
  ASSERT_NE(statistics1, statistics2);
  // the state of one of them has different type
  statistics2.max = static_cast<int64_t>(10);
  ASSERT_NE(statistics1.max, statistics2.max);
  ASSERT_OK_AND_ASSIGN(statistics2.max, MakeScalar(int64(), 5));
  // the state of both of them are equal
  ASSERT_EQ(statistics1, statistics2);

  statistics1.is_max_exact = true;
  ASSERT_NE(statistics1, statistics2);
  statistics2.is_max_exact = true;
  ASSERT_EQ(statistics1, statistics2);
}
TEST(ArrayStatisticsTest, EqualOptions) {
  auto list_nan_1 = std::make_shared<ListScalar>(
      ArrayFromJSON(float32(), R"([0,null,NaN])"), list(float32()));
  auto list_nan_2 = std::make_shared<ListScalar>(
      ArrayFromJSON(float32(), R"([0,null,NaN])"), list(float32()));
  ArrayStatistics statistics1;
  ArrayStatistics statistics2;
  statistics1.max = list_nan_1;
  statistics2.max = list_nan_2;
  ASSERT_FALSE(statistics1.Equals(statistics2));
  ASSERT_TRUE(statistics1.Equals(statistics2, EqualOptions::Defaults().nans_equal(true)));
}

}  // namespace arrow
