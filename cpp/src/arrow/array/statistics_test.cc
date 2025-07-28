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

#include <limits>
#include <variant>

#include <gtest/gtest.h>

#include "arrow/array/statistics.h"
#include "arrow/compare.h"

namespace arrow {

TEST(TestArrayStatistics, NullCount) {
  ArrayStatistics statistics;
  ASSERT_FALSE(statistics.null_count.has_value());
  statistics.null_count = 29;
  ASSERT_TRUE(statistics.null_count.has_value());
  ASSERT_EQ(29, statistics.null_count.value());
}

TEST(TestArrayStatistics, DistinctCount) {
  ArrayStatistics statistics;
  ASSERT_FALSE(statistics.distinct_count.has_value());
  statistics.distinct_count = 29;
  ASSERT_TRUE(statistics.distinct_count.has_value());
  ASSERT_EQ(29, statistics.distinct_count.value());
}

TEST(TestArrayStatistics, AverageByteWidth) {
  ArrayStatistics statistics;
  ASSERT_FALSE(statistics.average_byte_width.has_value());
  ASSERT_FALSE(statistics.is_average_byte_width_exact);
  statistics.average_byte_width = 4.2;
  ASSERT_TRUE(statistics.average_byte_width.has_value());
  ASSERT_DOUBLE_EQ(4.2, statistics.average_byte_width.value());
  statistics.is_average_byte_width_exact = true;
  ASSERT_TRUE(statistics.is_average_byte_width_exact);
}

TEST(TestArrayStatistics, Min) {
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

TEST(TestArrayStatistics, Max) {
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

TEST(TestArrayStatistics, Equals) {
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

  statistics1.average_byte_width = 2.9;
  ASSERT_NE(statistics1, statistics2);
  statistics2.average_byte_width = 2.9;
  ASSERT_EQ(statistics1, statistics2);

  statistics1.is_average_byte_width_exact = true;
  ASSERT_NE(statistics1, statistics2);
  statistics2.is_average_byte_width_exact = true;
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

  statistics1.is_max_exact = true;
  ASSERT_NE(statistics1, statistics2);
  statistics2.is_max_exact = true;
  ASSERT_EQ(statistics1, statistics2);

  // Test different ArrayStatistics::ValueType
  statistics1.max = static_cast<uint64_t>(29);
  statistics1.max = static_cast<int64_t>(29);
  ASSERT_NE(statistics1, statistics2);
}

class TestArrayStatisticsEqualityDoubleValue : public ::testing::Test {
 protected:
  ArrayStatistics statistics1_;
  ArrayStatistics statistics2_;
  EqualOptions options_ = EqualOptions::Defaults();
};

TEST_F(TestArrayStatisticsEqualityDoubleValue, ExactValue) {
  statistics2_.min = 29.0;
  statistics1_.min = 29.0;
  ASSERT_EQ(statistics1_, statistics2_);
  statistics2_.min = 30.0;
  ASSERT_NE(statistics1_, statistics2_);
}

TEST_F(TestArrayStatisticsEqualityDoubleValue, SignedZero) {
  statistics1_.min = +0.0;
  statistics2_.min = -0.0;
  ASSERT_TRUE(statistics1_.Equals(statistics2_, options_.signed_zeros_equal(true)));
  ASSERT_FALSE(statistics1_.Equals(statistics2_, options_.signed_zeros_equal(false)));
}

TEST_F(TestArrayStatisticsEqualityDoubleValue, Infinity) {
  auto infinity = std::numeric_limits<double>::infinity();
  statistics1_.min = infinity;
  statistics2_.min = infinity;
  ASSERT_EQ(statistics1_, statistics2_);
  statistics1_.min = -infinity;
  ASSERT_NE(statistics1_, statistics2_);
}

TEST_F(TestArrayStatisticsEqualityDoubleValue, NaN) {
  statistics1_.min = std::numeric_limits<double>::quiet_NaN();
  statistics2_.min = std::numeric_limits<double>::quiet_NaN();
  ASSERT_TRUE(statistics1_.Equals(statistics2_, options_.nans_equal(true)));
  ASSERT_FALSE(statistics1_.Equals(statistics2_, options_.nans_equal(false)));
}

TEST_F(TestArrayStatisticsEqualityDoubleValue, ApproximateEquals) {
  statistics1_.max = 0.5001f;
  statistics2_.max = 0.5;
  ASSERT_FALSE(statistics1_.Equals(statistics2_, options_.atol(1e-3)));
  ASSERT_TRUE(statistics1_.Equals(statistics2_, options_.atol(1e-3).use_atol(true)));
}

}  // namespace arrow
