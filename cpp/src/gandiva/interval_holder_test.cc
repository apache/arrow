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

#include "gandiva/interval_holder.h"

#include <gtest/gtest.h>

#include <memory>

#include "arrow/testing/gtest_util.h"
#include "gandiva/execution_context.h"

namespace gandiva {

class TestIntervalHolder : public ::testing::Test {
 protected:
  ExecutionContext execution_context_;
};

TEST_F(TestIntervalHolder, TestMatchAllPeriods) {
  EXPECT_OK_AND_ASSIGN(auto interval_days_holder, IntervalDaysHolder::Make(0));
  EXPECT_OK_AND_ASSIGN(auto interval_years_holder, IntervalYearsHolder::Make(0));

  auto& cast_interval_day = *interval_days_holder;
  auto& cast_interval_year = *interval_years_holder;

  // Pass only numbers to cast
  bool out_valid;
  std::string data("73834992");
  int64_t response =
      cast_interval_day(&execution_context_, data.data(), 8, true, &out_valid);
  int64_t qty_days_in_response = 0;
  int64_t qty_millis_in_response = 73834992;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  int32_t response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 8, true, &out_valid);
  EXPECT_EQ(response_interval_yrs, 73834992);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());

  data = "1";
  response = cast_interval_day(&execution_context_, data.data(), 1, true, &out_valid);
  qty_days_in_response = 0;
  qty_millis_in_response = 1;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  data = "PT0.001S";
  response = cast_interval_day(&execution_context_, data.data(), 8, true, &out_valid);
  qty_days_in_response = 0;
  qty_millis_in_response = 1;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  // Pass only years and days to cast
  data = "P12Y15D";
  response = cast_interval_day(&execution_context_, data.data(), 7, true, &out_valid);
  qty_days_in_response = 15;
  qty_millis_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 7, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response_interval_yrs, 144);

  // Pass years and days and months to cast
  data = "P12Y2M15D";
  response = cast_interval_day(&execution_context_, data.data(), 9, true, &out_valid);
  qty_days_in_response = 15;
  qty_millis_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 9, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response_interval_yrs, 146);

  // Pass days and months to cast
  data = "P5M13D";
  response = cast_interval_day(&execution_context_, data.data(), 6, true, &out_valid);
  qty_days_in_response = 13;
  qty_millis_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 6, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response_interval_yrs, 5);

  // Pass all possible fields cast
  data = "P2Y5M13DT10H42M21S";
  response = cast_interval_day(&execution_context_, data.data(), 18, true, &out_valid);
  qty_days_in_response = 13;
  qty_millis_in_response = 38541000;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 18, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response_interval_yrs, 29);

  // Pass only time fields cast
  data = "PT10H42M21S";
  response = cast_interval_day(&execution_context_, data.data(), 11, true, &out_valid);
  qty_days_in_response = 0;
  qty_millis_in_response = 38541000;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 11, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response_interval_yrs, 0);

  // Pass only time fields to cast without hours
  data = "PT42M21S";
  response = cast_interval_day(&execution_context_, data.data(), 8, true, &out_valid);
  qty_days_in_response = 0;
  qty_millis_in_response = 2541000;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 8, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response_interval_yrs, 0);

  // Pass only weeks to cast
  data = "P25W";
  response = cast_interval_day(&execution_context_, data.data(), 4, true, &out_valid);
  qty_days_in_response = 175;
  qty_millis_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 4, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response_interval_yrs, 0);

  execution_context_.Reset();

  // Pass all possible fields using real values
  data = "P2,5Y5,5M13,5DT10,5H42,5M21,5S";
  response = cast_interval_day(&execution_context_, data.data(), 30, true, &out_valid);
  qty_days_in_response = 13;
  qty_millis_in_response = 40371500;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 30, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response_interval_yrs, 35);

  // Pass all possible fields using real values
  data = "P2.5Y5.5M13.5DT10.5H42.5M21.5S";
  response = cast_interval_day(&execution_context_, data.data(), 30, true, &out_valid);
  qty_days_in_response = 13;
  qty_millis_in_response = 40371500;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 30, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response_interval_yrs, 35);

  // Pass negative value
  data = "P-1D";
  response = cast_interval_day(&execution_context_, data.data(), 4, true, &out_valid);
  qty_days_in_response = -1;
  qty_millis_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response,
            (qty_millis_in_response << 32) | (qty_days_in_response & 0x00000000FFFFFFFF));

  data = "P-2D";
  response = cast_interval_day(&execution_context_, data.data(), 4, true, &out_valid);
  qty_days_in_response = -2;
  qty_millis_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response,
            (qty_millis_in_response << 32) | (qty_days_in_response & 0x00000000FFFFFFFF));

  data = "P-1W";
  response = cast_interval_day(&execution_context_, data.data(), 4, true, &out_valid);
  qty_days_in_response = -7;
  qty_millis_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response,
            (qty_millis_in_response << 32) | (qty_days_in_response & 0x00000000FFFFFFFF));

  data = "P-1M";
  response = cast_interval_day(&execution_context_, data.data(), 4, true, &out_valid);
  qty_days_in_response = 0;
  qty_millis_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response,
            (qty_millis_in_response << 32) | (qty_days_in_response & 0x00000000FFFFFFFF));

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 4, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response_interval_yrs, -1);

  data = "P-1Y";
  response = cast_interval_day(&execution_context_, data.data(), 4, true, &out_valid);
  qty_days_in_response = 0;
  qty_millis_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response,
            (qty_millis_in_response << 32) | (qty_days_in_response & 0x00000000FFFFFFFF));

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 4, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response_interval_yrs, -12);

  data = "P-1Y-2M";
  response = cast_interval_day(&execution_context_, data.data(), 7, true, &out_valid);
  qty_days_in_response = 0;
  qty_millis_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response,
            (qty_millis_in_response << 32) | (qty_days_in_response & 0x00000000FFFFFFFF));

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 7, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response_interval_yrs, -14);
}

TEST_F(TestIntervalHolder, TestMatchErrorsForCastIntervalDay) {
  EXPECT_OK_AND_ASSIGN(auto interval_days_holder, IntervalDaysHolder::Make(0));
  EXPECT_OK_AND_ASSIGN(auto interval_years_holder, IntervalYearsHolder::Make(0));

  auto& cast_interval_day = *interval_days_holder;
  auto& cast_interval_year = *interval_years_holder;

  // Pass an empty string
  bool out_valid;
  std::string data(" ");
  int64_t response =
      cast_interval_day(&execution_context_, data.data(), 1, true, &out_valid);
  EXPECT_EQ(response, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  int32_t response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 1, true, &out_valid);
  EXPECT_EQ(response_interval_yrs, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  data = "";
  response = cast_interval_day(&execution_context_, data.data(), 0, true, &out_valid);
  EXPECT_EQ(response, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 0, true, &out_valid);
  EXPECT_EQ(response_interval_yrs, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  // Pass only days before years
  data = "P15D12Y";
  response = cast_interval_day(&execution_context_, data.data(), 7, true, &out_valid);
  EXPECT_EQ(response, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 7, true, &out_valid);
  EXPECT_EQ(response_interval_yrs, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  // Pass years and days and months in wrong order
  data = "P12M15D2Y";
  response = cast_interval_day(&execution_context_, data.data(), 9, true, &out_valid);
  EXPECT_EQ(response, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 9, true, &out_valid);
  EXPECT_EQ(response_interval_yrs, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  // Forget the P in the first position
  data = "5M13D";
  response = cast_interval_day(&execution_context_, data.data(), 5, true, &out_valid);
  EXPECT_EQ(response, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 5, true, &out_valid);
  EXPECT_EQ(response_interval_yrs, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  // Use m instead M in the period format
  data = "P2Y5M13DT10H42m21S";
  response = cast_interval_day(&execution_context_, data.data(), 18, true, &out_valid);
  EXPECT_EQ(response, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 18, true, &out_valid);
  EXPECT_EQ(response_interval_yrs, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  // Does not pass the T when defining only time fields
  data = "P10H42M21S";
  response = cast_interval_day(&execution_context_, data.data(), 10, true, &out_valid);
  EXPECT_EQ(response, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 10, true, &out_valid);
  EXPECT_EQ(response_interval_yrs, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  // Pass weeks with other variables
  data = "P2Y25W2M3D";
  response = cast_interval_day(&execution_context_, data.data(), 10, true, &out_valid);
  EXPECT_EQ(response, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();

  response_interval_yrs =
      cast_interval_year(&execution_context_, data.data(), 10, true, &out_valid);
  EXPECT_EQ(response_interval_yrs, 0);
  EXPECT_FALSE(out_valid);
  EXPECT_TRUE(execution_context_.has_error());

  execution_context_.Reset();
}

TEST_F(TestIntervalHolder, TestUsingWeekFormatterForCastIntervalDay) {
  EXPECT_OK_AND_ASSIGN(auto interval_days_holder, IntervalDaysHolder::Make(0));
  auto& cast_interval_day = *interval_days_holder;

  bool out_valid;
  std::string data("P1W");
  int64_t response =
      cast_interval_day(&execution_context_, data.data(), 3, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, 7);

  data = "P10W";
  response = cast_interval_day(&execution_context_, data.data(), 4, true, &out_valid);
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, 70);

  execution_context_.Reset();
}

TEST_F(TestIntervalHolder, TestUsingCompleteFormatterForCastIntervalDay) {
  EXPECT_OK_AND_ASSIGN(auto interval_days_holder, IntervalDaysHolder::Make(0));
  auto& cast_interval_day = *interval_days_holder;

  bool out_valid;
  std::string data("1742461111");
  int64_t response =
      cast_interval_day(&execution_context_, data.data(), 10, true, &out_valid);
  int64_t qty_days_in_response = 20;
  int64_t qty_millis_in_response = 14461111;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  data = "P1Y1M1DT1H1M1S";
  response = cast_interval_day(&execution_context_, data.data(), 14, true, &out_valid);
  qty_days_in_response = 1;
  qty_millis_in_response = 3661000;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  data = "PT48H1M1S";
  response = cast_interval_day(&execution_context_, data.data(), 9, true, &out_valid);
  qty_days_in_response = 2;
  qty_millis_in_response = 61000;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  data = "PT1S";
  response = cast_interval_day(&execution_context_, data.data(), 4, true, &out_valid);
  qty_days_in_response = 0;
  qty_millis_in_response = 1000;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  data = "P10DT1S";
  response = cast_interval_day(&execution_context_, data.data(), 7, true, &out_valid);
  qty_days_in_response = 10;
  qty_millis_in_response = 1000;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  execution_context_.Reset();

  data = "P0DT0S";
  response = cast_interval_day(&execution_context_, data.data(), 6, true, &out_valid);
  qty_days_in_response = 0;
  qty_millis_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_millis_in_response << 32) | qty_days_in_response);

  execution_context_.Reset();
}

TEST_F(TestIntervalHolder, TestUsingCompleteFormatterForCastIntervalYear) {
  EXPECT_OK_AND_ASSIGN(auto interval_years_holder, IntervalYearsHolder::Make(0));
  auto& cast_interval_years = *interval_years_holder;

  bool out_valid;
  std::string data("65851111");
  int32_t response =
      cast_interval_years(&execution_context_, data.data(), 8, true, &out_valid);
  int32_t qty_yrs_in_response = 0;
  int32_t qty_months_in_response = 65851111;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_yrs_in_response * 12) + qty_months_in_response);

  data = "P1Y1M1DT1H1M1S";
  response = cast_interval_years(&execution_context_, data.data(), 14, true, &out_valid);
  qty_yrs_in_response = 1;
  qty_months_in_response = 1;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_yrs_in_response * 12) + qty_months_in_response);

  data = "PT48H1M1S";
  response = cast_interval_years(&execution_context_, data.data(), 9, true, &out_valid);
  qty_yrs_in_response = 0;
  qty_months_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_yrs_in_response * 12) + qty_months_in_response);

  data = "P1Y";
  response = cast_interval_years(&execution_context_, data.data(), 3, true, &out_valid);
  qty_yrs_in_response = 1;
  qty_months_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_yrs_in_response * 12) + qty_months_in_response);

  data = "P10MT1S";
  response = cast_interval_years(&execution_context_, data.data(), 7, true, &out_valid);
  qty_yrs_in_response = 0;
  qty_months_in_response = 10;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_yrs_in_response * 12) + qty_months_in_response);

  execution_context_.Reset();

  data = "P0Y0M";
  response = cast_interval_years(&execution_context_, data.data(), 5, true, &out_valid);
  qty_yrs_in_response = 0;
  qty_months_in_response = 0;
  EXPECT_TRUE(out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(response, (qty_yrs_in_response * 12) + qty_months_in_response);

  execution_context_.Reset();
}
}  // namespace gandiva
