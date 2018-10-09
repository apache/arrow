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

#include <gtest/gtest.h>
#include <math.h>
#include <time.h>
#include "arrow/memory_pool.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::date64;
using arrow::float32;
using arrow::int32;
using arrow::int64;

class TestProjector : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

int32_t MillisInDay(int32_t hh, int32_t mm, int32_t ss, int32_t millis) {
  int32_t mins = hh * 60 + mm;
  int32_t secs = mins * 60 + ss;

  return secs * 1000 + millis;
}

int64_t MillisSince(time_t base_line, int32_t yy, int32_t mm, int32_t dd, int32_t hr,
                    int32_t min, int32_t sec, int32_t millis) {
  struct tm given_ts;
  memset(&given_ts, 0, sizeof(struct tm));
  given_ts.tm_year = (yy - 1900);
  given_ts.tm_mon = (mm - 1);
  given_ts.tm_mday = dd;
  given_ts.tm_hour = hr;
  given_ts.tm_min = min;
  given_ts.tm_sec = sec;

  return (lround(difftime(mktime(&given_ts), base_line)) * 1000 + millis);
}

TEST_F(TestProjector, TestIsNull) {
  auto d0 = field("d0", date64());
  auto t0 = field("t0", time32(arrow::TimeUnit::MILLI));
  auto schema = arrow::schema({d0, t0});

  // output fields
  auto b0 = field("isnull", boolean());

  // isnull and isnotnull
  auto isnull_expr = TreeExprBuilder::MakeExpression("isnull", {d0}, b0);
  auto isnotnull_expr = TreeExprBuilder::MakeExpression("isnotnull", {t0}, b0);

  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {isnull_expr, isnotnull_expr}, &projector);
  ASSERT_TRUE(status.ok());

  int num_records = 4;
  std::vector<int64_t> d0_data = {0, 100, 0, 1000};
  auto t0_data = {0, 100, 0, 1000};
  auto validity = {false, true, false, true};
  auto d0_array =
      MakeArrowTypeArray<arrow::Date64Type, int64_t>(date64(), d0_data, validity);
  auto t0_array = MakeArrowTypeArray<arrow::Time32Type, int32_t>(
      time32(arrow::TimeUnit::MILLI), t0_data, validity);

  // expected output
  auto exp_isnull =
      MakeArrowArrayBool({true, false, true, false}, {true, true, true, true});
  auto exp_isnotnull = MakeArrowArrayBool(validity, {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {d0_array, t0_array});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_isnull, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_isnotnull, outputs.at(1));
}

TEST_F(TestProjector, TestDateTime) {
  auto field0 = field("f0", date64());
  auto field2 = field("f2", timestamp(arrow::TimeUnit::MILLI));
  auto schema = arrow::schema({field0, field2});

  // output fields
  auto field_year = field("yy", int64());
  auto field_month = field("mm", int64());
  auto field_day = field("dd", int64());
  auto field_hour = field("hh", int64());

  // extract year and month from date
  auto date2year_expr =
      TreeExprBuilder::MakeExpression("extractYear", {field0}, field_year);
  auto date2month_expr =
      TreeExprBuilder::MakeExpression("extractMonth", {field0}, field_month);

  // extract month and day from timestamp
  auto ts2month_expr =
      TreeExprBuilder::MakeExpression("extractMonth", {field2}, field_month);
  auto ts2day_expr = TreeExprBuilder::MakeExpression("extractDay", {field2}, field_day);

  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(
      schema, {date2year_expr, date2month_expr, ts2month_expr, ts2day_expr}, &projector);
  ASSERT_TRUE(status.ok());

  struct tm y1970;
  memset(&y1970, 0, sizeof(struct tm));
  y1970.tm_year = 70;
  y1970.tm_mon = 0;
  y1970.tm_mday = 1;
  y1970.tm_hour = 0;
  y1970.tm_min = 0;
  y1970.tm_sec = 0;
  time_t epoch = mktime(&y1970);

  // Create a row-batch with some sample data
  int num_records = 4;
  auto validity = {true, true, true, true};
  std::vector<int64_t> field0_data = {MillisSince(epoch, 2000, 1, 1, 5, 0, 0, 0),
                                      MillisSince(epoch, 1999, 12, 31, 5, 0, 0, 0),
                                      MillisSince(epoch, 2015, 6, 30, 20, 0, 0, 0),
                                      MillisSince(epoch, 2015, 7, 1, 20, 0, 0, 0)};
  auto array0 =
      MakeArrowTypeArray<arrow::Date64Type, int64_t>(date64(), field0_data, validity);

  std::vector<int64_t> field2_data = {MillisSince(epoch, 1999, 12, 31, 5, 0, 0, 0),
                                      MillisSince(epoch, 2000, 1, 2, 5, 0, 0, 0),
                                      MillisSince(epoch, 2015, 7, 1, 1, 0, 0, 0),
                                      MillisSince(epoch, 2015, 6, 29, 23, 0, 0, 0)};

  auto array2 = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      arrow::timestamp(arrow::TimeUnit::MILLI), field2_data, validity);

  // expected output
  // date 2 year and date 2 month
  auto exp_yy_from_date = MakeArrowArrayInt64({2000, 1999, 2015, 2015}, validity);
  auto exp_mm_from_date = MakeArrowArrayInt64({1, 12, 6, 7}, validity);

  // ts 2 month and ts 2 day
  auto exp_mm_from_ts = MakeArrowArrayInt64({12, 1, 7, 6}, validity);
  auto exp_dd_from_ts = MakeArrowArrayInt64({31, 2, 1, 29}, validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array2});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_yy_from_date, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_mm_from_date, outputs.at(1));
  EXPECT_ARROW_ARRAY_EQUALS(exp_mm_from_ts, outputs.at(2));
  EXPECT_ARROW_ARRAY_EQUALS(exp_dd_from_ts, outputs.at(3));
}

TEST_F(TestProjector, TestTime) {
  auto field0 = field("f0", time32(arrow::TimeUnit::MILLI));
  auto schema = arrow::schema({field0});

  auto field_min = field("mm", int64());
  auto field_hour = field("hh", int64());

  // extract day and hour from time32
  auto time2min_expr =
      TreeExprBuilder::MakeExpression("extractMinute", {field0}, field_min);
  auto time2hour_expr =
      TreeExprBuilder::MakeExpression("extractHour", {field0}, field_hour);

  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {time2min_expr, time2hour_expr}, &projector);
  ASSERT_TRUE(status.ok());

  // create input data
  int num_records = 4;
  auto validity = {true, true, true, true};
  std::vector<int32_t> field_data = {
      MillisInDay(5, 35, 25, 0),  // 5:35:25
      MillisInDay(0, 59, 0, 0),   // 0:59:12
      MillisInDay(12, 30, 0, 0),  // 12:30:0
      MillisInDay(23, 0, 0, 0)    // 23:0:0
  };
  auto array = MakeArrowTypeArray<arrow::Time32Type, int32_t>(
      time32(arrow::TimeUnit::MILLI), field_data, validity);

  // expected output
  auto exp_min = MakeArrowArrayInt64({35, 59, 30, 0}, validity);
  auto exp_hour = MakeArrowArrayInt64({5, 0, 12, 23}, validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_min, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_hour, outputs.at(1));
}

TEST_F(TestProjector, TestTimestampDiff) {
  auto f0 = field("f0", timestamp(arrow::TimeUnit::MILLI));
  auto f1 = field("f1", timestamp(arrow::TimeUnit::MILLI));
  auto schema = arrow::schema({f0, f1});

  // output fields
  auto diff_seconds = field("ss", int32());

  // get diff
  auto diff_secs_expr =
      TreeExprBuilder::MakeExpression("timestampdiffSecond", {f0, f1}, diff_seconds);

  auto diff_mins_expr =
      TreeExprBuilder::MakeExpression("timestampdiffMinute", {f0, f1}, diff_seconds);

  auto diff_hours_expr =
      TreeExprBuilder::MakeExpression("timestampdiffHour", {f0, f1}, diff_seconds);

  auto diff_days_expr =
      TreeExprBuilder::MakeExpression("timestampdiffDay", {f0, f1}, diff_seconds);

  auto diff_weeks_expr =
      TreeExprBuilder::MakeExpression("timestampdiffWeek", {f0, f1}, diff_seconds);

  auto diff_months_expr =
      TreeExprBuilder::MakeExpression("timestampdiffMonth", {f0, f1}, diff_seconds);

  auto diff_quarters_expr =
      TreeExprBuilder::MakeExpression("timestampdiffQuarter", {f0, f1}, diff_seconds);

  auto diff_years_expr =
      TreeExprBuilder::MakeExpression("timestampdiffYear", {f0, f1}, diff_seconds);

  std::shared_ptr<Projector> projector;
  auto exprs = {diff_secs_expr,  diff_mins_expr,   diff_hours_expr,    diff_days_expr,
                diff_weeks_expr, diff_months_expr, diff_quarters_expr, diff_years_expr};
  Status status = Projector::Make(schema, exprs, &projector);
  ASSERT_TRUE(status.ok());

  struct tm y1970;
  memset(&y1970, 0, sizeof(struct tm));
  y1970.tm_year = 70;
  y1970.tm_mon = 0;
  y1970.tm_mday = 1;
  y1970.tm_hour = 0;
  y1970.tm_min = 0;
  y1970.tm_sec = 0;
  time_t epoch = mktime(&y1970);

  // 2015-09-10T20:49:42.000
  auto start_millis = MillisSince(epoch, 2015, 9, 10, 20, 49, 42, 0);
  // 2017-03-30T22:50:59.050
  auto end_millis = MillisSince(epoch, 2017, 3, 30, 22, 50, 59, 50);
  std::vector<int64_t> f0_data = {start_millis, end_millis,
                                  // 2015-09-10T20:49:42.999
                                  start_millis + 999,
                                  // 2015-09-10T20:49:42.999
                                  MillisSince(epoch, 2015, 9, 10, 20, 49, 42, 999)};
  std::vector<int64_t> f1_data = {end_millis, start_millis,
                                  // 2015-09-10T20:49:42.999
                                  start_millis + 999,
                                  // 2015-09-9T21:49:42.999 (23 hours behind)
                                  MillisSince(epoch, 2015, 9, 9, 21, 49, 42, 999)};

  int64_t num_records = f0_data.size();
  std::vector<bool> validity(num_records, true);
  auto array0 = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      arrow::timestamp(arrow::TimeUnit::MILLI), f0_data, validity);
  auto array1 = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      arrow::timestamp(arrow::TimeUnit::MILLI), f1_data, validity);

  // expected output
  std::vector<ArrayPtr> exp_output;
  exp_output.push_back(
      MakeArrowArrayInt32({48996077, -48996077, 0, -23 * 3600}, validity));
  exp_output.push_back(MakeArrowArrayInt32({816601, -816601, 0, -23 * 60}, validity));
  exp_output.push_back(MakeArrowArrayInt32({13610, -13610, 0, -23}, validity));
  exp_output.push_back(MakeArrowArrayInt32({567, -567, 0, 0}, validity));
  exp_output.push_back(MakeArrowArrayInt32({81, -81, 0, 0}, validity));
  exp_output.push_back(MakeArrowArrayInt32({18, -18, 0, 0}, validity));
  exp_output.push_back(MakeArrowArrayInt32({6, -6, 0, 0}, validity));
  exp_output.push_back(MakeArrowArrayInt32({1, -1, 0, 0}, validity));

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  for (uint32_t i = 0; i < exp_output.size(); i++) {
    EXPECT_ARROW_ARRAY_EQUALS(exp_output.at(i), outputs.at(i));
  }
}

TEST_F(TestProjector, TestMonthsBetween) {
  auto f0 = field("f0", arrow::date64());
  auto f1 = field("f1", arrow::date64());
  auto schema = arrow::schema({f0, f1});

  // output fields
  auto output = field("out", arrow::float64());

  auto months_between_expr =
      TreeExprBuilder::MakeExpression("months_between", {f0, f1}, output);

  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(schema, {months_between_expr}, &projector);
  std::cout << status.message();
  ASSERT_TRUE(status.ok());

  struct tm y1970;
  memset(&y1970, 0, sizeof(struct tm));
  y1970.tm_year = 70;
  y1970.tm_mon = 0;
  y1970.tm_mday = 1;
  y1970.tm_hour = 0;
  y1970.tm_min = 0;
  y1970.tm_sec = 0;
  time_t epoch = mktime(&y1970);

  // Create a row-batch with some sample data
  int num_records = 4;
  auto validity = {true, true, true, true};
  std::vector<int64_t> f0_data = {MillisSince(epoch, 1995, 3, 2, 0, 0, 0, 0),
                                  MillisSince(epoch, 1995, 2, 2, 0, 0, 0, 0),
                                  MillisSince(epoch, 1995, 3, 31, 0, 0, 0, 0),
                                  MillisSince(epoch, 1996, 3, 31, 0, 0, 0, 0)};

  auto array0 =
      MakeArrowTypeArray<arrow::Date64Type, int64_t>(date64(), f0_data, validity);

  std::vector<int64_t> f1_data = {MillisSince(epoch, 1995, 2, 2, 0, 0, 0, 0),
                                  MillisSince(epoch, 1995, 3, 2, 0, 0, 0, 0),
                                  MillisSince(epoch, 1995, 2, 28, 0, 0, 0, 0),
                                  MillisSince(epoch, 1996, 2, 29, 0, 0, 0, 0)};

  auto array1 =
      MakeArrowTypeArray<arrow::Date64Type, int64_t>(date64(), f1_data, validity);

  // expected output
  auto exp_output = MakeArrowArrayFloat64({1.0, -1.0, 1.0, 1.0}, validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_output, outputs.at(0));
}

}  // namespace gandiva
