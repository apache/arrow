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

#include <cmath>
#include <ctime>

#include "arrow/memory_pool.h"
#include "gandiva/precompiled/time_constants.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::date32;
using arrow::date64;
using arrow::float32;
using arrow::int32;
using arrow::int64;
using arrow::timestamp;

class DateTimeTestProjector : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

time_t Epoch() {
  // HACK: MSVC mktime() fails on UTC times before 1970-01-01 00:00:00.
  // But it first converts its argument from local time to UTC time,
  // so we ask for 1970-01-02 to avoid failing in timezones ahead of UTC.
  struct tm y1970;
  memset(&y1970, 0, sizeof(struct tm));
  y1970.tm_year = 70;
  y1970.tm_mon = 0;
  y1970.tm_mday = 2;
  y1970.tm_hour = 0;
  y1970.tm_min = 0;
  y1970.tm_sec = 0;
  time_t epoch = mktime(&y1970);
  if (epoch == static_cast<time_t>(-1)) {
    ARROW_LOG(FATAL) << "mktime() failed";
  }
  // Adjust for the 24h offset above.
  return epoch - 24 * 3600;
}

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

  time_t ts = mktime(&given_ts);
  if (ts == static_cast<time_t>(-1)) {
    ARROW_LOG(FATAL) << "mktime() failed";
  }
  // time_t is an arithmetic type on both POSIX and Windows, we can simply
  // subtract to get a duration in seconds.
  return static_cast<int64_t>(ts - base_line) * 1000 + millis;
}

int32_t DaysSince(time_t base_line, int32_t yy, int32_t mm, int32_t dd, int32_t hr,
                  int32_t min, int32_t sec, int32_t millis) {
  struct tm given_ts;
  memset(&given_ts, 0, sizeof(struct tm));
  given_ts.tm_year = (yy - 1900);
  given_ts.tm_mon = (mm - 1);
  given_ts.tm_mday = dd;
  given_ts.tm_hour = hr;
  given_ts.tm_min = min;
  given_ts.tm_sec = sec;

  time_t ts = mktime(&given_ts);
  if (ts == static_cast<time_t>(-1)) {
    ARROW_LOG(FATAL) << "mktime() failed";
  }
  // time_t is an arithmetic type on both POSIX and Windows, we can simply
  // subtract to get a duration in seconds.
  return static_cast<int32_t>(((ts - base_line) * 1000 + millis) / MILLIS_IN_DAY);
}

TEST_F(DateTimeTestProjector, TestIsNull) {
  auto d0 = field("d0", date64());
  auto t0 = field("t0", time32(arrow::TimeUnit::MILLI));
  auto schema = arrow::schema({d0, t0});

  // output fields
  auto b0 = field("isnull", boolean());

  // isnull and isnotnull
  auto isnull_expr = TreeExprBuilder::MakeExpression("isnull", {d0}, b0);
  auto isnotnull_expr = TreeExprBuilder::MakeExpression("isnotnull", {t0}, b0);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {isnull_expr, isnotnull_expr},
                                TestConfiguration(), &projector);
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

TEST_F(DateTimeTestProjector, TestDate32IsNull) {
  auto d0 = field("d0", date32());
  auto schema = arrow::schema({d0});

  // output fields
  auto b0 = field("isnull", boolean());

  // isnull and isnotnull
  auto isnull_expr = TreeExprBuilder::MakeExpression("isnull", {d0}, b0);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {isnull_expr}, TestConfiguration(), &projector);
  ASSERT_TRUE(status.ok());

  int num_records = 4;
  std::vector<int32_t> d0_data = {0, 100, 0, 1000};
  auto validity = {false, true, false, true};
  auto d0_array =
      MakeArrowTypeArray<arrow::Date32Type, int32_t>(date32(), d0_data, validity);

  // expected output
  auto exp_isnull =
      MakeArrowArrayBool({true, false, true, false}, {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {d0_array});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_isnull, outputs.at(0));
}

TEST_F(DateTimeTestProjector, TestDateTime) {
  auto field0 = field("f0", date64());
  auto field1 = field("f1", date32());
  auto field2 = field("f2", timestamp(arrow::TimeUnit::MILLI));
  auto schema = arrow::schema({field0, field1, field2});

  // output fields
  auto field_year = field("yy", int64());
  auto field_month = field("mm", int64());
  auto field_day = field("dd", int64());
  auto field_hour = field("hh", int64());
  auto field_date64 = field("date64", date64());

  // extract year and month from date
  auto date2year_expr =
      TreeExprBuilder::MakeExpression("extractYear", {field0}, field_year);
  auto date2month_expr =
      TreeExprBuilder::MakeExpression("extractMonth", {field0}, field_month);

  // extract year and month from date32, cast to date64 first
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto date32_to_date64_func =
      TreeExprBuilder::MakeFunction("castDATE", {node_f1}, date64());

  auto date64_2year_func =
      TreeExprBuilder::MakeFunction("extractYear", {date32_to_date64_func}, int64());
  auto date64_2year_expr = TreeExprBuilder::MakeExpression(date64_2year_func, field_year);

  auto date64_2month_func =
      TreeExprBuilder::MakeFunction("extractMonth", {date32_to_date64_func}, int64());
  auto date64_2month_expr =
      TreeExprBuilder::MakeExpression(date64_2month_func, field_month);

  // extract month and day from timestamp
  auto ts2month_expr =
      TreeExprBuilder::MakeExpression("extractMonth", {field2}, field_month);
  auto ts2day_expr = TreeExprBuilder::MakeExpression("extractDay", {field2}, field_day);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema,
                                {date2year_expr, date2month_expr, date64_2year_expr,
                                 date64_2month_expr, ts2month_expr, ts2day_expr},
                                TestConfiguration(), &projector);
  ASSERT_TRUE(status.ok());

  // Create a row-batch with some sample data
  time_t epoch = Epoch();
  int num_records = 4;
  auto validity = {true, true, true, true};
  std::vector<int64_t> field0_data = {MillisSince(epoch, 2000, 1, 1, 5, 0, 0, 0),
                                      MillisSince(epoch, 1999, 12, 31, 5, 0, 0, 0),
                                      MillisSince(epoch, 2015, 6, 30, 20, 0, 0, 0),
                                      MillisSince(epoch, 2015, 7, 1, 20, 0, 0, 0)};
  auto array0 =
      MakeArrowTypeArray<arrow::Date64Type, int64_t>(date64(), field0_data, validity);

  std::vector<int32_t> field1_data = {DaysSince(epoch, 2000, 1, 1, 5, 0, 0, 0),
                                      DaysSince(epoch, 1999, 12, 31, 5, 0, 0, 0),
                                      DaysSince(epoch, 2015, 6, 30, 20, 0, 0, 0),
                                      DaysSince(epoch, 2015, 7, 1, 20, 0, 0, 0)};
  auto array1 =
      MakeArrowTypeArray<arrow::Date32Type, int32_t>(date32(), field1_data, validity);

  std::vector<int64_t> field2_data = {MillisSince(epoch, 1999, 12, 31, 5, 0, 0, 0),
                                      MillisSince(epoch, 2000, 1, 2, 5, 0, 0, 0),
                                      MillisSince(epoch, 2015, 7, 1, 1, 0, 0, 0),
                                      MillisSince(epoch, 2015, 6, 29, 23, 0, 0, 0)};

  auto array2 = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      arrow::timestamp(arrow::TimeUnit::MILLI), field2_data, validity);

  // expected output
  // date 2 year and date 2 month for date64
  auto exp_yy_from_date64 = MakeArrowArrayInt64({2000, 1999, 2015, 2015}, validity);
  auto exp_mm_from_date64 = MakeArrowArrayInt64({1, 12, 6, 7}, validity);

  // date 2 year and date 2 month for date32
  auto exp_yy_from_date32 = MakeArrowArrayInt64({2000, 1999, 2015, 2015}, validity);
  auto exp_mm_from_date32 = MakeArrowArrayInt64({1, 12, 6, 7}, validity);

  // ts 2 month and ts 2 day
  auto exp_mm_from_ts = MakeArrowArrayInt64({12, 1, 7, 6}, validity);
  auto exp_dd_from_ts = MakeArrowArrayInt64({31, 2, 1, 29}, validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1, array2});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_yy_from_date64, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_mm_from_date64, outputs.at(1));
  EXPECT_ARROW_ARRAY_EQUALS(exp_yy_from_date32, outputs.at(2));
  EXPECT_ARROW_ARRAY_EQUALS(exp_mm_from_date32, outputs.at(3));
  EXPECT_ARROW_ARRAY_EQUALS(exp_mm_from_ts, outputs.at(4));
  EXPECT_ARROW_ARRAY_EQUALS(exp_dd_from_ts, outputs.at(5));
}

TEST_F(DateTimeTestProjector, TestTime) {
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
  auto status = Projector::Make(schema, {time2min_expr, time2hour_expr},
                                TestConfiguration(), &projector);
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

TEST_F(DateTimeTestProjector, TestTimestampDiff) {
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

  auto diff_days_expr_with_datediff_fn =
      TreeExprBuilder::MakeExpression("datediff", {f0, f1}, diff_seconds);

  auto diff_weeks_expr =
      TreeExprBuilder::MakeExpression("timestampdiffWeek", {f0, f1}, diff_seconds);

  auto diff_months_expr =
      TreeExprBuilder::MakeExpression("timestampdiffMonth", {f0, f1}, diff_seconds);

  auto diff_quarters_expr =
      TreeExprBuilder::MakeExpression("timestampdiffQuarter", {f0, f1}, diff_seconds);

  auto diff_years_expr =
      TreeExprBuilder::MakeExpression("timestampdiffYear", {f0, f1}, diff_seconds);

  std::shared_ptr<Projector> projector;
  auto exprs = {diff_secs_expr,
                diff_mins_expr,
                diff_hours_expr,
                diff_days_expr,
                diff_days_expr_with_datediff_fn,
                diff_weeks_expr,
                diff_months_expr,
                diff_quarters_expr,
                diff_years_expr};
  auto status = Projector::Make(schema, exprs, TestConfiguration(), &projector);
  ASSERT_TRUE(status.ok());

  time_t epoch = Epoch();

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
  exp_output.push_back(MakeArrowArrayInt32({-567, 567, 0, 0}, validity));
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

TEST_F(DateTimeTestProjector, TestTimestampDiffMonth) {
  auto f0 = field("f0", timestamp(arrow::TimeUnit::MILLI));
  auto f1 = field("f1", timestamp(arrow::TimeUnit::MILLI));
  auto schema = arrow::schema({f0, f1});

  // output fields
  auto diff_seconds = field("ss", int32());

  auto diff_months_expr =
      TreeExprBuilder::MakeExpression("timestampdiffMonth", {f0, f1}, diff_seconds);

  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {diff_months_expr}, TestConfiguration(), &projector);

  ASSERT_TRUE(status.ok());

  time_t epoch = Epoch();

  // Create a row-batch with some sample data
  std::vector<int64_t> f0_data = {MillisSince(epoch, 2019, 1, 31, 0, 0, 0, 0),
                                  MillisSince(epoch, 2020, 1, 31, 0, 0, 0, 0),
                                  MillisSince(epoch, 2020, 1, 31, 0, 0, 0, 0),
                                  MillisSince(epoch, 2019, 3, 31, 0, 0, 0, 0),
                                  MillisSince(epoch, 2020, 3, 30, 0, 0, 0, 0),
                                  MillisSince(epoch, 2020, 5, 31, 0, 0, 0, 0)};
  std::vector<int64_t> f1_data = {MillisSince(epoch, 2019, 2, 28, 0, 0, 0, 0),
                                  MillisSince(epoch, 2020, 2, 28, 0, 0, 0, 0),
                                  MillisSince(epoch, 2020, 2, 29, 0, 0, 0, 0),
                                  MillisSince(epoch, 2019, 4, 30, 0, 0, 0, 0),
                                  MillisSince(epoch, 2020, 2, 29, 0, 0, 0, 0),
                                  MillisSince(epoch, 2020, 9, 30, 0, 0, 0, 0)};
  int64_t num_records = f0_data.size();
  std::vector<bool> validity(num_records, true);

  auto array0 = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      arrow::timestamp(arrow::TimeUnit::MILLI), f0_data, validity);
  auto array1 = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      arrow::timestamp(arrow::TimeUnit::MILLI), f1_data, validity);

  // expected output
  std::vector<ArrayPtr> exp_output;
  exp_output.push_back(MakeArrowArrayInt32({1, 0, 1, 1, -1, 4}, validity));

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

TEST_F(DateTimeTestProjector, TestMonthsBetween) {
  auto f0 = field("f0", arrow::date64());
  auto f1 = field("f1", arrow::date64());
  auto schema = arrow::schema({f0, f1});

  // output fields
  auto output = field("out", arrow::float64());

  auto months_between_expr =
      TreeExprBuilder::MakeExpression("months_between", {f0, f1}, output);

  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {months_between_expr}, TestConfiguration(), &projector);

  ASSERT_TRUE(status.ok());

  time_t epoch = Epoch();

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

TEST_F(DateTimeTestProjector, TestCastTimestampFromInt64) {
  auto f0 = field("f0", arrow::int64());
  auto schema = arrow::schema({f0});

  // output fields
  auto output = field("out", arrow::timestamp(arrow::TimeUnit::MILLI));

  auto casttimestamp_expr =
      TreeExprBuilder::MakeExpression("castTIMESTAMP", {f0}, output);

  std::shared_ptr<Projector> projector;
  auto status =
      Projector::Make(schema, {casttimestamp_expr}, TestConfiguration(), &projector);
  std::cout << status.message();
  ASSERT_TRUE(status.ok());

  time_t epoch = Epoch();

  int num_records = 5;
  auto validity = {true, true, true, true, true};
  std::vector<int64_t> f0_data = {MillisSince(epoch, 2016, 2, 3, 8, 20, 10, 34),
                                  MillisSince(epoch, 2016, 2, 29, 23, 59, 59, 59),
                                  MillisSince(epoch, 2016, 1, 30, 1, 15, 20, 0),
                                  MillisSince(epoch, 2017, 2, 3, 23, 15, 20, 0),
                                  MillisSince(epoch, 1970, 12, 30, 22, 50, 11, 0)};

  auto array0 = MakeArrowArrayInt64(f0_data, validity);

  std::vector<int64_t> f0_output_data = {MillisSince(epoch, 2016, 2, 3, 8, 20, 10, 34),
                                         MillisSince(epoch, 2016, 2, 29, 23, 59, 59, 59),
                                         MillisSince(epoch, 2016, 1, 30, 1, 15, 20, 0),
                                         MillisSince(epoch, 2017, 2, 3, 23, 15, 20, 0),
                                         MillisSince(epoch, 1970, 12, 30, 22, 50, 11, 0)};

  // expected output
  auto exp_output = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      timestamp(arrow::TimeUnit::MILLI), f0_output_data, validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_output, outputs.at(0));
}

TEST_F(DateTimeTestProjector, TestLastDay) {
  auto f0 = field("f0", arrow::date64());
  auto schema = arrow::schema({f0});

  // output fields
  auto output = field("out", arrow::date64());

  auto last_day_expr = TreeExprBuilder::MakeExpression("last_day", {f0}, output);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {last_day_expr}, TestConfiguration(), &projector);

  ASSERT_TRUE(status.ok());

  time_t epoch = Epoch();

  // Create a row-batch with some sample data
  // Used a leap year as example.
  int num_records = 5;
  auto validity = {true, true, true, true, true};
  std::vector<int64_t> f0_data = {MillisSince(epoch, 2016, 2, 3, 8, 20, 10, 34),
                                  MillisSince(epoch, 2016, 2, 29, 23, 59, 59, 59),
                                  MillisSince(epoch, 2016, 1, 30, 1, 15, 20, 0),
                                  MillisSince(epoch, 2017, 2, 3, 23, 15, 20, 0),
                                  MillisSince(epoch, 2015, 12, 30, 22, 50, 11, 0)};

  auto array0 =
      MakeArrowTypeArray<arrow::Date64Type, int64_t>(date64(), f0_data, validity);

  std::vector<int64_t> f0_output_data = {MillisSince(epoch, 2016, 2, 29, 0, 0, 0, 0),
                                         MillisSince(epoch, 2016, 2, 29, 0, 0, 0, 0),
                                         MillisSince(epoch, 2016, 1, 31, 0, 0, 0, 0),
                                         MillisSince(epoch, 2017, 2, 28, 0, 0, 0, 0),
                                         MillisSince(epoch, 2015, 12, 31, 0, 0, 0, 0)};

  // expected output
  auto exp_output = MakeArrowArrayDate64(f0_output_data, validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_output, outputs.at(0));
}

TEST_F(DateTimeTestProjector, TestToTimestampFromInt) {
  auto f0 = field("f0", arrow::int32());
  auto f1 = field("f1", arrow::int64());
  auto f2 = field("f2", arrow::float32());
  auto f3 = field("f3", arrow::float64());
  auto schema = arrow::schema({f0, f1, f2, f3});

  // output fields
  auto output = field("out", arrow::timestamp(arrow::TimeUnit::MILLI));
  auto output1 = field("out1", arrow::timestamp(arrow::TimeUnit::MILLI));
  auto output2 = field("out1", arrow::timestamp(arrow::TimeUnit::MILLI));
  auto output3 = field("out1", arrow::timestamp(arrow::TimeUnit::MILLI));

  auto totimestamp_expr = TreeExprBuilder::MakeExpression("to_timestamp", {f0}, output);
  auto totimestamp_expr1 = TreeExprBuilder::MakeExpression("to_timestamp", {f1}, output1);
  auto totimestamp_expr2 = TreeExprBuilder::MakeExpression("to_timestamp", {f2}, output2);
  auto totimestamp_expr3 = TreeExprBuilder::MakeExpression("to_timestamp", {f3}, output3);

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(
      schema, {totimestamp_expr, totimestamp_expr1, totimestamp_expr2, totimestamp_expr3},
      TestConfiguration(), &projector);
  std::cout << status.message();
  ASSERT_TRUE(status.ok());

  time_t epoch = Epoch();

  int num_records = 3;
  auto validity = {true, true, false};
  std::vector<int32_t> f0_data = {0, 1626255099, 0};
  std::vector<int64_t> f1_data = {0, 1626255099, 0};
  std::vector<float> f2_data = {0, 3601.411f, 0};
  std::vector<double> f3_data = {0, 3601.411, 0};

  auto array0 = MakeArrowArrayInt32(f0_data, validity);
  auto array1 = MakeArrowArrayInt64(f1_data, validity);
  auto array2 = MakeArrowArrayFloat32(f2_data, validity);
  auto array3 = MakeArrowArrayFloat64(f3_data, validity);

  std::vector<int64_t> f0_1_output_data = {MillisSince(epoch, 1970, 1, 1, 0, 0, 0, 0),
                                           MillisSince(epoch, 2021, 7, 14, 9, 31, 39, 0),
                                           0};

  std::vector<int64_t> f2_3_output_data = {MillisSince(epoch, 1970, 1, 1, 0, 0, 0, 0),
                                           MillisSince(epoch, 1970, 1, 1, 1, 0, 1, 411),
                                           0};

  // expected output
  auto exp_output = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      timestamp(arrow::TimeUnit::MILLI), f0_1_output_data, validity);

  // expected output
  auto exp_output1 = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      timestamp(arrow::TimeUnit::MILLI), f2_3_output_data, validity);

  // prepare input record batch
  auto in_batch =
      arrow::RecordBatch::Make(schema, num_records, {array0, array1, array2, array3});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_output, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_output, outputs.at(1));
  EXPECT_ARROW_ARRAY_EQUALS(exp_output1, outputs.at(2));
  EXPECT_ARROW_ARRAY_EQUALS(exp_output1, outputs.at(3));
}

TEST_F(DateTimeTestProjector, TestToUtcTimestamp) {
  auto f0 = field("f0", timestamp(arrow::TimeUnit::MILLI));
  auto f1 = field("f1", arrow::utf8());

  auto schema = arrow::schema({f0, f1});

  // output fields
  auto utc_timestamp = field("utc_time", timestamp(arrow::TimeUnit::MILLI));

  auto utc_time_expr =
      TreeExprBuilder::MakeExpression("to_utc_timestamp", {f0, f1}, utc_timestamp);
  std::shared_ptr<Projector> projector;
  Status status =
      Projector::Make(schema, {utc_time_expr}, TestConfiguration(), &projector);

  ASSERT_TRUE(status.ok());

  time_t epoch = Epoch();

  // Create a row-batch with some sample data
  std::vector<int64_t> f0_data = {MillisSince(epoch, 1970, 1, 1, 6, 0, 0, 0),
                                  MillisSince(epoch, 2001, 1, 5, 3, 0, 0, 0),
                                  MillisSince(epoch, 2018, 3, 12, 1, 0, 0, 0),
                                  MillisSince(epoch, 2018, 3, 11, 1, 0, 0, 0)};
  int64_t num_records = f0_data.size();
  std::vector<bool> validity(num_records, true);
  auto array0 = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      arrow::timestamp(arrow::TimeUnit::MILLI), f0_data, validity);

  auto array1 = MakeArrowArrayUtf8(
      {"Asia/Kolkata", "Asia/Kolkata", "America/Los_Angeles", "America/Los_Angeles"},
      {true, true, true, true});

  // expected output
  std::vector<int64_t> exp_output_data = {MillisSince(epoch, 1970, 1, 1, 0, 30, 0, 0),
                                          MillisSince(epoch, 2001, 1, 4, 21, 30, 0, 0),
                                          MillisSince(epoch, 2018, 3, 12, 8, 0, 0, 0),
                                          MillisSince(epoch, 2018, 3, 11, 9, 0, 0, 0)};
  auto exp_output = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      arrow::timestamp(arrow::TimeUnit::MILLI), exp_output_data, validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results

  EXPECT_ARROW_ARRAY_EQUALS(exp_output, outputs.at(0));
}

TEST_F(DateTimeTestProjector, TestFromUtcTimestamp) {
  auto f0 = field("f0", timestamp(arrow::TimeUnit::MILLI));
  auto f1 = field("f1", arrow::utf8());

  auto schema = arrow::schema({f0, f1});

  // output fields
  auto local_timestamp = field("local_time", timestamp(arrow::TimeUnit::MILLI));

  auto local_time_expr =
      TreeExprBuilder::MakeExpression("from_utc_timestamp", {f0, f1}, local_timestamp);
  std::shared_ptr<Projector> projector;
  Status status =
      Projector::Make(schema, {local_time_expr}, TestConfiguration(), &projector);

  ASSERT_TRUE(status.ok());

  time_t epoch = Epoch();

  // Create a row-batch with some sample data
  std::vector<int64_t> f0_data = {MillisSince(epoch, 1970, 1, 1, 0, 30, 0, 0),
                                  MillisSince(epoch, 2001, 1, 4, 21, 30, 0, 0),
                                  MillisSince(epoch, 2018, 3, 12, 8, 0, 0, 0),
                                  MillisSince(epoch, 2018, 3, 11, 9, 0, 0, 0)};

  int64_t num_records = f0_data.size();
  std::vector<bool> validity(num_records, true);
  auto array0 = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      arrow::timestamp(arrow::TimeUnit::MILLI), f0_data, validity);

  auto array1 = MakeArrowArrayUtf8(
      {"Asia/Kolkata", "Asia/Kolkata", "America/Los_Angeles", "America/Los_Angeles"},
      {true, true, true, true});

  // expected output
  std::vector<int64_t> exp_output_data = {MillisSince(epoch, 1970, 1, 1, 6, 0, 0, 0),
                                          MillisSince(epoch, 2001, 1, 5, 3, 0, 0, 0),
                                          MillisSince(epoch, 2018, 3, 12, 1, 0, 0, 0),
                                          MillisSince(epoch, 2018, 3, 11, 1, 0, 0, 0)};
  auto exp_output = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
      arrow::timestamp(arrow::TimeUnit::MILLI), exp_output_data, validity);

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
