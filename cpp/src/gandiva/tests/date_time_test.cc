// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <time.h>
#include <math.h>
#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
#include "integ/test_util.h"
#include "gandiva/projector.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::int32;
using arrow::int64;
using arrow::float32;
using arrow::boolean;
using arrow::date64;

class TestProjector : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

int64_t MillisSince(time_t base_line,
                     int32_t yy, int32_t mm, int32_t dd,
                     int32_t hr, int32_t min, int32_t sec) {
  struct tm given_ts = {0};
  given_ts.tm_year = (yy - 1900);
  given_ts.tm_mon = (mm - 1);
  given_ts.tm_mday = dd;
  given_ts.tm_hour = hr;
  given_ts.tm_min = min;
  given_ts.tm_sec = sec;

  return (lround(difftime(mktime(&given_ts), base_line)) * 1000);
}

TEST_F(TestProjector, TestTime) {
  auto field0 = field("f0", date64());
  auto field1 = field("f1", time32(arrow::TimeUnit::MILLI));
  auto field2 = field("f2", timestamp(arrow::TimeUnit::MILLI));
  auto schema = arrow::schema({field0, field2});

  // output fields
  auto field_year = field("yy", int64());
  auto field_month = field("mm", int64());
  auto field_day = field("dd", int64());
  auto field_hour = field("hh", int64());

  // extract year and month from date
  auto date2year_expr = TreeExprBuilder::MakeExpression(
    "extractYear",
    {field0},
    field_year);
  auto date2month_expr = TreeExprBuilder::MakeExpression(
    "extractMonth",
    {field0},
    field_month);

  // extract day and hour from time32
  auto time2day_expr = TreeExprBuilder::MakeExpression(
    "extractDay",
    {field1},
    field_day);
  auto time2hour_expr = TreeExprBuilder::MakeExpression(
    "extractHour",
    {field1},
    field_hour);

  // extract month and day from timestamp
  auto ts2month_expr = TreeExprBuilder::MakeExpression
    ("extractMonth",
    {field2},
    field_month);
  auto ts2day_expr = TreeExprBuilder::MakeExpression("extractDay", {field2}, field_day);

  std::shared_ptr<Projector> projector;
  Status status = Projector::Make(
    schema,
    {date2year_expr, date2month_expr, ts2month_expr, ts2day_expr},
    pool_,
    &projector);
  ASSERT_TRUE(status.ok());

  struct tm y1970 = {0};
  y1970.tm_year = 70; y1970.tm_mon = 0; y1970.tm_mday = 1;
  y1970.tm_hour = 0; y1970.tm_min = 0; y1970.tm_sec = 0;
  time_t epoch = mktime(&y1970);

  // Create a row-batch with some sample data
  int num_records = 4;
  auto validity = { true, true, true, true };
  std::vector<int64_t> field0_data = {
    MillisSince(epoch, 2000, 1, 1, 5, 0, 0),
    MillisSince(epoch, 1999, 12, 31, 5, 0, 0),
    MillisSince(epoch, 2015, 6, 30, 20, 0, 0),
    MillisSince(epoch, 2015, 7, 1, 20, 0, 0)
  };
  auto array0 = MakeArrowTypeArray<arrow::Date64Type, int64_t>(
    date64(),
    field0_data, validity);

  std::vector<int64_t> field1_data = {
    MillisSince(epoch, 2000, 1, 1, 5, 0, 0),
    MillisSince(epoch, 1999, 12, 31, 4, 0, 0),
    MillisSince(epoch, 2015, 6, 30, 20, 0, 0),
    MillisSince(epoch, 2015, 7, 3, 3, 0, 0)
  };

  auto array1 = MakeArrowTypeArray<arrow::Time32Type, int64_t>(
    time32(arrow::TimeUnit::MILLI),
    field1_data,
    validity);

  std::vector<int64_t> field2_data = {
    MillisSince(epoch, 1999, 12, 31, 5, 0, 0),
    MillisSince(epoch, 2000, 1, 2, 5, 0, 0),
    MillisSince(epoch, 2015, 7, 1, 1, 0, 0),
    MillisSince(epoch, 2015, 6, 29, 23, 0, 0)
  };

  auto array2 = MakeArrowTypeArray<arrow::TimestampType, int64_t>(
    arrow::timestamp(arrow::TimeUnit::MILLI),
    field2_data,
    validity);

  // expected output
  // date 2 year and date 2 month
  auto exp_yy_from_date = MakeArrowArrayInt64({ 2000, 1999, 2015, 2015 }, validity);
  auto exp_mm_from_date = MakeArrowArrayInt64({ 1, 12, 6, 7 }, validity);

  // ts 2 month and ts 2 day
  auto exp_mm_from_ts = MakeArrowArrayInt64({12, 1, 7, 6}, validity);
  auto exp_dd_from_ts = MakeArrowArrayInt64({31, 2, 1, 29}, validity);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array2});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_yy_from_date, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_mm_from_date, outputs.at(1));
  EXPECT_ARROW_ARRAY_EQUALS(exp_mm_from_ts, outputs.at(2));
  EXPECT_ARROW_ARRAY_EQUALS(exp_dd_from_ts, outputs.at(3));
}

} // namespace gandiva
