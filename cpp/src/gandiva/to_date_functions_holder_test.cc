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

#include "gandiva/to_date_functions_holder.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "arrow/testing/gtest_util.h"
#include "gandiva/execution_context.h"

namespace gandiva {

// Block of tests to test the holder for TO_TIME functions
class TestToTimeHolder : public ::testing::Test {
 protected:
  ExecutionContext execution_context_;
};

TEST_F(TestToTimeHolder, TestSimpleDateTime) {
  std::shared_ptr<ToTimeHolder> to_time_holder;
  ASSERT_OK(ToTimeHolder::Make("YYYY-MM-DD HH:MI:SS AM", 1, &to_time_holder));

  auto& to_date = *to_time_holder;
  bool out_valid;
  std::string s("1986-12-01 01:01:01 am");
  int64_t millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 3661000);

  s = std::string(" ");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);

  s = std::string("1986-12-01 01:01:01 am +0800");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 3661000);

  s = std::string("1886-12-01 01:01:01 am");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 3661000);

  s = std::string("1986-12-11 01:30:00 pm");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 48600000);
}

TEST_F(TestToTimeHolder, TestSimpleDate) {
  std::shared_ptr<ToTimeHolder> to_time_holder;
  // As the TO_TIME function returns the number of milliseconds since the midnight
  // and the tests are passing dates, without time information, the number of returned
  // millis are always 0
  ASSERT_OK(ToTimeHolder::Make("YYYY-MM-DD", 1, &to_time_holder));

  auto& to_time = *to_time_holder;
  bool out_valid;
  std::string s("1986-12-01");
  int64_t millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);

  s = std::string("1986-12-01");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);

  s = std::string("1886-12-1");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);

  s = std::string("2012-12-1");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);

  // wrong month. should return 0 since we are suppressing errors.
  s = std::string("1986-21-01 01:01:01 +0800");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
}

TEST_F(TestToTimeHolder, TestSimpleDateTimeError) {
  std::shared_ptr<ToTimeHolder> to_time_holder;

  auto status = ToTimeHolder::Make("YYYY-MM-DD HH24:MI:SS", 0, &to_time_holder);
  EXPECT_EQ(status.ok(), true) << status.message();
  auto& to_time = *to_time_holder;
  bool out_valid;

  std::string s("1986-01-40 01:01:01 +0800");
  int64_t millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(0, millis_since_epoch);
  std::string expected_error =
      "Error parsing value 1986-01-40 01:01:01 +0800 for given format";
  EXPECT_TRUE(execution_context_.get_error().find(expected_error) != std::string::npos)
      << status.message();

  // not valid should not return error
  execution_context_.Reset();
  millis_since_epoch = to_time(&execution_context_, "nullptr", 7, false, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
  EXPECT_FALSE(execution_context_.has_error());
}

TEST_F(TestToTimeHolder, TestSimpleDateTimeWithSubSeconds) {
  std::shared_ptr<ToTimestampHolder> to_time_holder;
  ASSERT_OK(ToTimestampHolder::Make("YYYY-MM-DD HH24:MI:SS.FFF", 1, &to_time_holder));

  auto& to_time = *to_time_holder;
  bool out_valid;
  std::string s("1970-01-01 01:01:01.347");
  int64_t millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 3661347);

  s = std::string("1970-01-01 01:01:01.34");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 3661340);

  s = std::string("1970-01-01 01:01:01.3");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 3661300);

  std::shared_ptr<ToTimestampHolder> to_time_holder_deciseconds;
  ASSERT_OK(ToTimestampHolder::Make("YYYY-MM-DD HH24:MI:SS.FF", 1,
                                    &to_time_holder_deciseconds));
  to_time = *to_time_holder_deciseconds;

  s = std::string("1970-01-01 01:01:01.347");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 3661340);

  s = std::string("1970-01-01 01:01:01.34");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 3661340);

  s = std::string("1970-01-01 01:01:01.3");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 3661300);

  std::shared_ptr<ToTimestampHolder> to_time_holder_centiseconds;
  ASSERT_OK(ToTimestampHolder::Make("YYYY-MM-DD HH24:MI:SS.F", 1,
                                    &to_time_holder_centiseconds));
  to_time = *to_time_holder_centiseconds;

  s = std::string("1970-01-01 01:01:01.347");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 3661300);

  s = std::string("1970-01-01 01:01:01.34");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 3661300);

  s = std::string("1970-01-01 01:01:01.3");
  millis_since_epoch =
      to_time(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 3661300);
}

TEST_F(TestToTimeHolder, TestWithTimezones) {
  std::shared_ptr<ToTimeHolder> to_time_holder;
  // Timezone information are yet not supported by the arrow formatters
  auto status = ToTimeHolder::Make("YYYY-MM-DD HH:MI:SS tzo", 0, &to_time_holder);
  EXPECT_EQ(status.IsInvalid(), true) << status.message();

  status = ToTimeHolder::Make("YYYY-MM-DD HH:MI:SS tzd", 0, &to_time_holder);
  EXPECT_EQ(status.IsInvalid(), true) << status.message();
}

TEST_F(TestToTimeHolder, TestIgnoredPatterns) {
  // Test using points as separators
  std::shared_ptr<ToTimeHolder> holder_point_as_separator;
  ASSERT_OK(
      ToTimeHolder::Make("YYYY.MM.DD HH24.MI.SS.FFF", 1, &holder_point_as_separator));

  auto& to_date = *holder_point_as_separator;
  bool out_valid;
  std::string s("1999.12.01 15.53.54.578");
  int64_t millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 57234578);

  std::shared_ptr<ToTimeHolder> holder_point_as_separator2;
  ASSERT_OK(ToTimeHolder::Make("YYYY.MM::::,,..DD", 0, &holder_point_as_separator));

  auto& to_time_pont_as_sep = *holder_point_as_separator;
  s = std::string("1999.12::::,,..01");
  millis_since_epoch = to_time_pont_as_sep(&execution_context_, s.data(), (int)s.length(),
                                           true, &out_valid);
  EXPECT_FALSE(execution_context_.has_error());
  EXPECT_EQ(millis_since_epoch, 0);
}

// Block of tests to check the holder for TO_DATE functions
class TestToDateHolder : public ::testing::Test {
 protected:
  ExecutionContext execution_context_;
};

TEST_F(TestToDateHolder, TestSimpleDateTime) {
  std::shared_ptr<ToDateHolder> to_date_holder;
  ASSERT_OK(ToDateHolder::Make("YYYY-MM-DD HH:MI:SS AM", 1, &to_date_holder));

  auto& to_date = *to_date_holder;
  bool out_valid;
  std::string s("1986-12-01 01:01:01 am");
  int64_t millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200000);

  s = std::string("1986-12-01 01:01:01.11 AM");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200000);

  s = std::string("1986-12-01 01:01:01 AM +0800");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200000);

#if 0
  // TODO : this fails parsing with date::parse and strptime on linux
  s = std::string("1886-12-01 00:00:00");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int) s.length(), true, &out_valid);
  EXPECT_EQ(out_valid, true);
  EXPECT_EQ(millis_since_epoch, -2621894400000);
#endif

  s = std::string("1886-12-01 01:01:01 AM");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, -2621894400000);

  s = std::string("1986-12-11 01:30:00 AM");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 534643200000);
}

TEST_F(TestToDateHolder, TestSimpleDate) {
  std::shared_ptr<ToDateHolder> to_date_holder;
  ASSERT_OK(ToDateHolder::Make("YYYY-MM-DD", 1, &to_date_holder));

  auto& to_date = *to_date_holder;
  bool out_valid;
  std::string s("1986-12-01");
  int64_t millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200000);

  s = std::string("1986-12-01");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 533779200000);

  s = std::string("1886-12-1");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, -2621894400000);

  s = std::string("2012-12-1");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 1354320000000);

  // wrong month. should return 0 since we are suppressing errors.
  s = std::string("1986-21-01 01:01:01 +0800");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
}

TEST_F(TestToDateHolder, TestSimpleDateTimeError) {
  std::shared_ptr<ToDateHolder> to_date_holder;

  auto status = ToDateHolder::Make("YYYY-MM-DD HH:MI:SS", 0, &to_date_holder);
  EXPECT_EQ(status.ok(), true) << status.message();
  auto& to_date = *to_date_holder;
  bool out_valid;

  std::string s("1986-01-40 01:01:01 +0800");
  int64_t millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(0, millis_since_epoch);
  std::string expected_error =
      "Error parsing value 1986-01-40 01:01:01 +0800 for given format";
  EXPECT_TRUE(execution_context_.get_error().find(expected_error) != std::string::npos)
      << status.message();

  // not valid should not return error
  execution_context_.Reset();
  millis_since_epoch = to_date(&execution_context_, "nullptr", 7, false, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
  EXPECT_TRUE(execution_context_.has_error() == false);
}

TEST_F(TestToDateHolder, TestSimpleDateTimeMakeError) {
  std::shared_ptr<ToDateHolder> to_date_holder;
  // reject timezones for now.
  auto status = ToDateHolder::Make("YYYY-MM-DD HH:MI:SS tzo", 0, &to_date_holder);
  EXPECT_EQ(status.IsInvalid(), true) << status.message();
}

// Block of tests to check the holder for UNIX_TIMESTAMP functions
class TestUnixTimestampHolder : public ::testing::Test {
 public:
  FunctionNode BuildUnixTimestampNode() {
    auto field = std::make_shared<FieldNode>(arrow::field("in", arrow::utf8()));
    return FunctionNode("unix_timestamp_utf8", {field}, arrow::int64());
  }

 protected:
  ExecutionContext execution_context_;
};

TEST_F(TestUnixTimestampHolder, TestSimpleDateTime) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;
  ASSERT_OK(
      UnixTimestampHolder::Make("YYYY-MM-DD HH24:MI:SS", 1, &unix_timestamp_holder));

  auto& to_date = *unix_timestamp_holder;
  bool out_valid;
  std::string s("1986-12-01 01:01:01");
  int64_t millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  int64_t expected_result = 533782861;
  EXPECT_EQ(millis_since_epoch, expected_result);

  s = std::string(" ");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  expected_result = 0;
  EXPECT_EQ(millis_since_epoch, expected_result);

  s = std::string("1986-12-01 01:01:01 +0800");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  expected_result = 533782861;
  EXPECT_EQ(millis_since_epoch, expected_result);

  s = std::string("1886-12-01 01:01:01");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  expected_result = -2621890739LL;
  EXPECT_EQ(millis_since_epoch, expected_result);

  s = std::string("1986-12-11 01:30:00");
  millis_since_epoch =
      to_date(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  expected_result = 534648600;
  EXPECT_EQ(millis_since_epoch, expected_result);
}

TEST_F(TestUnixTimestampHolder, TestSimpleDate) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;
  ASSERT_OK(UnixTimestampHolder::Make("YYYY-MM-DD", 1, &unix_timestamp_holder));

  auto& unix_timestamp = *unix_timestamp_holder;
  bool out_valid;
  std::string s("1986-12-01");
  int64_t millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  int64_t expected_result = 533779200;
  EXPECT_EQ(millis_since_epoch, expected_result);

  s = std::string("1886-12-1");
  millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  expected_result = -2621894400LL;
  EXPECT_EQ(millis_since_epoch, expected_result);

  s = std::string("2012-12-1");
  millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  expected_result = 1354320000;
  EXPECT_EQ(millis_since_epoch, expected_result);

  // wrong month. should return 0 since we are suppressing errors.
  s = std::string("1986-21-01 01:01:01 +0800");
  millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  expected_result = 0;
  EXPECT_EQ(millis_since_epoch, expected_result);
}

TEST_F(TestUnixTimestampHolder, TestSimpleDateTimeError) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;

  auto status =
      UnixTimestampHolder::Make("YYYY-MM-DD HH:MI:SS", 0, &unix_timestamp_holder);
  EXPECT_EQ(status.ok(), true) << status.message();
  auto& unix_timestamp = *unix_timestamp_holder;
  bool out_valid;

  std::string s("1986-01-40 01:01:01 +0800");
  int64_t millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(0, millis_since_epoch);
  std::string expected_error =
      "Error parsing value 1986-01-40 01:01:01 +0800 for given format";
  EXPECT_TRUE(execution_context_.get_error().find(expected_error) != std::string::npos)
      << status.message();

  // not valid should not return error
  execution_context_.Reset();
  millis_since_epoch =
      unix_timestamp(&execution_context_, "nullptr", 7, false, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
  EXPECT_TRUE(execution_context_.has_error() == false);
}

TEST_F(TestUnixTimestampHolder, TestWithTimezones) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;
  // Timezone information are yet not supported by the arrow formatters
  auto status =
      UnixTimestampHolder::Make("YYYY-MM-DD HH:MI:SS tzo", 0, &unix_timestamp_holder);
  EXPECT_EQ(status.IsInvalid(), true) << status.message();

  status =
      UnixTimestampHolder::Make("YYYY-MM-DD HH:MI:SS tzd", 0, &unix_timestamp_holder);
  EXPECT_EQ(status.IsInvalid(), true) << status.message();
}

TEST_F(TestUnixTimestampHolder, TestSimpleDateTimeWithoutPattern) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;
  ASSERT_OK(UnixTimestampHolder::Make(BuildUnixTimestampNode(), &unix_timestamp_holder));

  auto& unix_timestamp = *unix_timestamp_holder;
  bool out_valid;
  std::string s("1986-12-01 01:01:01");
  int64_t millis_since_epoch = unix_timestamp(
      &execution_context_, s.data(), static_cast<int>(s.length()), true, &out_valid);
  int64_t expected_result = 533782861;
  EXPECT_EQ(millis_since_epoch, expected_result);

  s = std::string(" ");
  millis_since_epoch = unix_timestamp(&execution_context_, s.data(),
                                      static_cast<int>(s.length()), true, &out_valid);
  expected_result = 0;
  EXPECT_EQ(millis_since_epoch, expected_result);

  s = std::string("1986-12-01 01:01:01 +0800");
  millis_since_epoch = unix_timestamp(&execution_context_, s.data(),
                                      static_cast<int>(s.length()), true, &out_valid);
  expected_result = 533782861;
  EXPECT_EQ(millis_since_epoch, expected_result);

  s = std::string("1886-12-01 01:01:01");
  millis_since_epoch = unix_timestamp(&execution_context_, s.data(),
                                      static_cast<int>(s.length()), true, &out_valid);
  expected_result = -2621890739LL;
  EXPECT_EQ(millis_since_epoch, expected_result);

  s = std::string("1986-12-11 01:30:00");
  millis_since_epoch = unix_timestamp(&execution_context_, s.data(),
                                      static_cast<int>(s.length()), true, &out_valid);
  expected_result = 534648600;
  EXPECT_EQ(millis_since_epoch, expected_result);
}

TEST_F(TestUnixTimestampHolder, TestSimpleDateTimeErrorWithoutPattern) {
  std::shared_ptr<UnixTimestampHolder> unix_timestamp_holder;
  ASSERT_OK(UnixTimestampHolder::Make(BuildUnixTimestampNode(), &unix_timestamp_holder));

  auto& unix_timestamp = *unix_timestamp_holder;
  bool out_valid;

  std::string s("1986-01-40 01:01:01 +0800");
  int64_t millis_since_epoch =
      unix_timestamp(&execution_context_, s.data(), (int)s.length(), true, &out_valid);
  EXPECT_EQ(0, millis_since_epoch);
  std::string expected_error =
      "Error parsing value 1986-01-40 01:01:01 +0800 for given format";
  EXPECT_TRUE(execution_context_.get_error().find(expected_error) != std::string::npos);

  // not valid should not return error
  execution_context_.Reset();
  millis_since_epoch =
      unix_timestamp(&execution_context_, "nullptr", 7, false, &out_valid);
  EXPECT_EQ(millis_since_epoch, 0);
  EXPECT_TRUE(execution_context_.has_error() == false);
}
}  // namespace gandiva
