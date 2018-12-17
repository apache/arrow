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

#include <sstream>

#include <gtest/gtest.h>

#include "arrow/status.h"

namespace arrow {
namespace test {

TEST(StatusTest, TestCodeAndMessage) {
  Status ok = Status::OK();
  ASSERT_EQ(StatusCode::OK, ok.code());
  ASSERT_EQ(nullptr, ok.status_range());
  Status file_error = Status::IOError("file error");
  ASSERT_EQ(StatusCode::IOError, file_error.code());
  ASSERT_EQ("file error", file_error.message());
  ASSERT_EQ(nullptr, file_error.status_range());
}

TEST(StatusTest, TestToString) {
  Status file_error = Status::IOError("file error");
  ASSERT_EQ("IOError: file error", file_error.ToString());

  std::stringstream ss;
  ss << file_error;
  ASSERT_EQ(file_error.ToString(), ss.str());
}

TEST(StatusTest, AndStatus) {
  Status a = Status::OK();
  Status b = Status::OK();
  Status c = Status::Invalid("invalid value");
  Status d = Status::IOError("file error");

  Status res;
  res = a & b;
  ASSERT_TRUE(res.ok());
  res = a & c;
  ASSERT_TRUE(res.IsInvalid());
  res = d & c;
  ASSERT_TRUE(res.IsIOError());

  res = Status::OK();
  res &= c;
  ASSERT_TRUE(res.IsInvalid());
  res &= d;
  ASSERT_TRUE(res.IsInvalid());

  // With rvalues
  res = Status::OK() & Status::Invalid("foo");
  ASSERT_TRUE(res.IsInvalid());
  res = Status::Invalid("foo") & Status::OK();
  ASSERT_TRUE(res.IsInvalid());
  res = Status::Invalid("foo") & Status::IOError("bar");
  ASSERT_TRUE(res.IsInvalid());

  res = Status::OK();
  res &= Status::OK();
  ASSERT_TRUE(res.ok());
  res &= Status::Invalid("foo");
  ASSERT_TRUE(res.IsInvalid());
  res &= Status::IOError("bar");
  ASSERT_TRUE(res.IsInvalid());
}

static const char* test_messages1[] = {"First error", "Second error"};
static const char* test_messages2[] = {"Third error", "Fourth error"};

enum class TestCode1 : int32_t {
  FirstError = 1,
  SecondError = 2
};

enum class TestCode2 : int32_t {
  ThirdError = 1,
  FourthError = 2
};

static StatusRange test_range1("xxtest", 0x787874657374ULL, 2, test_messages1);
static StatusRange test_range2("xxtest2", 2, test_messages2);

TEST(StatusTest, StatusRange) {
  ASSERT_EQ(test_range1.range_id(), 0x787874657374ULL);
  ASSERT_EQ(test_range2.range_id(), 0x78787465737432ULL);

  auto st = Status(test_range1.range_id(), 1, "foo");
  ASSERT_EQ(st.ToString(), "[xxtest] First error: foo");
  ASSERT_EQ(st.status_range(), &test_range1);
  ASSERT_EQ(st.code<TestCode1>(), TestCode1::FirstError);

  st = Status(test_range2.range_id(), 2, "bar");
  ASSERT_EQ(st.ToString(), "[xxtest2] Fourth error: bar");
  ASSERT_EQ(st.status_range(), &test_range2);
  ASSERT_EQ(st.code<TestCode2>(), TestCode2::FourthError);

  st = Status(12345ULL, 42, "bar");
  ASSERT_EQ(st.ToString(), "Error 42 in unregistered status range 12345: bar");
}

}  // namespace test
}  // namespace arrow
