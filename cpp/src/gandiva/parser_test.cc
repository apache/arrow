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
#include "gandiva/parser.h"

#include <gtest/gtest.h>
#include "arrow/type_fwd.h"
#include "gandiva/gandiva_aliases.h"

namespace gandiva {

class TestParser : public ::testing::Test {
 protected:
  void SetUp() override {
    auto field_x = arrow::field("x", arrow::uint64());
    auto field_y = arrow::field("y", arrow::int32());
    auto field_z = arrow::field("z", arrow::float32());
    auto field_a = arrow::field("a", arrow::boolean());
    auto field_s = arrow::field("s", arrow::utf8());
    auto field_t = arrow::field("t", arrow::time32(arrow::TimeUnit::SECOND));
    auto field_d = arrow::field("d", arrow::date64());
    auto schema =
        arrow::schema({field_x, field_y, field_z, field_a, field_s, field_t, field_d});
    auto ret = schema->GetFieldByName("name");
    parser_ = Parser(schema);
  }

  Parser parser_{nullptr};
  NodePtr expr_;
  Status status_;
};

TEST_F(TestParser, TestLiteral) {
  status_ = parser_.parse("0123", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(const untyped) 123");

  status_ = parser_.parse("65535u16", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(const uint16) 65535");

  // status_ = parser_.parse("-2147483648i32", &expr_);
  // EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  // EXPECT_EQ(expr_->ToString(), "(const int32) -2147483648");

  // status_ = parser_.parse("-2147483648u64", &expr_);
  // EXPECT_FALSE(status_.ok());
  // EXPECT_EQ(status_.message(), "-2147483648u64:1.2-14: wrong suffix for nagative
  // number");

  status_ = parser_.parse("100000000000000000000", &expr_);
  EXPECT_FALSE(status_.ok());
  EXPECT_EQ(status_.message(), "100000000000000000000:1.1-21: out of range");

  status_ = parser_.parse("0.123", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(const untyped) 0.123");

  status_ = parser_.parse("0.123f32", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(const float) 0.123 raw(3dfbe76d)");

  status_ = parser_.parse("456f64", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(const double) 456 raw(407c800000000000)");

  status_ = parser_.parse("78.999999999f64", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(const double) 79 raw(4053bffffffeed1f)");

  status_ = parser_.parse("78.999999999f32", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(const float) 79 raw(429e0000)");

  // status_ = parser_.parse("-0.123", &expr_);
  // EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  // EXPECT_EQ(expr_->ToString(), "(const untyped) -0.123");

  // status_ = parser_.parse("-0.123f32", &expr_);
  // EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  // EXPECT_EQ(expr_->ToString(), "(const float) -0.123 raw(bdfbe76d)");

  status_ = parser_.parse("\"Hello World\"", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(const string) 'Hello World'");

  status_ = parser_.parse("\'Hello World\'", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(const string) 'Hello World'");

  status_ = parser_.parse("\'\t你好\n\'", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(const string) '\t你好\n'");

  status_ = parser_.parse("true", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(const bool) 1");

  status_ = parser_.parse("false", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(const bool) 0");
}

TEST_F(TestParser, TestField) {
  status_ = parser_.parse("a", &expr_);
  EXPECT_TRUE(status_.ok());
  EXPECT_EQ(status_.message(), "");
  EXPECT_EQ(expr_->ToString(), "(bool) a");

  status_ = parser_.parse("k", &expr_);
  EXPECT_FALSE(status_.ok());
  EXPECT_EQ(status_.message(), "k:1.1: not defined in schema");
}

}  // namespace gandiva
