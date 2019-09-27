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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/dataset/api.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/test_util.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace dataset {

class TestPartitionScheme : public ::testing::Test {
 public:
  void AssertParseError(const std::string& path) {
    std::string unconsumed;
    std::shared_ptr<Expression> ignored;
    ASSERT_RAISES(Invalid, scheme_->Parse(path, &unconsumed, &ignored));
  }

  void AssertParse(const std::string& expected_consumed,
                   const std::string& expected_unconsumed,
                   std::shared_ptr<Expression> expected) {
    std::string unconsumed;
    std::shared_ptr<Expression> parsed;
    ASSERT_OK(
        scheme_->Parse(expected_consumed + expected_unconsumed, &unconsumed, &parsed));

    ASSERT_NE(parsed, nullptr);
    ASSERT_EQ(expected_unconsumed, unconsumed);
    ASSERT_TRUE(parsed->Equals(*expected)) << parsed->ToString() << "\n"
                                           << expected->ToString();
  }

  void AssertParse(const std::string& expected_consumed,
                   const std::string& expected_unconsumed, const Expression& expected) {
    AssertParse(expected_consumed, expected_unconsumed, expected.Copy());
  }

 protected:
  std::shared_ptr<PartitionScheme> scheme_;
};

TEST_F(TestPartitionScheme, Simple) {
  auto expr = equal(field_ref("alpha"), scalar<int16_t>(3));
  scheme_ = std::make_shared<SimplePartitionScheme>(expr);
  AssertParse("", "/hello/world", expr);
}

TEST_F(TestPartitionScheme, Hive) {
  scheme_ = std::make_shared<HivePartitionScheme>(
      schema({field("alpha", int32()), field("beta", float32())}));

  AssertParse("/alpha=0/beta=3.25", "", "alpha"_ == int32_t(0) and "beta"_ == 3.25f);

  AssertParse("/alpha=0/beta=3.25", "/ignored=2341",
              "alpha"_ == int32_t(0) and "beta"_ == 3.25f);

  AssertParse("", "", scalar(true));
  AssertParse("", "/ignored=2341", scalar(true));

  AssertParseError("/alpha=0.0/beta=3.25");  // conversion of "0.0" to int32 fails
}

TEST_F(TestPartitionScheme, ChainOfHive) {
  std::vector<std::shared_ptr<Schema>> schemas = {schema({field("alpha", int32())}),
                                                  schema({field("beta", float32())}),
                                                  schema({field("gamma", utf8())})};

  std::vector<std::unique_ptr<PartitionScheme>> schemes;
  for (auto schm : schemas) {
    schemes.emplace_back(new HivePartitionScheme(schm));
  }
  scheme_ = std::make_shared<ChainPartitionScheme>(std::move(schemes));

  AssertParse("/alpha=0/beta=3.25/gamma=hello", "",
              "alpha"_ == int32_t(0) and "beta"_ == 3.25f and "gamma"_ == "hello");

  auto wahr = scalar(true);
  AssertParse("/alpha=0/gamma=yo", "",
              "alpha"_ == int32_t(0) and *wahr and "gamma"_ == "yo");

  AssertParse("", "/ignored=2341", *wahr and *wahr and *wahr);
  AssertParse("", "/ignored=2341", *wahr and *wahr and *wahr);
}

TEST_F(TestPartitionScheme, EtlThenHive) {
  std::vector<std::unique_ptr<PartitionScheme>> schemes;
  schemes.emplace_back(new FieldPartitionScheme(field("year", int16())));
  schemes.emplace_back(new FieldPartitionScheme(field("month", int8())));
  schemes.emplace_back(new FieldPartitionScheme(field("day", int8())));
  schemes.emplace_back(new HivePartitionScheme(
      schema({field("alpha", int32()), field("beta", float32())})));
  scheme_ = std::make_shared<ChainPartitionScheme>(std::move(schemes));

  AssertParse("/1999/12/31/alpha=0/beta=3.25", "",
              "year"_ == int16_t(1999) and "month"_ == int8_t(12) and
                  "day"_ == int8_t(31) && ("alpha"_ == int32_t(0) and "beta"_ == 3.25f));

  AssertParseError("/20X6/03/21/alpha=0/beta=3.25");
}

}  // namespace dataset
}  // namespace arrow
