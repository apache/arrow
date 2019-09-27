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
    for (std::string suffix : {"", "/dat.parquet"}) {
      std::string unconsumed;
      std::shared_ptr<Expression> parsed;
      ASSERT_OK(scheme_->Parse(expected_consumed + expected_unconsumed + suffix,
                               &unconsumed, &parsed));

      ASSERT_NE(parsed, nullptr);
      ASSERT_EQ(expected_unconsumed + suffix, unconsumed);
      ASSERT_TRUE(parsed->Equals(*expected)) << parsed->ToString() << "\n"
                                             << expected->ToString();
    }
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
  scheme_ = std::make_shared<ConstantPartitionScheme>(expr);
  AssertParse("", "/hello/world", expr);
}

TEST_F(TestPartitionScheme, Hive) {
  scheme_ = std::make_shared<HivePartitionScheme>(
      schema({field("alpha", int32()), field("beta", float32())}));

  AssertParse("/alpha=0/beta=3.25", "", "alpha"_ == int32_t(0) and "beta"_ == 3.25f);
  AssertParse("/beta=3.25/alpha=0", "", "beta"_ == 3.25f and "alpha"_ == int32_t(0));
  AssertParse("/alpha=0", "", "alpha"_ == int32_t(0));
  AssertParse("/beta=3.25", "", "beta"_ == 3.25f);

  AssertParse("/alpha=0", "/unexpected/beta=3.25", "alpha"_ == int32_t(0));

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

TEST_F(TestPartitionScheme, Set) {
  // create an adhoc partition scheme which parses segments like "/x in [1 4 5]"
  // into ("x"_ == 1 or "x"_ == 4 or "x"_ == 5)
  scheme_ = std::make_shared<FunctionPartitionScheme>(
      [](const std::string& path, std::string* unconsumed,
         std::shared_ptr<Expression>* out) {
        std::smatch matches;
        std::regex re("^/x in \\[(.+)\\](.*)$");
        if (!std::regex_match(path, matches, re) || matches.size() != 3) {
          return Status::Invalid("regex failed to parse");
        }

        std::vector<std::shared_ptr<Expression>> element_equality;
        std::string element;
        std::istringstream elements(matches[1]);
        while (elements >> element) {
          std::shared_ptr<Scalar> s;
          RETURN_NOT_OK(Scalar::Parse(int32(), element, &s));
          element_equality.push_back(equal(field_ref("x"), scalar(s)));
        }

        *unconsumed = matches[2].str();
        *out = or_(std::move(element_equality));
        return Status::OK();
      });

  AssertParse("/x in [1 4 5]", "", "x"_ == 1 or "x"_ == 4 or "x"_ == 5);
}

TEST_F(TestPartitionScheme, Range) {
  std::vector<std::unique_ptr<PartitionScheme>> schemes;
  for (std::string x : {"x", "y", "z"}) {
    schemes.emplace_back(
        // create an adhoc partition scheme which parses segments like "/x=[-3.25, 0.0)"
        // into ("x"_ >= -3.25 and "x" < 0.0)
        new FunctionPartitionScheme([x](const std::string& path, std::string* unconsumed,
                                        std::shared_ptr<Expression>* out) {
          std::smatch matches;
          std::regex re("^/" + x +
                        "="
                        R"((\[|\())"  // open bracket or paren
                        "([^,/]+)"    // representation of range minimum
                        ", "
                        R"(([^,/\]\)]+))"  // representation of range maximum
                        R"((\]|\)))"       // close bracket or paren
                        "(.*)$");          // unconsumed
          if (!std::regex_match(path, matches, re) || matches.size() != 6) {
            return Status::Invalid("regex failed to parse");
          }

          auto& min_cmp = matches[1] == "[" ? greater_equal : greater;
          std::string min_repr = matches[2];
          std::string max_repr = matches[3];
          auto& max_cmp = matches[4] == "]" ? less_equal : less;
          *unconsumed = matches[5].str();

          std::shared_ptr<Scalar> min, max;
          RETURN_NOT_OK(Scalar::Parse(float64(), min_repr, &min));
          RETURN_NOT_OK(Scalar::Parse(float64(), max_repr, &max));

          auto field_x = field_ref(x);
          *out = and_(min_cmp(field_ref(x), scalar(min)),
                      max_cmp(field_ref(x), scalar(max)));
          return Status::OK();
        }));
  }
  scheme_ = std::make_shared<ChainPartitionScheme>(std::move(schemes));

  AssertParse("/x=[-1.5, 0.0)/y=[0.0, 1.5)/z=(1.5, 3.0]", "",
              ("x"_ >= -1.5 and "x"_ < 0.0) and ("y"_ >= 0.0 and "y"_ < 1.5) and
                  ("z"_ > 1.5 and "z"_ <= 3.0));
}

}  // namespace dataset
}  // namespace arrow
