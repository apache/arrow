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

namespace arrow {
namespace dataset {

using E = TestExpression;

class TestPartitionScheme : public ::testing::Test {
 public:
  void AssertParseError(const std::string& path) {
    ASSERT_RAISES(Invalid, scheme_->Parse(path).status());
  }

  void AssertParse(const std::string& path, std::shared_ptr<Expression> expected) {
    ASSERT_OK_AND_ASSIGN(auto parsed, scheme_->Parse(path));
    ASSERT_EQ(E{parsed}, E{expected});
  }

  void AssertParse(const std::string& path, const Expression& expected) {
    AssertParse(path, expected.Copy());
  }

 protected:
  std::shared_ptr<PartitionScheme> scheme_;
};

TEST_F(TestPartitionScheme, Simple) {
  auto expr = equal(field_ref("alpha"), scalar<int16_t>(3));
  scheme_ = std::make_shared<ConstantPartitionScheme>(expr);
  AssertParse("/hello/world", expr);
}

TEST_F(TestPartitionScheme, Schema) {
  scheme_ = std::make_shared<SchemaPartitionScheme>(
      schema({field("alpha", int32()), field("beta", utf8())}));

  AssertParse("/0/hello", "alpha"_ == int32_t(0) and "beta"_ == "hello");
  AssertParse("/3", "alpha"_ == int32_t(3));
  AssertParseError("/world/0");   // reversed order
  AssertParseError("/0.0/foo");   // invalid alpha
  AssertParseError("/3.25");      // invalid alpha with missing beta
  AssertParse("", scalar(true));  // no segments to parse

  // gotcha someday:
  AssertParse("/0/dat.parquet", "alpha"_ == int32_t(0) and "beta"_ == "dat.parquet");

  AssertParse("/0/foo/ignored=2341", "alpha"_ == int32_t(0) and "beta"_ == "foo");
}

TEST_F(TestPartitionScheme, Hive) {
  scheme_ = std::make_shared<HivePartitionScheme>(
      schema({field("alpha", int32()), field("beta", float32())}));

  AssertParse("/alpha=0/beta=3.25", "alpha"_ == int32_t(0) and "beta"_ == 3.25f);
  AssertParse("/beta=3.25/alpha=0", "beta"_ == 3.25f and "alpha"_ == int32_t(0));
  AssertParse("/alpha=0", "alpha"_ == int32_t(0));
  AssertParse("/beta=3.25", "beta"_ == 3.25f);
  AssertParse("", scalar(true));

  AssertParse("/alpha=0/unexpected/beta=3.25",
              "alpha"_ == int32_t(0) and "beta"_ == 3.25f);

  AssertParse("/alpha=0/beta=3.25/ignored=2341",
              "alpha"_ == int32_t(0) and "beta"_ == 3.25f);

  AssertParse("/ignored=2341", scalar(true));

  AssertParseError("/alpha=0.0/beta=3.25");  // conversion of "0.0" to int32 fails
}

template <typename T>
void PopFront(size_t n, std::vector<T>* v) {
  std::move(v->begin() + n, v->end(), v->begin());
  v->resize(v->size() - n);
}

TEST_F(TestPartitionScheme, EtlThenHive) {
  SchemaPartitionScheme etl_scheme(schema({field("year", int16()), field("month", int8()),
                                           field("day", int8()), field("hour", int8())}));
  HivePartitionScheme alphabeta_scheme(
      schema({field("alpha", int32()), field("beta", float32())}));

  scheme_ = std::make_shared<FunctionPartitionScheme>(
      [&](const std::string& path) -> Result<std::shared_ptr<Expression>> {
        ARROW_ASSIGN_OR_RAISE(auto etl_expr, etl_scheme.Parse(path));

        auto segments = fs::internal::SplitAbstractPath(path);
        PopFront(etl_scheme.schema()->num_fields(), &segments);
        ARROW_ASSIGN_OR_RAISE(
            auto alphabeta_expr,
            alphabeta_scheme.Parse(fs::internal::JoinAbstractPath(segments)));

        return and_(std::move(etl_expr), std::move(alphabeta_expr));
      });

  AssertParse("/1999/12/31/00/alpha=0/beta=3.25",
              "year"_ == int16_t(1999) and "month"_ == int8_t(12) and
                  "day"_ == int8_t(31) and "hour"_ == int8_t(0) and
                  ("alpha"_ == int32_t(0) and "beta"_ == 3.25f));

  AssertParseError("/20X6/03/21/05/alpha=0/beta=3.25");
}

TEST_F(TestPartitionScheme, Set) {
  // An adhoc partition scheme which parses segments like "/x in [1 4 5]"
  // into ("x"_ == 1 or "x"_ == 4 or "x"_ == 5)
  scheme_ = std::make_shared<FunctionPartitionScheme>(
      [](const std::string& path) -> Result<std::shared_ptr<Expression>> {
        std::smatch matches;
        auto segment = std::move(fs::internal::SplitAbstractPath(path)[0]);
        static std::regex re("^x in \\[(.*)\\]$");
        if (!std::regex_match(segment, matches, re) || matches.size() != 2) {
          return Status::Invalid("regex failed to parse");
        }

        ExpressionVector subexpressions;
        std::string element;
        std::istringstream elements(matches[1]);
        while (elements >> element) {
          std::shared_ptr<Scalar> s;
          RETURN_NOT_OK(Scalar::Parse(int32(), element, &s));
          subexpressions.push_back(equal(field_ref("x"), scalar(s)));
        }

        return or_(std::move(subexpressions));
      });

  AssertParse("/x in [1]", "x"_ == 1);
  AssertParse("/x in [1 4 5]", "x"_ == 1 or "x"_ == 4 or "x"_ == 5);
  AssertParse("/x in []", scalar(false));
}

// An adhoc partition scheme which parses segments like "/x=[-3.25, 0.0)"
// into ("x"_ >= -3.25 and "x" < 0.0)
class RangePartitionScheme : public HivePartitionScheme {
 public:
  using HivePartitionScheme::HivePartitionScheme;

  std::string name() const override { return "range_partition_scheme"; }

  Result<std::shared_ptr<Expression>> Parse(const std::string& path) const override {
    ExpressionVector ranges;
    for (auto key : GetUnconvertedKeys(path)) {
      std::smatch matches;
      RETURN_NOT_OK(DoRegex(key.value, &matches));

      auto& min_cmp = matches[1] == "[" ? greater_equal : greater;
      std::string min_repr = matches[2];
      std::string max_repr = matches[3];
      auto& max_cmp = matches[4] == "]" ? less_equal : less;

      const auto& type = schema_->GetFieldByName(key.name)->type();
      std::shared_ptr<Scalar> min, max;
      RETURN_NOT_OK(Scalar::Parse(type, min_repr, &min));
      RETURN_NOT_OK(Scalar::Parse(type, max_repr, &max));

      ranges.push_back(and_(min_cmp(field_ref(key.name), scalar(min)),
                            max_cmp(field_ref(key.name), scalar(max))));
    }
    return and_(ranges);
  }

  static Status DoRegex(const std::string& segment, std::smatch* matches) {
    static std::regex re(
        "^"
        "([\\[\\(])"  // open bracket or paren
        "([^ ]+)"     // representation of range minimum
        " "
        "([^ ]+)"     // representation of range maximum
        "([\\]\\)])"  // close bracket or paren
        "$");

    if (!std::regex_match(segment, *matches, re) || matches->size() != 5) {
      return Status::Invalid("regex failed to parse");
    }

    return Status::OK();
  }
};

TEST_F(TestPartitionScheme, Range) {
  scheme_ = std::make_shared<RangePartitionScheme>(
      schema({field("x", float64()), field("y", float64()), field("z", float64())}));

  AssertParse("/x=[-1.5 0.0)/y=[0.0 1.5)/z=(1.5 3.0]",
              ("x"_ >= -1.5 and "x"_ < 0.0) and ("y"_ >= 0.0 and "y"_ < 1.5) and
                  ("z"_ > 1.5 and "z"_ <= 3.0));
}

}  // namespace dataset
}  // namespace arrow
