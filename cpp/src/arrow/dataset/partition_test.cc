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
#include <regex>
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

class TestPartitioning : public ::testing::Test {
 public:
  void AssertParseError(const std::string& path) {
    ASSERT_RAISES(Invalid, partitioning_->Parse(path).status());
  }

  void AssertParse(const std::string& path, std::shared_ptr<Expression> expected) {
    ASSERT_OK_AND_ASSIGN(auto parsed, partitioning_->Parse(path));
    ASSERT_EQ(E{parsed}, E{expected});
  }

  void AssertParse(const std::string& path, const Expression& expected) {
    AssertParse(path, expected.Copy());
  }

  void AssertInspect(const std::vector<util::string_view>& paths,
                     const std::vector<std::shared_ptr<Field>>& expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, factory_->Inspect(paths));
    ASSERT_EQ(*actual, Schema(expected));
    ASSERT_OK(factory_->Finish(actual).status());
  }

 protected:
  static std::shared_ptr<Field> Int(std::string name) {
    return field(std::move(name), int32());
  }

  static std::shared_ptr<Field> Str(std::string name) {
    return field(std::move(name), utf8());
  }

  std::shared_ptr<Partitioning> partitioning_;
  std::shared_ptr<PartitioningFactory> factory_;
};

TEST_F(TestPartitioning, SegmentDictionary) {
  using Dict = std::unordered_map<std::string, std::shared_ptr<Expression>>;
  Dict alpha_dict, beta_dict;
  auto add_expr = [](const std::string& segment, const Expression& expr, Dict* dict) {
    dict->emplace(segment, expr.Copy());
  };

  add_expr("zero", "alpha"_ == 0, &alpha_dict);
  add_expr("one", "alpha"_ == 1, &alpha_dict);
  add_expr("more", "alpha"_ >= 2, &alpha_dict);

  add_expr("...", "beta"_ == "uh", &beta_dict);
  add_expr("?", "beta"_ == "what", &beta_dict);
  add_expr("!", "beta"_ == "OH", &beta_dict);

  partitioning_.reset(new SegmentDictionaryPartitioning(
      schema({field("alpha", int32()), field("beta", utf8())}), {alpha_dict, beta_dict}));

  AssertParse("/one/?", "alpha"_ == int32_t(1) and "beta"_ == "what");
  AssertParse("/one/?/---", "alpha"_ == int32_t(1) and "beta"_ == "what");
  AssertParse("/more", "alpha"_ >= int32_t(2));      // missing second segment
  AssertParse("/---/---", scalar(true));             // unrecognized segments
  AssertParse("/---/!", "beta"_ == "OH");            // unrecognized first segment
  AssertParse("/zero/---", "alpha"_ == int32_t(0));  // unrecognized second segment
  AssertParse("", scalar(true));                     // no segments to parse
}

TEST_F(TestPartitioning, DirectoryPartitioning) {
  partitioning_ = std::make_shared<DirectoryPartitioning>(
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

TEST_F(TestPartitioning, DiscoverSchema) {
  factory_ = DirectoryPartitioning::MakeFactory({"alpha", "beta"});

  // type is int32 if possible
  AssertInspect({"/0/1"}, {Int("alpha"), Int("beta")});

  // extra segments are ignored
  AssertInspect({"/0/1/what"}, {Int("alpha"), Int("beta")});

  // fall back to string if any segment for field alpha is not parseable as int
  AssertInspect({"/0/1", "/hello/1"}, {Str("alpha"), Int("beta")});

  // missing segment for beta doesn't cause an error or fallback
  AssertInspect({"/0/1", "/hello"}, {Str("alpha"), Int("beta")});
}

TEST_F(TestPartitioning, DiscoverSchemaSegfault) {
  // ARROW-7638
  factory_ = DirectoryPartitioning::MakeFactory({"alpha", "beta"});
  ASSERT_OK_AND_ASSIGN(auto actual, factory_->Inspect({"oops.txt"}));
  ASSERT_EQ(*actual, Schema({Str("alpha")}));
}

TEST_F(TestPartitioning, HivePartitioning) {
  partitioning_ = std::make_shared<HivePartitioning>(
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

TEST_F(TestPartitioning, DiscoverHiveSchema) {
  factory_ = HivePartitioning::MakeFactory();

  // type is int32 if possible
  AssertInspect({"/alpha=0/beta=1"}, {Int("alpha"), Int("beta")});

  // extra segments are ignored
  AssertInspect({"/gamma=0/unexpected/delta=1/dat.parquet"},
                {Int("delta"), Int("gamma")});

  // order doesn't matter
  AssertInspect({"/alpha=0/beta=1", "/beta=2/alpha=3", "/gamma=what"},
                {Int("alpha"), Int("beta"), Str("gamma")});
}

TEST_F(TestPartitioning, EtlThenHive) {
  DirectoryPartitioning etl_part(schema({field("year", int16()), field("month", int8()),
                                         field("day", int8()), field("hour", int8())}));
  HivePartitioning alphabeta_part(
      schema({field("alpha", int32()), field("beta", float32())}));

  partitioning_ = std::make_shared<FunctionPartitioning>(
      schema({field("year", int16()), field("month", int8()), field("day", int8()),
              field("hour", int8()), field("alpha", int32()), field("beta", float32())}),
      [&](const std::string& segment, int i) -> Result<std::shared_ptr<Expression>> {
        if (i < etl_part.schema()->num_fields()) {
          return etl_part.Parse(segment, i);
        }

        i -= etl_part.schema()->num_fields();
        return alphabeta_part.Parse(segment, i);
      });

  AssertParse("/1999/12/31/00/alpha=0/beta=3.25",
              "year"_ == int16_t(1999) and "month"_ == int8_t(12) and
                  "day"_ == int8_t(31) and "hour"_ == int8_t(0) and
                  "alpha"_ == int32_t(0) and "beta"_ == 3.25f);

  AssertParseError("/20X6/03/21/05/alpha=0/beta=3.25");
}

TEST_F(TestPartitioning, Set) {
  // An adhoc partitioning which parses segments like "/x in [1 4 5]"
  // into ("x"_ == 1 or "x"_ == 4 or "x"_ == 5)
  partitioning_ = std::make_shared<FunctionPartitioning>(
      schema({field("x", int32())}),
      [](const std::string& segment, int) -> Result<std::shared_ptr<Expression>> {
        std::smatch matches;

        static std::regex re("^x in \\[(.*)\\]$");
        if (!std::regex_match(segment, matches, re) || matches.size() != 2) {
          return Status::Invalid("regex failed to parse");
        }

        ExpressionVector subexpressions;
        std::string element;
        std::istringstream elements(matches[1]);
        while (elements >> element) {
          ARROW_ASSIGN_OR_RAISE(auto s, Scalar::Parse(int32(), element));
          subexpressions.push_back(equal(field_ref("x"), scalar(s)));
        }

        return or_(std::move(subexpressions));
      });

  AssertParse("/x in [1]", "x"_ == 1);
  AssertParse("/x in [1 4 5]", "x"_ == 1 or "x"_ == 4 or "x"_ == 5);
  AssertParse("/x in []", scalar(false));
}

// An adhoc partitioning which parses segments like "/x=[-3.25, 0.0)"
// into ("x"_ >= -3.25 and "x" < 0.0)
class RangePartitioning : public HivePartitioning {
 public:
  using HivePartitioning::HivePartitioning;

  std::string type_name() const override { return "range"; }

  Result<std::shared_ptr<Expression>> Parse(const std::string& segment,
                                            int i) const override {
    ExpressionVector ranges;
    auto key = ParseKey(segment, i);
    if (!key) {
      return Status::Invalid("can't parse '", segment, "' as a range");
    }

    std::smatch matches;
    RETURN_NOT_OK(DoRegex(key->value, &matches));

    auto& min_cmp = matches[1] == "[" ? greater_equal : greater;
    std::string min_repr = matches[2];
    std::string max_repr = matches[3];
    auto& max_cmp = matches[4] == "]" ? less_equal : less;

    const auto& type = schema_->GetFieldByName(key->name)->type();
    ARROW_ASSIGN_OR_RAISE(auto min, Scalar::Parse(type, min_repr));
    ARROW_ASSIGN_OR_RAISE(auto max, Scalar::Parse(type, max_repr));

    ranges.push_back(and_(min_cmp(field_ref(key->name), scalar(min)),
                          max_cmp(field_ref(key->name), scalar(max))));
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

TEST_F(TestPartitioning, Range) {
  partitioning_ = std::make_shared<RangePartitioning>(
      schema({field("x", float64()), field("y", float64()), field("z", float64())}));

  AssertParse("/x=[-1.5 0.0)/y=[0.0 1.5)/z=(1.5 3.0]",
              ("x"_ >= -1.5 and "x"_ < 0.0) and ("y"_ >= 0.0 and "y"_ < 1.5) and
                  ("z"_ > 1.5 and "z"_ <= 3.0));
}

}  // namespace dataset
}  // namespace arrow
