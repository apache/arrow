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

#include "arrow/dataset/partition.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <memory>
#include <regex>
#include <string>
#include <vector>

#include "arrow/dataset/file_base.h"
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

  void AssertInspect(const std::vector<std::string>& paths,
                     const std::vector<std::shared_ptr<Field>>& expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, factory_->Inspect(paths));
    ASSERT_EQ(*actual, Schema(expected));
    ASSERT_OK_AND_ASSIGN(partitioning_, factory_->Finish(actual));
  }

  void AssertInspectError(const std::vector<std::string>& paths) {
    ASSERT_RAISES(Invalid, factory_->Inspect(paths));
  }

 protected:
  static std::shared_ptr<Field> Int(std::string name) {
    return field(std::move(name), int32());
  }

  static std::shared_ptr<Field> Str(std::string name) {
    return field(std::move(name), utf8());
  }

  static std::shared_ptr<Field> Dict(std::string name) {
    return field(std::move(name), dictionary(int32(), utf8()));
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

TEST_F(TestPartitioning, DictionaryInference) {
  PartitioningFactoryOptions options;
  options.max_partition_dictionary_size = 2;
  factory_ = DirectoryPartitioning::MakeFactory({"alpha", "beta"}, options);

  // type is still int32 if possible
  AssertInspect({"/0/1"}, {Int("alpha"), Int("beta")});

  // successful dictionary inference
  AssertInspect({"/a/0"}, {Dict("alpha"), Int("beta")});
  AssertInspect({"/a/0", "/a/1"}, {Dict("alpha"), Int("beta")});
  AssertInspect({"/a/0", "/b/0", "/a/1", "/b/1"}, {Dict("alpha"), Int("beta")});
  AssertInspect({"/a/-", "/b/-", "/a/_", "/b/_"}, {Dict("alpha"), Dict("beta")});

  // fall back to string if max dictionary size is exceeded
  AssertInspect({"/a/0", "/b/0", "/c/1", "/d/1"}, {Str("alpha"), Int("beta")});
}

TEST_F(TestPartitioning, DictionaryHasUniqueValues) {
  PartitioningFactoryOptions options;
  options.max_partition_dictionary_size = -1;
  factory_ = DirectoryPartitioning::MakeFactory({"alpha"}, options);

  auto alpha = Dict("alpha");
  AssertInspect({"/a", "/b", "/a", "/b", "/c", "/a"}, {alpha});
  ASSERT_OK_AND_ASSIGN(auto partitioning, factory_->Finish(schema({alpha})));

  auto expected_dictionary = internal::checked_pointer_cast<StringArray>(
      ArrayFromJSON(utf8(), R"(["a", "b", "c"])"));

  for (int32_t i = 0; i < expected_dictionary->length(); ++i) {
    DictionaryScalar::ValueType index_and_dictionary{std::make_shared<Int32Scalar>(i),
                                                     expected_dictionary};
    auto dictionary_scalar =
        std::make_shared<DictionaryScalar>(index_and_dictionary, alpha->type());

    auto path = "/" + expected_dictionary->GetString(i);
    AssertParse(path, "alpha"_ == dictionary_scalar);
  }

  AssertParseError("/yosemite");  // not in inspected dictionary
}

TEST_F(TestPartitioning, DiscoverSchemaSegfault) {
  // ARROW-7638
  factory_ = DirectoryPartitioning::MakeFactory({"alpha", "beta"});
  AssertInspectError({"oops.txt"});
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
                {Int("gamma"), Int("delta")});

  // schema field names are in order of first occurrence
  // (...so ensure your partitions are ordered the same for all paths)
  AssertInspect({"/alpha=0/beta=1", "/beta=2/alpha=3"}, {Int("alpha"), Int("beta")});

  // missing path segments will not cause an error
  AssertInspect({"/alpha=0/beta=1", "/beta=2/alpha=3", "/gamma=what"},
                {Int("alpha"), Int("beta"), Str("gamma")});
}

TEST_F(TestPartitioning, HiveDictionaryInference) {
  PartitioningFactoryOptions options;
  options.max_partition_dictionary_size = 2;
  factory_ = HivePartitioning::MakeFactory(options);

  // type is still int32 if possible
  AssertInspect({"/alpha=0/beta=1"}, {Int("alpha"), Int("beta")});

  // successful dictionary inference
  AssertInspect({"/alpha=a/beta=0"}, {Dict("alpha"), Int("beta")});
  AssertInspect({"/alpha=a/beta=0", "/alpha=a/1"}, {Dict("alpha"), Int("beta")});
  AssertInspect(
      {"/alpha=a/beta=0", "/alpha=b/beta=0", "/alpha=a/beta=1", "/alpha=b/beta=1"},
      {Dict("alpha"), Int("beta")});
  AssertInspect(
      {"/alpha=a/beta=-", "/alpha=b/beta=-", "/alpha=a/beta=_", "/alpha=b/beta=_"},
      {Dict("alpha"), Dict("beta")});

  // fall back to string if max dictionary size is exceeded
  AssertInspect(
      {"/alpha=a/beta=0", "/alpha=b/beta=0", "/alpha=c/beta=1", "/alpha=d/beta=1"},
      {Str("alpha"), Int("beta")});
}

TEST_F(TestPartitioning, HiveDictionaryHasUniqueValues) {
  PartitioningFactoryOptions options;
  options.max_partition_dictionary_size = -1;
  factory_ = HivePartitioning::MakeFactory(options);

  auto alpha = Dict("alpha");
  AssertInspect({"/alpha=a", "/alpha=b", "/alpha=a", "/alpha=b", "/alpha=c", "/alpha=a"},
                {alpha});
  ASSERT_OK_AND_ASSIGN(auto partitioning, factory_->Finish(schema({alpha})));

  auto expected_dictionary = internal::checked_pointer_cast<StringArray>(
      ArrayFromJSON(utf8(), R"(["a", "b", "c"])"));

  for (int32_t i = 0; i < expected_dictionary->length(); ++i) {
    DictionaryScalar::ValueType index_and_dictionary{std::make_shared<Int32Scalar>(i),
                                                     expected_dictionary};
    auto dictionary_scalar =
        std::make_shared<DictionaryScalar>(index_and_dictionary, alpha->type());

    auto path = "/alpha=" + expected_dictionary->GetString(i);
    AssertParse(path, "alpha"_ == dictionary_scalar);
  }

  AssertParseError("/alpha=yosemite");  // not in inspected dictionary
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

class TestPartitioningWritePlan : public ::testing::Test {
 protected:
  FragmentIterator MakeFragments(const ExpressionVector& partition_expressions) {
    fragments_.clear();
    for (const auto& expr : partition_expressions) {
      fragments_.emplace_back(new InMemoryFragment(RecordBatchVector{}, expr));
    }
    return MakeVectorIterator(fragments_);
  }

  std::shared_ptr<Expression> ExpressionPtr(const Expression& e) { return e.Copy(); }
  std::shared_ptr<Expression> ExpressionPtr(std::shared_ptr<Expression> e) { return e; }

  template <typename... E>
  FragmentIterator MakeFragments(const E&... partition_expressions) {
    return MakeFragments(ExpressionVector{ExpressionPtr(partition_expressions)...});
  }

  template <typename... E>
  void MakeWritePlan(const E&... partition_expressions) {
    auto fragments = MakeFragments(partition_expressions...);
    EXPECT_OK_AND_ASSIGN(plan_,
                         factory_->MakeWritePlan(schema({}), std::move(fragments)));
  }

  template <typename... E>
  Status MakeWritePlanError(const E&... partition_expressions) {
    auto fragments = MakeFragments(partition_expressions...);
    return factory_->MakeWritePlan(schema({}), std::move(fragments)).status();
  }

  template <typename... E>
  void MakeWritePlanWithSchema(const std::shared_ptr<Schema>& partition_schema,
                               const E&... partition_expressions) {
    auto fragments = MakeFragments(partition_expressions...);
    EXPECT_OK_AND_ASSIGN(plan_, factory_->MakeWritePlan(schema({}), std::move(fragments),
                                                        partition_schema));
  }

  template <typename... E>
  Status MakeWritePlanWithSchemaError(const std::shared_ptr<Schema>& partition_schema,
                                      const E&... partition_expressions) {
    auto fragments = MakeFragments(partition_expressions...);
    return factory_->MakeWritePlan(schema({}), std::move(fragments), partition_schema)
        .status();
  }

  struct ExpectedWritePlan {
    ExpectedWritePlan() = default;

    ExpectedWritePlan(const WritePlan& actual_plan, const FragmentVector& fragments) {
      int i = 0;
      for (const auto& op : actual_plan.fragment_or_partition_expressions) {
        if (op.kind() == WritePlan::FragmentOrPartitionExpression::FRAGMENT) {
          auto fragment = op.fragment();
          auto fragment_index =
              static_cast<int>(std::find(fragments.begin(), fragments.end(), fragment) -
                               fragments.begin());
          auto path = fs::internal::GetAbstractPathParent(actual_plan.paths[i]).first;
          dirs_[path + "/"].fragments.push_back(fragment_index);
        } else {
          auto partition_expression = op.partition_expr();
          dirs_[actual_plan.paths[i]].partition_expression = partition_expression;
        }
        ++i;
      }
    }

    ExpectedWritePlan Dir(const std::string& path, const Expression& expr,
                          const std::vector<int>& fragments) && {
      dirs_.emplace(path, DirectoryWriteOp{expr.Copy(), fragments});
      return std::move(*this);
    }

    struct DirectoryWriteOp {
      std::shared_ptr<Expression> partition_expression;
      std::vector<int> fragments;

      bool operator==(const DirectoryWriteOp& other) const {
        return partition_expression->Equals(other.partition_expression) &&
               fragments == other.fragments;
      }

      friend void PrintTo(const DirectoryWriteOp& op, std::ostream* os) {
        *os << op.partition_expression->ToString();

        *os << " { ";
        for (const auto& fragment : op.fragments) {
          *os << fragment << " ";
        }
        *os << "}\n";
      }
    };
    std::map<std::string, DirectoryWriteOp> dirs_;
  };

  struct AssertPlanIs : ExpectedWritePlan {};

  void AssertPlanIs(ExpectedWritePlan expected_plan) {
    ExpectedWritePlan actual_plan(plan_, fragments_);
    EXPECT_THAT(actual_plan.dirs_, testing::ContainerEq(expected_plan.dirs_));
  }

  FragmentVector fragments_;
  std::shared_ptr<ScanOptions> scan_options_ = ScanOptions::Make(schema({}));
  std::shared_ptr<PartitioningFactory> factory_;
  WritePlan plan_;
};

TEST_F(TestPartitioningWritePlan, Empty) {
  factory_ = DirectoryPartitioning::MakeFactory({"a", "b"});

  // no expressions from which to infer the types of fields a, b
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("No fragments"),
                                  MakeWritePlanError());

  MakeWritePlanWithSchema(schema({field("a", int32()), field("b", utf8())}));
  AssertPlanIs({});

  factory_ = HivePartitioning::MakeFactory();
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented, testing::HasSubstr("hive"),
                                  MakeWritePlanError());
}

TEST_F(TestPartitioningWritePlan, SingleDirectory) {
  factory_ = DirectoryPartitioning::MakeFactory({"a"});

  MakeWritePlan("a"_ == 42, "a"_ == 99, "a"_ == 101);
  AssertPlanIs(ExpectedWritePlan()
                   .Dir("42/", "a"_ == 42, {0})
                   .Dir("99/", "a"_ == 99, {1})
                   .Dir("101/", "a"_ == 101, {2}));

  MakeWritePlan("a"_ == 42, "a"_ == 99, "a"_ == 99, "a"_ == 101, "a"_ == 99);
  AssertPlanIs(ExpectedWritePlan()
                   .Dir("42/", "a"_ == 42, {0})
                   .Dir("99/", "a"_ == 99, {1, 2, 4})
                   .Dir("101/", "a"_ == 101, {3}));
}

TEST_F(TestPartitioningWritePlan, NestedDirectories) {
  factory_ = DirectoryPartitioning::MakeFactory({"a", "b"});

  MakeWritePlan("a"_ == 42 and "b"_ == "hello", "a"_ == 42 and "b"_ == "world",
                "a"_ == 99 and "b"_ == "hello", "a"_ == 99 and "b"_ == "world");

  AssertPlanIs(ExpectedWritePlan()
                   .Dir("42/", "a"_ == 42, {})
                   .Dir("42/hello/", "b"_ == "hello", {0})
                   .Dir("42/world/", "b"_ == "world", {1})
                   .Dir("99/", "a"_ == 99, {})
                   .Dir("99/hello/", "b"_ == "hello", {2})
                   .Dir("99/world/", "b"_ == "world", {3}));
}

TEST_F(TestPartitioningWritePlan, Errors) {
  factory_ = DirectoryPartitioning::MakeFactory({"a"});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("no partition expression for field 'a'"),
      MakeWritePlanError("a"_ == 42, scalar(true), "a"_ == 101));

  EXPECT_RAISES_WITH_MESSAGE_THAT(TypeError,
                                  testing::HasSubstr("expected RHS to have type int32"),
                                  MakeWritePlanError("a"_ == 42, "a"_ == "hello"));

  factory_ = DirectoryPartitioning::MakeFactory({"a", "b"});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("no partition expression for field 'a'"),
      MakeWritePlanError("a"_ == 42 and "b"_ == "hello", "a"_ == 99 and "b"_ == "world",
                         "b"_ == "forever alone"));
}

TEST(TestStripPrefixAndFilename, Basic) {
  ASSERT_EQ(StripPrefixAndFilename("", ""), "");
  ASSERT_EQ(StripPrefixAndFilename("a.csv", ""), "");
  ASSERT_EQ(StripPrefixAndFilename("a/b.csv", ""), "a");
  ASSERT_EQ(StripPrefixAndFilename("/a/b/c.csv", "/a"), "b");
  ASSERT_EQ(StripPrefixAndFilename("/a/b/c/d.csv", "/a"), "b/c");
  ASSERT_EQ(StripPrefixAndFilename("/a/b/c.csv", "/a/b"), "");

  std::vector<std::string> input{"/data/year=2019/file.parquet",
                                 "/data/year=2019/month=12/file.parquet",
                                 "/data/year=2019/month=12/day=01/file.parquet"};
  EXPECT_THAT(StripPrefixAndFilename(input, "/data"),
              testing::ElementsAre("year=2019", "year=2019/month=12",
                                   "year=2019/month=12/day=01"));
}  // namespace dataset

}  // namespace dataset
}  // namespace arrow
