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
#include "arrow/dataset/scanner_internal.h"
#include "arrow/dataset/test_util.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

namespace arrow {
using internal::checked_pointer_cast;

namespace dataset {

using E = TestExpression;

class TestPartitioning : public ::testing::Test {
 public:
  void AssertParseError(const std::string& path) {
    ASSERT_RAISES(Invalid, partitioning_->Parse(path));
  }

  void AssertParse(const std::string& path, E expected) {
    ASSERT_OK_AND_ASSIGN(auto parsed, partitioning_->Parse(path));
    ASSERT_EQ(E{parsed}, expected);
  }

  template <StatusCode code = StatusCode::Invalid>
  void AssertFormatError(E expr) {
    ASSERT_EQ(partitioning_->Format(*expr.expression).status().code(), code);
  }

  void AssertFormat(E expr, const std::string& expected) {
    ASSERT_OK_AND_ASSIGN(auto formatted, partitioning_->Format(*expr.expression));
    ASSERT_EQ(formatted, expected);

    // ensure the formatted path round trips the relevant components of the partition
    // expression: roundtripped should be a subset of expr
    ASSERT_OK_AND_ASSIGN(auto roundtripped, partitioning_->Parse(formatted));
    ASSERT_EQ(E{roundtripped->Assume(*expr.expression)}, E{scalar(true)});
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

TEST_F(TestPartitioning, DirectoryPartitioningFormat) {
  partitioning_ = std::make_shared<DirectoryPartitioning>(
      schema({field("alpha", int32()), field("beta", utf8())}));

  AssertFormat("alpha"_ == int32_t(0) and "beta"_ == "hello", "0/hello");
  AssertFormat("beta"_ == "hello" and "alpha"_ == int32_t(0), "0/hello");
  AssertFormat("alpha"_ == int32_t(0), "0");
  AssertFormatError("beta"_ == "hello");
  AssertFormat(scalar(true), "");

  AssertFormatError<StatusCode::TypeError>("alpha"_ == 0.0 and "beta"_ == "hello");
  AssertFormat("gamma"_ == "yo" and "alpha"_ == int32_t(0) and "beta"_ == "hello",
               "0/hello");
}

TEST_F(TestPartitioning, DirectoryPartitioningWithTemporal) {
  for (auto temporal : {timestamp(TimeUnit::SECOND), date32()}) {
    partitioning_ = std::make_shared<DirectoryPartitioning>(
        schema({field("year", int32()), field("month", int8()), field("day", temporal)}));

    ASSERT_OK_AND_ASSIGN(auto day, StringScalar("2020-06-08").CastTo(temporal));
    AssertParse("/2020/06/2020-06-08",
                "year"_ == int32_t(2020) and "month"_ == int8_t(6) and "day"_ == day);
  }
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

TEST_F(TestPartitioning, HivePartitioningFormat) {
  partitioning_ = std::make_shared<HivePartitioning>(
      schema({field("alpha", int32()), field("beta", float32())}));

  AssertFormat("alpha"_ == int32_t(0) and "beta"_ == 3.25f, "alpha=0/beta=3.25");
  AssertFormat("beta"_ == 3.25f and "alpha"_ == int32_t(0), "alpha=0/beta=3.25");
  AssertFormat("alpha"_ == int32_t(0), "alpha=0");
  AssertFormat("beta"_ == 3.25f, "alpha/beta=3.25");
  AssertFormat(scalar(true), "");

  AssertFormatError<StatusCode::TypeError>("alpha"_ == "yo" and "beta"_ == 3.25f);
  AssertFormat("gamma"_ == "yo" and "alpha"_ == int32_t(0) and "beta"_ == 3.25f,
               "alpha=0/beta=3.25");
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
  FieldVector etl_fields{field("year", int16()), field("month", int8()),
                         field("day", int8()), field("hour", int8())};
  DirectoryPartitioning etl_part(schema(etl_fields));

  FieldVector alphabeta_fields{field("alpha", int32()), field("beta", float32())};
  HivePartitioning alphabeta_part(schema(alphabeta_fields));

  partitioning_ = std::make_shared<FunctionPartitioning>(
      schema({field("year", int16()), field("month", int8()), field("day", int8()),
              field("hour", int8()), field("alpha", int32()), field("beta", float32())}),
      [&](const std::string& path) -> Result<std::shared_ptr<Expression>> {
        auto segments = fs::internal::SplitAbstractPath(path);
        if (segments.size() < etl_fields.size() + alphabeta_fields.size()) {
          return Status::Invalid("path ", path, " can't be parsed");
        }

        auto etl_segments_end = segments.begin() + etl_fields.size();
        auto etl_path =
            fs::internal::JoinAbstractPath(segments.begin(), etl_segments_end);
        ARROW_ASSIGN_OR_RAISE(auto etl_expr, etl_part.Parse(etl_path));

        auto alphabeta_segments_end = etl_segments_end + alphabeta_fields.size();
        auto alphabeta_path =
            fs::internal::JoinAbstractPath(etl_segments_end, alphabeta_segments_end);
        ARROW_ASSIGN_OR_RAISE(auto alphabeta_expr, alphabeta_part.Parse(alphabeta_path));

        return and_(etl_expr, alphabeta_expr);
      });

  AssertParse("/1999/12/31/00/alpha=0/beta=3.25",
              "year"_ == int16_t(1999) and "month"_ == int8_t(12) and
                  "day"_ == int8_t(31) and "hour"_ == int8_t(0) and
                  ("alpha"_ == int32_t(0) and "beta"_ == 3.25f));

  AssertParseError("/20X6/03/21/05/alpha=0/beta=3.25");
}  // namespace dataset

TEST_F(TestPartitioning, Set) {
  auto ints = [](std::vector<int32_t> ints) {
    std::shared_ptr<Array> out;
    ArrayFromVector<Int32Type>(ints, &out);
    return out;
  };

  // An adhoc partitioning which parses segments like "/x in [1 4 5]"
  // into ("x"_ == 1 or "x"_ == 4 or "x"_ == 5)
  partitioning_ = std::make_shared<FunctionPartitioning>(
      schema({field("x", int32())}),
      [&](const std::string& path) -> Result<std::shared_ptr<Expression>> {
        ExpressionVector subexpressions;
        for (auto segment : fs::internal::SplitAbstractPath(path)) {
          std::smatch matches;

          static std::regex re(R"(^(\S+) in \[(.*)\]$)");
          if (!std::regex_match(segment, matches, re) || matches.size() != 3) {
            return Status::Invalid("regex failed to parse");
          }

          std::vector<int32_t> set;
          std::istringstream elements(matches[2]);
          for (std::string element; elements >> element;) {
            ARROW_ASSIGN_OR_RAISE(auto s, Scalar::Parse(int32(), element));
            set.push_back(checked_cast<const Int32Scalar&>(*s).value);
          }

          subexpressions.push_back(field_ref(matches[1])->In(ints(set)).Copy());
        }
        return and_(std::move(subexpressions));
      });

  AssertParse("/x in [1]", "x"_.In(ints({1})));
  AssertParse("/x in [1 4 5]", "x"_.In(ints({1, 4, 5})));
  AssertParse("/x in []", "x"_.In(ints({})));
}

// An adhoc partitioning which parses segments like "/x=[-3.25, 0.0)"
// into ("x"_ >= -3.25 and "x" < 0.0)
class RangePartitioning : public Partitioning {
 public:
  explicit RangePartitioning(std::shared_ptr<Schema> s) : Partitioning(std::move(s)) {}

  std::string type_name() const override { return "range"; }

  Result<std::shared_ptr<Expression>> Parse(const std::string& path) const override {
    ExpressionVector ranges;

    for (auto segment : fs::internal::SplitAbstractPath(path)) {
      auto key = HivePartitioning::ParseKey(segment);
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

  Result<std::string> Format(const Expression&) const override { return ""; }
  Result<std::vector<PartitionedBatch>> Partition(
      const std::shared_ptr<RecordBatch>&) const override {
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
}

}  // namespace dataset
}  // namespace arrow
