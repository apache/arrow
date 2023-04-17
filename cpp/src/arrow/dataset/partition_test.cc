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
#include <memory>
#include <regex>
#include <string>
#include <vector>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/test_util_internal.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/status.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/range.h"
#include "arrow/util/uri.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace dataset {

class TestPartitioning : public ::testing::Test {
 public:
  void AssertParseError(const std::string& path) {
    ASSERT_RAISES(Invalid, partitioning_->Parse(path));
  }

  void AssertParse(const std::string& path, compute::Expression expected) {
    ASSERT_OK_AND_ASSIGN(auto parsed, partitioning_->Parse(path));
    ASSERT_EQ(parsed, expected);
  }

  template <StatusCode code = StatusCode::Invalid>
  void AssertFormatError(compute::Expression expr) {
    ASSERT_EQ(partitioning_->Format(expr).status().code(), code);
  }

  void AssertFormat(compute::Expression expr, const std::string& expected_directory,
                    const std::string& expected_filename = "") {
    // formatted partition expressions are bound to the schema of the dataset being
    // written
    ASSERT_OK_AND_ASSIGN(auto formatted, partitioning_->Format(expr));
    ASSERT_EQ(formatted.directory, expected_directory);
    ASSERT_EQ(formatted.filename, expected_filename);

    // if ((formatted.filename).empty()){
    //   formatted.filename = "format.parquet";
    // }
    // ensure the formatted path round trips the relevant components of the partition
    // expression: roundtripped should be a subset of expr
    ASSERT_OK_AND_ASSIGN(compute::Expression roundtripped,
                         partitioning_->Parse(formatted.directory + formatted.filename));
    ASSERT_OK_AND_ASSIGN(roundtripped, roundtripped.Bind(*written_schema_));
    ASSERT_OK_AND_ASSIGN(auto simplified, SimplifyWithGuarantee(roundtripped, expr));
    ASSERT_EQ(simplified, literal(true));
  }

  void AssertInspect(const std::vector<std::string>& paths,
                     const std::vector<std::shared_ptr<Field>>& expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, factory_->Inspect(paths));
    ASSERT_EQ(*actual, Schema(expected));
    ASSERT_OK_AND_ASSIGN(partitioning_, factory_->Finish(actual));
  }

  void AssertPartition(const std::shared_ptr<Partitioning> partitioning,
                       const std::shared_ptr<RecordBatch> full_batch,
                       const RecordBatchVector& expected_batches,
                       const std::vector<compute::Expression>& expected_expressions) {
    ASSERT_OK_AND_ASSIGN(auto partition_results, partitioning->Partition(full_batch));
    std::shared_ptr<RecordBatch> rest = full_batch;

    ASSERT_EQ(partition_results.batches.size(), expected_batches.size());

    for (size_t i = 0; i < partition_results.batches.size(); i++) {
      std::shared_ptr<RecordBatch> actual_batch = partition_results.batches[i];
      compute::Expression actual_expression = partition_results.expressions[i];

      auto expected_expression = std::find(expected_expressions.begin(),
                                           expected_expressions.end(), actual_expression);
      ASSERT_NE(expected_expression, expected_expressions.end())
          << "Unexpected partition expr " << actual_expression.ToString();

      auto expected_batch =
          expected_batches[expected_expression - expected_expressions.begin()];

      SCOPED_TRACE("Batch for " + expected_expression->ToString());
      AssertBatchesEqual(*expected_batch, *actual_batch);
    }
  }

  void AssertPartition(const std::shared_ptr<Partitioning> partitioning,
                       const std::shared_ptr<Schema> schema,
                       const std::string& record_batch_json,
                       const std::shared_ptr<Schema> partitioned_schema,
                       const std::vector<std::string>& expected_record_batch_strs,
                       const std::vector<compute::Expression>& expected_expressions) {
    auto record_batch = RecordBatchFromJSON(schema, record_batch_json);
    RecordBatchVector expected_batches;
    for (const auto& expected_record_batch_str : expected_record_batch_strs) {
      expected_batches.push_back(
          RecordBatchFromJSON(partitioned_schema, expected_record_batch_str));
    }
    AssertPartition(partitioning, record_batch, expected_batches, expected_expressions);
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

  static std::shared_ptr<Field> DictStr(std::string name) {
    return field(std::move(name), dictionary(int32(), utf8()));
  }

  static std::shared_ptr<Field> DictInt(std::string name) {
    return field(std::move(name), dictionary(int32(), int32()));
  }

  std::shared_ptr<Partitioning> partitioning_;
  std::shared_ptr<PartitioningFactory> factory_;
  std::shared_ptr<Schema> written_schema_;
};

TEST_F(TestPartitioning, Partition) {
  auto dataset_schema =
      schema({field("a", int32()), field("b", utf8()), field("c", uint32())});

  auto partition_schema = schema({field("a", int32()), field("b", utf8())});

  auto physical_schema = schema({field("c", uint32())});

  auto partitioning = std::make_shared<DirectoryPartitioning>(partition_schema);
  std::string json = R"([{"a": 3,    "b": "x",  "c": 0},
                         {"a": 3,    "b": "x",  "c": 1},
                         {"a": 1,    "b": null, "c": 2},
                         {"a": null, "b": null, "c": 3},
                         {"a": null, "b": "z",  "c": 4},
                         {"a": null, "b": null, "c": 5}
                       ])";

  std::vector<std::string> expected_batches = {
      R"([{"c": 0}, {"c": 1}])",
      R"([{"c": 2}])",
      R"([{"c": 3}, {"c": 5}])",
      R"([{"c": 4}])",
  };

  std::vector<compute::Expression> expected_expressions = {
      and_(equal(field_ref("a"), literal(3)), equal(field_ref("b"), literal("x"))),
      and_(equal(field_ref("a"), literal(1)), is_null(field_ref("b"))),
      and_(is_null(field_ref("a")), is_null(field_ref("b"))),
      and_(is_null(field_ref("a")), equal(field_ref("b"), literal("z"))),
  };

  AssertPartition(partitioning, dataset_schema, json, physical_schema, expected_batches,
                  expected_expressions);
}

TEST_F(TestPartitioning, DefaultPartitioningIsDirectoryPartitioning) {
  auto partitioning = Partitioning::Default();
  ASSERT_EQ(partitioning->type_name(), "directory");
  AssertSchemaEqual(partitioning->schema(), schema({}));
}

TEST_F(TestPartitioning, DirectoryPartitioning) {
  partitioning_ = std::make_shared<DirectoryPartitioning>(
      schema({field("alpha", int32()), field("beta", utf8())}));

  AssertParse("/0/hello/", and_(equal(field_ref("alpha"), literal(0)),
                                equal(field_ref("beta"), literal("hello"))));
  AssertParse("/3/", equal(field_ref("alpha"), literal(3)));
  AssertParseError("/world/0/");   // reversed order
  AssertParseError("/0.0/foo/");   // invalid alpha
  AssertParseError("/3.25/");      // invalid alpha with missing beta
  AssertParse("", literal(true));  // no segments to parse

  // gotcha someday:
  AssertParse("/0/dat.parquet/", and_(equal(field_ref("alpha"), literal(0)),
                                      equal(field_ref("beta"), literal("dat.parquet"))));

  AssertParse("/0/foo/ignored=2341", and_(equal(field_ref("alpha"), literal(0)),
                                          equal(field_ref("beta"), literal("foo"))));
}

TEST_F(TestPartitioning, DirectoryPartitioningEmpty) {
  partitioning_ = std::make_shared<DirectoryPartitioning>(schema({}));
  written_schema_ = partitioning_->schema();

  // No partitioning info
  AssertParse("", literal(true));
  // Files can be in subdirectories
  AssertParse("/foo/", literal(true));
  // Partitioning info is discarded on write
  AssertFormat(equal(field_ref("alpha"), literal(7)), "");
}

TEST_F(TestPartitioning, DirectoryPartitioningEquals) {
  auto part = std::make_shared<DirectoryPartitioning>(
      schema({field("alpha", int32()), field("beta", utf8())}));
  auto other = std::make_shared<DirectoryPartitioning>(
      schema({field("alpha", int32()), field("gamma", utf8())}));
  auto another = std::make_shared<DirectoryPartitioning>(
      schema({field("alpha", int32()), field("beta", utf8())}));
  auto some_other = std::make_shared<DirectoryPartitioning>(
      schema({field("alpha", int32()), field("beta", utf8())}));
  EXPECT_TRUE(part->Equals(*part));
  EXPECT_FALSE(part->Equals(*other));
  EXPECT_TRUE(part->Equals(*another));
  EXPECT_TRUE(another->Equals(*some_other));
}

TEST_F(TestPartitioning, FilenamePartitioning) {
  partitioning_ = std::make_shared<FilenamePartitioning>(
      schema({field("alpha", int32()), field("beta", utf8())}));

  AssertParse("0_hello_", and_(equal(field_ref("alpha"), literal(0)),
                               equal(field_ref("beta"), literal("hello"))));
  AssertParse("0_", equal(field_ref("alpha"), literal(0)));
  AssertParseError("world_0_");    // reversed order
  AssertParseError("0.0_foo_");    // invalid alpha
  AssertParseError("3.25_");       // invalid alpha with missing beta
  AssertParse("", literal(true));  // no segments to parse

  AssertParse("0_foo_ignored=2341", and_(equal(field_ref("alpha"), literal(0)),
                                         equal(field_ref("beta"), literal("foo"))));
}

TEST_F(TestPartitioning, FilenamePartitioningEquals) {
  auto part = std::make_shared<FilenamePartitioning>(
      schema({field("alpha", int32()), field("beta", utf8())}));
  auto other_part = std::make_shared<FilenamePartitioning>(
      schema({field("sigma", int32()), field("beta", utf8())}));
  auto another_part = std::make_shared<FilenamePartitioning>(
      schema({field("sigma", int64()), field("beta", utf8())}));
  auto some_other_part = std::make_shared<FilenamePartitioning>(
      schema({field("sigma", int64()), field("beta", utf8())}));
  EXPECT_TRUE(part->Equals(*part));
  EXPECT_FALSE(part->Equals(*other_part));
  EXPECT_FALSE(other_part->Equals(*another_part));
  EXPECT_TRUE(another_part->Equals(*some_other_part));
}

TEST_F(TestPartitioning, DirectoryPartitioningFormat) {
  partitioning_ = std::make_shared<DirectoryPartitioning>(
      schema({field("alpha", int32()), field("beta", utf8())}));

  written_schema_ = partitioning_->schema();

  AssertFormat(and_(equal(field_ref("alpha"), literal(0)),
                    equal(field_ref("beta"), literal("hello"))),
               "0/hello");
  AssertFormat(and_(equal(field_ref("beta"), literal("hello")),
                    equal(field_ref("alpha"), literal(0))),
               "0/hello");
  AssertFormat(equal(field_ref("alpha"), literal(0)), "0");
  AssertFormat(and_(equal(field_ref("alpha"), literal(0)), is_null(field_ref("beta"))),
               "0");
  AssertFormatError(
      and_(is_null(field_ref("alpha")), equal(field_ref("beta"), literal("hello"))));
  AssertFormatError(equal(field_ref("beta"), literal("hello")));
  AssertFormat(literal(true), "");

  ASSERT_OK_AND_ASSIGN(written_schema_,
                       written_schema_->AddField(0, field("gamma", utf8())));
  AssertFormat(and_({equal(field_ref("gamma"), literal("yo")),
                     equal(field_ref("alpha"), literal(0)),
                     equal(field_ref("beta"), literal("hello"))}),
               "0/hello");

  // written_schema_ is incompatible with partitioning_'s schema
  written_schema_ = schema({field("alpha", utf8()), field("beta", utf8())});
  AssertFormatError<StatusCode::TypeError>(
      and_(equal(field_ref("alpha"), literal("0.0")),
           equal(field_ref("beta"), literal("hello"))));
}

TEST_F(TestPartitioning, DirectoryPartitioningFormatDictionary) {
  auto dictionary = ArrayFromJSON(utf8(), R"(["hello", "world"])");
  partitioning_ = std::make_shared<DirectoryPartitioning>(schema({DictStr("alpha")}),
                                                          ArrayVector{dictionary});
  written_schema_ = partitioning_->schema();

  ASSERT_OK_AND_ASSIGN(auto dict_hello, MakeScalar("hello")->CastTo(DictStr("")->type()));
  AssertFormat(equal(field_ref("alpha"), literal(dict_hello)), "hello");
}

TEST_F(TestPartitioning, DirectoryPartitioningFormatDictionaryCustomIndex) {
  // Make sure a non-int32 index type is properly cast to, else we fail a CHECK when
  // we construct a dictionary array with the wrong index type
  auto dict_type = dictionary(int8(), utf8());
  auto dictionary = ArrayFromJSON(utf8(), R"(["hello", "world"])");
  partitioning_ = std::make_shared<DirectoryPartitioning>(
      schema({field("alpha", dict_type)}), ArrayVector{dictionary});
  written_schema_ = partitioning_->schema();

  ASSERT_OK_AND_ASSIGN(auto dict_hello, MakeScalar("hello")->CastTo(dict_type));
  AssertFormat(equal(field_ref("alpha"), literal(dict_hello)), "hello");
}

TEST_F(TestPartitioning, DirectoryPartitioningWithTemporal) {
  for (auto temporal : {timestamp(TimeUnit::SECOND), date32()}) {
    partitioning_ = std::make_shared<DirectoryPartitioning>(
        schema({field("year", int32()), field("month", int8()), field("day", temporal)}));

    ASSERT_OK_AND_ASSIGN(auto day, StringScalar("2020-06-08").CastTo(temporal));
    AssertParse("/2020/06/2020-06-08/",
                and_({equal(field_ref("year"), literal(2020)),
                      equal(field_ref("month"), literal<int8_t>(6)),
                      equal(field_ref("day"), literal(day))}));
  }
}

TEST_F(TestPartitioning, DiscoverSchemaDirectory) {
  factory_ = DirectoryPartitioning::MakeFactory({"alpha", "beta"});

  // type is int32 if possible
  AssertInspect({"/0/1"}, {Int("alpha"), Int("beta")});

  // extra segments are ignored
  AssertInspect({"/0/1/what"}, {Int("alpha"), Int("beta")});

  // fall back to string if any segment for field alpha is not parseable as int
  AssertInspect({"/0/1", "/hello/1"}, {Str("alpha"), Int("beta")});

  // If there are too many digits fall back to string
  AssertInspect({"/3760212050/1"}, {Str("alpha"), Int("beta")});

  // missing segment for beta doesn't cause an error or fallback
  AssertInspect({"/0/1", "/hello"}, {Str("alpha"), Int("beta")});
}

TEST_F(TestPartitioning, DiscoverSchemaFilename) {
  factory_ = FilenamePartitioning::MakeFactory({"alpha", "beta"});

  // type is int32 if possible
  AssertInspect({"0_1_"}, {Int("alpha"), Int("beta")});

  // extra segments are ignored
  AssertInspect({"0_1_what_"}, {Int("alpha"), Int("beta")});

  // fall back to string if any segment for field alpha is not parseable as int
  AssertInspect({"0_1_", "hello_1_"}, {Str("alpha"), Int("beta")});

  // If there are too many digits fall back to string
  AssertInspect({"3760212050_1_"}, {Str("alpha"), Int("beta")});

  // Invalid syntax
  AssertInspectError({"234-12"});
  AssertInspectError({"hello"});
}

TEST_F(TestPartitioning, DirectoryDictionaryInference) {
  PartitioningFactoryOptions options;
  options.infer_dictionary = true;
  factory_ = DirectoryPartitioning::MakeFactory({"alpha", "beta"}, options);

  // type is still int32 if possible
  AssertInspect({"/0/1"}, {DictInt("alpha"), DictInt("beta")});

  // If there are too many digits fall back to string
  AssertInspect({"/3760212050/1"}, {DictStr("alpha"), DictInt("beta")});

  // successful dictionary inference
  AssertInspect({"/a/0"}, {DictStr("alpha"), DictInt("beta")});
  AssertInspect({"/a/0", "/a/1"}, {DictStr("alpha"), DictInt("beta")});
  AssertInspect({"/a/0", "/a"}, {DictStr("alpha"), DictInt("beta")});
  AssertInspect({"/0/a", "/1"}, {DictInt("alpha"), DictStr("beta")});
  AssertInspect({"/a/0", "/b/0", "/a/1", "/b/1"}, {DictStr("alpha"), DictInt("beta")});
  AssertInspect({"/a/-", "/b/-", "/a/_", "/b/_"}, {DictStr("alpha"), DictStr("beta")});
}

TEST_F(TestPartitioning, FilenameDictionaryInference) {
  PartitioningFactoryOptions options;
  options.infer_dictionary = true;
  factory_ = FilenamePartitioning::MakeFactory({"alpha", "beta"}, options);

  // type is still int32 if possible
  AssertInspect({"0_1_"}, {DictInt("alpha"), DictInt("beta")});

  // If there are too many digits fall back to string
  AssertInspect({"3760212050_1_"}, {DictStr("alpha"), DictInt("beta")});

  // successful dictionary inference
  AssertInspect({"a_0_"}, {DictStr("alpha"), DictInt("beta")});
  AssertInspect({"a_0_", "a_1_"}, {DictStr("alpha"), DictInt("beta")});
  AssertInspect({"a_0_", "b_0_", "a_1_", "b_1_"}, {DictStr("alpha"), DictInt("beta")});
}

TEST_F(TestPartitioning, DictionaryHasUniqueValues) {
  PartitioningFactoryOptions options;
  options.infer_dictionary = true;
  factory_ = DirectoryPartitioning::MakeFactory({"alpha"}, options);

  auto alpha = DictStr("alpha");
  AssertInspect({"/a", "/b", "/a", "/b", "/c", "/a"}, {alpha});
  ASSERT_OK_AND_ASSIGN(auto partitioning, factory_->Finish(schema({alpha})));

  auto expected_dictionary =
      checked_pointer_cast<StringArray>(ArrayFromJSON(utf8(), R"(["a", "b", "c"])"));

  for (int32_t i = 0; i < expected_dictionary->length(); ++i) {
    DictionaryScalar::ValueType index_and_dictionary{std::make_shared<Int32Scalar>(i),
                                                     expected_dictionary};
    auto dictionary_scalar =
        std::make_shared<DictionaryScalar>(index_and_dictionary, alpha->type());

    auto path = "/" + expected_dictionary->GetString(i) + "/";
    AssertParse(path, equal(field_ref("alpha"), literal(dictionary_scalar)));
  }

  AssertParseError("/yosemite/");  // not in inspected dictionary
}

TEST_F(TestPartitioning, DiscoverSchemaSegfault) {
  // ARROW-7638
  factory_ = DirectoryPartitioning::MakeFactory({"alpha", "beta"});
  AssertInspectError({"oops.txt"});
}

TEST_F(TestPartitioning, HivePartitioning) {
  partitioning_ = std::make_shared<HivePartitioning>(
      schema({field("alpha", int32()), field("beta", float32())}), ArrayVector(), "xyz");

  AssertParse("/alpha=0/beta=3.25/", and_(equal(field_ref("alpha"), literal(0)),
                                          equal(field_ref("beta"), literal(3.25f))));
  AssertParse("/beta=3.25/alpha=0/", and_(equal(field_ref("beta"), literal(3.25f)),
                                          equal(field_ref("alpha"), literal(0))));
  AssertParse("/alpha=0/", equal(field_ref("alpha"), literal(0)));
  AssertParse("/alpha=xyz/beta=3.25/", and_(is_null(field_ref("alpha")),
                                            equal(field_ref("beta"), literal(3.25f))));
  AssertParse("/beta=3.25/", equal(field_ref("beta"), literal(3.25f)));
  AssertParse("", literal(true));

  AssertParse("/alpha=0/beta=3.25/ignored=2341/",
              and_(equal(field_ref("alpha"), literal(0)),
                   equal(field_ref("beta"), literal(3.25f))));

  AssertParse("/alpha=0/beta=3.25/ignored=2341/",
              and_(equal(field_ref("alpha"), literal(0)),
                   equal(field_ref("beta"), literal(3.25f))));

  AssertParse("/ignored=2341/", literal(true));

  AssertParseError("/alpha=0.0/beta=3.25/");  // conversion of "0.0" to int32 fails
}

TEST_F(TestPartitioning, HivePartitioningEquals) {
  const auto& array_vector = ArrayVector();
  ArrayVector other_vector(2);
  other_vector[0] = ArrayFromJSON(utf8(), R"(["foo", "bar", "baz"])");
  other_vector[1] = ArrayFromJSON(utf8(), R"(["bar", "foo", "baz"])");
  auto part = std::make_shared<HivePartitioning>(
      schema({field("alpha", int32()), field("beta", float32())}), array_vector, "xyz");
  auto other_part = std::make_shared<HivePartitioning>(
      schema({field("sigma", int32()), field("beta", float32())}), array_vector, "xyz");
  auto another_part = std::make_shared<HivePartitioning>(
      schema({field("alpha", int32()), field("beta", float32())}), other_vector, "xyz");
  auto some_part = std::make_shared<HivePartitioning>(
      schema({field("alpha", int32()), field("beta", float32())}), array_vector, "abc");
  auto match_part = std::make_shared<HivePartitioning>(
      schema({field("alpha", int32()), field("beta", float32())}), array_vector, "xyz");
  EXPECT_TRUE(part->Equals(*part));
  EXPECT_FALSE(part->Equals(*other_part));
  EXPECT_FALSE(part->Equals(*another_part));
  EXPECT_FALSE(part->Equals(*some_part));
  EXPECT_TRUE(part->Equals(*match_part));
}

TEST_F(TestPartitioning, CrossCheckPartitioningEquals) {
  auto file_part = std::make_shared<FilenamePartitioning>(
      schema({field("alpha", int32()), field("beta", utf8())}));
  auto dir_part = std::make_shared<DirectoryPartitioning>(
      schema({field("alpha", int32()), field("beta", utf8())}));
  auto hive_part = std::make_shared<HivePartitioning>(
      schema({field("alpha", int32()), field("beta", float32())}), ArrayVector(), "xyz");
  EXPECT_FALSE(file_part->Equals(*dir_part));
  EXPECT_FALSE(dir_part->Equals(*file_part));
  EXPECT_FALSE(dir_part->Equals(*hive_part));
  EXPECT_FALSE(hive_part->Equals(*dir_part));
}

TEST_F(TestPartitioning, HivePartitioningFormat) {
  partitioning_ = std::make_shared<HivePartitioning>(
      schema({field("alpha", int32()), field("beta", float32())}), ArrayVector(), "xyz");

  written_schema_ = partitioning_->schema();

  AssertFormat(and_(equal(field_ref("alpha"), literal(0)),
                    equal(field_ref("beta"), literal(3.25f))),
               "alpha=0/beta=3.25");
  AssertFormat(and_(equal(field_ref("beta"), literal(3.25f)),
                    equal(field_ref("alpha"), literal(0))),
               "alpha=0/beta=3.25");
  AssertFormat(equal(field_ref("alpha"), literal(0)), "alpha=0");
  AssertFormat(and_(equal(field_ref("alpha"), literal(0)), is_null(field_ref("beta"))),
               "alpha=0/beta=xyz");
  AssertFormat(
      and_(is_null(field_ref("alpha")), equal(field_ref("beta"), literal(3.25f))),
      "alpha=xyz/beta=3.25");
  AssertFormat(literal(true), "");

  AssertFormat(and_(is_null(field_ref("alpha")), is_null(field_ref("beta"))),
               "alpha=xyz/beta=xyz");

  ASSERT_OK_AND_ASSIGN(written_schema_,
                       written_schema_->AddField(0, field("gamma", utf8())));
  AssertFormat(and_({equal(field_ref("gamma"), literal("yo")),
                     equal(field_ref("alpha"), literal(0)),
                     equal(field_ref("beta"), literal(3.25f))}),
               "alpha=0/beta=3.25");

  // written_schema_ is incompatible with partitioning_'s schema
  written_schema_ = schema({field("alpha", utf8()), field("beta", utf8())});
  AssertFormatError<StatusCode::TypeError>(
      and_(equal(field_ref("alpha"), literal("0.0")),
           equal(field_ref("beta"), literal("hello"))));

  partitioning_ = std::make_shared<HivePartitioning>(schema({field("x", large_utf8())}));
  AssertFormat(equal(field_ref("x"), literal("hello")), "x=hello");
}

TEST_F(TestPartitioning, FilenamePartitioningFormat) {
  partitioning_ = std::make_shared<FilenamePartitioning>(
      schema({field("alpha", int32()), field("beta", utf8())}));

  written_schema_ = partitioning_->schema();

  AssertFormat(and_(equal(field_ref("alpha"), literal(0)),
                    equal(field_ref("beta"), literal("hello"))),
               "", "0_hello_");
  AssertFormat(equal(field_ref("alpha"), literal(0)), "", "0_");
}

TEST_F(TestPartitioning, DiscoverHiveSchema) {
  auto options = HivePartitioningFactoryOptions();
  options.null_fallback = "xyz";
  factory_ = HivePartitioning::MakeFactory(options);

  // type is int32 if possible
  AssertInspect({"/alpha=0/beta=1"}, {Int("alpha"), Int("beta")});

  // extra segments are ignored
  AssertInspect({"/gamma=0/unexpected/delta=1/dat.parquet"},
                {Int("gamma"), Int("delta")});

  // schema field names are in order of first occurrence
  // (...so ensure your partitions are ordered the same for all paths)
  AssertInspect({"/alpha=0/beta=1", "/beta=2/alpha=3"}, {Int("alpha"), Int("beta")});

  // Null fallback strings shouldn't interfere with type inference
  AssertInspect({"/alpha=xyz/beta=x", "/alpha=7/beta=xyz"}, {Int("alpha"), Str("beta")});

  // Cannot infer if the only values are null
  AssertInspectError({"/alpha=xyz"});

  // If there are too many digits fall back to string
  AssertInspect({"/alpha=3760212050"}, {Str("alpha")});

  // missing path segments will not cause an error
  AssertInspect({"/alpha=0/beta=1", "/beta=2/alpha=3", "/gamma=what"},
                {Int("alpha"), Int("beta"), Str("gamma")});
}

TEST_F(TestPartitioning, HiveDictionaryInference) {
  HivePartitioningFactoryOptions options;
  options.infer_dictionary = true;
  options.null_fallback = "xyz";
  factory_ = HivePartitioning::MakeFactory(options);

  // type is still int32 if possible
  AssertInspect({"/alpha=0/beta=1"}, {DictInt("alpha"), DictInt("beta")});

  // If there are too many digits fall back to string
  AssertInspect({"/alpha=3760212050"}, {DictStr("alpha")});

  // successful dictionary inference
  AssertInspect({"/alpha=a/beta=0"}, {DictStr("alpha"), DictInt("beta")});
  AssertInspect({"/alpha=a/beta=0", "/alpha=a/1"}, {DictStr("alpha"), DictInt("beta")});
  AssertInspect({"/alpha=a/beta=0", "/alpha=xyz/beta=xyz"},
                {DictStr("alpha"), DictInt("beta")});
  AssertInspect(
      {"/alpha=a/beta=0", "/alpha=b/beta=0", "/alpha=a/beta=1", "/alpha=b/beta=1"},
      {DictStr("alpha"), DictInt("beta")});
  AssertInspect(
      {"/alpha=a/beta=-", "/alpha=b/beta=-", "/alpha=a/beta=_", "/alpha=b/beta=_"},
      {DictStr("alpha"), DictStr("beta")});
}

TEST_F(TestPartitioning, HiveNullFallbackPassedOn) {
  HivePartitioningFactoryOptions options;
  options.null_fallback = "xyz";
  factory_ = HivePartitioning::MakeFactory(options);

  EXPECT_OK_AND_ASSIGN(auto schema, factory_->Inspect({"/alpha=a/beta=0"}));
  EXPECT_OK_AND_ASSIGN(auto partitioning, factory_->Finish(schema));
  ASSERT_EQ("xyz",
            std::static_pointer_cast<HivePartitioning>(partitioning)->null_fallback());
}

TEST_F(TestPartitioning, HiveDictionaryHasUniqueValues) {
  HivePartitioningFactoryOptions options;
  options.infer_dictionary = true;
  factory_ = HivePartitioning::MakeFactory(options);

  auto alpha = DictStr("alpha");
  AssertInspect({"/alpha=a", "/alpha=b", "/alpha=a", "/alpha=b", "/alpha=c", "/alpha=a"},
                {alpha});
  ASSERT_OK_AND_ASSIGN(auto partitioning, factory_->Finish(schema({alpha})));

  auto expected_dictionary =
      checked_pointer_cast<StringArray>(ArrayFromJSON(utf8(), R"(["a", "b", "c"])"));

  for (int32_t i = 0; i < expected_dictionary->length(); ++i) {
    DictionaryScalar::ValueType index_and_dictionary{std::make_shared<Int32Scalar>(i),
                                                     expected_dictionary};
    auto dictionary_scalar =
        std::make_shared<DictionaryScalar>(index_and_dictionary, alpha->type());

    auto path = "/alpha=" + expected_dictionary->GetString(i) + "/";
    AssertParse(path, equal(field_ref("alpha"), literal(dictionary_scalar)));
  }

  AssertParseError("/alpha=yosemite/");  // not in inspected dictionary
}

TEST_F(TestPartitioning, ExistingSchemaDirectory) {
  // Infer dictionary values but with a given schema
  auto dict_type = dictionary(int8(), utf8());
  PartitioningFactoryOptions options;
  options.schema = schema({field("alpha", int64()), field("beta", dict_type)});
  factory_ = DirectoryPartitioning::MakeFactory({"alpha", "beta"}, options);

  AssertInspect({"/0/1"}, options.schema->fields());
  AssertInspect({"/0/1/what"}, options.schema->fields());

  // fail if any segment is not parseable as schema type
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("Failed to parse string"),
                                  factory_->Inspect({"/0/1", "/hello/1"}));
  factory_ = DirectoryPartitioning::MakeFactory({"alpha", "beta"}, options);

  // Now we don't fail since our type is large enough
  AssertInspect({"/3760212050/1"}, options.schema->fields());
  // If there are still too many digits, fail
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("Failed to parse string"),
                                  factory_->Inspect({"/1038581385102940193760212050/1"}));
  factory_ = DirectoryPartitioning::MakeFactory({"alpha", "beta"}, options);

  AssertInspect({"/0/1", "/2"}, options.schema->fields());
}

TEST_F(TestPartitioning, ExistingSchemaHive) {
  // Infer dictionary values but with a given schema
  auto dict_type = dictionary(int8(), utf8());
  HivePartitioningFactoryOptions options;
  options.schema = schema({field("a", int64()), field("b", dict_type)});
  factory_ = HivePartitioning::MakeFactory(options);

  AssertInspect({"/a=0/b=1"}, options.schema->fields());
  AssertInspect({"/a=0/b=1/what"}, options.schema->fields());
  AssertInspect({"/a=0", "/b=1"}, options.schema->fields());

  // fail if any segment for field alpha is not parseable as schema type
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "Could not cast segments for partition field a to requested type int64"),
      factory_->Inspect({"/a=0/b=1", "/a=hello/b=1"}));
  factory_ = HivePartitioning::MakeFactory(options);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Requested schema has 2 fields, but only 1 were detected"),
      factory_->Inspect({"/a=0", "/a=hello"}));
  factory_ = HivePartitioning::MakeFactory(options);

  // Now we don't fail since our type is large enough
  AssertInspect({"/a=3760212050/b=1"}, options.schema->fields());
  // If there are still too many digits, fail
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Failed to parse string"),
      factory_->Inspect({"/a=1038581385102940193760212050/b=1"}));
  factory_ = HivePartitioning::MakeFactory(options);

  AssertInspect({"/a=0/b=1", "/b=2"}, options.schema->fields());
}

TEST_F(TestPartitioning, ExistingSchemaFilename) {
  // Infer dictionary values but with a given schema
  auto dict_type = dictionary(int8(), utf8());
  PartitioningFactoryOptions options;
  options.schema = schema({field("alpha", int64()), field("beta", dict_type)});
  factory_ = FilenamePartitioning::MakeFactory({"alpha", "beta"}, options);

  AssertInspect({"0_1_"}, options.schema->fields());
  AssertInspect({"0_1_what_"}, options.schema->fields());

  // fail if any segment is not parseable as schema type
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("Failed to parse string"),
                                  factory_->Inspect({"0_1_", "hello_1_"}));
  factory_ = FilenamePartitioning::MakeFactory({"alpha", "beta"}, options);

  // Now we don't fail since our type is large enough
  AssertInspect({"3760212050_1_"}, options.schema->fields());
  // If there are still too many digits, fail
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("Failed to parse string"),
                                  factory_->Inspect({"1038581385102940193760212050_1_"}));
  factory_ = FilenamePartitioning::MakeFactory({"alpha", "beta"}, options);

  AssertInspect({"0_1_", "2_"}, options.schema->fields());
}

TEST_F(TestPartitioning, UrlEncodedDirectory) {
  PartitioningFactoryOptions options;
  auto ts = timestamp(TimeUnit::type::SECOND);
  options.schema = schema({field("date", ts), field("time", ts), field("str", utf8())});
  factory_ = DirectoryPartitioning::MakeFactory(options.schema->field_names(), options);

  AssertInspect({"/2021-05-04 00:00:00/2021-05-04 07:27:00/%24",
                 "/2021-05-04 00%3A00%3A00/2021-05-04 07%3A27%3A00/foo"},
                options.schema->fields());
  auto date = std::make_shared<TimestampScalar>(1620086400, ts);
  auto time = std::make_shared<TimestampScalar>(1620113220, ts);
  partitioning_ = std::make_shared<DirectoryPartitioning>(options.schema, ArrayVector());
  AssertParse("/2021-05-04 00%3A00%3A00/2021-05-04 07%3A27%3A00/%24/",
              and_({equal(field_ref("date"), literal(date)),
                    equal(field_ref("time"), literal(time)),
                    equal(field_ref("str"), literal("$"))}));

  // Invalid UTF-8
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("was not valid UTF-8"),
                                  factory_->Inspect({"/%AF/%BF/%CF"}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("was not valid UTF-8"),
                                  partitioning_->Parse("/%AF/%BF/%CF"));

  options.segment_encoding = SegmentEncoding::None;
  options.schema =
      schema({field("date", utf8()), field("time", utf8()), field("str", utf8())});
  factory_ = DirectoryPartitioning::MakeFactory(options.schema->field_names(), options);
  AssertInspect({"/2021-05-04 00:00:00/2021-05-04 07:27:00/%E3%81%8F%E3%81%BE",
                 "/2021-05-04 00%3A00%3A00/2021-05-04 07%3A27%3A00/foo"},
                options.schema->fields());
  partitioning_ = std::make_shared<DirectoryPartitioning>(
      options.schema, ArrayVector(), options.AsPartitioningOptions());
  AssertParse("/2021-05-04 00%3A00%3A00/2021-05-04 07%3A27%3A00/%24/",
              and_({equal(field_ref("date"), literal("2021-05-04 00%3A00%3A00")),
                    equal(field_ref("time"), literal("2021-05-04 07%3A27%3A00")),
                    equal(field_ref("str"), literal("%24"))}));
}

TEST_F(TestPartitioning, UrlEncodedHive) {
  HivePartitioningFactoryOptions options;
  auto ts = timestamp(TimeUnit::type::SECOND);
  options.schema = schema({field("date", ts), field("time", ts), field("str", utf8())});
  options.null_fallback = "$";
  factory_ = HivePartitioning::MakeFactory(options);

  AssertInspect(
      {"/date=2021-05-04 00:00:00/time=2021-05-04 07:27:00/str=$",
       "/date=2021-05-04 00:00:00/time=2021-05-04 07:27:00/str=%E3%81%8F%E3%81%BE",
       "/date=2021-05-04 00%3A00%3A00/time=2021-05-04 07%3A27%3A00/str=%24"},
      options.schema->fields());

  auto date = std::make_shared<TimestampScalar>(1620086400, ts);
  auto time = std::make_shared<TimestampScalar>(1620113220, ts);
  partitioning_ = std::make_shared<HivePartitioning>(options.schema, ArrayVector(),
                                                     options.AsHivePartitioningOptions());
  AssertParse("/date=2021-05-04 00:00:00/time=2021-05-04 07:27:00/str=$/",
              and_({equal(field_ref("date"), literal(date)),
                    equal(field_ref("time"), literal(time)), is_null(field_ref("str"))}));
  AssertParse(
      "/date=2021-05-04 00:00:00/time=2021-05-04 07:27:00/str=%E3%81%8F%E3%81%BE/",
      and_({equal(field_ref("date"), literal(date)),
            equal(field_ref("time"), literal(time)),
            equal(field_ref("str"), literal("\xE3\x81\x8F\xE3\x81\xBE"))}));
  // URL-encoded null fallback value
  AssertParse("/date=2021-05-04 00%3A00%3A00/time=2021-05-04 07%3A27%3A00/str=%24/",
              and_({equal(field_ref("date"), literal(date)),
                    equal(field_ref("time"), literal(time)), is_null(field_ref("str"))}));

  // Invalid UTF-8
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("was not valid UTF-8"),
                                  factory_->Inspect({"/date=%AF/time=%BF/str=%CF"}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("was not valid UTF-8"),
                                  partitioning_->Parse("/date=%AF/time=%BF/str=%CF"));

  options.segment_encoding = SegmentEncoding::None;
  options.schema =
      schema({field("date", utf8()), field("time", utf8()), field("str", utf8())});
  factory_ = HivePartitioning::MakeFactory(options);
  AssertInspect(
      {"/date=2021-05-04 00:00:00/time=2021-05-04 07:27:00/str=$",
       "/date=2021-05-04 00:00:00/time=2021-05-04 07:27:00/str=%E3%81%8F%E3%81%BE",
       "/date=2021-05-04 00%3A00%3A00/time=2021-05-04 07%3A27%3A00/str=%24"},
      options.schema->fields());
  partitioning_ = std::make_shared<HivePartitioning>(options.schema, ArrayVector(),
                                                     options.AsHivePartitioningOptions());
  AssertParse("/date=2021-05-04 00%3A00%3A00/time=2021-05-04 07%3A27%3A00/str=%24/",
              and_({equal(field_ref("date"), literal("2021-05-04 00%3A00%3A00")),
                    equal(field_ref("time"), literal("2021-05-04 07%3A27%3A00")),
                    equal(field_ref("str"), literal("%24"))}));

  // Invalid UTF-8
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("was not valid UTF-8"),
                                  factory_->Inspect({"/date=\xAF/time=\xBF/str=\xCF"}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("was not valid UTF-8"),
                                  partitioning_->Parse("/date=\xAF/time=\xBF/str=\xCF"));
}

TEST_F(TestPartitioning, UrlEncodedHiveWithKeyEncoded) {
  HivePartitioningFactoryOptions options;
  auto ts = timestamp(TimeUnit::type::SECOND);
  options.schema =
      schema({field("test'; date", ts), field("test'; time", ts), field("str", utf8())});
  options.null_fallback = "$";
  factory_ = HivePartitioning::MakeFactory(options);

  AssertInspect({"/test%27%3B%20date=2021-05-04 00:00:00/test%27%3B%20time=2021-05-04 "
                 "07:27:00/str=$",
                 "/test%27%3B%20date=2021-05-04 00:00:00/test%27%3B%20time=2021-05-04 "
                 "07:27:00/str=%E3%81%8F%E3%81%BE",
                 "/test%27%3B%20date=2021-05-04 "
                 "00%3A00%3A00/test%27%3B%20time=2021-05-04 07%3A27%3A00/str=%24"},
                options.schema->fields());

  auto date = std::make_shared<TimestampScalar>(1620086400, ts);
  auto time = std::make_shared<TimestampScalar>(1620113220, ts);
  partitioning_ = std::make_shared<HivePartitioning>(options.schema, ArrayVector(),
                                                     options.AsHivePartitioningOptions());
  AssertParse(
      "/test%27%3B%20date=2021-05-04 00:00:00/test%27%3B%20time=2021-05-04 "
      "07:27:00/str=$/",
      and_({equal(field_ref("test'; date"), literal(date)),
            equal(field_ref("test'; time"), literal(time)), is_null(field_ref("str"))}));
  AssertParse(
      "/test%27%3B%20date=2021-05-04 00:00:00/test%27%3B%20time=2021-05-04 "
      "07:27:00/str=%E3%81%8F%E3%81%BE/",
      and_({equal(field_ref("test'; date"), literal(date)),
            equal(field_ref("test'; time"), literal(time)),
            equal(field_ref("str"), literal("\xE3\x81\x8F\xE3\x81\xBE"))}));
  // URL-encoded null fallback value
  AssertParse(
      "/test%27%3B%20date=2021-05-04 00%3A00%3A00/test%27%3B%20time=2021-05-04 "
      "07%3A27%3A00/str=%24/",
      and_({equal(field_ref("test'; date"), literal(date)),
            equal(field_ref("test'; time"), literal(time)), is_null(field_ref("str"))}));

  // Invalid UTF-8
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("was not valid UTF-8"),
      factory_->Inspect({"/%AF=2021-05-04/time=2021-05-04 07%3A27%3A00/str=%24"}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("was not valid UTF-8"),
      partitioning_->Parse("/%AF=2021-05-04/%BF=2021-05-04 07%3A27%3A00/str=%24/"));
}

TEST_F(TestPartitioning, WriteHiveWithSlashesInValues) {
  // ARROW-18269: partition values should be URI-encoded when writing a Hive-like dataset
  fs::TimePoint mock_now = std::chrono::system_clock::now();
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<fs::FileSystem> filesystem,
                       fs::internal::MockFileSystem::Make(mock_now, {}));
  auto base_path = "";
  ASSERT_OK(filesystem->CreateDir(base_path));
  // Create an Arrow Table
  auto schema = arrow::schema(
      {arrow::field("a", arrow::int64()), arrow::field("part", arrow::utf8())});

  auto table = TableFromJSON(schema, {
                                         R"([
    [0, "experiment/A/f.csv"],
    [1, "experiment/B/f.csv"],
    [2, "experiment/A/f.csv"],
    [3, "experiment/C/k.csv"],
    [4, "experiment/M/i.csv"]
  ])",
                                     });

  // Write it using Datasets
  auto dataset = std::make_shared<dataset::InMemoryDataset>(table);
  ASSERT_OK_AND_ASSIGN(auto scanner_builder, dataset->NewScan());
  ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());

  auto partition_schema = arrow::schema({arrow::field("part", arrow::utf8())});
  auto partitioning = std::make_shared<dataset::HivePartitioning>(partition_schema);
  auto ipc_format = std::make_shared<dataset::IpcFileFormat>();
  dataset::FileSystemDatasetWriteOptions write_options;
  write_options.file_write_options = ipc_format->DefaultWriteOptions();
  write_options.filesystem = filesystem;
  write_options.base_dir = base_path;
  write_options.partitioning = partitioning;
  write_options.basename_template = "part{i}.arrow";
  ASSERT_OK(dataset::FileSystemDataset::Write(write_options, scanner));

  auto mockfs =
      arrow::internal::checked_pointer_cast<fs::internal::MockFileSystem>(filesystem);
  auto all_dirs = mockfs->AllDirs();

  std::vector<std::string> encoded_paths;
  std::vector<std::string> unique_partitions = {
      "experiment/A/f.csv", "experiment/B/f.csv", "experiment/C/k.csv",
      "experiment/M/i.csv"};
  for (auto partition : unique_partitions) {
    encoded_paths.push_back("part=" + arrow::internal::UriEscape(partition));
  }

  ASSERT_EQ(all_dirs.size(), encoded_paths.size());

  for (size_t i = 0; i < all_dirs.size(); i++) {
    ASSERT_EQ(all_dirs[i].full_path, encoded_paths[i]);
  }
}

TEST_F(TestPartitioning, EtlThenHive) {
  FieldVector etl_fields{field("year", int16()), field("month", int8()),
                         field("day", int8()), field("hour", int8())};
  DirectoryPartitioning etl_part(schema(etl_fields));

  FieldVector alphabeta_fields{field("alpha", int32()), field("beta", float32())};
  HivePartitioning alphabeta_part(schema(alphabeta_fields));

  auto schm =
      schema({field("year", int16()), field("month", int8()), field("day", int8()),
              field("hour", int8()), field("alpha", int32()), field("beta", float32())});

  partitioning_ = std::make_shared<FunctionPartitioning>(
      schm, [&](const std::string& path) -> Result<compute::Expression> {
        auto segments = fs::internal::SplitAbstractPath(path);
        if (segments.size() < etl_fields.size() + alphabeta_fields.size()) {
          return Status::Invalid("path ", path, " can't be parsed");
        }

        auto etl_segments_end = segments.begin() + etl_fields.size();
        auto etl_path =
            fs::internal::JoinAbstractPath(segments.begin(), etl_segments_end);
        ARROW_ASSIGN_OR_RAISE(auto etl_expr, etl_part.Parse(etl_path + "/"));

        auto alphabeta_segments_end = etl_segments_end + alphabeta_fields.size();
        auto alphabeta_path =
            fs::internal::JoinAbstractPath(etl_segments_end, alphabeta_segments_end);
        ARROW_ASSIGN_OR_RAISE(auto alphabeta_expr,
                              alphabeta_part.Parse(alphabeta_path + "/"));
        return and_(etl_expr, alphabeta_expr);
      });

  AssertParse("/1999/12/31/00/alpha=0/beta=3.25/",
              and_({equal(field_ref("year"), literal<int16_t>(1999)),
                    equal(field_ref("month"), literal<int8_t>(12)),
                    equal(field_ref("day"), literal<int8_t>(31)),
                    equal(field_ref("hour"), literal<int8_t>(0)),
                    and_(equal(field_ref("alpha"), literal<int32_t>(0)),
                         equal(field_ref("beta"), literal<float>(3.25f)))}));

  AssertParseError("/20X6/03/21/05/alpha=0/beta=3.25/");
}

TEST_F(TestPartitioning, Set) {
  auto ints = [](std::vector<int32_t> ints) {
    std::shared_ptr<Array> out;
    ArrayFromVector<Int32Type>(ints, &out);
    return out;
  };

  auto schm = schema({field("x", int32())});

  // An adhoc partitioning which parses segments like "/x in [1 4 5]"
  // into (field_ref("x") == 1 or field_ref("x") == 4 or field_ref("x") == 5)
  partitioning_ = std::make_shared<FunctionPartitioning>(
      schm, [&](const std::string& path) -> Result<compute::Expression> {
        std::vector<compute::Expression> subexpressions;
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

          subexpressions.push_back(call("is_in", {field_ref(std::string(matches[1]))},
                                        compute::SetLookupOptions{ints(set)}));
        }
        return and_(std::move(subexpressions));
      });

  auto x_in = [&](std::vector<int32_t> set) {
    return call("is_in", {field_ref("x")}, compute::SetLookupOptions{ints(set)});
  };
  AssertParse("/x in [1]", x_in({1}));
  AssertParse("/x in [1 4 5]", x_in({1, 4, 5}));
  AssertParse("/x in []", x_in({}));
}

// An adhoc partitioning which parses segments like "/x=[-3.25, 0.0)"
// into (field_ref("x") >= -3.25 and "x" < 0.0)
class RangePartitioning : public Partitioning {
 public:
  explicit RangePartitioning(std::shared_ptr<Schema> s) : Partitioning(std::move(s)) {}

  std::string type_name() const override { return "range"; }

  bool Equals(const Partitioning& other) const override {
    if (this == &other) {
      return true;
    }
    return checked_cast<const RangePartitioning&>(other).type_name() == type_name() &&
           Partitioning::Equals(other);
  }

  Result<compute::Expression> Parse(const std::string& path) const override {
    std::vector<compute::Expression> ranges;

    HivePartitioningOptions options;
    for (auto segment : fs::internal::SplitAbstractPath(path)) {
      ARROW_ASSIGN_OR_RAISE(auto key, HivePartitioning::ParseKey(segment, options));
      if (!key) {
        return Status::Invalid("can't parse '", segment, "' as a range");
      }

      std::smatch matches;
      RETURN_NOT_OK(DoRegex(*key->value, &matches));

      auto& min_cmp = matches[1] == "[" ? greater_equal : greater;
      std::string min_repr = matches[2];
      std::string max_repr = matches[3];
      auto& max_cmp = matches[4] == "]" ? less_equal : less;

      const auto& type = schema_->GetFieldByName(key->name)->type();
      ARROW_ASSIGN_OR_RAISE(auto min, Scalar::Parse(type, min_repr));
      ARROW_ASSIGN_OR_RAISE(auto max, Scalar::Parse(type, max_repr));

      ranges.push_back(and_(min_cmp(field_ref(key->name), literal(min)),
                            max_cmp(field_ref(key->name), literal(max))));
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

  Result<PartitionPathFormat> Format(const compute::Expression&) const override {
    return PartitionPathFormat{"", ""};
  }
  Result<PartitionedBatches> Partition(
      const std::shared_ptr<RecordBatch>&) const override {
    return Status::OK();
  }
};

TEST_F(TestPartitioning, Range) {
  partitioning_ = std::make_shared<RangePartitioning>(
      schema({field("x", float64()), field("y", float64()), field("z", float64())}));

  AssertParse("/x=[-1.5 0.0)/y=[0.0 1.5)/z=(1.5 3.0]",
              and_({and_(greater_equal(field_ref("x"), literal(-1.5)),
                         less(field_ref("x"), literal(0.0))),
                    and_(greater_equal(field_ref("y"), literal(0.0)),
                         less(field_ref("y"), literal(1.5))),
                    and_(greater(field_ref("z"), literal(1.5)),
                         less_equal(field_ref("z"), literal(3.0)))}));
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
  auto paths = StripPrefixAndFilename(input, "/data");
  EXPECT_THAT(paths, testing::ElementsAre("year=2019", "year=2019/month=12",
                                          "year=2019/month=12/day=01"));
}

}  // namespace dataset
}  // namespace arrow
