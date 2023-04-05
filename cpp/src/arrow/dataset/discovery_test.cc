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

#include "arrow/dataset/discovery.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <utility>

#include "arrow/dataset/partition.h"
#include "arrow/dataset/test_util_internal.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"

using testing::SizeIs;

namespace arrow {
namespace dataset {

void AssertSchemasAre(std::vector<std::shared_ptr<Schema>> actual,
                      std::vector<std::shared_ptr<Schema>> expected) {
  EXPECT_EQ(actual.size(), expected.size());
  for (size_t i = 0; i < actual.size(); i++) {
    EXPECT_EQ(*actual[i], *expected[i]);
  }
}

class DatasetFactoryTest : public TestFileSystemDataset {
 public:
  void AssertInspect(std::shared_ptr<Schema> expected, InspectOptions options = {}) {
    ASSERT_OK_AND_ASSIGN(auto actual, factory_->Inspect(options));
    EXPECT_EQ(*actual, *expected);
  }

  void AssertInspectSchemas(std::vector<std::shared_ptr<Schema>> expected,
                            InspectOptions options = {}) {
    ASSERT_OK_AND_ASSIGN(auto actual, factory_->InspectSchemas(options));
    AssertSchemasAre(actual, expected);
  }

 protected:
  std::shared_ptr<DatasetFactory> factory_;
};

class MockDatasetFactory : public DatasetFactory {
 public:
  explicit MockDatasetFactory(std::vector<std::shared_ptr<Schema>> schemas)
      : schemas_(std::move(schemas)) {}

  Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas(
      InspectOptions options) override {
    return schemas_;
  }

  Result<std::shared_ptr<Dataset>> Finish(FinishOptions options) override {
    return std::make_shared<InMemoryDataset>(options.schema,
                                             std::vector<std::shared_ptr<RecordBatch>>{});
  }

 protected:
  std::vector<std::shared_ptr<Schema>> schemas_;
};

class MockDatasetFactoryTest : public DatasetFactoryTest {
 public:
  void MakeFactory(std::vector<std::shared_ptr<Schema>> schemas) {
    factory_ = std::make_shared<MockDatasetFactory>(schemas);
  }

 protected:
  std::shared_ptr<Field> i32 = field("i32", int32());
  std::shared_ptr<Field> i64 = field("i64", int64());
  std::shared_ptr<Field> f32 = field("f32", float64());
  std::shared_ptr<Field> f64 = field("f64", float64());
  // Non-nullable
  std::shared_ptr<Field> i32_req = field("i32", int32(), false);
  // bad type with name `i32`
  std::shared_ptr<Field> i32_fake = field("i32", boolean());
};

TEST_F(MockDatasetFactoryTest, UnifySchemas) {
  MakeFactory({});
  AssertInspect(schema({}));

  MakeFactory({schema({i32}), schema({i32})});
  AssertInspect(schema({i32}));

  MakeFactory({schema({i32}), schema({i64})});
  AssertInspect(schema({i32, i64}));

  MakeFactory({schema({i32}), schema({i64})});
  AssertInspect(schema({i32, i64}));

  MakeFactory({schema({i32}), schema({i32_req})});
  AssertInspect(schema({i32}));

  MakeFactory({schema({i32, f64}), schema({i32_req, i64})});
  AssertInspect(schema({i32, f64, i64}));

  MakeFactory({schema({i32, f64}), schema({f64, i32_fake})});
  // Unification fails when fields with the same name have clashing types.
  ASSERT_RAISES(Invalid, factory_->Inspect());
  // Return the individual schema for closer inspection should not fail.
  AssertInspectSchemas({schema({i32, f64}), schema({f64, i32_fake})});
}

class FileSystemDatasetFactoryTest : public DatasetFactoryTest {
 public:
  void MakeFactory(const std::vector<fs::FileInfo>& files) {
    MakeFileSystem(files);
    ASSERT_OK_AND_ASSIGN(factory_, FileSystemDatasetFactory::Make(fs_, selector_, format_,
                                                                  factory_options_));
  }

  void AssertFinishWithPaths(std::vector<std::string> paths,
                             std::shared_ptr<Schema> schema = nullptr,
                             InspectOptions options = {}) {
    if (schema == nullptr) {
      ASSERT_OK_AND_ASSIGN(schema, factory_->Inspect(options));
    }
    options_ = std::make_shared<ScanOptions>();
    options_->dataset_schema = schema;
    ASSERT_OK_AND_ASSIGN(auto projection, ProjectionDescr::Default(*schema));
    SetProjection(options_.get(), std::move(projection));
    ASSERT_OK_AND_ASSIGN(dataset_, factory_->Finish(schema));
    ASSERT_OK_AND_ASSIGN(auto fragment_it, dataset_->GetFragments());
    AssertFragmentsAreFromPath(std::move(fragment_it), paths);
  }

 protected:
  fs::FileSelector selector_;
  FileSystemFactoryOptions factory_options_;
  std::shared_ptr<FileFormat> format_ = std::make_shared<DummyFileFormat>(schema({}));
};

TEST_F(FileSystemDatasetFactoryTest, Basic) {
  MakeFactory({fs::File("a"), fs::File("b")});
  AssertFinishWithPaths({"a", "b"});
  MakeFactory({fs::Dir("a"), fs::Dir("a/b"), fs::File("a/b/c")});
}

TEST_F(FileSystemDatasetFactoryTest, Selector) {
  selector_.base_dir = "A";
  selector_.recursive = true;

  MakeFactory({fs::File("0"), fs::File("A/a"), fs::File("A/A/a")});
  // "0" doesn't match selector, so it has been dropped:
  AssertFinishWithPaths({"A/a", "A/A/a"});

  factory_options_.partition_base_dir = "A/A";
  MakeFactory({fs::File("0"), fs::File("A/a"), fs::File("A/A/a")});
  // partition_base_dir should not affect filtered files, only the applied partition
  AssertInspect(schema({}));
  AssertFinishWithPaths({"A/a", "A/A/a"});
}

TEST_F(FileSystemDatasetFactoryTest, ExplicitPartition) {
  selector_.base_dir = "a=ignored/base";
  auto part_field = field("a", int32());
  factory_options_.partitioning =
      std::make_shared<HivePartitioning>(schema({part_field}));

  auto a_1 = "a=ignored/base/a=1";
  MakeFactory({fs::File(a_1)});

  InspectOptions options;
  // Should inspect the partition's Schema even if no files are inspected.
  options.fragments = 0;
  AssertInspect(schema({part_field}), options);
  AssertFinishWithPaths({a_1}, nullptr, options);
}

TEST_F(FileSystemDatasetFactoryTest, DiscoveredPartition) {
  selector_.base_dir = "a=ignored/base";
  selector_.recursive = true;
  factory_options_.partitioning = HivePartitioning::MakeFactory();

  auto a_1 = "a=ignored/base/a=1/file.data";
  MakeFactory({fs::File(a_1)});

  InspectOptions options;

  auto schema_with = schema({field("a", int32())});
  AssertInspect(schema_with, options);
  AssertFinishWithPaths({a_1}, schema_with);
}

TEST_F(FileSystemDatasetFactoryTest, MissingDirectories) {
  auto partition_path = "base_dir/a=3/b=3/dat";
  auto unpartition_path = "unpartitioned/ignored=3";
  MakeFileSystem({fs::File(partition_path), fs::File(unpartition_path)});

  factory_options_.partition_base_dir = "base_dir";
  factory_options_.partitioning = std::make_shared<HivePartitioning>(
      schema({field("a", int32()), field("b", int32())}));

  auto paths = std::vector<std::string>{partition_path, unpartition_path};

  ASSERT_OK_AND_ASSIGN(
      factory_, FileSystemDatasetFactory::Make(fs_, paths, format_, factory_options_));

  InspectOptions options;
  AssertInspect(schema({field("a", int32()), field("b", int32())}), options);
  AssertFinishWithPaths({partition_path, unpartition_path});
}

TEST_F(FileSystemDatasetFactoryTest, OptionsIgnoredDefaultPrefixes) {
  // When constructing a factory from a FileSelector,
  // `selector_ignore_prefixes` governs which files are filtered out.
  selector_.recursive = true;
  MakeFactory({
      fs::File("."),
      fs::File("_"),
      fs::File("_$folder$/dat"),
      fs::File("_SUCCESS"),
      fs::File("not_ignored_by_default"),
      fs::File("not_ignored_by_default_either/dat"),
  });

  AssertFinishWithPaths({"not_ignored_by_default", "not_ignored_by_default_either/dat"});
}

TEST_F(FileSystemDatasetFactoryTest, OptionsIgnoredDefaultExplicitFiles) {
  // When constructing a factory from an explicit list of paths,
  // `selector_ignore_prefixes` is ignored.
  selector_.recursive = true;
  std::vector<fs::FileInfo> ignored_by_default = {
      fs::File(".ignored_by_default.parquet"),
      fs::File("_ignored_by_default.csv"),
      fs::File("_$folder$/ignored_by_default.arrow"),
  };
  MakeFileSystem(ignored_by_default);

  std::vector<std::string> paths;
  for (const auto& info : ignored_by_default) paths.push_back(info.path());
  ASSERT_OK_AND_ASSIGN(
      factory_, FileSystemDatasetFactory::Make(fs_, paths, format_, factory_options_));

  AssertFinishWithPaths(paths);
}

TEST_F(FileSystemDatasetFactoryTest, OptionsIgnoredCustomPrefixes) {
  selector_.recursive = true;
  factory_options_.selector_ignore_prefixes = {"not_ignored"};
  MakeFactory({
      fs::File("."),
      fs::File("_"),
      fs::File("_$folder$/dat"),
      fs::File("_SUCCESS"),
      fs::File("not_ignored_by_default"),
      fs::File("not_ignored_by_default_either/dat"),
  });

  AssertFinishWithPaths({".", "_", "_$folder$/dat", "_SUCCESS"});
}

TEST_F(FileSystemDatasetFactoryTest, OptionsIgnoredNoPrefixes) {
  // Ignore nothing
  selector_.recursive = true;
  factory_options_.selector_ignore_prefixes = {};
  MakeFactory({
      fs::File("."),
      fs::File("_"),
      fs::File("_$folder$/dat"),
      fs::File("_SUCCESS"),
      fs::File("not_ignored_by_default"),
      fs::File("not_ignored_by_default_either/dat"),
  });

  AssertFinishWithPaths({".", "_", "_$folder$/dat", "_SUCCESS", "not_ignored_by_default",
                         "not_ignored_by_default_either/dat"});
}

TEST_F(FileSystemDatasetFactoryTest, OptionsIgnoredPrefixesWithBaseDirectory) {
  //  ARROW-9644: the selector base_dir shouldn't be filtered out even if matches
  // `selector_ignore_prefixes`.
  std::string dir = "_shouldnt_be_ignored/.dataset/";
  selector_.base_dir = dir;
  selector_.recursive = true;
  MakeFactory({
      fs::File(dir + "."),
      fs::File(dir + "_"),
      fs::File(dir + "_$folder$/dat"),
      fs::File(dir + "_SUCCESS"),
      fs::File(dir + "not_ignored_by_default"),
      fs::File(dir + "not_ignored_by_default_either/dat"),
  });

  AssertFinishWithPaths(
      {dir + "not_ignored_by_default", dir + "not_ignored_by_default_either/dat"});
}

TEST_F(FileSystemDatasetFactoryTest, Inspect) {
  auto s = schema({field("f64", float64())});
  format_ = std::make_shared<DummyFileFormat>(s);

  // No files
  MakeFactory({});
  AssertInspect(schema({}));

  MakeFactory({fs::File("test")});
  AssertInspect(s);
}

TEST_F(FileSystemDatasetFactoryTest, FinishWithIncompatibleSchemaShouldFail) {
  auto s = schema({field("f64", float64())});
  format_ = std::make_shared<DummyFileFormat>(s);

  auto broken_s = schema({field("f64", utf8())});

  FinishOptions options;
  options.schema = broken_s;
  options.validate_fragments = true;

  // No files and validation
  MakeFactory({});
  ASSERT_OK_AND_ASSIGN(auto dataset, factory_->Finish(options));

  MakeFactory({fs::File("test")});
  ASSERT_RAISES(Invalid, factory_->Finish(options));

  // Disable validation
  options.validate_fragments = false;
  ASSERT_OK_AND_ASSIGN(dataset, factory_->Finish(options));
}

TEST_F(FileSystemDatasetFactoryTest, InspectFragmentsLimit) {
  MakeFactory({fs::File("a"), fs::File("b"), fs::File("c")});

  InspectOptions options;
  // By default, inspect one fragment and the partitioning.
  ASSERT_OK_AND_ASSIGN(auto schemas, factory_->InspectSchemas(options));
  EXPECT_THAT(schemas, SizeIs(2));

  for (int fragments = 0; fragments < 3; fragments++) {
    options.fragments = fragments;
    ASSERT_OK_AND_ASSIGN(auto schemas, factory_->InspectSchemas(options));
    EXPECT_THAT(schemas, SizeIs(fragments + 1));
  }
}

TEST_F(FileSystemDatasetFactoryTest, FilenameNotPartOfPartitions) {
  // ARROW-8726: Ensure filename is not a partition.

  // Creates a partition with 2 explicit fields. The type `int32` is
  // specifically chosen such that parsing would fail given a non-integer
  // string.
  auto s = schema({field("first", utf8()), field("second", int32())});
  factory_options_.partitioning = std::make_shared<DirectoryPartitioning>(s);

  selector_.recursive = true;
  // The file doesn't have a directory component for the second partition
  // column. In such case, the filename should not be used.
  MakeFactory({fs::File("one/file.parquet")});

  auto expected = equal(field_ref("first"), literal("one"));

  ASSERT_OK_AND_ASSIGN(auto dataset, factory_->Finish());
  ASSERT_OK_AND_ASSIGN(auto fragment_it, dataset->GetFragments());
  for (const auto& maybe_fragment : fragment_it) {
    ASSERT_OK_AND_ASSIGN(auto fragment, maybe_fragment);
    EXPECT_EQ(fragment->partition_expression(), expected);
  }
}

TEST_F(FileSystemDatasetFactoryTest, UnparseablePartitionExpression) {
  auto s = schema({field("first", int32()), field("second", int32())});
  factory_options_.partitioning = std::make_shared<HivePartitioning>(s);
  selector_.recursive = true;

  for (auto pathlist : {"first=one/file.parquet", "second=one/file.parquet",
                        R"(first=1/second=0/file.parquet
                           first=1/second=zero/file.parquet)"}) {
    MakeFactory(ParsePathList(pathlist));
    ASSERT_RAISES(Invalid, factory_->Finish().status());
  }

  for (auto pathlist : {
           R"(first=1/file.parquet
              second=0/file.parquet)",
           R"(first=1/second=2/file.parquet
              second=0/file.parquet)",
           R"(first=1/file.parquet
              second=0/first=1/file.parquet)",
       }) {
    MakeFactory(ParsePathList(pathlist));
    ASSERT_OK(factory_->Finish().status());
  }
}

std::shared_ptr<DatasetFactory> DatasetFactoryFromSchemas(
    std::vector<std::shared_ptr<Schema>> schemas) {
  return std::make_shared<MockDatasetFactory>(schemas);
}

TEST(UnionDatasetFactoryTest, Basic) {
  auto f64 = field("f64", float64());
  auto i32 = field("i32", int32());
  auto i32_req = field("i32", int32(), /*nullable*/ false);
  auto str = field("str", utf8());

  auto schema_1 = schema({f64, i32_req});
  auto schema_2 = schema({f64, i32});
  auto schema_3 = schema({str, i32});

  auto dataset_1 = DatasetFactoryFromSchemas({schema_1, schema_2});
  auto dataset_2 = DatasetFactoryFromSchemas({schema_2});
  auto dataset_3 = DatasetFactoryFromSchemas({schema_3});

  ASSERT_OK_AND_ASSIGN(auto factory,
                       UnionDatasetFactory::Make({dataset_1, dataset_2, dataset_3}));

  ASSERT_OK_AND_ASSIGN(auto schemas, factory->InspectSchemas({}));
  AssertSchemasAre(schemas, {schema_2, schema_2, schema_3});

  auto expected_schema = schema({f64, i32, str});
  ASSERT_OK_AND_ASSIGN(auto inspected, factory->Inspect());
  EXPECT_EQ(*inspected, *expected_schema);

  ASSERT_OK_AND_ASSIGN(auto dataset, factory->Finish());
  EXPECT_EQ(*dataset->schema(), *expected_schema);

  auto f64_schema = schema({f64});
  ASSERT_OK_AND_ASSIGN(dataset, factory->Finish(f64_schema));
  EXPECT_EQ(*dataset->schema(), *f64_schema);
}

TEST(UnionDatasetFactoryTest, ConflictingSchemas) {
  auto f64 = field("f64", float64());
  auto i32 = field("i32", int32());
  auto i32_req = field("i32", int32(), /*nullable*/ false);
  auto bad_f64 = field("f64", float32());

  auto schema_1 = schema({f64, i32_req});
  auto schema_2 = schema({f64, i32});
  // Incompatible with schema_1
  auto schema_3 = schema({bad_f64, i32});

  auto dataset_factory_1 = DatasetFactoryFromSchemas({schema_1, schema_2});
  auto dataset_factory_2 = DatasetFactoryFromSchemas({schema_2});
  auto dataset_factory_3 = DatasetFactoryFromSchemas({schema_3});

  ASSERT_OK_AND_ASSIGN(auto factory,
                       UnionDatasetFactory::Make(
                           {dataset_factory_1, dataset_factory_2, dataset_factory_3}));

  // schema_3 conflicts with other, Inspect/Finish should not work
  ASSERT_RAISES(Invalid, factory->Inspect());
  ASSERT_RAISES(Invalid, factory->Finish());

  // The user can inspect without error
  ASSERT_OK_AND_ASSIGN(auto schemas, factory->InspectSchemas({}));
  AssertSchemasAre(schemas, {schema_2, schema_2, schema_3});

  // The user decided to ignore the conflicting `f64` field.
  auto i32_schema = schema({i32});
  ASSERT_OK_AND_ASSIGN(auto dataset, factory->Finish(i32_schema));
  EXPECT_EQ(*dataset->schema(), *i32_schema);
}

}  // namespace dataset
}  // namespace arrow
