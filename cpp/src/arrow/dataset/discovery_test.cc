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

#include <utility>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/dataset/partition.h"
#include "arrow/dataset/test_util.h"
#include "arrow/filesystem/test_util.h"

namespace arrow {
namespace dataset {

class DataSourceDiscoveryTest : public TestFileSystemDataSource {
 public:
  void AssertInspect(std::shared_ptr<Schema> schema) {
    ASSERT_OK_AND_ASSIGN(auto actual, discovery_->Inspect());
    EXPECT_EQ(*actual, *schema);
  }

  void AssertInspect(const std::vector<std::shared_ptr<Field>>& expected_fields) {
    AssertInspect(schema(expected_fields));
  }

  void AssertInspectSchemas(std::vector<std::shared_ptr<Schema>> expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, discovery_->InspectSchemas());

    EXPECT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); i++) {
      EXPECT_EQ(*actual[i], *expected[i]);
    }
  }

 protected:
  std::shared_ptr<DataSourceDiscovery> discovery_;
};

class MockDataSourceDiscovery : public DataSourceDiscovery {
 public:
  explicit MockDataSourceDiscovery(std::vector<std::shared_ptr<Schema>> schemas)
      : schemas_(std::move(schemas)) {}

  Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas() override {
    return schemas_;
  }

  Result<std::shared_ptr<DataSource>> Finish() override {
    return std::make_shared<SimpleDataSource>(
        std::vector<std::shared_ptr<DataFragment>>{});
  }

 protected:
  std::vector<std::shared_ptr<Schema>> schemas_;
};

class MockPartitionScheme : public PartitionScheme {
 public:
  explicit MockPartitionScheme(std::shared_ptr<Schema> schema)
      : PartitionScheme(std::move(schema)) {}

  Result<std::shared_ptr<Expression>> Parse(const std::string& segment,
                                            int i) const override {
    return nullptr;
  }

  std::string type_name() const override { return "mock_partition_scheme"; }
};

class MockDataSourceDiscoveryTest : public DataSourceDiscoveryTest {
 public:
  void MakeDiscovery(std::vector<std::shared_ptr<Schema>> schemas) {
    discovery_ = std::make_shared<MockDataSourceDiscovery>(schemas);
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

TEST_F(MockDataSourceDiscoveryTest, UnifySchemas) {
  MakeDiscovery({});
  AssertInspect(schema({}));

  MakeDiscovery({schema({i32}), schema({i32})});
  AssertInspect(schema({i32}));

  MakeDiscovery({schema({i32}), schema({i64})});
  AssertInspect(schema({i32, i64}));

  MakeDiscovery({schema({i32}), schema({i64})});
  AssertInspect(schema({i32, i64}));

  MakeDiscovery({schema({i32}), schema({i32_req})});
  AssertInspect(schema({i32}));

  MakeDiscovery({schema({i32, f64}), schema({i32_req, i64})});
  AssertInspect(schema({i32, f64, i64}));

  MakeDiscovery({schema({i32, f64}), schema({f64, i32_fake})});
  // Unification fails when fields with the same name have clashing types.
  ASSERT_RAISES(Invalid, discovery_->Inspect());
  // Return the individual schema for closer inspection should not fail.
  AssertInspectSchemas({schema({i32, f64}), schema({f64, i32_fake})});

  // Partition Scheme's schema should be taken into account
  MakeDiscovery({schema({i64, f64})});
  auto partition_scheme = std::make_shared<MockPartitionScheme>(schema({i32}));
  ASSERT_OK(discovery_->SetPartitionScheme(partition_scheme));
  AssertInspect(schema({i64, f64, i32}));

  // Partition scheme with an existing column should be fine.
  partition_scheme = std::make_shared<MockPartitionScheme>(schema({i64}));
  ASSERT_OK(discovery_->SetPartitionScheme(partition_scheme));
  AssertInspect(schema({i64, f64}));
}

class FileSystemDataSourceDiscoveryTest : public DataSourceDiscoveryTest {
 public:
  void MakeDiscovery(const std::vector<fs::FileStats>& files) {
    MakeFileSystem(files);
    ASSERT_OK_AND_ASSIGN(discovery_, FileSystemDataSourceDiscovery::Make(
                                         fs_, selector_, format_, discovery_options_));
  }

  void AssertFinishWithPaths(std::vector<std::string> paths) {
    options_ = ScanOptions::Make(discovery_->schema());
    ASSERT_OK_AND_ASSIGN(source_, discovery_->Finish());
    AssertFragmentsAreFromPath(source_->GetFragments(options_), paths);
  }

 protected:
  fs::FileSelector selector_;
  FileSystemDiscoveryOptions discovery_options_;
  std::shared_ptr<FileFormat> format_ = std::make_shared<DummyFileFormat>(schema({}));
};

TEST_F(FileSystemDataSourceDiscoveryTest, Basic) {
  MakeDiscovery({fs::File("a"), fs::File("b")});
  AssertFinishWithPaths({"a", "b"});
  MakeDiscovery({fs::Dir("a"), fs::Dir("a/b"), fs::File("a/b/c")});
}

TEST_F(FileSystemDataSourceDiscoveryTest, Selector) {
  selector_.base_dir = "A";
  selector_.recursive = true;

  MakeDiscovery({fs::File("0"), fs::File("A/a"), fs::File("A/A/a")});
  // "0" doesn't match selector, so it has been dropped:
  AssertFinishWithPaths({"A/a", "A/A/a"});

  discovery_options_.partition_base_dir = "A/A";
  MakeDiscovery({fs::File("0"), fs::File("A/a"), fs::File("A/A/a")});
  // partition_base_dir should not affect filtered files, ony the applied
  // partition scheme.
  AssertFinishWithPaths({"A/a", "A/A/a"});
}

TEST_F(FileSystemDataSourceDiscoveryTest, Partition) {
  selector_.base_dir = "a=ignored/base";
  MakeDiscovery(
      {fs::File(selector_.base_dir + "/a=1"), fs::File(selector_.base_dir + "/a=2")});

  auto partition_scheme =
      std::make_shared<HivePartitionScheme>(schema({field("a", int32())}));
  ASSERT_OK(discovery_->SetPartitionScheme(partition_scheme));
  AssertInspect({field("a", int32())});
  AssertFinishWithPaths({selector_.base_dir + "/a=1", selector_.base_dir + "/a=2"});
}

TEST_F(FileSystemDataSourceDiscoveryTest, MissingDirectories) {
  MakeFileSystem({fs::File("base_dir/a=3/b=3/dat"), fs::File("unpartitioned/ignored=3")});

  discovery_options_.partition_base_dir = "base_dir";
  ASSERT_OK_AND_ASSIGN(
      discovery_, FileSystemDataSourceDiscovery::Make(
                      fs_, {"base_dir/a=3/b=3/dat", "unpartitioned/ignored=3"}, format_,
                      discovery_options_));
  auto partition_scheme = std::make_shared<HivePartitionScheme>(
      schema({field("a", int32()), field("b", int32())}));
  ASSERT_OK(discovery_->SetPartitionScheme(partition_scheme));

  AssertInspect({field("a", int32()), field("b", int32())});
  AssertFinishWithPaths({"base_dir/a=3/b=3/dat", "unpartitioned/ignored=3"});
}

TEST_F(FileSystemDataSourceDiscoveryTest, OptionsIgnoredDefaultPrefixes) {
  MakeDiscovery({
      fs::File("."),
      fs::File("_"),
      fs::File("_$folder$"),
      fs::File("_SUCCESS"),
      fs::File("not_ignored_by_default"),
  });

  AssertFinishWithPaths({"not_ignored_by_default"});
}

TEST_F(FileSystemDataSourceDiscoveryTest, OptionsIgnoredCustomPrefixes) {
  discovery_options_.ignore_prefixes = {"not_ignored"};
  MakeDiscovery({
      fs::File("."),
      fs::File("_"),
      fs::File("_$folder$"),
      fs::File("_SUCCESS"),
      fs::File("not_ignored_by_default"),
  });

  AssertFinishWithPaths({".", "_", "_$folder$", "_SUCCESS"});
}

TEST_F(FileSystemDataSourceDiscoveryTest, Inspect) {
  auto s = schema({field("f64", float64())});
  format_ = std::make_shared<DummyFileFormat>(s);

  MakeDiscovery({});

  // No files
  ASSERT_OK_AND_ASSIGN(auto actual, discovery_->Inspect());
  EXPECT_EQ(*actual, Schema({}));

  MakeDiscovery({fs::File("test")});
  ASSERT_OK_AND_ASSIGN(actual, discovery_->Inspect());
  EXPECT_EQ(*actual, *s);
}

}  // namespace dataset
}  // namespace arrow
