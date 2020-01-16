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

class SourceFactoryTest : public TestFileSystemSource {
 public:
  void AssertInspect(const std::vector<std::shared_ptr<Field>>& expected_fields) {
    ASSERT_OK_AND_ASSIGN(auto actual, factory_->Inspect());
    EXPECT_EQ(*actual, Schema(expected_fields));
  }

  void AssertInspectSchemas(std::vector<std::shared_ptr<Schema>> expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, factory_->InspectSchemas());

    EXPECT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); i++) {
      EXPECT_EQ(*actual[i], *expected[i]);
    }
  }

 protected:
  std::shared_ptr<SourceFactory> factory_;
};

class MockSourceFactory : public SourceFactory {
 public:
  explicit MockSourceFactory(std::vector<std::shared_ptr<Schema>> schemas)
      : schemas_(std::move(schemas)) {}

  Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas() override {
    return schemas_;
  }

  Result<std::shared_ptr<Source>> Finish(const std::shared_ptr<Schema>& schema) override {
    return std::make_shared<InMemorySource>(schema,
                                            std::vector<std::shared_ptr<Fragment>>{});
  }

 protected:
  std::vector<std::shared_ptr<Schema>> schemas_;
};

class MockPartitioning : public Partitioning {
 public:
  explicit MockPartitioning(std::shared_ptr<Schema> schema)
      : Partitioning(std::move(schema)) {}

  Result<std::shared_ptr<Expression>> Parse(const std::string& segment,
                                            int i) const override {
    return nullptr;
  }

  std::string type_name() const override { return "mock_partitioning"; }
};

class MockSourceFactoryTest : public SourceFactoryTest {
 public:
  void MakeFactory(std::vector<std::shared_ptr<Schema>> schemas) {
    factory_ = std::make_shared<MockSourceFactory>(schemas);
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

TEST_F(MockSourceFactoryTest, UnifySchemas) {
  MakeFactory({});
  AssertInspect({});

  MakeFactory({schema({i32}), schema({i32})});
  AssertInspect({i32});

  MakeFactory({schema({i32}), schema({i64})});
  AssertInspect({i32, i64});

  MakeFactory({schema({i32}), schema({i64})});
  AssertInspect({i32, i64});

  MakeFactory({schema({i32}), schema({i32_req})});
  AssertInspect({i32});

  MakeFactory({schema({i32, f64}), schema({i32_req, i64})});
  AssertInspect({i32, f64, i64});

  MakeFactory({schema({i32, f64}), schema({f64, i32_fake})});
  // Unification fails when fields with the same name have clashing types.
  ASSERT_RAISES(Invalid, factory_->Inspect());
  // Return the individual schema for closer inspection should not fail.
  AssertInspectSchemas({schema({i32, f64}), schema({f64, i32_fake})});
}

class FileSystemSourceFactoryTest : public SourceFactoryTest {
 public:
  void MakeFactory(const std::vector<fs::FileStats>& files) {
    MakeFileSystem(files);
    ASSERT_OK_AND_ASSIGN(factory_, FileSystemSourceFactory::Make(fs_, selector_, format_,
                                                                 factory_options_));
  }

  void AssertFinishWithPaths(std::vector<std::string> paths,
                             std::shared_ptr<Schema> schema = nullptr) {
    if (schema == nullptr) {
      ASSERT_OK_AND_ASSIGN(schema, factory_->Inspect());
    }
    options_ = ScanOptions::Make(schema);
    ASSERT_OK_AND_ASSIGN(source_, factory_->Finish(schema));
    AssertFragmentsAreFromPath(source_->GetFragments(options_), paths);
  }

 protected:
  fs::FileSelector selector_;
  FileSystemFactoryOptions factory_options_;
  std::shared_ptr<FileFormat> format_ = std::make_shared<DummyFileFormat>(schema({}));
};

TEST_F(FileSystemSourceFactoryTest, Basic) {
  MakeFactory({fs::File("a"), fs::File("b")});
  AssertFinishWithPaths({"a", "b"});
  MakeFactory({fs::Dir("a"), fs::Dir("a/b"), fs::File("a/b/c")});
}

TEST_F(FileSystemSourceFactoryTest, Selector) {
  selector_.base_dir = "A";
  selector_.recursive = true;

  MakeFactory({fs::File("0"), fs::File("A/a"), fs::File("A/A/a")});
  // "0" doesn't match selector, so it has been dropped:
  AssertFinishWithPaths({"A/a", "A/A/a"});

  factory_options_.partition_base_dir = "A/A";
  MakeFactory({fs::File("0"), fs::File("A/a"), fs::File("A/A/a")});
  // partition_base_dir should not affect filtered files, only the applied partition
  AssertInspect({});
  AssertFinishWithPaths({"A/a", "A/A/a"});
}

TEST_F(FileSystemSourceFactoryTest, ExplicitPartition) {
  selector_.base_dir = "a=ignored/base";
  factory_options_.partitioning =
      std::make_shared<HivePartitioning>(schema({field("a", float64())}));

  MakeFactory(
      {fs::File(selector_.base_dir + "/a=1"), fs::File(selector_.base_dir + "/a=2")});

  AssertInspect({field("a", float64())});
  AssertFinishWithPaths({selector_.base_dir + "/a=1", selector_.base_dir + "/a=2"});
}

TEST_F(FileSystemSourceFactoryTest, DiscoveredPartition) {
  selector_.base_dir = "a=ignored/base";
  factory_options_.partitioning = HivePartitioning::MakeFactory();
  MakeFactory(
      {fs::File(selector_.base_dir + "/a=1"), fs::File(selector_.base_dir + "/a=2")});

  AssertInspect({field("a", int32())});
  AssertFinishWithPaths({selector_.base_dir + "/a=1", selector_.base_dir + "/a=2"});
}

TEST_F(FileSystemSourceFactoryTest, MissingDirectories) {
  MakeFileSystem({fs::File("base_dir/a=3/b=3/dat"), fs::File("unpartitioned/ignored=3")});

  factory_options_.partition_base_dir = "base_dir";
  factory_options_.partitioning = std::make_shared<HivePartitioning>(
      schema({field("a", int32()), field("b", int32())}));

  ASSERT_OK_AND_ASSIGN(
      factory_, FileSystemSourceFactory::Make(
                    fs_, {"base_dir/a=3/b=3/dat", "unpartitioned/ignored=3"}, format_,
                    factory_options_));

  AssertInspect({field("a", int32()), field("b", int32())});
  AssertFinishWithPaths({"base_dir/a=3/b=3/dat", "unpartitioned/ignored=3"});
}

TEST_F(FileSystemSourceFactoryTest, OptionsIgnoredDefaultPrefixes) {
  MakeFactory({
      fs::File("."),
      fs::File("_"),
      fs::File("_$folder$"),
      fs::File("_SUCCESS"),
      fs::File("not_ignored_by_default"),
  });

  AssertFinishWithPaths({"not_ignored_by_default"});
}

TEST_F(FileSystemSourceFactoryTest, OptionsIgnoredCustomPrefixes) {
  factory_options_.ignore_prefixes = {"not_ignored"};
  MakeFactory({
      fs::File("."),
      fs::File("_"),
      fs::File("_$folder$"),
      fs::File("_SUCCESS"),
      fs::File("not_ignored_by_default"),
  });

  AssertFinishWithPaths({".", "_", "_$folder$", "_SUCCESS"});
}

TEST_F(FileSystemSourceFactoryTest, Inspect) {
  auto s = schema({field("f64", float64())});
  format_ = std::make_shared<DummyFileFormat>(s);

  // No files
  MakeFactory({});
  AssertInspect({});

  MakeFactory({fs::File("test")});
  AssertInspect(s->fields());
}

}  // namespace dataset
}  // namespace arrow
