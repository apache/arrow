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
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

namespace arrow {
namespace dataset {

using fs::internal::GetAbstractPathExtension;
using internal::TemporaryDir;

TEST(FileSource, PathBased) {
  auto localfs = std::make_shared<fs::LocalFileSystem>();

  std::string p1 = "/path/to/file.ext";
  std::string p2 = "/path/to/file.ext.gz";

  FileSource source1(p1, localfs);
  FileSource source2(p2, localfs, Compression::GZIP);

  ASSERT_EQ(p1, source1.path());
  ASSERT_TRUE(localfs->Equals(*source1.filesystem()));
  ASSERT_EQ(Compression::UNCOMPRESSED, source1.compression());

  ASSERT_EQ(p2, source2.path());
  ASSERT_TRUE(localfs->Equals(*source2.filesystem()));
  ASSERT_EQ(Compression::GZIP, source2.compression());

  // Test copy constructor and comparison
  FileSource source3;
  source3 = source1;
  ASSERT_EQ(source1.path(), source3.path());
  ASSERT_EQ(source1.filesystem(), source3.filesystem());
}

TEST(FileSource, BufferBased) {
  std::string the_data = "this is the file contents";
  auto buf = std::make_shared<Buffer>(the_data);

  FileSource source1(buf);
  FileSource source2(buf, Compression::LZ4);

  ASSERT_TRUE(source1.buffer()->Equals(*buf));
  ASSERT_EQ(Compression::UNCOMPRESSED, source1.compression());

  ASSERT_TRUE(source2.buffer()->Equals(*buf));
  ASSERT_EQ(Compression::LZ4, source2.compression());

  FileSource source3;
  source3 = source1;
  ASSERT_EQ(source1.buffer(), source3.buffer());
}

TEST_F(TestFileSystemDataset, Basic) {
  MakeDataset({});
  AssertFragmentsAreFromPath(dataset_->GetFragments(), {});

  MakeDataset({fs::File("a"), fs::File("b"), fs::File("c")});
  AssertFragmentsAreFromPath(dataset_->GetFragments(), {"a", "b", "c"});
  AssertFilesAre(dataset_, {"a", "b", "c"});

  // Should not create fragment from directories.
  MakeDataset({fs::Dir("A"), fs::Dir("A/B"), fs::File("A/a"), fs::File("A/B/b")});
  AssertFragmentsAreFromPath(dataset_->GetFragments(), {"A/a", "A/B/b"});
  AssertFilesAre(dataset_, {"A/a", "A/B/b"});
}

TEST_F(TestFileSystemDataset, ReplaceSchema) {
  auto schm = schema({field("i32", int32()), field("f64", float64())});
  auto format = std::make_shared<DummyFileFormat>(schm);
  ASSERT_OK_AND_ASSIGN(auto dataset,
                       FileSystemDataset::Make(schm, scalar(true), format, {}));

  // drop field
  ASSERT_OK(dataset->ReplaceSchema(schema({field("i32", int32())})).status());
  // add nullable field (will be materialized as null during projection)
  ASSERT_OK(dataset->ReplaceSchema(schema({field("str", utf8())})).status());
  // incompatible type
  ASSERT_RAISES(TypeError,
                dataset->ReplaceSchema(schema({field("i32", utf8())})).status());
  // incompatible nullability
  ASSERT_RAISES(
      TypeError,
      dataset->ReplaceSchema(schema({field("f64", float64(), /*nullable=*/false)}))
          .status());
  // add non-nullable field
  ASSERT_RAISES(TypeError,
                dataset->ReplaceSchema(schema({field("str", utf8(), /*nullable=*/false)}))
                    .status());
}

TEST_F(TestFileSystemDataset, RootPartitionPruning) {
  auto root_partition = ("a"_ == 5).Copy();
  MakeDataset({fs::File("a"), fs::File("b")}, root_partition);

  // Default filter should always return all data.
  AssertFragmentsAreFromPath(dataset_->GetFragments(), {"a", "b"});

  // filter == partition
  AssertFragmentsAreFromPath(dataset_->GetFragments(root_partition), {"a", "b"});

  // Same partition key, but non matching filter
  AssertFragmentsAreFromPath(dataset_->GetFragments(("a"_ == 6).Copy()), {});

  AssertFragmentsAreFromPath(dataset_->GetFragments(("a"_ > 1).Copy()), {"a", "b"});

  // different key shouldn't prune
  AssertFragmentsAreFromPath(dataset_->GetFragments(("b"_ == 6).Copy()), {"a", "b"});

  // No partition should match
  MakeDataset({fs::File("a"), fs::File("b")});
  AssertFragmentsAreFromPath(dataset_->GetFragments(("b"_ == 6).Copy()), {"a", "b"});
}

TEST_F(TestFileSystemDataset, TreePartitionPruning) {
  auto root_partition = ("country"_ == "US").Copy();
  std::vector<fs::FileInfo> regions = {
      fs::Dir("NY"), fs::File("NY/New York"),      fs::File("NY/Franklin"),
      fs::Dir("CA"), fs::File("CA/San Francisco"), fs::File("CA/Franklin"),
  };

  ExpressionVector partitions = {
      ("state"_ == "NY").Copy(),
      ("state"_ == "NY" and "city"_ == "New York").Copy(),
      ("state"_ == "NY" and "city"_ == "Franklin").Copy(),
      ("state"_ == "CA").Copy(),
      ("state"_ == "CA" and "city"_ == "San Francisco").Copy(),
      ("state"_ == "CA" and "city"_ == "Franklin").Copy(),
  };

  MakeDataset(regions, root_partition, partitions);

  std::vector<std::string> all_cities = {"CA/San Francisco", "CA/Franklin", "NY/New York",
                                         "NY/Franklin"};
  std::vector<std::string> ca_cities = {"CA/San Francisco", "CA/Franklin"};
  std::vector<std::string> franklins = {"CA/Franklin", "NY/Franklin"};

  // Default filter should always return all data.
  AssertFragmentsAreFromPath(dataset_->GetFragments(), all_cities);

  // Dataset's partitions are respected
  AssertFragmentsAreFromPath(dataset_->GetFragments(("country"_ == "US").Copy()),
                             all_cities);
  AssertFragmentsAreFromPath(dataset_->GetFragments(("country"_ == "FR").Copy()), {});

  AssertFragmentsAreFromPath(dataset_->GetFragments(("state"_ == "CA").Copy()),
                             ca_cities);

  // Filter where no decisions can be made on inner nodes when filter don't
  // apply to inner partitions.
  AssertFragmentsAreFromPath(dataset_->GetFragments(("city"_ == "Franklin").Copy()),
                             franklins);
}

TEST_F(TestFileSystemDataset, FragmentPartitions) {
  auto root_partition = ("country"_ == "US").Copy();
  std::vector<fs::FileInfo> regions = {
      fs::Dir("NY"), fs::File("NY/New York"),      fs::File("NY/Franklin"),
      fs::Dir("CA"), fs::File("CA/San Francisco"), fs::File("CA/Franklin"),
  };

  ExpressionVector partitions = {
      ("state"_ == "NY").Copy(),
      ("state"_ == "NY" and "city"_ == "New York").Copy(),
      ("state"_ == "NY" and "city"_ == "Franklin").Copy(),
      ("state"_ == "CA").Copy(),
      ("state"_ == "CA" and "city"_ == "San Francisco").Copy(),
      ("state"_ == "CA" and "city"_ == "Franklin").Copy(),
  };

  MakeDataset(regions, root_partition, partitions);

  auto with_root = [&](const Expression& state, const Expression& city) {
    return and_(state.Copy(), city.Copy());
  };

  AssertFragmentsHavePartitionExpressions(
      dataset_->GetFragments(),
      {
          with_root("state"_ == "CA", "city"_ == "San Francisco"),
          with_root("state"_ == "CA", "city"_ == "Franklin"),
          with_root("state"_ == "NY", "city"_ == "New York"),
          with_root("state"_ == "NY", "city"_ == "Franklin"),
      });
}

}  // namespace dataset
}  // namespace arrow
