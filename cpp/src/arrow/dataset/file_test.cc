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
#include "arrow/dataset/forest_internal.h"
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
using testing::ContainerEq;

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
  AssertFragmentsAreFromPath(*dataset_->GetFragments(), {});

  MakeDataset({fs::File("a"), fs::File("b"), fs::File("c")});
  AssertFragmentsAreFromPath(*dataset_->GetFragments(), {"a", "b", "c"});
  AssertFilesAre(dataset_, {"a", "b", "c"});

  // Should not create fragment from directories.
  MakeDataset({fs::Dir("A"), fs::Dir("A/B"), fs::File("A/a"), fs::File("A/B/b")});
  AssertFragmentsAreFromPath(*dataset_->GetFragments(), {"A/a", "A/B/b"});
  AssertFilesAre(dataset_, {"A/a", "A/B/b"});
}

TEST_F(TestFileSystemDataset, ReplaceSchema) {
  auto schm = schema({field("i32", int32()), field("f64", float64())});
  auto format = std::make_shared<DummyFileFormat>(schm);
  ASSERT_OK_AND_ASSIGN(auto dataset,
                       FileSystemDataset::Make(schm, literal(true), format, nullptr, {}));

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
  auto root_partition = equal(field_ref("i32"), literal(5));
  MakeDataset({fs::File("a"), fs::File("b")}, root_partition);

  auto GetFragments = [&](Expression filter) {
    return *dataset_->GetFragments(*filter.Bind(*dataset_->schema()));
  };

  // Default filter should always return all data.
  AssertFragmentsAreFromPath(*dataset_->GetFragments(), {"a", "b"});

  // filter == partition
  AssertFragmentsAreFromPath(GetFragments(root_partition), {"a", "b"});

  // Same partition key, but non matching filter
  AssertFragmentsAreFromPath(GetFragments(equal(field_ref("i32"), literal(6))), {});

  AssertFragmentsAreFromPath(GetFragments(greater(field_ref("i32"), literal(1))),
                             {"a", "b"});

  // different key shouldn't prune
  AssertFragmentsAreFromPath(GetFragments(equal(field_ref("f32"), literal(3.F))),
                             {"a", "b"});

  // No partition should match
  MakeDataset({fs::File("a"), fs::File("b")});
  AssertFragmentsAreFromPath(GetFragments(equal(field_ref("f32"), literal(3.F))),
                             {"a", "b"});
}

TEST_F(TestFileSystemDataset, TreePartitionPruning) {
  auto root_partition = equal(field_ref("country"), literal("US"));

  std::vector<fs::FileInfo> regions = {
      fs::Dir("NY"), fs::File("NY/New York"),      fs::File("NY/Franklin"),
      fs::Dir("CA"), fs::File("CA/San Francisco"), fs::File("CA/Franklin"),
  };

  std::vector<Expression> partitions = {
      equal(field_ref("state"), literal("NY")),

      and_(equal(field_ref("state"), literal("NY")),
           equal(field_ref("city"), literal("New York"))),

      and_(equal(field_ref("state"), literal("NY")),
           equal(field_ref("city"), literal("Franklin"))),

      equal(field_ref("state"), literal("CA")),

      and_(equal(field_ref("state"), literal("CA")),
           equal(field_ref("city"), literal("San Francisco"))),

      and_(equal(field_ref("state"), literal("CA")),
           equal(field_ref("city"), literal("Franklin"))),
  };

  MakeDataset(
      regions, root_partition, partitions,
      schema({field("country", utf8()), field("state", utf8()), field("city", utf8())}));

  std::vector<std::string> all_cities = {"CA/San Francisco", "CA/Franklin", "NY/New York",
                                         "NY/Franklin"};
  std::vector<std::string> ca_cities = {"CA/San Francisco", "CA/Franklin"};
  std::vector<std::string> franklins = {"CA/Franklin", "NY/Franklin"};

  // Default filter should always return all data.
  AssertFragmentsAreFromPath(*dataset_->GetFragments(), all_cities);

  auto GetFragments = [&](Expression filter) {
    return *dataset_->GetFragments(*filter.Bind(*dataset_->schema()));
  };

  // Dataset's partitions are respected
  AssertFragmentsAreFromPath(GetFragments(equal(field_ref("country"), literal("US"))),
                             all_cities);
  AssertFragmentsAreFromPath(GetFragments(equal(field_ref("country"), literal("FR"))),
                             {});

  AssertFragmentsAreFromPath(GetFragments(equal(field_ref("state"), literal("CA"))),
                             ca_cities);

  // Filter where no decisions can be made on inner nodes when filter don't
  // apply to inner partitions.
  AssertFragmentsAreFromPath(GetFragments(equal(field_ref("city"), literal("Franklin"))),
                             franklins);
}

TEST_F(TestFileSystemDataset, FragmentPartitions) {
  auto root_partition = equal(field_ref("country"), literal("US"));
  std::vector<fs::FileInfo> regions = {
      fs::Dir("NY"), fs::File("NY/New York"),      fs::File("NY/Franklin"),
      fs::Dir("CA"), fs::File("CA/San Francisco"), fs::File("CA/Franklin"),
  };

  std::vector<Expression> partitions = {
      equal(field_ref("state"), literal("NY")),

      and_(equal(field_ref("state"), literal("NY")),
           equal(field_ref("city"), literal("New York"))),

      and_(equal(field_ref("state"), literal("NY")),
           equal(field_ref("city"), literal("Franklin"))),

      equal(field_ref("state"), literal("CA")),

      and_(equal(field_ref("state"), literal("CA")),
           equal(field_ref("city"), literal("San Francisco"))),

      and_(equal(field_ref("state"), literal("CA")),
           equal(field_ref("city"), literal("Franklin"))),
  };

  MakeDataset(
      regions, root_partition, partitions,
      schema({field("country", utf8()), field("state", utf8()), field("city", utf8())}));

  AssertFragmentsHavePartitionExpressions(
      dataset_, {
                    and_(equal(field_ref("state"), literal("CA")),
                         equal(field_ref("city"), literal("San Francisco"))),
                    and_(equal(field_ref("state"), literal("CA")),
                         equal(field_ref("city"), literal("Franklin"))),
                    and_(equal(field_ref("state"), literal("NY")),
                         equal(field_ref("city"), literal("New York"))),
                    and_(equal(field_ref("state"), literal("NY")),
                         equal(field_ref("city"), literal("Franklin"))),
                });
}

class TestFilesystemDatasetNestedParallelism : public NestedParallelismMixin {};

TEST_F(TestFilesystemDatasetNestedParallelism, Write) {
  constexpr int NUM_BATCHES = 32;
  RecordBatchVector batches;
  for (int i = 0; i < NUM_BATCHES; i++) {
    batches.push_back(ConstantArrayGenerator::Zeroes(/*size=*/1, schema_));
  }
  auto dataset = std::make_shared<NestedParallelismDataset>(schema_, std::move(batches));
  ScannerBuilder builder{dataset, options_};
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());

  ASSERT_OK_AND_ASSIGN(auto output_dir, TemporaryDir::Make("nested-parallel-dataset"));

  auto format = std::make_shared<DiscardingRowCountingFormat>();
  auto rows_written = std::make_shared<std::atomic<int>>(0);
  std::shared_ptr<FileWriteOptions> file_write_options =
      std::make_shared<DiscardingRowCountingFileWriteOptions>(rows_written);
  FileSystemDatasetWriteOptions dataset_write_options;
  dataset_write_options.file_write_options = file_write_options;
  dataset_write_options.basename_template = "{i}";
  dataset_write_options.partitioning = std::make_shared<HivePartitioning>(schema({}));
  dataset_write_options.base_dir = output_dir->path().ToString();
  dataset_write_options.filesystem = std::make_shared<fs::LocalFileSystem>();

  ASSERT_OK(FileSystemDataset::Write(dataset_write_options, scanner));
  ASSERT_EQ(NUM_BATCHES, rows_written->load());
}

// Tests of subtree pruning

struct TestPathTree {
  fs::FileInfo info;
  std::vector<TestPathTree> subtrees;

  explicit TestPathTree(std::string file_path) : info(fs::File(std::move(file_path))) {}

  TestPathTree(std::string dir_path, std::vector<TestPathTree> subtrees)
      : info(fs::Dir(std::move(dir_path))), subtrees(std::move(subtrees)) {}

  TestPathTree(Forest::Ref ref, const std::vector<fs::FileInfo>& infos)
      : info(infos[ref.i]) {
    const Forest& forest = *ref.forest;

    int begin = ref.i + 1;
    int end = begin + ref.num_descendants();

    for (int i = begin; i < end; ++i) {
      subtrees.emplace_back(forest[i], infos);
      i += forest[i].num_descendants();
    }
  }

  bool operator==(const TestPathTree& other) const {
    return info == other.info && subtrees == other.subtrees;
  }

  std::string ToString() const {
    auto out = "\n" + info.path();
    if (info.IsDirectory()) out += "/";

    for (const auto& subtree : subtrees) {
      out += subtree.ToString();
    }
    return out;
  }

  friend std::ostream& operator<<(std::ostream& os, const TestPathTree& tree) {
    return os << tree.ToString();
  }
};

using PT = TestPathTree;

Forest MakeForest(std::vector<fs::FileInfo>* infos) {
  std::sort(infos->begin(), infos->end(), fs::FileInfo::ByPath{});

  return Forest(static_cast<int>(infos->size()), [&](int i, int j) {
    return fs::internal::IsAncestorOf(infos->at(i).path(), infos->at(j).path());
  });
}

void ExpectForestIs(std::vector<fs::FileInfo> infos, std::vector<PT> expected_roots) {
  auto forest = MakeForest(&infos);

  std::vector<PT> actual_roots;
  ASSERT_OK(forest.Visit(
      [&](Forest::Ref ref) -> Result<bool> {
        actual_roots.emplace_back(ref, infos);
        return false;  // only vist roots
      },
      [](Forest::Ref) {}));

  // visit expected and assert equality
  EXPECT_THAT(actual_roots, ContainerEq(expected_roots));
}

TEST(Forest, Basic) {
  ExpectForestIs({}, {});

  ExpectForestIs({fs::File("aa")}, {PT("aa")});
  ExpectForestIs({fs::Dir("AA")}, {PT("AA", {})});
  ExpectForestIs({fs::Dir("AA"), fs::File("AA/aa")}, {PT("AA", {PT("AA/aa")})});
  ExpectForestIs({fs::Dir("AA"), fs::Dir("AA/BB"), fs::File("AA/BB/0")},
                 {PT("AA", {PT("AA/BB", {PT("AA/BB/0")})})});

  // Missing parent can still find ancestor.
  ExpectForestIs({fs::Dir("AA"), fs::File("AA/BB/bb")}, {PT("AA", {PT("AA/BB/bb")})});

  // Ancestors should link to parent regardless of ordering.
  ExpectForestIs({fs::File("AA/aa"), fs::Dir("AA")}, {PT("AA", {PT("AA/aa")})});

  // Multiple roots are supported.
  ExpectForestIs({fs::File("aa"), fs::File("bb")}, {PT("aa"), PT("bb")});
  ExpectForestIs({fs::File("00"), fs::Dir("AA"), fs::File("AA/aa"), fs::File("BB/bb")},
                 {PT("00"), PT("AA", {PT("AA/aa")}), PT("BB/bb")});
  ExpectForestIs({fs::Dir("AA"), fs::Dir("AA/BB"), fs::File("AA/BB/0"), fs::Dir("CC"),
                  fs::Dir("CC/BB"), fs::File("CC/BB/0")},
                 {PT("AA", {PT("AA/BB", {PT("AA/BB/0")})}),
                  PT("CC", {PT("CC/BB", {PT("CC/BB/0")})})});
}

TEST(Forest, HourlyETL) {
  // This test mimics a scenario where an ETL dumps hourly files in a structure
  // `$year/$month/$day/$hour/*.parquet`.
  constexpr int64_t kYears = 3;
  constexpr int64_t kMonthsPerYear = 12;
  constexpr int64_t kDaysPerMonth = 31;
  constexpr int64_t kHoursPerDay = 24;
  constexpr int64_t kFilesPerHour = 2;

  // Avoid constructing strings
  std::vector<std::string> numbers{kDaysPerMonth + 1};
  for (size_t i = 0; i < numbers.size(); i++) {
    numbers[i] = std::to_string(i);
    if (numbers[i].size() == 1) {
      numbers[i] = "0" + numbers[i];
    }
  }

  auto join = [](const std::vector<std::string>& path) {
    return fs::internal::JoinAbstractPath(path);
  };

  std::vector<fs::FileInfo> infos;

  std::vector<PT> forest;
  for (int64_t year = 0; year < kYears; year++) {
    auto year_str = std::to_string(year + 2000);
    auto year_dir = fs::Dir(year_str);
    infos.push_back(year_dir);

    std::vector<PT> months;
    for (int64_t month = 0; month < kMonthsPerYear; month++) {
      auto month_str = join({year_str, numbers[month + 1]});
      auto month_dir = fs::Dir(month_str);
      infos.push_back(month_dir);

      std::vector<PT> days;
      for (int64_t day = 0; day < kDaysPerMonth; day++) {
        auto day_str = join({month_str, numbers[day + 1]});
        auto day_dir = fs::Dir(day_str);
        infos.push_back(day_dir);

        std::vector<PT> hours;
        for (int64_t hour = 0; hour < kHoursPerDay; hour++) {
          auto hour_str = join({day_str, numbers[hour]});
          auto hour_dir = fs::Dir(hour_str);
          infos.push_back(hour_dir);

          std::vector<PT> files;
          for (int64_t file = 0; file < kFilesPerHour; file++) {
            auto file_str = join({hour_str, numbers[file] + ".parquet"});
            auto file_fd = fs::File(file_str);
            infos.push_back(file_fd);
            files.emplace_back(file_str);
          }

          auto hour_pt = PT(hour_str, std::move(files));
          hours.push_back(hour_pt);
        }

        auto day_pt = PT(day_str, std::move(hours));
        days.push_back(day_pt);
      }

      auto month_pt = PT(month_str, std::move(days));
      months.push_back(month_pt);
    }

    auto year_pt = PT(year_str, std::move(months));
    forest.push_back(year_pt);
  }

  ExpectForestIs(infos, forest);
}

TEST(Forest, Visit) {
  using Infos = std::vector<fs::FileInfo>;

  for (auto infos : {Infos{}, Infos{fs::Dir("A"), fs::File("A/a")},
                     Infos{fs::Dir("AA"), fs::Dir("AA/BB"), fs::File("AA/BB/0"),
                           fs::Dir("CC"), fs::Dir("CC/BB"), fs::File("CC/BB/0")}}) {
    ASSERT_TRUE(std::is_sorted(infos.begin(), infos.end(), fs::FileInfo::ByPath{}));

    auto forest = MakeForest(&infos);

    auto ignore_post = [](Forest::Ref) {};

    // noop is fine
    ASSERT_OK(
        forest.Visit([](Forest::Ref) -> Result<bool> { return false; }, ignore_post));

    // Should propagate failure
    if (forest.size() != 0) {
      ASSERT_RAISES(
          Invalid,
          forest.Visit([](Forest::Ref) -> Result<bool> { return Status::Invalid(""); },
                       ignore_post));
    }

    // Ensure basic visit of all nodes
    int i = 0;
    ASSERT_OK(forest.Visit(
        [&](Forest::Ref ref) -> Result<bool> {
          EXPECT_EQ(ref.i, i);
          ++i;
          return true;
        },
        ignore_post));

    // Visit only directories
    Infos actual_dirs;
    ASSERT_OK(forest.Visit(
        [&](Forest::Ref ref) -> Result<bool> {
          if (!infos[ref.i].IsDirectory()) {
            return false;
          }
          actual_dirs.push_back(infos[ref.i]);
          return true;
        },
        ignore_post));

    Infos expected_dirs;
    for (const auto& info : infos) {
      if (info.IsDirectory()) {
        expected_dirs.push_back(info);
      }
    }
    EXPECT_THAT(actual_dirs, ContainerEq(expected_dirs));
  }
}

TEST(Subtree, EncodeExpression) {
  SubtreeImpl tree;
  ASSERT_EQ(0, tree.GetOrInsert(equal(field_ref("a"), literal("1"))));
  // Should be idempotent
  ASSERT_EQ(0, tree.GetOrInsert(equal(field_ref("a"), literal("1"))));
  ASSERT_EQ(equal(field_ref("a"), literal("1")), tree.code_to_expr_[0]);

  SubtreeImpl::expression_codes codes;
  auto conj =
      and_(equal(field_ref("a"), literal("1")), equal(field_ref("b"), literal("2")));
  tree.EncodeConjunctionMembers(conj, &codes);
  ASSERT_EQ(SubtreeImpl::expression_codes({0, 1}), codes);

  codes.clear();
  conj = or_(equal(field_ref("a"), literal("1")), equal(field_ref("b"), literal("2")));
  tree.EncodeConjunctionMembers(conj, &codes);
  ASSERT_EQ(SubtreeImpl::expression_codes({2}), codes);
}

TEST(Subtree, GetSubtreeExpression) {
  SubtreeImpl tree;
  const auto expr_a = equal(field_ref("a"), literal("1"));
  const auto expr_b = equal(field_ref("b"), literal("2"));
  const auto code_a = tree.GetOrInsert(expr_a);
  const auto code_b = tree.GetOrInsert(expr_b);
  ASSERT_EQ(expr_a,
            tree.GetSubtreeExpression(SubtreeImpl::Encoded{util::nullopt, {code_a}}));
  ASSERT_EQ(expr_b, tree.GetSubtreeExpression(
                        SubtreeImpl::Encoded{util::nullopt, {code_a, code_b}}));
}

TEST(Subtree, EncodeFragments) {
  auto fragment_schema = schema({});
  const auto expr_a =
      and_(equal(field_ref("a"), literal("1")), equal(field_ref("b"), literal("2")));
  const auto expr_b =
      and_(equal(field_ref("a"), literal("2")), equal(field_ref("b"), literal("3")));
  std::vector<std::shared_ptr<InMemoryFragment>> fragments;
  fragments.push_back(std::make_shared<InMemoryFragment>(
      fragment_schema, arrow::RecordBatchVector(), expr_a));
  fragments.push_back(std::make_shared<InMemoryFragment>(
      fragment_schema, arrow::RecordBatchVector(), expr_b));

  SubtreeImpl tree;
  auto encoded = tree.EncodeFragments(fragments);
  EXPECT_THAT(
      tree.code_to_expr_,
      ContainerEq(std::vector<Expression>{
          equal(field_ref("a"), literal("1")), equal(field_ref("b"), literal("2")),
          equal(field_ref("a"), literal("2")), equal(field_ref("b"), literal("3"))}));
  EXPECT_THAT(
      encoded,
      testing::UnorderedElementsAreArray({
          SubtreeImpl::Encoded{util::make_optional<int>(0),
                               SubtreeImpl::expression_codes({0, 1})},
          SubtreeImpl::Encoded{util::make_optional<int>(1),
                               SubtreeImpl::expression_codes({2, 3})},
          SubtreeImpl::Encoded{util::nullopt, SubtreeImpl::expression_codes({0})},
          SubtreeImpl::Encoded{util::nullopt, SubtreeImpl::expression_codes({2})},
          SubtreeImpl::Encoded{util::nullopt, SubtreeImpl::expression_codes({0, 1})},
          SubtreeImpl::Encoded{util::nullopt, SubtreeImpl::expression_codes({2, 3})},
      }));
}

}  // namespace dataset
}  // namespace arrow
