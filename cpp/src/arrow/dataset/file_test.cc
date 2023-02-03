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
#include <tuple>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array/array_primitive.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/dataset/api.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/test_util_internal.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/status.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

namespace cp = arrow::compute;

namespace arrow {

using internal::TemporaryDir;

namespace dataset {

using fs::internal::GetAbstractPathExtension;
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

constexpr int kNumBatches = 4;
constexpr int kRowsPerBatch = 1024;
class MockFileFormat : public FileFormat {
 public:
  MockFileFormat() : FileFormat(/*default_fragment_scan_options=*/nullptr) {}

  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>& options,
      const std::shared_ptr<FileFragment>& file) const override {
    auto sch = schema({field("i32", int32())});
    RecordBatchVector batches;
    for (int i = 0; i < kNumBatches; i++) {
      batches.push_back(ConstantArrayGenerator::Zeroes(kRowsPerBatch, sch));
    }
    return MakeVectorGenerator(std::move(batches));
  }

 protected:
  std::string type_name() const override { return "mock"; }
  bool Equals(const FileFormat& other) const override { return false; }
  Result<bool> IsSupported(const FileSource& source) const override { return true; }
  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override {
    return Status::NotImplemented("Not needed for test");
  }
  Result<std::shared_ptr<FileWriter>> MakeWriter(
      std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileWriteOptions> options,
      fs::FileLocator destination_locator) const override {
    return Status::NotImplemented("Not needed for test");
  }
  std::shared_ptr<FileWriteOptions> DefaultWriteOptions() override { return nullptr; }
};

TEST(FileFormat, ScanAsync) {
  MockFileFormat format;
  auto scan_options = std::make_shared<ScanOptions>();
  ASSERT_OK_AND_ASSIGN(auto batch_gen, format.ScanBatchesAsync(scan_options, nullptr));
  ASSERT_FINISHES_OK_AND_ASSIGN(auto batches, CollectAsyncGenerator(batch_gen));
  ASSERT_EQ(kNumBatches, static_cast<int>(batches.size()));
  for (int i = 0; i < kNumBatches; i++) {
    ASSERT_EQ(kRowsPerBatch, batches[i]->num_rows());
  }
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
  auto new_schema = schema({field("i32", int32())});
  ASSERT_OK_AND_ASSIGN(auto new_dataset, dataset->ReplaceSchema(new_schema));
  AssertDatasetHasSchema(new_dataset, new_schema);
  // add nullable field (will be materialized as null during projection)
  new_schema = schema({field("str", utf8())});
  ASSERT_OK_AND_ASSIGN(new_dataset, dataset->ReplaceSchema(new_schema));
  AssertDatasetHasSchema(new_dataset, new_schema);
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
  MakeDataset({fs::File("a"), fs::File("b")}, root_partition, {},
              schema({field("i32", int32()), field("f32", float32())}));

  auto GetFragments = [&](compute::Expression filter) {
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

  // No root partition: don't prune any fragments
  MakeDataset({fs::File("a"), fs::File("b")}, literal(true), {},
              schema({field("i32", int32()), field("f32", float32())}));
  AssertFragmentsAreFromPath(GetFragments(equal(field_ref("f32"), literal(3.F))),
                             {"a", "b"});
}

TEST_F(TestFileSystemDataset, TreePartitionPruning) {
  auto root_partition = equal(field_ref("country"), literal("US"));

  std::vector<fs::FileInfo> regions = {
      fs::Dir("NY"), fs::File("NY/New York"),      fs::File("NY/Franklin"),
      fs::Dir("CA"), fs::File("CA/San Francisco"), fs::File("CA/Franklin"),
  };

  std::vector<compute::Expression> partitions = {
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

  auto GetFragments = [&](compute::Expression filter) {
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

  std::vector<compute::Expression> partitions = {
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

TEST_F(TestFileSystemDataset, WriteProjected) {
  // Regression test for ARROW-12620
  auto format = std::make_shared<IpcFileFormat>();
  auto fs = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
  FileSystemDatasetWriteOptions write_options;
  write_options.file_write_options = format->DefaultWriteOptions();
  write_options.filesystem = fs;
  write_options.base_dir = "root";
  write_options.partitioning = std::make_shared<HivePartitioning>(schema({}));
  write_options.basename_template = "{i}.feather";

  auto dataset_schema = schema({field("a", int64())});
  RecordBatchVector batches{
      ConstantArrayGenerator::Zeroes(kRowsPerBatch, dataset_schema)};
  ASSERT_EQ(0, batches[0]->column(0)->null_count());
  auto dataset = std::make_shared<InMemoryDataset>(dataset_schema, batches);
  ASSERT_OK_AND_ASSIGN(auto scanner_builder, dataset->NewScan());
  ASSERT_OK(scanner_builder->Project(
      {compute::call("add", {compute::field_ref("a"), compute::literal(1)})},
      {"a_plus_one"}));
  ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());

  ASSERT_OK(FileSystemDataset::Write(write_options, scanner));

  ASSERT_OK_AND_ASSIGN(auto dataset_factory, FileSystemDatasetFactory::Make(
                                                 fs, {"root/0.feather"}, format, {}));
  ASSERT_OK_AND_ASSIGN(auto written_dataset, dataset_factory->Finish(FinishOptions{}));
  auto expected_schema = schema({field("a_plus_one", int64())});
  AssertSchemaEqual(*expected_schema, *written_dataset->schema());
  ASSERT_OK_AND_ASSIGN(scanner_builder, written_dataset->NewScan());
  ASSERT_OK_AND_ASSIGN(scanner, scanner_builder->Finish());
  ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
  auto col = table->column(0);
  ASSERT_EQ(0, col->null_count());
  for (auto chunk : col->chunks()) {
    auto arr = std::dynamic_pointer_cast<Int64Array>(chunk);
    for (auto val : *arr) {
      ASSERT_TRUE(val.has_value());
      ASSERT_EQ(1, *val);
    }
  }
}

class FileSystemWriteTest : public testing::TestWithParam<std::tuple<bool, bool>> {
  using PlanFactory = std::function<std::vector<cp::Declaration>(
      const FileSystemDatasetWriteOptions&,
      std::function<Future<std::optional<cp::ExecBatch>>()>*)>;

 protected:
  bool IsParallel() { return std::get<0>(GetParam()); }
  bool IsSlow() { return std::get<1>(GetParam()); }

  FileSystemWriteTest() { dataset::internal::Initialize(); }

  void TestDatasetWriteRoundTrip(PlanFactory plan_factory, bool has_output) {
    // Runs in-memory data through the plan and then scans out the written
    // data to ensure it matches the source data
    auto format = std::make_shared<IpcFileFormat>();
    auto fs = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
    FileSystemDatasetWriteOptions write_options;
    write_options.file_write_options = format->DefaultWriteOptions();
    write_options.filesystem = fs;
    write_options.base_dir = "root";
    write_options.partitioning = std::make_shared<HivePartitioning>(schema({}));
    write_options.basename_template = "{i}.feather";
    const std::string kExpectedFilename = "root/0.feather";

    cp::BatchesWithSchema source_data;
    source_data.batches = {
        cp::ExecBatchFromJSON({int32(), boolean()}, "[[null, true], [4, false]]"),
        cp::ExecBatchFromJSON({int32(), boolean()},
                              "[[5, null], [6, false], [7, false]]")};
    source_data.schema = schema({field("i32", int32()), field("bool", boolean())});

    AsyncGenerator<std::optional<cp::ExecBatch>> sink_gen;

    ASSERT_OK_AND_ASSIGN(auto plan, cp::ExecPlan::Make());
    auto source_decl = cp::Declaration::Sequence(
        {{"source", cp::SourceNodeOptions{source_data.schema,
                                          source_data.gen(IsParallel(), IsSlow())}}});
    auto declarations = plan_factory(write_options, &sink_gen);
    declarations.insert(declarations.begin(), std::move(source_decl));
    ASSERT_OK(cp::Declaration::Sequence(std::move(declarations)).AddToPlan(plan.get()));

    if (has_output) {
      ASSERT_FINISHES_OK_AND_ASSIGN(auto out_batches,
                                    cp::StartAndCollect(plan.get(), sink_gen));
      cp::AssertExecBatchesEqualIgnoringOrder(source_data.schema, source_data.batches,
                                              out_batches);
    } else {
      ASSERT_FINISHES_OK(cp::StartAndFinish(plan.get()));
    }

    // Read written dataset and make sure it matches
    ASSERT_OK_AND_ASSIGN(auto dataset_factory, FileSystemDatasetFactory::Make(
                                                   fs, {kExpectedFilename}, format, {}));
    ASSERT_OK_AND_ASSIGN(auto written_dataset, dataset_factory->Finish(FinishOptions{}));
    AssertSchemaEqual(*source_data.schema, *written_dataset->schema());

    ASSERT_OK_AND_ASSIGN(plan, cp::ExecPlan::Make());
    ASSERT_OK_AND_ASSIGN(auto scanner_builder, written_dataset->NewScan());
    ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder->Finish());
    ASSERT_OK(cp::Declaration::Sequence(
                  {
                      {"scan", ScanNodeOptions{written_dataset, scanner->options()}},
                      {"sink", cp::SinkNodeOptions{&sink_gen}},
                  })
                  .AddToPlan(plan.get()));

    ASSERT_FINISHES_OK_AND_ASSIGN(auto written_batches,
                                  cp::StartAndCollect(plan.get(), sink_gen));
    cp::AssertExecBatchesEqualIgnoringOrder(source_data.schema, source_data.batches,
                                            written_batches);
  }
};

TEST_P(FileSystemWriteTest, Write) {
  auto plan_factory =
      [](const FileSystemDatasetWriteOptions& write_options,
         std::function<Future<std::optional<cp::ExecBatch>>()>* sink_gen) {
        return std::vector<cp::Declaration>{{"write", WriteNodeOptions{write_options}}};
      };
  TestDatasetWriteRoundTrip(plan_factory, /*has_output=*/false);
}

TEST_P(FileSystemWriteTest, TeeWrite) {
  auto plan_factory =
      [](const FileSystemDatasetWriteOptions& write_options,
         std::function<Future<std::optional<cp::ExecBatch>>()>* sink_gen) {
        return std::vector<cp::Declaration>{
            {"tee", WriteNodeOptions{write_options}},
            {"sink", cp::SinkNodeOptions{sink_gen}},
        };
      };
  TestDatasetWriteRoundTrip(plan_factory, /*has_output=*/true);
}

INSTANTIATE_TEST_SUITE_P(
    FileSystemWrite, FileSystemWriteTest,
    testing::Combine(testing::Values(false, true), testing::Values(false, true)),
    [](const testing::TestParamInfo<FileSystemWriteTest::ParamType>& info) {
      std::string parallel_desc = std::get<0>(info.param) ? "parallel" : "serial";
      std::string speed_desc = std::get<1>(info.param) ? "slow" : "fast";
      return parallel_desc + "_" + speed_desc;
    });

}  // namespace dataset
}  // namespace arrow
