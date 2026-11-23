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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <memory>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/compute/api_vector.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/plan.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/reader.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

#include "arrow/table.h"
#include "arrow/util/config.h"
#include "arrow/util/io_util.h"
#include "arrow/util/key_value_metadata.h"

#ifdef ARROW_PARQUET
#  include "arrow/dataset/file_parquet.h"
#endif

namespace arrow {

namespace dataset {

class SimpleWriteNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    internal::Initialize();
    mock_fs_ = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
    auto ipc_format = std::make_shared<dataset::IpcFileFormat>();

    fs_write_options_.filesystem = mock_fs_;
    fs_write_options_.base_dir = "/my_dataset";
    fs_write_options_.basename_template = "{i}.arrow";
    fs_write_options_.file_write_options = ipc_format->DefaultWriteOptions();
    fs_write_options_.partitioning = dataset::Partitioning::Default();
  }

  std::shared_ptr<fs::internal::MockFileSystem> mock_fs_;
  dataset::FileSystemDatasetWriteOptions fs_write_options_;
};

#ifdef ARROW_PARQUET
TEST_F(SimpleWriteNodeTest, DeleteMatchingPartitions) {
  const auto data_schema = schema({field("year", int32()), field("value", int32())});
  const auto partition_schema = schema({data_schema->field(0)});
  auto original = TableFromJSON(data_schema, {R"([[2019, -1], [20190, -2]])"});
  auto updates = TableFromJSON(data_schema, {R"([[2019, 1], [2020, 2], [2021, 3]])",
                                             R"([[2019, 4], [2021, 5], [2022, 6]])"});
  auto expected = TableFromJSON(data_schema, {R"([
      [20190, -2], [2019, 1], [2020, 2], [2021, 3], [2019, 4], [2021, 5],
      [2022, 6]
  ])"});
  auto filesystem = std::make_shared<fs::LocalFileSystem>();
  auto format = std::make_shared<ParquetFileFormat>();
  auto write = [](const FileSystemDatasetWriteOptions& options,
                  const std::shared_ptr<Table>& table) {
    auto dataset = std::make_shared<InMemoryDataset>(table);
    ARROW_ASSIGN_OR_RAISE(auto builder, dataset->NewScan());
    ARROW_ASSIGN_OR_RAISE(auto scanner, builder->Finish());
    return FileSystemDataset::Write(options, scanner);
  };

  for (const auto& partitioning : std::vector<std::shared_ptr<Partitioning>>{
           std::make_shared<DirectoryPartitioning>(partition_schema),
           std::make_shared<FilenamePartitioning>(partition_schema)}) {
    SCOPED_TRACE(partitioning->type_name());
    ASSERT_OK_AND_ASSIGN(auto tempdir,
                         ::arrow::internal::TemporaryDir::Make("delete-matching-"));
    ASSERT_OK_AND_ASSIGN(auto path, tempdir->path().Join("data"));
    auto options = fs_write_options_;
    options.filesystem = filesystem;
    options.base_dir = path.ToString();
    options.file_write_options = format->DefaultWriteOptions();
    options.partitioning = partitioning;
    options.basename_template = "old_{i}.parquet";
    options.existing_data_behavior = ExistingDataBehavior::kDeleteMatchingPartitions;
    ASSERT_OK(write(options, original->Slice(0, 1)));
    options.existing_data_behavior = ExistingDataBehavior::kOverwriteOrIgnore;
    ASSERT_OK(write(options, original->Slice(1, 1)));

    const bool filenames = partitioning->type_name() == "filename";
    const auto join = [&](std::string_view part) {
      return fs::internal::ConcatAbstractPath(options.base_dir, part);
    };
    const auto old_match = join(filenames ? "2019_old_0.parquet" : "2019/old_0.parquet");
    const auto alias = join("02019_older.parquet");
    const auto notes = join("2019_notes.txt");
    if (filenames) {
      ASSERT_OK(filesystem->Move(old_match, alias));
      ASSERT_OK_AND_ASSIGN(auto stream, filesystem->OpenOutputStream(notes));
      ASSERT_OK(stream->Write("Notes independent of this Parquet dataset"));
      ASSERT_OK(stream->Close());
    }

    options.basename_template = "part{i}.parquet";
    options.existing_data_behavior = ExistingDataBehavior::kDeleteMatchingPartitions;
    options.max_rows_per_file = options.max_rows_per_group = 1;
    ASSERT_OK(write(options, updates));

    ASSERT_OK_AND_ASSIGN(auto stale,
                         filesystem->GetFileInfo(filenames ? alias : old_match));
    EXPECT_EQ(stale.type(), fs::FileType::NotFound);
    if (filenames) {
      ASSERT_OK_AND_ASSIGN(auto info, filesystem->GetFileInfo(notes));
      EXPECT_EQ(info.type(), fs::FileType::File);
    }

    fs::FileSelector selector;
    selector.base_dir = options.base_dir;
    selector.recursive = true;
    FileSystemFactoryOptions factory_options;
    factory_options.partitioning = partitioning;
    factory_options.partition_base_dir = options.base_dir;
    factory_options.exclude_invalid_files = true;
    ASSERT_OK_AND_ASSIGN(
        auto factory,
        FileSystemDatasetFactory::Make(filesystem, selector, format, factory_options));
    ASSERT_OK_AND_ASSIGN(auto dataset, factory->Finish());
    ASSERT_OK_AND_ASSIGN(auto builder, dataset->NewScan());
    ASSERT_OK(builder->Project({"year", "value"}));
    ASSERT_OK_AND_ASSIGN(auto scanner, builder->Finish());
    ASSERT_OK_AND_ASSIGN(auto readback, scanner->ToTable());
    ASSERT_OK_AND_ASSIGN(
        auto indices, compute::SortIndices(
                          readback, compute::SortOptions({compute::SortKey("value")})));
    ASSERT_OK_AND_ASSIGN(auto sorted, compute::Take(readback, indices));
    AssertTablesEqual(*expected, *sorted.table(), /*same_chunk_layout=*/false);
  }
}
#endif

TEST_F(SimpleWriteNodeTest, CustomNullability) {
  // Create an input table with a nullable and a non-nullable type
  ExecBatch batch = gen::Gen({gen::Step()})->FailOnError()->ExecBatch(/*num_rows=*/1);
  std::shared_ptr<Schema> test_schema =
      schema({field("nullable_i32", uint32(), /*nullable=*/true),
              field("non_nullable_i32", uint32(), /*nullable=*/false)});
  std::shared_ptr<RecordBatch> record_batch =
      RecordBatch::Make(test_schema, /*num_rows=*/1,
                        {batch.values[0].make_array(), batch.values[0].make_array()});
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                       Table::FromRecordBatches({std::move(record_batch)}));

  ASSERT_TRUE(table->field(0)->nullable());
  ASSERT_FALSE(table->field(1)->nullable());

  dataset::WriteNodeOptions write_options(fs_write_options_);
  write_options.custom_schema = test_schema;

  // Write the data to disk (these plans use a project because it destroys whatever
  // metadata happened to be in the table source node's output schema).  This more
  // accurately simulates reading from a dataset.
  acero::Declaration plan = acero::Declaration::Sequence(
      {{"table_source", acero::TableSourceNodeOptions(table)},
       {"project",
        acero::ProjectNodeOptions({compute::field_ref(0), compute::field_ref(1)})},
       {"write", write_options}});

  ASSERT_OK(DeclarationToStatus(plan));

  // Read the file back out and verify the nullability
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<io::RandomAccessFile> file,
                       mock_fs_->OpenInputFile("/my_dataset/0.arrow"));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ipc::RecordBatchFileReader> file_reader,
                       ipc::RecordBatchFileReader::Open(file));
  std::shared_ptr<Schema> file_schema = file_reader->schema();

  ASSERT_TRUE(file_schema->field(0)->nullable());
  ASSERT_FALSE(file_schema->field(1)->nullable());

  // Invalid custom schema

  // Incorrect # of fields
  write_options.custom_schema = schema({});
  plan = acero::Declaration::Sequence(
      {{"table_source", acero::TableSourceNodeOptions(table)},
       {"project",
        acero::ProjectNodeOptions({compute::field_ref(0), compute::field_ref(1)})},
       {"write", write_options}});

  ASSERT_THAT(
      DeclarationToStatus(plan),
      Raises(StatusCode::TypeError,
             ::testing::HasSubstr("did not have the same number of fields as the data")));

  // Incorrect types
  write_options.custom_schema =
      schema({field("nullable_i32", int32()), field("non_nullable_i32", int32())});
  plan = acero::Declaration::Sequence(
      {{"table_source", acero::TableSourceNodeOptions(table)},
       {"project",
        acero::ProjectNodeOptions({compute::field_ref(0), compute::field_ref(1)})},
       {"write", write_options}});
  ASSERT_THAT(
      DeclarationToStatus(plan),
      Raises(StatusCode::TypeError, ::testing::HasSubstr("and the input data has type")));

  // Cannot have both custom_schema and custom_metadata
  write_options.custom_schema = test_schema;
  write_options.custom_metadata = key_value_metadata({{"foo", "bar"}});
  plan = acero::Declaration::Sequence(
      {{"table_source", acero::TableSourceNodeOptions(std::move(table))},
       {"project",
        acero::ProjectNodeOptions({compute::field_ref(0), compute::field_ref(1)})},
       {"write", write_options}});
  ASSERT_THAT(DeclarationToStatus(plan),
              Raises(StatusCode::TypeError,
                     ::testing::HasSubstr(
                         "Do not provide both custom_metadata and custom_schema")));
}

TEST_F(SimpleWriteNodeTest, CustomMetadata) {
  constexpr int64_t kRowsPerChunk = 1;
  constexpr int64_t kNumChunks = 1;
  // Create an input table with no schema metadata
  std::shared_ptr<Table> table =
      gen::Gen({gen::Step()})->FailOnError()->Table(kRowsPerChunk, kNumChunks);

  std::shared_ptr<KeyValueMetadata> custom_metadata =
      key_value_metadata({{"foo", "bar"}});

  dataset::WriteNodeOptions write_options(fs_write_options_);
  write_options.custom_metadata = custom_metadata;

  // Write the data to disk
  acero::Declaration plan = acero::Declaration::Sequence(
      {{"table_source", acero::TableSourceNodeOptions(table)},
       {"project", acero::ProjectNodeOptions({compute::field_ref(0)})},
       {"write", write_options}});

  ASSERT_OK(DeclarationToStatus(plan));

  // Read the file back out and verify the schema metadata
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<io::RandomAccessFile> file,
                       mock_fs_->OpenInputFile("/my_dataset/0.arrow"));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ipc::RecordBatchFileReader> file_reader,
                       ipc::RecordBatchFileReader::Open(file));
  std::shared_ptr<Schema> file_schema = file_reader->schema();

  ASSERT_TRUE(custom_metadata->Equals(*file_schema->metadata()));
}

}  // namespace dataset
}  // namespace arrow
