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
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/plan.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/reader.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

#include "arrow/table.h"
#include "arrow/util/key_value_metadata.h"

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
