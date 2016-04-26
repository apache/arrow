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

#include <gtest/gtest.h>

#include "parquet/column/reader.h"
#include "parquet/column/writer.h"
#include "parquet/file/reader.h"
#include "parquet/file/writer.h"
#include "parquet/types.h"
#include "parquet/util/input.h"
#include "parquet/util/output.h"

namespace parquet {

using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;

namespace test {

class TestSerialize : public ::testing::Test {
 public:
  void SetUpSchemaRequired() {
    auto pnode = PrimitiveNode::Make("int64", Repetition::REQUIRED, Type::INT64);
    node_ = GroupNode::Make("schema", Repetition::REQUIRED,
        std::vector<NodePtr>({pnode}));
    schema_.Init(node_);
  }

  void SetUpSchemaOptional() {
    auto pnode = PrimitiveNode::Make("int64", Repetition::OPTIONAL, Type::INT64);
    node_ = GroupNode::Make("schema", Repetition::REQUIRED,
        std::vector<NodePtr>({pnode}));
    schema_.Init(node_);
  }

  void SetUpSchemaRepeated() {
    auto pnode = PrimitiveNode::Make("int64", Repetition::REPEATED, Type::INT64);
    node_ = GroupNode::Make("schema", Repetition::REQUIRED,
        std::vector<NodePtr>({pnode}));
    schema_.Init(node_);
  }

  void SetUp() {
    SetUpSchemaRequired();
  }

 protected:
  NodePtr node_;
  SchemaDescriptor schema_;
};


TEST_F(TestSerialize, SmallFile) {
  std::shared_ptr<InMemoryOutputStream> sink(new InMemoryOutputStream());
  auto gnode = std::static_pointer_cast<GroupNode>(node_);
  auto file_writer = ParquetFileWriter::Open(sink, gnode);
  auto row_group_writer = file_writer->AppendRowGroup(100);
  auto column_writer = static_cast<Int64Writer*>(row_group_writer->NextColumn());
  std::vector<int64_t> values(100, 128);
  column_writer->WriteBatch(values.size(), nullptr, nullptr, values.data());
  column_writer->Close();
  row_group_writer->Close();
  file_writer->Close();

  auto buffer = sink->GetBuffer();
  std::unique_ptr<RandomAccessSource> source(new BufferReader(buffer));
  auto file_reader = ParquetFileReader::Open(std::move(source));
  ASSERT_EQ(1, file_reader->num_columns());
  ASSERT_EQ(1, file_reader->num_row_groups());
  ASSERT_EQ(100, file_reader->num_rows());

  auto rg_reader = file_reader->RowGroup(0);
  ASSERT_EQ(1, rg_reader->num_columns());
  ASSERT_EQ(100, rg_reader->num_rows());

  auto col_reader = std::static_pointer_cast<Int64Reader>(rg_reader->Column(0));
  std::vector<int64_t> values_out(100);
  std::vector<int16_t> def_levels_out(100);
  std::vector<int16_t> rep_levels_out(100);
  int64_t values_read;
  col_reader->ReadBatch(values_out.size(), def_levels_out.data(), rep_levels_out.data(),
      values_out.data(), &values_read);
  ASSERT_EQ(100, values_read);
  ASSERT_EQ(values, values_out);
}

} // namespace test

} // namespace parquet
