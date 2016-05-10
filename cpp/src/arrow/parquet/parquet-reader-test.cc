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

#include "gtest/gtest.h"

#include "arrow/test-util.h"
#include "arrow/parquet/reader.h"
#include "arrow/types/primitive.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

#include "parquet/api/reader.h"
#include "parquet/api/writer.h"

using ParquetBuffer = parquet::Buffer;
using parquet::BufferReader;
using parquet::InMemoryOutputStream;
using parquet::Int64Writer;
using parquet::ParquetFileReader;
using parquet::ParquetFileWriter;
using parquet::RandomAccessSource;
using parquet::Repetition;
using parquet::SchemaDescriptor;
using ParquetType = parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::NodePtr;
using parquet::schema::PrimitiveNode;

namespace arrow {

namespace parquet {

class TestReadParquet : public ::testing::Test {
 public:
  virtual void SetUp() {}

  std::shared_ptr<GroupNode> Int64Schema() {
    auto pnode = PrimitiveNode::Make("int64", Repetition::REQUIRED, ParquetType::INT64);
    NodePtr node_ =
        GroupNode::Make("schema", Repetition::REQUIRED, std::vector<NodePtr>({pnode}));
    return std::static_pointer_cast<GroupNode>(node_);
  }

  std::unique_ptr<ParquetFileReader> Int64File(
      std::vector<int64_t>& values, int num_chunks) {
    std::shared_ptr<GroupNode> schema = Int64Schema();
    std::shared_ptr<InMemoryOutputStream> sink(new InMemoryOutputStream());
    auto file_writer = ParquetFileWriter::Open(sink, schema);
    size_t chunk_size = values.size() / num_chunks;
    for (int i = 0; i < num_chunks; i++) {
      auto row_group_writer = file_writer->AppendRowGroup(chunk_size);
      auto column_writer = static_cast<Int64Writer*>(row_group_writer->NextColumn());
      int64_t* data = values.data() + i * chunk_size;
      column_writer->WriteBatch(chunk_size, nullptr, nullptr, data);
      column_writer->Close();
      row_group_writer->Close();
    }
    file_writer->Close();

    std::shared_ptr<ParquetBuffer> buffer = sink->GetBuffer();
    std::unique_ptr<RandomAccessSource> source(new BufferReader(buffer));
    return ParquetFileReader::Open(std::move(source));
  }

 private:
};

TEST_F(TestReadParquet, SingleColumnInt64) {
  std::vector<int64_t> values(100, 128);
  std::unique_ptr<ParquetFileReader> file_reader = Int64File(values, 1);
  arrow::parquet::FileReader reader(default_memory_pool(), std::move(file_reader));
  std::unique_ptr<arrow::parquet::FlatColumnReader> column_reader;
  ASSERT_NO_THROW(ASSERT_OK(reader.GetFlatColumn(0, &column_reader)));
  ASSERT_NE(nullptr, column_reader.get());
  std::shared_ptr<Array> out;
  ASSERT_OK(column_reader->NextBatch(100, &out));
  ASSERT_NE(nullptr, out.get());
  Int64Array* out_array = static_cast<Int64Array*>(out.get());
  for (size_t i = 0; i < values.size(); i++) {
    EXPECT_EQ(values[i], out_array->raw_data()[i]);
  }
}

TEST_F(TestReadParquet, SingleColumnInt64Chunked) {
  std::vector<int64_t> values(100, 128);
  std::unique_ptr<ParquetFileReader> file_reader = Int64File(values, 4);
  arrow::parquet::FileReader reader(default_memory_pool(), std::move(file_reader));
  std::unique_ptr<arrow::parquet::FlatColumnReader> column_reader;
  ASSERT_NO_THROW(ASSERT_OK(reader.GetFlatColumn(0, &column_reader)));
  ASSERT_NE(nullptr, column_reader.get());
  std::shared_ptr<Array> out;
  ASSERT_OK(column_reader->NextBatch(100, &out));
  ASSERT_NE(nullptr, out.get());
  Int64Array* out_array = static_cast<Int64Array*>(out.get());
  for (size_t i = 0; i < values.size(); i++) {
    EXPECT_EQ(values[i], out_array->raw_data()[i]);
  }
}

}  // namespace parquet

}  // namespace arrow
