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
#include "arrow/parquet/writer.h"
#include "arrow/types/primitive.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

#include "parquet/api/reader.h"
#include "parquet/api/writer.h"

using ParquetBuffer = parquet::Buffer;
using parquet::BufferReader;
using parquet::InMemoryOutputStream;
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

class TestParquetIO : public ::testing::Test {
 public:
  virtual void SetUp() {}

  std::shared_ptr<GroupNode> Schema(ParquetType::type parquet_type, Repetition::type repetition) {
    auto pnode = PrimitiveNode::Make("column1", repetition, parquet_type);
    NodePtr node_ =
        GroupNode::Make("schema", Repetition::REQUIRED, std::vector<NodePtr>({pnode}));
    return std::static_pointer_cast<GroupNode>(node_);
  }

  std::shared_ptr<PrimitiveArray> DoubleValueArray(
      size_t size, double value, size_t num_nulls) {
    std::vector<double> values(size, value);
    std::vector<uint8_t> valid_bytes(size, 1);

    for (size_t i = 0; i < num_nulls; i++) {
      valid_bytes[i * 2] = 0;
    }

    DoubleBuilder builder(default_memory_pool(), std::make_shared<DoubleType>());
    builder.Append(values.data(), values.size(), valid_bytes.data());
    return std::static_pointer_cast<PrimitiveArray>(builder.Finish());
  }

  std::shared_ptr<PrimitiveArray> Int64ValueArray(size_t size, int64_t value) {
    std::vector<int64_t> values(size, value);
    Int64Builder builder(default_memory_pool(), std::make_shared<Int64Type>());
    builder.Append(values.data(), values.size());
    return std::static_pointer_cast<PrimitiveArray>(builder.Finish());
  }

  std::unique_ptr<ParquetFileWriter> Int64FileWriter() {
    std::shared_ptr<GroupNode> schema = Schema(ParquetType::INT64, Repetition::REQUIRED);
    sink_ = std::make_shared<InMemoryOutputStream>();
    return ParquetFileWriter::Open(sink_, schema);
  }

  std::unique_ptr<ParquetFileWriter> DoubleFileWriter() {
    std::shared_ptr<GroupNode> schema = Schema(ParquetType::DOUBLE, Repetition::OPTIONAL);
    sink_ = std::make_shared<InMemoryOutputStream>();
    return ParquetFileWriter::Open(sink_, schema);
  }

  std::unique_ptr<ParquetFileReader> ReaderFromSink() {
    std::shared_ptr<ParquetBuffer> buffer = sink_->GetBuffer();
    std::unique_ptr<RandomAccessSource> source(new BufferReader(buffer));
    return ParquetFileReader::Open(std::move(source));
  }

  void ReadSingleColumnFile(
      std::unique_ptr<ParquetFileReader> file_reader, std::shared_ptr<Array>* out) {
    arrow::parquet::FileReader reader(default_memory_pool(), std::move(file_reader));
    std::unique_ptr<arrow::parquet::FlatColumnReader> column_reader;
    ASSERT_NO_THROW(ASSERT_OK(reader.GetFlatColumn(0, &column_reader)));
    ASSERT_NE(nullptr, column_reader.get());
    ASSERT_OK(column_reader->NextBatch(100, out));
    ASSERT_NE(nullptr, out->get());
  }

  std::unique_ptr<ParquetFileReader> Int64File(
      std::vector<int64_t>& values, int num_chunks) {
    std::unique_ptr<ParquetFileWriter> file_writer = Int64FileWriter();
    size_t chunk_size = values.size() / num_chunks;
    for (int i = 0; i < num_chunks; i++) {
      auto row_group_writer = file_writer->AppendRowGroup(chunk_size);
      auto column_writer =
          static_cast<::parquet::Int64Writer*>(row_group_writer->NextColumn());
      int64_t* data = values.data() + i * chunk_size;
      column_writer->WriteBatch(chunk_size, nullptr, nullptr, data);
      column_writer->Close();
      row_group_writer->Close();
    }
    file_writer->Close();
    return ReaderFromSink();
  }

 private:
  std::shared_ptr<InMemoryOutputStream> sink_;
};

TEST_F(TestParquetIO, SingleColumnInt64Read) {
  std::vector<int64_t> values(100, 128);
  std::unique_ptr<ParquetFileReader> file_reader = Int64File(values, 1);

  std::shared_ptr<Array> out;
  ReadSingleColumnFile(std::move(file_reader), &out);

  Int64Array* out_array = static_cast<Int64Array*>(out.get());
  for (size_t i = 0; i < values.size(); i++) {
    EXPECT_EQ(values[i], out_array->raw_data()[i]);
  }
}

TEST_F(TestParquetIO, SingleColumnInt64ChunkedRead) {
  std::vector<int64_t> values(100, 128);
  std::unique_ptr<ParquetFileReader> file_reader = Int64File(values, 4);

  std::shared_ptr<Array> out;
  ReadSingleColumnFile(std::move(file_reader), &out);

  Int64Array* out_array = static_cast<Int64Array*>(out.get());
  for (size_t i = 0; i < values.size(); i++) {
    EXPECT_EQ(values[i], out_array->raw_data()[i]);
  }
}

TEST_F(TestParquetIO, SingleColumnInt64Write) {
  std::shared_ptr<PrimitiveArray> values = Int64ValueArray(100, 128);

  FileWriter writer(default_memory_pool(), Int64FileWriter());
  ASSERT_NO_THROW(ASSERT_OK(writer.NewRowGroup(values->length())));
  ASSERT_NO_THROW(ASSERT_OK(writer.WriteFlatColumnChunk(values.get())));
  ASSERT_NO_THROW(ASSERT_OK(writer.Close()));

  std::shared_ptr<Array> out;
  ReadSingleColumnFile(ReaderFromSink(), &out);
  ASSERT_TRUE(values->Equals(out));
}

TEST_F(TestParquetIO, SingleColumnDoubleReadWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<PrimitiveArray> values = DoubleValueArray(100, 128, 10);

  FileWriter writer(default_memory_pool(), DoubleFileWriter());
  ASSERT_NO_THROW(ASSERT_OK(writer.NewRowGroup(values->length())));
  ASSERT_NO_THROW(ASSERT_OK(writer.WriteFlatColumnChunk(values.get())));
  ASSERT_NO_THROW(ASSERT_OK(writer.Close()));

  std::shared_ptr<Array> out;
  ReadSingleColumnFile(ReaderFromSink(), &out);
  ASSERT_TRUE(values->Equals(out));
}

TEST_F(TestParquetIO, SingleColumnInt64ChunkedWrite) {
  std::shared_ptr<PrimitiveArray> values = Int64ValueArray(100, 128);
  std::shared_ptr<PrimitiveArray> values_chunk = Int64ValueArray(25, 128);

  FileWriter writer(default_memory_pool(), Int64FileWriter());
  for (int i = 0; i < 4; i++) {
    ASSERT_NO_THROW(ASSERT_OK(writer.NewRowGroup(values_chunk->length())));
    ASSERT_NO_THROW(ASSERT_OK(writer.WriteFlatColumnChunk(values_chunk.get())));
  }
  ASSERT_NO_THROW(ASSERT_OK(writer.Close()));

  std::shared_ptr<Array> out;
  ReadSingleColumnFile(ReaderFromSink(), &out);
  ASSERT_TRUE(values->Equals(out));
}

TEST_F(TestParquetIO, SingleColumnDoubleChunkedWrite) {
  std::shared_ptr<PrimitiveArray> values = DoubleValueArray(100, 128, 10);
  std::shared_ptr<PrimitiveArray> values_chunk_nulls = DoubleValueArray(25, 128, 10);
  std::shared_ptr<PrimitiveArray> values_chunk = DoubleValueArray(25, 128, 0);

  FileWriter writer(default_memory_pool(), DoubleFileWriter());
  ASSERT_NO_THROW(ASSERT_OK(writer.NewRowGroup(values_chunk_nulls->length())));
  ASSERT_NO_THROW(ASSERT_OK(writer.WriteFlatColumnChunk(values_chunk_nulls.get())));
  for (int i = 0; i < 3; i++) {
    ASSERT_NO_THROW(ASSERT_OK(writer.NewRowGroup(values_chunk->length())));
    ASSERT_NO_THROW(ASSERT_OK(writer.WriteFlatColumnChunk(values_chunk.get())));
  }
  ASSERT_NO_THROW(ASSERT_OK(writer.Close()));

  std::shared_ptr<Array> out;
  ReadSingleColumnFile(ReaderFromSink(), &out);
  ASSERT_TRUE(values->Equals(out));
}

}  // namespace parquet

}  // namespace arrow
