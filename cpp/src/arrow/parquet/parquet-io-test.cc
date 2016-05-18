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

template <typename ArrowType>
std::shared_ptr<PrimitiveArray> NonNullArray(
    size_t size, typename ArrowType::c_type value) {
  std::vector<typename ArrowType::c_type> values(size, value);
  NumericBuilder<ArrowType> builder(default_memory_pool(), std::make_shared<ArrowType>());
  builder.Append(values.data(), values.size());
  return std::static_pointer_cast<PrimitiveArray>(builder.Finish());
}

// This helper function only supports (size/2) nulls yet.
template <typename ArrowType>
std::shared_ptr<PrimitiveArray> NullableArray(
    size_t size, typename ArrowType::c_type value, size_t num_nulls) {
  std::vector<typename ArrowType::c_type> values(size, value);
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  NumericBuilder<ArrowType> builder(default_memory_pool(), std::make_shared<ArrowType>());
  builder.Append(values.data(), values.size(), valid_bytes.data());
  return std::static_pointer_cast<PrimitiveArray>(builder.Finish());
}

class TestParquetIO : public ::testing::Test {
 public:
  virtual void SetUp() {}

  std::shared_ptr<GroupNode> Schema(
      ParquetType::type parquet_type, Repetition::type repetition) {
    auto pnode = PrimitiveNode::Make("column1", repetition, parquet_type);
    NodePtr node_ =
        GroupNode::Make("schema", Repetition::REQUIRED, std::vector<NodePtr>({pnode}));
    return std::static_pointer_cast<GroupNode>(node_);
  }

  std::unique_ptr<ParquetFileWriter> MakeWriter(std::shared_ptr<GroupNode>& schema) {
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
    std::shared_ptr<GroupNode> schema = Schema(ParquetType::INT64, Repetition::REQUIRED);
    std::unique_ptr<ParquetFileWriter> file_writer = MakeWriter(schema);
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
  std::shared_ptr<PrimitiveArray> values = NonNullArray<Int64Type>(100, 128);

  std::shared_ptr<GroupNode> schema = Schema(ParquetType::INT64, Repetition::REQUIRED);
  FileWriter writer(default_memory_pool(), MakeWriter(schema));
  ASSERT_NO_THROW(ASSERT_OK(writer.NewRowGroup(values->length())));
  ASSERT_NO_THROW(ASSERT_OK(writer.WriteFlatColumnChunk(values.get())));
  ASSERT_NO_THROW(ASSERT_OK(writer.Close()));

  std::shared_ptr<Array> out;
  ReadSingleColumnFile(ReaderFromSink(), &out);
  ASSERT_TRUE(values->Equals(out));
}

TEST_F(TestParquetIO, SingleColumnDoubleReadWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<PrimitiveArray> values = NullableArray<DoubleType>(100, 128, 10);

  std::shared_ptr<GroupNode> schema = Schema(ParquetType::DOUBLE, Repetition::OPTIONAL);
  FileWriter writer(default_memory_pool(), MakeWriter(schema));
  ASSERT_NO_THROW(ASSERT_OK(writer.NewRowGroup(values->length())));
  ASSERT_NO_THROW(ASSERT_OK(writer.WriteFlatColumnChunk(values.get())));
  ASSERT_NO_THROW(ASSERT_OK(writer.Close()));

  std::shared_ptr<Array> out;
  ReadSingleColumnFile(ReaderFromSink(), &out);
  ASSERT_TRUE(values->Equals(out));
}

TEST_F(TestParquetIO, SingleColumnInt64ChunkedWrite) {
  std::shared_ptr<PrimitiveArray> values = NonNullArray<Int64Type>(100, 128);
  std::shared_ptr<PrimitiveArray> values_chunk = NonNullArray<Int64Type>(25, 128);

  std::shared_ptr<GroupNode> schema = Schema(ParquetType::INT64, Repetition::REQUIRED);
  FileWriter writer(default_memory_pool(), MakeWriter(schema));
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
  std::shared_ptr<PrimitiveArray> values = NullableArray<DoubleType>(100, 128, 10);
  std::shared_ptr<PrimitiveArray> values_chunk_nulls =
      NullableArray<DoubleType>(25, 128, 10);
  std::shared_ptr<PrimitiveArray> values_chunk = NullableArray<DoubleType>(25, 128, 0);

  std::shared_ptr<GroupNode> schema = Schema(ParquetType::DOUBLE, Repetition::OPTIONAL);
  FileWriter writer(default_memory_pool(), MakeWriter(schema));
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
