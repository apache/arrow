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
#include "arrow/parquet/test-util.h"
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

const int SMALL_SIZE = 100;
const int LARGE_SIZE = 10000;

template <typename TestType>
struct test_traits {};

template <>
struct test_traits<Int32Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
};

template <>
struct test_traits<Int64Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT64;
};

template <>
struct test_traits<FloatType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::FLOAT;
};

template <>
struct test_traits<DoubleType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::DOUBLE;
};

template <typename T>
using ParquetDataType = ::parquet::DataType<test_traits<T>::parquet_enum>;

template <typename T>
using ParquetWriter = ::parquet::TypedColumnWriter<ParquetDataType<T>>;

template <typename TestType>
class TestParquetIO : public ::testing::Test {
 public:
  typedef typename TestType::c_type T;
  virtual void SetUp() {}

  std::shared_ptr<GroupNode> MakeSchema(
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
    ASSERT_OK(column_reader->NextBatch(SMALL_SIZE, out));
    ASSERT_NE(nullptr, out->get());
  }

  void ReadTableFromFile(
      std::unique_ptr<ParquetFileReader> file_reader, std::shared_ptr<Table>* out) {
    arrow::parquet::FileReader reader(default_memory_pool(), std::move(file_reader));
    ASSERT_NO_THROW(ASSERT_OK(reader.ReadFlatTable(out)));
    ASSERT_NE(nullptr, out->get());
  }

  std::unique_ptr<ParquetFileReader> TestFile(std::vector<T>& values, int num_chunks) {
    std::shared_ptr<GroupNode> schema =
        MakeSchema(test_traits<TestType>::parquet_enum, Repetition::REQUIRED);
    std::unique_ptr<ParquetFileWriter> file_writer = MakeWriter(schema);
    size_t chunk_size = values.size() / num_chunks;
    for (int i = 0; i < num_chunks; i++) {
      auto row_group_writer = file_writer->AppendRowGroup(chunk_size);
      auto column_writer =
          static_cast<ParquetWriter<TestType>*>(row_group_writer->NextColumn());
      T* data = values.data() + i * chunk_size;
      column_writer->WriteBatch(chunk_size, nullptr, nullptr, data);
      column_writer->Close();
      row_group_writer->Close();
    }
    file_writer->Close();
    return ReaderFromSink();
  }

  std::shared_ptr<InMemoryOutputStream> sink_;
};

typedef ::testing::Types<Int32Type, Int64Type, FloatType, DoubleType> TestTypes;

TYPED_TEST_CASE(TestParquetIO, TestTypes);

TYPED_TEST(TestParquetIO, SingleColumnRequiredRead) {
  std::vector<typename TypeParam::c_type> values(SMALL_SIZE, 128);
  std::unique_ptr<ParquetFileReader> file_reader = this->TestFile(values, 1);

  std::shared_ptr<Array> out;
  this->ReadSingleColumnFile(std::move(file_reader), &out);

  ExpectArray<typename TypeParam::c_type>(values.data(), out.get());
}

TYPED_TEST(TestParquetIO, SingleColumnRequiredTableRead) {
  std::vector<typename TypeParam::c_type> values(SMALL_SIZE, 128);
  std::unique_ptr<ParquetFileReader> file_reader = this->TestFile(values, 1);

  std::shared_ptr<Table> out;
  this->ReadTableFromFile(std::move(file_reader), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(SMALL_SIZE, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ExpectArray<typename TypeParam::c_type>(values.data(), chunked_array->chunk(0).get());
}

TYPED_TEST(TestParquetIO, SingleColumnRequiredChunkedRead) {
  std::vector<typename TypeParam::c_type> values(SMALL_SIZE, 128);
  std::unique_ptr<ParquetFileReader> file_reader = this->TestFile(values, 4);

  std::shared_ptr<Array> out;
  this->ReadSingleColumnFile(std::move(file_reader), &out);

  ExpectArray<typename TypeParam::c_type>(values.data(), out.get());
}

TYPED_TEST(TestParquetIO, SingleColumnRequiredChunkedTableRead) {
  std::vector<typename TypeParam::c_type> values(SMALL_SIZE, 128);
  std::unique_ptr<ParquetFileReader> file_reader = this->TestFile(values, 4);

  std::shared_ptr<Table> out;
  this->ReadTableFromFile(std::move(file_reader), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(SMALL_SIZE, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ExpectArray<typename TypeParam::c_type>(values.data(), chunked_array->chunk(0).get());
}

TYPED_TEST(TestParquetIO, SingleColumnRequiredWrite) {
  std::shared_ptr<PrimitiveArray> values = NonNullArray<TypeParam>(SMALL_SIZE, 128);

  std::shared_ptr<GroupNode> schema =
      this->MakeSchema(test_traits<TypeParam>::parquet_enum, Repetition::REQUIRED);
  FileWriter writer(default_memory_pool(), this->MakeWriter(schema));
  ASSERT_NO_THROW(ASSERT_OK(writer.NewRowGroup(values->length())));
  ASSERT_NO_THROW(ASSERT_OK(writer.WriteFlatColumnChunk(values.get())));
  ASSERT_NO_THROW(ASSERT_OK(writer.Close()));

  std::shared_ptr<Array> out;
  this->ReadSingleColumnFile(this->ReaderFromSink(), &out);
  ASSERT_TRUE(values->Equals(out));
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredWrite) {
  std::shared_ptr<PrimitiveArray> values = NonNullArray<TypeParam>(SMALL_SIZE, 128);
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_NO_THROW(ASSERT_OK(
      WriteFlatTable(table.get(), default_memory_pool(), this->sink_, values->length())));

  std::shared_ptr<Table> out;
  this->ReadTableFromFile(this->ReaderFromSink(), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(100, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalReadWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<PrimitiveArray> values = NullableArray<TypeParam>(SMALL_SIZE, 128, 10);

  std::shared_ptr<GroupNode> schema =
      this->MakeSchema(test_traits<TypeParam>::parquet_enum, Repetition::OPTIONAL);
  FileWriter writer(default_memory_pool(), this->MakeWriter(schema));
  ASSERT_NO_THROW(ASSERT_OK(writer.NewRowGroup(values->length())));
  ASSERT_NO_THROW(ASSERT_OK(writer.WriteFlatColumnChunk(values.get())));
  ASSERT_NO_THROW(ASSERT_OK(writer.Close()));

  std::shared_ptr<Array> out;
  this->ReadSingleColumnFile(this->ReaderFromSink(), &out);
  ASSERT_TRUE(values->Equals(out));
}

TYPED_TEST(TestParquetIO, SingleColumnTableOptionalReadWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<PrimitiveArray> values = NullableArray<TypeParam>(SMALL_SIZE, 128, 10);
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_NO_THROW(ASSERT_OK(
      WriteFlatTable(table.get(), default_memory_pool(), this->sink_, values->length())));

  std::shared_ptr<Table> out;
  this->ReadTableFromFile(this->ReaderFromSink(), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(SMALL_SIZE, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
}

TYPED_TEST(TestParquetIO, SingleColumnIntRequiredChunkedWrite) {
  std::shared_ptr<PrimitiveArray> values = NonNullArray<TypeParam>(SMALL_SIZE, 128);
  std::shared_ptr<PrimitiveArray> values_chunk =
      NonNullArray<TypeParam>(SMALL_SIZE / 4, 128);

  std::shared_ptr<GroupNode> schema =
      this->MakeSchema(test_traits<TypeParam>::parquet_enum, Repetition::REQUIRED);
  FileWriter writer(default_memory_pool(), this->MakeWriter(schema));
  for (int i = 0; i < 4; i++) {
    ASSERT_NO_THROW(ASSERT_OK(writer.NewRowGroup(values_chunk->length())));
    ASSERT_NO_THROW(ASSERT_OK(writer.WriteFlatColumnChunk(values_chunk.get())));
  }
  ASSERT_NO_THROW(ASSERT_OK(writer.Close()));

  std::shared_ptr<Array> out;
  this->ReadSingleColumnFile(this->ReaderFromSink(), &out);
  ASSERT_TRUE(values->Equals(out));
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredChunkedWrite) {
  std::shared_ptr<PrimitiveArray> values = NonNullArray<TypeParam>(LARGE_SIZE, 128);
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_NO_THROW(
      ASSERT_OK(WriteFlatTable(table.get(), default_memory_pool(), this->sink_, 512)));

  std::shared_ptr<Table> out;
  this->ReadTableFromFile(this->ReaderFromSink(), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(LARGE_SIZE, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalChunkedWrite) {
  std::shared_ptr<PrimitiveArray> values = NullableArray<TypeParam>(SMALL_SIZE, 128, 10);
  std::shared_ptr<PrimitiveArray> values_chunk_nulls =
      NullableArray<TypeParam>(SMALL_SIZE / 4, 128, 10);
  std::shared_ptr<PrimitiveArray> values_chunk =
      NullableArray<TypeParam>(SMALL_SIZE / 4, 128, 0);

  std::shared_ptr<GroupNode> schema =
      this->MakeSchema(test_traits<TypeParam>::parquet_enum, Repetition::OPTIONAL);
  FileWriter writer(default_memory_pool(), this->MakeWriter(schema));
  ASSERT_NO_THROW(ASSERT_OK(writer.NewRowGroup(values_chunk_nulls->length())));
  ASSERT_NO_THROW(ASSERT_OK(writer.WriteFlatColumnChunk(values_chunk_nulls.get())));
  for (int i = 0; i < 3; i++) {
    ASSERT_NO_THROW(ASSERT_OK(writer.NewRowGroup(values_chunk->length())));
    ASSERT_NO_THROW(ASSERT_OK(writer.WriteFlatColumnChunk(values_chunk.get())));
  }
  ASSERT_NO_THROW(ASSERT_OK(writer.Close()));

  std::shared_ptr<Array> out;
  this->ReadSingleColumnFile(this->ReaderFromSink(), &out);
  ASSERT_TRUE(values->Equals(out));
}

TYPED_TEST(TestParquetIO, SingleColumnTableOptionalChunkedWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<PrimitiveArray> values = NullableArray<TypeParam>(LARGE_SIZE, 128, 100);
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_NO_THROW(
      ASSERT_OK(WriteFlatTable(table.get(), default_memory_pool(), this->sink_, 512)));

  std::shared_ptr<Table> out;
  this->ReadTableFromFile(this->ReaderFromSink(), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(LARGE_SIZE, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
}

}  // namespace parquet

}  // namespace arrow
