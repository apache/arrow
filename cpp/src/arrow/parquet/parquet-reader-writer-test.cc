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
#include "arrow/types/construct.h"
#include "arrow/types/primitive.h"
#include "arrow/types/string.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

#include "parquet/api/reader.h"
#include "parquet/api/writer.h"

using ParquetBuffer = parquet::Buffer;
using parquet::BufferReader;
using parquet::default_writer_properties;
using parquet::InMemoryOutputStream;
using parquet::LogicalType;
using parquet::ParquetFileReader;
using parquet::ParquetFileWriter;
using parquet::RandomAccessSource;
using parquet::Repetition;
using parquet::SchemaDescriptor;
using parquet::ParquetVersion;
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
struct test_traits<BooleanType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::BOOLEAN;
  static constexpr LogicalType::type logical_enum = LogicalType::NONE;
  static uint8_t const value;
};

const uint8_t test_traits<BooleanType>::value(1);

template <>
struct test_traits<UInt8Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static constexpr LogicalType::type logical_enum = LogicalType::UINT_8;
  static uint8_t const value;
};

const uint8_t test_traits<UInt8Type>::value(64);

template <>
struct test_traits<Int8Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static constexpr LogicalType::type logical_enum = LogicalType::INT_8;
  static int8_t const value;
};

const int8_t test_traits<Int8Type>::value(-64);

template <>
struct test_traits<UInt16Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static constexpr LogicalType::type logical_enum = LogicalType::UINT_16;
  static uint16_t const value;
};

const uint16_t test_traits<UInt16Type>::value(1024);

template <>
struct test_traits<Int16Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static constexpr LogicalType::type logical_enum = LogicalType::INT_16;
  static int16_t const value;
};

const int16_t test_traits<Int16Type>::value(-1024);

template <>
struct test_traits<UInt32Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static constexpr LogicalType::type logical_enum = LogicalType::UINT_32;
  static uint32_t const value;
};

const uint32_t test_traits<UInt32Type>::value(1024);

template <>
struct test_traits<Int32Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static constexpr LogicalType::type logical_enum = LogicalType::NONE;
  static int32_t const value;
};

const int32_t test_traits<Int32Type>::value(-1024);

template <>
struct test_traits<UInt64Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT64;
  static constexpr LogicalType::type logical_enum = LogicalType::UINT_64;
  static uint64_t const value;
};

const uint64_t test_traits<UInt64Type>::value(1024);

template <>
struct test_traits<Int64Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT64;
  static constexpr LogicalType::type logical_enum = LogicalType::NONE;
  static int64_t const value;
};

const int64_t test_traits<Int64Type>::value(-1024);

template <>
struct test_traits<FloatType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::FLOAT;
  static constexpr LogicalType::type logical_enum = LogicalType::NONE;
  static float const value;
};

const float test_traits<FloatType>::value(2.1f);

template <>
struct test_traits<DoubleType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::DOUBLE;
  static constexpr LogicalType::type logical_enum = LogicalType::NONE;
  static double const value;
};

const double test_traits<DoubleType>::value(4.2);

template <>
struct test_traits<StringType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::BYTE_ARRAY;
  static constexpr LogicalType::type logical_enum = LogicalType::UTF8;
  static std::string const value;
};

const std::string test_traits<StringType>::value("Test");

template <typename T>
using ParquetDataType = ::parquet::DataType<test_traits<T>::parquet_enum>;

template <typename T>
using ParquetWriter = ::parquet::TypedColumnWriter<ParquetDataType<T>>;

template <typename TestType>
class TestParquetIO : public ::testing::Test {
 public:
  virtual void SetUp() {}

  std::shared_ptr<GroupNode> MakeSchema(Repetition::type repetition) {
    auto pnode = PrimitiveNode::Make("column1", repetition,
        test_traits<TestType>::parquet_enum, test_traits<TestType>::logical_enum);
    NodePtr node_ =
        GroupNode::Make("schema", Repetition::REQUIRED, std::vector<NodePtr>({pnode}));
    return std::static_pointer_cast<GroupNode>(node_);
  }

  std::unique_ptr<ParquetFileWriter> MakeWriter(
      const std::shared_ptr<GroupNode>& schema) {
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
    ASSERT_OK_NO_THROW(reader.GetFlatColumn(0, &column_reader));
    ASSERT_NE(nullptr, column_reader.get());

    ASSERT_OK(column_reader->NextBatch(SMALL_SIZE, out));
    ASSERT_NE(nullptr, out->get());
  }

  void ReadAndCheckSingleColumnFile(Array* values) {
    std::shared_ptr<Array> out;
    ReadSingleColumnFile(ReaderFromSink(), &out);
    ASSERT_TRUE(values->Equals(out));
  }

  void ReadTableFromFile(
      std::unique_ptr<ParquetFileReader> file_reader, std::shared_ptr<Table>* out) {
    arrow::parquet::FileReader reader(default_memory_pool(), std::move(file_reader));
    ASSERT_OK_NO_THROW(reader.ReadFlatTable(out));
    ASSERT_NE(nullptr, out->get());
  }

  void ReadAndCheckSingleColumnTable(const std::shared_ptr<Array>& values) {
    std::shared_ptr<Table> out;
    ReadTableFromFile(ReaderFromSink(), &out);
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(values->length(), out->num_rows());

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
    ASSERT_EQ(1, chunked_array->num_chunks());
    ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
  }

  template <typename ArrayType>
  void WriteFlatColumn(const std::shared_ptr<GroupNode>& schema,
      const std::shared_ptr<ArrayType>& values) {
    FileWriter writer(default_memory_pool(), MakeWriter(schema));
    ASSERT_OK_NO_THROW(writer.NewRowGroup(values->length()));
    ASSERT_OK_NO_THROW(writer.WriteFlatColumnChunk(values.get()));
    ASSERT_OK_NO_THROW(writer.Close());
  }

  std::shared_ptr<InMemoryOutputStream> sink_;
};

// We habe separate tests for UInt32Type as this is currently the only type
// where a roundtrip does not yield the identical Array structure.
// There we write an UInt32 Array but receive an Int64 Array as result for
// Parquet version 1.0.

typedef ::testing::Types<BooleanType, UInt8Type, Int8Type, UInt16Type, Int16Type,
    Int32Type, UInt64Type, Int64Type, FloatType, DoubleType, StringType> TestTypes;

TYPED_TEST_CASE(TestParquetIO, TestTypes);

TYPED_TEST(TestParquetIO, SingleColumnRequiredWrite) {
  auto values = NonNullArray<TypeParam>(SMALL_SIZE);

  std::shared_ptr<GroupNode> schema = this->MakeSchema(Repetition::REQUIRED);
  this->WriteFlatColumn(schema, values);

  this->ReadAndCheckSingleColumnFile(values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredWrite) {
  auto values = NonNullArray<TypeParam>(SMALL_SIZE);
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteFlatTable(table.get(), default_memory_pool(), this->sink_,
      values->length(), default_writer_properties()));

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
  auto values = NullableArray<TypeParam>(SMALL_SIZE, 10);

  std::shared_ptr<GroupNode> schema = this->MakeSchema(Repetition::OPTIONAL);
  this->WriteFlatColumn(schema, values);

  this->ReadAndCheckSingleColumnFile(values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnTableOptionalReadWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values = NullableArray<TypeParam>(SMALL_SIZE, 10);
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteFlatTable(table.get(), default_memory_pool(), this->sink_,
      values->length(), default_writer_properties()));

  this->ReadAndCheckSingleColumnTable(values);
}

TYPED_TEST(TestParquetIO, SingleColumnRequiredChunkedWrite) {
  auto values = NonNullArray<TypeParam>(SMALL_SIZE);
  int64_t chunk_size = values->length() / 4;

  std::shared_ptr<GroupNode> schema = this->MakeSchema(Repetition::REQUIRED);
  FileWriter writer(default_memory_pool(), this->MakeWriter(schema));
  for (int i = 0; i < 4; i++) {
    ASSERT_OK_NO_THROW(writer.NewRowGroup(chunk_size));
    ASSERT_OK_NO_THROW(
        writer.WriteFlatColumnChunk(values.get(), i * chunk_size, chunk_size));
  }
  ASSERT_OK_NO_THROW(writer.Close());

  this->ReadAndCheckSingleColumnFile(values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredChunkedWrite) {
  auto values = NonNullArray<TypeParam>(LARGE_SIZE);
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteFlatTable(
      table.get(), default_memory_pool(), this->sink_, 512, default_writer_properties()));

  this->ReadAndCheckSingleColumnTable(values);
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalChunkedWrite) {
  int64_t chunk_size = SMALL_SIZE / 4;
  auto values = NullableArray<TypeParam>(SMALL_SIZE, 10);

  std::shared_ptr<GroupNode> schema = this->MakeSchema(Repetition::OPTIONAL);
  FileWriter writer(default_memory_pool(), this->MakeWriter(schema));
  for (int i = 0; i < 4; i++) {
    ASSERT_OK_NO_THROW(writer.NewRowGroup(chunk_size));
    ASSERT_OK_NO_THROW(
        writer.WriteFlatColumnChunk(values.get(), i * chunk_size, chunk_size));
  }
  ASSERT_OK_NO_THROW(writer.Close());

  this->ReadAndCheckSingleColumnFile(values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnTableOptionalChunkedWrite) {
  // This also tests max_definition_level = 1
  auto values = NullableArray<TypeParam>(LARGE_SIZE, 100);
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteFlatTable(
      table.get(), default_memory_pool(), this->sink_, 512, default_writer_properties()));

  this->ReadAndCheckSingleColumnTable(values);
}

using TestUInt32ParquetIO = TestParquetIO<UInt32Type>;

TEST_F(TestUInt32ParquetIO, Parquet_2_0_Compability) {
  // This also tests max_definition_level = 1
  std::shared_ptr<PrimitiveArray> values = NullableArray<UInt32Type>(LARGE_SIZE, 100);
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);

  // Parquet 2.0 roundtrip should yield an uint32_t column again
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  std::shared_ptr<::parquet::WriterProperties> properties =
      ::parquet::WriterProperties::Builder()
          .version(ParquetVersion::PARQUET_2_0)
          ->build();
  ASSERT_OK_NO_THROW(
      WriteFlatTable(table.get(), default_memory_pool(), this->sink_, 512, properties));
  this->ReadAndCheckSingleColumnTable(values);
}

TEST_F(TestUInt32ParquetIO, Parquet_1_0_Compability) {
  // This also tests max_definition_level = 1
  std::shared_ptr<PrimitiveArray> values = NullableArray<UInt32Type>(LARGE_SIZE, 100);
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);

  // Parquet 1.0 returns an int64_t column as there is no way to tell a Parquet 1.0
  // reader that a column is unsigned.
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  std::shared_ptr<::parquet::WriterProperties> properties =
      ::parquet::WriterProperties::Builder()
          .version(ParquetVersion::PARQUET_1_0)
          ->build();
  ASSERT_OK_NO_THROW(
      WriteFlatTable(table.get(), default_memory_pool(), this->sink_, 512, properties));

  std::shared_ptr<Array> expected_values;
  std::shared_ptr<PoolBuffer> int64_data =
      std::make_shared<PoolBuffer>(default_memory_pool());
  {
    ASSERT_OK(int64_data->Resize(sizeof(int64_t) * values->length()));
    int64_t* int64_data_ptr = reinterpret_cast<int64_t*>(int64_data->mutable_data());
    const uint32_t* uint32_data_ptr =
        reinterpret_cast<const uint32_t*>(values->data()->data());
    // std::copy might be faster but this is explicit on the casts)
    for (int64_t i = 0; i < values->length(); i++) {
      int64_data_ptr[i] = static_cast<int64_t>(uint32_data_ptr[i]);
    }
  }
  ASSERT_OK(MakePrimitiveArray(std::make_shared<Int64Type>(), values->length(),
      int64_data, values->null_count(), values->null_bitmap(), &expected_values));
  this->ReadAndCheckSingleColumnTable(expected_values);
}

template <typename T>
using ParquetCDataType = typename ParquetDataType<T>::c_type;

template <typename TestType>
class TestPrimitiveParquetIO : public TestParquetIO<TestType> {
 public:
  typedef typename TestType::c_type T;

  void MakeTestFile(std::vector<T>& values, int num_chunks,
      std::unique_ptr<ParquetFileReader>* file_reader) {
    std::shared_ptr<GroupNode> schema = this->MakeSchema(Repetition::REQUIRED);
    std::unique_ptr<ParquetFileWriter> file_writer = this->MakeWriter(schema);
    size_t chunk_size = values.size() / num_chunks;
    // Convert to Parquet's expected physical type
    std::vector<uint8_t> values_buffer(
        sizeof(ParquetCDataType<TestType>) * values.size());
    auto values_parquet =
        reinterpret_cast<ParquetCDataType<TestType>*>(values_buffer.data());
    std::copy(values.cbegin(), values.cend(), values_parquet);
    for (int i = 0; i < num_chunks; i++) {
      auto row_group_writer = file_writer->AppendRowGroup(chunk_size);
      auto column_writer =
          static_cast<ParquetWriter<TestType>*>(row_group_writer->NextColumn());
      ParquetCDataType<TestType>* data = values_parquet + i * chunk_size;
      column_writer->WriteBatch(chunk_size, nullptr, nullptr, data);
      column_writer->Close();
      row_group_writer->Close();
    }
    file_writer->Close();
    *file_reader = this->ReaderFromSink();
  }

  void CheckSingleColumnRequiredTableRead(int num_chunks) {
    std::vector<T> values(SMALL_SIZE, test_traits<TestType>::value);
    std::unique_ptr<ParquetFileReader> file_reader;
    ASSERT_NO_THROW(MakeTestFile(values, num_chunks, &file_reader));

    std::shared_ptr<Table> out;
    this->ReadTableFromFile(std::move(file_reader), &out);
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(SMALL_SIZE, out->num_rows());

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
    ASSERT_EQ(1, chunked_array->num_chunks());
    ExpectArray<TestType>(values.data(), chunked_array->chunk(0).get());
  }

  void CheckSingleColumnRequiredRead(int num_chunks) {
    std::vector<T> values(SMALL_SIZE, test_traits<TestType>::value);
    std::unique_ptr<ParquetFileReader> file_reader;
    ASSERT_NO_THROW(MakeTestFile(values, num_chunks, &file_reader));

    std::shared_ptr<Array> out;
    this->ReadSingleColumnFile(std::move(file_reader), &out);

    ExpectArray<TestType>(values.data(), out.get());
  }
};

typedef ::testing::Types<BooleanType, UInt8Type, Int8Type, UInt16Type, Int16Type,
    UInt32Type, Int32Type, UInt64Type, Int64Type, FloatType,
    DoubleType> PrimitiveTestTypes;

TYPED_TEST_CASE(TestPrimitiveParquetIO, PrimitiveTestTypes);

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredRead) {
  this->CheckSingleColumnRequiredRead(1);
}

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredTableRead) {
  this->CheckSingleColumnRequiredTableRead(1);
}

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredChunkedRead) {
  this->CheckSingleColumnRequiredRead(4);
}

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredChunkedTableRead) {
  this->CheckSingleColumnRequiredTableRead(4);
}

}  // namespace parquet

}  // namespace arrow
