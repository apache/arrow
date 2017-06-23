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

#ifdef _MSC_VER
#pragma warning(push)
// Disable forcing value to bool warnings
#pragma warning(disable : 4800)
#endif

#include "gtest/gtest.h"

#include <sstream>

#include "parquet/api/reader.h"
#include "parquet/api/writer.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/test-util.h"
#include "parquet/arrow/writer.h"

#include "parquet/file/writer.h"

#include "arrow/api.h"
#include "arrow/test-util.h"

using arrow::Array;
using arrow::Buffer;
using arrow::Column;
using arrow::ChunkedArray;
using arrow::default_memory_pool;
using arrow::io::BufferReader;
using arrow::ListArray;
using arrow::PoolBuffer;
using arrow::PrimitiveArray;
using arrow::Status;
using arrow::Table;
using arrow::TimeUnit;
using arrow::ArrayVisitor;

using ArrowId = ::arrow::Type;
using ParquetType = parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::NodePtr;
using parquet::schema::PrimitiveNode;
using parquet::arrow::FromParquetSchema;

namespace parquet {
namespace arrow {

const int SMALL_SIZE = 100;
const int LARGE_SIZE = 10000;

constexpr uint32_t kDefaultSeed = 0;

LogicalType::type get_logical_type(const ::arrow::DataType& type) {
  switch (type.id()) {
    case ArrowId::UINT8:
      return LogicalType::UINT_8;
    case ArrowId::INT8:
      return LogicalType::INT_8;
    case ArrowId::UINT16:
      return LogicalType::UINT_16;
    case ArrowId::INT16:
      return LogicalType::INT_16;
    case ArrowId::UINT32:
      return LogicalType::UINT_32;
    case ArrowId::INT32:
      return LogicalType::INT_32;
    case ArrowId::UINT64:
      return LogicalType::UINT_64;
    case ArrowId::INT64:
      return LogicalType::INT_64;
    case ArrowId::STRING:
      return LogicalType::UTF8;
    case ArrowId::DATE32:
      return LogicalType::DATE;
    case ArrowId::DATE64:
      return LogicalType::DATE;
    case ArrowId::TIMESTAMP: {
      const auto& ts_type = static_cast<const ::arrow::TimestampType&>(type);
      switch (ts_type.unit()) {
        case TimeUnit::MILLI:
          return LogicalType::TIMESTAMP_MILLIS;
        case TimeUnit::MICRO:
          return LogicalType::TIMESTAMP_MICROS;
        default:
          DCHECK(false) << "Only MILLI and MICRO units supported for Arrow timestamps "
                           "with Parquet.";
      }
    }
    case ArrowId::TIME32:
      return LogicalType::TIME_MILLIS;
    case ArrowId::TIME64:
      return LogicalType::TIME_MICROS;
    default:
      break;
  }
  return LogicalType::NONE;
}

ParquetType::type get_physical_type(const ::arrow::DataType& type) {
  switch (type.id()) {
    case ArrowId::BOOL:
      return ParquetType::BOOLEAN;
    case ArrowId::UINT8:
    case ArrowId::INT8:
    case ArrowId::UINT16:
    case ArrowId::INT16:
    case ArrowId::UINT32:
    case ArrowId::INT32:
      return ParquetType::INT32;
    case ArrowId::UINT64:
    case ArrowId::INT64:
      return ParquetType::INT64;
    case ArrowId::FLOAT:
      return ParquetType::FLOAT;
    case ArrowId::DOUBLE:
      return ParquetType::DOUBLE;
    case ArrowId::BINARY:
      return ParquetType::BYTE_ARRAY;
    case ArrowId::STRING:
      return ParquetType::BYTE_ARRAY;
    case ArrowId::FIXED_SIZE_BINARY:
      return ParquetType::FIXED_LEN_BYTE_ARRAY;
    case ArrowId::DATE32:
      return ParquetType::INT32;
    case ArrowId::DATE64:
      // Convert to date32 internally
      return ParquetType::INT32;
    case ArrowId::TIME32:
      return ParquetType::INT32;
    case ArrowId::TIME64:
      return ParquetType::INT64;
    case ArrowId::TIMESTAMP:
      return ParquetType::INT64;
    default:
      break;
  }
  DCHECK(false) << "cannot reach this code";
  return ParquetType::INT32;
}

template <typename TestType>
struct test_traits {};

template <>
struct test_traits<::arrow::BooleanType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::BOOLEAN;
  static uint8_t const value;
};

const uint8_t test_traits<::arrow::BooleanType>::value(1);

template <>
struct test_traits<::arrow::UInt8Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static uint8_t const value;
};

const uint8_t test_traits<::arrow::UInt8Type>::value(64);

template <>
struct test_traits<::arrow::Int8Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static int8_t const value;
};

const int8_t test_traits<::arrow::Int8Type>::value(-64);

template <>
struct test_traits<::arrow::UInt16Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static uint16_t const value;
};

const uint16_t test_traits<::arrow::UInt16Type>::value(1024);

template <>
struct test_traits<::arrow::Int16Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static int16_t const value;
};

const int16_t test_traits<::arrow::Int16Type>::value(-1024);

template <>
struct test_traits<::arrow::UInt32Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static uint32_t const value;
};

const uint32_t test_traits<::arrow::UInt32Type>::value(1024);

template <>
struct test_traits<::arrow::Int32Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static int32_t const value;
};

const int32_t test_traits<::arrow::Int32Type>::value(-1024);

template <>
struct test_traits<::arrow::UInt64Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT64;
  static uint64_t const value;
};

const uint64_t test_traits<::arrow::UInt64Type>::value(1024);

template <>
struct test_traits<::arrow::Int64Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT64;
  static int64_t const value;
};

const int64_t test_traits<::arrow::Int64Type>::value(-1024);

template <>
struct test_traits<::arrow::TimestampType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT64;
  static int64_t const value;
};

const int64_t test_traits<::arrow::TimestampType>::value(14695634030000);

template <>
struct test_traits<::arrow::Date32Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static int32_t const value;
};

const int32_t test_traits<::arrow::Date32Type>::value(170000);

template <>
struct test_traits<::arrow::FloatType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::FLOAT;
  static float const value;
};

const float test_traits<::arrow::FloatType>::value(2.1f);

template <>
struct test_traits<::arrow::DoubleType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::DOUBLE;
  static double const value;
};

const double test_traits<::arrow::DoubleType>::value(4.2);

template <>
struct test_traits<::arrow::StringType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::BYTE_ARRAY;
  static std::string const value;
};

template <>
struct test_traits<::arrow::BinaryType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::BYTE_ARRAY;
  static std::string const value;
};

template <>
struct test_traits<::arrow::FixedSizeBinaryType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::FIXED_LEN_BYTE_ARRAY;
  static std::string const value;
};

const std::string test_traits<::arrow::StringType>::value("Test");
const std::string test_traits<::arrow::BinaryType>::value("\x00\x01\x02\x03");
const std::string test_traits<::arrow::FixedSizeBinaryType>::value("Fixed");
template <typename T>
using ParquetDataType = DataType<test_traits<T>::parquet_enum>;

template <typename T>
using ParquetWriter = TypedColumnWriter<ParquetDataType<T>>;

void WriteTableToBuffer(const std::shared_ptr<Table>& table, int num_threads,
    int64_t row_group_size, std::shared_ptr<Buffer>* out) {
  auto sink = std::make_shared<InMemoryOutputStream>();

  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), sink,
      row_group_size, default_writer_properties()));
  *out = sink->GetBuffer();
}

void DoSimpleRoundtrip(const std::shared_ptr<Table>& table, int num_threads,
    int64_t row_group_size, const std::vector<int>& column_subset,
    std::shared_ptr<Table>* out) {
  std::shared_ptr<Buffer> buffer;
  WriteTableToBuffer(table, num_threads, row_group_size, &buffer);

  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(
      OpenFile(std::make_shared<BufferReader>(buffer), ::arrow::default_memory_pool(),
          ::parquet::default_reader_properties(), nullptr, &reader));

  reader->set_num_threads(num_threads);

  if (column_subset.size() > 0) {
    ASSERT_OK_NO_THROW(reader->ReadTable(column_subset, out));
  } else {
    // Read everything
    ASSERT_OK_NO_THROW(reader->ReadTable(out));
  }
}

static std::shared_ptr<GroupNode> MakeSimpleSchema(
    const ::arrow::DataType& type, Repetition::type repetition) {
  int byte_width;
  // Decimal is not implemented yet.
  switch (type.id()) {
    case ::arrow::Type::FIXED_SIZE_BINARY:
      byte_width = static_cast<const ::arrow::FixedSizeBinaryType&>(type).byte_width();
      break;
    default:
      byte_width = -1;
  }
  auto pnode = PrimitiveNode::Make(
      "column1", repetition, get_physical_type(type), get_logical_type(type), byte_width);
  NodePtr node_ =
      GroupNode::Make("schema", Repetition::REQUIRED, std::vector<NodePtr>({pnode}));
  return std::static_pointer_cast<GroupNode>(node_);
}

template <typename TestType>
class TestParquetIO : public ::testing::Test {
 public:
  virtual void SetUp() {}

  std::unique_ptr<ParquetFileWriter> MakeWriter(
      const std::shared_ptr<GroupNode>& schema) {
    sink_ = std::make_shared<InMemoryOutputStream>();
    return ParquetFileWriter::Open(sink_, schema);
  }

  void ReaderFromSink(std::unique_ptr<FileReader>* out) {
    std::shared_ptr<Buffer> buffer = sink_->GetBuffer();
    ASSERT_OK_NO_THROW(
        OpenFile(std::make_shared<BufferReader>(buffer), ::arrow::default_memory_pool(),
            ::parquet::default_reader_properties(), nullptr, out));
  }

  void ReadSingleColumnFile(
      std::unique_ptr<FileReader> file_reader, std::shared_ptr<Array>* out) {
    std::unique_ptr<ColumnReader> column_reader;
    ASSERT_OK_NO_THROW(file_reader->GetColumn(0, &column_reader));
    ASSERT_NE(nullptr, column_reader.get());

    ASSERT_OK(column_reader->NextBatch(SMALL_SIZE, out));
    ASSERT_NE(nullptr, out->get());
  }

  void ReadAndCheckSingleColumnFile(::arrow::Array* values) {
    std::shared_ptr<::arrow::Array> out;

    std::unique_ptr<FileReader> reader;
    ReaderFromSink(&reader);
    ReadSingleColumnFile(std::move(reader), &out);
    ASSERT_TRUE(values->Equals(out));
  }

  void ReadTableFromFile(
      std::unique_ptr<FileReader> reader, std::shared_ptr<Table>* out) {
    ASSERT_OK_NO_THROW(reader->ReadTable(out));
    auto key_value_metadata =
        reader->parquet_reader()->metadata()->key_value_metadata().get();
    ASSERT_EQ(nullptr, key_value_metadata);
    ASSERT_NE(nullptr, out->get());
  }

  void PrepareListTable(int64_t size, bool nullable_lists, bool nullable_elements,
      int64_t null_count, std::shared_ptr<Table>* out) {
    std::shared_ptr<Array> values;
    ASSERT_OK(NullableArray<TestType>(
        size * size, nullable_elements ? null_count : 0, kDefaultSeed, &values));
    // Also test that slice offsets are respected
    values = values->Slice(5, values->length() - 5);
    std::shared_ptr<ListArray> lists;
    ASSERT_OK(MakeListArray(
        values, size, nullable_lists ? null_count : 0, nullable_elements, &lists));
    *out = MakeSimpleTable(lists->Slice(3, size - 6), nullable_lists);
  }

  void PrepareListOfListTable(int64_t size, bool nullable_parent_lists,
      bool nullable_lists, bool nullable_elements, int64_t null_count,
      std::shared_ptr<Table>* out) {
    std::shared_ptr<Array> values;
    ASSERT_OK(NullableArray<TestType>(
        size * 6, nullable_elements ? null_count : 0, kDefaultSeed, &values));
    std::shared_ptr<ListArray> lists;
    ASSERT_OK(MakeListArray(
        values, size * 3, nullable_lists ? null_count : 0, nullable_elements, &lists));
    std::shared_ptr<ListArray> parent_lists;
    ASSERT_OK(MakeListArray(lists, size, nullable_parent_lists ? null_count : 0,
        nullable_lists, &parent_lists));
    *out = MakeSimpleTable(parent_lists, nullable_parent_lists);
  }

  void ReadAndCheckSingleColumnTable(const std::shared_ptr<::arrow::Array>& values) {
    std::shared_ptr<::arrow::Table> out;
    std::unique_ptr<FileReader> reader;
    ReaderFromSink(&reader);
    ReadTableFromFile(std::move(reader), &out);
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(values->length(), out->num_rows());

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
    ASSERT_EQ(1, chunked_array->num_chunks());
    ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
  }

  void CheckRoundTrip(const std::shared_ptr<Table>& table) {
    std::shared_ptr<Table> result;
    DoSimpleRoundtrip(table, 1, table->num_rows(), {}, &result);
    ASSERT_TRUE(table->Equals(*result));
  }

  template <typename ArrayType>
  void WriteColumn(const std::shared_ptr<GroupNode>& schema,
      const std::shared_ptr<ArrayType>& values) {
    FileWriter writer(::arrow::default_memory_pool(), MakeWriter(schema));
    ASSERT_OK_NO_THROW(writer.NewRowGroup(values->length()));
    ASSERT_OK_NO_THROW(writer.WriteColumnChunk(*values));
    ASSERT_OK_NO_THROW(writer.Close());
  }

  std::shared_ptr<InMemoryOutputStream> sink_;
};

// We have separate tests for UInt32Type as this is currently the only type
// where a roundtrip does not yield the identical Array structure.
// There we write an UInt32 Array but receive an Int64 Array as result for
// Parquet version 1.0.

typedef ::testing::Types<::arrow::BooleanType, ::arrow::UInt8Type, ::arrow::Int8Type,
    ::arrow::UInt16Type, ::arrow::Int16Type, ::arrow::Int32Type, ::arrow::UInt64Type,
    ::arrow::Int64Type, ::arrow::Date32Type, ::arrow::FloatType, ::arrow::DoubleType,
    ::arrow::StringType, ::arrow::BinaryType, ::arrow::FixedSizeBinaryType>
    TestTypes;

TYPED_TEST_CASE(TestParquetIO, TestTypes);

TYPED_TEST(TestParquetIO, SingleColumnRequiredWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));

  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::REQUIRED);
  this->WriteColumn(schema, values);

  this->ReadAndCheckSingleColumnFile(values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
      values->length(), default_writer_properties()));

  std::shared_ptr<Table> out;
  std::unique_ptr<FileReader> reader;
  this->ReaderFromSink(&reader);
  this->ReadTableFromFile(std::move(reader), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(100, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalReadWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(SMALL_SIZE, 10, kDefaultSeed, &values));

  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::OPTIONAL);
  this->WriteColumn(schema, values);

  this->ReadAndCheckSingleColumnFile(values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnRequiredSliceWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(2 * SMALL_SIZE, &values));
  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::REQUIRED);

  std::shared_ptr<Array> sliced_values = values->Slice(SMALL_SIZE / 2, SMALL_SIZE);
  this->WriteColumn(schema, sliced_values);
  this->ReadAndCheckSingleColumnFile(sliced_values.get());

  // Slice offset 1 higher
  sliced_values = values->Slice(SMALL_SIZE / 2 + 1, SMALL_SIZE);
  this->WriteColumn(schema, sliced_values);
  this->ReadAndCheckSingleColumnFile(sliced_values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalSliceWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NullableArray<TypeParam>(2 * SMALL_SIZE, SMALL_SIZE, kDefaultSeed, &values));
  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::OPTIONAL);

  std::shared_ptr<Array> sliced_values = values->Slice(SMALL_SIZE / 2, SMALL_SIZE);
  this->WriteColumn(schema, sliced_values);
  this->ReadAndCheckSingleColumnFile(sliced_values.get());

  // Slice offset 1 higher, thus different null bitmap.
  sliced_values = values->Slice(SMALL_SIZE / 2 + 1, SMALL_SIZE);
  this->WriteColumn(schema, sliced_values);
  this->ReadAndCheckSingleColumnFile(sliced_values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnTableOptionalReadWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(SMALL_SIZE, 10, kDefaultSeed, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  this->CheckRoundTrip(table);
}

TYPED_TEST(TestParquetIO, SingleNullableListNullableColumnReadWrite) {
  std::shared_ptr<Table> table;
  this->PrepareListTable(SMALL_SIZE, true, true, 10, &table);
  this->CheckRoundTrip(table);
}

TYPED_TEST(TestParquetIO, SingleRequiredListNullableColumnReadWrite) {
  std::shared_ptr<Table> table;
  this->PrepareListTable(SMALL_SIZE, false, true, 10, &table);
  this->CheckRoundTrip(table);
}

TYPED_TEST(TestParquetIO, SingleNullableListRequiredColumnReadWrite) {
  std::shared_ptr<Table> table;
  this->PrepareListTable(SMALL_SIZE, true, false, 10, &table);
  this->CheckRoundTrip(table);
}

TYPED_TEST(TestParquetIO, SingleRequiredListRequiredColumnReadWrite) {
  std::shared_ptr<Table> table;
  this->PrepareListTable(SMALL_SIZE, false, false, 0, &table);
  this->CheckRoundTrip(table);
}

TYPED_TEST(TestParquetIO, SingleNullableListRequiredListRequiredColumnReadWrite) {
  std::shared_ptr<Table> table;
  this->PrepareListOfListTable(SMALL_SIZE, true, false, false, 0, &table);
  this->CheckRoundTrip(table);
}

TYPED_TEST(TestParquetIO, SingleColumnRequiredChunkedWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));
  int64_t chunk_size = values->length() / 4;

  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::REQUIRED);
  FileWriter writer(default_memory_pool(), this->MakeWriter(schema));
  for (int i = 0; i < 4; i++) {
    ASSERT_OK_NO_THROW(writer.NewRowGroup(chunk_size));
    std::shared_ptr<Array> sliced_array = values->Slice(i * chunk_size, chunk_size);
    ASSERT_OK_NO_THROW(writer.WriteColumnChunk(*sliced_array));
  }
  ASSERT_OK_NO_THROW(writer.Close());

  this->ReadAndCheckSingleColumnFile(values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredChunkedWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(LARGE_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(
      *table, default_memory_pool(), this->sink_, 512, default_writer_properties()));

  this->ReadAndCheckSingleColumnTable(values);
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredChunkedWriteArrowIO) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(LARGE_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  auto buffer = std::make_shared<::arrow::PoolBuffer>();

  {
    // BufferOutputStream closed on gc
    auto arrow_sink_ = std::make_shared<::arrow::io::BufferOutputStream>(buffer);
    ASSERT_OK_NO_THROW(WriteTable(
        *table, default_memory_pool(), arrow_sink_, 512, default_writer_properties()));

    // XXX: Remove this after ARROW-455 completed
    ASSERT_OK(arrow_sink_->Close());
  }

  auto pbuffer = std::make_shared<Buffer>(buffer->data(), buffer->size());

  auto source = std::make_shared<BufferReader>(pbuffer);
  std::shared_ptr<::arrow::Table> out;
  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(OpenFile(source, ::arrow::default_memory_pool(), &reader));
  this->ReadTableFromFile(std::move(reader), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(values->length(), out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalChunkedWrite) {
  int64_t chunk_size = SMALL_SIZE / 4;
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(SMALL_SIZE, 10, kDefaultSeed, &values));

  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::OPTIONAL);
  FileWriter writer(::arrow::default_memory_pool(), this->MakeWriter(schema));
  for (int i = 0; i < 4; i++) {
    ASSERT_OK_NO_THROW(writer.NewRowGroup(chunk_size));
    std::shared_ptr<Array> sliced_array = values->Slice(i * chunk_size, chunk_size);
    ASSERT_OK_NO_THROW(writer.WriteColumnChunk(*sliced_array));
  }
  ASSERT_OK_NO_THROW(writer.Close());

  this->ReadAndCheckSingleColumnFile(values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnTableOptionalChunkedWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(LARGE_SIZE, 100, kDefaultSeed, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_, 512,
      default_writer_properties()));

  this->ReadAndCheckSingleColumnTable(values);
}

using TestInt96ParquetIO = TestParquetIO<::arrow::TimestampType>;

TEST_F(TestInt96ParquetIO, ReadIntoTimestamp) {
  // This test explicitly tests the conversion from an Impala-style timestamp
  // to a nanoseconds-since-epoch one.

  // 2nd January 1970, 11:35min 145738543ns
  Int96 day;
  day.value[2] = UINT32_C(2440589);
  int64_t seconds = (11 * 60 + 35) * 60;
  *(reinterpret_cast<int64_t*>(&(day.value))) =
      seconds * INT64_C(1000) * INT64_C(1000) * INT64_C(1000) + 145738543;
  // Compute the corresponding nanosecond timestamp
  struct tm datetime = {0};
  datetime.tm_year = 70;
  datetime.tm_mon = 0;
  datetime.tm_mday = 2;
  datetime.tm_hour = 11;
  datetime.tm_min = 35;
  struct tm epoch = {0};
  epoch.tm_year = 70;
  epoch.tm_mday = 1;
  // Nanoseconds since the epoch
  int64_t val = lrint(difftime(mktime(&datetime), mktime(&epoch))) * INT64_C(1000000000);
  val += 145738543;

  std::vector<std::shared_ptr<schema::Node>> fields(
      {schema::PrimitiveNode::Make("int96", Repetition::REQUIRED, ParquetType::INT96)});
  std::shared_ptr<schema::GroupNode> schema = std::static_pointer_cast<GroupNode>(
      schema::GroupNode::Make("schema", Repetition::REQUIRED, fields));

  // We cannot write this column with Arrow, so we have to use the plain parquet-cpp API
  // to write an Int96 file.
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  auto writer = ParquetFileWriter::Open(this->sink_, schema);
  RowGroupWriter* rg_writer = writer->AppendRowGroup(1);
  ColumnWriter* c_writer = rg_writer->NextColumn();
  auto typed_writer = dynamic_cast<TypedColumnWriter<Int96Type>*>(c_writer);
  ASSERT_NE(typed_writer, nullptr);
  typed_writer->WriteBatch(1, nullptr, nullptr, &day);
  c_writer->Close();
  rg_writer->Close();
  writer->Close();

  ::arrow::TimestampBuilder builder(
      default_memory_pool(), ::arrow::timestamp(TimeUnit::NANO));
  builder.Append(val);
  std::shared_ptr<Array> values;
  ASSERT_OK(builder.Finish(&values));
  this->ReadAndCheckSingleColumnFile(values.get());
}

using TestUInt32ParquetIO = TestParquetIO<::arrow::UInt32Type>;

TEST_F(TestUInt32ParquetIO, Parquet_2_0_Compability) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<::arrow::UInt32Type>(LARGE_SIZE, 100, kDefaultSeed, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);

  // Parquet 2.0 roundtrip should yield an uint32_t column again
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  std::shared_ptr<::parquet::WriterProperties> properties =
      ::parquet::WriterProperties::Builder()
          .version(ParquetVersion::PARQUET_2_0)
          ->build();
  ASSERT_OK_NO_THROW(
      WriteTable(*table, default_memory_pool(), this->sink_, 512, properties));
  this->ReadAndCheckSingleColumnTable(values);
}

TEST_F(TestUInt32ParquetIO, Parquet_1_0_Compability) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> arr;
  ASSERT_OK(NullableArray<::arrow::UInt32Type>(LARGE_SIZE, 100, kDefaultSeed, &arr));

  std::shared_ptr<::arrow::UInt32Array> values =
      std::dynamic_pointer_cast<::arrow::UInt32Array>(arr);

  std::shared_ptr<Table> table = MakeSimpleTable(values, true);

  // Parquet 1.0 returns an int64_t column as there is no way to tell a Parquet 1.0
  // reader that a column is unsigned.
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  std::shared_ptr<::parquet::WriterProperties> properties =
      ::parquet::WriterProperties::Builder()
          .version(ParquetVersion::PARQUET_1_0)
          ->build();
  ASSERT_OK_NO_THROW(
      WriteTable(*table, ::arrow::default_memory_pool(), this->sink_, 512, properties));

  std::shared_ptr<Array> expected_values;
  std::shared_ptr<PoolBuffer> int64_data =
      std::make_shared<PoolBuffer>(::arrow::default_memory_pool());
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

  const int32_t kOffset = 0;
  ASSERT_OK(MakePrimitiveArray(std::make_shared<::arrow::Int64Type>(), values->length(),
      int64_data, values->null_bitmap(), values->null_count(), kOffset,
      &expected_values));
  this->ReadAndCheckSingleColumnTable(expected_values);
}

using TestStringParquetIO = TestParquetIO<::arrow::StringType>;

TEST_F(TestStringParquetIO, EmptyStringColumnRequiredWrite) {
  std::shared_ptr<Array> values;
  ::arrow::StringBuilder builder(::arrow::default_memory_pool());
  for (size_t i = 0; i < SMALL_SIZE; i++) {
    builder.Append("");
  }
  ASSERT_OK(builder.Finish(&values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
      values->length(), default_writer_properties()));

  std::shared_ptr<Table> out;
  std::unique_ptr<FileReader> reader;
  this->ReaderFromSink(&reader);
  this->ReadTableFromFile(std::move(reader), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(100, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
}

using TestNullParquetIO = TestParquetIO<::arrow::NullType>;

TEST_F(TestNullParquetIO, NullColumn) {
  std::shared_ptr<Array> values = std::make_shared<::arrow::NullArray>(SMALL_SIZE);
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
      values->length(), default_writer_properties()));

  std::shared_ptr<Table> out;
  std::unique_ptr<FileReader> reader;
  this->ReaderFromSink(&reader);
  this->ReadTableFromFile(std::move(reader), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(100, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
}

template <typename T>
using ParquetCDataType = typename ParquetDataType<T>::c_type;

template <typename T>
struct c_type_trait {
  using ArrowCType = typename T::c_type;
};

template <>
struct c_type_trait<::arrow::BooleanType> {
  using ArrowCType = uint8_t;
};

template <typename TestType>
class TestPrimitiveParquetIO : public TestParquetIO<TestType> {
 public:
  typedef typename c_type_trait<TestType>::ArrowCType T;

  void MakeTestFile(
      std::vector<T>& values, int num_chunks, std::unique_ptr<FileReader>* reader) {
    TestType dummy;

    std::shared_ptr<GroupNode> schema = MakeSimpleSchema(dummy, Repetition::REQUIRED);
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
    this->ReaderFromSink(reader);
  }

  void CheckSingleColumnRequiredTableRead(int num_chunks) {
    std::vector<T> values(SMALL_SIZE, test_traits<TestType>::value);
    std::unique_ptr<FileReader> file_reader;
    ASSERT_NO_THROW(MakeTestFile(values, num_chunks, &file_reader));

    std::shared_ptr<Table> out;
    this->ReadTableFromFile(std::move(file_reader), &out);
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(SMALL_SIZE, out->num_rows());

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
    ASSERT_EQ(1, chunked_array->num_chunks());
    ExpectArrayT<TestType>(values.data(), chunked_array->chunk(0).get());
  }

  void CheckSingleColumnRequiredRead(int num_chunks) {
    std::vector<T> values(SMALL_SIZE, test_traits<TestType>::value);
    std::unique_ptr<FileReader> file_reader;
    ASSERT_NO_THROW(MakeTestFile(values, num_chunks, &file_reader));

    std::shared_ptr<Array> out;
    this->ReadSingleColumnFile(std::move(file_reader), &out);

    ExpectArrayT<TestType>(values.data(), out.get());
  }
};

typedef ::testing::Types<::arrow::BooleanType, ::arrow::UInt8Type, ::arrow::Int8Type,
    ::arrow::UInt16Type, ::arrow::Int16Type, ::arrow::UInt32Type, ::arrow::Int32Type,
    ::arrow::UInt64Type, ::arrow::Int64Type, ::arrow::FloatType, ::arrow::DoubleType>
    PrimitiveTestTypes;

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

void MakeDateTimeTypesTable(std::shared_ptr<Table>* out) {
  using ::arrow::ArrayFromVector;

  std::vector<bool> is_valid = {true, true, true, false, true, true};

  // These are only types that roundtrip without modification
  auto f0 = field("f0", ::arrow::date32());
  auto f1 = field("f1", ::arrow::timestamp(TimeUnit::MILLI));
  auto f2 = field("f2", ::arrow::timestamp(TimeUnit::MICRO));
  auto f3 = field("f3", ::arrow::time32(TimeUnit::MILLI));
  auto f4 = field("f4", ::arrow::time64(TimeUnit::MICRO));
  std::shared_ptr<::arrow::Schema> schema(new ::arrow::Schema({f0, f1, f2, f3, f4}));

  std::vector<int32_t> t32_values = {
      1489269000, 1489270000, 1489271000, 1489272000, 1489272000, 1489273000};
  std::vector<int64_t> t64_values = {1489269000000, 1489270000000, 1489271000000,
      1489272000000, 1489272000000, 1489273000000};

  std::shared_ptr<Array> a0, a1, a2, a3, a4;
  ArrayFromVector<::arrow::Date32Type, int32_t>(f0->type(), is_valid, t32_values, &a0);
  ArrayFromVector<::arrow::TimestampType, int64_t>(f1->type(), is_valid, t64_values, &a1);
  ArrayFromVector<::arrow::TimestampType, int64_t>(f2->type(), is_valid, t64_values, &a2);
  ArrayFromVector<::arrow::Time32Type, int32_t>(f3->type(), is_valid, t32_values, &a3);
  ArrayFromVector<::arrow::Time64Type, int64_t>(f4->type(), is_valid, t64_values, &a4);

  std::vector<std::shared_ptr<::arrow::Column>> columns = {
      std::make_shared<Column>("f0", a0), std::make_shared<Column>("f1", a1),
      std::make_shared<Column>("f2", a2), std::make_shared<Column>("f3", a3),
      std::make_shared<Column>("f4", a4)};
  *out = std::make_shared<::arrow::Table>(schema, columns);
}

TEST(TestArrowReadWrite, DateTimeTypes) {
  std::shared_ptr<Table> table;
  MakeDateTimeTypesTable(&table);

  std::shared_ptr<Table> result;
  DoSimpleRoundtrip(table, 1, table->num_rows(), {}, &result);

  ASSERT_TRUE(table->Equals(*result));
}

TEST(TestArrowReadWrite, ConvertedDateTimeTypes) {
  using ::arrow::ArrayFromVector;

  std::vector<bool> is_valid = {true, true, true, false, true, true};

  auto f0 = field("f0", ::arrow::date64());
  auto f1 = field("f1", ::arrow::time32(TimeUnit::SECOND));
  std::shared_ptr<::arrow::Schema> schema(new ::arrow::Schema({f0, f1}));

  std::vector<int64_t> a0_values = {1489190400000, 1489276800000, 1489363200000,
      1489449600000, 1489536000000, 1489622400000};
  std::vector<int32_t> a1_values = {0, 1, 2, 3, 4, 5};

  std::shared_ptr<Array> a0, a1, x0, x1;
  ArrayFromVector<::arrow::Date64Type, int64_t>(f0->type(), is_valid, a0_values, &a0);
  ArrayFromVector<::arrow::Time32Type, int32_t>(f1->type(), is_valid, a1_values, &a1);

  std::vector<std::shared_ptr<::arrow::Column>> columns = {
      std::make_shared<Column>("f0", a0), std::make_shared<Column>("f1", a1)};
  auto table = std::make_shared<::arrow::Table>(schema, columns);

  // Expected schema and values
  auto e0 = field("f0", ::arrow::date32());
  auto e1 = field("f1", ::arrow::time32(TimeUnit::MILLI));
  std::shared_ptr<::arrow::Schema> ex_schema(new ::arrow::Schema({e0, e1}));

  std::vector<int32_t> x0_values = {17236, 17237, 17238, 17239, 17240, 17241};
  std::vector<int32_t> x1_values = {0, 1000, 2000, 3000, 4000, 5000};
  ArrayFromVector<::arrow::Date32Type, int32_t>(e0->type(), is_valid, x0_values, &x0);
  ArrayFromVector<::arrow::Time32Type, int32_t>(e1->type(), is_valid, x1_values, &x1);

  std::vector<std::shared_ptr<::arrow::Column>> ex_columns = {
      std::make_shared<Column>("f0", x0), std::make_shared<Column>("f1", x1)};
  auto ex_table = std::make_shared<::arrow::Table>(ex_schema, ex_columns);

  std::shared_ptr<Table> result;
  DoSimpleRoundtrip(table, 1, table->num_rows(), {}, &result);

  ASSERT_TRUE(result->Equals(*ex_table));
}

void MakeDoubleTable(
    int num_columns, int num_rows, int nchunks, std::shared_ptr<Table>* out) {
  std::shared_ptr<::arrow::Column> column;
  std::vector<std::shared_ptr<::arrow::Column>> columns(num_columns);
  std::vector<std::shared_ptr<::arrow::Field>> fields(num_columns);

  for (int i = 0; i < num_columns; ++i) {
    std::vector<std::shared_ptr<Array>> arrays;
    std::shared_ptr<Array> values;
    ASSERT_OK(NullableArray<::arrow::DoubleType>(
        num_rows, num_rows / 10, static_cast<uint32_t>(i), &values));
    std::stringstream ss;
    ss << "col" << i;

    for (int j = 0; j < nchunks; ++j) {
      arrays.push_back(values);
    }
    column = MakeColumn(ss.str(), arrays, true);

    columns[i] = column;
    fields[i] = column->field();
  }
  auto schema = std::make_shared<::arrow::Schema>(fields);
  *out = std::make_shared<Table>(schema, columns);
}

TEST(TestArrowReadWrite, MultithreadedRead) {
  const int num_columns = 20;
  const int num_rows = 1000;
  const int num_threads = 4;

  std::shared_ptr<Table> table;
  MakeDoubleTable(num_columns, num_rows, 1, &table);

  std::shared_ptr<Table> result;
  DoSimpleRoundtrip(table, num_threads, table->num_rows(), {}, &result);

  ASSERT_TRUE(table->Equals(*result));
}

TEST(TestArrowReadWrite, ReadSingleRowGroup) {
  const int num_columns = 20;
  const int num_rows = 1000;

  std::shared_ptr<Table> table;
  MakeDoubleTable(num_columns, num_rows, 1, &table);

  std::shared_ptr<Buffer> buffer;
  WriteTableToBuffer(table, 1, num_rows / 2, &buffer);

  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(
      OpenFile(std::make_shared<BufferReader>(buffer), ::arrow::default_memory_pool(),
          ::parquet::default_reader_properties(), nullptr, &reader));

  ASSERT_EQ(2, reader->num_row_groups());

  std::shared_ptr<Table> r1, r2;
  // Read everything
  ASSERT_OK_NO_THROW(reader->ReadRowGroup(0, &r1));
  ASSERT_OK_NO_THROW(reader->ReadRowGroup(1, &r2));

  std::shared_ptr<Table> concatenated;
  ASSERT_OK(ConcatenateTables({r1, r2}, &concatenated));

  ASSERT_TRUE(table->Equals(*concatenated));
}

TEST(TestArrowReadWrite, ReadColumnSubset) {
  const int num_columns = 20;
  const int num_rows = 1000;
  const int num_threads = 4;

  std::shared_ptr<Table> table;
  MakeDoubleTable(num_columns, num_rows, 1, &table);

  std::shared_ptr<Table> result;
  std::vector<int> column_subset = {0, 4, 8, 10};
  DoSimpleRoundtrip(table, num_threads, table->num_rows(), column_subset, &result);

  std::vector<std::shared_ptr<::arrow::Column>> ex_columns;
  std::vector<std::shared_ptr<::arrow::Field>> ex_fields;
  for (int i : column_subset) {
    ex_columns.push_back(table->column(i));
    ex_fields.push_back(table->column(i)->field());
  }

  auto ex_schema = std::make_shared<::arrow::Schema>(ex_fields);
  Table expected(ex_schema, ex_columns);
  ASSERT_TRUE(result->Equals(expected));
}

TEST(TestArrowWrite, CheckChunkSize) {
  const int num_columns = 2;
  const int num_rows = 128;
  const int64_t chunk_size = 0;  // note the chunk_size is 0
  std::shared_ptr<Table> table;
  MakeDoubleTable(num_columns, num_rows, 1, &table);

  auto sink = std::make_shared<InMemoryOutputStream>();

  ASSERT_RAISES(
      Invalid, WriteTable(*table, ::arrow::default_memory_pool(), sink, chunk_size));
}

class TestNestedSchemaRead : public ::testing::TestWithParam<Repetition::type> {
 protected:
  // make it *3 to make it easily divisible by 3
  const int NUM_SIMPLE_TEST_ROWS = SMALL_SIZE * 3;
  std::shared_ptr<::arrow::Int32Array> values_array_ = nullptr;

  void InitReader() {
    std::shared_ptr<Buffer> buffer = nested_parquet_->GetBuffer();
    ASSERT_OK_NO_THROW(
        OpenFile(std::make_shared<BufferReader>(buffer), ::arrow::default_memory_pool(),
            ::parquet::default_reader_properties(), nullptr, &reader_));
  }

  void InitNewParquetFile(const std::shared_ptr<GroupNode>& schema, int num_rows) {
    nested_parquet_ = std::make_shared<InMemoryOutputStream>();
    writer_ = parquet::ParquetFileWriter::Open(
        nested_parquet_, schema, default_writer_properties());
    row_group_writer_ = writer_->AppendRowGroup(num_rows);
  }

  void FinalizeParquetFile() {
    row_group_writer_->Close();
    writer_->Close();
  }

  void MakeValues(int num_rows) {
    std::shared_ptr<Array> arr;
    ASSERT_OK(NullableArray<::arrow::Int32Type>(num_rows, 0, kDefaultSeed, &arr));
    values_array_ = std::dynamic_pointer_cast<::arrow::Int32Array>(arr);
  }

  void WriteColumnData(size_t num_rows, int16_t* def_levels,
      int16_t* rep_levels, int32_t* values) {
    auto typed_writer = static_cast<TypedColumnWriter<Int32Type>*>(
      row_group_writer_->NextColumn());
    typed_writer->WriteBatch(num_rows, def_levels, rep_levels, values);
  }

  void ValidateArray(const Array& array, size_t expected_nulls) {
    ASSERT_EQ(array.length(), values_array_->length());
    ASSERT_EQ(array.null_count(), expected_nulls);
    // Also independently count the nulls
    auto local_null_count = 0;
    for (int i = 0; i < array.length(); i++) {
      if (array.IsNull(i)) {
        local_null_count++;
      }
    }
    ASSERT_EQ(local_null_count, expected_nulls);
  }

  void ValidateColumnArray(const ::arrow::Int32Array& array,
      size_t expected_nulls) {
    ValidateArray(array, expected_nulls);

    int j = 0;
    for (int i = 0; i < values_array_->length(); i++) {
      if (array.IsNull(i)) {
        continue;
      }
      ASSERT_EQ(array.Value(i), values_array_->Value(j));
      j++;
    }
  }

  void ValidateTableArrayTypes(const Table& table) {
    for (int i = 0; i < table.num_columns(); i++) {
      const std::shared_ptr<::arrow::Field> schema_field = table.schema()->field(i);
      const std::shared_ptr<Column> column = table.column(i);
      // Compare with the column field
      ASSERT_TRUE(schema_field->Equals(column->field()));
      // Compare with the array type
      ASSERT_TRUE(schema_field->type()->Equals(column->data()->chunk(0)->type()));
    }
  }

  // A parquet with a simple nested schema
  void CreateSimpleNestedParquet(Repetition::type struct_repetition) {
    std::vector<NodePtr> parquet_fields;
    // TODO(itaiin): We are using parquet low-level file api to create the nested parquet
    // this needs to change when a nested writes are implemented

    // create the schema:
    // <struct_repetition> group group1 {
    //   required int32 leaf1;
    //   optional int32 leaf2;
    // }
    // required int32 leaf3;

    parquet_fields.push_back(GroupNode::Make("group1", struct_repetition,
        {PrimitiveNode::Make("leaf1", Repetition::REQUIRED, ParquetType::INT32),
         PrimitiveNode::Make("leaf2", Repetition::OPTIONAL, ParquetType::INT32)}));
    parquet_fields.push_back(
        PrimitiveNode::Make("leaf3", Repetition::REQUIRED, ParquetType::INT32));

    auto schema_node = GroupNode::Make("schema", Repetition::REQUIRED, parquet_fields);

    // Create definition levels for the different columns that contain interleaved
    // nulls and values at all nesting levels

    //  definition levels for optional fields
    std::vector<int16_t> leaf1_def_levels(NUM_SIMPLE_TEST_ROWS);
    std::vector<int16_t> leaf2_def_levels(NUM_SIMPLE_TEST_ROWS);
    std::vector<int16_t> leaf3_def_levels(NUM_SIMPLE_TEST_ROWS);
    for (int i = 0; i < NUM_SIMPLE_TEST_ROWS; i++)  {
      // leaf1 is required within the optional group1, so it is only null
      // when the group is null
      leaf1_def_levels[i] = (i % 3 == 0) ? 0 : 1;
      // leaf2 is optional, can be null in the primitive (def-level 1) or
      // struct level (def-level 0)
      leaf2_def_levels[i] = i % 3;
      // leaf3 is required
      leaf3_def_levels[i] = 0;
    }

    std::vector<int16_t> rep_levels(NUM_SIMPLE_TEST_ROWS, 0);

    // Produce values for the columns
    MakeValues(NUM_SIMPLE_TEST_ROWS);
    int32_t* values = reinterpret_cast<int32_t*>(values_array_->data()->mutable_data());

    // Create the actual parquet file
    InitNewParquetFile(std::static_pointer_cast<GroupNode>(schema_node),
      NUM_SIMPLE_TEST_ROWS);

    // leaf1 column
    WriteColumnData(NUM_SIMPLE_TEST_ROWS, leaf1_def_levels.data(),
      rep_levels.data(), values);
    // leaf2 column
    WriteColumnData(NUM_SIMPLE_TEST_ROWS, leaf2_def_levels.data(),
      rep_levels.data(), values);
    // leaf3 column
    WriteColumnData(NUM_SIMPLE_TEST_ROWS, leaf3_def_levels.data(),
      rep_levels.data(), values);

    FinalizeParquetFile();
    InitReader();
  }

  NodePtr CreateSingleTypedNestedGroup(int index, int depth, int num_children,
      Repetition::type node_repetition, ParquetType::type leaf_type) {
    std::vector<NodePtr> children;

    for (int i = 0; i < num_children; i++) {
      if (depth <= 1) {
        children.push_back(PrimitiveNode::Make("leaf",
          node_repetition, leaf_type));
      } else {
        children.push_back(CreateSingleTypedNestedGroup(i, depth - 1, num_children,
          node_repetition, leaf_type));
      }
    }

    std::stringstream ss;
    ss << "group-" << depth << "-" << index;
    return NodePtr(GroupNode::Make(ss.str(), node_repetition, children));
  }

  // A deeply nested schema
  void CreateMultiLevelNestedParquet(int num_trees, int tree_depth,
      int num_children, int num_rows, Repetition::type node_repetition) {
    // Create the schema
    std::vector<NodePtr> parquet_fields;
    for (int i = 0; i < num_trees; i++) {
      parquet_fields.push_back(CreateSingleTypedNestedGroup(i, tree_depth, num_children,
        node_repetition, ParquetType::INT32));
    }
    auto schema_node = GroupNode::Make("schema", Repetition::REQUIRED, parquet_fields);

    int num_columns = num_trees * static_cast<int>((std::pow(num_children, tree_depth)));

    std::vector<int16_t> def_levels(num_rows);
    std::vector<int16_t> rep_levels(num_rows);
    for (int i = 0; i < num_rows; i++) {
      if (node_repetition == Repetition::REQUIRED) {
        def_levels[i] = 0; // all is required
      } else {
        def_levels[i] = i % tree_depth; // all is optional
      }
      rep_levels[i] = 0; // none is repeated
    }

    // Produce values for the columns
    MakeValues(num_rows);
    int32_t* values = reinterpret_cast<int32_t*>(values_array_->data()->mutable_data());

    // Create the actual parquet file
    InitNewParquetFile(std::static_pointer_cast<GroupNode>(schema_node), num_rows);

    for (int i = 0; i < num_columns; i++) {
      WriteColumnData(num_rows, def_levels.data(), rep_levels.data(), values);
    }
    FinalizeParquetFile();
    InitReader();
  }

  class DeepParquetTestVisitor : public ArrayVisitor {
   public:
    DeepParquetTestVisitor(Repetition::type node_repetition,
      std::shared_ptr<::arrow::Int32Array> expected) :
      node_repetition_(node_repetition), expected_(expected) {}

    Status Validate(std::shared_ptr<Array> tree) {
      return tree->Accept(this);
    }

    virtual Status Visit(const ::arrow::Int32Array& array) {
      if (node_repetition_ == Repetition::REQUIRED) {
        if (!array.Equals(expected_)) {
          return Status::Invalid("leaf array data mismatch");
        }
      } else if (node_repetition_ == Repetition::OPTIONAL) {
        if (array.length() != expected_->length()) {
          return Status::Invalid("Bad leaf array length");
        }
        // expect only 1 value every `depth` row
        if (array.null_count() != SMALL_SIZE) {
          return Status::Invalid("Unexpected null count");
        }
      } else {
        return Status::NotImplemented("Unsupported repetition");
      }
      return Status::OK();
    }

    virtual Status Visit(const ::arrow::StructArray& array) {
      for (auto& child : array.fields()) {
        if (node_repetition_ == Repetition::REQUIRED) {
          RETURN_NOT_OK(child->Accept(this));
        } else if (node_repetition_ == Repetition::OPTIONAL) {
          // Null count Must be a multiple of SMALL_SIZE
          if (array.null_count() % SMALL_SIZE != 0) {
            return Status::Invalid("Unexpected struct null count");
          }
        } else {
          return Status::NotImplemented("Unsupported repetition");
        }
      }
      return Status::OK();
    }

   private:
    Repetition::type node_repetition_;
    std::shared_ptr<::arrow::Int32Array> expected_;
  };

  std::shared_ptr<InMemoryOutputStream> nested_parquet_;
  std::unique_ptr<FileReader> reader_;
  std::unique_ptr<ParquetFileWriter> writer_;
  RowGroupWriter* row_group_writer_;
};

TEST_F(TestNestedSchemaRead, ReadIntoTableFull) {
  CreateSimpleNestedParquet(Repetition::OPTIONAL);

  std::shared_ptr<Table> table;
  ASSERT_OK_NO_THROW(reader_->ReadTable(&table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 2);
  ASSERT_EQ(table->schema()->field(0)->type()->num_children(), 2);
  ValidateTableArrayTypes(*table);

  auto struct_field_array = std::static_pointer_cast<::arrow::StructArray>(
    table->column(0)->data()->chunk(0));
  auto leaf1_array = std::static_pointer_cast<::arrow::Int32Array>(
    struct_field_array->field(0));
  auto leaf2_array = std::static_pointer_cast<::arrow::Int32Array>(
    struct_field_array->field(1));
  auto leaf3_array = std::static_pointer_cast<::arrow::Int32Array>(
    table->column(1)->data()->chunk(0));

  // validate struct and leaf arrays

  // validate struct array
  ValidateArray(*struct_field_array, NUM_SIMPLE_TEST_ROWS / 3);
  // validate leaf1
  ValidateColumnArray(*leaf1_array, NUM_SIMPLE_TEST_ROWS / 3);
  // validate leaf2
  ValidateColumnArray(*leaf2_array, NUM_SIMPLE_TEST_ROWS * 2/ 3);
  // validate leaf3
  ValidateColumnArray(*leaf3_array, 0);
}

TEST_F(TestNestedSchemaRead, ReadTablePartial) {
  CreateSimpleNestedParquet(Repetition::OPTIONAL);
  std::shared_ptr<Table> table;

  // columns: {group1.leaf1, leaf3}
  ASSERT_OK_NO_THROW(reader_->ReadTable({0, 2}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 2);
  ASSERT_EQ(table->schema()->field(0)->name(), "group1");
  ASSERT_EQ(table->schema()->field(1)->name(), "leaf3");
  ASSERT_EQ(table->schema()->field(0)->type()->num_children(), 1);
  ValidateTableArrayTypes(*table);

  // columns: {group1.leaf1, group1.leaf2}
  ASSERT_OK_NO_THROW(reader_->ReadTable({0, 1}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 1);
  ASSERT_EQ(table->schema()->field(0)->name(), "group1");
  ASSERT_EQ(table->schema()->field(0)->type()->num_children(), 2);
  ValidateTableArrayTypes(*table);

  // columns: {leaf3}
  ASSERT_OK_NO_THROW(reader_->ReadTable({2}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 1);
  ASSERT_EQ(table->schema()->field(0)->name(), "leaf3");
  ASSERT_EQ(table->schema()->field(0)->type()->num_children(), 0);
  ValidateTableArrayTypes(*table);

  // Test with different ordering
  ASSERT_OK_NO_THROW(reader_->ReadTable({2, 0}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 2);
  ASSERT_EQ(table->schema()->field(0)->name(), "leaf3");
  ASSERT_EQ(table->schema()->field(1)->name(), "group1");
  ASSERT_EQ(table->schema()->field(1)->type()->num_children(), 1);
  ValidateTableArrayTypes(*table);
}

TEST_F(TestNestedSchemaRead, StructAndListTogetherUnsupported) {
  CreateSimpleNestedParquet(Repetition::REPEATED);
  std::shared_ptr<Table> table;
  ASSERT_RAISES(NotImplemented, reader_->ReadTable(&table));
}

TEST_P(TestNestedSchemaRead, DeepNestedSchemaRead) {
  const int num_trees = 10;
  const int depth = 5;
  const int num_children = 3;
  int num_rows = SMALL_SIZE * depth;
  CreateMultiLevelNestedParquet(num_trees, depth, num_children, num_rows, GetParam());
  std::shared_ptr<Table> table;
  ASSERT_OK_NO_THROW(reader_->ReadTable(&table));
  ASSERT_EQ(table->num_columns(), num_trees);
  ASSERT_EQ(table->num_rows(), num_rows);

  DeepParquetTestVisitor visitor(GetParam(), values_array_);
  for (int i = 0; i < table->num_columns(); i++) {
    auto tree = table->column(i)->data()->chunk(0);
    ASSERT_OK_NO_THROW(visitor.Validate(tree));
  }
}

INSTANTIATE_TEST_CASE_P(Repetition_type, TestNestedSchemaRead,
  ::testing::Values(Repetition::REQUIRED, Repetition::OPTIONAL));

TEST(TestArrowReaderAdHoc, Int96BadMemoryAccess) {
  // PARQUET-995
  const char* data_dir = std::getenv("PARQUET_TEST_DATA");
  std::string dir_string(data_dir);
  std::stringstream ss;
  ss << dir_string << "/"
     << "alltypes_plain.parquet";
  auto path = ss.str();

  auto pool = ::arrow::default_memory_pool();

  std::unique_ptr<FileReader> arrow_reader;
  ASSERT_NO_THROW(
      arrow_reader.reset(new FileReader(pool, ParquetFileReader::OpenFile(path, false))));
  std::shared_ptr<::arrow::Table> table;
  ASSERT_OK_NO_THROW(arrow_reader->ReadTable(&table));
}

}  // namespace arrow

}  // namespace parquet
