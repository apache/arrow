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

#include <arrow/compute/api.h>
#include <cstdint>
#include <functional>
#include <sstream>
#include <vector>

#include "arrow/api.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"

#include "parquet/api/reader.h"
#include "parquet/api/writer.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/test-util.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_writer.h"
#include "parquet/util/test-common.h"

using arrow::Array;
using arrow::ArrayVisitor;
using arrow::Buffer;
using arrow::ChunkedArray;
using arrow::Column;
using arrow::DataType;
using arrow::default_memory_pool;
using arrow::ListArray;
using arrow::PrimitiveArray;
using arrow::ResizableBuffer;
using arrow::Status;
using arrow::Table;
using arrow::TimeUnit;
using arrow::compute::Datum;
using arrow::compute::DictionaryEncode;
using arrow::compute::FunctionContext;
using arrow::io::BufferReader;

using arrow::randint;
using arrow::random_is_valid;

using ArrowId = ::arrow::Type;
using ParquetType = parquet::Type;
using parquet::arrow::FromParquetSchema;
using parquet::schema::GroupNode;
using parquet::schema::NodePtr;
using parquet::schema::PrimitiveNode;

using ColumnVector = std::vector<std::shared_ptr<arrow::Column>>;

namespace parquet {
namespace arrow {

static constexpr int SMALL_SIZE = 100;
#ifdef PARQUET_VALGRIND
static constexpr int LARGE_SIZE = 1000;
#else
static constexpr int LARGE_SIZE = 10000;
#endif

static constexpr uint32_t kDefaultSeed = 0;

LogicalType::type get_logical_type(const ::DataType& type) {
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
      break;
    }
    case ArrowId::TIME32:
      return LogicalType::TIME_MILLIS;
    case ArrowId::TIME64:
      return LogicalType::TIME_MICROS;
    case ArrowId::DICTIONARY: {
      const ::arrow::DictionaryType& dict_type =
          static_cast<const ::arrow::DictionaryType&>(type);
      return get_logical_type(*dict_type.dictionary()->type());
    }
    case ArrowId::DECIMAL:
      return LogicalType::DECIMAL;
    default:
      break;
  }
  return LogicalType::NONE;
}

ParquetType::type get_physical_type(const ::DataType& type) {
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
    case ArrowId::DECIMAL:
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
    case ArrowId::DICTIONARY: {
      const ::arrow::DictionaryType& dict_type =
          static_cast<const ::arrow::DictionaryType&>(type);
      return get_physical_type(*dict_type.dictionary()->type());
    }
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

const std::string test_traits<::arrow::StringType>::value("Test");              // NOLINT
const std::string test_traits<::arrow::BinaryType>::value("\x00\x01\x02\x03");  // NOLINT
const std::string test_traits<::arrow::FixedSizeBinaryType>::value("Fixed");    // NOLINT

template <typename T>
using ParquetDataType = DataType<test_traits<T>::parquet_enum>;

template <typename T>
using ParquetWriter = TypedColumnWriter<ParquetDataType<T>>;

void WriteTableToBuffer(const std::shared_ptr<Table>& table, int64_t row_group_size,
                        const std::shared_ptr<ArrowWriterProperties>& arrow_properties,
                        std::shared_ptr<Buffer>* out) {
  auto sink = std::make_shared<InMemoryOutputStream>();

  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), sink,
                                row_group_size, default_writer_properties(),
                                arrow_properties));
  *out = sink->GetBuffer();
}

void AssertChunkedEqual(const ChunkedArray& expected, const ChunkedArray& actual) {
  ASSERT_EQ(expected.num_chunks(), actual.num_chunks()) << "# chunks unequal";
  if (!actual.Equals(expected)) {
    std::stringstream pp_result;
    std::stringstream pp_expected;

    for (int i = 0; i < actual.num_chunks(); ++i) {
      auto c1 = actual.chunk(i);
      auto c2 = expected.chunk(i);
      if (!c1->Equals(*c2)) {
        ARROW_EXPECT_OK(::arrow::PrettyPrint(*c1, 0, &pp_result));
        ARROW_EXPECT_OK(::arrow::PrettyPrint(*c2, 0, &pp_expected));
        FAIL() << "Chunk " << i << " Got: " << pp_result.str()
               << "\nExpected: " << pp_expected.str();
      }
    }
  }
}

void PrintColumn(const Column& col, std::stringstream* ss) {
  const ChunkedArray& carr = *col.data();
  for (int i = 0; i < carr.num_chunks(); ++i) {
    auto c1 = carr.chunk(i);
    *ss << "Chunk " << i << std::endl;
    ARROW_EXPECT_OK(::arrow::PrettyPrint(*c1, 0, ss));
    *ss << std::endl;
  }
}

void DoSimpleRoundtrip(const std::shared_ptr<Table>& table, bool use_threads,
                       int64_t row_group_size, const std::vector<int>& column_subset,
                       std::shared_ptr<Table>* out,
                       const std::shared_ptr<ArrowWriterProperties>& arrow_properties =
                           default_arrow_writer_properties()) {
  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(
      WriteTableToBuffer(table, row_group_size, arrow_properties, &buffer));

  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                              ::arrow::default_memory_pool(),
                              ::parquet::default_reader_properties(), nullptr, &reader));

  reader->set_use_threads(use_threads);

  if (column_subset.size() > 0) {
    ASSERT_OK_NO_THROW(reader->ReadTable(column_subset, out));
  } else {
    // Read everything
    ASSERT_OK_NO_THROW(reader->ReadTable(out));
  }
}

void CheckSimpleRoundtrip(const std::shared_ptr<Table>& table, int64_t row_group_size,
                          const std::shared_ptr<ArrowWriterProperties>& arrow_properties =
                              default_arrow_writer_properties()) {
  std::shared_ptr<Table> result;
  DoSimpleRoundtrip(table, false /* use_threads */, row_group_size, {}, &result,
                    arrow_properties);
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result, false));
}

static std::shared_ptr<GroupNode> MakeSimpleSchema(const ::DataType& type,
                                                   Repetition::type repetition) {
  int32_t byte_width = -1;
  int32_t precision = -1;
  int32_t scale = -1;

  switch (type.id()) {
    case ::arrow::Type::DICTIONARY: {
      const auto& dict_type = static_cast<const ::arrow::DictionaryType&>(type);
      const ::DataType& values_type = *dict_type.dictionary()->type();
      switch (values_type.id()) {
        case ::arrow::Type::FIXED_SIZE_BINARY:
          byte_width =
              static_cast<const ::arrow::FixedSizeBinaryType&>(values_type).byte_width();
          break;
        case ::arrow::Type::DECIMAL: {
          const auto& decimal_type =
              static_cast<const ::arrow::Decimal128Type&>(values_type);
          precision = decimal_type.precision();
          scale = decimal_type.scale();
          byte_width = DecimalSize(precision);
        } break;
        default:
          break;
      }
    } break;
    case ::arrow::Type::FIXED_SIZE_BINARY:
      byte_width = static_cast<const ::arrow::FixedSizeBinaryType&>(type).byte_width();
      break;
    case ::arrow::Type::DECIMAL: {
      const auto& decimal_type = static_cast<const ::arrow::Decimal128Type&>(type);
      precision = decimal_type.precision();
      scale = decimal_type.scale();
      byte_width = DecimalSize(precision);
    } break;
    default:
      break;
  }
  auto pnode = PrimitiveNode::Make("column1", repetition, get_physical_type(type),
                                   get_logical_type(type), byte_width, precision, scale);
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
    ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                                ::arrow::default_memory_pool(),
                                ::parquet::default_reader_properties(), nullptr, out));
  }

  void ReadSingleColumnFile(std::unique_ptr<FileReader> file_reader,
                            std::shared_ptr<Array>* out) {
    std::unique_ptr<ColumnReader> column_reader;
    ASSERT_OK_NO_THROW(file_reader->GetColumn(0, &column_reader));
    ASSERT_NE(nullptr, column_reader.get());

    std::shared_ptr<ChunkedArray> chunked_out;
    ASSERT_OK(column_reader->NextBatch(SMALL_SIZE, &chunked_out));

    ASSERT_EQ(1, chunked_out->num_chunks());
    *out = chunked_out->chunk(0);
    ASSERT_NE(nullptr, out->get());
  }

  void ReadAndCheckSingleColumnFile(const Array& values) {
    std::shared_ptr<Array> out;

    std::unique_ptr<FileReader> reader;
    ReaderFromSink(&reader);
    ReadSingleColumnFile(std::move(reader), &out);

    AssertArraysEqual(values, *out);
  }

  void ReadTableFromFile(std::unique_ptr<FileReader> reader,
                         std::shared_ptr<Table>* out) {
    ASSERT_OK_NO_THROW(reader->ReadTable(out));
    auto key_value_metadata =
        reader->parquet_reader()->metadata()->key_value_metadata().get();
    ASSERT_EQ(nullptr, key_value_metadata);
    ASSERT_NE(nullptr, out->get());
  }

  void PrepareListTable(int64_t size, bool nullable_lists, bool nullable_elements,
                        int64_t null_count, std::shared_ptr<Table>* out) {
    std::shared_ptr<Array> values;
    ASSERT_OK(NullableArray<TestType>(size * size, nullable_elements ? null_count : 0,
                                      kDefaultSeed, &values));
    // Also test that slice offsets are respected
    values = values->Slice(5, values->length() - 5);
    std::shared_ptr<ListArray> lists;
    ASSERT_OK(MakeListArray(values, size, nullable_lists ? null_count : 0,
                            nullable_elements, &lists));
    *out = MakeSimpleTable(lists->Slice(3, size - 6), nullable_lists);
  }

  // Prepare table of empty lists, with null values array (ARROW-2744)
  void PrepareEmptyListsTable(int64_t size, std::shared_ptr<Table>* out) {
    std::shared_ptr<Array> lists;
    ASSERT_OK(MakeEmptyListsArray(size, &lists));
    *out = MakeSimpleTable(lists, true /* nullable_lists */);
  }

  void PrepareListOfListTable(int64_t size, bool nullable_parent_lists,
                              bool nullable_lists, bool nullable_elements,
                              int64_t null_count, std::shared_ptr<Table>* out) {
    std::shared_ptr<Array> values;
    ASSERT_OK(NullableArray<TestType>(size * 6, nullable_elements ? null_count : 0,
                                      kDefaultSeed, &values));
    std::shared_ptr<ListArray> lists;
    ASSERT_OK(MakeListArray(values, size * 3, nullable_lists ? null_count : 0,
                            nullable_elements, &lists));
    std::shared_ptr<ListArray> parent_lists;
    ASSERT_OK(MakeListArray(lists, size, nullable_parent_lists ? null_count : 0,
                            nullable_lists, &parent_lists));
    *out = MakeSimpleTable(parent_lists, nullable_parent_lists);
  }

  void ReadAndCheckSingleColumnTable(const std::shared_ptr<Array>& values) {
    std::shared_ptr<::arrow::Table> out;
    std::unique_ptr<FileReader> reader;
    ReaderFromSink(&reader);
    ReadTableFromFile(std::move(reader), &out);
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(values->length(), out->num_rows());

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
    ASSERT_EQ(1, chunked_array->num_chunks());
    auto result = chunked_array->chunk(0);

    AssertArraysEqual(*values, *result);
  }

  void CheckRoundTrip(const std::shared_ptr<Table>& table) {
    CheckSimpleRoundtrip(table, table->num_rows());
  }

  template <typename ArrayType>
  void WriteColumn(const std::shared_ptr<GroupNode>& schema,
                   const std::shared_ptr<ArrayType>& values) {
    SchemaDescriptor descriptor;
    ASSERT_NO_THROW(descriptor.Init(schema));
    std::shared_ptr<::arrow::Schema> arrow_schema;
    ASSERT_OK_NO_THROW(FromParquetSchema(&descriptor, &arrow_schema));
    FileWriter writer(::arrow::default_memory_pool(), MakeWriter(schema), arrow_schema);
    ASSERT_OK_NO_THROW(writer.NewRowGroup(values->length()));
    ASSERT_OK_NO_THROW(writer.WriteColumnChunk(*values));
    ASSERT_OK_NO_THROW(writer.Close());
    // writer.Close() should be idempotent
    ASSERT_OK_NO_THROW(writer.Close());
  }

  std::shared_ptr<InMemoryOutputStream> sink_;
};

// We have separate tests for UInt32Type as this is currently the only type
// where a roundtrip does not yield the identical Array structure.
// There we write an UInt32 Array but receive an Int64 Array as result for
// Parquet version 1.0.

typedef ::testing::Types<
    ::arrow::BooleanType, ::arrow::UInt8Type, ::arrow::Int8Type, ::arrow::UInt16Type,
    ::arrow::Int16Type, ::arrow::Int32Type, ::arrow::UInt64Type, ::arrow::Int64Type,
    ::arrow::Date32Type, ::arrow::FloatType, ::arrow::DoubleType, ::arrow::StringType,
    ::arrow::BinaryType, ::arrow::FixedSizeBinaryType, DecimalWithPrecisionAndScale<1>,
    DecimalWithPrecisionAndScale<3>, DecimalWithPrecisionAndScale<5>,
    DecimalWithPrecisionAndScale<7>, DecimalWithPrecisionAndScale<10>,
    DecimalWithPrecisionAndScale<12>, DecimalWithPrecisionAndScale<15>,
    DecimalWithPrecisionAndScale<17>, DecimalWithPrecisionAndScale<19>,
    DecimalWithPrecisionAndScale<22>, DecimalWithPrecisionAndScale<23>,
    DecimalWithPrecisionAndScale<24>, DecimalWithPrecisionAndScale<27>,
    DecimalWithPrecisionAndScale<29>, DecimalWithPrecisionAndScale<32>,
    DecimalWithPrecisionAndScale<34>, DecimalWithPrecisionAndScale<38>>
    TestTypes;

TYPED_TEST_CASE(TestParquetIO, TestTypes);

TYPED_TEST(TestParquetIO, SingleColumnRequiredWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));

  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::REQUIRED);
  ASSERT_NO_FATAL_FAILURE(this->WriteColumn(schema, values));

  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*values));
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
  ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
  ASSERT_NO_FATAL_FAILURE(this->ReadTableFromFile(std::move(reader), &out));
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(100, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());

  AssertArraysEqual(*values, *chunked_array->chunk(0));
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalReadWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(SMALL_SIZE, 10, kDefaultSeed, &values));

  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::OPTIONAL);
  ASSERT_NO_FATAL_FAILURE(this->WriteColumn(schema, values));

  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*values));
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalDictionaryWrite) {
  // Skip tests for BOOL as we don't create dictionaries for it.
  if (TypeParam::type_id == ::arrow::Type::BOOL) {
    return;
  }

  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(SMALL_SIZE, 10, kDefaultSeed, &values));

  Datum out;
  FunctionContext ctx(default_memory_pool());
  ASSERT_OK(DictionaryEncode(&ctx, Datum(values), &out));
  std::shared_ptr<Array> dict_values = MakeArray(out.array());
  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*dict_values->type(), Repetition::OPTIONAL);
  ASSERT_NO_FATAL_FAILURE(this->WriteColumn(schema, dict_values));

  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*values));
}

TYPED_TEST(TestParquetIO, SingleColumnRequiredSliceWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(2 * SMALL_SIZE, &values));
  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::REQUIRED);

  std::shared_ptr<Array> sliced_values = values->Slice(SMALL_SIZE / 2, SMALL_SIZE);
  ASSERT_NO_FATAL_FAILURE(this->WriteColumn(schema, sliced_values));
  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*sliced_values));

  // Slice offset 1 higher
  sliced_values = values->Slice(SMALL_SIZE / 2 + 1, SMALL_SIZE);
  ASSERT_NO_FATAL_FAILURE(this->WriteColumn(schema, sliced_values));
  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*sliced_values));
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalSliceWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NullableArray<TypeParam>(2 * SMALL_SIZE, SMALL_SIZE, kDefaultSeed, &values));
  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::OPTIONAL);

  std::shared_ptr<Array> sliced_values = values->Slice(SMALL_SIZE / 2, SMALL_SIZE);
  ASSERT_NO_FATAL_FAILURE(this->WriteColumn(schema, sliced_values));
  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*sliced_values));

  // Slice offset 1 higher, thus different null bitmap.
  sliced_values = values->Slice(SMALL_SIZE / 2 + 1, SMALL_SIZE);
  ASSERT_NO_FATAL_FAILURE(this->WriteColumn(schema, sliced_values));
  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*sliced_values));
}

TYPED_TEST(TestParquetIO, SingleColumnTableOptionalReadWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(SMALL_SIZE, 10, kDefaultSeed, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  ASSERT_NO_FATAL_FAILURE(this->CheckRoundTrip(table));
}

TYPED_TEST(TestParquetIO, SingleEmptyListsColumnReadWrite) {
  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(this->PrepareEmptyListsTable(SMALL_SIZE, &table));
  ASSERT_NO_FATAL_FAILURE(this->CheckRoundTrip(table));
}

TYPED_TEST(TestParquetIO, SingleNullableListNullableColumnReadWrite) {
  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(this->PrepareListTable(SMALL_SIZE, true, true, 10, &table));
  ASSERT_NO_FATAL_FAILURE(this->CheckRoundTrip(table));
}

TYPED_TEST(TestParquetIO, SingleRequiredListNullableColumnReadWrite) {
  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(this->PrepareListTable(SMALL_SIZE, false, true, 10, &table));
  ASSERT_NO_FATAL_FAILURE(this->CheckRoundTrip(table));
}

TYPED_TEST(TestParquetIO, SingleNullableListRequiredColumnReadWrite) {
  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(this->PrepareListTable(SMALL_SIZE, true, false, 10, &table));
  ASSERT_NO_FATAL_FAILURE(this->CheckRoundTrip(table));
}

TYPED_TEST(TestParquetIO, SingleRequiredListRequiredColumnReadWrite) {
  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(this->PrepareListTable(SMALL_SIZE, false, false, 0, &table));
  ASSERT_NO_FATAL_FAILURE(this->CheckRoundTrip(table));
}

TYPED_TEST(TestParquetIO, SingleNullableListRequiredListRequiredColumnReadWrite) {
  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(
      this->PrepareListOfListTable(SMALL_SIZE, true, false, false, 0, &table));
  ASSERT_NO_FATAL_FAILURE(this->CheckRoundTrip(table));
}

TYPED_TEST(TestParquetIO, SingleColumnRequiredChunkedWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));
  int64_t chunk_size = values->length() / 4;

  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::REQUIRED);
  SchemaDescriptor descriptor;
  ASSERT_NO_THROW(descriptor.Init(schema));
  std::shared_ptr<::arrow::Schema> arrow_schema;
  ASSERT_OK_NO_THROW(FromParquetSchema(&descriptor, &arrow_schema));
  FileWriter writer(default_memory_pool(), this->MakeWriter(schema), arrow_schema);
  for (int i = 0; i < 4; i++) {
    ASSERT_OK_NO_THROW(writer.NewRowGroup(chunk_size));
    std::shared_ptr<Array> sliced_array = values->Slice(i * chunk_size, chunk_size);
    ASSERT_OK_NO_THROW(writer.WriteColumnChunk(*sliced_array));
  }
  ASSERT_OK_NO_THROW(writer.Close());

  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*values));
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredChunkedWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(LARGE_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(*table, default_memory_pool(), this->sink_, 512,
                                default_writer_properties()));

  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnTable(values));
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredChunkedWriteArrowIO) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(LARGE_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  auto buffer = AllocateBuffer();

  {
    // BufferOutputStream closed on gc
    auto arrow_sink_ = std::make_shared<::arrow::io::BufferOutputStream>(buffer);
    ASSERT_OK_NO_THROW(WriteTable(*table, default_memory_pool(), arrow_sink_, 512,
                                  default_writer_properties()));

    // XXX: Remove this after ARROW-455 completed
    ASSERT_OK(arrow_sink_->Close());
  }

  auto pbuffer = std::make_shared<Buffer>(buffer->data(), buffer->size());

  auto source = std::make_shared<BufferReader>(pbuffer);
  std::shared_ptr<::arrow::Table> out;
  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(OpenFile(source, ::arrow::default_memory_pool(), &reader));
  ASSERT_NO_FATAL_FAILURE(this->ReadTableFromFile(std::move(reader), &out));
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(values->length(), out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());

  AssertArraysEqual(*values, *chunked_array->chunk(0));
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalChunkedWrite) {
  int64_t chunk_size = SMALL_SIZE / 4;
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(SMALL_SIZE, 10, kDefaultSeed, &values));

  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::OPTIONAL);
  SchemaDescriptor descriptor;
  ASSERT_NO_THROW(descriptor.Init(schema));
  std::shared_ptr<::arrow::Schema> arrow_schema;
  ASSERT_OK_NO_THROW(FromParquetSchema(&descriptor, &arrow_schema));
  FileWriter writer(::arrow::default_memory_pool(), this->MakeWriter(schema),
                    arrow_schema);
  for (int i = 0; i < 4; i++) {
    ASSERT_OK_NO_THROW(writer.NewRowGroup(chunk_size));
    std::shared_ptr<Array> sliced_array = values->Slice(i * chunk_size, chunk_size);
    ASSERT_OK_NO_THROW(writer.WriteColumnChunk(*sliced_array));
  }
  ASSERT_OK_NO_THROW(writer.Close());

  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*values));
}

TYPED_TEST(TestParquetIO, SingleColumnTableOptionalChunkedWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(LARGE_SIZE, 100, kDefaultSeed, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_, 512,
                                default_writer_properties()));

  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnTable(values));
}

TYPED_TEST(TestParquetIO, FileMetaDataWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                values->length(), default_writer_properties()));

  std::unique_ptr<FileReader> reader;
  ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
  auto metadata = reader->parquet_reader()->metadata();
  ASSERT_EQ(1, metadata->num_columns());
  ASSERT_EQ(100, metadata->num_rows());

  this->sink_ = std::make_shared<InMemoryOutputStream>();

  ASSERT_OK_NO_THROW(::parquet::arrow::WriteFileMetaData(*metadata, this->sink_.get()));

  ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
  auto metadata_written = reader->parquet_reader()->metadata();
  ASSERT_EQ(metadata->size(), metadata_written->size());
  ASSERT_EQ(metadata->num_row_groups(), metadata_written->num_row_groups());
  ASSERT_EQ(metadata->num_rows(), metadata_written->num_rows());
  ASSERT_EQ(metadata->num_columns(), metadata_written->num_columns());
  ASSERT_EQ(metadata->RowGroup(0)->num_rows(), metadata_written->RowGroup(0)->num_rows());
}

using TestInt96ParquetIO = TestParquetIO<::arrow::TimestampType>;

TEST_F(TestInt96ParquetIO, ReadIntoTimestamp) {
  // This test explicitly tests the conversion from an Impala-style timestamp
  // to a nanoseconds-since-epoch one.

  // 2nd January 1970, 11:35min 145738543ns
  Int96 day;
  day.value[2] = UINT32_C(2440589);
  int64_t seconds = (11 * 60 + 35) * 60;
  Int96SetNanoSeconds(
      day, seconds * INT64_C(1000) * INT64_C(1000) * INT64_C(1000) + 145738543);
  // Compute the corresponding nanosecond timestamp
  struct tm datetime;
  memset(&datetime, 0, sizeof(struct tm));
  datetime.tm_year = 70;
  datetime.tm_mon = 0;
  datetime.tm_mday = 2;
  datetime.tm_hour = 11;
  datetime.tm_min = 35;
  struct tm epoch;
  memset(&epoch, 0, sizeof(struct tm));

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
  RowGroupWriter* rg_writer = writer->AppendRowGroup();
  ColumnWriter* c_writer = rg_writer->NextColumn();
  auto typed_writer = dynamic_cast<TypedColumnWriter<Int96Type>*>(c_writer);
  ASSERT_NE(typed_writer, nullptr);
  typed_writer->WriteBatch(1, nullptr, nullptr, &day);
  c_writer->Close();
  rg_writer->Close();
  writer->Close();

  ::arrow::TimestampBuilder builder(::arrow::timestamp(TimeUnit::NANO),
                                    ::arrow::default_memory_pool());
  ASSERT_OK(builder.Append(val));
  std::shared_ptr<Array> values;
  ASSERT_OK(builder.Finish(&values));
  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*values));
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
  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnTable(values));
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

  std::shared_ptr<ResizableBuffer> int64_data = AllocateBuffer();
  {
    ASSERT_OK(int64_data->Resize(sizeof(int64_t) * values->length()));
    auto int64_data_ptr = reinterpret_cast<int64_t*>(int64_data->mutable_data());
    auto uint32_data_ptr = reinterpret_cast<const uint32_t*>(values->values()->data());
    const auto cast_uint32_to_int64 = [](uint32_t value) {
      return static_cast<int64_t>(value);
    };
    std::transform(uint32_data_ptr, uint32_data_ptr + values->length(), int64_data_ptr,
                   cast_uint32_to_int64);
  }

  std::vector<std::shared_ptr<Buffer>> buffers{values->null_bitmap(), int64_data};
  auto arr_data = std::make_shared<::arrow::ArrayData>(::arrow::int64(), values->length(),
                                                       buffers, values->null_count());
  std::shared_ptr<Array> expected_values = MakeArray(arr_data);
  ASSERT_NE(expected_values, NULLPTR);

  const auto& expected = static_cast<const ::arrow::Int64Array&>(*expected_values);
  ASSERT_GT(values->length(), 0);
  ASSERT_EQ(values->length(), expected.length());

  // TODO(phillipc): Is there a better way to compare these two arrays?
  // AssertArraysEqual requires the same type, but we only care about values in this case
  for (int i = 0; i < expected.length(); ++i) {
    const bool value_is_valid = values->IsValid(i);
    const bool expected_value_is_valid = expected.IsValid(i);

    ASSERT_EQ(expected_value_is_valid, value_is_valid);

    if (value_is_valid) {
      uint32_t value = values->Value(i);
      int64_t expected_value = expected.Value(i);
      ASSERT_EQ(expected_value, static_cast<int64_t>(value));
    }
  }
}

using TestStringParquetIO = TestParquetIO<::arrow::StringType>;

TEST_F(TestStringParquetIO, EmptyStringColumnRequiredWrite) {
  std::shared_ptr<Array> values;
  ::arrow::StringBuilder builder;
  for (size_t i = 0; i < SMALL_SIZE; i++) {
    ASSERT_OK(builder.Append(""));
  }
  ASSERT_OK(builder.Finish(&values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                values->length(), default_writer_properties()));

  std::shared_ptr<Table> out;
  std::unique_ptr<FileReader> reader;
  ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
  ASSERT_NO_FATAL_FAILURE(this->ReadTableFromFile(std::move(reader), &out));
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(100, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());

  AssertArraysEqual(*values, *chunked_array->chunk(0));
}

using TestNullParquetIO = TestParquetIO<::arrow::NullType>;

TEST_F(TestNullParquetIO, NullColumn) {
  for (int32_t num_rows : {0, SMALL_SIZE}) {
    std::shared_ptr<Array> values = std::make_shared<::arrow::NullArray>(num_rows);
    std::shared_ptr<Table> table = MakeSimpleTable(values, true /* nullable */);
    this->sink_ = std::make_shared<InMemoryOutputStream>();

    const int64_t chunk_size = std::max(static_cast<int64_t>(1), table->num_rows());
    ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                  chunk_size, default_writer_properties()));

    std::shared_ptr<Table> out;
    std::unique_ptr<FileReader> reader;
    ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
    ASSERT_NO_FATAL_FAILURE(this->ReadTableFromFile(std::move(reader), &out));
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(num_rows, out->num_rows());

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
    ASSERT_EQ(1, chunked_array->num_chunks());
    AssertArraysEqual(*values, *chunked_array->chunk(0));
  }
}

TEST_F(TestNullParquetIO, NullListColumn) {
  std::vector<int32_t> offsets1 = {0};
  std::vector<int32_t> offsets2 = {0, 2, 2, 3, 115};
  for (std::vector<int32_t> offsets : {offsets1, offsets2}) {
    std::shared_ptr<Array> offsets_array, values_array, list_array;
    ::arrow::ArrayFromVector<::arrow::Int32Type, int32_t>(offsets, &offsets_array);
    values_array = std::make_shared<::arrow::NullArray>(offsets.back());
    ASSERT_OK(::arrow::ListArray::FromArrays(*offsets_array, *values_array,
                                             default_memory_pool(), &list_array));

    std::shared_ptr<Table> table = MakeSimpleTable(list_array, false /* nullable */);
    this->sink_ = std::make_shared<InMemoryOutputStream>();

    const int64_t chunk_size = std::max(static_cast<int64_t>(1), table->num_rows());
    ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                  chunk_size, default_writer_properties()));

    std::shared_ptr<Table> out;
    std::unique_ptr<FileReader> reader;
    this->ReaderFromSink(&reader);
    this->ReadTableFromFile(std::move(reader), &out);
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(offsets.size() - 1, out->num_rows());

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
    ASSERT_EQ(1, chunked_array->num_chunks());
    AssertArraysEqual(*list_array, *chunked_array->chunk(0));
  }
}

TEST_F(TestNullParquetIO, NullDictionaryColumn) {
  std::shared_ptr<Array> values = std::make_shared<::arrow::NullArray>(0);
  std::shared_ptr<Array> indices =
      std::make_shared<::arrow::Int8Array>(SMALL_SIZE, nullptr, nullptr, SMALL_SIZE);
  std::shared_ptr<::arrow::DictionaryType> dict_type =
      std::make_shared<::arrow::DictionaryType>(::arrow::int8(), values);
  std::shared_ptr<Array> dict_values =
      std::make_shared<::arrow::DictionaryArray>(dict_type, indices);
  std::shared_ptr<Table> table = MakeSimpleTable(dict_values, true);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                dict_values->length(), default_writer_properties()));

  std::shared_ptr<Table> out;
  std::unique_ptr<FileReader> reader;
  ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
  ASSERT_NO_FATAL_FAILURE(this->ReadTableFromFile(std::move(reader), &out));
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(100, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());

  std::shared_ptr<Array> expected_values =
      std::make_shared<::arrow::NullArray>(SMALL_SIZE);
  AssertArraysEqual(*expected_values, *chunked_array->chunk(0));
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

  void MakeTestFile(std::vector<T>& values, int num_chunks,
                    std::unique_ptr<FileReader>* reader) {
    TestType dummy;

    std::shared_ptr<GroupNode> schema = MakeSimpleSchema(dummy, Repetition::REQUIRED);
    std::unique_ptr<ParquetFileWriter> file_writer = this->MakeWriter(schema);
    size_t chunk_size = values.size() / num_chunks;
    // Convert to Parquet's expected physical type
    std::vector<uint8_t> values_buffer(sizeof(ParquetCDataType<TestType>) *
                                       values.size());
    auto values_parquet =
        reinterpret_cast<ParquetCDataType<TestType>*>(values_buffer.data());
    std::copy(values.cbegin(), values.cend(), values_parquet);
    for (int i = 0; i < num_chunks; i++) {
      auto row_group_writer = file_writer->AppendRowGroup();
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
    ASSERT_NO_FATAL_FAILURE(MakeTestFile(values, num_chunks, &file_reader));

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
    ASSERT_NO_FATAL_FAILURE(MakeTestFile(values, num_chunks, &file_reader));

    std::shared_ptr<Array> out;
    this->ReadSingleColumnFile(std::move(file_reader), &out);

    ExpectArrayT<TestType>(values.data(), out.get());
  }
};

typedef ::testing::Types<::arrow::BooleanType, ::arrow::UInt8Type, ::arrow::Int8Type,
                         ::arrow::UInt16Type, ::arrow::Int16Type, ::arrow::UInt32Type,
                         ::arrow::Int32Type, ::arrow::UInt64Type, ::arrow::Int64Type,
                         ::arrow::FloatType, ::arrow::DoubleType>
    PrimitiveTestTypes;

TYPED_TEST_CASE(TestPrimitiveParquetIO, PrimitiveTestTypes);

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredRead) {
  ASSERT_NO_FATAL_FAILURE(this->CheckSingleColumnRequiredRead(1));
}

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredTableRead) {
  ASSERT_NO_FATAL_FAILURE(this->CheckSingleColumnRequiredTableRead(1));
}

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredChunkedRead) {
  ASSERT_NO_FATAL_FAILURE(this->CheckSingleColumnRequiredRead(4));
}

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredChunkedTableRead) {
  ASSERT_NO_FATAL_FAILURE(this->CheckSingleColumnRequiredTableRead(4));
}

void MakeDateTimeTypesTable(std::shared_ptr<Table>* out, bool nanos_as_micros = false) {
  using ::arrow::ArrayFromVector;

  std::vector<bool> is_valid = {true, true, true, false, true, true};

  // These are only types that roundtrip without modification
  auto f0 = field("f0", ::arrow::date32());
  auto f1 = field("f1", ::arrow::timestamp(TimeUnit::MILLI));
  auto f2 = field("f2", ::arrow::timestamp(TimeUnit::MICRO));
  auto f3_unit = nanos_as_micros ? TimeUnit::MICRO : TimeUnit::NANO;
  auto f3 = field("f3", ::arrow::timestamp(f3_unit));
  auto f4 = field("f4", ::arrow::time32(TimeUnit::MILLI));
  auto f5 = field("f5", ::arrow::time64(TimeUnit::MICRO));

  std::shared_ptr<::arrow::Schema> schema(new ::arrow::Schema({f0, f1, f2, f3, f4, f5}));

  std::vector<int32_t> t32_values = {1489269000, 1489270000, 1489271000,
                                     1489272000, 1489272000, 1489273000};
  std::vector<int64_t> t64_ns_values = {1489269000000, 1489270000000, 1489271000000,
                                        1489272000000, 1489272000000, 1489273000000};
  std::vector<int64_t> t64_us_values = {1489269000, 1489270000, 1489271000,
                                        1489272000, 1489272000, 1489273000};
  std::vector<int64_t> t64_ms_values = {1489269, 1489270, 1489271,
                                        1489272, 1489272, 1489273};

  std::shared_ptr<Array> a0, a1, a2, a3, a4, a5;
  ArrayFromVector<::arrow::Date32Type, int32_t>(f0->type(), is_valid, t32_values, &a0);
  ArrayFromVector<::arrow::TimestampType, int64_t>(f1->type(), is_valid, t64_ms_values,
                                                   &a1);
  ArrayFromVector<::arrow::TimestampType, int64_t>(f2->type(), is_valid, t64_us_values,
                                                   &a2);
  auto f3_data = nanos_as_micros ? t64_us_values : t64_ns_values;
  ArrayFromVector<::arrow::TimestampType, int64_t>(f3->type(), is_valid, f3_data, &a3);
  ArrayFromVector<::arrow::Time32Type, int32_t>(f4->type(), is_valid, t32_values, &a4);
  ArrayFromVector<::arrow::Time64Type, int64_t>(f5->type(), is_valid, t64_us_values, &a5);

  std::vector<std::shared_ptr<::arrow::Column>> columns = {
      std::make_shared<Column>("f0", a0), std::make_shared<Column>("f1", a1),
      std::make_shared<Column>("f2", a2), std::make_shared<Column>("f3", a3),
      std::make_shared<Column>("f4", a4), std::make_shared<Column>("f5", a5)};

  *out = Table::Make(schema, columns);
}

TEST(TestArrowReadWrite, DateTimeTypes) {
  std::shared_ptr<Table> table, result;
  MakeDateTimeTypesTable(&table);

  // Cast nanaoseconds to microseconds and use INT64 physical type
  ASSERT_NO_FATAL_FAILURE(
      DoSimpleRoundtrip(table, false /* use_threads */, table->num_rows(), {}, &result));
  MakeDateTimeTypesTable(&table, true);

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result));
}

TEST(TestArrowReadWrite, UseDeprecatedInt96) {
  using ::arrow::ArrayFromVector;
  using ::arrow::field;
  using ::arrow::schema;

  std::vector<bool> is_valid = {true, true, true, false, true, true};

  auto t_s = ::arrow::timestamp(TimeUnit::SECOND);
  auto t_ms = ::arrow::timestamp(TimeUnit::MILLI);
  auto t_us = ::arrow::timestamp(TimeUnit::MICRO);
  auto t_ns = ::arrow::timestamp(TimeUnit::NANO);

  std::vector<int64_t> s_values = {1489269, 1489270, 1489271, 1489272, 1489272, 1489273};
  std::vector<int64_t> ms_values = {1489269000, 1489270000, 1489271000,
                                    1489272001, 1489272000, 1489273000};
  std::vector<int64_t> us_values = {1489269000000, 1489270000000, 1489271000000,
                                    1489272000001, 1489272000000, 1489273000000};
  std::vector<int64_t> ns_values = {1489269000000000LL, 1489270000000000LL,
                                    1489271000000000LL, 1489272000000001LL,
                                    1489272000000000LL, 1489273000000000LL};

  std::shared_ptr<Array> a_s, a_ms, a_us, a_ns;
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_s, is_valid, s_values, &a_s);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_ms, is_valid, ms_values, &a_ms);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_us, is_valid, us_values, &a_us);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_ns, is_valid, ns_values, &a_ns);

  // Each input is typed with a unique TimeUnit
  auto input_schema = schema(
      {field("f_s", t_s), field("f_ms", t_ms), field("f_us", t_us), field("f_ns", t_ns)});
  auto input = Table::Make(
      input_schema,
      {std::make_shared<Column>("f_s", a_s), std::make_shared<Column>("f_ms", a_ms),
       std::make_shared<Column>("f_us", a_us), std::make_shared<Column>("f_ns", a_ns)});

  // When reading parquet files, all int96 schema fields are converted to
  // timestamp nanoseconds
  auto ex_schema = schema({field("f_s", t_ns), field("f_ms", t_ns), field("f_us", t_ns),
                           field("f_ns", t_ns)});
  auto ex_result = Table::Make(
      ex_schema,
      {std::make_shared<Column>("f_s", a_ns), std::make_shared<Column>("f_ms", a_ns),
       std::make_shared<Column>("f_us", a_ns), std::make_shared<Column>("f_ns", a_ns)});

  std::shared_ptr<Table> result;
  ASSERT_NO_FATAL_FAILURE(DoSimpleRoundtrip(
      input, false /* use_threads */, input->num_rows(), {}, &result,
      ArrowWriterProperties::Builder().enable_deprecated_int96_timestamps()->build()));

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*ex_result, *result));

  // Ensure enable_deprecated_int96_timestamps as precedence over
  // coerce_timestamps.
  ASSERT_NO_FATAL_FAILURE(DoSimpleRoundtrip(input, false /* use_threads */,
                                            input->num_rows(), {}, &result,
                                            ArrowWriterProperties::Builder()
                                                .enable_deprecated_int96_timestamps()
                                                ->coerce_timestamps(TimeUnit::MILLI)
                                                ->build()));

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*ex_result, *result));
}

TEST(TestArrowReadWrite, CoerceTimestamps) {
  using ::arrow::ArrayFromVector;
  using ::arrow::field;

  // PARQUET-1078, coerce Arrow timestamps to either TIMESTAMP_MILLIS or TIMESTAMP_MICROS
  std::vector<bool> is_valid = {true, true, true, false, true, true};

  auto t_s = ::arrow::timestamp(TimeUnit::SECOND);
  auto t_ms = ::arrow::timestamp(TimeUnit::MILLI);
  auto t_us = ::arrow::timestamp(TimeUnit::MICRO);
  auto t_ns = ::arrow::timestamp(TimeUnit::NANO);

  std::vector<int64_t> s_values = {1489269, 1489270, 1489271, 1489272, 1489272, 1489273};
  std::vector<int64_t> ms_values = {1489269000, 1489270000, 1489271000,
                                    1489272001, 1489272000, 1489273000};
  std::vector<int64_t> us_values = {1489269000000, 1489270000000, 1489271000000,
                                    1489272000001, 1489272000000, 1489273000000};
  std::vector<int64_t> ns_values = {1489269000000000LL, 1489270000000000LL,
                                    1489271000000000LL, 1489272000000001LL,
                                    1489272000000000LL, 1489273000000000LL};

  std::shared_ptr<Array> a_s, a_ms, a_us, a_ns;
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_s, is_valid, s_values, &a_s);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_ms, is_valid, ms_values, &a_ms);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_us, is_valid, us_values, &a_us);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_ns, is_valid, ns_values, &a_ns);

  // Input table, all data as is
  auto s1 = std::shared_ptr<::arrow::Schema>(
      new ::arrow::Schema({field("f_s", t_s), field("f_ms", t_ms), field("f_us", t_us),
                           field("f_ns", t_ns)}));
  auto input = Table::Make(
      s1,
      {std::make_shared<Column>("f_s", a_s), std::make_shared<Column>("f_ms", a_ms),
       std::make_shared<Column>("f_us", a_us), std::make_shared<Column>("f_ns", a_ns)});

  // Result when coercing to milliseconds
  auto s2 = std::shared_ptr<::arrow::Schema>(
      new ::arrow::Schema({field("f_s", t_ms), field("f_ms", t_ms), field("f_us", t_ms),
                           field("f_ns", t_ms)}));
  auto ex_milli_result = Table::Make(
      s2,
      {std::make_shared<Column>("f_s", a_ms), std::make_shared<Column>("f_ms", a_ms),
       std::make_shared<Column>("f_us", a_ms), std::make_shared<Column>("f_ns", a_ms)});

  std::shared_ptr<Table> milli_result;
  ASSERT_NO_FATAL_FAILURE(DoSimpleRoundtrip(
      input, false /* use_threads */, input->num_rows(), {}, &milli_result,
      ArrowWriterProperties::Builder().coerce_timestamps(TimeUnit::MILLI)->build()));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*ex_milli_result, *milli_result));

  // Result when coercing to microseconds
  auto s3 = std::shared_ptr<::arrow::Schema>(
      new ::arrow::Schema({field("f_s", t_us), field("f_ms", t_us), field("f_us", t_us),
                           field("f_ns", t_us)}));
  auto ex_micro_result = Table::Make(
      s3,
      {std::make_shared<Column>("f_s", a_us), std::make_shared<Column>("f_ms", a_us),
       std::make_shared<Column>("f_us", a_us), std::make_shared<Column>("f_ns", a_us)});

  std::shared_ptr<Table> micro_result;
  ASSERT_NO_FATAL_FAILURE(DoSimpleRoundtrip(
      input, false /* use_threads */, input->num_rows(), {}, &micro_result,
      ArrowWriterProperties::Builder().coerce_timestamps(TimeUnit::MICRO)->build()));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*ex_micro_result, *micro_result));
}

TEST(TestArrowReadWrite, CoerceTimestampsLosePrecision) {
  using ::arrow::ArrayFromVector;
  using ::arrow::field;

  // PARQUET-1078, coerce Arrow timestamps to either TIMESTAMP_MILLIS or TIMESTAMP_MICROS
  std::vector<bool> is_valid = {true, true, true, false, true, true};

  auto t_s = ::arrow::timestamp(TimeUnit::SECOND);
  auto t_ms = ::arrow::timestamp(TimeUnit::MILLI);
  auto t_us = ::arrow::timestamp(TimeUnit::MICRO);
  auto t_ns = ::arrow::timestamp(TimeUnit::NANO);

  std::vector<int64_t> s_values = {1489269, 1489270, 1489271, 1489272, 1489272, 1489273};
  std::vector<int64_t> ms_values = {1489269001, 1489270001, 1489271001,
                                    1489272001, 1489272001, 1489273001};
  std::vector<int64_t> us_values = {1489269000001, 1489270000001, 1489271000001,
                                    1489272000001, 1489272000001, 1489273000001};
  std::vector<int64_t> ns_values = {1489269000000001LL, 1489270000000001LL,
                                    1489271000000001LL, 1489272000000001LL,
                                    1489272000000001LL, 1489273000000001LL};

  std::shared_ptr<Array> a_s, a_ms, a_us, a_ns;
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_s, is_valid, s_values, &a_s);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_ms, is_valid, ms_values, &a_ms);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_us, is_valid, us_values, &a_us);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_ns, is_valid, ns_values, &a_ns);

  auto s1 = std::shared_ptr<::arrow::Schema>(new ::arrow::Schema({field("f_s", t_s)}));
  auto s2 = std::shared_ptr<::arrow::Schema>(new ::arrow::Schema({field("f_ms", t_ms)}));
  auto s3 = std::shared_ptr<::arrow::Schema>(new ::arrow::Schema({field("f_us", t_us)}));
  auto s4 = std::shared_ptr<::arrow::Schema>(new ::arrow::Schema({field("f_ns", t_ns)}));

  auto c1 = std::make_shared<Column>("f_s", a_s);
  auto c2 = std::make_shared<Column>("f_ms", a_ms);
  auto c3 = std::make_shared<Column>("f_us", a_us);
  auto c4 = std::make_shared<Column>("f_ns", a_ns);

  auto t1 = Table::Make(s1, {c1});
  auto t2 = Table::Make(s2, {c2});
  auto t3 = Table::Make(s3, {c3});
  auto t4 = Table::Make(s4, {c4});

  auto sink = std::make_shared<InMemoryOutputStream>();

  // OK to write to millis
  auto coerce_millis =
      (ArrowWriterProperties::Builder().coerce_timestamps(TimeUnit::MILLI)->build());
  ASSERT_OK_NO_THROW(WriteTable(*t1, ::arrow::default_memory_pool(), sink, 10,
                                default_writer_properties(), coerce_millis));
  ASSERT_OK_NO_THROW(WriteTable(*t2, ::arrow::default_memory_pool(), sink, 10,
                                default_writer_properties(), coerce_millis));

  // Loss of precision
  ASSERT_RAISES(Invalid, WriteTable(*t3, ::arrow::default_memory_pool(), sink, 10,
                                    default_writer_properties(), coerce_millis));
  ASSERT_RAISES(Invalid, WriteTable(*t4, ::arrow::default_memory_pool(), sink, 10,
                                    default_writer_properties(), coerce_millis));

  // OK to lose precision if we explicitly allow it
  auto allow_truncation = (ArrowWriterProperties::Builder()
                               .coerce_timestamps(TimeUnit::MILLI)
                               ->allow_truncated_timestamps()
                               ->build());
  ASSERT_OK_NO_THROW(WriteTable(*t3, ::arrow::default_memory_pool(), sink, 10,
                                default_writer_properties(), allow_truncation));
  ASSERT_OK_NO_THROW(WriteTable(*t4, ::arrow::default_memory_pool(), sink, 10,
                                default_writer_properties(), allow_truncation));

  // OK to write micros to micros
  auto coerce_micros =
      (ArrowWriterProperties::Builder().coerce_timestamps(TimeUnit::MICRO)->build());
  ASSERT_OK_NO_THROW(WriteTable(*t3, ::arrow::default_memory_pool(), sink, 10,
                                default_writer_properties(), coerce_micros));

  // Loss of precision
  ASSERT_RAISES(Invalid, WriteTable(*t4, ::arrow::default_memory_pool(), sink, 10,
                                    default_writer_properties(), coerce_micros));
}

TEST(TestArrowReadWrite, ConvertedDateTimeTypes) {
  using ::arrow::ArrayFromVector;

  std::vector<bool> is_valid = {true, true, true, false, true, true};

  auto f0 = field("f0", ::arrow::date64());
  auto f1 = field("f1", ::arrow::time32(TimeUnit::SECOND));
  auto f2 = field("f2", ::arrow::date64());
  auto f3 = field("f3", ::arrow::time32(TimeUnit::SECOND));

  auto schema = ::arrow::schema({f0, f1, f2, f3});

  std::vector<int64_t> a0_values = {1489190400000, 1489276800000, 1489363200000,
                                    1489449600000, 1489536000000, 1489622400000};
  std::vector<int32_t> a1_values = {0, 1, 2, 3, 4, 5};

  std::shared_ptr<Array> a0, a1, a0_nonnull, a1_nonnull, x0, x1, x0_nonnull, x1_nonnull;

  ArrayFromVector<::arrow::Date64Type, int64_t>(f0->type(), is_valid, a0_values, &a0);
  ArrayFromVector<::arrow::Date64Type, int64_t>(f0->type(), a0_values, &a0_nonnull);

  ArrayFromVector<::arrow::Time32Type, int32_t>(f1->type(), is_valid, a1_values, &a1);
  ArrayFromVector<::arrow::Time32Type, int32_t>(f1->type(), a1_values, &a1_nonnull);

  std::vector<std::shared_ptr<::arrow::Column>> columns = {
      std::make_shared<Column>("f0", a0), std::make_shared<Column>("f1", a1),
      std::make_shared<Column>("f2", a0_nonnull),
      std::make_shared<Column>("f3", a1_nonnull)};
  auto table = Table::Make(schema, columns);

  // Expected schema and values
  auto e0 = field("f0", ::arrow::date32());
  auto e1 = field("f1", ::arrow::time32(TimeUnit::MILLI));
  auto e2 = field("f2", ::arrow::date32());
  auto e3 = field("f3", ::arrow::time32(TimeUnit::MILLI));
  auto ex_schema = ::arrow::schema({e0, e1, e2, e3});

  std::vector<int32_t> x0_values = {17236, 17237, 17238, 17239, 17240, 17241};
  std::vector<int32_t> x1_values = {0, 1000, 2000, 3000, 4000, 5000};
  ArrayFromVector<::arrow::Date32Type, int32_t>(e0->type(), is_valid, x0_values, &x0);
  ArrayFromVector<::arrow::Date32Type, int32_t>(e0->type(), x0_values, &x0_nonnull);

  ArrayFromVector<::arrow::Time32Type, int32_t>(e1->type(), is_valid, x1_values, &x1);
  ArrayFromVector<::arrow::Time32Type, int32_t>(e1->type(), x1_values, &x1_nonnull);

  std::vector<std::shared_ptr<::arrow::Column>> ex_columns = {
      std::make_shared<Column>("f0", x0), std::make_shared<Column>("f1", x1),
      std::make_shared<Column>("f2", x0_nonnull),
      std::make_shared<Column>("f3", x1_nonnull)};
  auto ex_table = Table::Make(ex_schema, ex_columns);

  std::shared_ptr<Table> result;
  ASSERT_NO_FATAL_FAILURE(
      DoSimpleRoundtrip(table, false /* use_threads */, table->num_rows(), {}, &result));

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*ex_table, *result));
}

void MakeDoubleTable(int num_columns, int num_rows, int nchunks,
                     std::shared_ptr<Table>* out) {
  std::shared_ptr<::arrow::Column> column;
  std::vector<std::shared_ptr<::arrow::Column>> columns(num_columns);
  std::vector<std::shared_ptr<::arrow::Field>> fields(num_columns);

  for (int i = 0; i < num_columns; ++i) {
    std::vector<std::shared_ptr<Array>> arrays;
    std::shared_ptr<Array> values;
    ASSERT_OK(NullableArray<::arrow::DoubleType>(num_rows, num_rows / 10,
                                                 static_cast<uint32_t>(i), &values));
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
  *out = Table::Make(schema, columns);
}

void MakeListArray(int num_rows, int max_value_length,
                   std::shared_ptr<::DataType>* out_type,
                   std::shared_ptr<Array>* out_array) {
  std::vector<int32_t> length_draws;
  randint(num_rows, 0, max_value_length, &length_draws);

  std::vector<int32_t> offset_values;

  // Make sure some of them are length 0
  int32_t total_elements = 0;
  for (size_t i = 0; i < length_draws.size(); ++i) {
    if (length_draws[i] < max_value_length / 10) {
      length_draws[i] = 0;
    }
    offset_values.push_back(total_elements);
    total_elements += length_draws[i];
  }
  offset_values.push_back(total_elements);

  std::vector<int8_t> value_draws;
  randint(total_elements, 0, 100, &value_draws);

  std::vector<bool> is_valid;
  random_is_valid(total_elements, 0.1, &is_valid);

  std::shared_ptr<Array> values, offsets;
  ::arrow::ArrayFromVector<::arrow::Int8Type, int8_t>(::arrow::int8(), is_valid,
                                                      value_draws, &values);
  ::arrow::ArrayFromVector<::arrow::Int32Type, int32_t>(offset_values, &offsets);

  ASSERT_OK(::arrow::ListArray::FromArrays(*offsets, *values, default_memory_pool(),
                                           out_array));

  *out_type = ::arrow::list(::arrow::int8());
}

TEST(TestArrowReadWrite, MultithreadedRead) {
  const int num_columns = 20;
  const int num_rows = 1000;
  const bool use_threads = true;

  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));

  std::shared_ptr<Table> result;
  ASSERT_NO_FATAL_FAILURE(
      DoSimpleRoundtrip(table, use_threads, table->num_rows(), {}, &result));

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result));
}

TEST(TestArrowReadWrite, ReadSingleRowGroup) {
  const int num_columns = 20;
  const int num_rows = 1000;

  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));

  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(WriteTableToBuffer(table, num_rows / 2,
                                             default_arrow_writer_properties(), &buffer));

  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                              ::arrow::default_memory_pool(),
                              ::parquet::default_reader_properties(), nullptr, &reader));

  ASSERT_EQ(2, reader->num_row_groups());

  std::shared_ptr<Table> r1, r2, r3, r4;
  // Read everything
  ASSERT_OK_NO_THROW(reader->ReadRowGroup(0, &r1));
  ASSERT_OK_NO_THROW(reader->RowGroup(1)->ReadTable(&r2));
  ASSERT_OK_NO_THROW(reader->ReadRowGroups({0, 1}, &r3));
  ASSERT_OK_NO_THROW(reader->ReadRowGroups({1}, &r4));

  std::shared_ptr<Table> concatenated;

  ASSERT_OK(ConcatenateTables({r1, r2}, &concatenated));
  ASSERT_TRUE(table->Equals(*concatenated));

  ASSERT_TRUE(table->Equals(*r3));
  ASSERT_TRUE(r2->Equals(*r4));
  ASSERT_OK(ConcatenateTables({r1, r4}, &concatenated));
  ASSERT_TRUE(table->Equals(*concatenated));
}

TEST(TestArrowReadWrite, GetRecordBatchReader) {
  const int num_columns = 20;
  const int num_rows = 1000;

  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));

  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(WriteTableToBuffer(table, num_rows / 2,
                                             default_arrow_writer_properties(), &buffer));

  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                              ::arrow::default_memory_pool(),
                              ::parquet::default_reader_properties(), nullptr, &reader));

  std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
  ASSERT_OK_NO_THROW(reader->GetRecordBatchReader({0, 1}, &rb_reader));

  std::shared_ptr<::arrow::RecordBatch> batch;

  ASSERT_OK(rb_reader->ReadNext(&batch));
  ASSERT_EQ(500, batch->num_rows());
  ASSERT_EQ(20, batch->num_columns());

  ASSERT_OK(rb_reader->ReadNext(&batch));
  ASSERT_EQ(500, batch->num_rows());
  ASSERT_EQ(20, batch->num_columns());

  ASSERT_OK(rb_reader->ReadNext(&batch));
  ASSERT_EQ(nullptr, batch);
}

TEST(TestArrowReadWrite, ScanContents) {
  const int num_columns = 20;
  const int num_rows = 1000;

  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));

  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(WriteTableToBuffer(table, num_rows / 2,
                                             default_arrow_writer_properties(), &buffer));

  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                              ::arrow::default_memory_pool(),
                              ::parquet::default_reader_properties(), nullptr, &reader));

  int64_t num_rows_returned = 0;
  ASSERT_OK_NO_THROW(reader->ScanContents({}, 256, &num_rows_returned));
  ASSERT_EQ(num_rows, num_rows_returned);

  ASSERT_OK_NO_THROW(reader->ScanContents({0, 1, 2}, 256, &num_rows_returned));
  ASSERT_EQ(num_rows, num_rows_returned);
}

TEST(TestArrowReadWrite, ReadColumnSubset) {
  const int num_columns = 20;
  const int num_rows = 1000;
  const bool use_threads = true;

  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));

  std::shared_ptr<Table> result;
  std::vector<int> column_subset = {0, 4, 8, 10};
  ASSERT_NO_FATAL_FAILURE(
      DoSimpleRoundtrip(table, use_threads, table->num_rows(), column_subset, &result));

  std::vector<std::shared_ptr<::arrow::Column>> ex_columns;
  std::vector<std::shared_ptr<::arrow::Field>> ex_fields;
  for (int i : column_subset) {
    ex_columns.push_back(table->column(i));
    ex_fields.push_back(table->column(i)->field());
  }

  auto ex_schema = ::arrow::schema(ex_fields);
  auto expected = Table::Make(ex_schema, ex_columns);
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*expected, *result));
}

TEST(TestArrowReadWrite, ListLargeRecords) {
  // PARQUET-1308: This test passed on Linux when num_rows was smaller
  const int num_rows = 2000;
  const int row_group_size = 100;

  std::shared_ptr<Array> list_array;
  std::shared_ptr<::DataType> list_type;

  MakeListArray(num_rows, 20, &list_type, &list_array);

  auto schema = ::arrow::schema({::arrow::field("a", list_type)});

  std::shared_ptr<Table> table = Table::Make(schema, {list_array});

  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(WriteTableToBuffer(table, row_group_size,
                                             default_arrow_writer_properties(), &buffer));

  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                              ::arrow::default_memory_pool(),
                              ::parquet::default_reader_properties(), nullptr, &reader));

  // Read everything
  std::shared_ptr<Table> result;
  ASSERT_OK_NO_THROW(reader->ReadTable(&result));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result));

  // Read 1 record at a time
  ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                              ::arrow::default_memory_pool(),
                              ::parquet::default_reader_properties(), nullptr, &reader));

  std::unique_ptr<ColumnReader> col_reader;
  ASSERT_OK(reader->GetColumn(0, &col_reader));

  std::vector<std::shared_ptr<Array>> pieces;
  for (int i = 0; i < num_rows; ++i) {
    std::shared_ptr<ChunkedArray> chunked_piece;
    ASSERT_OK(col_reader->NextBatch(1, &chunked_piece));
    ASSERT_EQ(1, chunked_piece->length());
    ASSERT_EQ(1, chunked_piece->num_chunks());
    pieces.push_back(chunked_piece->chunk(0));
  }
  auto chunked = std::make_shared<::arrow::ChunkedArray>(pieces);

  auto chunked_col =
      std::make_shared<::arrow::Column>(table->schema()->field(0), chunked);
  std::vector<std::shared_ptr<::arrow::Column>> columns = {chunked_col};
  auto chunked_table = Table::Make(table->schema(), columns);

  ASSERT_TRUE(table->Equals(*chunked_table));
}

typedef std::function<void(int, std::shared_ptr<::DataType>*, std::shared_ptr<Array>*)>
    ArrayFactory;

template <typename ArrowType>
struct GenerateArrayFunctor {
  explicit GenerateArrayFunctor(double pct_null = 0.1) : pct_null(pct_null) {}

  void operator()(int length, std::shared_ptr<::DataType>* type,
                  std::shared_ptr<Array>* array) {
    using T = typename ArrowType::c_type;

    // TODO(wesm): generate things other than integers
    std::vector<T> draws;
    randint(length, 0, 100, &draws);

    std::vector<bool> is_valid;
    random_is_valid(length, this->pct_null, &is_valid);

    *type = ::arrow::TypeTraits<ArrowType>::type_singleton();
    ::arrow::ArrayFromVector<ArrowType, T>(*type, is_valid, draws, array);
  }

  double pct_null;
};

typedef std::function<void(int, std::shared_ptr<::DataType>*, std::shared_ptr<Array>*)>
    ArrayFactory;

auto GenerateInt32 = [](int length, std::shared_ptr<::DataType>* type,
                        std::shared_ptr<Array>* array) {
  GenerateArrayFunctor<::arrow::Int32Type> func;
  func(length, type, array);
};

auto GenerateList = [](int length, std::shared_ptr<::DataType>* type,
                       std::shared_ptr<Array>* array) {
  MakeListArray(length, 100, type, array);
};

std::shared_ptr<Table> InvalidTable() {
  auto type = ::arrow::int8();
  auto field = ::arrow::field("a", type);
  auto schema = ::arrow::schema({field, field});

  // Invalid due to array size not matching
  auto array1 = ArrayFromJSON(type, "[1, 2]");
  auto array2 = ArrayFromJSON(type, "[1]");
  return Table::Make(schema, {array1, array2});
}

TEST(TestArrowReadWrite, InvalidTable) {
  // ARROW-4774: Shouldn't segfault on writing an invalid table.
  auto sink = std::make_shared<InMemoryOutputStream>();
  auto invalid_table = InvalidTable();

  ASSERT_RAISES(Invalid, WriteTable(*invalid_table, ::arrow::default_memory_pool(), sink,
                                    1, default_writer_properties(),
                                    default_arrow_writer_properties()));
}

TEST(TestArrowReadWrite, TableWithChunkedColumns) {
  std::vector<ArrayFactory> functions = {GenerateInt32, GenerateList};

  std::vector<int> chunk_sizes = {2, 4, 10, 2};
  const int64_t total_length = 18;

  for (const auto& datagen_func : functions) {
    ::arrow::ArrayVector arrays;
    std::shared_ptr<Array> arr;
    std::shared_ptr<::DataType> type;
    datagen_func(total_length, &type, &arr);

    int64_t offset = 0;
    for (int chunk_size : chunk_sizes) {
      arrays.push_back(arr->Slice(offset, chunk_size));
      offset += chunk_size;
    }

    auto field = ::arrow::field("fname", type);
    auto schema = ::arrow::schema({field});
    auto col = std::make_shared<::arrow::Column>(field, arrays);
    auto table = Table::Make(schema, {col});

    ASSERT_NO_FATAL_FAILURE(CheckSimpleRoundtrip(table, 2));
    ASSERT_NO_FATAL_FAILURE(CheckSimpleRoundtrip(table, 3));
    ASSERT_NO_FATAL_FAILURE(CheckSimpleRoundtrip(table, 10));
  }
}

TEST(TestArrowReadWrite, TableWithDuplicateColumns) {
  // See ARROW-1974
  using ::arrow::ArrayFromVector;

  auto f0 = field("duplicate", ::arrow::int8());
  auto f1 = field("duplicate", ::arrow::int16());
  auto schema = ::arrow::schema({f0, f1});

  std::vector<int8_t> a0_values = {1, 2, 3};
  std::vector<int16_t> a1_values = {14, 15, 16};

  std::shared_ptr<Array> a0, a1;

  ArrayFromVector<::arrow::Int8Type, int8_t>(a0_values, &a0);
  ArrayFromVector<::arrow::Int16Type, int16_t>(a1_values, &a1);

  auto table = Table::Make(schema, {std::make_shared<Column>(f0->name(), a0),
                                    std::make_shared<Column>(f1->name(), a1)});
  ASSERT_NO_FATAL_FAILURE(CheckSimpleRoundtrip(table, table->num_rows()));
}

TEST(TestArrowReadWrite, DictionaryColumnChunkedWrite) {
  // This is a regression test for this:
  //
  // https://issues.apache.org/jira/browse/ARROW-1938
  //
  // As of the writing of this test, columns of type
  // dictionary are written as their raw/expanded values.
  // The regression was that the whole column was being
  // written for each chunk.
  using ::arrow::ArrayFromVector;

  std::vector<std::string> values = {"first", "second", "third"};
  auto type = ::arrow::utf8();
  std::shared_ptr<Array> dict_values;
  ArrayFromVector<::arrow::StringType, std::string>(values, &dict_values);

  auto dict_type = ::arrow::dictionary(::arrow::int32(), dict_values);
  auto f0 = field("dictionary", dict_type);
  std::vector<std::shared_ptr<::arrow::Field>> fields;
  fields.emplace_back(f0);
  auto schema = ::arrow::schema(fields);

  std::shared_ptr<Array> f0_values, f1_values;
  ArrayFromVector<::arrow::Int32Type, int32_t>({0, 1, 0, 2, 1}, &f0_values);
  ArrayFromVector<::arrow::Int32Type, int32_t>({2, 0, 1, 0, 2}, &f1_values);
  ::arrow::ArrayVector dict_arrays = {
      std::make_shared<::arrow::DictionaryArray>(dict_type, f0_values),
      std::make_shared<::arrow::DictionaryArray>(dict_type, f1_values)};

  std::vector<std::shared_ptr<::arrow::Column>> columns;
  auto column = MakeColumn("dictionary", dict_arrays, true);
  columns.emplace_back(column);

  auto table = Table::Make(schema, columns);

  std::shared_ptr<Table> result;
  ASSERT_NO_FATAL_FAILURE(DoSimpleRoundtrip(table, 1,
                                            // Just need to make sure that we make
                                            // a chunk size that is smaller than the
                                            // total number of values
                                            2, {}, &result));

  std::vector<std::string> expected_values = {"first",  "second", "first", "third",
                                              "second", "third",  "first", "second",
                                              "first",  "third"};
  columns.clear();

  std::shared_ptr<Array> expected_array;
  ArrayFromVector<::arrow::StringType, std::string>(expected_values, &expected_array);

  // The column name gets changed on output to the name of the
  // field, and it also turns into a nullable column
  columns.emplace_back(MakeColumn("dictionary", expected_array, true));

  schema = ::arrow::schema({::arrow::field("dictionary", ::arrow::utf8())});

  auto expected_table = Table::Make(schema, columns);

  ::arrow::AssertTablesEqual(*expected_table, *result, false);
}

TEST(TestArrowWrite, CheckChunkSize) {
  const int num_columns = 2;
  const int num_rows = 128;
  const int64_t chunk_size = 0;  // note the chunk_size is 0
  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));

  auto sink = std::make_shared<InMemoryOutputStream>();

  ASSERT_RAISES(Invalid,
                WriteTable(*table, ::arrow::default_memory_pool(), sink, chunk_size));
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

    writer_ = parquet::ParquetFileWriter::Open(nested_parquet_, schema,
                                               default_writer_properties());
    row_group_writer_ = writer_->AppendRowGroup();
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

  void WriteColumnData(size_t num_rows, int16_t* def_levels, int16_t* rep_levels,
                       int32_t* values) {
    auto typed_writer =
        static_cast<TypedColumnWriter<Int32Type>*>(row_group_writer_->NextColumn());
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

  void ValidateColumnArray(const ::arrow::Int32Array& array, size_t expected_nulls) {
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

    parquet_fields.push_back(GroupNode::Make(
        "group1", struct_repetition,
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
    for (int i = 0; i < NUM_SIMPLE_TEST_ROWS; i++) {
      // leaf1 is required within the optional group1, so it is only null
      // when the group is null
      leaf1_def_levels[i] = (i % 3 == 0) ? 0 : 1;
      // leaf2 is optional, can be null in the primitive (def-level 1) or
      // struct level (def-level 0)
      leaf2_def_levels[i] = static_cast<int16_t>(i % 3);
      // leaf3 is required
      leaf3_def_levels[i] = 0;
    }

    std::vector<int16_t> rep_levels(NUM_SIMPLE_TEST_ROWS, 0);

    // Produce values for the columns
    MakeValues(NUM_SIMPLE_TEST_ROWS);
    int32_t* values = reinterpret_cast<int32_t*>(values_array_->values()->mutable_data());

    // Create the actual parquet file
    InitNewParquetFile(std::static_pointer_cast<GroupNode>(schema_node),
                       NUM_SIMPLE_TEST_ROWS);

    // leaf1 column
    WriteColumnData(NUM_SIMPLE_TEST_ROWS, leaf1_def_levels.data(), rep_levels.data(),
                    values);
    // leaf2 column
    WriteColumnData(NUM_SIMPLE_TEST_ROWS, leaf2_def_levels.data(), rep_levels.data(),
                    values);
    // leaf3 column
    WriteColumnData(NUM_SIMPLE_TEST_ROWS, leaf3_def_levels.data(), rep_levels.data(),
                    values);

    FinalizeParquetFile();
    InitReader();
  }

  NodePtr CreateSingleTypedNestedGroup(int index, int depth, int num_children,
                                       Repetition::type node_repetition,
                                       ParquetType::type leaf_type) {
    std::vector<NodePtr> children;

    for (int i = 0; i < num_children; i++) {
      if (depth <= 1) {
        children.push_back(PrimitiveNode::Make("leaf", node_repetition, leaf_type));
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
  void CreateMultiLevelNestedParquet(int num_trees, int tree_depth, int num_children,
                                     int num_rows, Repetition::type node_repetition) {
    // Create the schema
    std::vector<NodePtr> parquet_fields;
    for (int i = 0; i < num_trees; i++) {
      parquet_fields.push_back(CreateSingleTypedNestedGroup(
          i, tree_depth, num_children, node_repetition, ParquetType::INT32));
    }
    auto schema_node = GroupNode::Make("schema", Repetition::REQUIRED, parquet_fields);

    int num_columns = num_trees * static_cast<int>((std::pow(num_children, tree_depth)));

    std::vector<int16_t> def_levels;
    std::vector<int16_t> rep_levels;

    int num_levels = 0;
    while (num_levels < num_rows) {
      if (node_repetition == Repetition::REQUIRED) {
        def_levels.push_back(0);  // all are required
      } else {
        int16_t level = static_cast<int16_t>(num_levels % (tree_depth + 2));
        def_levels.push_back(level);  // all are optional
      }
      rep_levels.push_back(0);  // none is repeated
      ++num_levels;
    }

    // Produce values for the columns
    MakeValues(num_rows);
    int32_t* values = reinterpret_cast<int32_t*>(values_array_->values()->mutable_data());

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
                           std::shared_ptr<::arrow::Int32Array> expected)
        : node_repetition_(node_repetition), expected_(expected) {}

    Status Validate(std::shared_ptr<Array> tree) { return tree->Accept(this); }

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
      for (int32_t i = 0; i < array.num_fields(); ++i) {
        auto child = array.field(i);
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
  ASSERT_NO_FATAL_FAILURE(CreateSimpleNestedParquet(Repetition::OPTIONAL));

  std::shared_ptr<Table> table;
  ASSERT_OK_NO_THROW(reader_->ReadTable(&table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 2);
  ASSERT_EQ(table->schema()->field(0)->type()->num_children(), 2);
  ASSERT_NO_FATAL_FAILURE(ValidateTableArrayTypes(*table));

  auto struct_field_array =
      std::static_pointer_cast<::arrow::StructArray>(table->column(0)->data()->chunk(0));
  auto leaf1_array =
      std::static_pointer_cast<::arrow::Int32Array>(struct_field_array->field(0));
  auto leaf2_array =
      std::static_pointer_cast<::arrow::Int32Array>(struct_field_array->field(1));
  auto leaf3_array =
      std::static_pointer_cast<::arrow::Int32Array>(table->column(1)->data()->chunk(0));

  // validate struct and leaf arrays

  // validate struct array
  ASSERT_NO_FATAL_FAILURE(ValidateArray(*struct_field_array, NUM_SIMPLE_TEST_ROWS / 3));
  // validate leaf1
  ASSERT_NO_FATAL_FAILURE(ValidateColumnArray(*leaf1_array, NUM_SIMPLE_TEST_ROWS / 3));
  // validate leaf2
  ASSERT_NO_FATAL_FAILURE(
      ValidateColumnArray(*leaf2_array, NUM_SIMPLE_TEST_ROWS * 2 / 3));
  // validate leaf3
  ASSERT_NO_FATAL_FAILURE(ValidateColumnArray(*leaf3_array, 0));
}

TEST_F(TestNestedSchemaRead, ReadTablePartial) {
  ASSERT_NO_FATAL_FAILURE(CreateSimpleNestedParquet(Repetition::OPTIONAL));
  std::shared_ptr<Table> table;

  // columns: {group1.leaf1, leaf3}
  ASSERT_OK_NO_THROW(reader_->ReadTable({0, 2}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 2);
  ASSERT_EQ(table->schema()->field(0)->name(), "group1");
  ASSERT_EQ(table->schema()->field(1)->name(), "leaf3");
  ASSERT_EQ(table->schema()->field(0)->type()->num_children(), 1);
  ASSERT_NO_FATAL_FAILURE(ValidateTableArrayTypes(*table));

  // columns: {group1.leaf1, leaf3}
  ASSERT_OK_NO_THROW(reader_->ReadRowGroup(0, {0, 2}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 2);
  ASSERT_EQ(table->schema()->field(0)->name(), "group1");
  ASSERT_EQ(table->schema()->field(1)->name(), "leaf3");
  ASSERT_EQ(table->schema()->field(0)->type()->num_children(), 1);
  ASSERT_NO_FATAL_FAILURE(ValidateTableArrayTypes(*table));

  // columns: {group1.leaf1, group1.leaf2}
  ASSERT_OK_NO_THROW(reader_->ReadTable({0, 1}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 1);
  ASSERT_EQ(table->schema()->field(0)->name(), "group1");
  ASSERT_EQ(table->schema()->field(0)->type()->num_children(), 2);
  ASSERT_NO_FATAL_FAILURE(ValidateTableArrayTypes(*table));

  // columns: {leaf3}
  ASSERT_OK_NO_THROW(reader_->ReadTable({2}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 1);
  ASSERT_EQ(table->schema()->field(0)->name(), "leaf3");
  ASSERT_EQ(table->schema()->field(0)->type()->num_children(), 0);
  ASSERT_NO_FATAL_FAILURE(ValidateTableArrayTypes(*table));

  // Test with different ordering
  ASSERT_OK_NO_THROW(reader_->ReadTable({2, 0}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 2);
  ASSERT_EQ(table->schema()->field(0)->name(), "leaf3");
  ASSERT_EQ(table->schema()->field(1)->name(), "group1");
  ASSERT_EQ(table->schema()->field(1)->type()->num_children(), 1);
  ASSERT_NO_FATAL_FAILURE(ValidateTableArrayTypes(*table));
}

TEST_F(TestNestedSchemaRead, StructAndListTogetherUnsupported) {
  ASSERT_NO_FATAL_FAILURE(CreateSimpleNestedParquet(Repetition::REPEATED));
  std::shared_ptr<Table> table;
  ASSERT_RAISES(NotImplemented, reader_->ReadTable(&table));
}

TEST_P(TestNestedSchemaRead, DeepNestedSchemaRead) {
#ifdef PARQUET_VALGRIND
  const int num_trees = 3;
  const int depth = 3;
#else
  const int num_trees = 5;
  const int depth = 5;
#endif
  const int num_children = 3;
  int num_rows = SMALL_SIZE * (depth + 2);
  ASSERT_NO_FATAL_FAILURE(CreateMultiLevelNestedParquet(num_trees, depth, num_children,
                                                        num_rows, GetParam()));
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

TEST(TestImpalaConversion, ArrowTimestampToImpalaTimestamp) {
  // June 20, 2017 16:32:56 and 123456789 nanoseconds
  int64_t nanoseconds = INT64_C(1497976376123456789);

  Int96 calculated;

  Int96 expected = {{UINT32_C(632093973), UINT32_C(13871), UINT32_C(2457925)}};
  internal::NanosecondsToImpalaTimestamp(nanoseconds, &calculated);
  ASSERT_EQ(expected, calculated);
}

void TryReadDataFile(const std::string& path,
                     ::arrow::StatusCode expected_code = ::arrow::StatusCode::OK) {
  auto pool = ::arrow::default_memory_pool();

  std::unique_ptr<FileReader> arrow_reader;

  arrow_reader.reset(new FileReader(pool, ParquetFileReader::OpenFile(path, false)));
  std::shared_ptr<::arrow::Table> table;
  auto status = arrow_reader->ReadTable(&table);

  ASSERT_TRUE(status.code() == expected_code)
      << "Expected reading file to return "
      << arrow::Status(expected_code, "").CodeAsString() << ", but got "
      << status.ToString();
}

TEST(TestArrowReaderAdHoc, Int96BadMemoryAccess) {
  // PARQUET-995
  TryReadDataFile(test::get_data_file("alltypes_plain.parquet"));
}

TEST(TestArrowReaderAdHoc, CorruptedSchema) {
  // PARQUET-1481
  auto path = test::get_data_file("PARQUET-1481.parquet", /*is_good=*/false);
  TryReadDataFile(path, ::arrow::StatusCode::IOError);
}

class TestArrowReaderAdHocSparkAndHvr
    : public ::testing::TestWithParam<
          std::tuple<std::string, std::shared_ptr<::DataType>>> {};

TEST_P(TestArrowReaderAdHocSparkAndHvr, ReadDecimals) {
  std::string path(test::get_data_dir());

  std::string filename;
  std::shared_ptr<::DataType> decimal_type;
  std::tie(filename, decimal_type) = GetParam();

  path += "/" + filename;
  ASSERT_GT(path.size(), 0);

  auto pool = ::arrow::default_memory_pool();

  std::unique_ptr<FileReader> arrow_reader;
  ASSERT_NO_THROW(
      arrow_reader.reset(new FileReader(pool, ParquetFileReader::OpenFile(path, false))));
  std::shared_ptr<::arrow::Table> table;
  ASSERT_OK_NO_THROW(arrow_reader->ReadTable(&table));

  ASSERT_EQ(1, table->num_columns());

  constexpr int32_t expected_length = 24;

  auto value_column = table->column(0);
  ASSERT_EQ(expected_length, value_column->length());

  auto raw_array = value_column->data();
  ASSERT_EQ(1, raw_array->num_chunks());

  auto chunk = raw_array->chunk(0);

  std::shared_ptr<Array> expected_array;

  ::arrow::Decimal128Builder builder(decimal_type, pool);

  for (int32_t i = 0; i < expected_length; ++i) {
    ::arrow::Decimal128 value((i + 1) * 100);
    ASSERT_OK(builder.Append(value));
  }
  ASSERT_OK(builder.Finish(&expected_array));

  AssertArraysEqual(*expected_array, *chunk);
}

INSTANTIATE_TEST_CASE_P(
    ReadDecimals, TestArrowReaderAdHocSparkAndHvr,
    ::testing::Values(
        std::make_tuple("int32_decimal.parquet", ::arrow::decimal(4, 2)),
        std::make_tuple("int64_decimal.parquet", ::arrow::decimal(10, 2)),
        std::make_tuple("fixed_length_decimal.parquet", ::arrow::decimal(25, 2)),
        std::make_tuple("fixed_length_decimal_legacy.parquet", ::arrow::decimal(13, 2)),
        std::make_tuple("byte_array_decimal.parquet", ::arrow::decimal(4, 2))));

// direct-as-possible translation of
// pyarrow/tests/test_parquet.py::test_validate_schema_write_table
TEST(TestArrowWriterAdHoc, SchemaMismatch) {
  auto pool = ::arrow::default_memory_pool();
  auto writer_schm = ::arrow::schema({field("POS", ::arrow::uint32())});
  auto table_schm = ::arrow::schema({field("POS", ::arrow::int64())});
  using ::arrow::io::BufferOutputStream;
  std::shared_ptr<BufferOutputStream> outs;
  ASSERT_OK(BufferOutputStream::Create(1 << 10, pool, &outs));
  auto props = default_writer_properties();
  std::unique_ptr<arrow::FileWriter> writer;
  ASSERT_OK(arrow::FileWriter::Open(*writer_schm, pool, outs, props, &writer));
  std::shared_ptr<::arrow::Array> col;
  ::arrow::Int64Builder builder;
  ASSERT_OK(builder.Append(1));
  ASSERT_OK(builder.Finish(&col));
  auto tbl = ::arrow::Table::Make(table_schm, {col});
  ASSERT_RAISES(Invalid, writer->WriteTable(*tbl, 1));
}

// ----------------------------------------------------------------------
// Tests for directly reading DictionaryArray
class TestArrowReadDictionary : public ::testing::TestWithParam<double> {
 public:
  void SetUp() override {
    GenerateData(GetParam());
    ASSERT_NO_FATAL_FAILURE(
        WriteTableToBuffer(expected_dense_, expected_dense_->num_rows() / 2,
                           default_arrow_writer_properties(), &buffer_));

    properties_ = default_arrow_reader_properties();
  }

  void GenerateData(double null_probability) {
    constexpr int num_unique = 100;
    constexpr int repeat = 10;
    constexpr int64_t min_length = 2;
    constexpr int64_t max_length = 10;
    ::arrow::random::RandomArrayGenerator rag(0);
    auto dense_array = rag.StringWithRepeats(repeat * num_unique, num_unique, min_length,
                                             max_length, null_probability);
    expected_dense_ = MakeSimpleTable(dense_array, /*nullable=*/true);

    ::arrow::StringDictionaryBuilder builder(default_memory_pool());
    const auto& string_array = static_cast<const ::arrow::StringArray&>(*dense_array);
    ASSERT_OK(builder.AppendArray(string_array));

    std::shared_ptr<::arrow::Array> dict_array;
    ASSERT_OK(builder.Finish(&dict_array));
    expected_dict_ = MakeSimpleTable(dict_array, /*nullable=*/true);

    // TODO(hatemhelal): Figure out if we can use the following to init the expected_dict_
    // Currently fails due to DataType mismatch for indices array.
    //    Datum out;
    //    FunctionContext ctx(default_memory_pool());
    //    ASSERT_OK(DictionaryEncode(&ctx, Datum(dense_array), &out));
    //    expected_dict_ = MakeSimpleTable(out.make_array(), /*nullable=*/true);
  }

  void TearDown() override {}

  void CheckReadWholeFile(const Table& expected) {
    std::unique_ptr<FileReader> reader;
    ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer_),
                                ::arrow::default_memory_pool(), properties_, &reader));

    std::shared_ptr<Table> actual;
    ASSERT_OK_NO_THROW(reader->ReadTable(&actual));
    ::arrow::AssertTablesEqual(*actual, expected, /*same_chunk_layout=*/false);
  }

  static std::vector<double> null_probabilites() { return {0.0, 0.5, 1}; }

 protected:
  std::shared_ptr<Table> expected_dense_;
  std::shared_ptr<Table> expected_dict_;
  std::shared_ptr<Buffer> buffer_;
  ArrowReaderProperties properties_;
};

TEST_P(TestArrowReadDictionary, ReadWholeFileDict) {
  properties_.set_read_dictionary(0, true);
  CheckReadWholeFile(*expected_dict_);
}

TEST_P(TestArrowReadDictionary, ReadWholeFileDense) {
  properties_.set_read_dictionary(0, false);
  CheckReadWholeFile(*expected_dense_);
}

INSTANTIATE_TEST_CASE_P(
    ReadDictionary, TestArrowReadDictionary,
    ::testing::ValuesIn(TestArrowReadDictionary::null_probabilites()));

}  // namespace arrow

}  // namespace parquet
