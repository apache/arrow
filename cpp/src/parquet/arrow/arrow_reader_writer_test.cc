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

#include <cstdint>
#include <functional>
#include <iostream>
#include <sstream>
#include <vector>

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api.h"
#include "arrow/io/api.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/range.h"

#ifdef ARROW_CSV
#include "arrow/csv/api.h"
#endif

#include "parquet/api/reader.h"
#include "parquet/api/writer.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/reader_internal.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/test_util.h"
#include "parquet/arrow/writer.h"
#include "parquet/column_writer.h"
#include "parquet/file_writer.h"
#include "parquet/test_util.h"

using arrow::Array;
using arrow::ArrayData;
using arrow::ArrayFromJSON;
using arrow::ArrayVector;
using arrow::ArrayVisitor;
using arrow::Buffer;
using arrow::ChunkedArray;
using arrow::DataType;
using arrow::Datum;
using arrow::DecimalType;
using arrow::default_memory_pool;
using arrow::ListArray;
using arrow::PrimitiveArray;
using arrow::ResizableBuffer;
using arrow::Scalar;
using arrow::Status;
using arrow::Table;
using arrow::TimeUnit;
using arrow::compute::DictionaryEncode;
using arrow::internal::checked_cast;
using arrow::internal::checked_pointer_cast;
using arrow::internal::Iota;
using arrow::io::BufferReader;

using arrow::randint;
using arrow::random_is_valid;

using ArrowId = ::arrow::Type;
using ParquetType = parquet::Type;

using parquet::arrow::FromParquetSchema;
using parquet::schema::GroupNode;
using parquet::schema::NodePtr;
using parquet::schema::PrimitiveNode;

namespace parquet {
namespace arrow {

static constexpr int SMALL_SIZE = 100;
#ifdef PARQUET_VALGRIND
static constexpr int LARGE_SIZE = 1000;
#else
static constexpr int LARGE_SIZE = 10000;
#endif

static constexpr uint32_t kDefaultSeed = 0;

std::shared_ptr<const LogicalType> get_logical_type(const DataType& type) {
  switch (type.id()) {
    case ArrowId::UINT8:
      return LogicalType::Int(8, false);
    case ArrowId::INT8:
      return LogicalType::Int(8, true);
    case ArrowId::UINT16:
      return LogicalType::Int(16, false);
    case ArrowId::INT16:
      return LogicalType::Int(16, true);
    case ArrowId::UINT32:
      return LogicalType::Int(32, false);
    case ArrowId::INT32:
      return LogicalType::Int(32, true);
    case ArrowId::UINT64:
      return LogicalType::Int(64, false);
    case ArrowId::INT64:
      return LogicalType::Int(64, true);
    case ArrowId::STRING:
      return LogicalType::String();
    case ArrowId::DATE32:
      return LogicalType::Date();
    case ArrowId::DATE64:
      return LogicalType::Date();
    case ArrowId::TIMESTAMP: {
      const auto& ts_type = static_cast<const ::arrow::TimestampType&>(type);
      const bool adjusted_to_utc = !(ts_type.timezone().empty());
      switch (ts_type.unit()) {
        case TimeUnit::MILLI:
          return LogicalType::Timestamp(adjusted_to_utc, LogicalType::TimeUnit::MILLIS);
        case TimeUnit::MICRO:
          return LogicalType::Timestamp(adjusted_to_utc, LogicalType::TimeUnit::MICROS);
        case TimeUnit::NANO:
          return LogicalType::Timestamp(adjusted_to_utc, LogicalType::TimeUnit::NANOS);
        default:
          DCHECK(false)
              << "Only MILLI, MICRO, and NANO units supported for Arrow TIMESTAMP.";
      }
      break;
    }
    case ArrowId::TIME32:
      return LogicalType::Time(false, LogicalType::TimeUnit::MILLIS);
    case ArrowId::TIME64: {
      const auto& tm_type = static_cast<const ::arrow::TimeType&>(type);
      switch (tm_type.unit()) {
        case TimeUnit::MICRO:
          return LogicalType::Time(false, LogicalType::TimeUnit::MICROS);
        case TimeUnit::NANO:
          return LogicalType::Time(false, LogicalType::TimeUnit::NANOS);
        default:
          DCHECK(false) << "Only MICRO and NANO units supported for Arrow TIME64.";
      }
      break;
    }
    case ArrowId::DICTIONARY: {
      const ::arrow::DictionaryType& dict_type =
          static_cast<const ::arrow::DictionaryType&>(type);
      return get_logical_type(*dict_type.value_type());
    }
    case ArrowId::DECIMAL128: {
      const auto& dec_type = static_cast<const ::arrow::Decimal128Type&>(type);
      return LogicalType::Decimal(dec_type.precision(), dec_type.scale());
    }
    case ArrowId::DECIMAL256: {
      const auto& dec_type = static_cast<const ::arrow::Decimal256Type&>(type);
      return LogicalType::Decimal(dec_type.precision(), dec_type.scale());
    }

    default:
      break;
  }
  return LogicalType::None();
}

ParquetType::type get_physical_type(const DataType& type) {
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
    case ArrowId::LARGE_BINARY:
      return ParquetType::BYTE_ARRAY;
    case ArrowId::STRING:
    case ArrowId::LARGE_STRING:
      return ParquetType::BYTE_ARRAY;
    case ArrowId::FIXED_SIZE_BINARY:
    case ArrowId::DECIMAL128:
    case ArrowId::DECIMAL256:
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
      return get_physical_type(*dict_type.value_type());
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

const std::string test_traits<::arrow::StringType>::value("Test");            // NOLINT
const std::string test_traits<::arrow::BinaryType>::value({0, 1, 2, 3});      // NOLINT
const std::string test_traits<::arrow::FixedSizeBinaryType>::value("Fixed");  // NOLINT

template <typename T>
using ParquetDataType = PhysicalType<test_traits<T>::parquet_enum>;

template <typename T>
using ParquetWriter = TypedColumnWriter<ParquetDataType<T>>;

void WriteTableToBuffer(const std::shared_ptr<Table>& table, int64_t row_group_size,
                        const std::shared_ptr<ArrowWriterProperties>& arrow_properties,
                        std::shared_ptr<Buffer>* out) {
  auto sink = CreateOutputStream();

  auto write_props = WriterProperties::Builder().write_batch_size(100)->build();

  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), sink,
                                row_group_size, write_props, arrow_properties));
  ASSERT_OK_AND_ASSIGN(*out, sink->Finish());
}

void DoRoundtrip(const std::shared_ptr<Table>& table, int64_t row_group_size,
                 std::shared_ptr<Table>* out,
                 const std::shared_ptr<::parquet::WriterProperties>& writer_properties =
                     ::parquet::default_writer_properties(),
                 const std::shared_ptr<ArrowWriterProperties>& arrow_writer_properties =
                     default_arrow_writer_properties(),
                 const ArrowReaderProperties& arrow_reader_properties =
                     default_arrow_reader_properties()) {
  auto sink = CreateOutputStream();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), sink,
                                row_group_size, writer_properties,
                                arrow_writer_properties));
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

  std::unique_ptr<FileReader> reader;
  FileReaderBuilder builder;
  ASSERT_OK_NO_THROW(builder.Open(std::make_shared<BufferReader>(buffer)));
  ASSERT_OK(builder.properties(arrow_reader_properties)->Build(&reader));
  ASSERT_OK_NO_THROW(reader->ReadTable(out));
}

void CheckConfiguredRoundtrip(
    const std::shared_ptr<Table>& input_table,
    const std::shared_ptr<Table>& expected_table = nullptr,
    const std::shared_ptr<::parquet::WriterProperties>& writer_properties =
        ::parquet::default_writer_properties(),
    const std::shared_ptr<ArrowWriterProperties>& arrow_writer_properties =
        default_arrow_writer_properties()) {
  std::shared_ptr<Table> actual_table;
  ASSERT_NO_FATAL_FAILURE(DoRoundtrip(input_table, input_table->num_rows(), &actual_table,
                                      writer_properties, arrow_writer_properties));
  if (expected_table) {
    ASSERT_NO_FATAL_FAILURE(::arrow::AssertSchemaEqual(*actual_table->schema(),
                                                       *expected_table->schema(),
                                                       /*check_metadata=*/false));
    ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*expected_table, *actual_table));
  } else {
    ASSERT_NO_FATAL_FAILURE(::arrow::AssertSchemaEqual(*actual_table->schema(),
                                                       *input_table->schema(),
                                                       /*check_metadata=*/false));
    ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*input_table, *actual_table));
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
                              ::arrow::default_memory_pool(), &reader));

  reader->set_use_threads(use_threads);
  if (column_subset.size() > 0) {
    ASSERT_OK_NO_THROW(reader->ReadTable(column_subset, out));
  } else {
    // Read everything
    ASSERT_OK_NO_THROW(reader->ReadTable(out));
  }
}

void DoRoundTripWithBatches(
    const std::shared_ptr<Table>& table, bool use_threads, int64_t row_group_size,
    const std::vector<int>& column_subset, std::shared_ptr<Table>* out,
    const std::shared_ptr<ArrowWriterProperties>& arrow_writer_properties =
        default_arrow_writer_properties()) {
  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(
      WriteTableToBuffer(table, row_group_size, arrow_writer_properties, &buffer));

  std::unique_ptr<FileReader> reader;
  FileReaderBuilder builder;
  ASSERT_OK_NO_THROW(builder.Open(std::make_shared<BufferReader>(buffer)));
  ArrowReaderProperties arrow_reader_properties;
  arrow_reader_properties.set_batch_size(row_group_size - 1);
  ASSERT_OK_NO_THROW(builder.memory_pool(::arrow::default_memory_pool())
                         ->properties(arrow_reader_properties)
                         ->Build(&reader));
  std::unique_ptr<::arrow::RecordBatchReader> batch_reader;
  if (column_subset.size() > 0) {
    ASSERT_OK_NO_THROW(reader->GetRecordBatchReader(
        Iota(reader->parquet_reader()->metadata()->num_row_groups()), column_subset,
        &batch_reader));
  } else {
    // Read everything

    ASSERT_OK_NO_THROW(reader->GetRecordBatchReader(
        Iota(reader->parquet_reader()->metadata()->num_row_groups()), &batch_reader));
  }
  ASSERT_OK_AND_ASSIGN(*out, Table::FromRecordBatchReader(batch_reader.get()));
}

void CheckSimpleRoundtrip(
    const std::shared_ptr<Table>& table, int64_t row_group_size,
    const std::shared_ptr<ArrowWriterProperties>& arrow_writer_properties =
        default_arrow_writer_properties()) {
  std::shared_ptr<Table> result;
  ASSERT_NO_FATAL_FAILURE(DoSimpleRoundtrip(table, false /* use_threads */,
                                            row_group_size, {}, &result,
                                            arrow_writer_properties));
  ::arrow::AssertSchemaEqual(*table->schema(), *result->schema(),
                             /*check_metadata=*/false);
  ASSERT_OK(result->ValidateFull());

  ::arrow::AssertTablesEqual(*table, *result, false);

  ASSERT_NO_FATAL_FAILURE(DoRoundTripWithBatches(table, false /* use_threads */,
                                                 row_group_size, {}, &result,
                                                 arrow_writer_properties));
  ::arrow::AssertSchemaEqual(*table->schema(), *result->schema(),
                             /*check_metadata=*/false);
  ASSERT_OK(result->ValidateFull());

  ::arrow::AssertTablesEqual(*table, *result, false);
}

static std::shared_ptr<GroupNode> MakeSimpleSchema(const DataType& type,
                                                   Repetition::type repetition) {
  int32_t byte_width = -1;

  switch (type.id()) {
    case ::arrow::Type::DICTIONARY: {
      const auto& dict_type = static_cast<const ::arrow::DictionaryType&>(type);
      const DataType& values_type = *dict_type.value_type();
      switch (values_type.id()) {
        case ::arrow::Type::FIXED_SIZE_BINARY:
          byte_width =
              static_cast<const ::arrow::FixedSizeBinaryType&>(values_type).byte_width();
          break;
        case ::arrow::Type::DECIMAL128:
        case ::arrow::Type::DECIMAL256: {
          const auto& decimal_type = static_cast<const DecimalType&>(values_type);
          byte_width = DecimalType::DecimalSize(decimal_type.precision());
        } break;
        default:
          break;
      }
    } break;
    case ::arrow::Type::FIXED_SIZE_BINARY:
      byte_width = static_cast<const ::arrow::FixedSizeBinaryType&>(type).byte_width();
      break;
    case ::arrow::Type::DECIMAL128:
    case ::arrow::Type::DECIMAL256: {
      const auto& decimal_type = static_cast<const DecimalType&>(type);
      byte_width = DecimalType::DecimalSize(decimal_type.precision());
    } break;
    default:
      break;
  }
  auto pnode = PrimitiveNode::Make("column1", repetition, get_logical_type(type),
                                   get_physical_type(type), byte_width);
  NodePtr node_ =
      GroupNode::Make("schema", Repetition::REQUIRED, std::vector<NodePtr>({pnode}));
  return std::static_pointer_cast<GroupNode>(node_);
}

void ReadSingleColumnFileStatistics(std::unique_ptr<FileReader> file_reader,
                                    std::shared_ptr<Scalar>* min,
                                    std::shared_ptr<Scalar>* max) {
  auto metadata = file_reader->parquet_reader()->metadata();
  ASSERT_EQ(1, metadata->num_row_groups());
  ASSERT_EQ(1, metadata->num_columns());

  auto row_group = metadata->RowGroup(0);
  ASSERT_EQ(1, row_group->num_columns());

  auto column = row_group->ColumnChunk(0);
  ASSERT_TRUE(column->is_stats_set());
  auto statistics = column->statistics();

  ASSERT_OK(StatisticsAsScalars(*statistics, min, max));
}

void DownsampleInt96RoundTrip(std::shared_ptr<Array> arrow_vector_in,
                              std::shared_ptr<Array> arrow_vector_out,
                              ::arrow::TimeUnit::type unit) {
  // Create single input table of NS to be written to parquet with INT96
  auto input_schema =
      ::arrow::schema({::arrow::field("f", ::arrow::timestamp(TimeUnit::NANO))});
  auto input = Table::Make(input_schema, {arrow_vector_in});

  // Create an expected schema for each resulting table (one for each "downsampled" ts)
  auto ex_schema = ::arrow::schema({::arrow::field("f", ::arrow::timestamp(unit))});
  auto ex_result = Table::Make(ex_schema, {arrow_vector_out});

  std::shared_ptr<Table> result;

  ArrowReaderProperties arrow_reader_prop;
  arrow_reader_prop.set_coerce_int96_timestamp_unit(unit);

  ASSERT_NO_FATAL_FAILURE(DoRoundtrip(
      input, input->num_rows(), &result, default_writer_properties(),
      ArrowWriterProperties::Builder().enable_deprecated_int96_timestamps()->build(),
      arrow_reader_prop));

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertSchemaEqual(*ex_result->schema(),
                                                     *result->schema(),
                                                     /*check_metadata=*/false));

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*ex_result, *result));
}

// Non-template base class for TestParquetIO, to avoid code duplication
class ParquetIOTestBase : public ::testing::Test {
 public:
  virtual void SetUp() {}

  std::unique_ptr<ParquetFileWriter> MakeWriter(
      const std::shared_ptr<GroupNode>& schema) {
    sink_ = CreateOutputStream();
    return ParquetFileWriter::Open(sink_, schema);
  }

  void ReaderFromSink(std::unique_ptr<FileReader>* out) {
    ASSERT_OK_AND_ASSIGN(auto buffer, sink_->Finish());
    ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                                ::arrow::default_memory_pool(), out));
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
    ASSERT_OK((*out)->ValidateFull());
  }

  void ReadAndCheckSingleColumnFile(const Array& values) {
    std::shared_ptr<Array> out;

    std::unique_ptr<FileReader> reader;
    ReaderFromSink(&reader);
    ReadSingleColumnFile(std::move(reader), &out);

    AssertArraysEqual(values, *out);
  }

  void ReadTableFromFile(std::unique_ptr<FileReader> reader, bool expect_metadata,
                         std::shared_ptr<Table>* out) {
    ASSERT_OK_NO_THROW(reader->ReadTable(out));
    auto key_value_metadata =
        reader->parquet_reader()->metadata()->key_value_metadata().get();
    if (!expect_metadata) {
      ASSERT_EQ(nullptr, key_value_metadata);
    } else {
      ASSERT_NE(nullptr, key_value_metadata);
    }
    ASSERT_NE(nullptr, out->get());
  }

  void ReadTableFromFile(std::unique_ptr<FileReader> reader,
                         std::shared_ptr<Table>* out) {
    ReadTableFromFile(std::move(reader), /*expect_metadata=*/false, out);
  }

  void RoundTripSingleColumn(
      const std::shared_ptr<Array>& values, const std::shared_ptr<Array>& expected,
      const std::shared_ptr<::parquet::ArrowWriterProperties>& arrow_properties,
      bool nullable = true) {
    std::shared_ptr<Table> table = MakeSimpleTable(values, nullable);
    this->ResetSink();
    ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                  values->length(), default_writer_properties(),
                                  arrow_properties));

    std::shared_ptr<Table> out;
    std::unique_ptr<FileReader> reader;
    ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
    const bool expect_metadata = arrow_properties->store_schema();
    ASSERT_NO_FATAL_FAILURE(
        this->ReadTableFromFile(std::move(reader), expect_metadata, &out));
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(table->num_rows(), out->num_rows());

    const auto chunked_array = out->column(0);
    ASSERT_EQ(1, chunked_array->num_chunks());

    AssertArraysEqual(*expected, *chunked_array->chunk(0), /*verbose=*/true);
  }

  // Prepare table of empty lists, with null values array (ARROW-2744)
  void PrepareEmptyListsTable(int64_t size, std::shared_ptr<Table>* out) {
    std::shared_ptr<Array> lists;
    ASSERT_OK(MakeEmptyListsArray(size, &lists));
    *out = MakeSimpleTable(lists, true /* nullable_lists */);
  }

  void ReadAndCheckSingleColumnTable(const std::shared_ptr<Array>& values) {
    std::shared_ptr<::arrow::Table> out;
    std::unique_ptr<FileReader> reader;
    ReaderFromSink(&reader);
    ReadTableFromFile(std::move(reader), &out);
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(values->length(), out->num_rows());

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0);
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
    ArrowReaderProperties props;
    ASSERT_OK_NO_THROW(FromParquetSchema(&descriptor, props, &arrow_schema));

    std::unique_ptr<FileWriter> writer;
    ASSERT_OK_NO_THROW(FileWriter::Make(::arrow::default_memory_pool(),
                                        MakeWriter(schema), arrow_schema,
                                        default_arrow_writer_properties(), &writer));
    ASSERT_OK_NO_THROW(writer->NewRowGroup(values->length()));
    ASSERT_OK_NO_THROW(writer->WriteColumnChunk(*values));
    ASSERT_OK_NO_THROW(writer->Close());
    // writer->Close() should be idempotent
    ASSERT_OK_NO_THROW(writer->Close());
  }

  void ResetSink() { sink_ = CreateOutputStream(); }

  std::shared_ptr<::arrow::io::BufferOutputStream> sink_;
};

class TestReadDecimals : public ParquetIOTestBase {
 public:
  void CheckReadFromByteArrays(const std::shared_ptr<const LogicalType>& logical_type,
                               const std::vector<std::vector<uint8_t>>& values,
                               const Array& expected) {
    std::vector<ByteArray> byte_arrays(values.size());
    std::transform(values.begin(), values.end(), byte_arrays.begin(),
                   [](const std::vector<uint8_t>& bytes) {
                     return ByteArray(static_cast<uint32_t>(bytes.size()), bytes.data());
                   });

    auto node = PrimitiveNode::Make("decimals", Repetition::REQUIRED, logical_type,
                                    Type::BYTE_ARRAY);
    auto schema =
        GroupNode::Make("schema", Repetition::REQUIRED, std::vector<NodePtr>{node});

    auto file_writer = MakeWriter(checked_pointer_cast<GroupNode>(schema));
    auto column_writer = file_writer->AppendRowGroup()->NextColumn();
    auto typed_writer = checked_cast<TypedColumnWriter<ByteArrayType>*>(column_writer);
    typed_writer->WriteBatch(static_cast<int64_t>(byte_arrays.size()),
                             /*def_levels=*/nullptr,
                             /*rep_levels=*/nullptr, byte_arrays.data());
    column_writer->Close();
    file_writer->Close();

    ReadAndCheckSingleColumnFile(expected);
  }
};

// The Decimal roundtrip tests always go through the FixedLenByteArray path,
// check the ByteArray case manually.

TEST_F(TestReadDecimals, Decimal128ByteArray) {
  const std::vector<std::vector<uint8_t>> big_endian_decimals = {
      // 123456
      {1, 226, 64},
      // 987654
      {15, 18, 6},
      // -123456
      {255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 254, 29, 192},
  };

  auto expected =
      ArrayFromJSON(::arrow::decimal128(6, 3), R"(["123.456", "987.654", "-123.456"])");
  CheckReadFromByteArrays(LogicalType::Decimal(6, 3), big_endian_decimals, *expected);
}

TEST_F(TestReadDecimals, Decimal256ByteArray) {
  const std::vector<std::vector<uint8_t>> big_endian_decimals = {
      // 123456
      {1, 226, 64},
      // 987654
      {15, 18, 6},
      // -123456
      {255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
       255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 254, 29,  192},
  };

  auto expected =
      ArrayFromJSON(::arrow::decimal256(40, 3), R"(["123.456", "987.654", "-123.456"])");
  CheckReadFromByteArrays(LogicalType::Decimal(40, 3), big_endian_decimals, *expected);
}

template <typename TestType>
class TestParquetIO : public ParquetIOTestBase {
 public:
  void PrepareListTable(int64_t size, bool nullable_lists, bool nullable_elements,
                        int64_t null_count, std::shared_ptr<Table>* out) {
    std::shared_ptr<Array> values;
    ASSERT_OK(NullableArray<TestType>(size * size, nullable_elements ? null_count : 0,
                                      kDefaultSeed, &values));
    // Also test that slice offsets are respected
    values = values->Slice(5, values->length() - 5);
    std::shared_ptr<ListArray> lists;
    ASSERT_OK(MakeListArray(values, size, nullable_lists ? null_count : 0, "element",
                            nullable_elements, &lists));
    *out = MakeSimpleTable(lists->Slice(3, size - 6), nullable_lists);
  }

  void PrepareListOfListTable(int64_t size, bool nullable_parent_lists,
                              bool nullable_lists, bool nullable_elements,
                              int64_t null_count, std::shared_ptr<Table>* out) {
    std::shared_ptr<Array> values;
    ASSERT_OK(NullableArray<TestType>(size * 6, nullable_elements ? null_count : 0,
                                      kDefaultSeed, &values));
    std::shared_ptr<ListArray> lists;
    ASSERT_OK(MakeListArray(values, size * 3, nullable_lists ? null_count : 0, "item",
                            nullable_elements, &lists));
    std::shared_ptr<ListArray> parent_lists;
    ASSERT_OK(MakeListArray(lists, size, nullable_parent_lists ? null_count : 0, "item",
                            nullable_lists, &parent_lists));
    *out = MakeSimpleTable(parent_lists, nullable_parent_lists);
  }
};

// Below, we only test types which map bijectively to logical Parquet types
// (these tests don't serialize the original Arrow schema in Parquet metadata).
// Other Arrow types are tested elsewhere:
// - UInt32Type is serialized as Parquet INT64 in Parquet 1.0 (but not 2.0)
// - LargeBinaryType and LargeStringType are serialized as Parquet BYTE_ARRAY
//   (and deserialized as BinaryType and StringType, respectively)

typedef ::testing::Types<
    ::arrow::BooleanType, ::arrow::UInt8Type, ::arrow::Int8Type, ::arrow::UInt16Type,
    ::arrow::Int16Type, ::arrow::Int32Type, ::arrow::UInt64Type, ::arrow::Int64Type,
    ::arrow::Date32Type, ::arrow::FloatType, ::arrow::DoubleType, ::arrow::StringType,
    ::arrow::BinaryType, ::arrow::FixedSizeBinaryType, DecimalWithPrecisionAndScale<1>,
    DecimalWithPrecisionAndScale<5>, DecimalWithPrecisionAndScale<10>,
    DecimalWithPrecisionAndScale<19>, DecimalWithPrecisionAndScale<23>,
    DecimalWithPrecisionAndScale<27>, DecimalWithPrecisionAndScale<38>,
    Decimal256WithPrecisionAndScale<39>, Decimal256WithPrecisionAndScale<56>,
    Decimal256WithPrecisionAndScale<76>>
    TestTypes;

TYPED_TEST_SUITE(TestParquetIO, TestTypes);

TYPED_TEST(TestParquetIO, SingleColumnRequiredWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));

  std::shared_ptr<GroupNode> schema =
      MakeSimpleSchema(*values->type(), Repetition::REQUIRED);
  ASSERT_NO_FATAL_FAILURE(this->WriteColumn(schema, values));

  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*values));
}

TYPED_TEST(TestParquetIO, ZeroChunksTable) {
  auto values = std::make_shared<ChunkedArray>(::arrow::ArrayVector{}, ::arrow::int32());
  auto table = MakeSimpleTable(values, false);

  this->ResetSink();
  ASSERT_OK_NO_THROW(
      WriteTable(*table, ::arrow::default_memory_pool(), this->sink_, SMALL_SIZE));

  std::shared_ptr<Table> out;
  std::unique_ptr<FileReader> reader;
  ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
  ASSERT_NO_FATAL_FAILURE(this->ReadTableFromFile(std::move(reader), &out));
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(0, out->num_rows());
  ASSERT_EQ(0, out->column(0)->length());
  // odd: even though zero chunks were written, a single empty chunk is read
  ASSERT_EQ(1, out->column(0)->num_chunks());
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);

  this->ResetSink();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                values->length(), default_writer_properties()));

  std::shared_ptr<Table> out;
  std::unique_ptr<FileReader> reader;
  ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
  ASSERT_NO_FATAL_FAILURE(this->ReadTableFromFile(std::move(reader), &out));
  ASSERT_EQ(1, out->num_columns());
  EXPECT_EQ(table->num_rows(), out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0);
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

  ASSERT_OK_AND_ASSIGN(Datum out, DictionaryEncode(values));
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
  this->PrepareListTable(SMALL_SIZE, true, true, 10, &table);
  this->CheckRoundTrip(table);
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
  ArrowReaderProperties props;
  ASSERT_OK_NO_THROW(FromParquetSchema(&descriptor, props, &arrow_schema));

  std::unique_ptr<FileWriter> writer;
  ASSERT_OK_NO_THROW(FileWriter::Make(::arrow::default_memory_pool(),
                                      this->MakeWriter(schema), arrow_schema,
                                      default_arrow_writer_properties(), &writer));
  for (int i = 0; i < 4; i++) {
    ASSERT_OK_NO_THROW(writer->NewRowGroup(chunk_size));
    std::shared_ptr<Array> sliced_array = values->Slice(i * chunk_size, chunk_size);
    ASSERT_OK_NO_THROW(writer->WriteColumnChunk(*sliced_array));
  }
  ASSERT_OK_NO_THROW(writer->Close());

  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*values));
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredChunkedWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(LARGE_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);

  this->ResetSink();
  ASSERT_OK_NO_THROW(WriteTable(*table, default_memory_pool(), this->sink_, 512,
                                default_writer_properties()));

  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnTable(values));
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredChunkedWriteArrowIO) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(LARGE_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);

  this->ResetSink();
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

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0);
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
  ArrowReaderProperties props;
  ASSERT_OK_NO_THROW(FromParquetSchema(&descriptor, props, &arrow_schema));

  std::unique_ptr<FileWriter> writer;
  ASSERT_OK_NO_THROW(FileWriter::Make(::arrow::default_memory_pool(),
                                      this->MakeWriter(schema), arrow_schema,
                                      default_arrow_writer_properties(), &writer));
  for (int i = 0; i < 4; i++) {
    ASSERT_OK_NO_THROW(writer->NewRowGroup(chunk_size));
    std::shared_ptr<Array> sliced_array = values->Slice(i * chunk_size, chunk_size);
    ASSERT_OK_NO_THROW(writer->WriteColumnChunk(*sliced_array));
  }
  ASSERT_OK_NO_THROW(writer->Close());

  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnFile(*values));
}

TYPED_TEST(TestParquetIO, SingleColumnTableOptionalChunkedWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(LARGE_SIZE, 100, kDefaultSeed, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  this->ResetSink();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_, 512,
                                default_writer_properties()));

  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnTable(values));
}

TYPED_TEST(TestParquetIO, FileMetaDataWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->ResetSink();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                values->length(), default_writer_properties()));

  std::unique_ptr<FileReader> reader;
  ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
  auto metadata = reader->parquet_reader()->metadata();
  ASSERT_EQ(1, metadata->num_columns());
  EXPECT_EQ(table->num_rows(), metadata->num_rows());

  this->ResetSink();

  ASSERT_OK_NO_THROW(::parquet::arrow::WriteFileMetaData(*metadata, this->sink_.get()));

  ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
  auto metadata_written = reader->parquet_reader()->metadata();
  ASSERT_EQ(metadata->size(), metadata_written->size());
  ASSERT_EQ(metadata->num_row_groups(), metadata_written->num_row_groups());
  ASSERT_EQ(metadata->num_rows(), metadata_written->num_rows());
  ASSERT_EQ(metadata->num_columns(), metadata_written->num_columns());
  ASSERT_EQ(metadata->RowGroup(0)->num_rows(), metadata_written->RowGroup(0)->num_rows());
}

TYPED_TEST(TestParquetIO, CheckIterativeColumnRead) {
  // ARROW-5608: Test using ColumnReader with small batch size (1) and non-repeated
  // nullable fields with ASAN.
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  this->ResetSink();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                values->length(), default_writer_properties()));

  std::unique_ptr<FileReader> reader;
  this->ReaderFromSink(&reader);
  std::unique_ptr<ColumnReader> column_reader;
  ASSERT_OK_NO_THROW(reader->GetColumn(0, &column_reader));
  ASSERT_NE(nullptr, column_reader.get());

  // Read one record at a time.
  std::vector<std::shared_ptr<::arrow::Array>> batches;

  for (int64_t i = 0; i < values->length(); ++i) {
    std::shared_ptr<::arrow::ChunkedArray> batch;
    ASSERT_OK_NO_THROW(column_reader->NextBatch(1, &batch));
    ASSERT_EQ(1, batch->length());
    ASSERT_EQ(1, batch->num_chunks());
    batches.push_back(batch->chunk(0));
  }

  auto chunked = std::make_shared<::arrow::ChunkedArray>(batches);
  auto chunked_table = ::arrow::Table::Make(table->schema(), {chunked});
  ASSERT_TRUE(table->Equals(*chunked_table));
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
  this->ResetSink();
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

TEST_F(TestUInt32ParquetIO, Parquet_2_0_Compatibility) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<::arrow::UInt32Type>(LARGE_SIZE, 100, kDefaultSeed, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);

  // Parquet 2.4 roundtrip should yield an uint32_t column again
  this->ResetSink();
  std::shared_ptr<::parquet::WriterProperties> properties =
      ::parquet::WriterProperties::Builder()
          .version(ParquetVersion::PARQUET_2_4)
          ->build();
  ASSERT_OK_NO_THROW(
      WriteTable(*table, default_memory_pool(), this->sink_, 512, properties));
  ASSERT_NO_FATAL_FAILURE(this->ReadAndCheckSingleColumnTable(values));
}

TEST_F(TestUInt32ParquetIO, Parquet_1_0_Compatibility) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> arr;
  ASSERT_OK(NullableArray<::arrow::UInt32Type>(LARGE_SIZE, 100, kDefaultSeed, &arr));

  std::shared_ptr<::arrow::UInt32Array> values =
      std::dynamic_pointer_cast<::arrow::UInt32Array>(arr);

  std::shared_ptr<Table> table = MakeSimpleTable(values, true);

  // Parquet 1.0 returns an int64_t column as there is no way to tell a Parquet 1.0
  // reader that a column is unsigned.
  this->ResetSink();
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
  auto arr_data = std::make_shared<ArrayData>(::arrow::int64(), values->length(), buffers,
                                              values->null_count());
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
  this->ResetSink();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                values->length(), default_writer_properties()));

  std::shared_ptr<Table> out;
  std::unique_ptr<FileReader> reader;
  ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
  ASSERT_NO_FATAL_FAILURE(this->ReadTableFromFile(std::move(reader), &out));
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(table->num_rows(), out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0);
  ASSERT_EQ(1, chunked_array->num_chunks());

  AssertArraysEqual(*values, *chunked_array->chunk(0));
}

using TestLargeBinaryParquetIO = TestParquetIO<::arrow::LargeBinaryType>;

TEST_F(TestLargeBinaryParquetIO, Basics) {
  const char* json = "[\"foo\", \"\", null, \"\xff\"]";

  const auto large_type = ::arrow::large_binary();
  const auto narrow_type = ::arrow::binary();
  const auto large_array = ::arrow::ArrayFromJSON(large_type, json);
  const auto narrow_array = ::arrow::ArrayFromJSON(narrow_type, json);

  // When the original Arrow schema isn't stored, a LargeBinary array
  // is decoded as Binary (since there is no specific Parquet logical
  // type for it).
  this->RoundTripSingleColumn(large_array, narrow_array,
                              default_arrow_writer_properties());

  // When the original Arrow schema is stored, the LargeBinary array
  // is read back as LargeBinary.
  const auto arrow_properties =
      ::parquet::ArrowWriterProperties::Builder().store_schema()->build();
  this->RoundTripSingleColumn(large_array, large_array, arrow_properties);
}

using TestLargeStringParquetIO = TestParquetIO<::arrow::LargeStringType>;

TEST_F(TestLargeStringParquetIO, Basics) {
  const char* json = R"(["foo", "", null, "bar"])";

  const auto large_type = ::arrow::large_utf8();
  const auto narrow_type = ::arrow::utf8();
  const auto large_array = ::arrow::ArrayFromJSON(large_type, json);
  const auto narrow_array = ::arrow::ArrayFromJSON(narrow_type, json);

  // When the original Arrow schema isn't stored, a LargeBinary array
  // is decoded as Binary (since there is no specific Parquet logical
  // type for it).
  this->RoundTripSingleColumn(large_array, narrow_array,
                              default_arrow_writer_properties());

  // When the original Arrow schema is stored, the LargeBinary array
  // is read back as LargeBinary.
  const auto arrow_properties =
      ::parquet::ArrowWriterProperties::Builder().store_schema()->build();
  this->RoundTripSingleColumn(large_array, large_array, arrow_properties);
}

using TestNullParquetIO = TestParquetIO<::arrow::NullType>;

TEST_F(TestNullParquetIO, NullColumn) {
  for (int32_t num_rows : {0, SMALL_SIZE}) {
    std::shared_ptr<Array> values = std::make_shared<::arrow::NullArray>(num_rows);
    std::shared_ptr<Table> table = MakeSimpleTable(values, true /* nullable */);
    this->ResetSink();

    const int64_t chunk_size = std::max(static_cast<int64_t>(1), table->num_rows());
    ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                  chunk_size, default_writer_properties()));

    std::shared_ptr<Table> out;
    std::unique_ptr<FileReader> reader;
    ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
    ASSERT_NO_FATAL_FAILURE(this->ReadTableFromFile(std::move(reader), &out));
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(num_rows, out->num_rows());

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0);
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
    ASSERT_OK_AND_ASSIGN(list_array,
                         ::arrow::ListArray::FromArrays(*offsets_array, *values_array));

    std::shared_ptr<Table> table = MakeSimpleTable(list_array, false /* nullable */);
    this->ResetSink();

    const int64_t chunk_size = std::max(static_cast<int64_t>(1), table->num_rows());
    ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                  chunk_size, default_writer_properties()));

    std::shared_ptr<Table> out;
    std::unique_ptr<FileReader> reader;
    this->ReaderFromSink(&reader);
    this->ReadTableFromFile(std::move(reader), &out);
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(offsets.size() - 1, out->num_rows());

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0);
    ASSERT_EQ(1, chunked_array->num_chunks());
    AssertArraysEqual(*list_array, *chunked_array->chunk(0));
  }
}

TEST_F(TestNullParquetIO, NullDictionaryColumn) {
  ASSERT_OK_AND_ASSIGN(auto null_bitmap, ::arrow::AllocateEmptyBitmap(SMALL_SIZE));

  ASSERT_OK_AND_ASSIGN(auto indices, MakeArrayOfNull(::arrow::int8(), SMALL_SIZE));
  std::shared_ptr<::arrow::DictionaryType> dict_type =
      std::make_shared<::arrow::DictionaryType>(::arrow::int8(), ::arrow::null());

  std::shared_ptr<Array> dict = std::make_shared<::arrow::NullArray>(0);
  std::shared_ptr<Array> dict_values =
      std::make_shared<::arrow::DictionaryArray>(dict_type, indices, dict);
  std::shared_ptr<Table> table = MakeSimpleTable(dict_values, true);
  this->ResetSink();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
                                dict_values->length(), default_writer_properties()));

  std::shared_ptr<Table> out;
  std::unique_ptr<FileReader> reader;
  ASSERT_NO_FATAL_FAILURE(this->ReaderFromSink(&reader));
  ASSERT_NO_FATAL_FAILURE(this->ReadTableFromFile(std::move(reader), &out));
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(100, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0);
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

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0);
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

  void CheckSingleColumnStatisticsRequiredRead() {
    std::vector<T> values(SMALL_SIZE, test_traits<TestType>::value);
    std::unique_ptr<FileReader> file_reader;
    ASSERT_NO_FATAL_FAILURE(MakeTestFile(values, 1, &file_reader));

    std::shared_ptr<Scalar> min, max;
    ReadSingleColumnFileStatistics(std::move(file_reader), &min, &max);

    ASSERT_OK_AND_ASSIGN(
        auto value, ::arrow::MakeScalar(::arrow::TypeTraits<TestType>::type_singleton(),
                                        test_traits<TestType>::value));

    ASSERT_TRUE(value->Equals(*min));
    ASSERT_TRUE(value->Equals(*max));
  }
};

typedef ::testing::Types<::arrow::BooleanType, ::arrow::UInt8Type, ::arrow::Int8Type,
                         ::arrow::UInt16Type, ::arrow::Int16Type, ::arrow::UInt32Type,
                         ::arrow::Int32Type, ::arrow::UInt64Type, ::arrow::Int64Type,
                         ::arrow::FloatType, ::arrow::DoubleType>
    PrimitiveTestTypes;

TYPED_TEST_SUITE(TestPrimitiveParquetIO, PrimitiveTestTypes);

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredRead) {
  ASSERT_NO_FATAL_FAILURE(this->CheckSingleColumnRequiredRead(1));
}

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnStatisticsRequiredRead) {
  ASSERT_NO_FATAL_FAILURE(this->CheckSingleColumnStatisticsRequiredRead());
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

void MakeDateTimeTypesTable(std::shared_ptr<Table>* out, bool expected = false) {
  using ::arrow::ArrayFromVector;

  std::vector<bool> is_valid = {true, true, true, false, true, true};

  // These are only types that roundtrip without modification
  auto f0 = field("f0", ::arrow::date32());
  auto f1 = field("f1", ::arrow::timestamp(TimeUnit::MILLI));
  auto f2 = field("f2", ::arrow::timestamp(TimeUnit::MICRO));
  auto f3 = field("f3", ::arrow::timestamp(TimeUnit::NANO));
  auto f3_x = field("f3", ::arrow::timestamp(TimeUnit::MICRO));
  auto f4 = field("f4", ::arrow::time32(TimeUnit::MILLI));
  auto f5 = field("f5", ::arrow::time64(TimeUnit::MICRO));
  auto f6 = field("f6", ::arrow::time64(TimeUnit::NANO));

  std::shared_ptr<::arrow::Schema> schema(
      new ::arrow::Schema({f0, f1, f2, (expected ? f3_x : f3), f4, f5, f6}));

  std::vector<int32_t> t32_values = {1489269000, 1489270000, 1489271000,
                                     1489272000, 1489272000, 1489273000};
  std::vector<int64_t> t64_ns_values = {1489269000000, 1489270000000, 1489271000000,
                                        1489272000000, 1489272000000, 1489273000000};
  std::vector<int64_t> t64_us_values = {1489269000, 1489270000, 1489271000,
                                        1489272000, 1489272000, 1489273000};
  std::vector<int64_t> t64_ms_values = {1489269, 1489270, 1489271,
                                        1489272, 1489272, 1489273};

  std::shared_ptr<Array> a0, a1, a2, a3, a3_x, a4, a5, a6;
  ArrayFromVector<::arrow::Date32Type, int32_t>(f0->type(), is_valid, t32_values, &a0);
  ArrayFromVector<::arrow::TimestampType, int64_t>(f1->type(), is_valid, t64_ms_values,
                                                   &a1);
  ArrayFromVector<::arrow::TimestampType, int64_t>(f2->type(), is_valid, t64_us_values,
                                                   &a2);
  ArrayFromVector<::arrow::TimestampType, int64_t>(f3->type(), is_valid, t64_ns_values,
                                                   &a3);
  ArrayFromVector<::arrow::TimestampType, int64_t>(f3_x->type(), is_valid, t64_us_values,
                                                   &a3_x);
  ArrayFromVector<::arrow::Time32Type, int32_t>(f4->type(), is_valid, t32_values, &a4);
  ArrayFromVector<::arrow::Time64Type, int64_t>(f5->type(), is_valid, t64_us_values, &a5);
  ArrayFromVector<::arrow::Time64Type, int64_t>(f6->type(), is_valid, t64_ns_values, &a6);

  *out = Table::Make(schema, {a0, a1, a2, expected ? a3_x : a3, a4, a5, a6});
}

TEST(TestArrowReadWrite, DateTimeTypes) {
  std::shared_ptr<Table> table, result;

  MakeDateTimeTypesTable(&table);
  ASSERT_NO_FATAL_FAILURE(
      DoSimpleRoundtrip(table, false /* use_threads */, table->num_rows(), {}, &result));

  MakeDateTimeTypesTable(&table, true);  // build expected result
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertSchemaEqual(*table->schema(), *result->schema(),
                                                     /*check_metadata=*/false));
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
  auto input = Table::Make(input_schema, {a_s, a_ms, a_us, a_ns});

  // When reading parquet files, all int96 schema fields are converted to
  // timestamp nanoseconds
  auto ex_schema = schema({field("f_s", t_ns), field("f_ms", t_ns), field("f_us", t_ns),
                           field("f_ns", t_ns)});
  auto ex_result = Table::Make(ex_schema, {a_ns, a_ns, a_ns, a_ns});

  std::shared_ptr<Table> result;
  ASSERT_NO_FATAL_FAILURE(DoSimpleRoundtrip(
      input, false /* use_threads */, input->num_rows(), {}, &result,
      ArrowWriterProperties::Builder().enable_deprecated_int96_timestamps()->build()));

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertSchemaEqual(*ex_result->schema(),
                                                     *result->schema(),
                                                     /*check_metadata=*/false));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*ex_result, *result));

  // Ensure enable_deprecated_int96_timestamps as precedence over
  // coerce_timestamps.
  ASSERT_NO_FATAL_FAILURE(DoSimpleRoundtrip(input, false /* use_threads */,
                                            input->num_rows(), {}, &result,
                                            ArrowWriterProperties::Builder()
                                                .enable_deprecated_int96_timestamps()
                                                ->coerce_timestamps(TimeUnit::MILLI)
                                                ->build()));

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertSchemaEqual(*ex_result->schema(),
                                                     *result->schema(),
                                                     /*check_metadata=*/false));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*ex_result, *result));
}

TEST(TestArrowReadWrite, DownsampleDeprecatedInt96) {
  using ::arrow::ArrayFromJSON;
  using ::arrow::field;
  using ::arrow::schema;

  // Timestamp values at 2000-01-01 00:00:00,
  // then with increment unit of 1ns, 1us, 1ms and 1s.
  auto a_nano =
      ArrayFromJSON(timestamp(TimeUnit::NANO),
                    "[946684800000000000, 946684800000000001, 946684800000001000, "
                    "946684800001000000, 946684801000000000]");
  auto a_micro = ArrayFromJSON(timestamp(TimeUnit::MICRO),
                               "[946684800000000, 946684800000000, 946684800000001, "
                               "946684800001000, 946684801000000]");
  auto a_milli = ArrayFromJSON(
      timestamp(TimeUnit::MILLI),
      "[946684800000, 946684800000, 946684800000, 946684800001, 946684801000]");
  auto a_second =
      ArrayFromJSON(timestamp(TimeUnit::SECOND),
                    "[946684800, 946684800, 946684800, 946684800, 946684801]");

  ASSERT_NO_FATAL_FAILURE(DownsampleInt96RoundTrip(a_nano, a_nano, TimeUnit::NANO));
  ASSERT_NO_FATAL_FAILURE(DownsampleInt96RoundTrip(a_nano, a_micro, TimeUnit::MICRO));
  ASSERT_NO_FATAL_FAILURE(DownsampleInt96RoundTrip(a_nano, a_milli, TimeUnit::MILLI));
  ASSERT_NO_FATAL_FAILURE(DownsampleInt96RoundTrip(a_nano, a_second, TimeUnit::SECOND));
}

TEST(TestArrowReadWrite, CoerceTimestamps) {
  using ::arrow::ArrayFromVector;
  using ::arrow::field;

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
  auto s1 = ::arrow::schema(
      {field("f_s", t_s), field("f_ms", t_ms), field("f_us", t_us), field("f_ns", t_ns)});
  auto input = Table::Make(s1, {a_s, a_ms, a_us, a_ns});

  // Result when coercing to milliseconds
  auto s2 = ::arrow::schema({field("f_s", t_ms), field("f_ms", t_ms), field("f_us", t_ms),
                             field("f_ns", t_ms)});
  auto ex_milli_result = Table::Make(s2, {a_ms, a_ms, a_ms, a_ms});
  std::shared_ptr<Table> milli_result;
  ASSERT_NO_FATAL_FAILURE(DoSimpleRoundtrip(
      input, false /* use_threads */, input->num_rows(), {}, &milli_result,
      ArrowWriterProperties::Builder().coerce_timestamps(TimeUnit::MILLI)->build()));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertSchemaEqual(*ex_milli_result->schema(),
                                                     *milli_result->schema(),
                                                     /*check_metadata=*/false));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*ex_milli_result, *milli_result));

  // Result when coercing to microseconds
  auto s3 = ::arrow::schema({field("f_s", t_us), field("f_ms", t_us), field("f_us", t_us),
                             field("f_ns", t_us)});
  auto ex_micro_result = Table::Make(s3, {a_us, a_us, a_us, a_us});
  std::shared_ptr<Table> micro_result;
  ASSERT_NO_FATAL_FAILURE(DoSimpleRoundtrip(
      input, false /* use_threads */, input->num_rows(), {}, &micro_result,
      ArrowWriterProperties::Builder().coerce_timestamps(TimeUnit::MICRO)->build()));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertSchemaEqual(*ex_micro_result->schema(),
                                                     *micro_result->schema(),
                                                     /*check_metadata=*/false));
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

  auto s1 = ::arrow::schema({field("f_s", t_s)});
  auto s2 = ::arrow::schema({field("f_ms", t_ms)});
  auto s3 = ::arrow::schema({field("f_us", t_us)});
  auto s4 = ::arrow::schema({field("f_ns", t_ns)});

  auto t1 = Table::Make(s1, {a_s});
  auto t2 = Table::Make(s2, {a_ms});
  auto t3 = Table::Make(s3, {a_us});
  auto t4 = Table::Make(s4, {a_ns});

  // OK to write to millis
  auto coerce_millis =
      (ArrowWriterProperties::Builder().coerce_timestamps(TimeUnit::MILLI)->build());
  ASSERT_OK_NO_THROW(WriteTable(*t1, ::arrow::default_memory_pool(), CreateOutputStream(),
                                10, default_writer_properties(), coerce_millis));

  ASSERT_OK_NO_THROW(WriteTable(*t2, ::arrow::default_memory_pool(), CreateOutputStream(),
                                10, default_writer_properties(), coerce_millis));

  // Loss of precision
  ASSERT_RAISES(Invalid,
                WriteTable(*t3, ::arrow::default_memory_pool(), CreateOutputStream(), 10,
                           default_writer_properties(), coerce_millis));
  ASSERT_RAISES(Invalid,
                WriteTable(*t4, ::arrow::default_memory_pool(), CreateOutputStream(), 10,
                           default_writer_properties(), coerce_millis));

  // OK to lose micros/nanos -> millis precision if we explicitly allow it
  auto allow_truncation_to_millis = (ArrowWriterProperties::Builder()
                                         .coerce_timestamps(TimeUnit::MILLI)
                                         ->allow_truncated_timestamps()
                                         ->build());
  ASSERT_OK_NO_THROW(WriteTable(*t3, ::arrow::default_memory_pool(), CreateOutputStream(),
                                10, default_writer_properties(),
                                allow_truncation_to_millis));
  ASSERT_OK_NO_THROW(WriteTable(*t4, ::arrow::default_memory_pool(), CreateOutputStream(),
                                10, default_writer_properties(),
                                allow_truncation_to_millis));

  // OK to write to micros
  auto coerce_micros =
      (ArrowWriterProperties::Builder().coerce_timestamps(TimeUnit::MICRO)->build());
  ASSERT_OK_NO_THROW(WriteTable(*t1, ::arrow::default_memory_pool(), CreateOutputStream(),
                                10, default_writer_properties(), coerce_micros));
  ASSERT_OK_NO_THROW(WriteTable(*t2, ::arrow::default_memory_pool(), CreateOutputStream(),
                                10, default_writer_properties(), coerce_micros));
  ASSERT_OK_NO_THROW(WriteTable(*t3, ::arrow::default_memory_pool(), CreateOutputStream(),
                                10, default_writer_properties(), coerce_micros));

  // Loss of precision
  ASSERT_RAISES(Invalid,
                WriteTable(*t4, ::arrow::default_memory_pool(), CreateOutputStream(), 10,
                           default_writer_properties(), coerce_micros));

  // OK to lose nanos -> micros precision if we explicitly allow it
  auto allow_truncation_to_micros = (ArrowWriterProperties::Builder()
                                         .coerce_timestamps(TimeUnit::MICRO)
                                         ->allow_truncated_timestamps()
                                         ->build());
  ASSERT_OK_NO_THROW(WriteTable(*t4, ::arrow::default_memory_pool(), CreateOutputStream(),
                                10, default_writer_properties(),
                                allow_truncation_to_micros));
}

TEST(TestArrowReadWrite, ImplicitSecondToMillisecondTimestampCoercion) {
  using ::arrow::ArrayFromVector;
  using ::arrow::field;
  using ::arrow::schema;

  std::vector<bool> is_valid = {true, true, true, false, true, true};

  auto t_s = ::arrow::timestamp(TimeUnit::SECOND);
  auto t_ms = ::arrow::timestamp(TimeUnit::MILLI);

  std::vector<int64_t> s_values = {1489269, 1489270, 1489271, 1489272, 1489272, 1489273};
  std::vector<int64_t> ms_values = {1489269000, 1489270000, 1489271000,
                                    1489272000, 1489272000, 1489273000};

  std::shared_ptr<Array> a_s, a_ms;
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_s, is_valid, s_values, &a_s);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_ms, is_valid, ms_values, &a_ms);

  auto si = schema({field("timestamp", t_s)});
  auto sx = schema({field("timestamp", t_ms)});

  auto ti = Table::Make(si, {a_s});   // input
  auto tx = Table::Make(sx, {a_ms});  // expected output
  std::shared_ptr<Table> to;          // actual output

  // default properties (without explicit coercion instructions) used ...
  ASSERT_NO_FATAL_FAILURE(
      DoSimpleRoundtrip(ti, false /* use_threads */, ti->num_rows(), {}, &to));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertSchemaEqual(*tx->schema(), *to->schema(),
                                                     /*check_metadata=*/false));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*tx, *to));
}

TEST(TestArrowReadWrite, ParquetVersionTimestampDifferences) {
  using ::arrow::ArrayFromVector;
  using ::arrow::field;
  using ::arrow::schema;

  auto t_s = ::arrow::timestamp(TimeUnit::SECOND);
  auto t_ms = ::arrow::timestamp(TimeUnit::MILLI);
  auto t_us = ::arrow::timestamp(TimeUnit::MICRO);
  auto t_ns = ::arrow::timestamp(TimeUnit::NANO);

  const int N = 24;
  int64_t instant = INT64_C(1262304000);  // 2010-01-01T00:00:00 seconds offset
  std::vector<int64_t> d_s, d_ms, d_us, d_ns;
  for (int i = 0; i < N; ++i) {
    d_s.push_back(instant);
    d_ms.push_back(instant * INT64_C(1000));
    d_us.push_back(instant * INT64_C(1000000));
    d_ns.push_back(instant * INT64_C(1000000000));
    instant += 3600;
  }

  std::shared_ptr<Array> a_s, a_ms, a_us, a_ns;
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_s, d_s, &a_s);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_ms, d_ms, &a_ms);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_us, d_us, &a_us);
  ArrayFromVector<::arrow::TimestampType, int64_t>(t_ns, d_ns, &a_ns);

  auto input_schema = schema({field("ts:s", t_s), field("ts:ms", t_ms),
                              field("ts:us", t_us), field("ts:ns", t_ns)});
  auto input_table = Table::Make(input_schema, {a_s, a_ms, a_us, a_ns});

  auto parquet_version_1_properties = ::parquet::default_writer_properties();
  ARROW_SUPPRESS_DEPRECATION_WARNING
  auto parquet_version_2_0_properties = ::parquet::WriterProperties::Builder()
                                            .version(ParquetVersion::PARQUET_2_0)
                                            ->build();
  ARROW_UNSUPPRESS_DEPRECATION_WARNING
  auto parquet_version_2_4_properties = ::parquet::WriterProperties::Builder()
                                            .version(ParquetVersion::PARQUET_2_4)
                                            ->build();
  auto parquet_version_2_6_properties = ::parquet::WriterProperties::Builder()
                                            .version(ParquetVersion::PARQUET_2_6)
                                            ->build();
  const std::vector<std::shared_ptr<WriterProperties>> all_properties = {
      parquet_version_1_properties, parquet_version_2_0_properties,
      parquet_version_2_4_properties, parquet_version_2_6_properties};

  {
    // Using Parquet version 1.0 and 2.4 defaults, seconds should be coerced to
    // milliseconds and nanoseconds should be coerced to microseconds
    auto expected_schema = schema({field("ts:s", t_ms), field("ts:ms", t_ms),
                                   field("ts:us", t_us), field("ts:ns", t_us)});
    auto expected_table = Table::Make(expected_schema, {a_ms, a_ms, a_us, a_us});
    ASSERT_NO_FATAL_FAILURE(CheckConfiguredRoundtrip(input_table, expected_table,
                                                     parquet_version_1_properties));
    ASSERT_NO_FATAL_FAILURE(CheckConfiguredRoundtrip(input_table, expected_table,
                                                     parquet_version_2_4_properties));
  }
  {
    // Using Parquet version 2.0 and 2.6 defaults, seconds should be coerced to
    // milliseconds and nanoseconds should be retained
    auto expected_schema = schema({field("ts:s", t_ms), field("ts:ms", t_ms),
                                   field("ts:us", t_us), field("ts:ns", t_ns)});
    auto expected_table = Table::Make(expected_schema, {a_ms, a_ms, a_us, a_ns});
    ASSERT_NO_FATAL_FAILURE(CheckConfiguredRoundtrip(input_table, expected_table,
                                                     parquet_version_2_0_properties));
    ASSERT_NO_FATAL_FAILURE(CheckConfiguredRoundtrip(input_table, expected_table,
                                                     parquet_version_2_6_properties));
  }

  auto arrow_coerce_to_seconds_properties =
      ArrowWriterProperties::Builder().coerce_timestamps(TimeUnit::SECOND)->build();
  auto arrow_coerce_to_millis_properties =
      ArrowWriterProperties::Builder().coerce_timestamps(TimeUnit::MILLI)->build();
  auto arrow_coerce_to_micros_properties =
      ArrowWriterProperties::Builder().coerce_timestamps(TimeUnit::MICRO)->build();
  auto arrow_coerce_to_nanos_properties =
      ArrowWriterProperties::Builder().coerce_timestamps(TimeUnit::NANO)->build();

  for (const auto& properties : all_properties) {
    // Using all Parquet versions, coercing to milliseconds or microseconds is allowed
    ARROW_SCOPED_TRACE("format = ", ParquetVersionToString(properties->version()));
    auto expected_schema = schema({field("ts:s", t_ms), field("ts:ms", t_ms),
                                   field("ts:us", t_ms), field("ts:ns", t_ms)});
    auto expected_table = Table::Make(expected_schema, {a_ms, a_ms, a_ms, a_ms});
    ASSERT_NO_FATAL_FAILURE(CheckConfiguredRoundtrip(
        input_table, expected_table, properties, arrow_coerce_to_millis_properties));

    expected_schema = schema({field("ts:s", t_us), field("ts:ms", t_us),
                              field("ts:us", t_us), field("ts:ns", t_us)});
    expected_table = Table::Make(expected_schema, {a_us, a_us, a_us, a_us});
    ASSERT_NO_FATAL_FAILURE(CheckConfiguredRoundtrip(
        input_table, expected_table, properties, arrow_coerce_to_micros_properties));

    // Neither Parquet version allows coercing to seconds
    std::shared_ptr<Table> actual_table;
    ASSERT_RAISES(NotImplemented,
                  WriteTable(*input_table, ::arrow::default_memory_pool(),
                             CreateOutputStream(), input_table->num_rows(), properties,
                             arrow_coerce_to_seconds_properties));
  }
  // Using Parquet versions 1.0 and 2.4, coercing to (int64) nanoseconds is not allowed
  for (const auto& properties :
       {parquet_version_1_properties, parquet_version_2_4_properties}) {
    ARROW_SCOPED_TRACE("format = ", ParquetVersionToString(properties->version()));
    std::shared_ptr<Table> actual_table;
    ASSERT_RAISES(NotImplemented,
                  WriteTable(*input_table, ::arrow::default_memory_pool(),
                             CreateOutputStream(), input_table->num_rows(), properties,
                             arrow_coerce_to_nanos_properties));
  }
  // Using Parquet versions "2.0" and 2.6, coercing to (int64) nanoseconds is allowed
  for (const auto& properties :
       {parquet_version_2_0_properties, parquet_version_2_6_properties}) {
    ARROW_SCOPED_TRACE("format = ", ParquetVersionToString(properties->version()));
    auto expected_schema = schema({field("ts:s", t_ns), field("ts:ms", t_ns),
                                   field("ts:us", t_ns), field("ts:ns", t_ns)});
    auto expected_table = Table::Make(expected_schema, {a_ns, a_ns, a_ns, a_ns});
    ASSERT_NO_FATAL_FAILURE(CheckConfiguredRoundtrip(
        input_table, expected_table, properties, arrow_coerce_to_nanos_properties));
  }

  // Using all Parquet versions, coercing to nanoseconds is allowed if Int96
  // storage is used
  auto arrow_enable_int96_properties =
      ArrowWriterProperties::Builder().enable_deprecated_int96_timestamps()->build();
  for (const auto& properties : all_properties) {
    ARROW_SCOPED_TRACE("format = ", ParquetVersionToString(properties->version()));
    auto expected_schema = schema({field("ts:s", t_ns), field("ts:ms", t_ns),
                                   field("ts:us", t_ns), field("ts:ns", t_ns)});
    auto expected_table = Table::Make(expected_schema, {a_ns, a_ns, a_ns, a_ns});
    ASSERT_NO_FATAL_FAILURE(CheckConfiguredRoundtrip(
        input_table, expected_table, properties, arrow_enable_int96_properties));
  }
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

  auto table = Table::Make(schema, {a0, a1, a0_nonnull, a1_nonnull});

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

  auto ex_table = Table::Make(ex_schema, {x0, x1, x0_nonnull, x1_nonnull});

  std::shared_ptr<Table> result;
  ASSERT_NO_FATAL_FAILURE(
      DoSimpleRoundtrip(table, false /* use_threads */, table->num_rows(), {}, &result));

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertSchemaEqual(*ex_table->schema(),
                                                     *result->schema(),
                                                     /*check_metadata=*/false));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*ex_table, *result));
}

void MakeDoubleTable(int num_columns, int num_rows, int nchunks,
                     std::shared_ptr<Table>* out) {
  std::vector<std::shared_ptr<::arrow::ChunkedArray>> columns(num_columns);
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
    columns[i] = std::make_shared<ChunkedArray>(arrays);
    fields[i] = ::arrow::field(ss.str(), values->type());
  }
  auto schema = std::make_shared<::arrow::Schema>(fields);
  *out = Table::Make(schema, columns, num_rows);
}

void MakeSimpleListArray(int num_rows, int max_value_length, const std::string& item_name,
                         std::shared_ptr<DataType>* out_type,
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

  *out_type = ::arrow::list(::arrow::field(item_name, ::arrow::int8()));
  *out_array = std::make_shared<ListArray>(*out_type, offsets->length() - 1,
                                           offsets->data()->buffers[1], values);
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
  const int num_columns = 10;
  const int num_rows = 100;

  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));

  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(WriteTableToBuffer(table, num_rows / 2,
                                             default_arrow_writer_properties(), &buffer));

  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                              ::arrow::default_memory_pool(), &reader));

  ASSERT_EQ(2, reader->num_row_groups());

  std::shared_ptr<Table> r1, r2, r3, r4;
  // Read everything
  ASSERT_OK_NO_THROW(reader->ReadRowGroup(0, &r1));
  ASSERT_OK_NO_THROW(reader->RowGroup(1)->ReadTable(&r2));
  ASSERT_OK_NO_THROW(reader->ReadRowGroups({0, 1}, &r3));
  ASSERT_OK_NO_THROW(reader->ReadRowGroups({1}, &r4));

  std::shared_ptr<Table> concatenated;

  ASSERT_OK_AND_ASSIGN(concatenated, ::arrow::ConcatenateTables({r1, r2}));
  AssertTablesEqual(*concatenated, *table, /*same_chunk_layout=*/false);

  AssertTablesEqual(*table, *r3, /*same_chunk_layout=*/false);
  ASSERT_TRUE(r2->Equals(*r4));
  ASSERT_OK_AND_ASSIGN(concatenated, ::arrow::ConcatenateTables({r1, r4}));

  AssertTablesEqual(*table, *concatenated, /*same_chunk_layout=*/false);
}

//  Exercise reading table manually with nested RowGroup and Column loops, i.e.
//
//  for (int i = 0; i < n_row_groups; i++)
//    for (int j = 0; j < n_cols; j++)
//      reader->RowGroup(i)->Column(j)->Read(&chunked_array);
::arrow::Result<std::shared_ptr<Table>> ReadTableManually(FileReader* reader) {
  std::vector<std::shared_ptr<Table>> tables;

  std::shared_ptr<::arrow::Schema> schema;
  RETURN_NOT_OK(reader->GetSchema(&schema));

  int n_row_groups = reader->num_row_groups();
  int n_columns = schema->num_fields();
  for (int i = 0; i < n_row_groups; i++) {
    std::vector<std::shared_ptr<ChunkedArray>> columns{static_cast<size_t>(n_columns)};

    for (int j = 0; j < n_columns; j++) {
      RETURN_NOT_OK(reader->RowGroup(i)->Column(j)->Read(&columns[j]));
    }

    tables.push_back(Table::Make(schema, columns));
  }

  return ConcatenateTables(tables);
}

TEST(TestArrowReadWrite, ReadTableManually) {
  const int num_columns = 1;
  const int num_rows = 128;

  std::shared_ptr<Table> expected;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &expected));

  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(WriteTableToBuffer(expected, num_rows / 2,
                                             default_arrow_writer_properties(), &buffer));

  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                              ::arrow::default_memory_pool(), &reader));

  ASSERT_EQ(2, reader->num_row_groups());

  ASSERT_OK_AND_ASSIGN(auto actual, ReadTableManually(reader.get()));

  AssertTablesEqual(*actual, *expected, /*same_chunk_layout=*/false);
}

void TestGetRecordBatchReader(
    ArrowReaderProperties properties = default_arrow_reader_properties()) {
  const int num_columns = 20;
  const int num_rows = 1000;
  const int batch_size = 100;

  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));

  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(WriteTableToBuffer(table, num_rows / 2,
                                             default_arrow_writer_properties(), &buffer));

  properties.set_batch_size(batch_size);

  std::unique_ptr<FileReader> reader;
  FileReaderBuilder builder;
  ASSERT_OK(builder.Open(std::make_shared<BufferReader>(buffer)));
  ASSERT_OK(builder.properties(properties)->Build(&reader));

  // Read the whole file, one batch at a time.
  std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
  ASSERT_OK_NO_THROW(reader->GetRecordBatchReader({0, 1}, &rb_reader));
  std::shared_ptr<::arrow::RecordBatch> actual_batch, expected_batch;
  ::arrow::TableBatchReader table_reader(*table);
  table_reader.set_chunksize(batch_size);

  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(rb_reader->ReadNext(&actual_batch));
    ASSERT_OK(table_reader.ReadNext(&expected_batch));
    ASSERT_NO_FATAL_FAILURE(::arrow::AssertBatchesEqual(*expected_batch, *actual_batch));
  }

  ASSERT_OK(rb_reader->ReadNext(&actual_batch));
  ASSERT_EQ(nullptr, actual_batch);

  // ARROW-6005: Read just the second row group
  ASSERT_OK_NO_THROW(reader->GetRecordBatchReader({1}, &rb_reader));
  std::shared_ptr<Table> second_rowgroup = table->Slice(num_rows / 2);
  ::arrow::TableBatchReader second_table_reader(*second_rowgroup);
  second_table_reader.set_chunksize(batch_size);

  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(rb_reader->ReadNext(&actual_batch));
    ASSERT_OK(second_table_reader.ReadNext(&expected_batch));
    ASSERT_NO_FATAL_FAILURE(::arrow::AssertBatchesEqual(*expected_batch, *actual_batch));
  }

  ASSERT_OK(rb_reader->ReadNext(&actual_batch));
  ASSERT_EQ(nullptr, actual_batch);
}

TEST(TestArrowReadWrite, GetRecordBatchReader) { TestGetRecordBatchReader(); }

// Same as the test above, but using coalesced reads.
TEST(TestArrowReadWrite, CoalescedReads) {
  ArrowReaderProperties arrow_properties = default_arrow_reader_properties();
  arrow_properties.set_pre_buffer(true);
  TestGetRecordBatchReader(arrow_properties);
}

// Use coalesced reads, and explicitly wait for I/O to complete.
TEST(TestArrowReadWrite, WaitCoalescedReads) {
  ArrowReaderProperties properties = default_arrow_reader_properties();
  const int num_rows = 10;
  const int num_columns = 5;

  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));

  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(
      WriteTableToBuffer(table, num_rows, default_arrow_writer_properties(), &buffer));

  std::unique_ptr<FileReader> reader;
  FileReaderBuilder builder;
  ASSERT_OK(builder.Open(std::make_shared<BufferReader>(buffer)));
  ASSERT_OK(builder.properties(properties)->Build(&reader));
  // Pre-buffer data and wait for I/O to complete.
  reader->parquet_reader()->PreBuffer({0}, {0, 1, 2, 3, 4}, ::arrow::io::IOContext(),
                                      ::arrow::io::CacheOptions::Defaults());
  ASSERT_OK(reader->parquet_reader()->WhenBuffered({0}, {0, 1, 2, 3, 4}).status());

  std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
  ASSERT_OK_NO_THROW(reader->GetRecordBatchReader({0}, {0, 1, 2, 3, 4}, &rb_reader));

  std::shared_ptr<::arrow::RecordBatch> actual_batch;
  ASSERT_OK(rb_reader->ReadNext(&actual_batch));

  ASSERT_NE(actual_batch, nullptr);
  ASSERT_EQ(actual_batch->num_columns(), num_columns);
  ASSERT_EQ(actual_batch->num_rows(), num_rows);
}

TEST(TestArrowReadWrite, GetRecordBatchReaderNoColumns) {
  ArrowReaderProperties properties = default_arrow_reader_properties();
  const int num_rows = 10;
  const int num_columns = 20;

  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));

  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(
      WriteTableToBuffer(table, num_rows, default_arrow_writer_properties(), &buffer));

  std::unique_ptr<FileReader> reader;
  FileReaderBuilder builder;
  ASSERT_OK(builder.Open(std::make_shared<BufferReader>(buffer)));
  ASSERT_OK(builder.properties(properties)->Build(&reader));

  std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
  ASSERT_OK_NO_THROW(reader->GetRecordBatchReader({0}, {}, &rb_reader));

  std::shared_ptr<::arrow::RecordBatch> actual_batch;
  ASSERT_OK(rb_reader->ReadNext(&actual_batch));

  ASSERT_NE(actual_batch, nullptr);
  ASSERT_EQ(actual_batch->num_columns(), 0);
  ASSERT_EQ(actual_batch->num_rows(), num_rows);
}

TEST(TestArrowReadWrite, GetRecordBatchGenerator) {
  ArrowReaderProperties properties = default_arrow_reader_properties();
  const int num_rows = 1024;
  const int row_group_size = 512;
  const int num_columns = 2;

  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));

  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(WriteTableToBuffer(table, row_group_size,
                                             default_arrow_writer_properties(), &buffer));

  std::shared_ptr<FileReader> reader;
  {
    std::unique_ptr<FileReader> unique_reader;
    FileReaderBuilder builder;
    ASSERT_OK(builder.Open(std::make_shared<BufferReader>(buffer)));
    ASSERT_OK(builder.properties(properties)->Build(&unique_reader));
    reader = std::move(unique_reader);
  }

  auto check_batches = [](const std::shared_ptr<::arrow::RecordBatch>& batch,
                          int num_columns, int num_rows) {
    ASSERT_NE(batch, nullptr);
    ASSERT_EQ(batch->num_columns(), num_columns);
    ASSERT_EQ(batch->num_rows(), num_rows);
  };
  {
    ASSERT_OK_AND_ASSIGN(auto batch_generator,
                         reader->GetRecordBatchGenerator(reader, {0, 1}, {0, 1}));
    auto fut1 = batch_generator();
    auto fut2 = batch_generator();
    auto fut3 = batch_generator();
    ASSERT_OK_AND_ASSIGN(auto batch1, fut1.result());
    ASSERT_OK_AND_ASSIGN(auto batch2, fut2.result());
    ASSERT_OK_AND_ASSIGN(auto batch3, fut3.result());
    ASSERT_EQ(batch3, nullptr);
    check_batches(batch1, num_columns, row_group_size);
    check_batches(batch2, num_columns, row_group_size);
    ASSERT_OK_AND_ASSIGN(auto actual, ::arrow::Table::FromRecordBatches(
                                          batch1->schema(), {batch1, batch2}));
    AssertTablesEqual(*table, *actual, /*same_chunk_layout=*/false);
  }
  {
    // No columns case
    ASSERT_OK_AND_ASSIGN(auto batch_generator,
                         reader->GetRecordBatchGenerator(reader, {0, 1}, {}));
    auto fut1 = batch_generator();
    auto fut2 = batch_generator();
    auto fut3 = batch_generator();
    ASSERT_OK_AND_ASSIGN(auto batch1, fut1.result());
    ASSERT_OK_AND_ASSIGN(auto batch2, fut2.result());
    ASSERT_OK_AND_ASSIGN(auto batch3, fut3.result());
    ASSERT_EQ(batch3, nullptr);
    check_batches(batch1, 0, row_group_size);
    check_batches(batch2, 0, row_group_size);
  }
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
                              ::arrow::default_memory_pool(), &reader));

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

  std::vector<std::shared_ptr<::arrow::ChunkedArray>> ex_columns;
  std::vector<std::shared_ptr<::arrow::Field>> ex_fields;
  for (int i : column_subset) {
    ex_columns.push_back(table->column(i));
    ex_fields.push_back(table->field(i));
  }

  auto ex_schema = ::arrow::schema(ex_fields);
  auto expected = Table::Make(ex_schema, ex_columns);
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*expected, *result));
}

TEST(TestArrowReadWrite, ReadCoalescedColumnSubset) {
  const int num_columns = 20;
  const int num_rows = 1000;

  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));
  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(WriteTableToBuffer(table, num_rows / 2,
                                             default_arrow_writer_properties(), &buffer));

  std::unique_ptr<FileReader> reader;
  FileReaderBuilder builder;
  ReaderProperties properties = default_reader_properties();
  ArrowReaderProperties arrow_properties = default_arrow_reader_properties();
  arrow_properties.set_pre_buffer(true);
  ASSERT_OK(builder.Open(std::make_shared<BufferReader>(buffer), properties));
  ASSERT_OK(builder.properties(arrow_properties)->Build(&reader));
  reader->set_use_threads(true);

  // Test multiple subsets to ensure we can read from the file multiple times
  std::vector<std::vector<int>> column_subsets = {
      {0, 4, 8, 10}, {0, 1, 2, 3}, {5, 17, 18, 19}};

  for (std::vector<int>& column_subset : column_subsets) {
    std::shared_ptr<Table> result;
    ASSERT_OK(reader->ReadTable(column_subset, &result));

    std::vector<std::shared_ptr<::arrow::ChunkedArray>> ex_columns;
    std::vector<std::shared_ptr<::arrow::Field>> ex_fields;
    for (int i : column_subset) {
      ex_columns.push_back(table->column(i));
      ex_fields.push_back(table->field(i));
    }

    auto ex_schema = ::arrow::schema(ex_fields);
    auto expected = Table::Make(ex_schema, ex_columns);
    ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*expected, *result));
  }
}

TEST(TestArrowReadWrite, ListLargeRecords) {
  // PARQUET-1308: This test passed on Linux when num_rows was smaller
  const int num_rows = 2000;
  const int row_group_size = 100;

  std::shared_ptr<Array> list_array;
  std::shared_ptr<DataType> list_type;

  MakeSimpleListArray(num_rows, 20, "item", &list_type, &list_array);

  auto schema = ::arrow::schema({::arrow::field("a", list_type)});

  std::shared_ptr<Table> table = Table::Make(schema, {list_array});

  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(WriteTableToBuffer(table, row_group_size,
                                             default_arrow_writer_properties(), &buffer));

  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                              ::arrow::default_memory_pool(), &reader));

  // Read everything
  std::shared_ptr<Table> result;
  ASSERT_OK_NO_THROW(reader->ReadTable(&result));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result));

  // Read 1 record at a time
  ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                              ::arrow::default_memory_pool(), &reader));

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
  auto chunked_table = Table::Make(table->schema(), {chunked});

  ASSERT_TRUE(table->Equals(*chunked_table));
}

typedef std::function<void(int, std::shared_ptr<DataType>*, std::shared_ptr<Array>*)>
    ArrayFactory;

template <typename ArrowType>
struct GenerateArrayFunctor {
  explicit GenerateArrayFunctor(double pct_null = 0.1) : pct_null(pct_null) {}

  void operator()(int length, std::shared_ptr<DataType>* type,
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

typedef std::function<void(int, std::shared_ptr<DataType>*, std::shared_ptr<Array>*)>
    ArrayFactory;

auto GenerateInt32 = [](int length, std::shared_ptr<DataType>* type,
                        std::shared_ptr<Array>* array) {
  GenerateArrayFunctor<::arrow::Int32Type> func;
  func(length, type, array);
};

auto GenerateList = [](int length, std::shared_ptr<DataType>* type,
                       std::shared_ptr<Array>* array) {
  MakeSimpleListArray(length, 100, "element", type, array);
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
  auto sink = CreateOutputStream();
  auto invalid_table = InvalidTable();

  ASSERT_RAISES(Invalid, WriteTable(*invalid_table, ::arrow::default_memory_pool(),
                                    CreateOutputStream(), 1, default_writer_properties(),
                                    default_arrow_writer_properties()));
}

TEST(TestArrowReadWrite, TableWithChunkedColumns) {
  std::vector<ArrayFactory> functions = {GenerateInt32, GenerateList};

  std::vector<int> chunk_sizes = {2, 4, 10, 2};
  const int64_t total_length = 18;

  for (const auto& datagen_func : functions) {
    ::arrow::ArrayVector arrays;
    std::shared_ptr<Array> arr;
    std::shared_ptr<DataType> type;
    datagen_func(total_length, &type, &arr);

    int64_t offset = 0;
    for (int chunk_size : chunk_sizes) {
      arrays.push_back(arr->Slice(offset, chunk_size));
      offset += chunk_size;
    }

    auto field = ::arrow::field("fname", type);
    auto schema = ::arrow::schema({field});
    auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(arrays)});

    ASSERT_NO_FATAL_FAILURE(CheckSimpleRoundtrip(table, 2));
    ASSERT_NO_FATAL_FAILURE(CheckSimpleRoundtrip(table, 3));
    ASSERT_NO_FATAL_FAILURE(CheckSimpleRoundtrip(table, 10));
  }
}

TEST(TestArrowReadWrite, ManySmallLists) {
  // ARROW-11607: The actual scenario this forces is no data reads for
  // a first batch, and then a single element read for the second batch.

  // Constructs
  std::shared_ptr<::arrow::Int32Builder> value_builder =
      std::make_shared<::arrow::Int32Builder>();
  constexpr int64_t kNullCount = 6;
  auto type = ::arrow::list(::arrow::int32());
  std::vector<std::shared_ptr<Array>> arrays(1);
  arrays[0] = ArrayFromJSON(type, R"([null, null, null, null, null, null, [1]])");

  auto field = ::arrow::field("fname", type);
  auto schema = ::arrow::schema({field});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(arrays)});
  ASSERT_EQ(table->num_rows(), kNullCount + 1);

  CheckSimpleRoundtrip(table, /*row_group_size=*/kNullCount,
                       default_arrow_writer_properties());
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

  auto table = Table::Make(schema, {a0, a1});
  ASSERT_NO_FATAL_FAILURE(CheckSimpleRoundtrip(table, table->num_rows()));
}

TEST(ArrowReadWrite, EmptyStruct) {
  // ARROW-10928: empty struct type not supported
  {
    // Empty struct as only column
    auto fields = ::arrow::FieldVector{
        ::arrow::field("structs", ::arrow::struct_(::arrow::FieldVector{}))};
    auto schema = ::arrow::schema(fields);
    auto columns = ArrayVector{ArrayFromJSON(fields[0]->type(), "[null, {}]")};
    auto table = Table::Make(schema, columns);

    auto sink = CreateOutputStream();
    ASSERT_RAISES(
        NotImplemented,
        WriteTable(*table, ::arrow::default_memory_pool(), sink, /*chunk_size=*/1,
                   default_writer_properties(), default_arrow_writer_properties()));
  }
  {
    // Empty struct as nested column
    auto fields = ::arrow::FieldVector{::arrow::field(
        "structs", ::arrow::list(::arrow::struct_(::arrow::FieldVector{})))};
    auto schema = ::arrow::schema(fields);
    auto columns =
        ArrayVector{ArrayFromJSON(fields[0]->type(), "[null, [], [null, {}]]")};
    auto table = Table::Make(schema, columns);

    auto sink = CreateOutputStream();
    ASSERT_RAISES(
        NotImplemented,
        WriteTable(*table, ::arrow::default_memory_pool(), sink, /*chunk_size=*/1,
                   default_writer_properties(), default_arrow_writer_properties()));
  }
  {
    // Empty struct along other column
    auto fields = ::arrow::FieldVector{
        ::arrow::field("structs", ::arrow::struct_(::arrow::FieldVector{})),
        ::arrow::field("ints", ::arrow::int32())};
    auto schema = ::arrow::schema(fields);
    auto columns = ArrayVector{ArrayFromJSON(fields[0]->type(), "[null, {}]"),
                               ArrayFromJSON(fields[1]->type(), "[1, 2]")};
    auto table = Table::Make(schema, columns);

    auto sink = CreateOutputStream();
    ASSERT_RAISES(
        NotImplemented,
        WriteTable(*table, ::arrow::default_memory_pool(), sink, /*chunk_size=*/1,
                   default_writer_properties(), default_arrow_writer_properties()));
  }
}

TEST(ArrowReadWrite, SimpleStructRoundTrip) {
  auto links = field(
      "Links", ::arrow::struct_({field("Backward", ::arrow::int64(), /*nullable=*/true),
                                 field("Forward", ::arrow::int64(), /*nullable=*/true)}));

  auto links_id_array = ::arrow::ArrayFromJSON(links->type(),
                                               "[{\"Backward\": null, \"Forward\": 20}, "
                                               "{\"Backward\": 10, \"Forward\": 40}]");

  CheckSimpleRoundtrip(
      ::arrow::Table::Make(std::make_shared<::arrow::Schema>(
                               std::vector<std::shared_ptr<::arrow::Field>>{links}),
                           {links_id_array}),
      2);
}

TEST(ArrowReadWrite, SingleColumnNullableStruct) {
  auto links =
      field("Links",
            ::arrow::struct_({field("Backward", ::arrow::int64(), /*nullable=*/true)}));

  auto links_id_array = ::arrow::ArrayFromJSON(links->type(),
                                               "[null, "
                                               "{\"Backward\": 10}"
                                               "]");

  CheckSimpleRoundtrip(
      ::arrow::Table::Make(std::make_shared<::arrow::Schema>(
                               std::vector<std::shared_ptr<::arrow::Field>>{links}),
                           {links_id_array}),
      3);
}

TEST(ArrowReadWrite, NestedRequiredField) {
  auto int_field = ::arrow::field("int_array", ::arrow::int32(), /*nullable=*/false);
  auto int_array = ::arrow::ArrayFromJSON(int_field->type(), "[0, 1, 2, 3, 4, 5, 7, 8]");
  auto struct_field =
      ::arrow::field("root", ::arrow::struct_({int_field}), /*nullable=*/true);
  std::shared_ptr<Buffer> validity_bitmap;
  ASSERT_OK_AND_ASSIGN(validity_bitmap, ::arrow::AllocateBitmap(8));
  validity_bitmap->mutable_data()[0] = 0xCC;

  auto struct_data = ArrayData::Make(struct_field->type(), /*length=*/8,
                                     {validity_bitmap}, {int_array->data()});
  CheckSimpleRoundtrip(::arrow::Table::Make(::arrow::schema({struct_field}),
                                            {::arrow::MakeArray(struct_data)}),
                       /*row_group_size=*/8);
}

TEST(ArrowReadWrite, Decimal256) {
  using ::arrow::Decimal256;
  using ::arrow::field;

  auto type = ::arrow::decimal256(8, 4);

  const char* json = R"(["1.0000", null, "-1.2345", "-1000.5678",
                         "-9999.9999", "9999.9999"])";
  auto array = ::arrow::ArrayFromJSON(type, json);
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});
  auto props_store_schema = ArrowWriterProperties::Builder().store_schema()->build();
  CheckSimpleRoundtrip(table, 2, props_store_schema);
}

TEST(ArrowReadWrite, DecimalStats) {
  using ::arrow::Decimal128;
  using ::arrow::field;

  auto type = ::arrow::decimal128(/*precision=*/8, /*scale=*/0);

  const char* json = R"(["255", "128", null, "0", "1", "-127", "-128", "-129", "-255"])";
  auto array = ::arrow::ArrayFromJSON(type, json);
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});

  std::shared_ptr<Buffer> buffer;
  ASSERT_NO_FATAL_FAILURE(WriteTableToBuffer(table, /*row_grop_size=*/100,
                                             default_arrow_writer_properties(), &buffer));

  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                              ::arrow::default_memory_pool(), &reader));

  std::shared_ptr<Scalar> min, max;
  ReadSingleColumnFileStatistics(std::move(reader), &min, &max);

  std::shared_ptr<Scalar> expected_min, expected_max;
  ASSERT_OK_AND_ASSIGN(expected_min, array->GetScalar(array->length() - 1));
  ASSERT_OK_AND_ASSIGN(expected_max, array->GetScalar(0));
  ::arrow::AssertScalarsEqual(*expected_min, *min, /*verbose=*/true);
  ::arrow::AssertScalarsEqual(*expected_max, *max, /*verbose=*/true);
}

TEST(ArrowReadWrite, NestedNullableField) {
  auto int_field = ::arrow::field("int_array", ::arrow::int32());
  auto int_array =
      ::arrow::ArrayFromJSON(int_field->type(), "[0, null, 2, null, 4, 5, null, 8]");
  auto struct_field =
      ::arrow::field("root", ::arrow::struct_({int_field}), /*nullable=*/true);
  std::shared_ptr<Buffer> validity_bitmap;
  ASSERT_OK_AND_ASSIGN(validity_bitmap, ::arrow::AllocateBitmap(8));
  validity_bitmap->mutable_data()[0] = 0xCC;

  auto struct_data = ArrayData::Make(struct_field->type(), /*length=*/8,
                                     {validity_bitmap}, {int_array->data()});
  CheckSimpleRoundtrip(::arrow::Table::Make(::arrow::schema({struct_field}),
                                            {::arrow::MakeArray(struct_data)}),
                       /*row_group_size=*/8);
}

TEST(TestArrowReadWrite, CanonicalNestedRoundTrip) {
  auto doc_id = field("DocId", ::arrow::int64(), /*nullable=*/false);
  auto links = field(
      "Links",
      ::arrow::struct_({field("Backward", list(::arrow::int64()), /*nullable=*/false),
                        field("Forward", list(::arrow::int64()), /*nullable=*/false)}));
  auto name_struct = field(
      "NameStruct",
      ::arrow::struct_(
          {field("Language",
                 ::arrow::list(field(
                     "lang_struct",
                     ::arrow::struct_({field("Code", ::arrow::utf8(), /*nullable=*/false),
                                       field("Country", ::arrow::utf8())})))),
           field("Url", ::arrow::utf8())}));
  auto name = field("Name", ::arrow::list(name_struct), /*nullable=*/false);
  auto schema = std::make_shared<::arrow::Schema>(
      std::vector<std::shared_ptr<::arrow::Field>>({doc_id, links, name}));

  auto doc_id_array = ::arrow::ArrayFromJSON(doc_id->type(), "[10, 20]");
  auto links_id_array =
      ::arrow::ArrayFromJSON(links->type(),
                             "[{\"Backward\":[], \"Forward\":[20, 40, 60]}, "
                             "{\"Backward\":[10, 30], \"Forward\":[80]}]");

  // Written without C++11 string literal because many editors don't have C++11
  // string literals implemented properly
  auto name_array = ::arrow::ArrayFromJSON(
      name->type(),
      "[[{\"Language\": [{\"Code\": \"en_us\", \"Country\":\"us\"},"
      "{\"Code\": \"en_us\", \"Country\": null}],"
      "\"Url\": \"http://A\"},"
      "{\"Url\": \"http://B\"},"
      "{\"Language\": [{\"Code\": \"en-gb\", \"Country\": \"gb\"}]}],"
      "[{\"Url\": \"http://C\"}]]");
  auto expected =
      ::arrow::Table::Make(schema, {doc_id_array, links_id_array, name_array});
  CheckSimpleRoundtrip(expected, 2);
}

TEST(ArrowReadWrite, ListOfStruct) {
  using ::arrow::field;

  auto type = ::arrow::list(::arrow::struct_(
      {field("a", ::arrow::int16(), /*nullable=*/false), field("b", ::arrow::utf8())}));

  const char* json = R"([
      [{"a": 4, "b": "foo"}, {"a": 5}, {"a": 6, "b": "bar"}],
      [null, {"a": 7}],
      null,
      []])";
  auto array = ::arrow::ArrayFromJSON(type, json);
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});
  CheckSimpleRoundtrip(table, 2);
}

TEST(ArrowReadWrite, ListOfStructOfList1) {
  using ::arrow::field;
  using ::arrow::list;
  using ::arrow::struct_;

  auto type = list(struct_({field("a", ::arrow::int16(), /*nullable=*/false),
                            field("b", list(::arrow::int64()))}));

  const char* json = R"([
      [{"a": 123, "b": [1, 2, null, 3]}, null],
      null,
      [],
      [{"a": 456}, {"a": 789, "b": []}, {"a": 876, "b": [4, 5, 6]}]])";
  auto array = ::arrow::ArrayFromJSON(type, json);
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});
  CheckSimpleRoundtrip(table, 2);
}

TEST(ArrowReadWrite, ListWithNoValues) {
  using ::arrow::Buffer;
  using ::arrow::field;

  auto type = list(field("item", ::arrow::int32(), /*nullable=*/false));
  auto array = ::arrow::ArrayFromJSON(type, "[null, []]");
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});
  auto props_store_schema = ArrowWriterProperties::Builder().store_schema()->build();
  CheckSimpleRoundtrip(table, 2, props_store_schema);
}

TEST(ArrowReadWrite, Map) {
  using ::arrow::field;
  using ::arrow::map;

  auto type = map(::arrow::int16(), ::arrow::utf8());

  const char* json = R"([
      [[1, "a"], [2, "b"]],
      [[3, "c"]],
      [],
      null,
      [[4, "d"], [5, "e"], [6, "f"]]
  ])";
  auto array = ::arrow::ArrayFromJSON(type, json);
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});
  auto props_store_schema = ArrowWriterProperties::Builder().store_schema()->build();
  CheckSimpleRoundtrip(table, 2, props_store_schema);
}

TEST(ArrowReadWrite, LargeList) {
  using ::arrow::field;
  using ::arrow::large_list;
  using ::arrow::struct_;

  auto type = large_list(::arrow::int16());

  const char* json = R"([
      [1, 2, 3],
      [4, 5, 6],
      [7, 8, 9]])";
  auto array = ::arrow::ArrayFromJSON(type, json);
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});
  auto props_store_schema = ArrowWriterProperties::Builder().store_schema()->build();
  CheckSimpleRoundtrip(table, 2, props_store_schema);
}

TEST(ArrowReadWrite, FixedSizeList) {
  using ::arrow::field;
  using ::arrow::fixed_size_list;
  using ::arrow::struct_;

  auto type = fixed_size_list(::arrow::int16(), /*size=*/3);

  const char* json = R"([
      [1, 2, 3],
      [4, 5, 6],
      [7, 8, 9]])";
  auto array = ::arrow::ArrayFromJSON(type, json);
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});
  auto props_store_schema = ArrowWriterProperties::Builder().store_schema()->build();
  CheckSimpleRoundtrip(table, 2, props_store_schema);
}

TEST(ArrowReadWrite, ListOfStructOfList2) {
  using ::arrow::field;
  using ::arrow::list;
  using ::arrow::struct_;

  auto type =
      list(field("item",
                 struct_({field("a", ::arrow::int16(), /*nullable=*/false),
                          field("b", list(::arrow::int64()), /*nullable=*/false)}),
                 /*nullable=*/false));

  const char* json = R"([
      [{"a": 123, "b": [1, 2, 3]}],
      null,
      [],
      [{"a": 456, "b": []}, {"a": 789, "b": [null]}, {"a": 876, "b": [4, 5, 6]}]])";
  auto array = ::arrow::ArrayFromJSON(type, json);
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});
  CheckSimpleRoundtrip(table, 2);
}

TEST(ArrowReadWrite, StructOfLists) {
  using ::arrow::field;
  using ::arrow::list;

  auto type = ::arrow::struct_(
      {field("a", list(::arrow::utf8()), /*nullable=*/false),
       field("b", list(field("f", ::arrow::int64(), /*nullable=*/false)))});

  const char* json = R"([
      {"a": ["1", "2"], "b": []},
      {"a": [], "b": [3, 4, 5]},
      {"a": ["6"], "b": null},
      {"a": [null, "7"], "b": [8]}])";
  auto array = ::arrow::ArrayFromJSON(type, json);
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});
  CheckSimpleRoundtrip(table, 2);
}

TEST(ArrowReadWrite, ListOfStructOfLists1) {
  using ::arrow::field;
  using ::arrow::list;

  auto type = list(::arrow::struct_(
      {field("a", list(::arrow::utf8()), /*nullable=*/false),
       field("b", list(field("f", ::arrow::int64(), /*nullable=*/false)))}));

  const char* json = R"([
      [{"a": ["1", "2"], "b": []}, null],
      [],
      null,
      [null],
      [{"a": [], "b": [3, 4, 5]}, {"a": ["6"], "b": null}],
      [null, {"a": [null, "7"], "b": [8]}]])";
  auto array = ::arrow::ArrayFromJSON(type, json);
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});
  CheckSimpleRoundtrip(table, 2);
}

TEST(ArrowReadWrite, ListOfStructOfLists2) {
  using ::arrow::field;
  using ::arrow::list;

  auto type = list(
      field("x",
            ::arrow::struct_(
                {field("a", list(::arrow::utf8()), /*nullable=*/false),
                 field("b", list(field("f", ::arrow::int64(), /*nullable=*/false)))}),
            /*nullable=*/false));

  const char* json = R"([
      [{"a": ["1", "2"], "b": []}],
      [],
      null,
      [],
      [{"a": [], "b": [3, 4, 5]}, {"a": ["6"], "b": null}],
      [{"a": [null, "7"], "b": [8]}]])";
  auto array = ::arrow::ArrayFromJSON(type, json);
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});
  CheckSimpleRoundtrip(table, 2);
}

TEST(ArrowReadWrite, ListOfStructOfLists3) {
  using ::arrow::field;
  using ::arrow::list;

  auto type = list(field(
      "x",
      ::arrow::struct_({field("a", list(::arrow::utf8()), /*nullable=*/false),
                        field("b", list(field("f", ::arrow::int64(), /*nullable=*/false)),
                              /*nullable=*/false)}),
      /*nullable=*/false));

  const char* json = R"([
      [{"a": ["1", "2"], "b": []}],
      [],
      null,
      [],
      [{"a": [], "b": [3, 4, 5]}, {"a": ["6"], "b": []}],
      [{"a": [null, "7"], "b": [8]}]])";
  auto array = ::arrow::ArrayFromJSON(type, json);
  auto table = ::arrow::Table::Make(::arrow::schema({field("root", type)}), {array});
  CheckSimpleRoundtrip(table, 2);
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

  auto value_type = ::arrow::utf8();
  auto dict_type = ::arrow::dictionary(::arrow::int32(), value_type);

  auto f0 = field("dictionary", dict_type);
  std::vector<std::shared_ptr<::arrow::Field>> fields;
  fields.emplace_back(f0);
  auto schema = ::arrow::schema(fields);

  std::shared_ptr<Array> f0_values, f1_values;
  ArrayFromVector<::arrow::Int32Type, int32_t>({0, 1, 0, 2, 1}, &f0_values);
  ArrayFromVector<::arrow::Int32Type, int32_t>({2, 0, 1, 0, 2}, &f1_values);
  ::arrow::ArrayVector dict_arrays = {
      std::make_shared<::arrow::DictionaryArray>(dict_type, f0_values, dict_values),
      std::make_shared<::arrow::DictionaryArray>(dict_type, f1_values, dict_values)};

  std::vector<std::shared_ptr<ChunkedArray>> columns;
  columns.emplace_back(std::make_shared<ChunkedArray>(dict_arrays));

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
  columns.emplace_back(std::make_shared<ChunkedArray>(expected_array));

  schema = ::arrow::schema({::arrow::field("dictionary", ::arrow::utf8())});

  auto expected_table = Table::Make(schema, columns);

  ::arrow::AssertTablesEqual(*expected_table, *result, false);
}

TEST(TestArrowReadWrite, NonUniqueDictionaryValues) {
  // ARROW-10237
  auto dict_with_dupes = ArrayFromJSON(::arrow::utf8(), R"(["a", "a", "b"])");
  // test with all valid 4-long `indices`
  for (int i = 0; i < 4 * 4 * 4 * 4; ++i) {
    int j = i;
    ASSERT_OK_AND_ASSIGN(
        auto indices,
        ArrayFromBuilderVisitor(::arrow::int32(), 4, [&](::arrow::Int32Builder* b) {
          if (j % 4 < dict_with_dupes->length()) {
            b->UnsafeAppend(j % 4);
          } else {
            b->UnsafeAppendNull();
          }
          j /= 4;
        }));
    ASSERT_OK_AND_ASSIGN(auto plain, ::arrow::compute::Take(*dict_with_dupes, *indices));
    ASSERT_OK_AND_ASSIGN(auto encoded,
                         ::arrow::DictionaryArray::FromArrays(indices, dict_with_dupes));

    auto table = Table::Make(::arrow::schema({::arrow::field("d", encoded->type())}),
                             ::arrow::ArrayVector{encoded});

    ASSERT_OK(table->ValidateFull());

    std::shared_ptr<Table> round_tripped;
    ASSERT_NO_FATAL_FAILURE(DoSimpleRoundtrip(table, true, 20, {}, &round_tripped));

    ASSERT_OK(round_tripped->ValidateFull());
    ::arrow::AssertArraysEqual(*plain, *round_tripped->column(0)->chunk(0), true);
  }
}

TEST(TestArrowWrite, CheckChunkSize) {
  const int num_columns = 2;
  const int num_rows = 128;
  const int64_t chunk_size = 0;  // note the chunk_size is 0
  std::shared_ptr<Table> table;
  ASSERT_NO_FATAL_FAILURE(MakeDoubleTable(num_columns, num_rows, 1, &table));

  auto sink = CreateOutputStream();

  ASSERT_RAISES(Invalid,
                WriteTable(*table, ::arrow::default_memory_pool(), sink, chunk_size));
}

class TestNestedSchemaRead : public ::testing::TestWithParam<Repetition::type> {
 protected:
  // make it *3 to make it easily divisible by 3
  const int NUM_SIMPLE_TEST_ROWS = SMALL_SIZE * 3;
  std::shared_ptr<::arrow::Int32Array> values_array_ = nullptr;

  void InitReader() {
    ASSERT_OK_AND_ASSIGN(auto buffer, nested_parquet_->Finish());
    ASSERT_OK_NO_THROW(OpenFile(std::make_shared<BufferReader>(buffer),
                                ::arrow::default_memory_pool(), &reader_));
  }

  void InitNewParquetFile(const std::shared_ptr<GroupNode>& schema, int num_rows) {
    nested_parquet_ = CreateOutputStream();

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
    ASSERT_OK(array.ValidateFull());
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
      const std::shared_ptr<ChunkedArray> column = table.column(i);
      // Compare with the array type
      ASSERT_TRUE(schema_field->type()->Equals(column->chunk(0)->type()));
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

  std::shared_ptr<::arrow::io::BufferOutputStream> nested_parquet_;
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
  ASSERT_EQ(table->schema()->field(0)->type()->num_fields(), 2);
  ASSERT_NO_FATAL_FAILURE(ValidateTableArrayTypes(*table));

  auto struct_field_array =
      std::static_pointer_cast<::arrow::StructArray>(table->column(0)->chunk(0));
  auto leaf1_array =
      std::static_pointer_cast<::arrow::Int32Array>(struct_field_array->field(0));
  auto leaf2_array =
      std::static_pointer_cast<::arrow::Int32Array>(struct_field_array->field(1));
  auto leaf3_array =
      std::static_pointer_cast<::arrow::Int32Array>(table->column(1)->chunk(0));

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
  ASSERT_EQ(table->schema()->field(0)->type()->num_fields(), 1);
  ASSERT_NO_FATAL_FAILURE(ValidateTableArrayTypes(*table));

  // columns: {group1.leaf1, leaf3}
  ASSERT_OK_NO_THROW(reader_->ReadRowGroup(0, {0, 2}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 2);
  ASSERT_EQ(table->schema()->field(0)->name(), "group1");
  ASSERT_EQ(table->schema()->field(1)->name(), "leaf3");
  ASSERT_EQ(table->schema()->field(0)->type()->num_fields(), 1);
  ASSERT_NO_FATAL_FAILURE(ValidateTableArrayTypes(*table));

  // columns: {group1.leaf1, group1.leaf2}
  ASSERT_OK_NO_THROW(reader_->ReadTable({0, 1}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 1);
  ASSERT_EQ(table->schema()->field(0)->name(), "group1");
  ASSERT_EQ(table->schema()->field(0)->type()->num_fields(), 2);
  ASSERT_NO_FATAL_FAILURE(ValidateTableArrayTypes(*table));

  // columns: {leaf3}
  ASSERT_OK_NO_THROW(reader_->ReadTable({2}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 1);
  ASSERT_EQ(table->schema()->field(0)->name(), "leaf3");
  ASSERT_EQ(table->schema()->field(0)->type()->num_fields(), 0);
  ASSERT_NO_FATAL_FAILURE(ValidateTableArrayTypes(*table));

  // Test with different ordering
  ASSERT_OK_NO_THROW(reader_->ReadTable({2, 0}, &table));
  ASSERT_EQ(table->num_rows(), NUM_SIMPLE_TEST_ROWS);
  ASSERT_EQ(table->num_columns(), 2);
  ASSERT_EQ(table->schema()->field(0)->name(), "leaf3");
  ASSERT_EQ(table->schema()->field(1)->name(), "group1");
  ASSERT_EQ(table->schema()->field(1)->type()->num_fields(), 1);
  ASSERT_NO_FATAL_FAILURE(ValidateTableArrayTypes(*table));
}

TEST_P(TestNestedSchemaRead, DeepNestedSchemaRead) {
#ifdef PARQUET_VALGRIND
  const int num_trees = 3;
  const int depth = 3;
#else
  const int num_trees = 2;
  const int depth = 2;
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
    auto tree = table->column(i)->chunk(0);
    ASSERT_OK_NO_THROW(visitor.Validate(tree));
  }
}

INSTANTIATE_TEST_SUITE_P(Repetition_type, TestNestedSchemaRead,
                         ::testing::Values(Repetition::REQUIRED, Repetition::OPTIONAL));

TEST(TestImpalaConversion, ArrowTimestampToImpalaTimestamp) {
  // June 20, 2017 16:32:56 and 123456789 nanoseconds
  int64_t nanoseconds = INT64_C(1497976376123456789);

  Int96 calculated;

  Int96 expected = {{UINT32_C(632093973), UINT32_C(13871), UINT32_C(2457925)}};
  ::parquet::internal::NanosecondsToImpalaTimestamp(nanoseconds, &calculated);
  ASSERT_EQ(expected, calculated);
}

void TryReadDataFile(const std::string& path,
                     ::arrow::StatusCode expected_code = ::arrow::StatusCode::OK) {
  auto pool = ::arrow::default_memory_pool();

  std::unique_ptr<FileReader> arrow_reader;
  Status s =
      FileReader::Make(pool, ParquetFileReader::OpenFile(path, false), &arrow_reader);
  if (s.ok()) {
    std::shared_ptr<::arrow::Table> table;
    s = arrow_reader->ReadTable(&table);
  }

  ASSERT_EQ(s.code(), expected_code)
      << "Expected reading file to return " << arrow::Status::CodeAsString(expected_code)
      << ", but got " << s.ToString();
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

TEST(TestArrowReaderAdHoc, LARGE_MEMORY_TEST(LargeStringColumn)) {
  // ARROW-3762
  ::arrow::StringBuilder builder;
  int64_t length = 1 << 30;
  ASSERT_OK(builder.Resize(length));
  ASSERT_OK(builder.ReserveData(length));
  for (int64_t i = 0; i < length; ++i) {
    builder.UnsafeAppend("1", 1);
  }
  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));
  auto table =
      Table::Make(::arrow::schema({::arrow::field("x", ::arrow::utf8())}), {array});
  std::shared_ptr<SchemaDescriptor> schm;
  ASSERT_OK_NO_THROW(
      ToParquetSchema(table->schema().get(), *default_writer_properties(), &schm));

  auto sink = CreateOutputStream();

  auto schm_node = std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, {schm->group_node()->field(0)}));

  auto writer = ParquetFileWriter::Open(sink, schm_node);

  std::unique_ptr<FileWriter> arrow_writer;
  ASSERT_OK_NO_THROW(FileWriter::Make(::arrow::default_memory_pool(), std::move(writer),
                                      table->schema(), default_arrow_writer_properties(),
                                      &arrow_writer));
  for (int i : {0, 1}) {
    ASSERT_OK_NO_THROW(arrow_writer->WriteTable(*table, table->num_rows())) << i;
  }
  ASSERT_OK_NO_THROW(arrow_writer->Close());

  ASSERT_OK_AND_ASSIGN(auto tables_buffer, sink->Finish());

  // drop to save memory
  table.reset();
  array.reset();

  auto reader = ParquetFileReader::Open(std::make_shared<BufferReader>(tables_buffer));
  std::unique_ptr<FileReader> arrow_reader;
  ASSERT_OK(FileReader::Make(default_memory_pool(), std::move(reader), &arrow_reader));
  ASSERT_OK_NO_THROW(arrow_reader->ReadTable(&table));
  ASSERT_OK(table->ValidateFull());

  // ARROW-9297: ensure RecordBatchReader also works
  reader = ParquetFileReader::Open(std::make_shared<BufferReader>(tables_buffer));
  ASSERT_OK(FileReader::Make(default_memory_pool(), std::move(reader), &arrow_reader));
  std::shared_ptr<::arrow::RecordBatchReader> batch_reader;
  std::vector<int> all_row_groups =
      ::arrow::internal::Iota(reader->metadata()->num_row_groups());
  ASSERT_OK_NO_THROW(arrow_reader->GetRecordBatchReader(all_row_groups, &batch_reader));
  ASSERT_OK_AND_ASSIGN(auto batched_table,
                       ::arrow::Table::FromRecordBatchReader(batch_reader.get()));

  ASSERT_OK(batched_table->ValidateFull());
  AssertTablesEqual(*table, *batched_table, /*same_chunk_layout=*/false);
}

TEST(TestArrowReaderAdHoc, HandleDictPageOffsetZero) {
  // PARQUET-1402: parquet-mr writes files this way which tripped up
  // some business logic
  TryReadDataFile(test::get_data_file("dict-page-offset-zero.parquet"));
}

TEST(TestArrowReaderAdHoc, WriteBatchedNestedNullableStringColumn) {
  // ARROW-10493
  std::vector<std::shared_ptr<::arrow::Field>> fields{
      ::arrow::field("s", ::arrow::utf8(), /*nullable=*/true),
      ::arrow::field("d", ::arrow::decimal128(4, 2), /*nullable=*/true),
      ::arrow::field("b", ::arrow::boolean(), /*nullable=*/true),
      ::arrow::field("i8", ::arrow::int8(), /*nullable=*/true),
      ::arrow::field("i64", ::arrow::int64(), /*nullable=*/true)};
  auto type = ::arrow::struct_(fields);
  auto outer_array = ::arrow::ArrayFromJSON(
      type,
      R"([{"s": "abc", "d": "1.23", "b": true, "i8": 10, "i64": 11 },
          {"s": "de", "d": "3.45", "b": true, "i8": 12, "i64": 13 },
          {"s": "fghi", "d": "6.78", "b": false, "i8": 14, "i64": 15 },
          {},
          {"s": "jklmo", "d": "9.10", "b": true, "i8": 16, "i64": 17 },
          null,
          {"s": "p", "d": "11.12", "b": false, "i8": 18, "i64": 19 },
          {"s": "qrst", "d": "13.14", "b": false, "i8": 20, "i64": 21 },
          {},
          {"s": "uvw", "d": "15.16", "b": true, "i8": 22, "i64": 23 },
          {"s": "x", "d": "17.18", "b": false, "i8": 24, "i64": 25 },
          {},
          null])");

  auto expected = Table::Make(
      ::arrow::schema({::arrow::field("outer", type, /*nullable=*/true)}), {outer_array});

  auto write_props = WriterProperties::Builder().write_batch_size(4)->build();

  std::shared_ptr<Table> actual;
  DoRoundtrip(expected, /*row_group_size=*/outer_array->length(), &actual, write_props);
  ::arrow::AssertTablesEqual(*expected, *actual, /*same_chunk_layout=*/false);
}

class TestArrowReaderAdHocSparkAndHvr
    : public ::testing::TestWithParam<
          std::tuple<std::string, std::shared_ptr<DataType>>> {};

TEST_P(TestArrowReaderAdHocSparkAndHvr, ReadDecimals) {
  std::string path(test::get_data_dir());

  std::string filename;
  std::shared_ptr<DataType> decimal_type;
  std::tie(filename, decimal_type) = GetParam();

  path += "/" + filename;
  ASSERT_GT(path.size(), 0);

  auto pool = ::arrow::default_memory_pool();

  std::unique_ptr<FileReader> arrow_reader;
  ASSERT_OK_NO_THROW(
      FileReader::Make(pool, ParquetFileReader::OpenFile(path, false), &arrow_reader));
  std::shared_ptr<::arrow::Table> table;
  ASSERT_OK_NO_THROW(arrow_reader->ReadTable(&table));

  std::shared_ptr<::arrow::Schema> schema;
  ASSERT_OK_NO_THROW(arrow_reader->GetSchema(&schema));
  ASSERT_EQ(1, schema->num_fields());
  ASSERT_TRUE(schema->field(0)->type()->Equals(*decimal_type));

  ASSERT_EQ(1, table->num_columns());

  constexpr int32_t expected_length = 24;

  auto value_column = table->column(0);
  ASSERT_EQ(expected_length, value_column->length());

  ASSERT_EQ(1, value_column->num_chunks());

  auto chunk = value_column->chunk(0);

  std::shared_ptr<Array> expected_array;

  ::arrow::Decimal128Builder builder(decimal_type, pool);

  for (int32_t i = 0; i < expected_length; ++i) {
    ::arrow::Decimal128 value((i + 1) * 100);
    ASSERT_OK(builder.Append(value));
  }
  ASSERT_OK(builder.Finish(&expected_array));
  AssertArraysEqual(*expected_array, *chunk);
}

INSTANTIATE_TEST_SUITE_P(
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
  ASSERT_OK_AND_ASSIGN(auto outs, BufferOutputStream::Create(1 << 10, pool));
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

class TestArrowWriteDictionary : public ::testing::TestWithParam<ParquetDataPageVersion> {
 public:
  ParquetDataPageVersion GetParquetDataPageVersion() { return GetParam(); }
};

TEST_P(TestArrowWriteDictionary, Statistics) {
  std::vector<std::shared_ptr<::arrow::Array>> test_dictionaries = {
      ArrayFromJSON(::arrow::utf8(), R"(["b", "c", "d", "a", "b", "c", "d", "a"])"),
      ArrayFromJSON(::arrow::utf8(), R"(["b", "c", "d", "a", "b", "c", "d", "a"])"),
      ArrayFromJSON(::arrow::binary(), R"(["d", "c", "b", "a", "d", "c", "b", "a"])"),
      ArrayFromJSON(::arrow::large_utf8(), R"(["a", "b", "c", "a", "b", "c"])")};
  std::vector<std::shared_ptr<::arrow::Array>> test_indices = {
      ArrayFromJSON(::arrow::int32(), R"([0, null, 3, 0, null, 3])"),
      ArrayFromJSON(::arrow::int32(), R"([0, 1, null, 0, 1, null])"),
      ArrayFromJSON(::arrow::int32(), R"([0, 1, 3, 0, 1, 3])"),
      ArrayFromJSON(::arrow::int32(), R"([null, null, null, null, null, null])")};
  // Arrays will be written with 3 values per row group, 2 values per data page.  The
  // row groups are identical for ease of testing.
  std::vector<int32_t> expected_valid_counts = {2, 2, 3, 0};
  std::vector<int32_t> expected_null_counts = {1, 1, 0, 3};
  std::vector<int> expected_num_data_pages = {2, 2, 2, 1};
  std::vector<std::vector<int32_t>> expected_valid_by_page = {
      {1, 1}, {2, 0}, {2, 1}, {0}};
  std::vector<std::vector<int64_t>> expected_null_by_page = {{1, 0}, {0, 1}, {0, 0}, {3}};
  std::vector<int32_t> expected_dict_counts = {4, 4, 4, 3};
  // Pairs of (min, max)
  std::vector<std::vector<std::string>> expected_min_max_ = {
      {"a", "b"}, {"b", "c"}, {"a", "d"}, {"", ""}};

  for (std::size_t case_index = 0; case_index < test_dictionaries.size(); case_index++) {
    SCOPED_TRACE(test_dictionaries[case_index]->type()->ToString());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<::arrow::Array> dict_encoded,
                         ::arrow::DictionaryArray::FromArrays(
                             test_indices[case_index], test_dictionaries[case_index]));
    std::shared_ptr<::arrow::Schema> schema =
        ::arrow::schema({::arrow::field("values", dict_encoded->type())});
    std::shared_ptr<::arrow::Table> table = ::arrow::Table::Make(schema, {dict_encoded});

    std::shared_ptr<::arrow::ResizableBuffer> serialized_data = AllocateBuffer();
    auto out_stream = std::make_shared<::arrow::io::BufferOutputStream>(serialized_data);
    std::shared_ptr<WriterProperties> writer_properties =
        WriterProperties::Builder()
            .max_row_group_length(3)
            ->data_page_version(this->GetParquetDataPageVersion())
            ->write_batch_size(2)
            ->data_pagesize(2)
            ->build();
    std::unique_ptr<FileWriter> writer;
    ASSERT_OK(FileWriter::Open(*schema, ::arrow::default_memory_pool(), out_stream,
                               writer_properties, default_arrow_writer_properties(),
                               &writer));
    ASSERT_OK(writer->WriteTable(*table, std::numeric_limits<int64_t>::max()));
    ASSERT_OK(writer->Close());
    ASSERT_OK(out_stream->Close());

    auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(serialized_data);
    std::unique_ptr<ParquetFileReader> parquet_reader =
        ParquetFileReader::Open(std::move(buffer_reader));

    // Check row group statistics
    std::shared_ptr<FileMetaData> metadata = parquet_reader->metadata();
    ASSERT_EQ(metadata->num_row_groups(), 2);
    for (int row_group_index = 0; row_group_index < 2; row_group_index++) {
      ASSERT_EQ(metadata->RowGroup(row_group_index)->num_columns(), 1);
      std::shared_ptr<Statistics> stats =
          metadata->RowGroup(row_group_index)->ColumnChunk(0)->statistics();

      EXPECT_EQ(stats->num_values(), expected_valid_counts[case_index]);
      EXPECT_EQ(stats->null_count(), expected_null_counts[case_index]);

      std::vector<std::string> case_expected_min_max = expected_min_max_[case_index];
      EXPECT_EQ(stats->EncodeMin(), case_expected_min_max[0]);
      EXPECT_EQ(stats->EncodeMax(), case_expected_min_max[1]);
    }

    for (int row_group_index = 0; row_group_index < 2; row_group_index++) {
      std::unique_ptr<PageReader> page_reader =
          parquet_reader->RowGroup(row_group_index)->GetColumnPageReader(0);
      std::shared_ptr<Page> page = page_reader->NextPage();
      ASSERT_NE(page, nullptr);
      DictionaryPage* dict_page = (DictionaryPage*)page.get();
      ASSERT_EQ(dict_page->num_values(), expected_dict_counts[case_index]);
      for (int page_index = 0; page_index < expected_num_data_pages[case_index];
           page_index++) {
        page = page_reader->NextPage();
        ASSERT_NE(page, nullptr);
        DataPage* data_page = (DataPage*)page.get();
        const EncodedStatistics& stats = data_page->statistics();
        EXPECT_EQ(stats.null_count, expected_null_by_page[case_index][page_index]);
        EXPECT_EQ(stats.has_min, false);
        EXPECT_EQ(stats.has_max, false);
        EXPECT_EQ(data_page->num_values(),
                  expected_valid_by_page[case_index][page_index] +
                      expected_null_by_page[case_index][page_index]);
      }
      ASSERT_EQ(page_reader->NextPage(), nullptr);
    }
  }
}

INSTANTIATE_TEST_SUITE_P(WriteDictionary, TestArrowWriteDictionary,
                         ::testing::Values(ParquetDataPageVersion::V1,
                                           ParquetDataPageVersion::V2));
// ----------------------------------------------------------------------
// Tests for directly reading DictionaryArray

class TestArrowReadDictionary : public ::testing::TestWithParam<double> {
 public:
  static constexpr int kNumRowGroups = 16;

  struct {
    int num_rows = 1024 * kNumRowGroups;
    int num_row_groups = kNumRowGroups;
    int num_uniques = 128;
  } options;

  void SetUp() override {
    properties_ = default_arrow_reader_properties();

    GenerateData(GetParam());
  }

  void GenerateData(double null_probability) {
    constexpr int64_t min_length = 2;
    constexpr int64_t max_length = 100;
    ::arrow::random::RandomArrayGenerator rag(0);
    dense_values_ = rag.StringWithRepeats(options.num_rows, options.num_uniques,
                                          min_length, max_length, null_probability);
    expected_dense_ = MakeSimpleTable(dense_values_, /*nullable=*/true);
  }

  void TearDown() override {}

  void WriteSimple() {
    // Write `num_row_groups` row groups; each row group will have a different dictionary
    ASSERT_NO_FATAL_FAILURE(
        WriteTableToBuffer(expected_dense_, options.num_rows / options.num_row_groups,
                           default_arrow_writer_properties(), &buffer_));
  }

  void CheckReadWholeFile(const Table& expected) {
    ASSERT_OK_AND_ASSIGN(auto reader, GetReader());

    std::shared_ptr<Table> actual;
    ASSERT_OK_NO_THROW(reader->ReadTable(&actual));
    ::arrow::AssertTablesEqual(expected, *actual, /*same_chunk_layout=*/false);
  }

  void CheckStreamReadWholeFile(const Table& expected) {
    ASSERT_OK_AND_ASSIGN(auto reader, GetReader());

    std::unique_ptr<::arrow::RecordBatchReader> rb;
    ASSERT_OK(reader->GetRecordBatchReader(
        ::arrow::internal::Iota(options.num_row_groups), &rb));

    std::shared_ptr<Table> actual;
    ASSERT_OK_NO_THROW(rb->ReadAll(&actual));
    ::arrow::AssertTablesEqual(expected, *actual, /*same_chunk_layout=*/false);
  }

  static std::vector<double> null_probabilities() { return {0.0, 0.5, 1}; }

 protected:
  std::shared_ptr<Array> dense_values_;
  std::shared_ptr<Table> expected_dense_;
  std::shared_ptr<Table> expected_dict_;
  std::shared_ptr<Buffer> buffer_;
  ArrowReaderProperties properties_;

  ::arrow::Result<std::unique_ptr<FileReader>> GetReader() {
    std::unique_ptr<FileReader> reader;

    FileReaderBuilder builder;
    RETURN_NOT_OK(builder.Open(std::make_shared<BufferReader>(buffer_)));
    RETURN_NOT_OK(builder.properties(properties_)->Build(&reader));

    return std::move(reader);
  }
};

void AsDictionary32Encoded(const Array& arr, std::shared_ptr<Array>* out) {
  ::arrow::StringDictionary32Builder builder(default_memory_pool());
  const auto& string_array = static_cast<const ::arrow::StringArray&>(arr);
  ASSERT_OK(builder.AppendArray(string_array));
  ASSERT_OK(builder.Finish(out));
}

TEST_P(TestArrowReadDictionary, ReadWholeFileDict) {
  properties_.set_read_dictionary(0, true);

  WriteSimple();

  auto num_row_groups = options.num_row_groups;
  auto chunk_size = options.num_rows / num_row_groups;

  std::vector<std::shared_ptr<Array>> chunks(num_row_groups);
  for (int i = 0; i < num_row_groups; ++i) {
    AsDictionary32Encoded(*dense_values_->Slice(chunk_size * i, chunk_size), &chunks[i]);
  }
  auto ex_table = MakeSimpleTable(std::make_shared<ChunkedArray>(chunks),
                                  /*nullable=*/true);
  CheckReadWholeFile(*ex_table);
}

TEST_P(TestArrowReadDictionary, ZeroChunksListOfDictionary) {
  // ARROW-8799
  properties_.set_read_dictionary(0, true);
  dense_values_.reset();
  auto values = std::make_shared<ChunkedArray>(::arrow::ArrayVector{},
                                               ::arrow::list(::arrow::utf8()));
  options.num_rows = 0;
  options.num_uniques = 0;
  options.num_row_groups = 1;
  expected_dense_ = MakeSimpleTable(values, false);

  WriteSimple();

  ASSERT_OK_AND_ASSIGN(auto reader, GetReader());

  std::unique_ptr<ColumnReader> column_reader;
  ASSERT_OK_NO_THROW(reader->GetColumn(0, &column_reader));

  std::shared_ptr<ChunkedArray> chunked_out;
  ASSERT_OK(column_reader->NextBatch(1 << 15, &chunked_out));

  ASSERT_EQ(chunked_out->length(), 0);
  ASSERT_EQ(chunked_out->num_chunks(), 1);
}

TEST_P(TestArrowReadDictionary, IncrementalReads) {
  // ARROW-6895
  options.num_rows = 100;
  options.num_uniques = 10;
  SetUp();

  properties_.set_read_dictionary(0, true);

  // Just write a single row group
  ASSERT_NO_FATAL_FAILURE(WriteTableToBuffer(
      expected_dense_, options.num_rows, default_arrow_writer_properties(), &buffer_));

  // Read in one shot
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileReader> reader, GetReader());
  std::shared_ptr<Table> expected;
  ASSERT_OK_NO_THROW(reader->ReadTable(&expected));

  ASSERT_OK_AND_ASSIGN(reader, GetReader());
  std::unique_ptr<ColumnReader> col;
  ASSERT_OK(reader->GetColumn(0, &col));

  int num_reads = 4;
  int batch_size = options.num_rows / num_reads;

  for (int i = 0; i < num_reads; ++i) {
    std::shared_ptr<ChunkedArray> chunk;
    ASSERT_OK(col->NextBatch(batch_size, &chunk));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> result_dense,
                         ::arrow::compute::Cast(*chunk->chunk(0), ::arrow::utf8()));
    AssertArraysEqual(*dense_values_->Slice(i * batch_size, batch_size), *result_dense);
  }
}

TEST_P(TestArrowReadDictionary, StreamReadWholeFileDict) {
  // ARROW-6895 and ARROW-7545 reading a parquet file with a dictionary of
  // binary data, e.g. String, will return invalid values when using the
  // RecordBatchReader (stream) interface. In some cases, this will trigger an
  // infinite loop of the calling thread.

  // Recompute generated data with only one row-group
  options.num_row_groups = 1;
  options.num_rows = 16;
  options.num_uniques = 7;
  SetUp();
  WriteSimple();

  // Would trigger an infinite loop when requesting a batch greater than the
  // number of available rows in a row group.
  properties_.set_batch_size(options.num_rows * 2);
  CheckStreamReadWholeFile(*expected_dense_);
}

TEST_P(TestArrowReadDictionary, ReadWholeFileDense) {
  properties_.set_read_dictionary(0, false);
  WriteSimple();
  CheckReadWholeFile(*expected_dense_);
}

INSTANTIATE_TEST_SUITE_P(
    ReadDictionary, TestArrowReadDictionary,
    ::testing::ValuesIn(TestArrowReadDictionary::null_probabilities()));

TEST(TestArrowWriteDictionaries, ChangingDictionaries) {
  constexpr int num_unique = 50;
  constexpr int repeat = 10000;
  constexpr int64_t min_length = 2;
  constexpr int64_t max_length = 20;
  ::arrow::random::RandomArrayGenerator rag(0);
  auto values = rag.StringWithRepeats(repeat * num_unique, num_unique, min_length,
                                      max_length, /*null_probability=*/0.1);
  auto expected = MakeSimpleTable(values, /*nullable=*/true);

  const int num_chunks = 10;
  std::vector<std::shared_ptr<Array>> chunks(num_chunks);
  const int64_t chunk_size = values->length() / num_chunks;
  for (int i = 0; i < num_chunks; ++i) {
    AsDictionary32Encoded(*values->Slice(chunk_size * i, chunk_size), &chunks[i]);
  }

  auto dict_table = MakeSimpleTable(std::make_shared<ChunkedArray>(chunks),
                                    /*nullable=*/true);

  std::shared_ptr<Table> actual;
  DoRoundtrip(dict_table, /*row_group_size=*/values->length() / 2, &actual);
  ::arrow::AssertTablesEqual(*expected, *actual, /*same_chunk_layout=*/false);
}

TEST(TestArrowWriteDictionaries, AutoReadAsDictionary) {
  constexpr int num_unique = 50;
  constexpr int repeat = 100;
  constexpr int64_t min_length = 2;
  constexpr int64_t max_length = 20;
  ::arrow::random::RandomArrayGenerator rag(0);
  auto values = rag.StringWithRepeats(repeat * num_unique, num_unique, min_length,
                                      max_length, /*null_probability=*/0.1);
  std::shared_ptr<Array> dict_values;
  AsDictionary32Encoded(*values, &dict_values);

  auto expected = MakeSimpleTable(dict_values, /*nullable=*/true);
  auto expected_dense = MakeSimpleTable(values, /*nullable=*/true);

  auto props_store_schema = ArrowWriterProperties::Builder().store_schema()->build();
  std::shared_ptr<Table> actual, actual_dense;

  DoRoundtrip(expected, values->length(), &actual, default_writer_properties(),
              props_store_schema);
  ::arrow::AssertTablesEqual(*expected, *actual);

  auto props_no_store_schema = ArrowWriterProperties::Builder().build();
  DoRoundtrip(expected, values->length(), &actual_dense, default_writer_properties(),
              props_no_store_schema);
  ::arrow::AssertTablesEqual(*expected_dense, *actual_dense);
}

TEST(TestArrowWriteDictionaries, NestedSubfield) {
  auto offsets = ::arrow::ArrayFromJSON(::arrow::int32(), "[0, 0, 2, 3]");
  auto indices = ::arrow::ArrayFromJSON(::arrow::int32(), "[0, 0, 0]");
  auto dict = ::arrow::ArrayFromJSON(::arrow::utf8(), "[\"foo\"]");

  auto dict_ty = ::arrow::dictionary(::arrow::int32(), ::arrow::utf8());
  ASSERT_OK_AND_ASSIGN(auto dict_values,
                       ::arrow::DictionaryArray::FromArrays(dict_ty, indices, dict));
  ASSERT_OK_AND_ASSIGN(auto values,
                       ::arrow::ListArray::FromArrays(*offsets, *dict_values));

  auto table = MakeSimpleTable(values, /*nullable=*/true);

  auto props_store_schema = ArrowWriterProperties::Builder().store_schema()->build();
  std::shared_ptr<Table> actual;
  DoRoundtrip(table, values->length(), &actual, default_writer_properties(),
              props_store_schema);

  ::arrow::AssertTablesEqual(*table, *actual);
}

#ifdef ARROW_CSV
TEST(TestArrowReadDeltaEncoding, DeltaBinaryPacked) {
  auto file = test::get_data_file("delta_binary_packed.parquet");
  auto expect_file = test::get_data_file("delta_binary_packed_expect.csv");
  auto pool = ::arrow::default_memory_pool();
  std::unique_ptr<FileReader> parquet_reader;
  std::shared_ptr<::arrow::Table> table;
  ASSERT_OK(
      FileReader::Make(pool, ParquetFileReader::OpenFile(file, false), &parquet_reader));
  ASSERT_OK(parquet_reader->ReadTable(&table));

  ASSERT_OK_AND_ASSIGN(auto input_file, ::arrow::io::ReadableFile::Open(expect_file));
  auto convert_options = ::arrow::csv::ConvertOptions::Defaults();
  for (int i = 0; i <= 64; ++i) {
    std::string column_name = "bitwidth" + std::to_string(i);
    convert_options.column_types[column_name] = ::arrow::int64();
  }
  convert_options.column_types["int_value"] = ::arrow::int32();
  ASSERT_OK_AND_ASSIGN(auto csv_reader,
                       ::arrow::csv::TableReader::Make(
                           ::arrow::io::default_io_context(), input_file,
                           ::arrow::csv::ReadOptions::Defaults(),
                           ::arrow::csv::ParseOptions::Defaults(), convert_options));
  ASSERT_OK_AND_ASSIGN(auto expect_table, csv_reader->Read());

  ::arrow::AssertTablesEqual(*table, *expect_table);
}
#else
TEST(TestArrowReadDeltaEncoding, DeltaBinaryPacked) {
  GTEST_SKIP() << "Test needs CSV reader";
}
#endif

struct NestedFilterTestCase {
  std::shared_ptr<::arrow::DataType> write_schema;
  std::vector<int> indices_to_read;
  std::shared_ptr<::arrow::DataType> expected_schema;
  std::string write_data;
  std::string read_data;
};
class TestNestedSchemaFilteredReader
    : public ::testing::TestWithParam<NestedFilterTestCase> {};

TEST_P(TestNestedSchemaFilteredReader, ReadWrite) {
  std::shared_ptr<::arrow::io::BufferOutputStream> sink = CreateOutputStream();
  auto write_props = WriterProperties::Builder().build();
  std::shared_ptr<::arrow::Array> array =
      ArrayFromJSON(GetParam().write_schema, GetParam().write_data);

  ASSERT_OK_NO_THROW(
      WriteTable(**Table::FromRecordBatches({::arrow::RecordBatch::Make(
                     ::arrow::schema({::arrow::field("col", array->type())}),
                     array->length(), {array})}),
                 ::arrow::default_memory_pool(), sink, /*chunk_size=*/100, write_props,
                 ArrowWriterProperties::Builder().store_schema()->build()));
  std::shared_ptr<::arrow::Buffer> buffer;
  ASSERT_OK_AND_ASSIGN(buffer, sink->Finish());

  std::unique_ptr<FileReader> reader;
  FileReaderBuilder builder;
  ASSERT_OK_NO_THROW(builder.Open(std::make_shared<BufferReader>(buffer)));
  ASSERT_OK(builder.properties(default_arrow_reader_properties())->Build(&reader));
  std::shared_ptr<::arrow::Table> read_table;
  ASSERT_OK_NO_THROW(reader->ReadTable(GetParam().indices_to_read, &read_table));

  std::shared_ptr<::arrow::Array> expected =
      ArrayFromJSON(GetParam().expected_schema, GetParam().read_data);
  AssertArraysEqual(*read_table->column(0)->chunk(0), *expected, /*verbose=*/true);
}

std::vector<NestedFilterTestCase> GenerateListFilterTestCases() {
  auto struct_type = ::arrow::struct_(
      {::arrow::field("a", ::arrow::int64()), ::arrow::field("b", ::arrow::int64())});

  constexpr auto kWriteData = R"([[{"a": 1, "b": 2}]])";
  constexpr auto kReadData = R"([[{"a": 1}]])";

  std::vector<NestedFilterTestCase> cases;
  auto first_selected_type = ::arrow::struct_({struct_type->field(0)});
  cases.push_back({::arrow::list(struct_type),
                   /*indices=*/{0}, ::arrow::list(first_selected_type), kWriteData,
                   kReadData});
  cases.push_back({::arrow::large_list(struct_type),
                   /*indices=*/{0}, ::arrow::large_list(first_selected_type), kWriteData,
                   kReadData});
  cases.push_back({::arrow::fixed_size_list(struct_type, /*list_size=*/1),
                   /*indices=*/{0},
                   ::arrow::fixed_size_list(first_selected_type, /*list_size=*/1),
                   kWriteData, kReadData});
  return cases;
}

INSTANTIATE_TEST_SUITE_P(ListFilteredReads, TestNestedSchemaFilteredReader,
                         ::testing::ValuesIn(GenerateListFilterTestCases()));

std::vector<NestedFilterTestCase> GenerateNestedStructFilteredTestCases() {
  using ::arrow::field;
  using ::arrow::struct_;
  auto struct_type = struct_(
      {field("t1", struct_({field("a", ::arrow::int64()), field("b", ::arrow::int64())})),
       field("t2", ::arrow::int64())});

  constexpr auto kWriteData = R"([{"t1": {"a": 1, "b":2}, "t2": 3}])";

  std::vector<NestedFilterTestCase> cases;
  auto selected_type = ::arrow::struct_(
      {field("t1", struct_({field("a", ::arrow::int64())})), struct_type->field(1)});
  cases.push_back({struct_type,
                   /*indices=*/{0, 2}, selected_type, kWriteData,
                   /*expected=*/R"([{"t1": {"a": 1}, "t2": 3}])"});
  selected_type = ::arrow::struct_(
      {field("t1", struct_({field("b", ::arrow::int64())})), struct_type->field(1)});

  cases.push_back({struct_type,
                   /*indices=*/{1, 2}, selected_type, kWriteData,
                   /*expected=*/R"([{"t1": {"b": 2}, "t2": 3}])"});

  return cases;
}

INSTANTIATE_TEST_SUITE_P(StructFilteredReads, TestNestedSchemaFilteredReader,
                         ::testing::ValuesIn(GenerateNestedStructFilteredTestCases()));

std::vector<NestedFilterTestCase> GenerateMapFilteredTestCases() {
  using ::arrow::field;
  using ::arrow::struct_;
  auto map_type = std::static_pointer_cast<::arrow::MapType>(::arrow::map(
      struct_({field("a", ::arrow::int64()), field("b", ::arrow::int64())}),
      struct_({field("c", ::arrow::int64()), field("d", ::arrow::int64())})));

  constexpr auto kWriteData = R"([[[{"a": 0, "b": 1}, {"c": 2, "d": 3}]]])";
  std::vector<NestedFilterTestCase> cases;
  // Remove the value element completely converts to a list of struct.
  cases.push_back(
      {map_type,
       /*indices=*/{0, 1},
       /*selected_type=*/
       ::arrow::list(field("col", struct_({map_type->key_field()}), /*nullable=*/false)),
       kWriteData, /*expected_data=*/R"([[{"key": {"a": 0, "b":1}}]])"});
  // The "col" field name below comes from how naming is done when writing out the
  // array (it is assigned the column name col.

  // Removing the full key converts to a list of struct.
  cases.push_back(
      {map_type,
       /*indices=*/{3},
       /*selected_type=*/
       ::arrow::list(field(
           "col", struct_({field("value", struct_({field("d", ::arrow::int64())}))}),
           /*nullable=*/false)),
       kWriteData, /*expected_data=*/R"([[{"value": {"d": 3}}]])"});
  // Selecting the full key and a value maintains the map
  cases.push_back(
      {map_type, /*indices=*/{0, 1, 2},
       /*selected_type=*/
       ::arrow::map(map_type->key_type(), struct_({field("c", ::arrow::int64())})),
       kWriteData, /*expected=*/R"([[[{"a": 0, "b": 1}, {"c": 2}]]])"});

  // Selecting the partial key (with some part of the value converts to
  // list of structs (because the key might no longer be unique).
  cases.push_back(
      {map_type, /*indices=*/{1, 2, 3},
       /*selected_type=*/
       ::arrow::list(field("col",
                           struct_({field("key", struct_({field("b", ::arrow::int64())}),
                                          /*nullable=*/false),
                                    map_type->item_field()}),
                           /*nullable=*/false)),
       kWriteData, /*expected=*/R"([[{"key":{"b": 1}, "value": {"c": 2, "d": 3}}]])"});

  return cases;
}

INSTANTIATE_TEST_SUITE_P(MapFilteredReads, TestNestedSchemaFilteredReader,
                         ::testing::ValuesIn(GenerateMapFilteredTestCases()));

}  // namespace arrow
}  // namespace parquet
