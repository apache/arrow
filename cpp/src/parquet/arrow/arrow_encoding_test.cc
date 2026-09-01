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

// End-to-end tests for the column encodings, driven through the Arrow reader
// and writer rather than through the encoder classes directly.

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array/array_primitive.h"
#include "arrow/chunked_array.h"
#include "arrow/io/memory.h"
#include "arrow/table.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/config.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/test_util.h"
#include "parquet/types.h"

using arrow::ChunkedArray;
using arrow::Table;
using arrow::internal::checked_cast;
using arrow::io::BufferReader;

namespace parquet {
namespace arrow {
namespace {

// Write `table` with `writer_props` and read it straight back.
void DoRoundtrip(const std::shared_ptr<Table>& table, int64_t row_group_size,
                 std::shared_ptr<Table>* out,
                 const std::shared_ptr<WriterProperties>& writer_properties) {
  auto sink = CreateOutputStream();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), sink,
                                row_group_size, writer_properties));
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

  std::unique_ptr<FileReader> reader;
  FileReaderBuilder builder;
  ASSERT_OK_NO_THROW(builder.Open(std::make_shared<BufferReader>(buffer)));
  ASSERT_OK(builder.Build(&reader));
  ASSERT_OK_AND_ASSIGN(*out, reader->ReadTable());
}

// The PLAIN reference columns of alp_extended.zstd.parquet are zstd-compressed,
// so the whole fixture needs zstd support.
#ifdef ARROW_WITH_ZSTD

// ALP encoding conformance tests, run against `alp_extended.zstd.parquet`, which
// apache/parquet-testing publishes for exactly this purpose.
//
// All eight columns of `alp_extended.zstd.parquet` hold the same 9032 values.
// `float_plain` and `double_plain` are PLAIN-encoded references, so a correctly
// decoded ALP column is bit-identical to its reference and the test needs no
// hardcoded expected values. The three ALP columns per type use vector sizes
// 1024, 4096 and 32, which forces a reader to honour `log_vector_size` from the
// page header instead of assuming the default.
//
// The value distribution deliberately covers the corner cases: three distinct
// NaN bit patterns, +/-Inf, -0.0, subnormals, a full-mantissa value that cannot
// round-trip as a decimal, large magnitudes, vectors that are entirely
// exceptions, a constant vector (bit_width 0), and nulls. See `data/README.md`
// in apache/parquet-testing for the full table.
//
// Comparison is on bit patterns rather than values, so that NaN payloads are
// checked (NaN != NaN under ==) and -0.0 is not accepted in place of 0.0.
class TestArrowReadAlpEncoding : public ::testing::Test {
 public:
  static constexpr int64_t kNumRows = 9032;

  // The unsigned integer type holding the bit pattern of a floating point value.
  template <typename T>
  using FloatBits = std::conditional_t<sizeof(T) == 4, uint32_t, uint64_t>;

  template <typename T>
  static FloatBits<T> ToBits(T value) {
    static_assert(sizeof(T) == 4 || sizeof(T) == 8,
                  "only 32- and 64-bit floating point values are covered here");
    FloatBits<T> bits;
    std::memcpy(&bits, &value, sizeof(value));
    return bits;
  }

  // The corner cases a reference column is expected to carry. NaNs are collected
  // as bit patterns rather than counted, because the payload has to survive the
  // round trip, not just the fact that the value is a NaN.
  struct CornerCaseCounts {
    std::set<uint64_t> distinct_nans;
    int64_t infinities = 0;
    int64_t negative_zeros = 0;
    int64_t subnormals = 0;
    int64_t nulls = 0;
  };

  void SetUp() override {
    auto path = test::get_data_file("alp_extended.zstd.parquet");
    auto reader = ParquetFileReader::OpenFile(path, /*memory_map=*/false);
    metadata_ = reader->metadata();
    ASSERT_OK_AND_ASSIGN(
        auto file_reader,
        FileReader::Make(::arrow::default_memory_pool(), std::move(reader)));
    ASSERT_OK_AND_ASSIGN(table_, file_reader->ReadTable());
    ASSERT_OK(table_->ValidateFull());
    ASSERT_EQ(table_->num_rows(), kNumRows);
  }

  // Flatten a chunked column to one bit pattern per row, widening float bits to
  // 64 bits so both types share a comparison path. std::nullopt marks a null,
  // which keeps nulls distinguishable from every representable value.
  template <typename ArrowType>
  std::vector<std::optional<uint64_t>> ColumnBits(const std::string& name) {
    using ArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

    std::vector<std::optional<uint64_t>> bits;
    const auto column = table_->GetColumnByName(name);
    EXPECT_NE(column, nullptr) << "no column named " << name;
    if (column == nullptr) return bits;
    bits.reserve(column->length());

    for (const auto& chunk : column->chunks()) {
      const auto& values = checked_cast<const ArrayType&>(*chunk);
      for (int64_t i = 0; i < values.length(); ++i) {
        if (values.IsNull(i)) {
          bits.push_back(std::nullopt);
          continue;
        }
        bits.push_back(ToBits(values.Value(i)));
      }
    }
    return bits;
  }

  // Assert that `alp_column` decoded to exactly the bits of `plain_column`, and
  // that it really was stored with ALP so the test cannot pass vacuously.
  template <typename ArrowType>
  void AssertMatchesPlainReference(const std::string& alp_column,
                                   const std::string& plain_column) {
    ASSERT_NO_FATAL_FAILURE(AssertColumnUsesAlp(alp_column));

    const auto expected = ColumnBits<ArrowType>(plain_column);
    const auto actual = ColumnBits<ArrowType>(alp_column);
    ASSERT_EQ(expected.size(), static_cast<size_t>(kNumRows));
    ASSERT_EQ(actual.size(), expected.size());

    for (size_t i = 0; i < expected.size(); ++i) {
      ASSERT_EQ(actual[i].has_value(), expected[i].has_value())
          << alp_column << " null-ness differs from " << plain_column << " at row " << i;
      if (expected[i].has_value()) {
        ASSERT_EQ(actual[i].value(), expected[i].value())
            << alp_column << " bits differ from " << plain_column << " at row " << i
            << ": 0x" << std::hex << actual[i].value() << " vs 0x" << expected[i].value();
      }
    }
  }

  // Tally the corner cases present in a column.
  template <typename ArrowType>
  CornerCaseCounts CountCornerCases(const std::string& name) {
    using ArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

    CornerCaseCounts counts;
    const auto column = table_->GetColumnByName(name);
    EXPECT_NE(column, nullptr) << "no column named " << name;
    if (column == nullptr) return counts;

    for (const auto& chunk : column->chunks()) {
      const auto& values = checked_cast<const ArrayType&>(*chunk);
      for (int64_t i = 0; i < values.length(); ++i) {
        if (values.IsNull(i)) {
          ++counts.nulls;
          continue;
        }
        const auto value = values.Value(i);
        if (std::isnan(value)) counts.distinct_nans.insert(ToBits(value));
        if (std::isinf(value)) ++counts.infinities;
        if (value == 0 && std::signbit(value)) ++counts.negative_zeros;
        if (std::fpclassify(value) == FP_SUBNORMAL) ++counts.subnormals;
      }
    }
    return counts;
  }

  static void AssertCornerCases(const CornerCaseCounts& counts) {
    EXPECT_EQ(counts.distinct_nans.size(), 3u)
        << "expected three distinct NaN bit patterns";
    EXPECT_EQ(counts.infinities, 2) << "expected +Inf and -Inf";
    EXPECT_EQ(counts.negative_zeros, 1);
    EXPECT_EQ(counts.subnormals, 1);
    EXPECT_EQ(counts.nulls, 8);
  }

  // Every row group must record ALP for this column.
  void AssertColumnUsesAlp(const std::string& name) {
    const int column_index = metadata_->schema()->ColumnIndex(name);
    ASSERT_GE(column_index, 0) << "no column named " << name;
    for (int rg = 0; rg < metadata_->num_row_groups(); ++rg) {
      // Keep the owners alive: encodings() hands back a reference into the
      // column chunk metadata.
      const auto row_group = metadata_->RowGroup(rg);
      const auto column_chunk = row_group->ColumnChunk(column_index);
      ASSERT_THAT(column_chunk->encodings(), ::testing::Contains(Encoding::ALP))
          << name << " row group " << rg << " was not written with ALP";
    }
  }

 protected:
  std::shared_ptr<Table> table_;
  std::shared_ptr<FileMetaData> metadata_;
};

TEST_F(TestArrowReadAlpEncoding, FloatVectorSize1024) {
  AssertMatchesPlainReference<::arrow::FloatType>("float_alp_1024", "float_plain");
}

TEST_F(TestArrowReadAlpEncoding, FloatVectorSize4096) {
  AssertMatchesPlainReference<::arrow::FloatType>("float_alp_4096", "float_plain");
}

TEST_F(TestArrowReadAlpEncoding, FloatVectorSize32) {
  AssertMatchesPlainReference<::arrow::FloatType>("float_alp_32", "float_plain");
}

TEST_F(TestArrowReadAlpEncoding, DoubleVectorSize1024) {
  AssertMatchesPlainReference<::arrow::DoubleType>("double_alp_1024", "double_plain");
}

TEST_F(TestArrowReadAlpEncoding, DoubleVectorSize4096) {
  AssertMatchesPlainReference<::arrow::DoubleType>("double_alp_4096", "double_plain");
}

TEST_F(TestArrowReadAlpEncoding, DoubleVectorSize32) {
  AssertMatchesPlainReference<::arrow::DoubleType>("double_alp_32", "double_plain");
}

// The reference columns carry the corner cases the ALP columns are checked
// against, so assert they are actually there. Without this, a file whose
// references had been regenerated as ordinary values would make every test
// above pass while checking nothing interesting.
TEST_F(TestArrowReadAlpEncoding, ReferenceColumnsCoverCornerCases) {
  {
    SCOPED_TRACE("double_plain");
    AssertCornerCases(CountCornerCases<::arrow::DoubleType>("double_plain"));
  }
  {
    SCOPED_TRACE("float_plain");
    AssertCornerCases(CountCornerCases<::arrow::FloatType>("float_plain"));
  }
}

#endif  // ARROW_WITH_ZSTD

// ============================================================================
// ALP Encoding File-Level Integration Tests
// ============================================================================

class ParquetAlpEncodingTest : public ::testing::Test {
 public:
  void SetUp() override {}

  void TestAlpRoundTrip(const std::shared_ptr<Table>& table) {
    // Create writer properties with ALP encoding for float/double columns
    auto writer_props = WriterProperties::Builder()
                            .disable_dictionary()
                            ->enable_alp_encoding()
                            ->encoding(Encoding::ALP)
                            ->build();

    std::shared_ptr<Table> result;
    DoRoundtrip(table, table->num_rows(), &result, writer_props);

    ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result));
  }

  void TestAlpWithCompression(const std::shared_ptr<Table>& table,
                              Compression::type compression) {
    auto writer_props = WriterProperties::Builder()
                            .disable_dictionary()
                            ->enable_alp_encoding()
                            ->encoding(Encoding::ALP)
                            ->compression(compression)
                            ->build();

    std::shared_ptr<Table> result;
    DoRoundtrip(table, table->num_rows(), &result, writer_props);

    ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result));
  }
};

TEST_F(ParquetAlpEncodingTest, SimpleFloatTable) {
  auto schema = ::arrow::schema({::arrow::field("floats", ::arrow::float32())});
  auto table = ::arrow::TableFromJSON(
      schema,
      {R"([[1.5], [2.5], [3.5], [4.5], [5.5], [6.5], [7.5], [8.5], [9.5], [10.5]])"});
  TestAlpRoundTrip(table);
}

TEST_F(ParquetAlpEncodingTest, SimpleDoubleTable) {
  auto schema = ::arrow::schema({::arrow::field("doubles", ::arrow::float64())});
  auto table =
      ::arrow::TableFromJSON(schema, {R"([[1.123], [2.234], [3.345], [4.456], [5.567],)"
                                      R"( [6.678], [7.789], [8.890], [9.901]])"});
  TestAlpRoundTrip(table);
}

TEST_F(ParquetAlpEncodingTest, MixedTypesWithFloatDouble) {
  auto schema = ::arrow::schema({::arrow::field("id", ::arrow::int64()),
                                 ::arrow::field("value_f", ::arrow::float32()),
                                 ::arrow::field("value_d", ::arrow::float64()),
                                 ::arrow::field("name", ::arrow::utf8())});
  auto table = ::arrow::TableFromJSON(schema, {R"([[1, 1.5, 1.125, "a"],
                                          [2, 2.5, 2.250, "b"],
                                          [3, 3.5, 3.375, "c"],
                                          [4, 4.5, 4.500, "d"],
                                          [5, 5.5, 5.625, "e"]])"});
  // Use ALP encoding only for float/double columns, default for others
  auto writer_props = WriterProperties::Builder()
                          .disable_dictionary()
                          ->enable_alp_encoding("value_f")
                          ->enable_alp_encoding("value_d")
                          ->encoding("value_f", Encoding::ALP)
                          ->encoding("value_d", Encoding::ALP)
                          ->build();

  std::shared_ptr<Table> result;
  DoRoundtrip(table, table->num_rows(), &result, writer_props);

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result));
}

TEST_F(ParquetAlpEncodingTest, LargeFloatDataset) {
  ::arrow::random::RandomArrayGenerator rag(42);
  auto float_array = rag.Float32(10000, -1000.0f, 1000.0f);

  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::float32())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(float_array)});

  TestAlpRoundTrip(table);
}

TEST_F(ParquetAlpEncodingTest, LargeDoubleDataset) {
  ::arrow::random::RandomArrayGenerator rag(42);
  auto double_array = rag.Float64(10000, -1000.0, 1000.0);

  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::float64())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(double_array)});

  TestAlpRoundTrip(table);
}

TEST_F(ParquetAlpEncodingTest, DecimalLikeValues) {
  // Test values that ALP compresses well (2 decimal places)
  std::vector<double> values(1000);
  for (size_t i = 0; i < values.size(); ++i) {
    values[i] = 100.0 + static_cast<double>(i) * 0.01;
  }

  std::shared_ptr<::arrow::Array> array;
  ::arrow::ArrayFromVector<::arrow::DoubleType>(values, &array);

  auto schema = ::arrow::schema({::arrow::field("decimals", ::arrow::float64())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});

  TestAlpRoundTrip(table);
}

TEST_F(ParquetAlpEncodingTest, SpecialFloatValues) {
  // Test with NaN, Inf, -Inf, -0.0
  auto schema = ::arrow::schema({::arrow::field("specials", ::arrow::float64())});

  // TableFromJSON doesn't support Infinity/NaN literals, so we create the array manually
  std::vector<double> values = {1.0,
                                std::numeric_limits<double>::infinity(),
                                -std::numeric_limits<double>::infinity(),
                                std::numeric_limits<double>::quiet_NaN(),
                                0.0,
                                -0.0,
                                2.5,
                                3.5};

  std::shared_ptr<::arrow::Array> array;
  ::arrow::ArrayFromVector<::arrow::DoubleType>(values, &array);

  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});
  TestAlpRoundTrip(table);
}

TEST_F(ParquetAlpEncodingTest, FloatWithNulls) {
  // Test with null values
  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::float64())});
  auto table = ::arrow::TableFromJSON(
      schema, {R"([[1.5], [null], [3.5], [null], [5.5], [6.5], [null], [8.5]])"});

  TestAlpRoundTrip(table);
}

TEST_F(ParquetAlpEncodingTest, MultipleRowGroups) {
  ::arrow::random::RandomArrayGenerator rag(123);
  auto double_array = rag.Float64(5000, -100.0, 100.0);

  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::float64())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(double_array)});

  // Write with small row group size to create multiple row groups
  auto writer_props = WriterProperties::Builder()
                          .disable_dictionary()
                          ->enable_alp_encoding()
                          ->encoding(Encoding::ALP)
                          ->build();

  std::shared_ptr<Table> result;
  DoRoundtrip(table, /*row_group_size=*/1000, &result, writer_props);

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result));
}

#ifdef ARROW_WITH_ZSTD
TEST_F(ParquetAlpEncodingTest, AlpWithZstdCompression) {
  ::arrow::random::RandomArrayGenerator rag(42);
  auto double_array = rag.Float64(5000, -1000.0, 1000.0);

  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::float64())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(double_array)});

  TestAlpWithCompression(table, Compression::ZSTD);
}
#endif

#ifdef ARROW_WITH_SNAPPY
TEST_F(ParquetAlpEncodingTest, AlpWithSnappyCompression) {
  ::arrow::random::RandomArrayGenerator rag(42);
  auto float_array = rag.Float32(5000, -1000.0f, 1000.0f);

  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::float32())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(float_array)});

  TestAlpWithCompression(table, Compression::SNAPPY);
}
#endif

TEST_F(ParquetAlpEncodingTest, VerifyAlpEncodingUsed) {
  // Verify that ALP encoding is actually being used
  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::float64())});

  std::vector<double> values(1000);
  for (size_t i = 0; i < values.size(); ++i) {
    values[i] = static_cast<double>(i) * 0.123;
  }

  std::shared_ptr<::arrow::Array> array;
  ::arrow::ArrayFromVector<::arrow::DoubleType>(values, &array);
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});

  auto writer_props = WriterProperties::Builder()
                          .disable_dictionary()
                          ->enable_alp_encoding()
                          ->encoding(Encoding::ALP)
                          ->build();

  auto sink = CreateOutputStream();
  ASSERT_OK(WriteTable(*table, ::arrow::default_memory_pool(), sink, table->num_rows(),
                       writer_props));
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

  // Read back and verify encoding in metadata
  auto reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer));
  auto metadata = reader->metadata();

  ASSERT_EQ(metadata->num_row_groups(), 1);
  auto row_group = metadata->RowGroup(0);
  ASSERT_EQ(row_group->num_columns(), 1);

  auto column_chunk = row_group->ColumnChunk(0);
  auto encodings = column_chunk->encodings();

  // Verify ALP is one of the encodings used
  bool has_alp = false;
  for (auto encoding : encodings) {
    if (encoding == Encoding::ALP) {
      has_alp = true;
      break;
    }
  }
  EXPECT_TRUE(has_alp) << "ALP encoding not found in column encodings";
}

// Values whose decimal-scaled form sits at or beyond the bounds of the target
// integer type (int32 for FLOAT, int64 for DOUBLE) cannot be ALP-encoded and
// must travel as exceptions. Encodings.md lists this as an exception
// condition; these tests pin that the file round-trips them exactly.
TEST_F(ParquetAlpEncodingTest, DoubleAtEncodedIntegerBounds) {
  constexpr int64_t kIntMax = std::numeric_limits<int64_t>::max();
  constexpr int64_t kIntMin = std::numeric_limits<int64_t>::lowest();

  std::vector<double> values = {
      0.0, 1.0, -1.0, static_cast<double>(kIntMax), static_cast<double>(kIntMin),
      std::nextafter(static_cast<double>(kIntMax),
                     std::numeric_limits<double>::infinity()),
      std::nextafter(static_cast<double>(kIntMin),
                     -std::numeric_limits<double>::infinity()),
      std::numeric_limits<double>::max(), std::numeric_limits<double>::lowest(),
      // A small decimal alongside them, so the vector still picks a scaling
      // exponent rather than degenerating to all-exceptions.
      1.25, 2.5, 3.75};

  std::shared_ptr<::arrow::Array> array;
  ::arrow::ArrayFromVector<::arrow::DoubleType>(values, &array);

  auto schema = ::arrow::schema({::arrow::field("bounds", ::arrow::float64())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});
  TestAlpRoundTrip(table);
}

TEST_F(ParquetAlpEncodingTest, FloatAtEncodedIntegerBounds) {
  constexpr int32_t kIntMax = std::numeric_limits<int32_t>::max();
  constexpr int32_t kIntMin = std::numeric_limits<int32_t>::lowest();

  std::vector<float> values = {
      0.0f,
      1.0f,
      -1.0f,
      static_cast<float>(kIntMax),
      static_cast<float>(kIntMin),
      std::nextafter(static_cast<float>(kIntMax), std::numeric_limits<float>::infinity()),
      std::nextafter(static_cast<float>(kIntMin),
                     -std::numeric_limits<float>::infinity()),
      std::numeric_limits<float>::max(),
      std::numeric_limits<float>::lowest(),
      1.25f,
      2.5f,
      3.75f};

  std::shared_ptr<::arrow::Array> array;
  ::arrow::ArrayFromVector<::arrow::FloatType>(values, &array);

  auto schema = ::arrow::schema({::arrow::field("bounds", ::arrow::float32())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});
  TestAlpRoundTrip(table);
}

// A column in which every value is an exception: no exponent/factor pair
// encodes anything, so each vector carries num_elements exceptions and the
// page is larger than PLAIN. The data must still round-trip bit-exactly.
// (The 32768-exception boundary, where the count no longer fits in a signed
// 16-bit integer, is covered by arrow/util/alp/alp_test.cc; the writer uses
// the default 1024-element vector size.)
TEST_F(ParquetAlpEncodingTest, AllExceptionsColumn) {
  // NaN is never equal to itself, so no exponent/factor pair can encode it and
  // every value takes the exception path.
  std::vector<double> values(70000, std::numeric_limits<double>::quiet_NaN());

  std::shared_ptr<::arrow::Array> array;
  ::arrow::ArrayFromVector<::arrow::DoubleType>(values, &array);

  auto schema = ::arrow::schema({::arrow::field("all_exceptions", ::arrow::float64())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});

  auto writer_props = WriterProperties::Builder()
                          .disable_dictionary()
                          ->enable_alp_encoding()
                          ->encoding(Encoding::ALP)
                          ->build();

  std::shared_ptr<Table> result;
  DoRoundtrip(table, table->num_rows(), &result, writer_props);

  // AssertTablesEqual compares NaN by value, so check the bits directly.
  ASSERT_EQ(result->num_rows(), table->num_rows());
  auto chunked = result->column(0);
  int64_t seen = 0;
  for (const auto& chunk : chunked->chunks()) {
    const auto& doubles =
        ::arrow::internal::checked_cast<const ::arrow::DoubleArray&>(*chunk);
    for (int64_t i = 0; i < doubles.length(); ++i) {
      ASSERT_FALSE(doubles.IsNull(i));
      ASSERT_TRUE(std::isnan(doubles.Value(i))) << "row " << seen;
      ++seen;
    }
  }
  ASSERT_EQ(seen, table->num_rows());
}

}  // namespace
}  // namespace arrow
}  // namespace parquet
