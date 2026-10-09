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

#include <bit>
#include <cmath>
#include <cstdint>
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

// Assert that every row group of `column_index` recorded ALP, so a round trip
// cannot pass by silently falling back to another encoding.
void AssertAlpEncodingUsed(const std::shared_ptr<FileMetaData>& metadata,
                           int column_index) {
  ASSERT_GE(column_index, 0);
  for (int rg = 0; rg < metadata->num_row_groups(); ++rg) {
    // Keep the owners alive: encodings() hands back a reference into the column
    // chunk metadata.
    const auto row_group = metadata->RowGroup(rg);
    const auto column_chunk = row_group->ColumnChunk(column_index);
    ASSERT_THAT(column_chunk->encodings(), ::testing::Contains(Encoding::ALP))
        << "column " << column_index << " row group " << rg
        << " was not written with ALP";
  }
}

// Write `table` with `writer_props` and read it straight back. Optionally hands
// back the file metadata.
void WriteAndReadBack(const std::shared_ptr<Table>& table, int64_t row_group_size,
                      std::shared_ptr<Table>* out,
                      const std::shared_ptr<WriterProperties>& writer_properties,
                      std::shared_ptr<FileMetaData>* metadata = nullptr) {
  auto sink = CreateOutputStream();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), sink,
                                row_group_size, writer_properties));
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

  std::unique_ptr<FileReader> reader;
  FileReaderBuilder builder;
  ASSERT_OK_NO_THROW(builder.Open(std::make_shared<BufferReader>(buffer)));
  ASSERT_OK(builder.Build(&reader));
  if (metadata != nullptr) {
    *metadata = reader->parquet_reader()->metadata();
  }
  ASSERT_OK_AND_ASSIGN(*out, reader->ReadTable());
}

}  // namespace

// The PLAIN reference columns of alp_extended.zstd.parquet are zstd-compressed,
// so the whole fixture needs zstd support.
#ifdef ARROW_WITH_ZSTD

// Conformance tests against `alp_extended.zstd.parquet` from parquet-testing.
// The `*_plain` columns are PLAIN-encoded references of the same 9032 values, so
// each ALP column is compared against its reference bit for bit and no values are
// hardcoded. The three ALP columns per type use vector sizes 1024, 4096 and 32,
// so a reader has to take the size from the page header, and the values cover NaN
// payloads, infinities, -0.0, subnormals and nulls (see `data/README.md`).
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
    return std::bit_cast<FloatBits<T>>(value);
  }

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

  // The reference column has to carry the corner cases the ALP columns are
  // compared against; a regenerated fixture without them would make every test
  // above pass while checking nothing. NaNs are collected as bit patterns, which
  // is also what the round-trip comparison checks.
  template <typename ArrowType>
  void AssertReferenceHasCornerCases(const std::string& name) {
    using ArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

    const auto column = table_->GetColumnByName(name);
    ASSERT_NE(column, nullptr) << "no column named " << name;

    std::set<uint64_t> distinct_nans;
    int64_t infinities = 0;
    int64_t negative_zeros = 0;
    int64_t subnormals = 0;
    int64_t nulls = 0;
    for (const auto& chunk : column->chunks()) {
      const auto& values = checked_cast<const ArrayType&>(*chunk);
      for (int64_t i = 0; i < values.length(); ++i) {
        if (values.IsNull(i)) {
          ++nulls;
          continue;
        }
        const auto value = values.Value(i);
        if (std::isnan(value)) distinct_nans.insert(ToBits(value));
        if (std::isinf(value)) ++infinities;
        if (value == 0 && std::signbit(value)) ++negative_zeros;
        if (std::fpclassify(value) == FP_SUBNORMAL) ++subnormals;
      }
    }

    EXPECT_EQ(distinct_nans.size(), 3u) << "expected three distinct NaN bit patterns";
    EXPECT_EQ(infinities, 2) << "expected +Inf and -Inf";
    EXPECT_EQ(negative_zeros, 1);
    EXPECT_EQ(subnormals, 1);
    EXPECT_EQ(nulls, 8);
  }

  void AssertColumnUsesAlp(const std::string& name) {
    const int column_index = metadata_->schema()->ColumnIndex(name);
    ASSERT_GE(column_index, 0) << "no column named " << name;
    ASSERT_NO_FATAL_FAILURE(AssertAlpEncodingUsed(metadata_, column_index));
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
    AssertReferenceHasCornerCases<::arrow::DoubleType>("double_plain");
  }
  {
    SCOPED_TRACE("float_plain");
    AssertReferenceHasCornerCases<::arrow::FloatType>("float_plain");
  }
}

#endif  // ARROW_WITH_ZSTD

// ----------------------------------------------------------------------
// ALP Encoding File-Level Integration Tests

class ParquetAlpEncodingTest : public ::testing::Test {
 public:
  // Round-trip `table` through a file whose only value column is ALP-encoded, and
  // check the encoding really was used. A non-positive `row_group_size` writes one
  // row group.
  void TestAlpRoundTrip(const std::shared_ptr<Table>& table, int64_t row_group_size = 0,
                        std::shared_ptr<Table>* result = nullptr) {
    auto writer_props = WriterProperties::Builder()
                            .disable_dictionary()
                            ->encoding(Encoding::ALP)
                            ->build();
    if (row_group_size <= 0) {
      row_group_size = table->num_rows();
    }

    std::shared_ptr<Table> round_tripped;
    std::shared_ptr<FileMetaData> metadata;
    WriteAndReadBack(table, row_group_size, &round_tripped, writer_props, &metadata);
    ASSERT_NO_FATAL_FAILURE(AssertAlpEncodingUsed(metadata, /*column_index=*/0));
    ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *round_tripped));

    if (result != nullptr) {
      *result = std::move(round_tripped);
    }
  }

  // Round-trip a single-column table holding `values`.
  template <typename ArrowType>
  void TestValuesRoundTrip(const std::vector<typename ArrowType::c_type>& values,
                           int64_t row_group_size = 0,
                           std::shared_ptr<Table>* result = nullptr) {
    std::shared_ptr<::arrow::Array> array;
    ::arrow::ArrayFromVector<ArrowType>(values, &array);
    TestAlpRoundTrip(SingleColumnTable<ArrowType>(array), row_group_size, result);
  }

  // Round-trip a single-column table of random values.
  template <typename ArrowType>
  void TestRandomRoundTrip(int64_t num_values, typename ArrowType::c_type min,
                           typename ArrowType::c_type max, int64_t row_group_size = 0,
                           int32_t seed = 42) {
    ::arrow::random::RandomArrayGenerator rag(seed);
    TestAlpRoundTrip(
        SingleColumnTable<ArrowType>(rag.Numeric<ArrowType>(num_values, min, max)),
        row_group_size);
  }

  template <typename ArrowType>
  static std::shared_ptr<Table> SingleColumnTable(
      const std::shared_ptr<::arrow::Array>& array) {
    auto schema = ::arrow::schema(
        {::arrow::field("values", ::arrow::TypeTraits<ArrowType>::type_singleton())});
    return Table::Make(schema, {std::make_shared<ChunkedArray>(array)});
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
  auto writer_props = WriterProperties::Builder()
                          .disable_dictionary()
                          ->encoding("value_f", Encoding::ALP)
                          ->encoding("value_d", Encoding::ALP)
                          ->build();

  std::shared_ptr<Table> result;
  std::shared_ptr<FileMetaData> metadata;
  WriteAndReadBack(table, table->num_rows(), &result, writer_props, &metadata);

  // Only the float and double columns asked for ALP.
  ASSERT_NO_FATAL_FAILURE(AssertAlpEncodingUsed(metadata, /*column_index=*/1));
  ASSERT_NO_FATAL_FAILURE(AssertAlpEncodingUsed(metadata, /*column_index=*/2));
  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result));
}

TEST_F(ParquetAlpEncodingTest, LargeFloatDataset) {
  TestRandomRoundTrip<::arrow::FloatType>(/*num_values=*/10000, -1000.0f, 1000.0f);
}

TEST_F(ParquetAlpEncodingTest, LargeDoubleDataset) {
  TestRandomRoundTrip<::arrow::DoubleType>(/*num_values=*/10000, -1000.0, 1000.0);
}

TEST_F(ParquetAlpEncodingTest, MultipleRowGroups) {
  // A small row group size splits the data across several row groups.
  TestRandomRoundTrip<::arrow::DoubleType>(/*num_values=*/5000, -100.0, 100.0,
                                           /*row_group_size=*/1000, /*seed=*/123);
}

TEST_F(ParquetAlpEncodingTest, DecimalLikeValues) {
  std::vector<double> values(1000);
  for (size_t i = 0; i < values.size(); ++i) {
    values[i] = 100.0 + static_cast<double>(i) * 0.01;
  }
  TestValuesRoundTrip<::arrow::DoubleType>(values);
}

TEST_F(ParquetAlpEncodingTest, SpecialFloatValues) {
  // TableFromJSON cannot express infinities or NaN.
  const std::vector<double> values = {1.0,
                                      std::numeric_limits<double>::infinity(),
                                      -std::numeric_limits<double>::infinity(),
                                      std::numeric_limits<double>::quiet_NaN(),
                                      0.0,
                                      -0.0,
                                      2.5,
                                      3.5};
  TestValuesRoundTrip<::arrow::DoubleType>(values);
}

TEST_F(ParquetAlpEncodingTest, DoubleWithNulls) {
  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::float64())});
  auto table = ::arrow::TableFromJSON(
      schema, {R"([[1.5], [null], [3.5], [null], [5.5], [6.5], [null], [8.5]])"});
  TestAlpRoundTrip(table);
}

// Values whose decimal-scaled form sits at or beyond the bounds of the target
// integer type (int32 for FLOAT, int64 for DOUBLE) cannot be ALP-encoded and must
// travel as exceptions; Encodings.md lists this as an exception condition. A small
// decimal travels alongside them, so the vector still picks a scaling exponent
// instead of degenerating to all exceptions.
TEST_F(ParquetAlpEncodingTest, DoubleAtEncodedIntegerBounds) {
  constexpr int64_t kIntMax = std::numeric_limits<int64_t>::max();
  constexpr int64_t kIntMin = std::numeric_limits<int64_t>::lowest();

  const std::vector<double> values = {
      0.0,
      1.0,
      -1.0,
      static_cast<double>(kIntMax),
      static_cast<double>(kIntMin),
      std::nextafter(static_cast<double>(kIntMax),
                     std::numeric_limits<double>::infinity()),
      std::nextafter(static_cast<double>(kIntMin),
                     -std::numeric_limits<double>::infinity()),
      std::numeric_limits<double>::max(),
      std::numeric_limits<double>::lowest(),
      1.25,
      2.5,
      3.75};
  TestValuesRoundTrip<::arrow::DoubleType>(values);
}

TEST_F(ParquetAlpEncodingTest, FloatAtEncodedIntegerBounds) {
  constexpr int32_t kIntMax = std::numeric_limits<int32_t>::max();
  constexpr int32_t kIntMin = std::numeric_limits<int32_t>::lowest();

  const std::vector<float> values = {
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
  TestValuesRoundTrip<::arrow::FloatType>(values);
}

// Every value is an exception: no exponent/factor pair encodes anything, so each
// vector carries num_elements exceptions and the page is larger than PLAIN. (The
// 32768-exception count boundary is covered in arrow/util/alp/alp_test.cc; the
// writer here uses 1024-element vectors.)
TEST_F(ParquetAlpEncodingTest, AllExceptionsColumn) {
  // NaN never round-trips, so every value takes the exception path.
  const std::vector<double> values(70000, std::numeric_limits<double>::quiet_NaN());

  std::shared_ptr<Table> result;
  TestValuesRoundTrip<::arrow::DoubleType>(values, /*row_group_size=*/0, &result);

  // Array equality treats NaN as equal, so compare the payloads instead.
  const uint64_t expected_bits = std::bit_cast<uint64_t>(values[0]);
  ASSERT_EQ(result->num_rows(), static_cast<int64_t>(values.size()));
  int64_t seen = 0;
  for (const auto& chunk : result->column(0)->chunks()) {
    const auto& doubles =
        ::arrow::internal::checked_cast<const ::arrow::DoubleArray&>(*chunk);
    for (int64_t i = 0; i < doubles.length(); ++i) {
      ASSERT_FALSE(doubles.IsNull(i));
      ASSERT_EQ(std::bit_cast<uint64_t>(doubles.Value(i)), expected_bits)
          << "row " << seen;
      ++seen;
    }
  }
  ASSERT_EQ(seen, static_cast<int64_t>(values.size()));
}

}  // namespace arrow
}  // namespace parquet
