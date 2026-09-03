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

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "arrow/chunked_array.h"
#include "arrow/io/memory.h"
#include "arrow/table.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
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
using arrow::io::BufferReader;

namespace parquet {
namespace arrow {
namespace {

// Write `table` with `writer_properties` and read it straight back, handing back
// the file's bytes as well so a caller can check what the writer chose.
void DoRoundtrip(const std::shared_ptr<Table>& table, int64_t row_group_size,
                 std::shared_ptr<Table>* out,
                 const std::shared_ptr<WriterProperties>& writer_properties,
                 std::shared_ptr<Buffer>* out_file = nullptr) {
  auto sink = CreateOutputStream();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), sink,
                                row_group_size, writer_properties));
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());
  if (out_file != nullptr) {
    *out_file = buffer;
  }

  std::unique_ptr<FileReader> reader;
  FileReaderBuilder builder;
  ASSERT_OK_NO_THROW(builder.Open(std::make_shared<BufferReader>(buffer)));
  ASSERT_OK(builder.Build(&reader));
  ASSERT_OK_AND_ASSIGN(*out, reader->ReadTable());
}

// Fail unless every column chunk of `file` was written with `encoding`. Without
// this a round trip still passes when the writer quietly chose something else,
// and the test would be checking the default encoding instead.
void AssertAllColumnsUse(const std::shared_ptr<Buffer>& file, Encoding::type encoding) {
  auto metadata =
      ParquetFileReader::Open(std::make_shared<BufferReader>(file))->metadata();
  ASSERT_GT(metadata->num_row_groups(), 0);
  for (int rg = 0; rg < metadata->num_row_groups(); ++rg) {
    auto row_group = metadata->RowGroup(rg);
    for (int col = 0; col < row_group->num_columns(); ++col) {
      EXPECT_THAT(row_group->ColumnChunk(col)->encodings(), ::testing::Contains(encoding))
          << "row group " << rg << " column " << col << " does not use "
          << EncodingToString(encoding);
    }
  }
}

// ============================================================================
// PFOR encoding file-level integration tests
// ============================================================================

class ParquetPforEncodingTest : public ::testing::Test {
 public:
  void TestPforRoundTrip(const std::shared_ptr<Table>& table,
                         int64_t row_group_size = -1) {
    auto writer_props = WriterProperties::Builder()
                            .disable_dictionary()
                            ->enable_pfor_encoding()
                            ->encoding(Encoding::PFOR)
                            ->build();

    std::shared_ptr<Table> result;
    std::shared_ptr<Buffer> file;
    DoRoundtrip(table, row_group_size < 0 ? table->num_rows() : row_group_size, &result,
                writer_props, &file);

    ASSERT_NO_FATAL_FAILURE(AssertAllColumnsUse(file, Encoding::PFOR));
    ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result));
  }

  void TestPforWithCompression(const std::shared_ptr<Table>& table,
                               Compression::type compression) {
    auto writer_props = WriterProperties::Builder()
                            .disable_dictionary()
                            ->enable_pfor_encoding()
                            ->encoding(Encoding::PFOR)
                            ->compression(compression)
                            ->build();

    std::shared_ptr<Table> result;
    std::shared_ptr<Buffer> file;
    DoRoundtrip(table, table->num_rows(), &result, writer_props, &file);

    ASSERT_NO_FATAL_FAILURE(AssertAllColumnsUse(file, Encoding::PFOR));
    ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result));
  }

  // A table of one column named "values" holding `values`.
  template <typename ArrowType, typename C>
  static std::shared_ptr<Table> TableOf(const std::shared_ptr<::arrow::DataType>& type,
                                        const C& values) {
    std::shared_ptr<::arrow::Array> array;
    ::arrow::ArrayFromVector<ArrowType>(values, &array);
    return Table::Make(::arrow::schema({::arrow::field("values", type)}),
                       {std::make_shared<ChunkedArray>(array)});
  }
};

TEST_F(ParquetPforEncodingTest, SimpleInt32Table) {
  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::int32())});
  auto table = ::arrow::TableFromJSON(
      schema, {R"([[1], [2], [3], [4], [5], [6], [7], [8], [9], [10]])"});
  TestPforRoundTrip(table);
}

TEST_F(ParquetPforEncodingTest, SimpleInt64Table) {
  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::int64())});
  auto table = ::arrow::TableFromJSON(
      schema, {R"([[-100], [0], [100], [200], [300], [400], [500], [600], [700]])"});
  TestPforRoundTrip(table);
}

TEST_F(ParquetPforEncodingTest, MixedTypesWithInt32Int64) {
  auto schema = ::arrow::schema({::arrow::field("id", ::arrow::int32()),
                                 ::arrow::field("count", ::arrow::int64()),
                                 ::arrow::field("ratio", ::arrow::float64()),
                                 ::arrow::field("name", ::arrow::utf8())});
  auto table = ::arrow::TableFromJSON(schema, {R"([[1, 1000, 1.5, "a"],
                                          [2, 1001, 2.5, "b"],
                                          [3, 1002, 3.5, "c"],
                                          [4, 1003, 4.5, "d"],
                                          [5, 1004, 5.5, "e"]])"});
  // PFOR takes only the two integer columns; the rest keep their defaults.
  auto writer_props = WriterProperties::Builder()
                          .disable_dictionary()
                          ->enable_pfor_encoding()
                          ->encoding("id", Encoding::PFOR)
                          ->encoding("count", Encoding::PFOR)
                          ->build();

  std::shared_ptr<Table> result;
  DoRoundtrip(table, table->num_rows(), &result, writer_props);

  ASSERT_NO_FATAL_FAILURE(::arrow::AssertTablesEqual(*table, *result));
}

TEST_F(ParquetPforEncodingTest, LargeInt32Dataset) {
  ::arrow::random::RandomArrayGenerator rag(42);
  auto array = rag.Int32(10000, -1000000, 1000000);

  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::int32())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});

  TestPforRoundTrip(table);
}

TEST_F(ParquetPforEncodingTest, LargeInt64Dataset) {
  ::arrow::random::RandomArrayGenerator rag(42);
  auto array = rag.Int64(10000, -1000000000LL, 1000000000LL);

  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::int64())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});

  TestPforRoundTrip(table);
}

// The distribution PFOR is built for: a tight cluster sitting a long way from
// zero, so the frame of reference carries the magnitude and the bit-packed
// payload carries only the spread.
TEST_F(ParquetPforEncodingTest, ClusteredValuesFarFromZero) {
  std::vector<int64_t> values(4000);
  for (size_t i = 0; i < values.size(); ++i) {
    values[i] = 1'000'000'000'000LL + static_cast<int64_t>(i % 37);
  }
  TestPforRoundTrip(TableOf<::arrow::Int64Type>(::arrow::int64(), values));
}

// One vector of a constant, which encodes at bit width 0 and so has an empty
// bit-packed payload.
TEST_F(ParquetPforEncodingTest, ConstantColumn) {
  std::vector<int32_t> values(2048, -7);
  TestPforRoundTrip(TableOf<::arrow::Int32Type>(::arrow::int32(), values));
}

// A cluster with a few values far above it, which is what the exception path
// exists to hold.
TEST_F(ParquetPforEncodingTest, ClusterWithOutliers) {
  std::vector<int32_t> values(3000);
  for (size_t i = 0; i < values.size(); ++i) {
    values[i] = 500 + static_cast<int32_t>(i % 11);
    if (i % 97 == 0) {
      values[i] = 1'000'000 + static_cast<int32_t>(i);
    }
  }
  TestPforRoundTrip(TableOf<::arrow::Int32Type>(::arrow::int32(), values));
}

// The extremes of each type in one vector, where the span between the smallest
// and largest value does not fit in the type and the encoder has to fall back
// to storing full-width values.
TEST_F(ParquetPforEncodingTest, Int32AtTypeBounds) {
  std::vector<int32_t> values = {std::numeric_limits<int32_t>::lowest(),
                                 std::numeric_limits<int32_t>::max(),
                                 0,
                                 -1,
                                 1,
                                 std::numeric_limits<int32_t>::lowest() + 1,
                                 std::numeric_limits<int32_t>::max() - 1};
  TestPforRoundTrip(TableOf<::arrow::Int32Type>(::arrow::int32(), values));
}

TEST_F(ParquetPforEncodingTest, Int64AtTypeBounds) {
  std::vector<int64_t> values = {std::numeric_limits<int64_t>::lowest(),
                                 std::numeric_limits<int64_t>::max(),
                                 0,
                                 -1,
                                 1,
                                 std::numeric_limits<int64_t>::lowest() + 1,
                                 std::numeric_limits<int64_t>::max() - 1};
  TestPforRoundTrip(TableOf<::arrow::Int64Type>(::arrow::int64(), values));
}

TEST_F(ParquetPforEncodingTest, Int32WithNulls) {
  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::int32())});
  auto table = ::arrow::TableFromJSON(
      schema, {R"([[1], [null], [3], [null], [5], [6], [null], [8]])"});
  TestPforRoundTrip(table);
}

// A PFOR page holds no nulls, so an all-null page carries no values at all and
// has to survive the round trip as a header with an empty payload.
TEST_F(ParquetPforEncodingTest, AllNullColumn) {
  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::int64())});
  auto table = ::arrow::TableFromJSON(
      schema, {R"([[null], [null], [null], [null], [null], [null]])"});
  TestPforRoundTrip(table);
}

// Nulls ahead of the first value, which makes the level count and the value
// count differ from the very first batch a reader sees.
TEST_F(ParquetPforEncodingTest, LeadingNulls) {
  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::int32())});
  auto table = ::arrow::TableFromJSON(
      schema, {R"([[null], [null], [null], [7], [8], [9], [null], [11]])"});
  TestPforRoundTrip(table);
}

TEST_F(ParquetPforEncodingTest, MultipleRowGroups) {
  ::arrow::random::RandomArrayGenerator rag(123);
  auto array = rag.Int64(5000, -100, 100);

  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::int64())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});

  TestPforRoundTrip(table, /*row_group_size=*/1000);
}

// Row groups that do not divide the vector size, so most pages end on a partial
// vector.
TEST_F(ParquetPforEncodingTest, RowGroupsEndOnPartialVectors) {
  ::arrow::random::RandomArrayGenerator rag(7);
  auto array = rag.Int32(5000, -50000, 50000);

  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::int32())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});

  TestPforRoundTrip(table, /*row_group_size=*/1500);
}

#ifdef ARROW_WITH_ZSTD
TEST_F(ParquetPforEncodingTest, PforWithZstdCompression) {
  ::arrow::random::RandomArrayGenerator rag(42);
  auto array = rag.Int64(5000, -1000, 1000);

  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::int64())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});

  TestPforWithCompression(table, Compression::ZSTD);
}
#endif

#ifdef ARROW_WITH_SNAPPY
TEST_F(ParquetPforEncodingTest, PforWithSnappyCompression) {
  ::arrow::random::RandomArrayGenerator rag(42);
  auto array = rag.Int32(5000, -1000, 1000);

  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::int32())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});

  TestPforWithCompression(table, Compression::SNAPPY);
}
#endif

TEST_F(ParquetPforEncodingTest, VerifyPforEncodingUsed) {
  std::vector<int64_t> values(1000);
  for (size_t i = 0; i < values.size(); ++i) {
    values[i] = 5'000'000 + static_cast<int64_t>(i);
  }
  auto table = TableOf<::arrow::Int64Type>(::arrow::int64(), values);

  auto writer_props = WriterProperties::Builder()
                          .disable_dictionary()
                          ->enable_pfor_encoding()
                          ->encoding(Encoding::PFOR)
                          ->build();

  auto sink = CreateOutputStream();
  ASSERT_OK(WriteTable(*table, ::arrow::default_memory_pool(), sink, table->num_rows(),
                       writer_props));
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

  auto reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer));
  auto metadata = reader->metadata();

  ASSERT_EQ(metadata->num_row_groups(), 1);
  auto row_group = metadata->RowGroup(0);
  ASSERT_EQ(row_group->num_columns(), 1);

  auto encodings = row_group->ColumnChunk(0)->encodings();
  EXPECT_THAT(encodings, ::testing::Contains(Encoding::PFOR))
      << "PFOR encoding not found in column encodings";
}

// A reader that asks for fewer values than the page holds is served the rest on
// later calls, so batches smaller than a page must reassemble exactly.
TEST_F(ParquetPforEncodingTest, ReadInSmallBatches) {
  ::arrow::random::RandomArrayGenerator rag(99);
  auto array = rag.Int32(4096, -30000, 30000);

  auto schema = ::arrow::schema({::arrow::field("values", ::arrow::int32())});
  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(array)});

  auto writer_props = WriterProperties::Builder()
                          .disable_dictionary()
                          ->enable_pfor_encoding()
                          ->encoding(Encoding::PFOR)
                          ->build();

  auto sink = CreateOutputStream();
  ASSERT_OK(WriteTable(*table, ::arrow::default_memory_pool(), sink, table->num_rows(),
                       writer_props));
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

  std::unique_ptr<FileReader> reader;
  FileReaderBuilder builder;
  ASSERT_OK_NO_THROW(builder.Open(std::make_shared<BufferReader>(buffer)));
  ASSERT_OK(builder.properties(default_arrow_reader_properties())->Build(&reader));
  reader->set_batch_size(97);

  ASSERT_OK_AND_ASSIGN(auto batch_reader, reader->GetRecordBatchReader());
  ASSERT_OK_AND_ASSIGN(auto result, batch_reader->ToTable());

  ASSERT_NO_FATAL_FAILURE(
      ::arrow::AssertTablesEqual(*table, *result, /*same_chunk_layout=*/false));
}

// The delta mode is a per-column writer option, so a column it clearly helps
// must come out smaller with the mode left on than with it turned off, and both
// files must read back the same values.
TEST_F(ParquetPforEncodingTest, DeltaModeCanBeTurnedOffPerColumn) {
  // A long arithmetic run: undifferenced it needs enough bits to cover the whole
  // span, differenced it is a constant and needs none.
  std::vector<int64_t> values(4096);
  for (size_t i = 0; i < values.size(); ++i) {
    values[i] = 3'000'000'000'000LL + static_cast<int64_t>(i) * 7;
  }
  auto table = TableOf<::arrow::Int64Type>(::arrow::int64(), values);

  auto size_of = [&](bool delta_enabled) {
    auto builder = WriterProperties::Builder();
    builder.disable_dictionary()
        ->enable_pfor_encoding()
        ->encoding(Encoding::PFOR)
        ->disable_statistics();
    if (!delta_enabled) {
      builder.disable_pfor_delta_encoding("values");
    }
    std::shared_ptr<Table> result;
    std::shared_ptr<Buffer> file;
    DoRoundtrip(table, table->num_rows(), &result, builder.build(), &file);
    ::arrow::AssertTablesEqual(*table, *result);

    auto metadata =
        ParquetFileReader::Open(std::make_shared<BufferReader>(file))->metadata();
    return metadata->RowGroup(0)->ColumnChunk(0)->total_uncompressed_size();
  };

  const int64_t with_delta = size_of(/*delta_enabled=*/true);
  const int64_t without_delta = size_of(/*delta_enabled=*/false);
  EXPECT_LT(with_delta, without_delta)
      << "disable_pfor_delta_encoding did not reach the encoder: " << with_delta
      << " bytes with the mode on, " << without_delta << " with it off";
}

}  // namespace
}  // namespace arrow
}  // namespace parquet
