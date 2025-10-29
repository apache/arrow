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

#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>

#include "arrow/array.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/builder_binary.h"
#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/config.h"
#include "arrow/util/range.h"

#include "parquet/column_reader.h"
#include "parquet/column_scanner.h"
#include "parquet/column_writer.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/geospatial/statistics.h"
#include "parquet/metadata.h"
#include "parquet/page_index.h"
#include "parquet/platform.h"
#include "parquet/printer.h"
#include "parquet/statistics.h"
#include "parquet/test_util.h"
#include "parquet/types.h"

namespace rj = arrow::rapidjson;

using arrow::internal::checked_pointer_cast;
using arrow::internal::Zip;

namespace parquet {
using schema::GroupNode;
using schema::PrimitiveNode;

using ReadableFile = ::arrow::io::ReadableFile;

std::string data_file(const char* file) {
  std::string dir_string(test::get_data_dir());
  std::stringstream ss;
  ss << dir_string << "/" << file;
  return ss.str();
}

std::string alltypes_plain() { return data_file("alltypes_plain.parquet"); }

std::string nation_dict_truncated_data_page() {
  return data_file("nation.dict-malformed.parquet");
}

// LZ4-compressed data files.
// These files come in three flavours:
// - legacy "LZ4" compression type, actually compressed with block LZ4 codec
//   (as emitted by some earlier versions of parquet-cpp)
// - legacy "LZ4" compression type, actually compressed with custom Hadoop LZ4 codec
//   (as emitted by parquet-mr)
// - "LZ4_RAW" compression type (added in Parquet format version 2.9.0)

std::string hadoop_lz4_compressed() { return data_file("hadoop_lz4_compressed.parquet"); }

std::string hadoop_lz4_compressed_larger() {
  return data_file("hadoop_lz4_compressed_larger.parquet");
}

std::string non_hadoop_lz4_compressed() {
  return data_file("non_hadoop_lz4_compressed.parquet");
}

std::string lz4_raw_compressed() { return data_file("lz4_raw_compressed.parquet"); }

std::string lz4_raw_compressed_larger() {
  return data_file("lz4_raw_compressed_larger.parquet");
}

std::string overflow_i16_page_ordinal() {
  return data_file("overflow_i16_page_cnt.parquet");
}

std::string data_page_v1_corrupt_checksum() {
  return data_file("datapage_v1-corrupt-checksum.parquet");
}

std::string data_page_v1_uncompressed_checksum() {
  return data_file("datapage_v1-uncompressed-checksum.parquet");
}

std::string data_page_v1_snappy_checksum() {
  return data_file("datapage_v1-snappy-compressed-checksum.parquet");
}

std::string plain_dict_uncompressed_checksum() {
  return data_file("plain-dict-uncompressed-checksum.parquet");
}

std::string rle_dict_snappy_checksum() {
  return data_file("rle-dict-snappy-checksum.parquet");
}

std::string rle_dict_uncompressed_corrupt_checksum() {
  return data_file("rle-dict-uncompressed-corrupt-checksum.parquet");
}

std::string concatenated_gzip_members() {
  return data_file("concatenated_gzip_members.parquet");
}

std::string byte_stream_split() { return data_file("byte_stream_split.zstd.parquet"); }

std::string byte_stream_split_extended() {
  return data_file("byte_stream_split_extended.gzip.parquet");
}

template <typename DType, typename ValueType = typename DType::c_type>
std::vector<ValueType> ReadColumnValues(ParquetFileReader* file_reader, int row_group,
                                        int column, int64_t expected_values_read) {
  auto column_reader = checked_pointer_cast<TypedColumnReader<DType>>(
      file_reader->RowGroup(row_group)->Column(column));
  std::vector<ValueType> values(expected_values_read);
  int64_t values_read;
  auto levels_read = column_reader->ReadBatch(expected_values_read, nullptr, nullptr,
                                              values.data(), &values_read);
  EXPECT_EQ(expected_values_read, levels_read);
  EXPECT_EQ(expected_values_read, values_read);
  return values;
}

template <typename ValueType>
void AssertColumnValuesEqual(const ColumnDescriptor* descr,
                             const std::vector<ValueType>& left_values,
                             const std::vector<ValueType>& right_values) {
  if constexpr (std::is_same_v<ValueType, FLBA>) {
    // operator== for FLBA in test_util.h is unusable (it hard-codes length to 12)
    const auto length = descr->type_length();
    for (const auto& [left, right] : Zip(left_values, right_values)) {
      std::string_view left_view(reinterpret_cast<const char*>(left.ptr), length);
      std::string_view right_view(reinterpret_cast<const char*>(right.ptr), length);
      ASSERT_EQ(left_view, right_view);
    }
  } else {
    ASSERT_EQ(left_values, right_values);
  }
}

// TODO: Assert on definition and repetition levels
template <typename DType, typename ValueType = typename DType::c_type>
void AssertColumnValues(std::shared_ptr<TypedColumnReader<DType>> col, int64_t batch_size,
                        int64_t expected_levels_read,
                        const std::vector<ValueType>& expected_values,
                        int64_t expected_values_read) {
  std::vector<ValueType> values(batch_size);
  int64_t values_read;

  auto levels_read =
      col->ReadBatch(batch_size, nullptr, nullptr, values.data(), &values_read);
  ASSERT_EQ(expected_levels_read, levels_read);
  ASSERT_EQ(expected_values_read, values_read);
  AssertColumnValuesEqual(col->descr(), expected_values, values);
}

template <typename DType, typename ValueType = typename DType::c_type>
void AssertColumnValuesEqual(std::shared_ptr<TypedColumnReader<DType>> left_col,
                             std::shared_ptr<TypedColumnReader<DType>> right_col,
                             int64_t batch_size, int64_t expected_levels_read,
                             int64_t expected_values_read) {
  std::vector<ValueType> left_values(batch_size);
  std::vector<ValueType> right_values(batch_size);
  int64_t values_read, levels_read;

  levels_read =
      left_col->ReadBatch(batch_size, nullptr, nullptr, left_values.data(), &values_read);
  ASSERT_EQ(expected_levels_read, levels_read);
  ASSERT_EQ(expected_values_read, values_read);

  levels_read = right_col->ReadBatch(batch_size, nullptr, nullptr, right_values.data(),
                                     &values_read);
  ASSERT_EQ(expected_levels_read, levels_read);
  ASSERT_EQ(expected_values_read, values_read);

  AssertColumnValuesEqual(left_col->descr(), left_values, right_values);
}

template <typename DType, typename ValueType = typename DType::c_type>
void AssertColumnValuesEqual(ParquetFileReader* file_reader, const std::string& left_col,
                             const std::string& right_col, int64_t num_rows,
                             int row_group = 0) {
  ARROW_SCOPED_TRACE("left_col = '", left_col, "', right_col = '", right_col, "'");

  auto left_col_index = file_reader->metadata()->schema()->ColumnIndex(left_col);
  auto right_col_index = file_reader->metadata()->schema()->ColumnIndex(right_col);
  auto row_group_reader = file_reader->RowGroup(row_group);
  auto left_reader = checked_pointer_cast<TypedColumnReader<DType>>(
      row_group_reader->Column(left_col_index));
  auto right_reader = checked_pointer_cast<TypedColumnReader<DType>>(
      row_group_reader->Column(right_col_index));
  AssertColumnValuesEqual(left_reader, right_reader, num_rows, num_rows, num_rows);
}

void CheckRowGroupMetadata(const RowGroupMetaData* rg_metadata,
                           bool allow_uncompressed_mismatch = false) {
  const int64_t total_byte_size = rg_metadata->total_byte_size();
  const int64_t total_compressed_size = rg_metadata->total_compressed_size();

  ASSERT_GE(total_byte_size, 0);
  ASSERT_GE(total_compressed_size, 0);

  int64_t total_column_byte_size = 0;
  int64_t total_column_compressed_size = 0;
  for (int i = 0; i < rg_metadata->num_columns(); ++i) {
    total_column_byte_size += rg_metadata->ColumnChunk(i)->total_uncompressed_size();
    total_column_compressed_size += rg_metadata->ColumnChunk(i)->total_compressed_size();
  }

  if (!allow_uncompressed_mismatch) {
    ASSERT_EQ(total_byte_size, total_column_byte_size);
  }
  if (total_compressed_size != 0) {
    ASSERT_EQ(total_compressed_size, total_column_compressed_size);
  }
}

class TestBooleanRLE : public ::testing::Test {
 public:
  void SetUp() {
    reader_ = ParquetFileReader::OpenFile(data_file("rle_boolean_encoding.parquet"));
  }

  void TearDown() {}

 protected:
  std::unique_ptr<ParquetFileReader> reader_;
};

TEST_F(TestBooleanRLE, TestBooleanScanner) {
#ifndef ARROW_WITH_ZLIB
  GTEST_SKIP() << "Test requires Zlib compression";
#endif
  int nvalues = 68;
  int validation_values = 16;

  auto group = reader_->RowGroup(0);

  // column 0, id
  auto scanner = std::make_shared<BoolScanner>(group->Column(0));

  bool val = false;
  bool is_null = false;

  // For this file, 3rd and 16th index value is null
  std::vector<bool> expected_null = {false, false, true,  false, false, false,
                                     false, false, false, false, false, false,
                                     false, false, false, true};
  std::vector<bool> expected_value = {true,  false, false, true, true,  false,
                                      false, true,  true,  true, false, false,
                                      true,  true,  false, false};

  // Assert sizes are same
  ASSERT_EQ(validation_values, expected_null.size());
  ASSERT_EQ(validation_values, expected_value.size());

  for (int i = 0; i < validation_values; i++) {
    ASSERT_TRUE(scanner->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));

    ASSERT_EQ(expected_null[i], is_null);

    // Only validate val if not null
    if (!is_null) {
      ASSERT_EQ(expected_value[i], val);
    }
  }

  // Loop through rest of the values to assert data exists
  for (int i = validation_values; i < nvalues; i++) {
    ASSERT_TRUE(scanner->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
  }

  // Attempt to read past end of column
  ASSERT_FALSE(scanner->HasNext());
  ASSERT_FALSE(scanner->NextValue(&val, &is_null));
}

TEST_F(TestBooleanRLE, TestBatchRead) {
#ifndef ARROW_WITH_ZLIB
  GTEST_SKIP() << "Test requires Zlib compression";
#endif
  int nvalues = 68;
  int num_row_groups = 1;
  int metadata_size = 111;

  auto group = reader_->RowGroup(0);

  // column 0, id
  auto col = std::dynamic_pointer_cast<BoolReader>(group->Column(0));

  // This file only has 68 rows
  ASSERT_EQ(nvalues, reader_->metadata()->num_rows());
  // This file only has 1 row group
  ASSERT_EQ(num_row_groups, reader_->metadata()->num_row_groups());
  // Size of the metadata is 111 bytes
  ASSERT_EQ(metadata_size, reader_->metadata()->size());
  // This row group must have 68 rows
  ASSERT_EQ(nvalues, group->metadata()->num_rows());

  // Check if the column is encoded with RLE
  auto col_chunk = group->metadata()->ColumnChunk(0);
  ASSERT_TRUE(std::find(col_chunk->encodings().begin(), col_chunk->encodings().end(),
                        Encoding::RLE) != col_chunk->encodings().end());

  // Assert column has values to be read
  ASSERT_TRUE(col->HasNext());
  int64_t curr_batch_read = 0;

  const int16_t batch_size = 17;
  const int16_t num_nulls = 2;
  int16_t def_levels[batch_size];
  int16_t rep_levels[batch_size];
  bool values[batch_size];
  std::fill_n(values, batch_size, false);

  auto levels_read =
      col->ReadBatch(batch_size, def_levels, rep_levels, values, &curr_batch_read);
  ASSERT_EQ(batch_size, levels_read);

  // Since two value's are null value, expect batches read to be num_nulls less than
  // indicated batch_size
  ASSERT_EQ(batch_size - num_nulls, curr_batch_read);

  // 3rd index is null value
  ASSERT_THAT(def_levels,
              testing::ElementsAre(1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1));

  // Validate inserted data is as expected
  ASSERT_THAT(values,
              testing::ElementsAre(1, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0));

  // Loop through rest of the values and assert batch_size read
  for (int i = batch_size; i < nvalues; i = i + batch_size) {
    levels_read =
        col->ReadBatch(batch_size, def_levels, rep_levels, values, &curr_batch_read);
    ASSERT_EQ(batch_size, levels_read);
  }

  // Now read past the end of the file
  ASSERT_FALSE(col->HasNext());
}

class TestTextDeltaLengthByteArray : public ::testing::Test {
 public:
  void SetUp() {
    reader_ = ParquetFileReader::OpenFile(data_file("delta_length_byte_array.parquet"));
  }

  void TearDown() {}

 protected:
  std::unique_ptr<ParquetFileReader> reader_;
};

TEST_F(TestTextDeltaLengthByteArray, TestTextScanner) {
#ifndef ARROW_WITH_ZSTD
  GTEST_SKIP() << "Test requires Zstd compression";
#endif
  auto group = reader_->RowGroup(0);

  // column 0, id
  auto scanner = std::make_shared<ByteArrayScanner>(group->Column(0));
  ByteArray val;
  bool is_null;
  std::string expected_prefix("apple_banana_mango");
  for (int i = 0; i < 1000; ++i) {
    ASSERT_TRUE(scanner->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_FALSE(is_null);
    std::string expected = expected_prefix + std::to_string(i * i);
    ASSERT_TRUE(val.len == expected.length());
    ASSERT_EQ(::std::string_view(reinterpret_cast<const char*>(val.ptr), val.len),
              expected);
  }
  ASSERT_FALSE(scanner->HasNext());
  ASSERT_FALSE(scanner->NextValue(&val, &is_null));
}

TEST_F(TestTextDeltaLengthByteArray, TestBatchRead) {
#ifndef ARROW_WITH_ZSTD
  GTEST_SKIP() << "Test requires Zstd compression";
#endif
  auto group = reader_->RowGroup(0);

  // column 0, id
  auto col = std::dynamic_pointer_cast<ByteArrayReader>(group->Column(0));

  // This file only has 1000 rows
  ASSERT_EQ(1000, reader_->metadata()->num_rows());
  // This file only has 1 row group
  ASSERT_EQ(1, reader_->metadata()->num_row_groups());
  // Size of the metadata is 105 bytes
  ASSERT_EQ(105, reader_->metadata()->size());
  // This row group must have 1000 rows
  ASSERT_EQ(1000, group->metadata()->num_rows());

  // Check if the column is encoded with DELTA_LENGTH_BYTE_ARRAY
  auto col_chunk = group->metadata()->ColumnChunk(0);

  ASSERT_TRUE(std::find(col_chunk->encodings().begin(), col_chunk->encodings().end(),
                        Encoding::DELTA_LENGTH_BYTE_ARRAY) !=
              col_chunk->encodings().end());

  ASSERT_TRUE(col->HasNext());
  int64_t values_read = 0;
  int64_t curr_batch_read;
  std::string expected_prefix("apple_banana_mango");
  while (values_read < 1000) {
    const int16_t batch_size = 25;
    int16_t def_levels[batch_size];
    int16_t rep_levels[batch_size];
    ByteArray values[batch_size];

    auto levels_read =
        col->ReadBatch(batch_size, def_levels, rep_levels, values, &curr_batch_read);
    ASSERT_EQ(batch_size, levels_read);
    ASSERT_EQ(batch_size, curr_batch_read);
    for (int16_t i = 0; i < batch_size; i++) {
      auto expected =
          expected_prefix + std::to_string((i + values_read) * (i + values_read));
      ASSERT_TRUE(values[i].len == expected.length());
      ASSERT_EQ(
          ::std::string_view(reinterpret_cast<const char*>(values[i].ptr), values[i].len),
          expected);
    }
    values_read += curr_batch_read;
  }

  // Now read past the end of the file
  ASSERT_FALSE(col->HasNext());
}

class TestAllTypesPlain : public ::testing::Test {
 public:
  void SetUp() { reader_ = ParquetFileReader::OpenFile(alltypes_plain()); }

  void TearDown() {}

 protected:
  std::unique_ptr<ParquetFileReader> reader_;
};

TEST_F(TestAllTypesPlain, NoopConstructDestruct) {}

TEST_F(TestAllTypesPlain, RowGroupMetaData) {
  auto group = reader_->RowGroup(0);
  CheckRowGroupMetadata(group->metadata());
}

TEST_F(TestAllTypesPlain, TestBatchRead) {
  std::shared_ptr<RowGroupReader> group = reader_->RowGroup(0);

  // column 0, id
  std::shared_ptr<Int32Reader> col =
      std::dynamic_pointer_cast<Int32Reader>(group->Column(0));

  int16_t def_levels[4];
  int16_t rep_levels[4];
  int32_t values[4];

  // This file only has 8 rows
  ASSERT_EQ(8, reader_->metadata()->num_rows());
  // This file only has 1 row group
  ASSERT_EQ(1, reader_->metadata()->num_row_groups());
  // Size of the metadata is 730 bytes
  ASSERT_EQ(730, reader_->metadata()->size());
  // This row group must have 8 rows
  ASSERT_EQ(8, group->metadata()->num_rows());

  ASSERT_TRUE(col->HasNext());
  int64_t values_read;
  auto levels_read = col->ReadBatch(4, def_levels, rep_levels, values, &values_read);
  ASSERT_EQ(4, levels_read);
  ASSERT_EQ(4, values_read);

  // Now read past the end of the file
  ASSERT_TRUE(col->HasNext());
  levels_read = col->ReadBatch(5, def_levels, rep_levels, values, &values_read);
  ASSERT_EQ(4, levels_read);
  ASSERT_EQ(4, values_read);

  ASSERT_FALSE(col->HasNext());
}

TEST_F(TestAllTypesPlain, RowGroupColumnBoundsChecking) {
  // Part of PARQUET-1857
  ASSERT_THROW(reader_->RowGroup(reader_->metadata()->num_row_groups()),
               ParquetException);

  auto row_group = reader_->RowGroup(0);
  ASSERT_THROW(row_group->Column(row_group->metadata()->num_columns()), ParquetException);
  ASSERT_THROW(row_group->GetColumnPageReader(row_group->metadata()->num_columns()),
               ParquetException);
}

TEST_F(TestAllTypesPlain, TestFlatScannerInt32) {
  std::shared_ptr<RowGroupReader> group = reader_->RowGroup(0);

  // column 0, id
  std::shared_ptr<Int32Scanner> scanner(new Int32Scanner(group->Column(0)));
  int32_t val;
  bool is_null;
  for (int i = 0; i < 8; ++i) {
    ASSERT_TRUE(scanner->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_FALSE(is_null);
  }
  ASSERT_FALSE(scanner->HasNext());
  ASSERT_FALSE(scanner->NextValue(&val, &is_null));
}

TEST_F(TestAllTypesPlain, TestSetScannerBatchSize) {
  std::shared_ptr<RowGroupReader> group = reader_->RowGroup(0);

  // column 0, id
  std::shared_ptr<Int32Scanner> scanner(new Int32Scanner(group->Column(0)));

  ASSERT_EQ(128, scanner->batch_size());
  scanner->SetBatchSize(1024);
  ASSERT_EQ(1024, scanner->batch_size());
}

TEST_F(TestAllTypesPlain, DebugPrintWorks) {
  std::stringstream ss;

  std::list<int> columns;
  ParquetFilePrinter printer(reader_.get());
  printer.DebugPrint(ss, columns);

  std::string result = ss.str();
  ASSERT_GT(result.size(), 0);
}

TEST_F(TestAllTypesPlain, ColumnSelection) {
  std::stringstream ss;

  std::list<int> columns;
  columns.push_back(5);
  columns.push_back(0);
  columns.push_back(10);
  ParquetFilePrinter printer(reader_.get());
  printer.DebugPrint(ss, columns);

  std::string result = ss.str();
  ASSERT_GT(result.size(), 0);
}

TEST_F(TestAllTypesPlain, ColumnSelectionOutOfRange) {
  std::stringstream ss;

  std::list<int> columns;
  columns.push_back(100);
  ParquetFilePrinter printer1(reader_.get());
  ASSERT_THROW(printer1.DebugPrint(ss, columns), ParquetException);

  columns.clear();
  columns.push_back(-1);
  ParquetFilePrinter printer2(reader_.get());
  ASSERT_THROW(printer2.DebugPrint(ss, columns), ParquetException);
}

// Tests that read_dense_for_nullable is passed down to the record
// reader. The functionality of read_dense_for_nullable is tested
// elsewhere.
TEST(TestFileReader, RecordReaderReadDenseForNullable) {
  // We test the default which is false, and also test enabling and disabling
  // read_dense_for_nullable.
  std::vector<ReaderProperties> reader_properties(3);
  reader_properties[1].enable_read_dense_for_nullable();
  reader_properties[2].disable_read_dense_for_nullable();
  for (const auto& reader_props : reader_properties) {
    std::unique_ptr<ParquetFileReader> file_reader = ParquetFileReader::OpenFile(
        alltypes_plain(), /* memory_map = */ false, reader_props);
    std::shared_ptr<RowGroupReader> group = file_reader->RowGroup(0);
    std::shared_ptr<internal::RecordReader> col_record_reader = group->RecordReader(0);
    ASSERT_EQ(reader_props.read_dense_for_nullable(),
              col_record_reader->read_dense_for_nullable());
  }
}

// Tests getting a record reader from a row group reader.
TEST(TestFileReader, GetRecordReader) {
  ReaderProperties reader_props;
  std::unique_ptr<ParquetFileReader> file_reader = ParquetFileReader::OpenFile(
      alltypes_plain(), /* memory_map = */ false, reader_props);
  std::shared_ptr<RowGroupReader> group = file_reader->RowGroup(0);

  std::shared_ptr<internal::RecordReader> col_record_reader_ = group->RecordReader(0);

  ASSERT_TRUE(col_record_reader_->HasMoreData());
  auto records_read = col_record_reader_->ReadRecords(4);
  ASSERT_EQ(records_read, 4);
  ASSERT_EQ(4, col_record_reader_->values_written());
  ASSERT_EQ(4, col_record_reader_->levels_position());
  ASSERT_EQ(8, col_record_reader_->levels_written());
}

TEST(TestFileReader, RecordReaderWithExposingDictionary) {
  const int num_rows = 1000;

  // Make schema
  schema::NodeVector fields;
  fields.push_back(PrimitiveNode::Make("field", Repetition::REQUIRED, Type::BYTE_ARRAY,
                                       ConvertedType::NONE));
  auto schema = std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));

  // Write small batches and small data pages
  std::shared_ptr<WriterProperties> writer_props = WriterProperties::Builder()
                                                       .write_batch_size(64)
                                                       ->data_pagesize(128)
                                                       ->enable_dictionary()
                                                       ->build();

  ASSERT_OK_AND_ASSIGN(auto out_file, ::arrow::io::BufferOutputStream::Create());
  std::shared_ptr<ParquetFileWriter> file_writer =
      ParquetFileWriter::Open(out_file, schema, writer_props);

  RowGroupWriter* rg_writer = file_writer->AppendRowGroup();

  // write one column
  ::arrow::random::RandomArrayGenerator rag(0);
  ByteArrayWriter* writer = static_cast<ByteArrayWriter*>(rg_writer->NextColumn());
  std::vector<std::string> raw_unique_data = {"a", "bc", "defg"};
  std::vector<ByteArray> col_typed;
  for (int i = 0; i < num_rows; i++) {
    std::string_view chosed_data = raw_unique_data[i % raw_unique_data.size()];
    col_typed.emplace_back(chosed_data);
  }
  writer->WriteBatch(num_rows, nullptr, nullptr, col_typed.data());
  rg_writer->Close();
  file_writer->Close();

  // Open the reader
  ASSERT_OK_AND_ASSIGN(auto file_buf, out_file->Finish());
  auto in_file = std::make_shared<::arrow::io::BufferReader>(file_buf);

  ReaderProperties reader_props;
  reader_props.enable_buffered_stream();
  reader_props.set_buffer_size(64);
  std::unique_ptr<ParquetFileReader> file_reader =
      ParquetFileReader::Open(in_file, reader_props);

  auto row_group = file_reader->RowGroup(0);
  auto record_reader = std::dynamic_pointer_cast<internal::DictionaryRecordReader>(
      row_group->RecordReaderWithExposeEncoding(0, ExposedEncoding::DICTIONARY));
  ASSERT_NE(record_reader, nullptr);
  ASSERT_TRUE(record_reader->read_dictionary());

  int32_t dict_len = 0;
  auto dict =
      reinterpret_cast<const ByteArray*>(record_reader->ReadDictionary(&dict_len));
  ASSERT_NE(dict, nullptr);
  ASSERT_EQ(dict_len, raw_unique_data.size());
  ASSERT_EQ(record_reader->ReadRecords(num_rows), num_rows);
  std::shared_ptr<::arrow::ChunkedArray> result_array = record_reader->GetResult();
  ASSERT_EQ(result_array->num_chunks(), 1);
  const std::shared_ptr<::arrow::Array> chunk = result_array->chunk(0);
  auto dictionary_array = std::dynamic_pointer_cast<::arrow::DictionaryArray>(chunk);
  const int32_t* indices =
      (std::dynamic_pointer_cast<::arrow::Int32Array>(dictionary_array->indices()))
          ->raw_values();

  // Verify values based on the dictionary from ReadDictionary().
  int64_t indices_read = chunk->length();
  ASSERT_EQ(indices_read, num_rows);
  for (int i = 0; i < indices_read; ++i) {
    ASSERT_LT(indices[i], dict_len);
    ASSERT_EQ(std::string_view(reinterpret_cast<const char* const>(dict[indices[i]].ptr),
                               dict[indices[i]].len),
              col_typed[i]);
  }
}

class TestLocalFile : public ::testing::Test {
 public:
  void SetUp() {
    std::string dir_string(test::get_data_dir());

    std::stringstream ss;
    ss << dir_string << "/"
       << "alltypes_plain.parquet";

    PARQUET_ASSIGN_OR_THROW(handle, ReadableFile::Open(ss.str()));
    fileno = handle->file_descriptor();
  }

  void TearDown() {}

 protected:
  int fileno;
  std::shared_ptr<::arrow::io::ReadableFile> handle;
};

TEST_F(TestLocalFile, OpenWithMetadata) {
  // PARQUET-808
  std::stringstream ss;
  std::shared_ptr<FileMetaData> metadata = ReadMetaData(handle);

  auto reader = ParquetFileReader::Open(handle, default_reader_properties(), metadata);

  // Compare pointers
  ASSERT_EQ(metadata.get(), reader->metadata().get());

  std::list<int> columns;
  ParquetFilePrinter printer(reader.get());
  printer.DebugPrint(ss, columns, true);

  // Make sure OpenFile passes on the external metadata, too
  auto reader2 = ParquetFileReader::OpenFile(alltypes_plain(), false,
                                             default_reader_properties(), metadata);

  // Compare pointers
  ASSERT_EQ(metadata.get(), reader2->metadata().get());
}

class TestCheckDataPageCrc : public ::testing::Test {
 public:
  void OpenExampleFile(const std::string& file_path) {
    file_reader_ = ParquetFileReader::OpenFile(file_path,
                                               /*memory_map=*/false, reader_props_);
    auto metadata_ptr = file_reader_->metadata();
    EXPECT_EQ(1, metadata_ptr->num_row_groups());
    EXPECT_EQ(2, metadata_ptr->num_columns());
    row_group_ = file_reader_->RowGroup(0);

    column_readers_.resize(2);
    column_readers_[0] = row_group_->Column(0);
    column_readers_[1] = row_group_->Column(1);

    page_readers_.resize(2);
    page_readers_[0] = row_group_->GetColumnPageReader(0);
    page_readers_[1] = row_group_->GetColumnPageReader(1);
  }

  template <typename DType>
  void CheckReadBatches(int col_index, int expect_values) {
    ASSERT_GT(column_readers_.size(), col_index);
    auto column_reader =
        std::dynamic_pointer_cast<TypedColumnReader<DType>>(column_readers_[col_index]);
    ASSERT_NE(nullptr, column_reader);
    int64_t total_values = 0;
    std::vector<typename DType::c_type> values(1024);

    while (column_reader->HasNext()) {
      int64_t values_read;
      int64_t levels_read = column_reader->ReadBatch(values.size(), nullptr, nullptr,
                                                     values.data(), &values_read);
      EXPECT_EQ(levels_read, values_read);
      total_values += values_read;
    }
    EXPECT_EQ(expect_values, total_values);
  }

  void CheckCorrectCrc(const std::string& file_path, bool page_checksum_verification) {
    reader_props_.set_page_checksum_verification(page_checksum_verification);
    {
      // Exercise column readers
      OpenExampleFile(file_path);
      CheckReadBatches<Int32Type>(/*col_index=*/0, kDataPageValuesPerColumn);
      CheckReadBatches<Int32Type>(/*col_index=*/1, kDataPageValuesPerColumn);
    }
    {
      // Exercise page readers directly
      OpenExampleFile(file_path);
      for (auto& page_reader : page_readers_) {
        EXPECT_NE(nullptr, page_reader->NextPage());
        EXPECT_NE(nullptr, page_reader->NextPage());
        EXPECT_EQ(nullptr, page_reader->NextPage());
      }
    }
  }

  void CheckCorrectDictCrc(const std::string& file_path,
                           bool page_checksum_verification) {
    reader_props_.set_page_checksum_verification(page_checksum_verification);
    {
      // Exercise column readers
      OpenExampleFile(file_path);
      CheckReadBatches<Int64Type>(/*col_index=*/0, kDictPageValuesPerColumn);
      CheckReadBatches<ByteArrayType>(/*col_index=*/1, kDictPageValuesPerColumn);
    }
    {
      // Exercise page readers directly
      OpenExampleFile(file_path);
      for (auto& page_reader : page_readers_) {
        // read dict page
        EXPECT_NE(nullptr, page_reader->NextPage());
        // read data page
        EXPECT_NE(nullptr, page_reader->NextPage());
      }
    }
  }

  void CheckNextPageCorrupt(PageReader* page_reader) {
    AssertCrcValidationError([&]() { page_reader->NextPage(); });
  }

  void AssertCrcValidationError(std::function<void()> func) {
    EXPECT_THROW_THAT(
        func, ParquetException,
        ::testing::Property(&ParquetException::what,
                            ::testing::HasSubstr("CRC checksum verification failed")));
  }

 protected:
  static constexpr int kDataPageSize = 1024 * 10;
  // Example CRC files have two v1 data pages per column
  static constexpr int kDataPageValuesPerColumn = kDataPageSize * 2 / sizeof(int32_t);
  static constexpr int kDictPageValuesPerColumn = 1000;

  ReaderProperties reader_props_;
  std::unique_ptr<ParquetFileReader> file_reader_;
  std::shared_ptr<RowGroupReader> row_group_;
  std::vector<std::shared_ptr<ColumnReader>> column_readers_;
  std::vector<std::unique_ptr<PageReader>> page_readers_;
};

TEST_F(TestCheckDataPageCrc, CorruptPageV1) {
  // Works when not checking crc
  CheckCorrectCrc(data_page_v1_corrupt_checksum(),
                  /*page_checksum_verification=*/false);
  // Fails when checking crc
  reader_props_.set_page_checksum_verification(true);
  {
    // With column readers
    OpenExampleFile(data_page_v1_corrupt_checksum());

    AssertCrcValidationError([this]() {
      CheckReadBatches<Int32Type>(/*col_index=*/0, kDataPageValuesPerColumn);
    });
    AssertCrcValidationError([this]() {
      CheckReadBatches<Int32Type>(/*col_index=*/1, kDataPageValuesPerColumn);
    });
  }
  {
    // With page readers
    OpenExampleFile(data_page_v1_corrupt_checksum());

    // First column has a corrupt CRC in first page
    CheckNextPageCorrupt(page_readers_[0].get());
    EXPECT_NE(nullptr, page_readers_[0]->NextPage());
    EXPECT_EQ(nullptr, page_readers_[0]->NextPage());

    // Second column has a corrupt CRC in second page
    EXPECT_NE(nullptr, page_readers_[1]->NextPage());
    CheckNextPageCorrupt(page_readers_[1].get());
    EXPECT_EQ(nullptr, page_readers_[1]->NextPage());
  }
}

TEST_F(TestCheckDataPageCrc, UncompressedPageV1) {
  CheckCorrectCrc(data_page_v1_uncompressed_checksum(),
                  /*page_checksum_verification=*/false);
  CheckCorrectCrc(data_page_v1_uncompressed_checksum(),
                  /*page_checksum_verification=*/true);
}

TEST_F(TestCheckDataPageCrc, SnappyPageV1) {
#ifndef ARROW_WITH_SNAPPY
  GTEST_SKIP() << "Test requires Snappy compression";
#endif
  CheckCorrectCrc(data_page_v1_snappy_checksum(),
                  /*page_checksum_verification=*/false);
  CheckCorrectCrc(data_page_v1_snappy_checksum(),
                  /*page_checksum_verification=*/true);
}

TEST_F(TestCheckDataPageCrc, UncompressedDict) {
  CheckCorrectDictCrc(plain_dict_uncompressed_checksum(),
                      /*page_checksum_verification=*/false);
  CheckCorrectDictCrc(plain_dict_uncompressed_checksum(),
                      /*page_checksum_verification=*/true);
}

TEST_F(TestCheckDataPageCrc, SnappyDict) {
#ifndef ARROW_WITH_SNAPPY
  GTEST_SKIP() << "Test requires Snappy compression";
#endif
  CheckCorrectDictCrc(rle_dict_snappy_checksum(),
                      /*page_checksum_verification=*/false);
  CheckCorrectDictCrc(rle_dict_snappy_checksum(),
                      /*page_checksum_verification=*/true);
}

TEST_F(TestCheckDataPageCrc, CorruptDict) {
  // Works when not checking crc
  CheckCorrectDictCrc(rle_dict_uncompressed_corrupt_checksum(),
                      /*page_checksum_verification=*/false);
  // Fails when checking crc
  reader_props_.set_page_checksum_verification(true);
  {
    // With column readers
    OpenExampleFile(rle_dict_uncompressed_corrupt_checksum());

    AssertCrcValidationError([this]() {
      CheckReadBatches<Int64Type>(/*col_index=*/0, kDictPageValuesPerColumn);
    });
    AssertCrcValidationError([this]() {
      CheckReadBatches<ByteArrayType>(/*col_index=*/1, kDictPageValuesPerColumn);
    });
  }
  {
    // With page readers
    OpenExampleFile(rle_dict_uncompressed_corrupt_checksum());

    CheckNextPageCorrupt(page_readers_[0].get());
    EXPECT_NE(nullptr, page_readers_[0]->NextPage());

    CheckNextPageCorrupt(page_readers_[1].get());
    EXPECT_NE(nullptr, page_readers_[1]->NextPage());
  }
}

TEST(TestGzipMembersRead, TwoConcatenatedMembers) {
#ifndef ARROW_WITH_ZLIB
  GTEST_SKIP() << "Test requires Zlib compression";
#endif
  auto file_reader = ParquetFileReader::OpenFile(concatenated_gzip_members(),
                                                 /*memory_map=*/false);
  auto col_reader = std::dynamic_pointer_cast<TypedColumnReader<Int64Type>>(
      file_reader->RowGroup(0)->Column(0));
  int64_t num_values = 0;
  int64_t num_repdef = 0;
  std::vector<int16_t> reps(1024);
  std::vector<int16_t> defs(1024);
  std::vector<int64_t> vals(1024);

  num_repdef =
      col_reader->ReadBatch(1024, defs.data(), reps.data(), vals.data(), &num_values);
  EXPECT_EQ(num_repdef, 513);
  for (int64_t i = 0; i < num_repdef; i++) {
    EXPECT_EQ(i + 1, vals[i]);
  }
}

TEST(TestFileReaderAdHoc, NationDictTruncatedDataPage) {
  // PARQUET-816. Some files generated by older Parquet implementations may
  // contain malformed data page metadata, and we can successfully decode them
  // if we optimistically proceed to decoding, even if there is not enough data
  // available in the stream. Before, we had quite aggressive checking of
  // stream reads, which are not found e.g. in Impala's Parquet implementation
  auto reader = ParquetFileReader::OpenFile(nation_dict_truncated_data_page(), false);
  std::stringstream ss;

  // empty list means print all
  std::list<int> columns;
  ParquetFilePrinter printer1(reader.get());
  printer1.DebugPrint(ss, columns, true);

  reader = ParquetFileReader::OpenFile(nation_dict_truncated_data_page(), true);
  std::stringstream ss2;
  ParquetFilePrinter printer2(reader.get());
  printer2.DebugPrint(ss2, columns, true);

  // The memory-mapped reads runs over the end of the column chunk and succeeds
  // by accident
  ASSERT_EQ(ss2.str(), ss.str());
}

TEST(TestDumpWithLocalFile, DumpOutput) {
#ifndef ARROW_WITH_SNAPPY
  GTEST_SKIP() << "Test requires Snappy compression";
#endif

  std::string header_output = R"###(File Name: nested_lists.snappy.parquet
Version: 1.0
Created By: parquet-mr version 1.8.2 (build c6522788629e590a53eb79874b95f6c3ff11f16c)
Total rows: 3
Number of RowGroups: 1
Number of Real Columns: 2
Number of Columns: 2
Number of Selected Columns: 2
Column 0: a.list.element.list.element.list.element (BYTE_ARRAY / String / UTF8)
Column 1: b (INT32)
--- Row Group: 0 ---
--- Total Bytes: 155 ---
--- Total Compressed Bytes: 0 ---
--- Rows: 3 ---
Column 0
  Values: 18  Statistics Not Set
  Compression: SNAPPY, Encodings: PLAIN_DICTIONARY(DICT_PAGE) PLAIN_DICTIONARY
  Uncompressed Size: 103, Compressed Size: 104
Column 1
  Values: 3, Null Values: 0, Distinct Values: 0
  Max (exact: unknown): 1, Min (exact: unknown): 1
  Compression: SNAPPY, Encodings: PLAIN_DICTIONARY(DICT_PAGE) PLAIN_DICTIONARY
  Uncompressed Size: 52, Compressed Size: 56
)###";
  std::string values_output = R"###(--- Values ---
element                       |b                             |
a                             |1                             |
b                             |1                             |
c                             |1                             |
NULL                          |
d                             |
a                             |
b                             |
c                             |
d                             |
NULL                          |
e                             |
a                             |
b                             |
c                             |
d                             |
e                             |
NULL                          |
f                             |

)###";
  std::string dump_output = R"###(--- Values ---
Column 0
  D:7 R:0 V:a
  D:7 R:3 V:b
  D:7 R:2 V:c
  D:4 R:1 NULL
  D:7 R:2 V:d
  D:7 R:0 V:a
  D:7 R:3 V:b
  D:7 R:2 V:c
  D:7 R:3 V:d
  D:4 R:1 NULL
  D:7 R:2 V:e
  D:7 R:0 V:a
  D:7 R:3 V:b
  D:7 R:2 V:c
  D:7 R:3 V:d
  D:7 R:2 V:e
  D:4 R:1 NULL
  D:7 R:2 V:f
Column 1
  D:0 R:0 V:1
  D:0 R:0 V:1
  D:0 R:0 V:1
)###";

  // empty list means print all
  std::list<int> columns;

  std::stringstream ss_values, ss_dump;
  const char* file = "nested_lists.snappy.parquet";
  auto reader_props = default_reader_properties();
  auto reader = ParquetFileReader::OpenFile(data_file(file), false, reader_props);
  ParquetFilePrinter printer(reader.get());

  printer.DebugPrint(ss_values, columns, true, false, false, file);
  printer.DebugPrint(ss_dump, columns, true, true, false, file);

  ASSERT_EQ(header_output + values_output, ss_values.str());
  ASSERT_EQ(header_output + dump_output, ss_dump.str());
}

class TestJSONWithLocalFile : public ::testing::Test {
 public:
  static std::string ReadFromLocalFile(std::string_view local_file_name) {
    std::stringstream ss;
    // empty list means print all
    std::list<int> columns;

    auto reader =
        ParquetFileReader::OpenFile(data_file(local_file_name.data()),
                                    /*memory_map=*/false, default_reader_properties());
    ParquetFilePrinter printer(reader.get());
    printer.JSONPrint(ss, columns, local_file_name.data());

    return ss.str();
  }
};

TEST_F(TestJSONWithLocalFile, JSONOutputWithStatistics) {
  std::string json_output = R"###({
  "FileName": "nested_lists.snappy.parquet",
  "Version": "1.0",
  "CreatedBy": "parquet-mr version 1.8.2 (build c6522788629e590a53eb79874b95f6c3ff11f16c)",
  "TotalRows": "3",
  "NumberOfRowGroups": "1",
  "NumberOfRealColumns": "2",
  "NumberOfColumns": "2",
  "Columns": [
     { "Id": "0", "Name": "a.list.element.list.element.list.element", "PhysicalType": "BYTE_ARRAY", "ConvertedType": "UTF8", "LogicalType": {"Type": "String"} },
     { "Id": "1", "Name": "b", "PhysicalType": "INT32", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} }
  ],
  "RowGroups": [
     {
       "Id": "0",  "TotalBytes": "155",  "TotalCompressedBytes": "0",  "Rows": "3",
       "ColumnChunks": [
          {"Id": "0", "Values": "18", "StatsSet": "False",
           "Compression": "SNAPPY", "Encodings": "PLAIN_DICTIONARY(DICT_PAGE) PLAIN_DICTIONARY", "UncompressedSize": "103", "CompressedSize": "104" },
          {"Id": "1", "Values": "3", "StatsSet": "True", "Stats": {"NumNulls": "0", "Max": "1", "Min": "1", "IsMaxValueExact": "unknown", "IsMinValueExact": "unknown" },
           "Compression": "SNAPPY", "Encodings": "PLAIN_DICTIONARY(DICT_PAGE) PLAIN_DICTIONARY", "UncompressedSize": "52", "CompressedSize": "56" }
        ]
     }
  ]
}
)###";

  std::string json_content = ReadFromLocalFile("nested_lists.snappy.parquet");
  ASSERT_EQ(json_output, json_content);
}

TEST_F(TestJSONWithLocalFile, JSONOutput) {
  std::string json_output = R"###({
  "FileName": "alltypes_plain.parquet",
  "Version": "1.0",
  "CreatedBy": "impala version 1.3.0-INTERNAL (build 8a48ddb1eff84592b3fc06bc6f51ec120e1fffc9)",
  "TotalRows": "8",
  "NumberOfRowGroups": "1",
  "NumberOfRealColumns": "11",
  "NumberOfColumns": "11",
  "Columns": [
     { "Id": "0", "Name": "id", "PhysicalType": "INT32", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} },
     { "Id": "1", "Name": "bool_col", "PhysicalType": "BOOLEAN", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} },
     { "Id": "2", "Name": "tinyint_col", "PhysicalType": "INT32", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} },
     { "Id": "3", "Name": "smallint_col", "PhysicalType": "INT32", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} },
     { "Id": "4", "Name": "int_col", "PhysicalType": "INT32", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} },
     { "Id": "5", "Name": "bigint_col", "PhysicalType": "INT64", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} },
     { "Id": "6", "Name": "float_col", "PhysicalType": "FLOAT", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} },
     { "Id": "7", "Name": "double_col", "PhysicalType": "DOUBLE", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} },
     { "Id": "8", "Name": "date_string_col", "PhysicalType": "BYTE_ARRAY", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} },
     { "Id": "9", "Name": "string_col", "PhysicalType": "BYTE_ARRAY", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} },
     { "Id": "10", "Name": "timestamp_col", "PhysicalType": "INT96", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} }
  ],
  "RowGroups": [
     {
       "Id": "0",  "TotalBytes": "671",  "TotalCompressedBytes": "0",  "Rows": "8",
       "ColumnChunks": [
          {"Id": "0", "Values": "8", "StatsSet": "False",
           "Compression": "UNCOMPRESSED", "Encodings": "RLE PLAIN_DICTIONARY PLAIN ", "UncompressedSize": "73", "CompressedSize": "73" },
          {"Id": "1", "Values": "8", "StatsSet": "False",
           "Compression": "UNCOMPRESSED", "Encodings": "RLE PLAIN_DICTIONARY PLAIN ", "UncompressedSize": "24", "CompressedSize": "24" },
          {"Id": "2", "Values": "8", "StatsSet": "False",
           "Compression": "UNCOMPRESSED", "Encodings": "RLE PLAIN_DICTIONARY PLAIN ", "UncompressedSize": "47", "CompressedSize": "47" },
          {"Id": "3", "Values": "8", "StatsSet": "False",
           "Compression": "UNCOMPRESSED", "Encodings": "RLE PLAIN_DICTIONARY PLAIN ", "UncompressedSize": "47", "CompressedSize": "47" },
          {"Id": "4", "Values": "8", "StatsSet": "False",
           "Compression": "UNCOMPRESSED", "Encodings": "RLE PLAIN_DICTIONARY PLAIN ", "UncompressedSize": "47", "CompressedSize": "47" },
          {"Id": "5", "Values": "8", "StatsSet": "False",
           "Compression": "UNCOMPRESSED", "Encodings": "RLE PLAIN_DICTIONARY PLAIN ", "UncompressedSize": "55", "CompressedSize": "55" },
          {"Id": "6", "Values": "8", "StatsSet": "False",
           "Compression": "UNCOMPRESSED", "Encodings": "RLE PLAIN_DICTIONARY PLAIN ", "UncompressedSize": "47", "CompressedSize": "47" },
          {"Id": "7", "Values": "8", "StatsSet": "False",
           "Compression": "UNCOMPRESSED", "Encodings": "RLE PLAIN_DICTIONARY PLAIN ", "UncompressedSize": "55", "CompressedSize": "55" },
          {"Id": "8", "Values": "8", "StatsSet": "False",
           "Compression": "UNCOMPRESSED", "Encodings": "RLE PLAIN_DICTIONARY PLAIN ", "UncompressedSize": "88", "CompressedSize": "88" },
          {"Id": "9", "Values": "8", "StatsSet": "False",
           "Compression": "UNCOMPRESSED", "Encodings": "RLE PLAIN_DICTIONARY PLAIN ", "UncompressedSize": "49", "CompressedSize": "49" },
          {"Id": "10", "Values": "8", "StatsSet": "False",
           "Compression": "UNCOMPRESSED", "Encodings": "RLE PLAIN_DICTIONARY PLAIN ", "UncompressedSize": "139", "CompressedSize": "139" }
        ]
     }
  ]
}
)###";

  std::string json_content = ReadFromLocalFile("alltypes_plain.parquet");
  ASSERT_EQ(json_output, json_content);
}

TEST_F(TestJSONWithLocalFile, JSONOutputFLBA) {
  // min-max stats for FLBA contains non-utf8 output, so we don't check
  // the whole json output.
  std::string json_content = ReadFromLocalFile("fixed_length_byte_array.parquet");

  std::string json_contains = R"###({
  "FileName": "fixed_length_byte_array.parquet",
  "Version": "1.0",
  "CreatedBy": "parquet-mr version 1.13.0-SNAPSHOT (build d057b39d93014fe40f5067ee4a33621e65c91552)",
  "TotalRows": "1000",
  "NumberOfRowGroups": "1",
  "NumberOfRealColumns": "1",
  "NumberOfColumns": "1",
  "Columns": [
     { "Id": "0", "Name": "flba_field", "PhysicalType": "FIXED_LEN_BYTE_ARRAY(4)", "ConvertedType": "NONE", "LogicalType": {"Type": "None"} }
  ])###";

  EXPECT_THAT(json_content, testing::HasSubstr(json_contains));
}

TEST_F(TestJSONWithLocalFile, JSONOutputSortColumns) {
  std::string json_content = ReadFromLocalFile("sort_columns.parquet");

  std::string json_contains = R"###("SortColumns": [
         {"column_idx": 0, "descending": 1, "nulls_first": 1},
         {"column_idx": 1, "descending": 0, "nulls_first": 0}
       ])###";
  EXPECT_THAT(json_content, testing::HasSubstr(json_contains));
}

namespace {

::arrow::Status CheckJsonValid(std::string_view json_string) {
  rj::Document json_doc;
  constexpr auto kParseFlags = rj::kParseFullPrecisionFlag | rj::kParseNanAndInfFlag;
  json_doc.Parse<kParseFlags>(json_string.data(), json_string.length());
  if (json_doc.HasParseError()) {
    return ::arrow::Status::Invalid("JSON parse error at offset ",
                                    json_doc.GetErrorOffset(), ": ",
                                    rj::GetParseError_En(json_doc.GetParseError()));
  }
  return ::arrow::Status::OK();
}

}  // namespace

// GH-44101: Test that JSON output is valid JSON
TEST_F(TestJSONWithLocalFile, ValidJsonOutput) {
  std::vector<std::string_view> check_file_lists = {
      "data_index_bloom_encoding_with_length.parquet",
      "data_index_bloom_encoding_stats.parquet",
      "alltypes_tiny_pages_plain.parquet",
      "concatenated_gzip_members.parquet",
      "nulls.snappy.parquet",
      "sort_columns.parquet"};
  for (const auto& file : check_file_lists) {
    std::string json_content = ReadFromLocalFile(file);
    ASSERT_OK(CheckJsonValid(json_content))
        << "Invalid JSON output for file: " << file << ", content:" << json_content;
  }
}

TEST(TestJSONWithMemoryFile, ValidJsonOutput) {
  using ::arrow::internal::checked_cast;
  auto schema = std::static_pointer_cast<GroupNode>(GroupNode::Make(
      "schema", Repetition::REQUIRED,
      schema::NodeVector{PrimitiveNode::Make("string_field", Repetition::REQUIRED,
                                             LogicalType::String(), Type::BYTE_ARRAY),
                         PrimitiveNode::Make("binary_field", Repetition::REQUIRED,
                                             LogicalType::None(), Type::BYTE_ARRAY)}));

  ASSERT_OK_AND_ASSIGN(auto out_file, ::arrow::io::BufferOutputStream::Create());
  auto file_writer = ParquetFileWriter::Open(out_file, schema);
  auto row_group_writer = file_writer->AppendRowGroup();

  // Write string column with valid UTF8 data
  auto string_writer = checked_cast<ByteArrayWriter*>(row_group_writer->NextColumn());
  std::vector<std::string> utf8_strings = {"Hello", "World", "UTF8 测试", "🌟"};
  std::vector<ByteArray> string_values;
  for (const auto& str : utf8_strings) {
    string_values.emplace_back(std::string_view(str));
  }
  string_writer->WriteBatch(string_values.size(), nullptr, nullptr, string_values.data());

  // Write binary column with non-UTF8 data
  auto binary_writer = checked_cast<ByteArrayWriter*>(row_group_writer->NextColumn());
  std::vector<std::vector<uint8_t>> binary_data = {{0x00, 0x01, 0x02, 0x03},
                                                   {0xFF, 0xFE, 0xFD, 0xFC},
                                                   {0x80, 0x81, 0x82, 0x83},
                                                   {0xC0, 0xC1, 0xF5, 0xF6}};
  std::vector<ByteArray> binary_values;
  for (const auto& data : binary_data) {
    binary_values.emplace_back(
        std::string_view(reinterpret_cast<const char*>(data.data()), data.size()));
  }
  binary_writer->WriteBatch(binary_values.size(), nullptr, nullptr, binary_values.data());

  row_group_writer->Close();
  file_writer->Close();

  // Read the file back and print as JSON
  ASSERT_OK_AND_ASSIGN(auto file_buf, out_file->Finish());
  auto reader =
      ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(file_buf));
  ParquetFilePrinter printer(reader.get());

  // Verify the output is valid JSON
  std::stringstream json_output;
  printer.JSONPrint(json_output, {});
  std::string json_content = json_output.str();
  ASSERT_OK(CheckJsonValid(json_content)) << "Invalid JSON output: " << json_content;
}

TEST(TestFileReader, BufferedReadsWithDictionary) {
  const int num_rows = 1000;

  // Make schema
  schema::NodeVector fields;
  fields.push_back(PrimitiveNode::Make("field", Repetition::REQUIRED, Type::DOUBLE,
                                       ConvertedType::NONE));
  auto schema = std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));

  // Write small batches and small data pages
  std::shared_ptr<WriterProperties> writer_props = WriterProperties::Builder()
                                                       .write_batch_size(64)
                                                       ->data_pagesize(128)
                                                       ->enable_dictionary()
                                                       ->build();

  ASSERT_OK_AND_ASSIGN(auto out_file, ::arrow::io::BufferOutputStream::Create());
  std::shared_ptr<ParquetFileWriter> file_writer =
      ParquetFileWriter::Open(out_file, schema, writer_props);

  RowGroupWriter* rg_writer = file_writer->AppendRowGroup();

  // write one column
  ::arrow::random::RandomArrayGenerator rag(0);
  DoubleWriter* writer = static_cast<DoubleWriter*>(rg_writer->NextColumn());
  std::shared_ptr<::arrow::Array> col = rag.Float64(num_rows, 0, 100);
  const auto& col_typed = static_cast<const ::arrow::DoubleArray&>(*col);
  writer->WriteBatch(num_rows, nullptr, nullptr, col_typed.raw_values());
  rg_writer->Close();
  file_writer->Close();

  // Open the reader
  ASSERT_OK_AND_ASSIGN(auto file_buf, out_file->Finish());
  auto in_file = std::make_shared<::arrow::io::BufferReader>(file_buf);

  ReaderProperties reader_props;
  reader_props.enable_buffered_stream();
  reader_props.set_buffer_size(64);
  std::unique_ptr<ParquetFileReader> file_reader =
      ParquetFileReader::Open(in_file, reader_props);

  auto row_group = file_reader->RowGroup(0);
  auto col_reader = std::static_pointer_cast<DoubleReader>(
      row_group->ColumnWithExposeEncoding(0, ExposedEncoding::DICTIONARY));
  EXPECT_EQ(col_reader->GetExposedEncoding(), ExposedEncoding::DICTIONARY);

  auto indices = std::make_unique<int32_t[]>(num_rows);
  const double* dict = nullptr;
  int32_t dict_len = 0;
  for (int row_index = 0; row_index < num_rows; ++row_index) {
    const double* tmp_dict = nullptr;
    int32_t tmp_dict_len = 0;
    int64_t values_read = 0;
    int64_t levels_read = col_reader->ReadBatchWithDictionary(
        /*batch_size=*/1, /*def_levels=*/nullptr, /*rep_levels=*/nullptr,
        indices.get() + row_index, &values_read, &tmp_dict, &tmp_dict_len);

    if (tmp_dict != nullptr) {
      EXPECT_EQ(values_read, 1);
      dict = tmp_dict;
      dict_len = tmp_dict_len;
    } else {
      EXPECT_EQ(values_read, 0);
    }

    ASSERT_EQ(1, levels_read);
    ASSERT_EQ(1, values_read);
  }

  // Check the results
  for (int row_index = 0; row_index < num_rows; ++row_index) {
    EXPECT_LT(indices[row_index], dict_len);
    EXPECT_EQ(dict[indices[row_index]], col_typed.Value(row_index));
  }
}

TEST(TestFileReader, PartiallyDictionaryEncodingNotExposed) {
  const int num_rows = 1000;

  // Make schema
  schema::NodeVector fields;
  fields.push_back(PrimitiveNode::Make("field", Repetition::REQUIRED, Type::DOUBLE,
                                       ConvertedType::NONE));
  auto schema = std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));

  // Write small batches and small data pages. Explicitly set the dictionary page size
  // limit such that the column chunk will not be fully dictionary encoded.
  std::shared_ptr<WriterProperties> writer_props = WriterProperties::Builder()
                                                       .write_batch_size(64)
                                                       ->data_pagesize(128)
                                                       ->enable_dictionary()
                                                       ->dictionary_pagesize_limit(4)
                                                       ->build();

  ASSERT_OK_AND_ASSIGN(auto out_file, ::arrow::io::BufferOutputStream::Create());
  std::shared_ptr<ParquetFileWriter> file_writer =
      ParquetFileWriter::Open(out_file, schema, writer_props);

  RowGroupWriter* rg_writer = file_writer->AppendRowGroup();

  // write one column
  ::arrow::random::RandomArrayGenerator rag(0);
  DoubleWriter* writer = static_cast<DoubleWriter*>(rg_writer->NextColumn());
  std::shared_ptr<::arrow::Array> col = rag.Float64(num_rows, 0, 100);
  const auto& col_typed = static_cast<const ::arrow::DoubleArray&>(*col);
  writer->WriteBatch(num_rows, nullptr, nullptr, col_typed.raw_values());
  rg_writer->Close();
  file_writer->Close();

  // Open the reader
  ASSERT_OK_AND_ASSIGN(auto file_buf, out_file->Finish());
  auto in_file = std::make_shared<::arrow::io::BufferReader>(file_buf);

  ReaderProperties reader_props;
  reader_props.enable_buffered_stream();
  reader_props.set_buffer_size(64);
  std::unique_ptr<ParquetFileReader> file_reader =
      ParquetFileReader::Open(in_file, reader_props);

  auto row_group = file_reader->RowGroup(0);
  auto col_reader = std::static_pointer_cast<DoubleReader>(
      row_group->ColumnWithExposeEncoding(0, ExposedEncoding::DICTIONARY));
  EXPECT_NE(col_reader->GetExposedEncoding(), ExposedEncoding::DICTIONARY);
}

TEST(TestFileReader, BufferedReads) {
  // PARQUET-1636: Buffered reads were broken before introduction of
  // RandomAccessFile::GetStream

  const int num_columns = 10;
  const int num_rows = 1000;

  // Make schema
  schema::NodeVector fields;
  for (int i = 0; i < num_columns; ++i) {
    fields.push_back(PrimitiveNode::Make("field" + std::to_string(i),
                                         Repetition::REQUIRED, Type::DOUBLE,
                                         ConvertedType::NONE));
  }
  auto schema = std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));

  // Write small batches and small data pages
  std::shared_ptr<WriterProperties> writer_props =
      WriterProperties::Builder().write_batch_size(64)->data_pagesize(128)->build();

  ASSERT_OK_AND_ASSIGN(auto out_file, ::arrow::io::BufferOutputStream::Create());
  std::shared_ptr<ParquetFileWriter> file_writer =
      ParquetFileWriter::Open(out_file, schema, writer_props);

  RowGroupWriter* rg_writer = file_writer->AppendRowGroup();

  ::arrow::ArrayVector column_data;
  ::arrow::random::RandomArrayGenerator rag(0);

  // Scratch space for reads
  ::arrow::BufferVector scratch_space;

  // write columns
  for (int col_index = 0; col_index < num_columns; ++col_index) {
    DoubleWriter* writer = static_cast<DoubleWriter*>(rg_writer->NextColumn());
    std::shared_ptr<::arrow::Array> col = rag.Float64(num_rows, 0, 100);
    const auto& col_typed = static_cast<const ::arrow::DoubleArray&>(*col);
    writer->WriteBatch(num_rows, nullptr, nullptr, col_typed.raw_values());
    column_data.push_back(col);

    // We use this later for reading back the columns
    scratch_space.push_back(
        AllocateBuffer(::arrow::default_memory_pool(), num_rows * sizeof(double)));
  }
  rg_writer->Close();
  file_writer->Close();

  // Open the reader
  ASSERT_OK_AND_ASSIGN(auto file_buf, out_file->Finish());
  auto in_file = std::make_shared<::arrow::io::BufferReader>(file_buf);

  ReaderProperties reader_props;
  reader_props.enable_buffered_stream();
  reader_props.set_buffer_size(64);
  std::unique_ptr<ParquetFileReader> file_reader =
      ParquetFileReader::Open(in_file, reader_props);

  auto row_group = file_reader->RowGroup(0);
  std::vector<std::shared_ptr<DoubleReader>> col_readers;
  for (int col_index = 0; col_index < num_columns; ++col_index) {
    col_readers.push_back(
        std::static_pointer_cast<DoubleReader>(row_group->Column(col_index)));
  }

  for (int row_index = 0; row_index < num_rows; ++row_index) {
    for (int col_index = 0; col_index < num_columns; ++col_index) {
      double* out =
          reinterpret_cast<double*>(scratch_space[col_index]->mutable_data()) + row_index;
      int64_t values_read = 0;
      int64_t levels_read =
          col_readers[col_index]->ReadBatch(1, nullptr, nullptr, out, &values_read);

      ASSERT_EQ(1, levels_read);
      ASSERT_EQ(1, values_read);
    }
  }

  // Check the results
  for (int col_index = 0; col_index < num_columns; ++col_index) {
    ASSERT_TRUE(
        scratch_space[col_index]->Equals(*column_data[col_index]->data()->buffers[1]));
  }
}

std::unique_ptr<ParquetFileReader> OpenBuffer(const std::string& contents) {
  auto buffer = ::arrow::Buffer::FromString(contents);
  return ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
}

::arrow::Future<> OpenBufferAsync(const std::string& contents) {
  auto buffer = ::arrow::Buffer::FromString(contents);
  return ::arrow::Future<>(
      ParquetFileReader::OpenAsync(std::make_shared<::arrow::io::BufferReader>(buffer)));
}

TEST(TestFileReader, TestOpenErrors) {
  EXPECT_THROW_THAT(
      []() { OpenBuffer(""); }, ParquetInvalidOrCorruptedFileException,
      ::testing::Property(&ParquetInvalidOrCorruptedFileException::what,
                          ::testing::HasSubstr("Parquet file size is 0 bytes")));
  EXPECT_THROW_THAT(
      []() { OpenBuffer("AAAAPAR0"); }, ParquetInvalidOrCorruptedFileException,
      ::testing::Property(&ParquetInvalidOrCorruptedFileException::what,
                          ::testing::HasSubstr("Parquet magic bytes not found")));
  EXPECT_THROW_THAT(
      []() { OpenBuffer("APAR1"); }, ParquetInvalidOrCorruptedFileException,
      ::testing::Property(
          &ParquetInvalidOrCorruptedFileException::what,
          ::testing::HasSubstr(
              "Parquet file size is 5 bytes, smaller than the minimum file footer")));
  EXPECT_THROW_THAT(
      []() { OpenBuffer("\xFF\xFF\xFF\x0FPAR1"); },
      ParquetInvalidOrCorruptedFileException,
      ::testing::Property(&ParquetInvalidOrCorruptedFileException::what,
                          ::testing::HasSubstr("Parquet file size is 8 bytes, smaller "
                                               "than the size reported by footer's")));
  EXPECT_THROW_THAT(
      []() { OpenBuffer(std::string("\x00\x00\x00\x00PAR1", 8)); }, ParquetException,
      ::testing::Property(
          &ParquetException::what,
          ::testing::HasSubstr("Couldn't deserialize thrift: No more data to read")));

  EXPECT_FINISHES_AND_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Parquet file size is 0 bytes"), OpenBufferAsync(""));
  EXPECT_FINISHES_AND_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Parquet magic bytes not found"),
      OpenBufferAsync("AAAAPAR0"));
  EXPECT_FINISHES_AND_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "Parquet file size is 5 bytes, smaller than the minimum file footer"),
      OpenBufferAsync("APAR1"));
  EXPECT_FINISHES_AND_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "Parquet file size is 8 bytes, smaller than the size reported by footer's"),
      OpenBufferAsync("\xFF\xFF\xFF\x0FPAR1"));
  EXPECT_FINISHES_AND_RAISES_WITH_MESSAGE_THAT(
      IOError, ::testing::HasSubstr("Couldn't deserialize thrift: No more data to read"),
      OpenBufferAsync(std::string("\x00\x00\x00\x00PAR1", 8)));
}

#undef EXPECT_THROW_THAT

#ifdef ARROW_WITH_LZ4
struct TestCodecParam {
  std::string name;
  std::string small_data_file;
  std::string larger_data_file;
};

void PrintTo(const TestCodecParam& p, std::ostream* os) { *os << p.name; }

class TestCodec : public ::testing::TestWithParam<TestCodecParam> {
 protected:
  const std::string& GetSmallDataFile() { return GetParam().small_data_file; }

  const std::string& GetLargerDataFile() { return GetParam().larger_data_file; }
};

TEST_P(TestCodec, SmallFileMetadataAndValues) {
  std::unique_ptr<ParquetFileReader> reader_ =
      ParquetFileReader::OpenFile(GetSmallDataFile());
  std::shared_ptr<RowGroupReader> group = reader_->RowGroup(0);
  const auto rg_metadata = group->metadata();

  // This file only has 4 rows
  ASSERT_EQ(4, reader_->metadata()->num_rows());
  // This file only has 3 columns
  ASSERT_EQ(3, reader_->metadata()->num_columns());
  // This file only has 1 row group
  ASSERT_EQ(1, reader_->metadata()->num_row_groups());

  // This row group must have 4 rows
  ASSERT_EQ(4, rg_metadata->num_rows());

  // Some parquet-cpp versions are susceptible to PARQUET-2008
  const auto& app_ver = reader_->metadata()->writer_version();
  const bool allow_uncompressed_mismatch =
      (app_ver.application_ == "parquet-cpp" && app_ver.version.major == 1 &&
       app_ver.version.minor == 5 && app_ver.version.patch == 1);

  CheckRowGroupMetadata(rg_metadata, allow_uncompressed_mismatch);

  // column 0, c0
  auto col0 = checked_pointer_cast<Int64Reader>(group->Column(0));
  std::vector<int64_t> expected_values = {1593604800, 1593604800, 1593604801, 1593604801};
  AssertColumnValues(col0, 4, 4, expected_values, 4);

  // column 1, c1
  std::vector<ByteArray> expected_byte_arrays = {ByteArray("abc"), ByteArray("def"),
                                                 ByteArray("abc"), ByteArray("def")};
  auto col1 = checked_pointer_cast<ByteArrayReader>(group->Column(1));
  AssertColumnValues(col1, 4, 4, expected_byte_arrays, 4);

  // column 2, v11
  std::vector<double> expected_double_values = {42.0, 7.7, 42.125, 7.7};
  auto col2 = checked_pointer_cast<DoubleReader>(group->Column(2));
  AssertColumnValues(col2, 4, 4, expected_double_values, 4);
}

TEST_P(TestCodec, LargeFileValues) {
  // Test codec with a larger data file such data may have been compressed
  // in several "frames" (ARROW-9177)
  auto file_path = GetParam().larger_data_file;
  if (file_path.empty()) {
    GTEST_SKIP() << "Larger data file not available for this codec";
  }
  auto file = ParquetFileReader::OpenFile(file_path);
  auto group = file->RowGroup(0);

  const int64_t kNumRows = 10000;

  ASSERT_EQ(kNumRows, file->metadata()->num_rows());
  ASSERT_EQ(1, file->metadata()->num_columns());
  ASSERT_EQ(1, file->metadata()->num_row_groups());
  ASSERT_EQ(kNumRows, group->metadata()->num_rows());

  // column 0 ("a")
  auto col = checked_pointer_cast<ByteArrayReader>(group->Column(0));
  std::vector<ByteArray> values(kNumRows);
  int64_t values_read;
  auto levels_read =
      col->ReadBatch(kNumRows, nullptr, nullptr, values.data(), &values_read);
  ASSERT_EQ(kNumRows, levels_read);
  ASSERT_EQ(kNumRows, values_read);
  ASSERT_EQ(values[0], ByteArray("c7ce6bef-d5b0-4863-b199-8ea8c7fb117b"));
  ASSERT_EQ(values[1], ByteArray("e8fb9197-cb9f-4118-b67f-fbfa65f61843"));
  ASSERT_EQ(values[kNumRows - 2], ByteArray("ab52a0cc-c6bb-4d61-8a8f-166dc4b8b13c"));
  ASSERT_EQ(values[kNumRows - 1], ByteArray("85440778-460a-41ac-aa2e-ac3ee41696bf"));
}

std::vector<TestCodecParam> test_codec_params{
    {"LegacyLZ4Hadoop", hadoop_lz4_compressed(), hadoop_lz4_compressed_larger()},
    {"LegacyLZ4NonHadoop", non_hadoop_lz4_compressed(), ""},
    {"LZ4Raw", lz4_raw_compressed(), lz4_raw_compressed_larger()}};

INSTANTIATE_TEST_SUITE_P(Lz4CodecTests, TestCodec, ::testing::ValuesIn(test_codec_params),
                         testing::PrintToStringParamName());
#endif  // ARROW_WITH_LZ4

// Test reading a data file with a ColumnChunk contains more than
// INT16_MAX pages. (GH-15074).
TEST(TestFileReader, TestOverflowInt16PageOrdinal) {
  ReaderProperties reader_props;
  auto file_reader = ParquetFileReader::OpenFile(overflow_i16_page_ordinal(),
                                                 /*memory_map=*/false, reader_props);
  auto metadata_ptr = file_reader->metadata();
  EXPECT_EQ(1, metadata_ptr->num_row_groups());
  EXPECT_EQ(1, metadata_ptr->num_columns());
  auto row_group = file_reader->RowGroup(0);

  {
    auto column_reader =
        std::dynamic_pointer_cast<TypedColumnReader<BooleanType>>(row_group->Column(0));
    EXPECT_NE(nullptr, column_reader);
    constexpr int kBatchLength = 1024;
    std::array<bool, kBatchLength> boolean_values{};
    int64_t total_values = 0;
    int64_t values_read = 0;
    do {
      values_read = 0;
      column_reader->ReadBatch(kBatchLength, nullptr, nullptr, boolean_values.data(),
                               &values_read);
      total_values += values_read;
      for (int i = 0; i < values_read; ++i) {
        EXPECT_FALSE(boolean_values[i]);
      }
    } while (values_read != 0);
    EXPECT_EQ(40000, total_values);
  }
  {
    auto page_reader = row_group->GetColumnPageReader(0);
    int32_t page_ordinal = 0;
    while (page_reader->NextPage() != nullptr) {
      ++page_ordinal;
    }
    EXPECT_EQ(40000, page_ordinal);
  }
}

#ifdef ARROW_WITH_ZSTD
TEST(TestByteStreamSplit, FloatIntegrationFile) {
  auto file_path = byte_stream_split();
  auto file = ParquetFileReader::OpenFile(file_path);

  const int64_t kNumRows = 300;

  ASSERT_EQ(kNumRows, file->metadata()->num_rows());
  ASSERT_EQ(2, file->metadata()->num_columns());
  ASSERT_EQ(1, file->metadata()->num_row_groups());

  // column 0 ("f32")
  {
    auto values =
        ReadColumnValues<FloatType>(file.get(), /*row_group=*/0, /*column=*/0, kNumRows);
    ASSERT_EQ(values[0], 1.7640524f);
    ASSERT_EQ(values[1], 0.4001572f);
    ASSERT_EQ(values[kNumRows - 2], -0.39944902f);
    ASSERT_EQ(values[kNumRows - 1], 0.37005588f);
  }
  // column 1 ("f64")
  {
    auto values =
        ReadColumnValues<DoubleType>(file.get(), /*row_group=*/0, /*column=*/1, kNumRows);
    ASSERT_EQ(values[0], -1.3065268517353166);
    ASSERT_EQ(values[1], 1.658130679618188);
    ASSERT_EQ(values[kNumRows - 2], -0.9301565025243212);
    ASSERT_EQ(values[kNumRows - 1], -0.17858909208732915);
  }
}
#endif  // ARROW_WITH_ZSTD

#ifdef ARROW_WITH_ZLIB
TEST(TestByteStreamSplit, ExtendedIntegrationFile) {
  auto file_path = byte_stream_split_extended();
  auto file = ParquetFileReader::OpenFile(file_path);

  const int64_t kNumRows = 200;

  ASSERT_EQ(kNumRows, file->metadata()->num_rows());
  ASSERT_EQ(14, file->metadata()->num_columns());
  ASSERT_EQ(1, file->metadata()->num_row_groups());

  AssertColumnValuesEqual<FloatType>(file.get(), "float_plain", "float_byte_stream_split",
                                     kNumRows);
  AssertColumnValuesEqual<DoubleType>(file.get(), "double_plain",
                                      "double_byte_stream_split", kNumRows);
  AssertColumnValuesEqual<Int32Type>(file.get(), "int32_plain", "int32_byte_stream_split",
                                     kNumRows);
  AssertColumnValuesEqual<Int64Type>(file.get(), "int64_plain", "int64_byte_stream_split",
                                     kNumRows);
  AssertColumnValuesEqual<FLBAType>(file.get(), "float16_plain",
                                    "float16_byte_stream_split", kNumRows);
  AssertColumnValuesEqual<FLBAType>(file.get(), "flba5_plain", "flba5_byte_stream_split",
                                    kNumRows);
  AssertColumnValuesEqual<FLBAType>(file.get(), "decimal_plain",
                                    "decimal_byte_stream_split", kNumRows);
}
#endif  // ARROW_WITH_ZLIB

struct PageIndexReaderParam {
  std::vector<int32_t> row_group_indices;
  std::vector<int32_t> column_indices;
  PageIndexSelection index_selection;
};

// For valgrind
std::ostream& operator<<(std::ostream& out, const PageIndexReaderParam& params) {
  out << "PageIndexReaderParam{row_group_indices = ";
  for (const auto& i : params.row_group_indices) {
    out << i << ", ";
  }
  out << "column_indices = ";
  for (const auto& i : params.column_indices) {
    out << i << ", ";
  }

  out << "index_selection = " << params.index_selection << "}";

  return out;
}

class ParameterizedPageIndexReaderTest
    : public ::testing::TestWithParam<PageIndexReaderParam> {};

// Test reading a data file with page index.
TEST_P(ParameterizedPageIndexReaderTest, TestReadPageIndex) {
  ReaderProperties properties;
  auto file_reader = ParquetFileReader::OpenFile(data_file("alltypes_tiny_pages.parquet"),
                                                 /*memory_map=*/false, properties);
  auto metadata = file_reader->metadata();
  EXPECT_EQ(1, metadata->num_row_groups());
  EXPECT_EQ(13, metadata->num_columns());

  // Create the page index reader and provide different read hints.
  auto page_index_reader = file_reader->GetPageIndexReader();
  ASSERT_NE(nullptr, page_index_reader);
  const auto params = GetParam();
  const bool call_will_need = !params.row_group_indices.empty();
  if (call_will_need) {
    page_index_reader->WillNeed(params.row_group_indices, params.column_indices,
                                params.index_selection);
  }

  auto row_group_index_reader = page_index_reader->RowGroup(0);
  if (!call_will_need || params.index_selection.offset_index ||
      params.index_selection.column_index) {
    ASSERT_NE(nullptr, row_group_index_reader);
  } else {
    // None of page index is requested.
    ASSERT_EQ(nullptr, row_group_index_reader);
    return;
  }

  auto column_index_requested = [&](int32_t column_id) {
    return !call_will_need ||
           (params.index_selection.column_index &&
            (params.column_indices.empty() ||
             (std::find(params.column_indices.cbegin(), params.column_indices.cend(),
                        column_id) != params.column_indices.cend())));
  };

  auto offset_index_requested = [&](int32_t column_id) {
    return !call_will_need ||
           (params.index_selection.offset_index &&
            (params.column_indices.empty() ||
             (std::find(params.column_indices.cbegin(), params.column_indices.cend(),
                        column_id) != params.column_indices.cend())));
  };

  if (offset_index_requested(0)) {
    // Verify offset index of column 0 and only partial data as it contains 325 pages.
    const size_t num_pages = 325;
    const std::vector<size_t> page_indices = {0, 100, 200, 300};
    const std::vector<PageLocation> page_locations = {
        PageLocation{4, 109, 0}, PageLocation{11480, 133, 2244},
        PageLocation{22980, 133, 4494}, PageLocation{34480, 133, 6744}};

    auto offset_index = row_group_index_reader->GetOffsetIndex(0);
    ASSERT_NE(nullptr, offset_index);

    EXPECT_EQ(num_pages, offset_index->page_locations().size());
    for (size_t i = 0; i < page_indices.size(); ++i) {
      size_t page_id = page_indices.at(i);
      const auto& read_page_location = offset_index->page_locations().at(page_id);
      const auto& expected_page_location = page_locations.at(i);
      EXPECT_EQ(expected_page_location.offset, read_page_location.offset);
      EXPECT_EQ(expected_page_location.compressed_page_size,
                read_page_location.compressed_page_size);
      EXPECT_EQ(expected_page_location.first_row_index,
                read_page_location.first_row_index);
    }
  } else {
    EXPECT_THROW(row_group_index_reader->GetOffsetIndex(0), ParquetException);
  }

  if (column_index_requested(5)) {
    // Verify column index of column 5 and only partial data as it contains 528 pages.
    const size_t num_pages = 528;
    const BoundaryOrder::type boundary_order = BoundaryOrder::Unordered;
    const std::vector<size_t> page_indices = {0, 99, 426, 520};
    const std::vector<bool> null_pages = {false, false, false, false};
    const bool has_null_counts = true;
    const std::vector<int64_t> null_counts = {0, 0, 0, 0};
    const std::vector<int64_t> min_values = {0, 10, 0, 0};
    const std::vector<int64_t> max_values = {90, 90, 80, 70};

    auto column_index = row_group_index_reader->GetColumnIndex(5);
    ASSERT_NE(nullptr, column_index);
    auto typed_column_index = std::dynamic_pointer_cast<Int64ColumnIndex>(column_index);
    ASSERT_NE(nullptr, typed_column_index);

    EXPECT_EQ(num_pages, column_index->null_pages().size());
    EXPECT_EQ(has_null_counts, column_index->has_null_counts());
    EXPECT_EQ(boundary_order, column_index->boundary_order());
    for (size_t i = 0; i < page_indices.size(); ++i) {
      size_t page_id = page_indices.at(i);
      EXPECT_EQ(null_pages.at(i), column_index->null_pages().at(page_id));
      if (has_null_counts) {
        EXPECT_EQ(null_counts.at(i), column_index->null_counts().at(page_id));
      }
      if (!null_pages.at(i)) {
        EXPECT_EQ(min_values.at(i), typed_column_index->min_values().at(page_id));
        EXPECT_EQ(max_values.at(i), typed_column_index->max_values().at(page_id));
      }
    }
  } else {
    EXPECT_THROW(row_group_index_reader->GetColumnIndex(5), ParquetException);
  }

  // Verify null is returned if column index does not exist.
  auto column_index = row_group_index_reader->GetColumnIndex(10);
  EXPECT_EQ(nullptr, column_index);
}

INSTANTIATE_TEST_SUITE_P(
    PageIndexReaderTests, ParameterizedPageIndexReaderTest,
    ::testing::Values(PageIndexReaderParam{{}, {}, {true, true}},
                      PageIndexReaderParam{{}, {}, {true, false}},
                      PageIndexReaderParam{{}, {}, {false, true}},
                      PageIndexReaderParam{{}, {}, {false, false}},
                      PageIndexReaderParam{{0}, {}, {true, true}},
                      PageIndexReaderParam{{0}, {}, {true, false}},
                      PageIndexReaderParam{{0}, {}, {false, true}},
                      PageIndexReaderParam{{0}, {}, {false, false}},
                      PageIndexReaderParam{{0}, {0}, {true, true}},
                      PageIndexReaderParam{{0}, {0}, {true, false}},
                      PageIndexReaderParam{{0}, {0}, {false, true}},
                      PageIndexReaderParam{{0}, {0}, {false, false}},
                      PageIndexReaderParam{{0}, {5}, {true, true}},
                      PageIndexReaderParam{{0}, {5}, {true, false}},
                      PageIndexReaderParam{{0}, {5}, {false, true}},
                      PageIndexReaderParam{{0}, {5}, {false, false}},
                      PageIndexReaderParam{{0}, {0, 5}, {true, true}},
                      PageIndexReaderParam{{0}, {0, 5}, {true, false}},
                      PageIndexReaderParam{{0}, {0, 5}, {false, true}},
                      PageIndexReaderParam{{0}, {0, 5}, {false, false}}));

TEST(PageIndexReaderTest, ReadFileWithoutPageIndex) {
  ReaderProperties properties;
  auto file_reader = ParquetFileReader::OpenFile(data_file("int32_decimal.parquet"),
                                                 /*memory_map=*/false, properties);
  auto metadata = file_reader->metadata();
  EXPECT_EQ(1, metadata->num_row_groups());

  auto page_index_reader = file_reader->GetPageIndexReader();
  ASSERT_NE(nullptr, page_index_reader);
  auto row_group_index_reader = page_index_reader->RowGroup(0);
  ASSERT_EQ(nullptr, row_group_index_reader);
}

class TestGeometryLogicalType : public ::testing::Test {
 public:
  const int kNumRows = 1000;

  void WriteTestData(std::shared_ptr<const LogicalType> type, bool write_arrow) {
    // Make schema
    schema::NodeVector fields;
    fields.push_back(
        PrimitiveNode::Make("g", Repetition::REQUIRED, type, Type::BYTE_ARRAY));
    auto schema = std::static_pointer_cast<GroupNode>(
        GroupNode::Make("schema", Repetition::REQUIRED, fields));

    // Write small batches and small data pages
    auto writer_props_builder = WriterProperties::Builder();
    writer_props_builder.write_batch_size(64);

    std::shared_ptr<WriterProperties> writer_props = writer_props_builder.build();

    ASSERT_OK_AND_ASSIGN(auto out_file, ::arrow::io::BufferOutputStream::Create());
    std::shared_ptr<ParquetFileWriter> file_writer =
        ParquetFileWriter::Open(out_file, schema, writer_props);
    RowGroupWriter* rg_writer = file_writer->AppendRowGroup();

    // write WKB points to columns
    auto* writer =
        ::arrow::internal::checked_cast<ByteArrayWriter*>(rg_writer->NextColumn());
    if (!write_arrow) {
      WriteTestDataUsingWriteBatch(writer);
    } else {
      WriteTestDataUsingWriteArrow(writer);
    }

    rg_writer->Close();
    file_writer->Close();

    ASSERT_OK_AND_ASSIGN(file_buf, out_file->Finish());
  }

  void WriteTestDataUsingWriteBatch(ByteArrayWriter* writer) {
    std::vector<uint8_t> buffer(test::kWkbPointXYSize * kNumRows);
    uint8_t* ptr = buffer.data();
    std::vector<ByteArray> values(kNumRows);
    for (int k = 0; k < kNumRows; k++) {
      std::string item = test::MakeWKBPoint(
          {static_cast<double>(k), static_cast<double>(k + 1)}, false, false);
      std::memcpy(ptr, item.data(), item.size());
      values[k].len = test::kWkbPointXYSize;
      values[k].ptr = ptr;
      ptr += test::kWkbPointXYSize;
    }
    writer->WriteBatch(kNumRows, nullptr, nullptr, values.data());
  }

  void WriteTestDataUsingWriteArrow(ByteArrayWriter* writer) {
    ::arrow::BinaryBuilder builder;
    for (int k = 0; k < kNumRows; k++) {
      std::string item = test::MakeWKBPoint(
          {static_cast<double>(k), static_cast<double>(k + 1)}, false, false);

      ASSERT_OK(builder.Append(item));
    }
    std::shared_ptr<::arrow::BinaryArray> array;
    ASSERT_OK(builder.Finish(&array));

    std::shared_ptr<ArrowWriterProperties> properties =
        ArrowWriterProperties::Builder().build();
    MemoryPool* pool = ::arrow::default_memory_pool();
    auto ctx = std::make_unique<ArrowWriteContext>(pool, properties.get());
    ASSERT_OK(writer->WriteArrow(nullptr, nullptr, kNumRows, *array, ctx.get(),
                                 /*leaf_field_nullable=*/true));
  }

  void TestWriteAndRead(std::shared_ptr<const LogicalType> type, bool write_arrow) {
    ASSERT_NO_FATAL_FAILURE(WriteTestData(type, write_arrow));

    auto in_file = std::make_shared<::arrow::io::BufferReader>(file_buf);

    ReaderProperties reader_props;
    reader_props.enable_buffered_stream();
    reader_props.set_buffer_size(64);
    auto file_reader = ParquetFileReader::Open(in_file, reader_props);

    // Check that the geometry statistics are correctly written and read
    auto metadata = file_reader->metadata();
    ASSERT_TRUE(type->Equals(*metadata->schema()->Column(0)->logical_type()));

    auto page_index_reader = file_reader->GetPageIndexReader();
    int num_row_groups = metadata->num_row_groups();
    int64_t start_index = 0;
    for (int i = 0; i < num_row_groups; i++) {
      auto row_group_metadata = metadata->RowGroup(i);
      auto column_chunk_metadata = row_group_metadata->ColumnChunk(0);
      auto geo_stats = column_chunk_metadata->geo_statistics();
      ASSERT_NO_FATAL_FAILURE(CheckGeoStatistics(type, geo_stats, start_index,
                                                 row_group_metadata->num_rows()));
      start_index += row_group_metadata->num_rows();
    }

    // Check the geometry values
    int64_t total_values_read = 0;
    for (int i = 0; i < num_row_groups; i++) {
      auto row_group = file_reader->RowGroup(i);
      std::shared_ptr<ByteArrayReader> reader =
          std::static_pointer_cast<ByteArrayReader>(row_group->Column(0));
      while (reader->HasNext()) {
        std::vector<ByteArray> out(kNumRows);
        int64_t values_read = 0;
        int64_t levels_read =
            reader->ReadBatch(kNumRows, nullptr, nullptr, out.data(), &values_read);
        ASSERT_GE(levels_read, 1);
        ASSERT_GE(values_read, 1);

        // Check the batch
        for (int64_t i = 0; i < values_read; i++) {
          const ByteArray& value = out[i];
          auto xy = test::GetWKBPointCoordinateXY(value);
          EXPECT_TRUE(xy.has_value());
          auto expected_x = static_cast<double>(i + total_values_read);
          auto expected_y = static_cast<double>(i + 1 + total_values_read);
          EXPECT_EQ(*xy, (std::pair<double, double>(expected_x, expected_y)));
        }

        total_values_read += values_read;
      }
    }
    EXPECT_EQ(kNumRows, total_values_read);
  }

  void CheckGeoStatistics(std::shared_ptr<const LogicalType> type,
                          std::shared_ptr<geospatial::GeoStatistics> geom_stats,
                          int64_t start_index, int64_t num_rows) {
    // We don't yet generate statistics for Geography
    if (type->is_geography()) {
      ASSERT_EQ(geom_stats, nullptr);
      return;
    }

    ASSERT_NE(geom_stats, nullptr);
    // We wrote exactly one geometry type (POINT, which has code 1)
    EXPECT_THAT(*geom_stats->geometry_types(), ::testing::ElementsAre(1));

    double expected_xmin = static_cast<double>(start_index);
    double expected_xmax = expected_xmin + num_rows - 1;
    double expected_ymin = expected_xmin + 1;
    double expected_ymax = expected_xmax + 1;

    EXPECT_EQ(geom_stats->lower_bound()[0], expected_xmin);
    EXPECT_EQ(geom_stats->upper_bound()[0], expected_xmax);
    EXPECT_EQ(geom_stats->lower_bound()[1], expected_ymin);
    EXPECT_EQ(geom_stats->upper_bound()[1], expected_ymax);
    EXPECT_THAT(geom_stats->dimension_valid(),
                ::testing::ElementsAre(true, true, false, false));
  }

 protected:
  std::shared_ptr<Buffer> file_buf;
};

TEST_F(TestGeometryLogicalType, TestWriteGeometry) {
  TestWriteAndRead(GeometryLogicalType::Make("srid:1234"), /*write_arrow=*/false);
}

TEST_F(TestGeometryLogicalType, TestWriteArrowAndReadGeometry) {
  TestWriteAndRead(GeometryLogicalType::Make("srid:1234"), /*write_arrow=*/true);
}

TEST_F(TestGeometryLogicalType, TestWriteGeography) {
  TestWriteAndRead(GeographyLogicalType::Make("srid:1234"), /*write_arrow=*/false);
}

TEST_F(TestGeometryLogicalType, TestWriteGeographyArrow) {
  TestWriteAndRead(GeographyLogicalType::Make("srid:1234"), /*write_arrow=*/true);
}

}  // namespace parquet
