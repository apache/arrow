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

#include <fcntl.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"

#include "parquet/column_reader.h"
#include "parquet/column_scanner.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/printer.h"
#include "parquet/test_util.h"

using arrow::internal::checked_pointer_cast;

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

// TODO: Assert on definition and repetition levels
template <typename DType, typename ValueType>
void AssertColumnValues(std::shared_ptr<TypedColumnReader<DType>> col, int64_t batch_size,
                        int64_t expected_levels_read,
                        std::vector<ValueType>& expected_values,
                        int64_t expected_values_read) {
  std::vector<ValueType> values(batch_size);
  int64_t values_read;

  auto levels_read =
      col->ReadBatch(batch_size, nullptr, nullptr, values.data(), &values_read);
  ASSERT_EQ(expected_levels_read, levels_read);

  ASSERT_EQ(expected_values, values);
  ASSERT_EQ(expected_values_read, values_read);
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

TEST_F(TestAllTypesPlain, RowGroupColumnBoundchecking) {
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
  Max: 1, Min: 1
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

TEST(TestJSONWithLocalFile, JSONOutput) {
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

  std::stringstream ss;
  // empty list means print all
  std::list<int> columns;

  auto reader =
      ParquetFileReader::OpenFile(alltypes_plain(), false, default_reader_properties());
  ParquetFilePrinter printer(reader.get());
  printer.JSONPrint(ss, columns, "alltypes_plain.parquet");

  ASSERT_EQ(json_output, ss.str());
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

// https://github.com/google/googletest/pull/2904 not available in our version of
// gtest/gmock
#define EXPECT_THROW_THAT(callable, ex_type, property)   \
  EXPECT_THROW(                                          \
      try { (callable)(); } catch (const ex_type& err) { \
        EXPECT_THAT(err, (property));                    \
        throw;                                           \
      },                                                 \
      ex_type)

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

}  // namespace parquet
