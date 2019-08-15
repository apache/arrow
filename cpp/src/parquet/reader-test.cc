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
#include <gtest/gtest.h>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

#include "parquet/column_reader.h"
#include "parquet/column_scanner.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/printer.h"
#include "parquet/test-util.h"

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

class TestAllTypesPlain : public ::testing::Test {
 public:
  void SetUp() { reader_ = ParquetFileReader::OpenFile(alltypes_plain()); }

  void TearDown() {}

 protected:
  std::unique_ptr<ParquetFileReader> reader_;
};

TEST_F(TestAllTypesPlain, NoopConstructDestruct) {}

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

    PARQUET_THROW_NOT_OK(ReadableFile::Open(ss.str(), &handle));
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
  std::string header_output = R"###(File Name: nested_lists.snappy.parquet
Version: 1.0
Created By: parquet-mr version 1.8.2 (build c6522788629e590a53eb79874b95f6c3ff11f16c)
Total rows: 3
Number of RowGroups: 1
Number of Real Columns: 2
Number of Columns: 2
Number of Selected Columns: 2
Column 0: a.list.element.list.element.list.element (BYTE_ARRAY/UTF8)
Column 1: b (INT32)
--- Row Group: 0 ---
--- Total Bytes: 155 ---
--- Rows: 3 ---
Column 0
  Values: 18  Statistics Not Set
  Compression: SNAPPY, Encodings: RLE PLAIN_DICTIONARY
  Uncompressed Size: 103, Compressed Size: 104
Column 1
  Values: 3, Null Values: 0, Distinct Values: 0
  Max: 1, Min: 1
  Compression: SNAPPY, Encodings: BIT_PACKED PLAIN_DICTIONARY
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
  "Version": "0",
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
       "Id": "0",  "TotalBytes": "671",  "Rows": "8",
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

  std::shared_ptr<arrow::io::BufferOutputStream> out_file;
  ASSERT_OK(arrow::io::BufferOutputStream::Create(1024, arrow::default_memory_pool(),
                                                  &out_file));
  std::shared_ptr<ParquetFileWriter> file_writer =
      ParquetFileWriter::Open(out_file, schema, writer_props);

  RowGroupWriter* rg_writer = file_writer->AppendRowGroup();

  std::vector<std::shared_ptr<arrow::Array>> column_data;
  ::arrow::random::RandomArrayGenerator rag(0);

  // Scratch space for reads
  std::vector<std::shared_ptr<Buffer>> scratch_space;

  // write columns
  for (int col_index = 0; col_index < num_columns; ++col_index) {
    DoubleWriter* writer = static_cast<DoubleWriter*>(rg_writer->NextColumn());
    std::shared_ptr<arrow::Array> col = rag.Float64(num_rows, 0, 100);
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
  std::shared_ptr<Buffer> file_buf;
  ASSERT_OK(out_file->Finish(&file_buf));
  auto in_file = std::make_shared<arrow::io::BufferReader>(file_buf);

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

}  // namespace parquet
