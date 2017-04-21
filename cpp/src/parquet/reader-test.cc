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
#include <fcntl.h>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <string>

#include "arrow/io/file.h"

#include "parquet/column/reader.h"
#include "parquet/column/scanner.h"
#include "parquet/file/printer.h"
#include "parquet/file/reader-internal.h"
#include "parquet/file/reader.h"
#include "parquet/util/memory.h"

using std::string;

namespace parquet {

using ReadableFile = ::arrow::io::ReadableFile;

const char* data_dir = std::getenv("PARQUET_TEST_DATA");

std::string alltypes_plain() {
  std::string dir_string(data_dir);
  std::stringstream ss;
  ss << dir_string << "/"
     << "alltypes_plain.parquet";
  return ss.str();
}

std::string nation_dict_truncated_data_page() {
  std::string dir_string(data_dir);
  std::stringstream ss;
  ss << dir_string << "/"
     << "nation.dict-malformed.parquet";
  return ss.str();
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
  // This row group must have 8 rows
  ASSERT_EQ(8, group->metadata()->num_rows());

  ASSERT_TRUE(col->HasNext());
  int64_t values_read;
  int levels_read = col->ReadBatch(4, def_levels, rep_levels, values, &values_read);
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
    std::string dir_string(data_dir);

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

class HelperFileClosed : public ArrowInputFile {
 public:
  explicit HelperFileClosed(
      const std::shared_ptr<::arrow::io::ReadableFileInterface>& file, bool* close_called)
      : ArrowInputFile(file), close_called_(close_called) {}

  void Close() override { *close_called_ = true; }

 private:
  bool* close_called_;
};

TEST_F(TestLocalFile, FileClosedOnDestruction) {
  bool close_called = false;
  {
    auto contents = SerializedFile::Open(
        std::unique_ptr<RandomAccessSource>(new HelperFileClosed(handle, &close_called)));
    std::unique_ptr<ParquetFileReader> result(new ParquetFileReader());
    result->Open(std::move(contents));
  }
  ASSERT_TRUE(close_called);
}

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
  auto reader2 = ParquetFileReader::OpenFile(
      alltypes_plain(), false, default_reader_properties(), metadata);

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

}  // namespace parquet
