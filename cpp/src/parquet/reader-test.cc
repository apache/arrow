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

#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "parquet/file/reader.h"
#include "parquet/column/reader.h"
#include "parquet/column/scanner.h"

using std::string;

namespace parquet_cpp {

const char* data_dir = std::getenv("PARQUET_TEST_DATA");


class TestAllTypesPlain : public ::testing::Test {
 public:
  void SetUp() {
    std::string dir_string(data_dir);

    std::stringstream ss;
    ss << dir_string << "/" << "alltypes_plain.parquet";

    reader_ = ParquetFileReader::OpenFile(ss.str());
  }

  void TearDown() {}

 protected:
  std::unique_ptr<ParquetFileReader> reader_;
};


TEST_F(TestAllTypesPlain, NoopConstructDestruct) {
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

  ASSERT_TRUE(col->HasNext());
  size_t values_read;
  size_t levels_read = col->ReadBatch(4, def_levels, rep_levels, values, &values_read);
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
  for (size_t i = 0; i < 8; ++i) {
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

  // Automatically parses metadata
  reader_->DebugPrint(ss);

  std::string result = ss.str();
  ASSERT_GT(result.size(), 0);
}

} // namespace parquet_cpp
