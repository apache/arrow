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
#include <iostream>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

#include "parquet/reader.h"

using std::string;

namespace parquet_cpp {

const char* data_dir = std::getenv("PARQUET_TEST_DATA");


class TestAllTypesPlain : public ::testing::Test {
 public:
  void SetUp() {
    std::string dir_string(data_dir);

    std::stringstream ss;
    ss << dir_string << "/" << "alltypes_plain.parquet";
    file_.Open(ss.str());
    reader_.Open(&file_);
  }

  void TearDown() {}

 protected:
  LocalFile file_;
  ParquetFileReader reader_;
};


TEST_F(TestAllTypesPlain, ParseMetaData) {
  reader_.ParseMetaData();
}

TEST_F(TestAllTypesPlain, DebugPrintWorks) {
  std::stringstream ss;

  // Automatically parses metadata
  reader_.DebugPrint(ss);

  std::string result = ss.str();
  ASSERT_TRUE(result.size() > 0);
}

} // namespace parquet_cpp
