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

// This module defines an abstract interface for iterating through pages in a
// Parquet column chunk within a row group. It could be extended in the future
// to iterate through all data pages in all chunks in a file.

#pragma once

#include <algorithm>
#include <chrono>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/testing/util.h"
#include "arrow/util/io_util.h"

#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/encoding.h"
#include "parquet/encryption.h"
#include "parquet/platform.h"
#include "parquet/test_util.h"

namespace parquet {
namespace test {

using arrow::internal::TemporaryDir;

using parquet::ConvertedType;
using parquet::Repetition;
using parquet::Type;
using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;

constexpr int kFixedLength = 10;

const char kFooterEncryptionKey[] = "0123456789012345";  // 128bit/16
const char kColumnEncryptionKey1[] = "1234567890123450";
const char kColumnEncryptionKey2[] = "1234567890123451";
const char kFileName[] = "tester";

inline std::string data_file(const char* file) {
  std::string dir_string(test::get_data_dir());
  std::stringstream ss;
  ss << dir_string << "/" << file;
  return ss.str();
}

// A temporary directory that contains the encrypted files generated in the tests.
extern std::unique_ptr<TemporaryDir> temp_dir;

inline arrow::Result<std::unique_ptr<TemporaryDir>> temp_data_dir() {
  arrow::Result<std::unique_ptr<TemporaryDir>> dir;
  ARROW_ASSIGN_OR_RAISE(dir, TemporaryDir::Make("parquet-encryption-test-"));
  return dir;
}

static constexpr char DOUBLE_FIELD_NAME[] = "double_field";
static constexpr char FLOAT_FIELD_NAME[] = "float_field";
static constexpr char BOOLEAN_FIELD_NAME[] = "boolean_field";
static constexpr char INT32_FIELD_NAME[] = "int32_field";
static constexpr char INT64_FIELD_NAME[] = "int64_field";
static constexpr char INT96_FIELD_NAME[] = "int96_field";
static constexpr char BA_FIELD_NAME[] = "ba_field";
static constexpr char FLBA_FIELD_NAME[] = "flba_field";

class FileEncryptor {
 public:
  FileEncryptor();

  void EncryptFile(
      std::string file,
      std::shared_ptr<parquet::FileEncryptionProperties> encryption_configurations);

 private:
  std::shared_ptr<GroupNode> SetupEncryptionSchema();

  int num_rgs = 5;
  int rows_per_rowgroup_ = 50;
  std::shared_ptr<GroupNode> schema_;
};

class FileDecryptor {
 public:
  void DecryptFile(std::string file_name,
                   std::shared_ptr<FileDecryptionProperties> file_decryption_properties);
};

}  // namespace test
}  // namespace parquet
