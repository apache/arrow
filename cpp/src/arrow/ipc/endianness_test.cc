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
#include <limits>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "arrow/io/file.h"
#include "arrow/io/test_common.h"
#include "arrow/ipc/reader.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type_fwd.h"

namespace arrow::ipc::test {

struct EndiannessTestParam {
  std::string integration_file_basename;
  // Whether it's still safe to access data in non-native endianness.
  // This is false for any type that involves indirect addressing through offsets.
  bool can_access_foreign_endian = false;
  // Escape hatch for integration files with invalid data (e.g. decimal values
  // exceeding the advertised precision).
  bool can_validate_full = true;
};

// To avoid Valgrind errors
void PrintTo(const EndiannessTestParam& v, std::ostream* os) {
  *os << v.integration_file_basename;
}

class TestEndianness : public ::testing::TestWithParam<EndiannessTestParam> {
 public:
  void SetUp() { param_ = GetParam(); }

  std::string GetIpcFilePath(Endianness endianness) const {
    return GetEndiannessBasePath(endianness) + "/" + param_.integration_file_basename +
           ".arrow_file";
  }

  std::string GetIpcStreamPath(Endianness endianness) const {
    return GetEndiannessBasePath(endianness) + "/" + param_.integration_file_basename +
           ".stream";
  }

  Endianness native_endianness() const { return Endianness::Native; }

  Endianness foreign_endianness() const {
    return Endianness::Native == Endianness::Little ? Endianness::Big
                                                    : Endianness::Little;
  }

  template <typename OpenReaderFunc>
  void TestReader(OpenReaderFunc&& open_reader) {
    ARROW_SCOPED_TRACE("Native endianness");
    ASSERT_OK_AND_ASSIGN(
        auto reader, open_reader(param_.integration_file_basename, native_endianness(),
                                 /*ensure_native_endian=*/false));
    ASSERT_OK_AND_ASSIGN(auto native_table, reader->ToTable());
    ASSERT_EQ(reader->stats().original_endianness, native_endianness());
    if (param_.can_validate_full) {
      ASSERT_OK(native_table->ValidateFull());
    }

    for (const bool ensure_native_endian : {true, false}) {
      ARROW_SCOPED_TRACE("Foreign endianness: ensure_native_endian = ",
                         ensure_native_endian);
      ASSERT_OK_AND_ASSIGN(
          auto reader, open_reader(param_.integration_file_basename, foreign_endianness(),
                                   ensure_native_endian));
      ASSERT_OK_AND_ASSIGN(auto foreign_table, reader->ToTable());
      ASSERT_NE(reader->stats().original_endianness, native_endianness());
      ASSERT_EQ(reader->stats().original_endianness, foreign_endianness());
      if (ensure_native_endian) {
        if (param_.can_validate_full) {
          ASSERT_OK(foreign_table->ValidateFull());
        }
        AssertTablesEqual(*native_table, *foreign_table);
      } else if (param_.can_access_foreign_endian) {
        ASSERT_FALSE(foreign_table->Equals(*native_table));
      }
    }
  }

 protected:
  std::string GetEndiannessBasePath(Endianness endianness) const {
    return endianness == Endianness::Little
               ? "arrow-ipc-stream/integration/1.0.0-littleendian"
               : "arrow-ipc-stream/integration/1.0.0-bigendian";
  }

  EndiannessTestParam param_;
};

TEST_P(TestEndianness, StreamReader) {
  auto open_stream = [this](const std::string& basename, Endianness endianness,
                            bool ensure_native_endian)
      -> Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>> {
    auto options = IpcReadOptions::Defaults();
    options.ensure_native_endian = ensure_native_endian;
    ARROW_ASSIGN_OR_RAISE(auto path, GetTestResourcePath(GetIpcStreamPath(endianness)));
    ARROW_ASSIGN_OR_RAISE(auto file, io::ReadableFile::Open(path));
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchStreamReader::Open(file, options));
    return reader;
  };

  TestReader(open_stream);
}

TEST_P(TestEndianness, FileReader) {
  auto open_file = [this](const std::string& basename, Endianness endianness,
                          bool ensure_native_endian)
      -> Result<std::shared_ptr<arrow::ipc::RecordBatchFileReader>> {
    auto options = IpcReadOptions::Defaults();
    options.ensure_native_endian = ensure_native_endian;
    ARROW_ASSIGN_OR_RAISE(auto path, GetTestResourcePath(GetIpcFilePath(endianness)));
    ARROW_ASSIGN_OR_RAISE(auto file, io::ReadableFile::Open(path));
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchFileReader::Open(file, options));
    return reader;
  };

  TestReader(open_file);
}

const std::vector<EndiannessTestParam> kEndiannessTestParams{
    {"generated_datetime", /*can_access_foreign_endian=*/true,
     /*can_validate_full=*/false},
    {"generated_decimal", /*can_access_foreign_endian=*/true,
     /*can_validate_full=*/false},
    {"generated_decimal256", /*can_access_foreign_endian=*/true,
     /*can_validate_full=*/false},
    {"generated_dictionary"},
    {"generated_dictionary_unsigned"},
    {"generated_extension"},
    {"generated_interval", /*can_access_foreign_endian=*/true},
    {"generated_map"},
    {"generated_nested"},
    {"generated_nested_dictionary"},
    {"generated_nested_large_offsets"},
    {"generated_null", /*can_access_foreign_endian=*/true},
    {"generated_null_trivial", /*can_access_foreign_endian=*/true},
    {"generated_primitive"},
    {"generated_primitive_large_offsets"},
    {"generated_primitive_no_batches"},
    {"generated_primitive_zerolength"},
    {"generated_recursive_nested"},
    {"generated_union"},
};

INSTANTIATE_TEST_SUITE_P(TestEndianness, TestEndianness,
                         ::testing::ValuesIn(kEndiannessTestParams),
                         [](const ::testing::TestParamInfo<EndiannessTestParam>& info) {
                           return info.param.integration_file_basename;
                         });

}  // namespace arrow::ipc::test
