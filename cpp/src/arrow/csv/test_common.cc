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

#include "arrow/csv/test_common.h"

#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace csv {

std::string MakeCSVData(std::vector<std::string> lines) {
  std::string s;
  for (const auto& line : lines) {
    s += line;
  }
  return s;
}

void MakeCSVParser(std::vector<std::string> lines, ParseOptions options, int32_t num_cols,
                   MemoryPool* pool, std::shared_ptr<BlockParser>* out) {
  auto csv = MakeCSVData(lines);
  auto parser = std::make_shared<BlockParser>(pool, options, num_cols);
  uint32_t out_size;
  ASSERT_OK(parser->Parse(std::string_view(csv), &out_size));
  ASSERT_EQ(out_size, csv.size()) << "trailing CSV data not parsed";
  *out = parser;
}

void MakeCSVParser(std::vector<std::string> lines, ParseOptions options,
                   std::shared_ptr<BlockParser>* out) {
  return MakeCSVParser(lines, options, -1, default_memory_pool(), out);
}

void MakeCSVParser(std::vector<std::string> lines, std::shared_ptr<BlockParser>* out) {
  MakeCSVParser(lines, ParseOptions::Defaults(), out);
}

void MakeColumnParser(std::vector<std::string> items, std::shared_ptr<BlockParser>* out) {
  auto options = ParseOptions::Defaults();
  // Need this to test for null (empty) values
  options.ignore_empty_lines = false;
  std::vector<std::string> lines;
  for (const auto& item : items) {
    lines.push_back(item + '\n');
  }
  MakeCSVParser(lines, options, 1, default_memory_pool(), out);
  ASSERT_EQ((*out)->num_cols(), 1) << "Should have seen only 1 CSV column";
  ASSERT_EQ((*out)->num_rows(), items.size());
}

namespace {

const std::vector<std::string> int64_rows = {"123", "4", "-317005557", "", "N/A", "0"};
const std::vector<std::string> float_rows = {"0", "123.456", "-3170.55766", "", "N/A"};
const std::vector<std::string> decimal128_rows = {"0", "123.456", "-3170.55766",
                                                  "",  "N/A",     "1233456789.123456789"};
const std::vector<std::string> iso8601_rows = {"1917-10-17", "2018-09-13",
                                               "1941-06-22 04:00", "1945-05-09 09:45:38"};
const std::vector<std::string> strptime_rows = {"10/17/1917", "9/13/2018", "9/5/1945"};

static void WriteHeader(std::ostream& writer) {
  writer << "Int64,Float,Decimal128,ISO8601,Strptime" << std::endl;
}

static std::string GetCell(const std::vector<std::string>& base_rows, size_t row_index) {
  return base_rows[row_index % base_rows.size()];
}

static void WriteRow(std::ostream& writer, size_t row_index) {
  writer << GetCell(int64_rows, row_index);
  writer << ',';
  writer << GetCell(float_rows, row_index);
  writer << ',';
  writer << GetCell(decimal128_rows, row_index);
  writer << ',';
  writer << GetCell(iso8601_rows, row_index);
  writer << ',';
  writer << GetCell(strptime_rows, row_index);
  writer << std::endl;
}

static void WriteInvalidRow(std::ostream& writer, size_t row_index) {
  writer << "\"" << std::endl << "\"";
  writer << std::endl;
}
}  // namespace

Result<std::shared_ptr<Buffer>> MakeSampleCsvBuffer(
    size_t num_rows, std::function<bool(size_t)> is_valid) {
  std::stringstream writer;

  WriteHeader(writer);
  for (size_t i = 0; i < num_rows; ++i) {
    if (!is_valid || is_valid(i)) {
      WriteRow(writer, i);
    } else {
      WriteInvalidRow(writer, i);
    }
  }

  auto table_str = writer.str();
  auto table_buffer = std::make_shared<Buffer>(table_str);
  return MemoryManager::CopyBuffer(table_buffer, default_cpu_memory_manager());
}

}  // namespace csv
}  // namespace arrow
