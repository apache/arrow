// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <utility>

#include "arrow/io/file.h"
#include "parquet/exception.h"
#include "parquet/stream_reader.h"
#include "parquet/stream_writer.h"

// This file gives an example of how to use the parquet::StreamWriter
// and parquet::StreamReader classes.
// It shows writing/reading of the supported types as well as how a
// user-defined type can be handled.

// Example of a user-defined type to be written to/read from Parquet
// using C++ input/output operators.
class UserTimestamp {
 public:
  UserTimestamp() = default;

  UserTimestamp(const std::chrono::microseconds v) : ts_{v} {}

  bool operator==(const UserTimestamp& x) const { return ts_ == x.ts_; }

  void dump(std::ostream& os) const {
    std::time_t t{std::chrono::duration_cast<std::chrono::seconds>(ts_).count()};
    os << std::put_time(std::gmtime(&t), "%Y%m%d-%H%M%S");
  }

  void dump(parquet::StreamWriter& os) const { os << ts_; }

 private:
  std::chrono::microseconds ts_;
};

std::ostream& operator<<(std::ostream& os, const UserTimestamp& v) {
  v.dump(os);
  return os;
}

parquet::StreamWriter& operator<<(parquet::StreamWriter& os, const UserTimestamp& v) {
  v.dump(os);
  return os;
}

parquet::StreamReader& operator>>(parquet::StreamReader& os, UserTimestamp& v) {
  std::chrono::microseconds ts;

  os >> ts;
  v = UserTimestamp{ts};

  return os;
}

std::shared_ptr<parquet::schema::GroupNode> GetSchema() {
  parquet::schema::NodeVector fields;

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "string_field", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY,
      parquet::ConvertedType::UTF8));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "char_field", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
      parquet::ConvertedType::NONE, 1));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "char[4]_field", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
      parquet::ConvertedType::NONE, 4));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "int8_field", parquet::Repetition::REQUIRED, parquet::Type::INT32,
      parquet::ConvertedType::INT_8));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "uint16_field", parquet::Repetition::REQUIRED, parquet::Type::INT32,
      parquet::ConvertedType::UINT_16));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "int32_field", parquet::Repetition::REQUIRED, parquet::Type::INT32,
      parquet::ConvertedType::INT_32));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "uint64_field", parquet::Repetition::REQUIRED, parquet::Type::INT64,
      parquet::ConvertedType::UINT_64));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "double_field", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
      parquet::ConvertedType::NONE));

  // User defined timestamp type.
  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "timestamp_field", parquet::Repetition::REQUIRED, parquet::Type::INT64,
      parquet::ConvertedType::TIMESTAMP_MICROS));

  fields.push_back(parquet::schema::PrimitiveNode::Make(
      "chrono_milliseconds_field", parquet::Repetition::REQUIRED, parquet::Type::INT64,
      parquet::ConvertedType::TIMESTAMP_MILLIS));

  return std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
}

struct TestData {
  static const int num_rows = 2000;

  static void init() { std::time(&ts_offset_); }

  static std::string GetString(const int i) { return "Str #" + std::to_string(i); }
  static arrow::util::string_view GetStringView(const int i) {
    string_ = "StringView #" + std::to_string(i);
    return arrow::util::string_view(string_);
  }
  static const char* GetCharPtr(const int i) {
    string_ = "CharPtr #" + std::to_string(i);
    return string_.c_str();
  }
  static char GetChar(const int i) { return i & 1 ? 'M' : 'F'; }
  static int8_t GetInt8(const int i) { return static_cast<int8_t>((i % 256) - 128); }
  static uint16_t GetUInt16(const int i) { return static_cast<uint16_t>(i); }
  static int32_t GetInt32(const int i) { return 3 * i - 17; }
  static uint64_t GetUInt64(const int i) { return (1ull << 40) + i * i + 101; }
  static double GetDouble(const int i) { return 6.62607004e-34 * 3e8 * i; }
  static UserTimestamp GetUserTimestamp(const int i) {
    return UserTimestamp{std::chrono::microseconds{(ts_offset_ + 3 * i) * 1000000 + i}};
  }
  static std::chrono::milliseconds GetChronoMilliseconds(const int i) {
    return std::chrono::milliseconds{(ts_offset_ + 3 * i) * 1000ull + i};
  }

  static char char4_array[4];

 private:
  static std::time_t ts_offset_;
  static std::string string_;
};

char TestData::char4_array[] = "XYZ";
std::time_t TestData::ts_offset_;
std::string TestData::string_;

void WriteParquetFile() {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;

  PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open("parquet-stream-api-example.parquet"));

  parquet::WriterProperties::Builder builder;

  builder.compression(parquet::Compression::BROTLI);

  auto file_writer =
      parquet::ParquetFileWriter::Open(outfile, GetSchema(), builder.build());

  parquet::StreamWriter os{std::move(file_writer)};

  os.SetMaxRowGroupSize(1000);

  for (auto i = 0; i < TestData::num_rows; ++i) {
    // Output string using 3 different types: std::string, arrow::util::string_view and
    // const char *.
    switch (i % 3) {
      case 0:
        os << TestData::GetString(i);
        break;
      case 1:
        os << TestData::GetStringView(i);
        break;
      case 2:
        os << TestData::GetCharPtr(i);
        break;
    }
    os << TestData::GetChar(i);
    switch (i % 2) {
      case 0:
        os << TestData::char4_array;
        break;
      case 1:
        os << parquet::StreamWriter::FixedStringView{TestData::GetCharPtr(i), 4};
        break;
    }
    os << TestData::GetInt8(i);
    os << TestData::GetUInt16(i);
    os << TestData::GetInt32(i);
    os << TestData::GetUInt64(i);
    os << TestData::GetDouble(i);
    os << TestData::GetUserTimestamp(i);
    os << TestData::GetChronoMilliseconds(i);
    os << parquet::EndRow;

    if (i == TestData::num_rows / 2) {
      os << parquet::EndRowGroup;
    }
  }
  std::cout << "Parquet Stream Writing complete." << std::endl;
}

void ReadParquetFile() {
  std::shared_ptr<arrow::io::ReadableFile> infile;

  PARQUET_ASSIGN_OR_THROW(
      infile,
      arrow::io::ReadableFile::Open("parquet-stream-api-example.parquet"));

  auto file_reader = parquet::ParquetFileReader::Open(infile);

  parquet::StreamReader os{std::move(file_reader)};

  std::string s;
  char ch;
  char char_array[4];
  int8_t int8;
  uint16_t uint16;
  int32_t int32;
  uint64_t uint64;
  double d;
  UserTimestamp ts_user;
  std::chrono::milliseconds ts_ms;
  int i;

  for (i = 0; !os.eof(); ++i) {
    os >> s;
    os >> ch;
    os >> char_array;
    os >> int8;
    os >> uint16;
    os >> int32;
    os >> uint64;
    os >> d;
    os >> ts_user;
    os >> ts_ms;
    os >> parquet::EndRow;

    if (0) {
      // For debugging.
      std::cout << s << ' ' << ch << ' ' << char_array << ' ' << int(int8) << ' '
                << uint16 << ' ' << int32 << ' ' << uint64 << ' ' << d << ' ' << ts_user
                << ' ' << ts_ms.count() << std::endl;
    }
    // Check data.
    switch (i % 3) {
      case 0:
        assert(s == TestData::GetString(i));
        break;
      case 1:
        assert(s == TestData::GetStringView(i));
        break;
      case 2:
        assert(s == TestData::GetCharPtr(i));
        break;
    }
    assert(ch == TestData::GetChar(i));
    switch (i % 2) {
      case 0:
        assert(0 == std::memcmp(char_array, TestData::char4_array, sizeof(char_array)));
        break;
      case 1:
        assert(0 == std::memcmp(char_array, TestData::GetCharPtr(i), sizeof(char_array)));
        break;
    }
    assert(int8 == TestData::GetInt8(i));
    assert(uint16 == TestData::GetUInt16(i));
    assert(int32 == TestData::GetInt32(i));
    assert(uint64 == TestData::GetUInt64(i));
    assert(std::abs(d - TestData::GetDouble(i)) < 1e-6);
    assert(ts_user == TestData::GetUserTimestamp(i));
    assert(ts_ms == TestData::GetChronoMilliseconds(i));
  }
  assert(TestData::num_rows == i);

  std::cout << "Parquet Stream Reading complete." << std::endl;
}

int main() {
  WriteParquetFile();
  ReadParquetFile();

  return 0;
}
