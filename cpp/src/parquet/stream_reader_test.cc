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

#include "parquet/stream_reader.h"

#include <fcntl.h>
#include <gtest/gtest.h>

#include <chrono>
#include <ctime>
#include <memory>
#include <utility>

#include "arrow/io/api.h"
#include "parquet/exception.h"
#include "parquet/test_util.h"

namespace parquet {
namespace test {

struct TestData {
  static void init() { std::time(&ts_offset_); }

  static constexpr int num_rows = 2000;

  static std::string GetString(const int i) { return "Str #" + std::to_string(i); }
  static bool GetBool(const int i) { return i % 7 < 3; }
  static char GetChar(const int i) { return i & 1 ? 'M' : 'F'; }
  static std::array<char, 4> GetCharArray(const int i) {
    return {'X', 'Y', 'Z', char('A' + i % 26)};
  }
  static int8_t GetInt8(const int i) { return static_cast<int8_t>((i % 256) - 128); }
  static uint16_t GetUInt16(const int i) { return static_cast<uint16_t>(i); }
  static int32_t GetInt32(const int i) { return 3 * i - 17; }
  static uint64_t GetUInt64(const int i) { return (1ull << 40) + i * i + 101; }
  static float GetFloat(const int i) { return 3.1415926535897f * 2.718281828459045f; }
  static double GetDouble(const int i) { return 6.62607004e-34 * 3e8 * i; }

  static std::chrono::microseconds GetChronoMicroseconds(const int i) {
    return std::chrono::microseconds{(ts_offset_ + 3 * i) * 1000000ull + i};
  }

 private:
  static std::time_t ts_offset_;
};

std::time_t TestData::ts_offset_;
constexpr int TestData::num_rows;

class TestStreamReader : public ::testing::Test {
 public:
  TestStreamReader() { createTestFile(); }

 protected:
  const char* GetDataFile() const { return "stream_reader_test.parquet"; }

  void SetUp() {
    std::shared_ptr<arrow::io::ReadableFile> infile;

    PARQUET_THROW_NOT_OK(arrow::io::ReadableFile::Open(GetDataFile(), &infile));

    auto file_reader = parquet::ParquetFileReader::Open(infile);

    reader_ = StreamReader{std::move(file_reader)};
  }

  void TearDown() { reader_ = StreamReader{}; }

  std::shared_ptr<parquet::schema::GroupNode> GetSchema() {
    parquet::schema::NodeVector fields;

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "bool_field", parquet::Repetition::REQUIRED, parquet::Type::BOOLEAN,
        parquet::ConvertedType::NONE));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "string_field", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY,
        parquet::ConvertedType::UTF8));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "char_field", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
        parquet::ConvertedType::NONE, 1));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "char[4]_field", parquet::Repetition::REQUIRED,
        parquet::Type::FIXED_LEN_BYTE_ARRAY, parquet::ConvertedType::NONE, 4));

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
        "chrono_microseconds_field", parquet::Repetition::REQUIRED, parquet::Type::INT64,
        parquet::ConvertedType::TIMESTAMP_MICROS));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "float_field", parquet::Repetition::REQUIRED, parquet::Type::FLOAT,
        parquet::ConvertedType::NONE));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "double_field", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
        parquet::ConvertedType::NONE));

    return std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED,
                                         fields));
  }

  void createTestFile() {
    std::shared_ptr<arrow::io::FileOutputStream> outfile;

    PARQUET_THROW_NOT_OK(arrow::io::FileOutputStream::Open(GetDataFile(), &outfile));

    parquet::WriterProperties::Builder builder;

    builder.compression(parquet::Compression::BROTLI);

    auto file_writer =
        parquet::ParquetFileWriter::Open(outfile, GetSchema(), builder.build());

    parquet::StreamWriter os{std::move(file_writer)};

    TestData::init();

    for (auto i = 0; i < TestData::num_rows; ++i) {
      os << TestData::GetBool(i);
      os << TestData::GetString(i);
      os << TestData::GetChar(i);
      os << TestData::GetCharArray(i);
      os << TestData::GetInt8(i);
      os << TestData::GetUInt16(i);
      os << TestData::GetInt32(i);
      os << TestData::GetUInt64(i);
      os << TestData::GetChronoMicroseconds(i);
      os << TestData::GetFloat(i);
      os << TestData::GetDouble(i);
      os << parquet::EndRow;
    }
  }

  StreamReader reader_;
};

TEST_F(TestStreamReader, DefaultConstructed) {
  StreamReader os;
  int i;
  std::string s;

  // N.B. Default constructor objects are not usable.
  ASSERT_THROW(os >> i, ParquetException);
  ASSERT_THROW(os >> s, ParquetException);
  ASSERT_THROW(os >> EndRow, ParquetException);
}

TEST_F(TestStreamReader, TypeChecking) {
  bool b;
  std::string s;
  std::array<char, 4> char_array;
  char c;
  int8_t int8;
  int16_t int16;
  uint16_t uint16;
  int32_t int32;
  int64_t int64;
  uint64_t uint64;
  std::chrono::microseconds ts_us;
  float f;
  double d;
  std::string str;

  ASSERT_THROW(reader_ >> int8, ParquetException);
  ASSERT_NO_THROW(reader_ >> b);
  ASSERT_THROW(reader_ >> c, ParquetException);
  ASSERT_NO_THROW(reader_ >> s);
  ASSERT_THROW(reader_ >> s, ParquetException);
  ASSERT_NO_THROW(reader_ >> c);
  ASSERT_THROW(reader_ >> s, ParquetException);
  ASSERT_NO_THROW(reader_ >> char_array);
  ASSERT_THROW(reader_ >> int16, ParquetException);
  ASSERT_NO_THROW(reader_ >> int8);
  ASSERT_THROW(reader_ >> int16, ParquetException);
  ASSERT_NO_THROW(reader_ >> uint16);
  ASSERT_THROW(reader_ >> int64, ParquetException);
  ASSERT_NO_THROW(reader_ >> int32);
  ASSERT_THROW(reader_ >> int64, ParquetException);
  ASSERT_NO_THROW(reader_ >> uint64);
  ASSERT_THROW(reader_ >> uint64, ParquetException);
  ASSERT_NO_THROW(reader_ >> ts_us);
  ASSERT_THROW(reader_ >> d, ParquetException);
  ASSERT_NO_THROW(reader_ >> f);
  ASSERT_THROW(reader_ >> f, ParquetException);
  ASSERT_NO_THROW(reader_ >> d);
  ASSERT_NO_THROW(reader_ >> EndRow);
}

TEST_F(TestStreamReader, ValueChecking) {
  bool b;
  std::string s;
  std::array<char, 4> char_array;
  char c;
  int8_t int8;
  uint16_t uint16;
  int32_t int32;
  uint64_t uint64;
  std::chrono::microseconds ts_us;
  float f;
  double d;
  std::string str;

  int i;

  for (i = 0; !reader_.eof(); ++i) {
    reader_ >> b;
    reader_ >> s;
    reader_ >> c;
    reader_ >> char_array;
    reader_ >> int8;
    reader_ >> uint16;
    reader_ >> int32;
    reader_ >> uint64;
    reader_ >> ts_us;
    reader_ >> f;
    reader_ >> d;
    reader_ >> EndRow;

    ASSERT_EQ(b, TestData::GetBool(i));
    ASSERT_EQ(s, TestData::GetString(i));
    ASSERT_EQ(c, TestData::GetChar(i));
    ASSERT_EQ(char_array, TestData::GetCharArray(i));
    ASSERT_EQ(int8, TestData::GetInt8(i));
    ASSERT_EQ(uint16, TestData::GetUInt16(i));
    ASSERT_EQ(int32, TestData::GetInt32(i));
    ASSERT_EQ(uint64, TestData::GetUInt64(i));
    ASSERT_EQ(ts_us, TestData::GetChronoMicroseconds(i));
    ASSERT_FLOAT_EQ(f, TestData::GetFloat(i));
    ASSERT_DOUBLE_EQ(d, TestData::GetDouble(i));
  }
  ASSERT_EQ(i, TestData::num_rows);
}

}  // namespace test
}  // namespace parquet
