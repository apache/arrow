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

#include "arrow/io/file.h"
#include "arrow/util/decimal.h"
#include "parquet/exception.h"
#include "parquet/test_util.h"

namespace parquet {
namespace test {

template <typename T>
using optional = StreamReader::optional<T>;
using ::std::nullopt;

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
  static float GetFloat(const int i) { return 3.1415926535897f * i; }
  static double GetDouble(const int i) { return 6.62607004e-34 * 3e8 * i; }

  static std::chrono::microseconds GetChronoMicroseconds(const int i) {
    return std::chrono::microseconds{(ts_offset_ + 3 * i) * 1000000ull + i};
  }

  static optional<bool> GetOptBool(const int i) {
    if (i % 11 == 0) {
      return nullopt;
    }
    return i % 7 < 3;
  }

  static optional<char> GetOptChar(const int i) {
    if ((i + 1) % 11 == 1) {
      return nullopt;
    }
    return i & 1 ? 'M' : 'F';
  }

  static optional<std::array<char, 4>> GetOptCharArray(const int i) {
    if ((i + 2) % 11 == 1) {
      return nullopt;
    }
    return std::array<char, 4>{{'X', 'Y', 'Z', char('A' + i % 26)}};
  }

  static optional<int8_t> GetOptInt8(const int i) {
    if ((i + 3) % 11 == 1) {
      return nullopt;
    }
    return static_cast<int8_t>((i % 256) - 128);
  }

  static optional<uint16_t> GetOptUInt16(const int i) {
    if ((i + 4) % 11 == 1) {
      return nullopt;
    }
    return static_cast<uint16_t>(i);
  }

  static optional<int32_t> GetOptInt32(const int i) {
    if ((i + 5) % 11 == 1) {
      return nullopt;
    }
    return 3 * i - 17;
  }

  static optional<uint64_t> GetOptUInt64(const int i) {
    if ((i + 6) % 11 == 1) {
      return nullopt;
    }
    return (1ull << 40) + i * i + 101;
  }

  static optional<std::string> GetOptString(const int i) {
    if (i % 5 == 0) {
      return nullopt;
    }
    return "Str #" + std::to_string(i);
  }

  static optional<float> GetOptFloat(const int i) {
    if ((i + 1) % 3 == 0) {
      return nullopt;
    }
    return 2.718281828459045f * i;
  }

  static optional<double> GetOptDouble(const int i) {
    if ((i + 2) % 3 == 0) {
      return nullopt;
    }
    return 6.62607004e-34 * 3e8 * i;
  }

  static optional<std::chrono::microseconds> GetOptChronoMicroseconds(const int i) {
    if ((i + 2) % 7 == 0) {
      return nullopt;
    }
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
    PARQUET_ASSIGN_OR_THROW(auto infile, ::arrow::io::ReadableFile::Open(GetDataFile()));
    auto file_reader = parquet::ParquetFileReader::Open(infile);
    reader_ = StreamReader{std::move(file_reader)};
  }

  void TearDown() { reader_ = StreamReader{}; }

  std::shared_ptr<schema::GroupNode> GetSchema() {
    schema::NodeVector fields;

    fields.push_back(schema::PrimitiveNode::Make("bool_field", Repetition::REQUIRED,
                                                 Type::BOOLEAN, ConvertedType::NONE));

    fields.push_back(schema::PrimitiveNode::Make("string_field", Repetition::REQUIRED,
                                                 Type::BYTE_ARRAY, ConvertedType::UTF8));

    fields.push_back(schema::PrimitiveNode::Make("char_field", Repetition::REQUIRED,
                                                 Type::FIXED_LEN_BYTE_ARRAY,
                                                 ConvertedType::NONE, 1));

    fields.push_back(schema::PrimitiveNode::Make("char[4]_field", Repetition::REQUIRED,
                                                 Type::FIXED_LEN_BYTE_ARRAY,
                                                 ConvertedType::NONE, 4));

    fields.push_back(schema::PrimitiveNode::Make("int8_field", Repetition::REQUIRED,
                                                 Type::INT32, ConvertedType::INT_8));

    fields.push_back(schema::PrimitiveNode::Make("uint16_field", Repetition::REQUIRED,
                                                 Type::INT32, ConvertedType::UINT_16));

    fields.push_back(schema::PrimitiveNode::Make("int32_field", Repetition::REQUIRED,
                                                 Type::INT32, ConvertedType::INT_32));

    fields.push_back(schema::PrimitiveNode::Make("uint64_field", Repetition::REQUIRED,
                                                 Type::INT64, ConvertedType::UINT_64));

    fields.push_back(schema::PrimitiveNode::Make("chrono_microseconds_field",
                                                 Repetition::REQUIRED, Type::INT64,
                                                 ConvertedType::TIMESTAMP_MICROS));

    fields.push_back(schema::PrimitiveNode::Make("float_field", Repetition::REQUIRED,
                                                 Type::FLOAT, ConvertedType::NONE));

    fields.push_back(schema::PrimitiveNode::Make("double_field", Repetition::REQUIRED,
                                                 Type::DOUBLE, ConvertedType::NONE));

    return std::static_pointer_cast<schema::GroupNode>(
        schema::GroupNode::Make("schema", Repetition::REQUIRED, fields));
  }

  void createTestFile() {
    PARQUET_ASSIGN_OR_THROW(auto outfile,
                            ::arrow::io::FileOutputStream::Open(GetDataFile()));

    auto file_writer = ParquetFileWriter::Open(outfile, GetSchema());

    StreamWriter os{std::move(file_writer)};

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
      os << EndRow;
    }
  }

  StreamReader reader_;
};

TEST_F(TestStreamReader, DefaultConstructed) {
  StreamReader os;
  int i;
  std::string s;

  // N.B. Default constructor objects are not usable.
  EXPECT_THROW(os >> i, ParquetException);
  EXPECT_THROW(os >> s, ParquetException);
  EXPECT_THROW(os >> EndRow, ParquetException);

  EXPECT_EQ(true, os.eof());
  EXPECT_EQ(0, os.current_column());
  EXPECT_EQ(0, os.current_row());

  EXPECT_EQ(0, os.num_columns());
  EXPECT_EQ(0, os.num_rows());

  // Skipping columns and rows is allowed.
  //
  EXPECT_EQ(0, os.SkipColumns(100));
  EXPECT_EQ(0, os.SkipRows(100));
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

  EXPECT_THROW(reader_ >> int8, ParquetException);
  EXPECT_NO_THROW(reader_ >> b);
  EXPECT_THROW(reader_ >> c, ParquetException);
  EXPECT_NO_THROW(reader_ >> s);
  EXPECT_THROW(reader_ >> s, ParquetException);
  EXPECT_NO_THROW(reader_ >> c);
  EXPECT_THROW(reader_ >> s, ParquetException);
  EXPECT_NO_THROW(reader_ >> char_array);
  EXPECT_THROW(reader_ >> int16, ParquetException);
  EXPECT_NO_THROW(reader_ >> int8);
  EXPECT_THROW(reader_ >> int16, ParquetException);
  EXPECT_NO_THROW(reader_ >> uint16);
  EXPECT_THROW(reader_ >> int64, ParquetException);
  EXPECT_NO_THROW(reader_ >> int32);
  EXPECT_THROW(reader_ >> int64, ParquetException);
  EXPECT_NO_THROW(reader_ >> uint64);
  EXPECT_THROW(reader_ >> uint64, ParquetException);
  EXPECT_NO_THROW(reader_ >> ts_us);
  EXPECT_THROW(reader_ >> d, ParquetException);
  EXPECT_NO_THROW(reader_ >> f);
  EXPECT_THROW(reader_ >> f, ParquetException);
  EXPECT_NO_THROW(reader_ >> d);
  EXPECT_NO_THROW(reader_ >> EndRow);
}

TEST_F(TestStreamReader, ValueChecking) {
  bool b;
  std::string str;
  std::array<char, 4> char_array;
  char c;
  int8_t int8;
  uint16_t uint16;
  int32_t int32;
  uint64_t uint64;
  std::chrono::microseconds ts_us;
  float f;
  double d;

  int i;

  for (i = 0; !reader_.eof(); ++i) {
    EXPECT_EQ(i, reader_.current_row());

    reader_ >> b;
    reader_ >> str;
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

    EXPECT_EQ(b, TestData::GetBool(i)) << "index: " << i;
    EXPECT_EQ(str, TestData::GetString(i)) << "index: " << i;
    EXPECT_EQ(c, TestData::GetChar(i)) << "index: " << i;
    EXPECT_EQ(char_array, TestData::GetCharArray(i)) << "index: " << i;
    EXPECT_EQ(int8, TestData::GetInt8(i)) << "index: " << i;
    EXPECT_EQ(uint16, TestData::GetUInt16(i)) << "index: " << i;
    EXPECT_EQ(int32, TestData::GetInt32(i)) << "index: " << i;
    EXPECT_EQ(uint64, TestData::GetUInt64(i)) << "index: " << i;
    EXPECT_EQ(ts_us, TestData::GetChronoMicroseconds(i)) << "index: " << i;
    EXPECT_FLOAT_EQ(f, TestData::GetFloat(i)) << "index: " << i;
    EXPECT_DOUBLE_EQ(d, TestData::GetDouble(i)) << "index: " << i;
  }
  EXPECT_EQ(reader_.current_row(), TestData::num_rows);
  EXPECT_EQ(reader_.num_rows(), TestData::num_rows);
  EXPECT_EQ(i, TestData::num_rows);
}

TEST_F(TestStreamReader, ReadRequiredFieldAsOptionalField) {
  /* Test that required fields can be read using optional types.

    This can be useful if a schema is changed such that a field which
    was optional is changed to be required.  Applications can continue
    to read the field as if it were still optional.
  */

  optional<bool> opt_bool;
  optional<std::string> opt_string;
  optional<std::array<char, 4>> opt_char_array;
  optional<char> opt_char;
  optional<int8_t> opt_int8;
  optional<uint16_t> opt_uint16;
  optional<int32_t> opt_int32;
  optional<uint64_t> opt_uint64;
  optional<std::chrono::microseconds> opt_ts_us;
  optional<float> opt_float;
  optional<double> opt_double;

  int i;

  for (i = 0; !reader_.eof(); ++i) {
    EXPECT_EQ(i, reader_.current_row());

    reader_ >> opt_bool;
    reader_ >> opt_string;
    reader_ >> opt_char;
    reader_ >> opt_char_array;
    reader_ >> opt_int8;
    reader_ >> opt_uint16;
    reader_ >> opt_int32;
    reader_ >> opt_uint64;
    reader_ >> opt_ts_us;
    reader_ >> opt_float;
    reader_ >> opt_double;
    reader_ >> EndRow;

    EXPECT_EQ(*opt_bool, TestData::GetBool(i)) << "index: " << i;
    EXPECT_EQ(*opt_string, TestData::GetString(i)) << "index: " << i;
    EXPECT_EQ(*opt_char, TestData::GetChar(i)) << "index: " << i;
    EXPECT_EQ(*opt_char_array, TestData::GetCharArray(i)) << "index: " << i;
    EXPECT_EQ(*opt_int8, TestData::GetInt8(i)) << "index: " << i;
    EXPECT_EQ(*opt_uint16, TestData::GetUInt16(i)) << "index: " << i;
    EXPECT_EQ(*opt_int32, TestData::GetInt32(i)) << "index: " << i;
    EXPECT_EQ(*opt_uint64, TestData::GetUInt64(i)) << "index: " << i;
    EXPECT_EQ(*opt_ts_us, TestData::GetChronoMicroseconds(i)) << "index: " << i;
    EXPECT_FLOAT_EQ(*opt_float, TestData::GetFloat(i)) << "index: " << i;
    EXPECT_DOUBLE_EQ(*opt_double, TestData::GetDouble(i)) << "index: " << i;
  }
  EXPECT_EQ(reader_.current_row(), TestData::num_rows);
  EXPECT_EQ(reader_.num_rows(), TestData::num_rows);
  EXPECT_EQ(i, TestData::num_rows);
}

TEST_F(TestStreamReader, SkipRows) {
  // Skipping zero and negative number of rows is ok.
  //
  EXPECT_EQ(0, reader_.SkipRows(0));
  EXPECT_EQ(0, reader_.SkipRows(-100));

  EXPECT_EQ(false, reader_.eof());
  EXPECT_EQ(0, reader_.current_row());
  EXPECT_EQ(TestData::num_rows, reader_.num_rows());

  const int iter_num_rows_to_read = 3;
  const int iter_num_rows_to_skip = 13;
  int num_rows_read = 0;
  int i = 0;
  int num_iterations;

  for (num_iterations = 0; !reader_.eof(); ++num_iterations) {
    // Each iteration of this loop reads some rows (iter_num_rows_to_read
    // are read) and then skips some rows (iter_num_rows_to_skip will be
    // skipped).
    // The loop variable i is the current row being read.
    // Loop variable j is used just to count the number of rows to
    // read.
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

    for (int j = 0; !reader_.eof() && (j < iter_num_rows_to_read); ++i, ++j) {
      EXPECT_EQ(i, reader_.current_row());

      reader_ >> b;
      reader_ >> s;
      reader_ >> c;
      reader_ >> char_array;
      reader_ >> int8;
      reader_ >> uint16;

      // Not allowed to skip row once reading columns has started.
      EXPECT_THROW(reader_.SkipRows(1), ParquetException);

      reader_ >> int32;
      reader_ >> uint64;
      reader_ >> ts_us;
      reader_ >> f;
      reader_ >> d;
      reader_ >> EndRow;
      num_rows_read += 1;

      EXPECT_EQ(b, TestData::GetBool(i));
      EXPECT_EQ(s, TestData::GetString(i));
      EXPECT_EQ(c, TestData::GetChar(i));
      EXPECT_EQ(char_array, TestData::GetCharArray(i));
      EXPECT_EQ(int8, TestData::GetInt8(i));
      EXPECT_EQ(uint16, TestData::GetUInt16(i));
      EXPECT_EQ(int32, TestData::GetInt32(i));
      EXPECT_EQ(uint64, TestData::GetUInt64(i));
      EXPECT_EQ(ts_us, TestData::GetChronoMicroseconds(i));
      EXPECT_FLOAT_EQ(f, TestData::GetFloat(i));
      EXPECT_DOUBLE_EQ(d, TestData::GetDouble(i));
    }
    EXPECT_EQ(iter_num_rows_to_skip, reader_.SkipRows(iter_num_rows_to_skip));
    i += iter_num_rows_to_skip;
  }
  EXPECT_EQ(TestData::num_rows, reader_.current_row());

  EXPECT_EQ(num_rows_read, num_iterations * iter_num_rows_to_read);

  // Skipping rows at eof is allowed.
  //
  EXPECT_EQ(0, reader_.SkipRows(100));
}

TEST_F(TestStreamReader, SkipAllRows) {
  EXPECT_EQ(false, reader_.eof());
  EXPECT_EQ(0, reader_.current_row());

  EXPECT_EQ(reader_.num_rows(), reader_.SkipRows(2 * reader_.num_rows()));

  EXPECT_EQ(true, reader_.eof());
  EXPECT_EQ(reader_.num_rows(), reader_.current_row());
}

TEST_F(TestStreamReader, SkipColumns) {
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

  // Skipping zero and negative number of columns is ok.
  //
  EXPECT_EQ(0, reader_.SkipColumns(0));
  EXPECT_EQ(0, reader_.SkipColumns(-100));

  for (i = 0; !reader_.eof(); ++i) {
    EXPECT_EQ(i, reader_.current_row());
    EXPECT_EQ(0, reader_.current_column());

    // Skip all columns every 31 rows.
    if (i % 31 == 0) {
      EXPECT_EQ(reader_.num_columns(), reader_.SkipColumns(reader_.num_columns()))
          << "index: " << i;
      EXPECT_EQ(reader_.num_columns(), reader_.current_column()) << "index: " << i;
      reader_ >> EndRow;
      continue;
    }
    reader_ >> b;
    EXPECT_EQ(b, TestData::GetBool(i)) << "index: " << i;
    EXPECT_EQ(1, reader_.current_column()) << "index: " << i;

    // Skip the next column every 3 rows.
    if (i % 3 == 0) {
      EXPECT_EQ(1, reader_.SkipColumns(1)) << "index: " << i;
    } else {
      reader_ >> s;
      EXPECT_EQ(s, TestData::GetString(i)) << "index: " << i;
    }
    EXPECT_EQ(2, reader_.current_column()) << "index: " << i;

    reader_ >> c;
    EXPECT_EQ(c, TestData::GetChar(i)) << "index: " << i;
    EXPECT_EQ(3, reader_.current_column()) << "index: " << i;
    reader_ >> char_array;
    EXPECT_EQ(char_array, TestData::GetCharArray(i)) << "index: " << i;
    EXPECT_EQ(4, reader_.current_column()) << "index: " << i;
    reader_ >> int8;
    EXPECT_EQ(int8, TestData::GetInt8(i)) << "index: " << i;
    EXPECT_EQ(5, reader_.current_column()) << "index: " << i;

    // Skip the next 3 columns every 7 rows.
    if (i % 7 == 0) {
      EXPECT_EQ(3, reader_.SkipColumns(3)) << "index: " << i;
    } else {
      reader_ >> uint16;
      EXPECT_EQ(uint16, TestData::GetUInt16(i)) << "index: " << i;
      EXPECT_EQ(6, reader_.current_column()) << "index: " << i;
      reader_ >> int32;
      EXPECT_EQ(int32, TestData::GetInt32(i)) << "index: " << i;
      EXPECT_EQ(7, reader_.current_column()) << "index: " << i;
      reader_ >> uint64;
      EXPECT_EQ(uint64, TestData::GetUInt64(i)) << "index: " << i;
    }
    EXPECT_EQ(8, reader_.current_column());

    reader_ >> ts_us;
    EXPECT_EQ(ts_us, TestData::GetChronoMicroseconds(i)) << "index: " << i;
    EXPECT_EQ(9, reader_.current_column()) << "index: " << i;

    // Skip 301 columns (i.e. all remaining) every 11 rows.
    if (i % 11 == 0) {
      EXPECT_EQ(2, reader_.SkipColumns(301)) << "index: " << i;
    } else {
      reader_ >> f;
      EXPECT_FLOAT_EQ(f, TestData::GetFloat(i)) << "index: " << i;
      EXPECT_EQ(10, reader_.current_column()) << "index: " << i;
      reader_ >> d;
      EXPECT_DOUBLE_EQ(d, TestData::GetDouble(i)) << "index: " << i;
    }
    EXPECT_EQ(11, reader_.current_column()) << "index: " << i;
    reader_ >> EndRow;
  }
  EXPECT_EQ(i, TestData::num_rows);
  EXPECT_EQ(reader_.current_row(), TestData::num_rows);

  // Skipping columns at eof is allowed.
  //
  EXPECT_EQ(0, reader_.SkipColumns(100));
}

class TestOptionalFields : public ::testing::Test {
 public:
  TestOptionalFields() { createTestFile(); }

 protected:
  const char* GetDataFile() const { return "stream_reader_test_optional_fields.parquet"; }

  void SetUp() {
    PARQUET_ASSIGN_OR_THROW(auto infile, ::arrow::io::ReadableFile::Open(GetDataFile()));

    auto file_reader = ParquetFileReader::Open(infile);

    reader_ = StreamReader{std::move(file_reader)};
  }

  void TearDown() { reader_ = StreamReader{}; }

  std::shared_ptr<schema::GroupNode> GetSchema() {
    schema::NodeVector fields;

    fields.push_back(schema::PrimitiveNode::Make("bool_field", Repetition::OPTIONAL,
                                                 Type::BOOLEAN, ConvertedType::NONE));

    fields.push_back(schema::PrimitiveNode::Make("string_field", Repetition::OPTIONAL,
                                                 Type::BYTE_ARRAY, ConvertedType::UTF8));

    fields.push_back(schema::PrimitiveNode::Make("char_field", Repetition::OPTIONAL,
                                                 Type::FIXED_LEN_BYTE_ARRAY,
                                                 ConvertedType::NONE, 1));

    fields.push_back(schema::PrimitiveNode::Make("char[4]_field", Repetition::OPTIONAL,
                                                 Type::FIXED_LEN_BYTE_ARRAY,
                                                 ConvertedType::NONE, 4));

    fields.push_back(schema::PrimitiveNode::Make("int8_field", Repetition::OPTIONAL,
                                                 Type::INT32, ConvertedType::INT_8));

    fields.push_back(schema::PrimitiveNode::Make("uint16_field", Repetition::OPTIONAL,
                                                 Type::INT32, ConvertedType::UINT_16));

    fields.push_back(schema::PrimitiveNode::Make("int32_field", Repetition::OPTIONAL,
                                                 Type::INT32, ConvertedType::INT_32));

    fields.push_back(schema::PrimitiveNode::Make("uint64_field", Repetition::OPTIONAL,
                                                 Type::INT64, ConvertedType::UINT_64));

    fields.push_back(schema::PrimitiveNode::Make("chrono_microseconds_field",
                                                 Repetition::OPTIONAL, Type::INT64,
                                                 ConvertedType::TIMESTAMP_MICROS));

    fields.push_back(schema::PrimitiveNode::Make("float_field", Repetition::OPTIONAL,
                                                 Type::FLOAT, ConvertedType::NONE));

    fields.push_back(schema::PrimitiveNode::Make("double_field", Repetition::OPTIONAL,
                                                 Type::DOUBLE, ConvertedType::NONE));

    return std::static_pointer_cast<schema::GroupNode>(
        schema::GroupNode::Make("schema", Repetition::REQUIRED, fields));
  }

  void createTestFile() {
    PARQUET_ASSIGN_OR_THROW(auto outfile,
                            ::arrow::io::FileOutputStream::Open(GetDataFile()));

    StreamWriter os{ParquetFileWriter::Open(outfile, GetSchema())};

    TestData::init();

    for (auto i = 0; i < TestData::num_rows; ++i) {
      os << TestData::GetOptBool(i);
      os << TestData::GetOptString(i);
      os << TestData::GetOptChar(i);
      os << TestData::GetOptCharArray(i);
      os << TestData::GetOptInt8(i);
      os << TestData::GetOptUInt16(i);
      os << TestData::GetOptInt32(i);
      os << TestData::GetOptUInt64(i);
      os << TestData::GetOptChronoMicroseconds(i);
      os << TestData::GetOptFloat(i);
      os << TestData::GetOptDouble(i);
      os << EndRow;
    }
  }

  StreamReader reader_;
};

TEST_F(TestOptionalFields, ValueChecking) {
  optional<bool> opt_bool;
  optional<std::string> opt_string;
  optional<std::array<char, 4>> opt_char_array;
  optional<char> opt_char;
  optional<int8_t> opt_int8;
  optional<uint16_t> opt_uint16;
  optional<int32_t> opt_int32;
  optional<uint64_t> opt_uint64;
  optional<std::chrono::microseconds> opt_ts_us;
  optional<float> opt_float;
  optional<double> opt_double;

  int i;

  for (i = 0; !reader_.eof(); ++i) {
    EXPECT_EQ(i, reader_.current_row());

    reader_ >> opt_bool;
    reader_ >> opt_string;
    reader_ >> opt_char;
    reader_ >> opt_char_array;
    reader_ >> opt_int8;
    reader_ >> opt_uint16;
    reader_ >> opt_int32;
    reader_ >> opt_uint64;
    reader_ >> opt_ts_us;
    reader_ >> opt_float;
    reader_ >> opt_double;
    reader_ >> EndRow;

    EXPECT_EQ(opt_bool, TestData::GetOptBool(i)) << "index: " << i;
    EXPECT_EQ(opt_string, TestData::GetOptString(i)) << "index: " << i;
    EXPECT_EQ(opt_char, TestData::GetOptChar(i)) << "index: " << i;
    EXPECT_EQ(opt_char_array, TestData::GetOptCharArray(i)) << "index: " << i;
    EXPECT_EQ(opt_int8, TestData::GetOptInt8(i)) << "index: " << i;
    EXPECT_EQ(opt_uint16, TestData::GetOptUInt16(i)) << "index: " << i;
    EXPECT_EQ(opt_int32, TestData::GetOptInt32(i)) << "index: " << i;
    EXPECT_EQ(opt_uint64, TestData::GetOptUInt64(i)) << "index: " << i;
    EXPECT_EQ(opt_ts_us, TestData::GetOptChronoMicroseconds(i)) << "index: " << i;
    if (opt_float && TestData::GetOptFloat(i)) {
      EXPECT_FLOAT_EQ(*opt_float, *TestData::GetOptFloat(i)) << "index: " << i;
    } else {
      EXPECT_EQ(opt_float, TestData::GetOptFloat(i)) << "index: " << i;
    }
    if (opt_double && TestData::GetOptDouble(i)) {
      EXPECT_DOUBLE_EQ(*opt_double, *TestData::GetOptDouble(i)) << "index: " << i;
    } else {
      EXPECT_EQ(opt_double, TestData::GetOptDouble(i)) << "index: " << i;
    }
  }
  EXPECT_EQ(reader_.current_row(), TestData::num_rows);
  EXPECT_EQ(reader_.num_rows(), TestData::num_rows);
  EXPECT_EQ(i, TestData::num_rows);
}

TEST_F(TestOptionalFields, ReadOptionalFieldAsRequiredField) {
  /* Test that optional fields can be read using non-optional types
    _provided_ that the optional value is available.

    This can be useful if a schema is changed such that a required
    field beomes optional.  Applications can continue reading the
    field as if it were mandatory and do not need to be changed if the
    field value is always provided.

    Of course if the optional value is not present, then the read will
    fail by throwing an exception.  This is also tested below.
  */

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
    EXPECT_EQ(i, reader_.current_row());

    if (TestData::GetOptBool(i)) {
      reader_ >> b;
      EXPECT_EQ(b, *TestData::GetOptBool(i)) << "index: " << i;
    } else {
      EXPECT_THROW(reader_ >> b, ParquetException) << "index: " << i;
    }
    if (TestData::GetOptString(i)) {
      reader_ >> s;
      EXPECT_EQ(s, *TestData::GetOptString(i)) << "index: " << i;
    } else {
      EXPECT_THROW(reader_ >> s, ParquetException) << "index: " << i;
    }
    if (TestData::GetOptChar(i)) {
      reader_ >> c;
      EXPECT_EQ(c, *TestData::GetOptChar(i)) << "index: " << i;
    } else {
      EXPECT_THROW(reader_ >> c, ParquetException) << "index: " << i;
    }
    if (TestData::GetOptCharArray(i)) {
      reader_ >> char_array;
      EXPECT_EQ(char_array, *TestData::GetOptCharArray(i)) << "index: " << i;
    } else {
      EXPECT_THROW(reader_ >> char_array, ParquetException) << "index: " << i;
    }
    if (TestData::GetOptInt8(i)) {
      reader_ >> int8;
      EXPECT_EQ(int8, *TestData::GetOptInt8(i)) << "index: " << i;
    } else {
      EXPECT_THROW(reader_ >> int8, ParquetException) << "index: " << i;
    }
    if (TestData::GetOptUInt16(i)) {
      reader_ >> uint16;
      EXPECT_EQ(uint16, *TestData::GetOptUInt16(i)) << "index: " << i;
    } else {
      EXPECT_THROW(reader_ >> uint16, ParquetException) << "index: " << i;
    }
    if (TestData::GetOptInt32(i)) {
      reader_ >> int32;
      EXPECT_EQ(int32, *TestData::GetOptInt32(i)) << "index: " << i;
    } else {
      EXPECT_THROW(reader_ >> int32, ParquetException) << "index: " << i;
    }
    if (TestData::GetOptUInt64(i)) {
      reader_ >> uint64;
      EXPECT_EQ(uint64, *TestData::GetOptUInt64(i)) << "index: " << i;
    } else {
      EXPECT_THROW(reader_ >> uint64, ParquetException) << "index: " << i;
    }
    if (TestData::GetOptChronoMicroseconds(i)) {
      reader_ >> ts_us;
      EXPECT_EQ(ts_us, *TestData::GetOptChronoMicroseconds(i)) << "index: " << i;
    } else {
      EXPECT_THROW(reader_ >> ts_us, ParquetException) << "index: " << i;
    }
    if (TestData::GetOptFloat(i)) {
      reader_ >> f;
      EXPECT_FLOAT_EQ(f, *TestData::GetOptFloat(i)) << "index: " << i;
    } else {
      EXPECT_THROW(reader_ >> f, ParquetException) << "index: " << i;
    }
    if (TestData::GetOptDouble(i)) {
      reader_ >> d;
      EXPECT_DOUBLE_EQ(d, *TestData::GetOptDouble(i)) << "index: " << i;
    } else {
      EXPECT_THROW(reader_ >> d, ParquetException) << "index: " << i;
    }
    reader_ >> EndRow;
  }
}

class TestReadingDataFiles : public ::testing::Test {
 protected:
  std::string GetDataFile(const std::string& filename) const {
    return std::string(get_data_dir()) + "/" + filename;
  }
};

TEST_F(TestReadingDataFiles, AllTypesPlain) {
  PARQUET_ASSIGN_OR_THROW(auto infile, ::arrow::io::ReadableFile::Open(
                                           GetDataFile("alltypes_plain.parquet")));

  auto file_reader = ParquetFileReader::Open(infile);
  auto reader = StreamReader{std::move(file_reader)};

  int32_t c0;
  bool c1;
  int32_t c2;
  int32_t c3;
  int32_t c4;
  int64_t c5;
  float c6;
  double c7;
  std::string c8;
  std::string c9;

  const char* expected_date_str[] = {"03/01/09", "03/01/09", "04/01/09", "04/01/09",
                                     "02/01/09", "02/01/09", "01/01/09", "01/01/09"};
  int i;

  for (i = 0; !reader.eof(); ++i) {
    reader >> c0 >> c1 >> c2 >> c3 >> c4 >> c5;
    reader >> c6 >> c7;
    reader >> c8 >> c9;
    reader.SkipColumns(1);  // Skip column with unsupported 96-bit type
    reader >> EndRow;

    EXPECT_EQ(c1, (i & 1) == 0);
    EXPECT_EQ(c2, i & 1);
    EXPECT_EQ(c3, i & 1);
    EXPECT_EQ(c4, i & 1);
    EXPECT_EQ(c5, i & 1 ? 10 : 0);
    EXPECT_FLOAT_EQ(c6, i & 1 ? 1.1f : 0.f);
    EXPECT_DOUBLE_EQ(c7, i & 1 ? 10.1 : 0.);
    ASSERT_LT(static_cast<std::size_t>(i),
              sizeof(expected_date_str) / sizeof(expected_date_str[0]));
    EXPECT_EQ(c8, expected_date_str[i]);
    EXPECT_EQ(c9, i & 1 ? "1" : "0");
  }
  EXPECT_EQ(i, sizeof(expected_date_str) / sizeof(expected_date_str[0]));
}

TEST_F(TestReadingDataFiles, Int32Decimal) {
  PARQUET_ASSIGN_OR_THROW(
      auto infile, ::arrow::io::ReadableFile::Open(GetDataFile("int32_decimal.parquet")));

  auto file_reader = ParquetFileReader::Open(infile);
  auto reader = StreamReader{std::move(file_reader)};

  int32_t x;
  int i = 0;

  for (i = 1; !reader.eof(); ++i) {
    reader >> x >> EndRow;
    EXPECT_EQ(x, i * 100);
  }
  EXPECT_EQ(i, 25);
}

TEST_F(TestReadingDataFiles, Int64Decimal) {
  PARQUET_ASSIGN_OR_THROW(
      auto infile, ::arrow::io::ReadableFile::Open(GetDataFile("int64_decimal.parquet")));

  auto file_reader = ParquetFileReader::Open(infile);
  auto reader = StreamReader{std::move(file_reader)};

  int64_t x;
  int i = 0;

  for (i = 1; !reader.eof(); ++i) {
    reader >> x >> EndRow;
    EXPECT_EQ(x, i * 100);
  }
  EXPECT_EQ(i, 25);
}

TEST_F(TestReadingDataFiles, FLBADecimal) {
  PARQUET_ASSIGN_OR_THROW(auto infile, ::arrow::io::ReadableFile::Open(
                                           GetDataFile("fixed_length_decimal.parquet")));

  auto file_reader = ParquetFileReader::Open(infile);
  auto reader = StreamReader{std::move(file_reader)};

  ::arrow::Decimal128 x;
  int i = 0;

  for (i = 1; !reader.eof(); ++i) {
    reader >> x >> EndRow;
    EXPECT_EQ(x, ::arrow::Decimal128(i * 100));
  }
  EXPECT_EQ(i, 25);
}

TEST_F(TestReadingDataFiles, ByteArrayDecimal) {
  PARQUET_ASSIGN_OR_THROW(auto infile, ::arrow::io::ReadableFile::Open(
                                           GetDataFile("byte_array_decimal.parquet")));

  auto file_reader = ParquetFileReader::Open(infile);
  auto reader = StreamReader{std::move(file_reader)};

  ::arrow::Decimal128 x;
  int i = 0;

  for (i = 1; !reader.eof(); ++i) {
    reader >> x >> EndRow;
    EXPECT_EQ(x, ::arrow::Decimal128(i * 100));
  }
  EXPECT_EQ(i, 25);
}

}  // namespace test
}  // namespace parquet
