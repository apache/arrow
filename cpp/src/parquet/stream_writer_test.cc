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

#include "parquet/stream_writer.h"

#include <fcntl.h>
#include <gtest/gtest.h>

#include <memory>
#include <utility>

#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "parquet/exception.h"

namespace parquet {
namespace test {

template <typename T>
using optional = StreamWriter::optional<T>;

using char4_array_type = std::array<char, 4>;

class TestStreamWriter : public ::testing::Test {
 protected:
  const char* GetDataFile() const { return "stream_writer_test.parquet"; }

  void SetUp() {
    writer_ = StreamWriter{ParquetFileWriter::Open(CreateOutputStream(), GetSchema())};
  }

  void TearDown() { writer_ = StreamWriter{}; }

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

    fields.push_back(schema::PrimitiveNode::Make("float_field", Repetition::REQUIRED,
                                                 Type::FLOAT, ConvertedType::NONE));

    fields.push_back(schema::PrimitiveNode::Make("double_field", Repetition::REQUIRED,
                                                 Type::DOUBLE, ConvertedType::NONE));

    return std::static_pointer_cast<schema::GroupNode>(
        schema::GroupNode::Make("schema", Repetition::REQUIRED, fields));
  }

  StreamWriter writer_;
};

TEST_F(TestStreamWriter, DefaultConstructed) {
  StreamWriter os;

  // Default constructor objects are not usable for writing data.
  EXPECT_THROW(os << 4, ParquetException);
  EXPECT_THROW(os << "bad", ParquetException);
  EXPECT_THROW(os << EndRow, ParquetException);
  EXPECT_THROW(os << EndRowGroup, ParquetException);

  EXPECT_EQ(0, os.current_column());
  EXPECT_EQ(0, os.current_row());
  EXPECT_EQ(0, os.num_columns());
  EXPECT_EQ(0, os.SkipColumns(10));
}

TEST_F(TestStreamWriter, TypeChecking) {
  std::array<char, 3> char3_array = {'T', 'S', 'T'};
  std::array<char, 4> char4_array = {'T', 'E', 'S', 'T'};
  std::array<char, 5> char5_array = {'T', 'E', 'S', 'T', '2'};

  // Required type: bool
  EXPECT_EQ(0, writer_.current_column());
  EXPECT_THROW(writer_ << 4.5, ParquetException);
  EXPECT_NO_THROW(writer_ << true);

  // Required type: Variable length string.
  EXPECT_EQ(1, writer_.current_column());
  EXPECT_THROW(writer_ << 5, ParquetException);
  EXPECT_THROW(writer_ << char3_array, ParquetException);
  EXPECT_THROW(writer_ << char4_array, ParquetException);
  EXPECT_THROW(writer_ << char5_array, ParquetException);
  EXPECT_NO_THROW(writer_ << "ok");

  // Required type: A char.
  EXPECT_EQ(2, writer_.current_column());
  EXPECT_THROW(writer_ << "no good", ParquetException);
  EXPECT_NO_THROW(writer_ << 'K');

  // Required type: Fixed string of length 4
  EXPECT_EQ(3, writer_.current_column());
  EXPECT_THROW(writer_ << "bad", ParquetException);
  EXPECT_THROW(writer_ << char3_array, ParquetException);
  EXPECT_THROW(writer_ << char5_array, ParquetException);
  EXPECT_NO_THROW(writer_ << char4_array);

  // Required type: int8_t
  EXPECT_EQ(4, writer_.current_column());
  EXPECT_THROW(writer_ << false, ParquetException);
  EXPECT_NO_THROW(writer_ << int8_t(51));

  // Required type: uint16_t
  EXPECT_EQ(5, writer_.current_column());
  EXPECT_THROW(writer_ << int16_t(15), ParquetException);
  EXPECT_NO_THROW(writer_ << uint16_t(15));

  // Required type: int32_t
  EXPECT_EQ(6, writer_.current_column());
  EXPECT_THROW(writer_ << int16_t(99), ParquetException);
  EXPECT_NO_THROW(writer_ << int32_t(329487));

  // Required type: uint64_t
  EXPECT_EQ(7, writer_.current_column());
  EXPECT_THROW(writer_ << uint32_t(9832423), ParquetException);
  EXPECT_NO_THROW(writer_ << uint64_t((1ull << 60) + 123));

  // Required type: float
  EXPECT_EQ(8, writer_.current_column());
  EXPECT_THROW(writer_ << 5.4, ParquetException);
  EXPECT_NO_THROW(writer_ << 5.4f);

  // Required type: double
  EXPECT_EQ(9, writer_.current_column());
  EXPECT_THROW(writer_ << 5.4f, ParquetException);
  EXPECT_NO_THROW(writer_ << 5.4);

  EXPECT_EQ(0, writer_.current_row());
  EXPECT_NO_THROW(writer_ << EndRow);
  EXPECT_EQ(1, writer_.current_row());
}

TEST_F(TestStreamWriter, RequiredFieldChecking) {
  char4_array_type char4_array = {'T', 'E', 'S', 'T'};

  // Required field of type: bool
  EXPECT_THROW(writer_ << optional<bool>(), ParquetException);
  EXPECT_NO_THROW(writer_ << optional<bool>(true));

  // Required field of type: Variable length string.
  EXPECT_THROW(writer_ << optional<std::string>(), ParquetException);
  EXPECT_NO_THROW(writer_ << optional<std::string>("ok"));

  // Required field of type: A char.
  EXPECT_THROW(writer_ << optional<char>(), ParquetException);
  EXPECT_NO_THROW(writer_ << optional<char>('K'));

  // Required field of type: Fixed string of length 4
  EXPECT_THROW(writer_ << optional<char4_array_type>(), ParquetException);
  EXPECT_NO_THROW(writer_ << optional<char4_array_type>(char4_array));

  // Required field of type: int8_t
  EXPECT_THROW(writer_ << optional<int8_t>(), ParquetException);
  EXPECT_NO_THROW(writer_ << optional<int8_t>(51));

  // Required field of type: uint16_t
  EXPECT_THROW(writer_ << optional<uint16_t>(), ParquetException);
  EXPECT_NO_THROW(writer_ << optional<uint16_t>(15));

  // Required field of type: int32_t
  EXPECT_THROW(writer_ << optional<int32_t>(), ParquetException);
  EXPECT_NO_THROW(writer_ << optional<int32_t>(329487));

  // Required field of type: uint64_t
  EXPECT_THROW(writer_ << optional<uint64_t>(), ParquetException);
  EXPECT_NO_THROW(writer_ << optional<uint64_t>((1ull << 60) + 123));

  // Required field of type: float
  EXPECT_THROW(writer_ << optional<float>(), ParquetException);
  EXPECT_NO_THROW(writer_ << optional<float>(5.4f));

  // Required field of type: double
  EXPECT_THROW(writer_ << optional<double>(), ParquetException);
  EXPECT_NO_THROW(writer_ << optional<double>(5.4));

  EXPECT_NO_THROW(writer_ << EndRow);
}

TEST_F(TestStreamWriter, EndRow) {
  // Attempt #1 to end row prematurely.
  EXPECT_EQ(0, writer_.current_row());
  EXPECT_THROW(writer_ << EndRow, ParquetException);
  EXPECT_EQ(0, writer_.current_row());

  EXPECT_NO_THROW(writer_ << true);
  EXPECT_NO_THROW(writer_ << "eschatology");
  EXPECT_NO_THROW(writer_ << 'z');
  EXPECT_NO_THROW(writer_ << StreamWriter::FixedStringView("Test", 4));
  EXPECT_NO_THROW(writer_ << int8_t(51));
  EXPECT_NO_THROW(writer_ << uint16_t(15));

  // Attempt #2 to end row prematurely.
  EXPECT_THROW(writer_ << EndRow, ParquetException);
  EXPECT_EQ(0, writer_.current_row());

  EXPECT_NO_THROW(writer_ << int32_t(329487));
  EXPECT_NO_THROW(writer_ << uint64_t((1ull << 60) + 123));
  EXPECT_NO_THROW(writer_ << 25.4f);
  EXPECT_NO_THROW(writer_ << 3.3424);
  // Correct use of end row after all fields have been output.
  EXPECT_NO_THROW(writer_ << EndRow);
  EXPECT_EQ(1, writer_.current_row());

  // Attempt #3 to end row prematurely.
  EXPECT_THROW(writer_ << EndRow, ParquetException);
  EXPECT_EQ(1, writer_.current_row());
}

TEST_F(TestStreamWriter, EndRowGroup) {
  writer_.SetMaxRowGroupSize(0);

  // It's ok to end a row group multiple times.
  EXPECT_NO_THROW(writer_ << EndRowGroup);
  EXPECT_NO_THROW(writer_ << EndRowGroup);
  EXPECT_NO_THROW(writer_ << EndRowGroup);

  std::array<char, 4> char_array = {'A', 'B', 'C', 'D'};

  for (int i = 0; i < 20000; ++i) {
    EXPECT_NO_THROW(writer_ << bool(i & 1)) << "index: " << i;
    EXPECT_NO_THROW(writer_ << std::to_string(i)) << "index: " << i;
    EXPECT_NO_THROW(writer_ << char(i % 26 + 'A')) << "index: " << i;
    // Rotate letters.
    {
      char tmp{char_array[0]};
      char_array[0] = char_array[3];
      char_array[3] = char_array[2];
      char_array[2] = char_array[1];
      char_array[1] = tmp;
    }
    EXPECT_NO_THROW(writer_ << char_array) << "index: " << i;
    EXPECT_NO_THROW(writer_ << int8_t(i & 0xff)) << "index: " << i;
    EXPECT_NO_THROW(writer_ << uint16_t(7 * i)) << "index: " << i;
    EXPECT_NO_THROW(writer_ << int32_t((1 << 30) - i * i)) << "index: " << i;
    EXPECT_NO_THROW(writer_ << uint64_t((1ull << 60) - i * i)) << "index: " << i;
    EXPECT_NO_THROW(writer_ << 42325.4f / float(i + 1)) << "index: " << i;
    EXPECT_NO_THROW(writer_ << 3.2342e5 / double(i + 1)) << "index: " << i;
    EXPECT_NO_THROW(writer_ << EndRow) << "index: " << i;

    if (i % 1000 == 0) {
      // It's ok to end a row group multiple times.
      EXPECT_NO_THROW(writer_ << EndRowGroup);
      EXPECT_NO_THROW(writer_ << EndRowGroup);
      EXPECT_NO_THROW(writer_ << EndRowGroup);
    }
  }
  // It's ok to end a row group multiple times.
  EXPECT_NO_THROW(writer_ << EndRowGroup);
  EXPECT_NO_THROW(writer_ << EndRowGroup);
  EXPECT_NO_THROW(writer_ << EndRowGroup);
}

TEST_F(TestStreamWriter, SkipColumns) {
  EXPECT_EQ(0, writer_.SkipColumns(0));
  EXPECT_THROW(writer_.SkipColumns(2), ParquetException);
  writer_ << true << std::string("Cannot skip mandatory columns");
  EXPECT_THROW(writer_.SkipColumns(1), ParquetException);
  writer_ << 'x' << std::array<char, 4>{'A', 'B', 'C', 'D'} << int8_t(2) << uint16_t(3)
          << int32_t(4) << uint64_t(5) << 6.0f << 7.0;
  writer_ << EndRow;
}

TEST_F(TestStreamWriter, AppendNotImplemented) {
  PARQUET_ASSIGN_OR_THROW(auto outfile,
                          ::arrow::io::FileOutputStream::Open(GetDataFile()));

  writer_ = StreamWriter{ParquetFileWriter::Open(outfile, GetSchema())};
  writer_ << false << std::string("Just one row") << 'x'
          << std::array<char, 4>{'A', 'B', 'C', 'D'} << int8_t(2) << uint16_t(3)
          << int32_t(4) << uint64_t(5) << 6.0f << 7.0;
  writer_ << EndRow;
  writer_ = StreamWriter{};

  // Re-open file in append mode.
  PARQUET_ASSIGN_OR_THROW(outfile,
                          ::arrow::io::FileOutputStream::Open(GetDataFile(), true));

  EXPECT_THROW(ParquetFileWriter::Open(outfile, GetSchema()), ParquetException);
}  // namespace test

class TestOptionalFields : public ::testing::Test {
 protected:
  void SetUp() {
    writer_ = StreamWriter{ParquetFileWriter::Open(CreateOutputStream(), GetSchema())};
  }

  void TearDown() { writer_ = StreamWriter{}; }

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

    fields.push_back(schema::PrimitiveNode::Make("float_field", Repetition::OPTIONAL,
                                                 Type::FLOAT, ConvertedType::NONE));

    fields.push_back(schema::PrimitiveNode::Make("double_field", Repetition::OPTIONAL,
                                                 Type::DOUBLE, ConvertedType::NONE));

    return std::static_pointer_cast<schema::GroupNode>(
        schema::GroupNode::Make("schema", Repetition::REQUIRED, fields));
  }

  StreamWriter writer_;
};

TEST_F(TestOptionalFields, OutputOperatorWithOptionalT) {
  for (int i = 0; i < 100; ++i) {
    // Write optional fields using operator<<(optional<T>).  Writing
    // of a value is skipped every 9 rows by using a optional<T>
    // object without a value.

    if (i % 9 == 0) {
      writer_ << optional<bool>() << optional<std::string>() << optional<char>()
              << optional<char4_array_type>() << optional<int8_t>()
              << optional<uint16_t>() << optional<int32_t>() << optional<uint64_t>()
              << optional<float>() << optional<double>();
    } else {
      writer_ << bool(i & 1) << optional<std::string>("#" + std::to_string(i))
              << optional<char>('A' + i % 26)
              << optional<char4_array_type>{{'F', 'O', 'O', 0}} << optional<int8_t>(i)
              << optional<uint16_t>(0xffff - i) << optional<int32_t>(0x7fffffff - 3 * i)
              << optional<uint64_t>((1ull << 60) + i) << optional<float>(5.4f * i)
              << optional<double>(5.1322e6 * i);
    }
    EXPECT_NO_THROW(writer_ << EndRow);
  }
}

TEST_F(TestOptionalFields, OutputOperatorTAndSkipColumns) {
  constexpr int num_rows = 100;

  EXPECT_EQ(0, writer_.current_row());

  for (int i = 0; i < num_rows; ++i) {
    // Write optional fields using standard operator<<(T).  Writing of
    // a value is skipped every 9 rows by using SkipColumns().

    EXPECT_EQ(0, writer_.current_column());
    EXPECT_EQ(i, writer_.current_row());

    if (i % 9 == 0) {
      EXPECT_EQ(writer_.num_columns(), writer_.SkipColumns(writer_.num_columns() + 99))
          << "index: " << i;
    } else {
      writer_ << bool(i & 1) << std::string("ok") << char('A' + i % 26)
              << char4_array_type{{'S', 'K', 'I', 'P'}} << int8_t(i)
              << uint16_t(0xffff - i) << int32_t(0x7fffffff - 3 * i)
              << uint64_t((1ull << 60) + i) << 5.4f * i << 5.1322e6 * i;
    }
    EXPECT_EQ(writer_.num_columns(), writer_.current_column()) << "index: " << i;
    EXPECT_NO_THROW(writer_ << EndRow) << "index: " << i;
  }
  EXPECT_EQ(num_rows, writer_.current_row());
}

}  // namespace test
}  // namespace parquet
