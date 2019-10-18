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

#include "arrow/io/api.h"
#include "parquet/exception.h"

namespace parquet {
namespace test {

class TestStreamWriter : public ::testing::Test {
 protected:
  void SetUp() {
    parquet::WriterProperties::Builder builder;

    builder.compression(parquet::Compression::BROTLI);

    auto file_writer = parquet::ParquetFileWriter::Open(CreateOutputStream(), GetSchema(),
                                                        builder.build());

    writer_ = StreamWriter{std::move(file_writer)};
  }

  void TearDown() { writer_ = StreamWriter{}; }

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
        "float_field", parquet::Repetition::REQUIRED, parquet::Type::FLOAT,
        parquet::ConvertedType::NONE));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "double_field", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
        parquet::ConvertedType::NONE));

    return std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED,
                                         fields));
  }

  StreamWriter writer_;
};

TEST_F(TestStreamWriter, DefaultConstructed) {
  StreamWriter os;

  // N.B. Default constructor objects are not usable.
  ASSERT_THROW(os << 4, ParquetException);
  ASSERT_THROW(os << "bad", ParquetException);
  ASSERT_THROW(os << EndRow, ParquetException);
  ASSERT_THROW(os << EndRowGroup, ParquetException);
}

TEST_F(TestStreamWriter, TypeChecking) {
  std::array<char, 3> char3_array = {'T', 'S', 'T'};
  std::array<char, 4> char4_array = {'T', 'E', 'S', 'T'};
  std::array<char, 5> char5_array = {'T', 'E', 'S', 'T', '2'};

  // Required type: bool
  ASSERT_THROW(writer_ << 4.5, ParquetException);
  ASSERT_NO_THROW(writer_ << true);

  // Required type: Variable length string.
  ASSERT_THROW(writer_ << 5, ParquetException);
  ASSERT_THROW(writer_ << char3_array, ParquetException);
  ASSERT_THROW(writer_ << char4_array, ParquetException);
  ASSERT_THROW(writer_ << char5_array, ParquetException);
  ASSERT_NO_THROW(writer_ << "ok");

  // Required type: A char.
  ASSERT_THROW(writer_ << "no good", ParquetException);
  ASSERT_NO_THROW(writer_ << 'K');

  // Required type: Fixed string of length 4
  ASSERT_THROW(writer_ << "bad", ParquetException);
  ASSERT_THROW(writer_ << char3_array, ParquetException);
  ASSERT_THROW(writer_ << char5_array, ParquetException);
  ASSERT_NO_THROW(writer_ << char4_array);

  // Required type: int8_t
  ASSERT_THROW(writer_ << false, ParquetException);
  ASSERT_NO_THROW(writer_ << int8_t(51));

  // Required type: uint16_t
  ASSERT_THROW(writer_ << int16_t(15), ParquetException);
  ASSERT_NO_THROW(writer_ << uint16_t(15));

  // Required type: int32_t
  ASSERT_THROW(writer_ << int16_t(99), ParquetException);
  ASSERT_NO_THROW(writer_ << int32_t(329487));

  // Required type: uint64_t
  ASSERT_THROW(writer_ << uint32_t(9832423), ParquetException);
  ASSERT_NO_THROW(writer_ << uint64_t((1ull << 60) + 123));

  // Required type: float
  ASSERT_THROW(writer_ << 5.4, ParquetException);
  ASSERT_NO_THROW(writer_ << 5.4f);

  // Required type: double
  ASSERT_THROW(writer_ << 5.4f, ParquetException);
  ASSERT_NO_THROW(writer_ << 5.4);

  ASSERT_NO_THROW(writer_ << EndRow);
}

TEST_F(TestStreamWriter, EndRow) {
  // Attempt #1 to end row prematurely.
  ASSERT_THROW(writer_ << EndRow, ParquetException);
  ASSERT_NO_THROW(writer_ << true);
  ASSERT_NO_THROW(writer_ << "eschatology");
  ASSERT_NO_THROW(writer_ << 'z');
  ASSERT_NO_THROW(writer_ << StreamWriter::FixedStringView("Test", 4));
  ASSERT_NO_THROW(writer_ << int8_t(51));
  ASSERT_NO_THROW(writer_ << uint16_t(15));
  // Attempt #2 to end row prematurely.
  ASSERT_THROW(writer_ << EndRow, ParquetException);
  ASSERT_NO_THROW(writer_ << int32_t(329487));
  ASSERT_NO_THROW(writer_ << uint64_t((1ull << 60) + 123));
  ASSERT_NO_THROW(writer_ << 25.4f);
  ASSERT_NO_THROW(writer_ << 3.3424);
  // Correct use of end row after all fields have been output.
  ASSERT_NO_THROW(writer_ << EndRow);
  // Attempt #3 to end row prematurely.
  ASSERT_THROW(writer_ << EndRow, ParquetException);
}

TEST_F(TestStreamWriter, EndRowGroup) {
  writer_.SetMaxRowGroupSize(0);

  // It's ok to end a row group multiple times.
  ASSERT_NO_THROW(writer_ << EndRowGroup);
  ASSERT_NO_THROW(writer_ << EndRowGroup);
  ASSERT_NO_THROW(writer_ << EndRowGroup);

  std::array<char, 4> char_array = {'A', 'B', 'C', 'D'};

  for (auto i = 0; i < 20000; ++i) {
    ASSERT_NO_THROW(writer_ << bool(i & 1));
    ASSERT_NO_THROW(writer_ << std::to_string(i));
    ASSERT_NO_THROW(writer_ << char(i % 26 + 'A'));
    // Rotate letters.
    {
      char tmp{char_array[0]};
      char_array[0] = char_array[3];
      char_array[3] = char_array[2];
      char_array[2] = char_array[1];
      char_array[1] = tmp;
    }
    ASSERT_NO_THROW(writer_ << char_array);
    ASSERT_NO_THROW(writer_ << int8_t(i & 0xff));
    ASSERT_NO_THROW(writer_ << uint16_t(7 * i));
    ASSERT_NO_THROW(writer_ << int32_t((1 << 30) - i * i));
    ASSERT_NO_THROW(writer_ << uint64_t((1ull << 60) - i * i));
    ASSERT_NO_THROW(writer_ << 42325.4f / float(i + 1));
    ASSERT_NO_THROW(writer_ << 3.2342e5 / double(i + 1));
    ASSERT_NO_THROW(writer_ << EndRow);

    if (i % 1000 == 0) {
      // It's ok to end a row group multiple times.
      ASSERT_NO_THROW(writer_ << EndRowGroup);
      ASSERT_NO_THROW(writer_ << EndRowGroup);
      ASSERT_NO_THROW(writer_ << EndRowGroup);
    }
  }
  // It's ok to end a row group multiple times.
  ASSERT_NO_THROW(writer_ << EndRowGroup);
  ASSERT_NO_THROW(writer_ << EndRowGroup);
  ASSERT_NO_THROW(writer_ << EndRowGroup);
}

}  // namespace test
}  // namespace parquet
