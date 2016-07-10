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
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/types/primitive.h"
#include "arrow/types/string.h"
#include "arrow/types/test-common.h"

namespace arrow {

class Buffer;

TEST(TypesTest, BinaryType) {
  BinaryType t1;
  BinaryType e1;
  StringType t2;
  EXPECT_TRUE(t1.Equals(&e1));
  EXPECT_FALSE(t1.Equals(&t2));
  ASSERT_EQ(t1.type, Type::BINARY);
  ASSERT_EQ(t1.ToString(), std::string("binary"));
}

TEST(TypesTest, TestStringType) {
  StringType str;
  ASSERT_EQ(str.type, Type::STRING);
  ASSERT_EQ(str.name(), std::string("string"));
}

// ----------------------------------------------------------------------
// String container

class TestStringContainer : public ::testing::Test {
 public:
  void SetUp() {
    chars_ = {'a', 'b', 'b', 'c', 'c', 'c'};
    offsets_ = {0, 1, 1, 1, 3, 6};
    valid_bytes_ = {1, 1, 0, 1, 1};
    expected_ = {"a", "", "", "bb", "ccc"};

    MakeArray();
  }

  void MakeArray() {
    length_ = offsets_.size() - 1;
    int nchars = chars_.size();

    value_buf_ = test::to_buffer(chars_);
    values_ = ArrayPtr(new UInt8Array(nchars, value_buf_));

    offsets_buf_ = test::to_buffer(offsets_);

    null_bitmap_ = test::bytes_to_null_buffer(valid_bytes_);
    null_count_ = test::null_count(valid_bytes_);

    strings_ = std::make_shared<StringArray>(
        length_, offsets_buf_, values_, null_count_, null_bitmap_);
  }

 protected:
  std::vector<int32_t> offsets_;
  std::vector<char> chars_;
  std::vector<uint8_t> valid_bytes_;

  std::vector<std::string> expected_;

  std::shared_ptr<Buffer> value_buf_;
  std::shared_ptr<Buffer> offsets_buf_;
  std::shared_ptr<Buffer> null_bitmap_;

  int null_count_;
  int length_;

  ArrayPtr values_;
  std::shared_ptr<StringArray> strings_;
};

TEST_F(TestStringContainer, TestArrayBasics) {
  ASSERT_EQ(length_, strings_->length());
  ASSERT_EQ(1, strings_->null_count());
  ASSERT_OK(strings_->Validate());
}

TEST_F(TestStringContainer, TestType) {
  TypePtr type = strings_->type();

  ASSERT_EQ(Type::STRING, type->type);
  ASSERT_EQ(Type::STRING, strings_->type_enum());
}

TEST_F(TestStringContainer, TestListFunctions) {
  int pos = 0;
  for (size_t i = 0; i < expected_.size(); ++i) {
    ASSERT_EQ(pos, strings_->value_offset(i));
    ASSERT_EQ(static_cast<int>(expected_[i].size()), strings_->value_length(i));
    pos += expected_[i].size();
  }
}

TEST_F(TestStringContainer, TestDestructor) {
  auto arr = std::make_shared<StringArray>(
      length_, offsets_buf_, values_, null_count_, null_bitmap_);
}

TEST_F(TestStringContainer, TestGetString) {
  for (size_t i = 0; i < expected_.size(); ++i) {
    if (valid_bytes_[i] == 0) {
      ASSERT_TRUE(strings_->IsNull(i));
    } else {
      ASSERT_EQ(expected_[i], strings_->GetString(i));
    }
  }
}

// ----------------------------------------------------------------------
// String builder tests

class TestStringBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();
    type_ = TypePtr(new StringType());
    builder_.reset(new StringBuilder(pool_, type_));
  }

  void Done() {
    result_ = std::dynamic_pointer_cast<StringArray>(builder_->Finish());
    result_->Validate();
  }

 protected:
  TypePtr type_;

  std::unique_ptr<StringBuilder> builder_;
  std::shared_ptr<StringArray> result_;
};

TEST_F(TestStringBuilder, TestScalarAppend) {
  std::vector<std::string> strings = {"", "bb", "a", "", "ccc"};
  std::vector<uint8_t> is_null = {0, 0, 0, 1, 0};

  int N = strings.size();
  int reps = 1000;

  for (int j = 0; j < reps; ++j) {
    for (int i = 0; i < N; ++i) {
      if (is_null[i]) {
        builder_->AppendNull();
      } else {
        builder_->Append(strings[i]);
      }
    }
  }
  Done();

  ASSERT_EQ(reps * N, result_->length());
  ASSERT_EQ(reps, result_->null_count());
  ASSERT_EQ(reps * 6, result_->values()->length());

  int32_t length;
  int32_t pos = 0;
  for (int i = 0; i < N * reps; ++i) {
    if (is_null[i % N]) {
      ASSERT_TRUE(result_->IsNull(i));
    } else {
      ASSERT_FALSE(result_->IsNull(i));
      result_->GetValue(i, &length);
      ASSERT_EQ(pos, result_->offset(i));
      ASSERT_EQ(static_cast<int>(strings[i % N].size()), length);
      ASSERT_EQ(strings[i % N], result_->GetString(i));

      pos += length;
    }
  }
}

TEST_F(TestStringBuilder, TestZeroLength) {
  // All buffers are null
  Done();
}

// Binary container type
// TODO(emkornfield) there should be some way to refactor these to avoid code duplicating
// with String
class TestBinaryContainer : public ::testing::Test {
 public:
  void SetUp() {
    chars_ = {'a', 'b', 'b', 'c', 'c', 'c'};
    offsets_ = {0, 1, 1, 1, 3, 6};
    valid_bytes_ = {1, 1, 0, 1, 1};
    expected_ = {"a", "", "", "bb", "ccc"};

    MakeArray();
  }

  void MakeArray() {
    length_ = offsets_.size() - 1;
    int nchars = chars_.size();

    value_buf_ = test::to_buffer(chars_);
    values_ = ArrayPtr(new UInt8Array(nchars, value_buf_));

    offsets_buf_ = test::to_buffer(offsets_);

    null_bitmap_ = test::bytes_to_null_buffer(valid_bytes_);
    null_count_ = test::null_count(valid_bytes_);

    strings_ = std::make_shared<BinaryArray>(
        length_, offsets_buf_, values_, null_count_, null_bitmap_);
  }

 protected:
  std::vector<int32_t> offsets_;
  std::vector<char> chars_;
  std::vector<uint8_t> valid_bytes_;

  std::vector<std::string> expected_;

  std::shared_ptr<Buffer> value_buf_;
  std::shared_ptr<Buffer> offsets_buf_;
  std::shared_ptr<Buffer> null_bitmap_;

  int null_count_;
  int length_;

  ArrayPtr values_;
  std::shared_ptr<BinaryArray> strings_;
};

TEST_F(TestBinaryContainer, TestArrayBasics) {
  ASSERT_EQ(length_, strings_->length());
  ASSERT_EQ(1, strings_->null_count());
  ASSERT_OK(strings_->Validate());
}

TEST_F(TestBinaryContainer, TestType) {
  TypePtr type = strings_->type();

  ASSERT_EQ(Type::BINARY, type->type);
  ASSERT_EQ(Type::BINARY, strings_->type_enum());
}

TEST_F(TestBinaryContainer, TestListFunctions) {
  int pos = 0;
  for (size_t i = 0; i < expected_.size(); ++i) {
    ASSERT_EQ(pos, strings_->value_offset(i));
    ASSERT_EQ(static_cast<int>(expected_[i].size()), strings_->value_length(i));
    pos += expected_[i].size();
  }
}

TEST_F(TestBinaryContainer, TestDestructor) {
  auto arr = std::make_shared<BinaryArray>(
      length_, offsets_buf_, values_, null_count_, null_bitmap_);
}

TEST_F(TestBinaryContainer, TestGetValue) {
  for (size_t i = 0; i < expected_.size(); ++i) {
    if (valid_bytes_[i] == 0) {
      ASSERT_TRUE(strings_->IsNull(i));
    } else {
      int32_t len = -1;
      const uint8_t* bytes = strings_->GetValue(i, &len);
      ASSERT_EQ(0, std::memcmp(expected_[i].data(), bytes, len));
    }
  }
}

class TestBinaryBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();
    type_ = TypePtr(new BinaryType());
    builder_.reset(new BinaryBuilder(pool_, type_));
  }

  void Done() {
    result_ = std::dynamic_pointer_cast<BinaryArray>(builder_->Finish());
    result_->Validate();
  }

 protected:
  TypePtr type_;

  std::unique_ptr<BinaryBuilder> builder_;
  std::shared_ptr<BinaryArray> result_;
};

TEST_F(TestBinaryBuilder, TestScalarAppend) {
  std::vector<std::string> strings = {"", "bb", "a", "", "ccc"};
  std::vector<uint8_t> is_null = {0, 0, 0, 1, 0};

  int N = strings.size();
  int reps = 1000;

  for (int j = 0; j < reps; ++j) {
    for (int i = 0; i < N; ++i) {
      if (is_null[i]) {
        builder_->AppendNull();
      } else {
        builder_->Append(
            reinterpret_cast<const uint8_t*>(strings[i].data()), strings[i].size());
      }
    }
  }
  Done();
  ASSERT_OK(result_->Validate());
  ASSERT_EQ(reps * N, result_->length());
  ASSERT_EQ(reps, result_->null_count());
  ASSERT_EQ(reps * 6, result_->values()->length());

  int32_t length;
  for (int i = 0; i < N * reps; ++i) {
    if (is_null[i % N]) {
      ASSERT_TRUE(result_->IsNull(i));
    } else {
      ASSERT_FALSE(result_->IsNull(i));
      const uint8_t* vals = result_->GetValue(i, &length);
      ASSERT_EQ(static_cast<int>(strings[i % N].size()), length);
      ASSERT_EQ(0, std::memcmp(vals, strings[i % N].data(), length));
    }
  }
}

TEST_F(TestBinaryBuilder, TestZeroLength) {
  // All buffers are null
  Done();
}

}  // namespace arrow
