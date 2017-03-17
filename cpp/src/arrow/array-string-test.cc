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
#include "arrow/builder.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"

namespace arrow {

class Buffer;

// ----------------------------------------------------------------------
// String container

class TestStringArray : public ::testing::Test {
 public:
  void SetUp() {
    chars_ = {'a', 'b', 'b', 'c', 'c', 'c'};
    offsets_ = {0, 1, 1, 1, 3, 6};
    valid_bytes_ = {1, 1, 0, 1, 1};
    expected_ = {"a", "", "", "bb", "ccc"};

    MakeArray();
  }

  void MakeArray() {
    length_ = static_cast<int64_t>(offsets_.size()) - 1;
    value_buf_ = test::GetBufferFromVector(chars_);
    offsets_buf_ = test::GetBufferFromVector(offsets_);
    null_bitmap_ = test::bytes_to_null_buffer(valid_bytes_);
    null_count_ = test::null_count(valid_bytes_);

    strings_ = std::make_shared<StringArray>(
        length_, offsets_buf_, value_buf_, null_bitmap_, null_count_);
  }

 protected:
  std::vector<int32_t> offsets_;
  std::vector<char> chars_;
  std::vector<uint8_t> valid_bytes_;

  std::vector<std::string> expected_;

  std::shared_ptr<Buffer> value_buf_;
  std::shared_ptr<Buffer> offsets_buf_;
  std::shared_ptr<Buffer> null_bitmap_;

  int64_t null_count_;
  int64_t length_;

  std::shared_ptr<StringArray> strings_;
};

TEST_F(TestStringArray, TestArrayBasics) {
  ASSERT_EQ(length_, strings_->length());
  ASSERT_EQ(1, strings_->null_count());
  ASSERT_OK(strings_->Validate());
}

TEST_F(TestStringArray, TestType) {
  TypePtr type = strings_->type();

  ASSERT_EQ(Type::STRING, type->type);
  ASSERT_EQ(Type::STRING, strings_->type_enum());
}

TEST_F(TestStringArray, TestListFunctions) {
  int pos = 0;
  for (size_t i = 0; i < expected_.size(); ++i) {
    ASSERT_EQ(pos, strings_->value_offset(i));
    ASSERT_EQ(static_cast<int>(expected_[i].size()), strings_->value_length(i));
    pos += static_cast<int>(expected_[i].size());
  }
}

TEST_F(TestStringArray, TestDestructor) {
  auto arr = std::make_shared<StringArray>(
      length_, offsets_buf_, value_buf_, null_bitmap_, null_count_);
}

TEST_F(TestStringArray, TestGetString) {
  for (size_t i = 0; i < expected_.size(); ++i) {
    if (valid_bytes_[i] == 0) {
      ASSERT_TRUE(strings_->IsNull(i));
    } else {
      ASSERT_EQ(expected_[i], strings_->GetString(i));
    }
  }
}

TEST_F(TestStringArray, TestEmptyStringComparison) {
  offsets_ = {0, 0, 0, 0, 0, 0};
  offsets_buf_ = test::GetBufferFromVector(offsets_);
  length_ = static_cast<int64_t>(offsets_.size() - 1);

  auto strings_a = std::make_shared<StringArray>(
      length_, offsets_buf_, nullptr, null_bitmap_, null_count_);
  auto strings_b = std::make_shared<StringArray>(
      length_, offsets_buf_, nullptr, null_bitmap_, null_count_);
  ASSERT_TRUE(strings_a->Equals(strings_b));
}

TEST_F(TestStringArray, CompareNullByteSlots) {
  StringBuilder builder(default_memory_pool());
  StringBuilder builder2(default_memory_pool());
  StringBuilder builder3(default_memory_pool());

  builder.Append("foo");
  builder2.Append("foo");
  builder3.Append("foo");

  builder.Append("bar");
  builder2.AppendNull();

  // same length, but different
  builder3.Append("xyz");

  builder.Append("baz");
  builder2.Append("baz");
  builder3.Append("baz");

  std::shared_ptr<Array> array, array2, array3;
  ASSERT_OK(builder.Finish(&array));
  ASSERT_OK(builder2.Finish(&array2));
  ASSERT_OK(builder3.Finish(&array3));

  const auto& a1 = static_cast<const StringArray&>(*array);
  const auto& a2 = static_cast<const StringArray&>(*array2);
  const auto& a3 = static_cast<const StringArray&>(*array3);

  // The validity bitmaps are the same, the data is different, but the unequal
  // portion is masked out
  StringArray equal_array(3, a1.value_offsets(), a1.data(), a2.null_bitmap(), 1);
  StringArray equal_array2(3, a3.value_offsets(), a3.data(), a2.null_bitmap(), 1);

  ASSERT_TRUE(equal_array.Equals(equal_array2));
  ASSERT_TRUE(a2.RangeEquals(equal_array2, 0, 3, 0));

  ASSERT_TRUE(equal_array.Array::Slice(1)->Equals(equal_array2.Array::Slice(1)));
  ASSERT_TRUE(
      equal_array.Array::Slice(1)->RangeEquals(0, 2, 0, equal_array2.Array::Slice(1)));
}

TEST_F(TestStringArray, TestSliceGetString) {
  StringBuilder builder(default_memory_pool());

  builder.Append("a");
  builder.Append("b");
  builder.Append("c");

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));
  auto s = array->Slice(1, 10);
  auto arr = std::dynamic_pointer_cast<StringArray>(s);
  ASSERT_EQ(arr->GetString(0), "b");
}

// ----------------------------------------------------------------------
// String builder tests

class TestStringBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();
    builder_.reset(new StringBuilder(pool_));
  }

  void Done() {
    std::shared_ptr<Array> out;
    EXPECT_OK(builder_->Finish(&out));

    result_ = std::dynamic_pointer_cast<StringArray>(out);
    result_->Validate();
  }

 protected:
  std::unique_ptr<StringBuilder> builder_;
  std::shared_ptr<StringArray> result_;
};

TEST_F(TestStringBuilder, TestScalarAppend) {
  std::vector<std::string> strings = {"", "bb", "a", "", "ccc"};
  std::vector<uint8_t> is_null = {0, 0, 0, 1, 0};

  int N = static_cast<int>(strings.size());
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
  ASSERT_EQ(reps * 6, result_->data()->size());

  int32_t length;
  int32_t pos = 0;
  for (int i = 0; i < N * reps; ++i) {
    if (is_null[i % N]) {
      ASSERT_TRUE(result_->IsNull(i));
    } else {
      ASSERT_FALSE(result_->IsNull(i));
      result_->GetValue(i, &length);
      ASSERT_EQ(pos, result_->value_offset(i));
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
class TestBinaryArray : public ::testing::Test {
 public:
  void SetUp() {
    chars_ = {'a', 'b', 'b', 'c', 'c', 'c'};
    offsets_ = {0, 1, 1, 1, 3, 6};
    valid_bytes_ = {1, 1, 0, 1, 1};
    expected_ = {"a", "", "", "bb", "ccc"};

    MakeArray();
  }

  void MakeArray() {
    length_ = static_cast<int64_t>(offsets_.size() - 1);
    value_buf_ = test::GetBufferFromVector(chars_);
    offsets_buf_ = test::GetBufferFromVector(offsets_);

    null_bitmap_ = test::bytes_to_null_buffer(valid_bytes_);
    null_count_ = test::null_count(valid_bytes_);

    strings_ = std::make_shared<BinaryArray>(
        length_, offsets_buf_, value_buf_, null_bitmap_, null_count_);
  }

 protected:
  std::vector<int32_t> offsets_;
  std::vector<char> chars_;
  std::vector<uint8_t> valid_bytes_;

  std::vector<std::string> expected_;

  std::shared_ptr<Buffer> value_buf_;
  std::shared_ptr<Buffer> offsets_buf_;
  std::shared_ptr<Buffer> null_bitmap_;

  int64_t null_count_;
  int64_t length_;

  std::shared_ptr<BinaryArray> strings_;
};

TEST_F(TestBinaryArray, TestArrayBasics) {
  ASSERT_EQ(length_, strings_->length());
  ASSERT_EQ(1, strings_->null_count());
  ASSERT_OK(strings_->Validate());
}

TEST_F(TestBinaryArray, TestType) {
  TypePtr type = strings_->type();

  ASSERT_EQ(Type::BINARY, type->type);
  ASSERT_EQ(Type::BINARY, strings_->type_enum());
}

TEST_F(TestBinaryArray, TestListFunctions) {
  size_t pos = 0;
  for (size_t i = 0; i < expected_.size(); ++i) {
    ASSERT_EQ(pos, strings_->value_offset(i));
    ASSERT_EQ(static_cast<int>(expected_[i].size()), strings_->value_length(i));
    pos += expected_[i].size();
  }
}

TEST_F(TestBinaryArray, TestDestructor) {
  auto arr = std::make_shared<BinaryArray>(
      length_, offsets_buf_, value_buf_, null_bitmap_, null_count_);
}

TEST_F(TestBinaryArray, TestGetValue) {
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

TEST_F(TestBinaryArray, TestEqualsEmptyStrings) {
  BinaryBuilder builder(default_memory_pool(), arrow::binary());

  std::string empty_string("");

  builder.Append(empty_string);
  builder.Append(empty_string);
  builder.Append(empty_string);
  builder.Append(empty_string);
  builder.Append(empty_string);

  std::shared_ptr<Array> left_arr;
  ASSERT_OK(builder.Finish(&left_arr));

  const BinaryArray& left = static_cast<const BinaryArray&>(*left_arr);
  std::shared_ptr<Array> right = std::make_shared<BinaryArray>(left.length(),
      left.value_offsets(), nullptr, left.null_bitmap(), left.null_count());

  ASSERT_TRUE(left.Equals(right));
  ASSERT_TRUE(left.RangeEquals(0, left.length(), 0, right));
}

class TestBinaryBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();
    builder_.reset(new BinaryBuilder(pool_));
  }

  void Done() {
    std::shared_ptr<Array> out;
    EXPECT_OK(builder_->Finish(&out));

    result_ = std::dynamic_pointer_cast<BinaryArray>(out);
    result_->Validate();
  }

 protected:
  std::unique_ptr<BinaryBuilder> builder_;
  std::shared_ptr<BinaryArray> result_;
};

TEST_F(TestBinaryBuilder, TestScalarAppend) {
  std::vector<std::string> strings = {"", "bb", "a", "", "ccc"};
  std::vector<uint8_t> is_null = {0, 0, 0, 1, 0};

  int N = static_cast<int>(strings.size());
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
  ASSERT_OK(result_->Validate());
  ASSERT_EQ(reps * N, result_->length());
  ASSERT_EQ(reps, result_->null_count());
  ASSERT_EQ(reps * 6, result_->data()->size());

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

// ----------------------------------------------------------------------
// Slice tests

template <typename TYPE>
void CheckSliceEquality() {
  using Traits = TypeTraits<TYPE>;
  using BuilderType = typename Traits::BuilderType;

  BuilderType builder(default_memory_pool());

  std::vector<std::string> strings = {"foo", "", "bar", "baz", "qux", ""};
  std::vector<uint8_t> is_null = {0, 1, 0, 1, 0, 0};

  int N = static_cast<int>(strings.size());
  int reps = 10;

  for (int j = 0; j < reps; ++j) {
    for (int i = 0; i < N; ++i) {
      if (is_null[i]) {
        builder.AppendNull();
      } else {
        builder.Append(strings[i]);
      }
    }
  }

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));

  std::shared_ptr<Array> slice, slice2;

  slice = array->Slice(5);
  slice2 = array->Slice(5);
  ASSERT_EQ(N * reps - 5, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(5, slice->length(), 0, slice));

  // Chained slices
  slice2 = array->Slice(2)->Slice(3);
  ASSERT_TRUE(slice->Equals(slice2));

  slice = array->Slice(5, 20);
  slice2 = array->Slice(5, 20);
  ASSERT_EQ(20, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(5, 25, 0, slice));
}

TEST_F(TestBinaryArray, TestSliceEquality) {
  CheckSliceEquality<BinaryType>();
}

TEST_F(TestStringArray, TestSliceEquality) {
  CheckSliceEquality<BinaryType>();
}

TEST_F(TestBinaryArray, LengthZeroCtor) {
  BinaryArray array(0, nullptr, nullptr);
}

// ----------------------------------------------------------------------
// FixedWidthBinary tests

class TestFWBinaryArray : public ::testing::Test {
 public:
  void SetUp() {}

  void InitBuilder(int byte_width) {
    auto type = fixed_width_binary(byte_width);
    builder_.reset(new FixedWidthBinaryBuilder(default_memory_pool(), type));
  }

 protected:
  std::unique_ptr<FixedWidthBinaryBuilder> builder_;
};

TEST_F(TestFWBinaryArray, Builder) {
  const int32_t byte_width = 10;
  int64_t length = 4096;

  int64_t nbytes = length * byte_width;

  std::vector<uint8_t> data(nbytes);
  test::random_bytes(nbytes, 0, data.data());

  std::vector<uint8_t> is_valid(length);
  test::random_null_bytes(length, 0.1, is_valid.data());

  const uint8_t* raw_data = data.data();

  std::shared_ptr<Array> result;

  auto CheckResult = [this, &length, &is_valid, &raw_data, &byte_width](
      const Array& result) {
    // Verify output
    const auto& fw_result = static_cast<const FixedWidthBinaryArray&>(result);

    ASSERT_EQ(length, result.length());

    for (int64_t i = 0; i < result.length(); ++i) {
      if (is_valid[i]) {
        ASSERT_EQ(
            0, memcmp(raw_data + byte_width * i, fw_result.GetValue(i), byte_width));
      } else {
        ASSERT_TRUE(fw_result.IsNull(i));
      }
    }
  };

  // Build using iterative API
  InitBuilder(byte_width);
  for (int64_t i = 0; i < length; ++i) {
    if (is_valid[i]) {
      builder_->Append(raw_data + byte_width * i);
    } else {
      builder_->AppendNull();
    }
  }

  ASSERT_OK(builder_->Finish(&result));
  CheckResult(*result);

  // Build using batch API
  InitBuilder(byte_width);

  const uint8_t* raw_is_valid = is_valid.data();

  ASSERT_OK(builder_->Append(raw_data, 50, raw_is_valid));
  ASSERT_OK(builder_->Append(raw_data + 50 * byte_width, length - 50, raw_is_valid + 50));
  ASSERT_OK(builder_->Finish(&result));
  CheckResult(*result);

  // Build from std::string
  InitBuilder(byte_width);
  for (int64_t i = 0; i < length; ++i) {
    if (is_valid[i]) {
      builder_->Append(std::string(
          reinterpret_cast<const char*>(raw_data + byte_width * i), byte_width));
    } else {
      builder_->AppendNull();
    }
  }

  ASSERT_OK(builder_->Finish(&result));
  CheckResult(*result);
}

TEST_F(TestFWBinaryArray, EqualsRangeEquals) {
  // Check that we don't compare data in null slots

  auto type = fixed_width_binary(4);
  FixedWidthBinaryBuilder builder1(default_memory_pool(), type);
  FixedWidthBinaryBuilder builder2(default_memory_pool(), type);

  ASSERT_OK(builder1.Append("foo1"));
  ASSERT_OK(builder1.AppendNull());

  ASSERT_OK(builder2.Append("foo1"));
  ASSERT_OK(builder2.Append("foo2"));

  std::shared_ptr<Array> array1, array2;
  ASSERT_OK(builder1.Finish(&array1));
  ASSERT_OK(builder2.Finish(&array2));

  const auto& a1 = static_cast<const FixedWidthBinaryArray&>(*array1);
  const auto& a2 = static_cast<const FixedWidthBinaryArray&>(*array2);

  FixedWidthBinaryArray equal1(type, 2, a1.data(), a1.null_bitmap(), 1);
  FixedWidthBinaryArray equal2(type, 2, a2.data(), a1.null_bitmap(), 1);

  ASSERT_TRUE(equal1.Equals(equal2));
  ASSERT_TRUE(equal1.RangeEquals(equal2, 0, 2, 0));
}

TEST_F(TestFWBinaryArray, ZeroSize) {
  auto type = fixed_width_binary(0);
  FixedWidthBinaryBuilder builder(default_memory_pool(), type);

  ASSERT_OK(builder.Append(nullptr));
  ASSERT_OK(builder.Append(nullptr));
  ASSERT_OK(builder.Append(nullptr));
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.AppendNull());

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));

  const auto& fw_array = static_cast<const FixedWidthBinaryArray&>(*array);

  // data is never allocated
  ASSERT_TRUE(fw_array.data() == nullptr);
  ASSERT_EQ(0, fw_array.byte_width());

  ASSERT_EQ(6, array->length());
  ASSERT_EQ(3, array->null_count());
}

TEST_F(TestFWBinaryArray, Slice) {
  auto type = fixed_width_binary(4);
  FixedWidthBinaryBuilder builder(default_memory_pool(), type);

  std::vector<std::string> strings = {"foo1", "foo2", "foo3", "foo4", "foo5"};
  std::vector<uint8_t> is_null = {0, 1, 0, 0, 0};

  for (int i = 0; i < 5; ++i) {
    if (is_null[i]) {
      builder.AppendNull();
    } else {
      builder.Append(strings[i]);
    }
  }

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));

  std::shared_ptr<Array> slice, slice2;

  slice = array->Slice(1);
  slice2 = array->Slice(1);
  ASSERT_EQ(4, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(1, slice->length(), 0, slice));

  // Chained slices
  slice = array->Slice(2);
  slice2 = array->Slice(1)->Slice(1);
  ASSERT_TRUE(slice->Equals(slice2));

  slice = array->Slice(1, 3);
  ASSERT_EQ(3, slice->length());

  slice2 = array->Slice(1, 3);
  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(1, 3, 0, slice));
}

}  // namespace arrow
