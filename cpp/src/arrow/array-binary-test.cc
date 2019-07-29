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
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/string_view.h"

namespace arrow {

using internal::checked_cast;

using StringTypes =
    ::testing::Types<StringType, LargeStringType, BinaryType, LargeBinaryType>;

// ----------------------------------------------------------------------
// String / Binary tests

template <typename ArrayType>
void CheckStringArray(const ArrayType& array, const std::vector<std::string>& strings,
                      const std::vector<uint8_t>& is_valid, int repeats = 1) {
  int64_t length = array.length();
  int64_t base_length = static_cast<int64_t>(strings.size());
  ASSERT_EQ(base_length, static_cast<int64_t>(is_valid.size()));
  ASSERT_EQ(base_length * repeats, length);

  int32_t value_pos = 0;
  for (int i = 0; i < length; ++i) {
    auto j = i % base_length;
    if (is_valid[j]) {
      ASSERT_FALSE(array.IsNull(i));
      auto view = array.GetView(i);
      ASSERT_EQ(value_pos, array.value_offset(i));
      ASSERT_EQ(strings[j].size(), view.size());
      ASSERT_EQ(util::string_view(strings[j]), view);
      value_pos += static_cast<int32_t>(view.size());
    } else {
      ASSERT_TRUE(array.IsNull(i));
    }
  }
}

template <typename T>
class TestStringArray : public ::testing::Test {
 public:
  using TypeClass = T;
  using offset_type = typename TypeClass::offset_type;
  using ArrayType = typename TypeTraits<TypeClass>::ArrayType;
  using BuilderType = typename TypeTraits<TypeClass>::BuilderType;

  void SetUp() {
    chars_ = {'a', 'b', 'b', 'c', 'c', 'c'};
    offsets_ = {0, 1, 1, 1, 3, 6};
    valid_bytes_ = {1, 1, 0, 1, 1};
    expected_ = {"a", "", "", "bb", "ccc"};

    MakeArray();
  }

  void MakeArray() {
    length_ = static_cast<int64_t>(offsets_.size()) - 1;
    value_buf_ = Buffer::Wrap(chars_);
    offsets_buf_ = Buffer::Wrap(offsets_);
    ASSERT_OK(BitUtil::BytesToBits(valid_bytes_, default_memory_pool(), &null_bitmap_));
    null_count_ = CountNulls(valid_bytes_);

    strings_ = std::make_shared<ArrayType>(length_, offsets_buf_, value_buf_,
                                           null_bitmap_, null_count_);
  }

  void TestArrayBasics() {
    ASSERT_EQ(length_, strings_->length());
    ASSERT_EQ(1, strings_->null_count());
    ASSERT_OK(ValidateArray(*strings_));
    TestInitialized(*strings_);
    AssertZeroPadded(*strings_);
  }

  void TestType() {
    std::shared_ptr<DataType> type = this->strings_->type();

    if (std::is_same<TypeClass, StringType>::value) {
      ASSERT_EQ(Type::STRING, type->id());
      ASSERT_EQ(Type::STRING, this->strings_->type_id());
    } else if (std::is_same<TypeClass, LargeStringType>::value) {
      ASSERT_EQ(Type::LARGE_STRING, type->id());
      ASSERT_EQ(Type::LARGE_STRING, this->strings_->type_id());
    } else if (std::is_same<TypeClass, BinaryType>::value) {
      ASSERT_EQ(Type::BINARY, type->id());
      ASSERT_EQ(Type::BINARY, this->strings_->type_id());
    } else if (std::is_same<TypeClass, LargeBinaryType>::value) {
      ASSERT_EQ(Type::LARGE_BINARY, type->id());
      ASSERT_EQ(Type::LARGE_BINARY, this->strings_->type_id());
    } else {
      FAIL();
    }
  }

  void TestListFunctions() {
    int64_t pos = 0;
    for (size_t i = 0; i < expected_.size(); ++i) {
      ASSERT_EQ(pos, strings_->value_offset(i));
      ASSERT_EQ(expected_[i].size(), strings_->value_length(i));
      pos += expected_[i].size();
    }
  }

  void TestDestructor() {
    auto arr = std::make_shared<ArrayType>(length_, offsets_buf_, value_buf_,
                                           null_bitmap_, null_count_);
  }

  void TestGetString() {
    for (size_t i = 0; i < expected_.size(); ++i) {
      if (valid_bytes_[i] == 0) {
        ASSERT_TRUE(strings_->IsNull(i));
      } else {
        ASSERT_FALSE(strings_->IsNull(i));
        ASSERT_EQ(expected_[i], strings_->GetString(i));
      }
    }
  }

  void TestEmptyStringComparison() {
    offsets_ = {0, 0, 0, 0, 0, 0};
    offsets_buf_ = Buffer::Wrap(offsets_);
    length_ = static_cast<int64_t>(offsets_.size() - 1);

    auto strings_a = std::make_shared<ArrayType>(length_, offsets_buf_, nullptr,
                                                 null_bitmap_, null_count_);
    auto strings_b = std::make_shared<ArrayType>(length_, offsets_buf_, nullptr,
                                                 null_bitmap_, null_count_);
    ASSERT_TRUE(strings_a->Equals(strings_b));
  }

  void TestCompareNullByteSlots() {
    BuilderType builder;
    BuilderType builder2;
    BuilderType builder3;

    ASSERT_OK(builder.Append("foo"));
    ASSERT_OK(builder2.Append("foo"));
    ASSERT_OK(builder3.Append("foo"));

    ASSERT_OK(builder.Append("bar"));
    ASSERT_OK(builder2.AppendNull());

    // same length, but different
    ASSERT_OK(builder3.Append("xyz"));

    ASSERT_OK(builder.Append("baz"));
    ASSERT_OK(builder2.Append("baz"));
    ASSERT_OK(builder3.Append("baz"));

    std::shared_ptr<Array> array, array2, array3;
    FinishAndCheckPadding(&builder, &array);
    ASSERT_OK(builder2.Finish(&array2));
    ASSERT_OK(builder3.Finish(&array3));

    const auto& a1 = checked_cast<const ArrayType&>(*array);
    const auto& a2 = checked_cast<const ArrayType&>(*array2);
    const auto& a3 = checked_cast<const ArrayType&>(*array3);

    // The validity bitmaps are the same, the data is different, but the unequal
    // portion is masked out
    ArrayType equal_array(3, a1.value_offsets(), a1.value_data(), a2.null_bitmap(), 1);
    ArrayType equal_array2(3, a3.value_offsets(), a3.value_data(), a2.null_bitmap(), 1);

    ASSERT_TRUE(equal_array.Equals(equal_array2));
    ASSERT_TRUE(a2.RangeEquals(equal_array2, 0, 3, 0));

    ASSERT_TRUE(equal_array.Array::Slice(1)->Equals(equal_array2.Array::Slice(1)));
    ASSERT_TRUE(
        equal_array.Array::Slice(1)->RangeEquals(0, 2, 0, equal_array2.Array::Slice(1)));
  }

  void TestSliceGetString() {
    BuilderType builder;

    ASSERT_OK(builder.Append("a"));
    ASSERT_OK(builder.Append("b"));
    ASSERT_OK(builder.Append("c"));

    std::shared_ptr<Array> array;
    ASSERT_OK(builder.Finish(&array));
    auto s = array->Slice(1, 10);
    auto arr = std::dynamic_pointer_cast<ArrayType>(s);
    ASSERT_EQ(arr->GetString(0), "b");
  }

 protected:
  std::vector<offset_type> offsets_;
  std::vector<char> chars_;
  std::vector<uint8_t> valid_bytes_;

  std::vector<std::string> expected_;

  std::shared_ptr<Buffer> value_buf_;
  std::shared_ptr<Buffer> offsets_buf_;
  std::shared_ptr<Buffer> null_bitmap_;

  int64_t null_count_;
  int64_t length_;

  std::shared_ptr<ArrayType> strings_;
};

TYPED_TEST_CASE(TestStringArray, StringTypes);

TYPED_TEST(TestStringArray, TestArrayBasics) { this->TestArrayBasics(); }

TYPED_TEST(TestStringArray, TestType) { this->TestType(); }

TYPED_TEST(TestStringArray, TestListFunctions) { this->TestListFunctions(); }

TYPED_TEST(TestStringArray, TestDestructor) { this->TestDestructor(); }

TYPED_TEST(TestStringArray, TestGetString) { this->TestGetString(); }

TYPED_TEST(TestStringArray, TestEmptyStringComparison) {
  this->TestEmptyStringComparison();
}

TYPED_TEST(TestStringArray, CompareNullByteSlots) { this->TestCompareNullByteSlots(); }

TYPED_TEST(TestStringArray, TestSliceGetString) { this->TestSliceGetString(); }

// ----------------------------------------------------------------------
// String builder tests

template <typename T>
class TestStringBuilder : public TestBuilder {
 public:
  using TypeClass = T;
  using offset_type = typename TypeClass::offset_type;
  using ArrayType = typename TypeTraits<TypeClass>::ArrayType;
  using BuilderType = typename TypeTraits<TypeClass>::BuilderType;

  void SetUp() {
    TestBuilder::SetUp();
    builder_.reset(new BuilderType(pool_));
  }

  void Done() {
    std::shared_ptr<Array> out;
    FinishAndCheckPadding(builder_.get(), &out);

    result_ = std::dynamic_pointer_cast<ArrayType>(out);
    ASSERT_OK(ValidateArray(*result_));
  }

  void TestScalarAppend() {
    std::vector<std::string> strings = {"", "bb", "a", "", "ccc"};
    std::vector<uint8_t> is_valid = {1, 1, 1, 0, 1};

    int N = static_cast<int>(strings.size());
    int reps = 10;

    for (int j = 0; j < reps; ++j) {
      for (int i = 0; i < N; ++i) {
        if (!is_valid[i]) {
          ASSERT_OK(builder_->AppendNull());
        } else {
          ASSERT_OK(builder_->Append(strings[i]));
        }
      }
    }
    Done();

    ASSERT_EQ(reps * N, result_->length());
    ASSERT_EQ(reps, result_->null_count());
    ASSERT_EQ(reps * 6, result_->value_data()->size());

    CheckStringArray(*result_, strings, is_valid, reps);
  }

  void TestScalarAppendUnsafe() {
    std::vector<std::string> strings = {"", "bb", "a", "", "ccc"};
    std::vector<uint8_t> is_valid = {1, 1, 1, 0, 1};

    int N = static_cast<int>(strings.size());
    int reps = 13;
    int64_t total_length = 0;
    for (const auto& s : strings) {
      total_length += static_cast<int64_t>(s.size());
    }

    ASSERT_OK(builder_->Reserve(N * reps));
    ASSERT_OK(builder_->ReserveData(total_length * reps));

    for (int j = 0; j < reps; ++j) {
      for (int i = 0; i < N; ++i) {
        if (!is_valid[i]) {
          builder_->UnsafeAppendNull();
        } else {
          builder_->UnsafeAppend(strings[i]);
        }
      }
    }
    ASSERT_EQ(builder_->value_data_length(), total_length * reps);
    Done();

    ASSERT_OK(ValidateArray(*result_));
    ASSERT_EQ(reps * N, result_->length());
    ASSERT_EQ(reps, result_->null_count());
    ASSERT_EQ(reps * total_length, result_->value_data()->size());

    CheckStringArray(*result_, strings, is_valid, reps);
  }

  void TestVectorAppend() {
    std::vector<std::string> strings = {"", "bb", "a", "", "ccc"};
    std::vector<uint8_t> valid_bytes = {1, 1, 1, 0, 1};

    int N = static_cast<int>(strings.size());
    int reps = 1000;

    for (int j = 0; j < reps; ++j) {
      ASSERT_OK(builder_->AppendValues(strings, valid_bytes.data()));
    }
    Done();

    ASSERT_EQ(reps * N, result_->length());
    ASSERT_EQ(reps, result_->null_count());
    ASSERT_EQ(reps * 6, result_->value_data()->size());

    CheckStringArray(*result_, strings, valid_bytes, reps);
  }

  void TestAppendCStringsWithValidBytes() {
    const char* strings[] = {nullptr, "aaa", nullptr, "ignored", ""};
    std::vector<uint8_t> valid_bytes = {1, 1, 1, 0, 1};

    int N = static_cast<int>(sizeof(strings) / sizeof(strings[0]));
    int reps = 1000;

    for (int j = 0; j < reps; ++j) {
      ASSERT_OK(builder_->AppendValues(strings, N, valid_bytes.data()));
    }
    Done();

    ASSERT_EQ(reps * N, result_->length());
    ASSERT_EQ(reps * 3, result_->null_count());
    ASSERT_EQ(reps * 3, result_->value_data()->size());

    CheckStringArray(*result_, {"", "aaa", "", "", ""}, {0, 1, 0, 0, 1}, reps);
  }

  void TestAppendCStringsWithoutValidBytes() {
    const char* strings[] = {"", "bb", "a", nullptr, "ccc"};

    int N = static_cast<int>(sizeof(strings) / sizeof(strings[0]));
    int reps = 1000;

    for (int j = 0; j < reps; ++j) {
      ASSERT_OK(builder_->AppendValues(strings, N));
    }
    Done();

    ASSERT_EQ(reps * N, result_->length());
    ASSERT_EQ(reps, result_->null_count());
    ASSERT_EQ(reps * 6, result_->value_data()->size());

    CheckStringArray(*result_, {"", "bb", "a", "", "ccc"}, {1, 1, 1, 0, 1}, reps);
  }

  void TestCapacityReserve() {
    std::vector<std::string> strings = {"aaaaa", "bbbbbbbbbb", "ccccccccccccccc",
                                        "dddddddddd"};
    int N = static_cast<int>(strings.size());
    int reps = 15;
    int64_t length = 0;
    int64_t capacity = 1000;
    int64_t expected_capacity = BitUtil::RoundUpToMultipleOf64(capacity);

    ASSERT_OK(builder_->ReserveData(capacity));

    ASSERT_EQ(length, builder_->value_data_length());
    ASSERT_EQ(expected_capacity, builder_->value_data_capacity());

    for (int j = 0; j < reps; ++j) {
      for (int i = 0; i < N; ++i) {
        ASSERT_OK(builder_->Append(strings[i]));
        length += static_cast<int64_t>(strings[i].size());

        ASSERT_EQ(length, builder_->value_data_length());
        ASSERT_EQ(expected_capacity, builder_->value_data_capacity());
      }
    }

    int extra_capacity = 500;
    expected_capacity = BitUtil::RoundUpToMultipleOf64(length + extra_capacity);

    ASSERT_OK(builder_->ReserveData(extra_capacity));

    ASSERT_EQ(length, builder_->value_data_length());
    int64_t actual_capacity = builder_->value_data_capacity();
    ASSERT_GE(actual_capacity, expected_capacity);
    ASSERT_EQ(actual_capacity & 63, 0);

    Done();

    ASSERT_EQ(reps * N, result_->length());
    ASSERT_EQ(0, result_->null_count());
    ASSERT_EQ(reps * 40, result_->value_data()->size());
  }

  void TestZeroLength() {
    // All buffers are null
    Done();
    ASSERT_EQ(result_->length(), 0);
    ASSERT_EQ(result_->null_count(), 0);
  }

 protected:
  std::unique_ptr<BuilderType> builder_;
  std::shared_ptr<ArrayType> result_;
};

TYPED_TEST_CASE(TestStringBuilder, StringTypes);

TYPED_TEST(TestStringBuilder, TestScalarAppend) { this->TestScalarAppend(); }

TYPED_TEST(TestStringBuilder, TestScalarAppendUnsafe) { this->TestScalarAppendUnsafe(); }

TYPED_TEST(TestStringBuilder, TestVectorAppend) { this->TestVectorAppend(); }

TYPED_TEST(TestStringBuilder, TestAppendCStringsWithValidBytes) {
  this->TestAppendCStringsWithValidBytes();
}

TYPED_TEST(TestStringBuilder, TestAppendCStringsWithoutValidBytes) {
  this->TestAppendCStringsWithoutValidBytes();
}

TYPED_TEST(TestStringBuilder, TestCapacityReserve) { this->TestCapacityReserve(); }

TYPED_TEST(TestStringBuilder, TestZeroLength) { this->TestZeroLength(); }

// ----------------------------------------------------------------------
// ChunkedBinaryBuilder tests

class TestChunkedBinaryBuilder : public ::testing::Test {
 public:
  void SetUp() {}

  void Init(int32_t chunksize) {
    builder_.reset(new internal::ChunkedBinaryBuilder(chunksize));
  }

  void Init(int32_t chunksize, int32_t chunklength) {
    builder_.reset(new internal::ChunkedBinaryBuilder(chunksize, chunklength));
  }

 protected:
  std::unique_ptr<internal::ChunkedBinaryBuilder> builder_;
};

TEST_F(TestChunkedBinaryBuilder, BasicOperation) {
  const int32_t chunksize = 1000;
  Init(chunksize);

  const int elem_size = 10;
  uint8_t buf[elem_size];

  BinaryBuilder unchunked_builder;

  const int iterations = 1000;
  for (int i = 0; i < iterations; ++i) {
    random_bytes(elem_size, i, buf);

    ASSERT_OK(unchunked_builder.Append(buf, elem_size));
    ASSERT_OK(builder_->Append(buf, elem_size));
  }

  std::shared_ptr<Array> unchunked;
  ASSERT_OK(unchunked_builder.Finish(&unchunked));

  ArrayVector chunks;
  ASSERT_OK(builder_->Finish(&chunks));

  // This assumes that everything is evenly divisible
  ArrayVector expected_chunks;
  const int elems_per_chunk = chunksize / elem_size;
  for (int i = 0; i < iterations / elems_per_chunk; ++i) {
    expected_chunks.emplace_back(unchunked->Slice(i * elems_per_chunk, elems_per_chunk));
  }

  ASSERT_EQ(expected_chunks.size(), chunks.size());
  for (size_t i = 0; i < chunks.size(); ++i) {
    AssertArraysEqual(*expected_chunks[i], *chunks[i]);
  }
}

TEST_F(TestChunkedBinaryBuilder, NoData) {
  Init(1000);

  ArrayVector chunks;
  ASSERT_OK(builder_->Finish(&chunks));

  ASSERT_EQ(1, chunks.size());
  ASSERT_EQ(0, chunks[0]->length());
}

TEST_F(TestChunkedBinaryBuilder, LargeElements) {
  Init(100);

  const int bufsize = 101;
  uint8_t buf[bufsize];

  const int iterations = 100;
  for (int i = 0; i < iterations; ++i) {
    random_bytes(bufsize, i, buf);
    ASSERT_OK(builder_->Append(buf, bufsize));
  }

  ArrayVector chunks;
  ASSERT_OK(builder_->Finish(&chunks));
  ASSERT_EQ(iterations, static_cast<int>(chunks.size()));

  int64_t total_data_size = 0;
  for (auto chunk : chunks) {
    ASSERT_EQ(1, chunk->length());
    total_data_size +=
        static_cast<int64_t>(static_cast<const BinaryArray&>(*chunk).GetView(0).size());
  }
  ASSERT_EQ(iterations * bufsize, total_data_size);
}

TEST_F(TestChunkedBinaryBuilder, LargeElementCount) {
  int32_t max_chunk_length = 100;
  Init(100, max_chunk_length);

  auto length = max_chunk_length + 1;

  // ChunkedBinaryBuilder can reserve memory for more than its configured maximum
  // (per chunk) element count
  ASSERT_OK(builder_->Reserve(length));

  for (int64_t i = 0; i < 2 * length; ++i) {
    // Appending more elements than have been reserved memory simply overflows to the next
    // chunk
    ASSERT_OK(builder_->Append(""));
  }

  ArrayVector chunks;
  ASSERT_OK(builder_->Finish(&chunks));

  // should have two chunks full of empty strings and another with two more empty strings
  ASSERT_EQ(chunks.size(), 3);
  ASSERT_EQ(chunks[0]->length(), max_chunk_length);
  ASSERT_EQ(chunks[1]->length(), max_chunk_length);
  ASSERT_EQ(chunks[2]->length(), 2);
  for (auto&& boxed_chunk : chunks) {
    const auto& chunk = checked_cast<const BinaryArray&>(*boxed_chunk);
    ASSERT_EQ(chunk.value_offset(0), chunk.value_offset(chunk.length()));
  }
}

TEST(TestChunkedStringBuilder, BasicOperation) {
  const int chunksize = 100;
  internal::ChunkedStringBuilder builder(chunksize);

  std::string value = "0123456789";

  const int iterations = 100;
  for (int i = 0; i < iterations; ++i) {
    ASSERT_OK(builder.Append(value));
  }

  ArrayVector chunks;
  ASSERT_OK(builder.Finish(&chunks));

  ASSERT_EQ(10, chunks.size());

  // Type is correct
  for (auto chunk : chunks) {
    ASSERT_TRUE(chunk->type()->Equals(utf8()));
  }
}

}  // namespace arrow
