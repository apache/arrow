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
#include <string_view>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/validate.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_builders.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/visit_data_inline.h"

namespace arrow {

using internal::checked_cast;

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
      ASSERT_EQ(std::string_view(strings[j]), view);
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
    ASSERT_OK_AND_ASSIGN(null_bitmap_, internal::BytesToBits(valid_bytes_));
    null_count_ = CountNulls(valid_bytes_);

    strings_ = std::make_shared<ArrayType>(length_, offsets_buf_, value_buf_,
                                           null_bitmap_, null_count_);
  }

  void TestArrayBasics() {
    ASSERT_EQ(length_, strings_->length());
    ASSERT_EQ(1, strings_->null_count());
    ASSERT_OK(strings_->ValidateFull());
    TestInitialized(*strings_);
    AssertZeroPadded(*strings_);
  }

  void TestArrayIndexOperator() {
    const auto& arr = *strings_;
    for (int64_t i = 0; i < arr.length(); ++i) {
      if (valid_bytes_[i]) {
        ASSERT_TRUE(arr[i].has_value());
        ASSERT_EQ(expected_[i], arr[i].value());
      } else {
        ASSERT_FALSE(arr[i].has_value());
      }
    }
  }

  void TestArrayCtors() {
    // ARROW-8863: ArrayData::null_count set to 0 when no validity bitmap
    // provided
    ArrayType arr(length_, offsets_buf_, value_buf_);
    ASSERT_EQ(arr.data()->null_count, 0);
  }

  void TestTotalValuesLength() {
    auto ty = TypeTraits<T>::type_singleton();
    auto arr = ArrayFromJSON(ty, R"(["a", null, "bbb", "cccc", "ddddd"])");

    offset_type values_length = arr.total_values_length();
    ASSERT_EQ(values_length, static_cast<offset_type>(13));

    offset_type sliced_values_length =
        checked_cast<const ArrayType&>(*arr.Slice(3)).total_values_length();
    ASSERT_EQ(sliced_values_length, static_cast<offset_type>(9));

    // Zero-length array is a special case
    offset_type zero_size_length =
        checked_cast<const ArrayType&>(*arr.Slice(0, 0)).total_values_length();
    ASSERT_EQ(zero_size_length, static_cast<offset_type>(0));
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

  Status ValidateFull(int64_t length, std::vector<offset_type> offsets,
                      std::string_view data, int64_t offset = 0) {
    ArrayType arr(length, Buffer::Wrap(offsets), std::make_shared<Buffer>(data),
                  /*null_bitmap=*/nullptr, /*null_count=*/0, offset);
    return arr.ValidateFull();
  }

  Status ValidateFull(const std::string& json) {
    auto ty = TypeTraits<T>::type_singleton();
    auto arr = ArrayFromJSON(ty, json);
    return arr->ValidateFull();
  }

  void TestValidateOffsets() {
    ASSERT_OK(ValidateFull(0, {0}, ""));
    ASSERT_OK(ValidateFull(1, {0, 4}, "data"));
    ASSERT_OK(ValidateFull(2, {0, 4, 4}, "data"));
    ASSERT_OK(ValidateFull(2, {0, 5, 9}, "some data"));

    // Non-zero array offset
    ASSERT_OK(ValidateFull(0, {0, 4}, "data", 1));
    ASSERT_OK(ValidateFull(1, {0, 5, 9}, "some data", 1));
    ASSERT_OK(ValidateFull(0, {0, 5, 9}, "some data", 2));

    // Not enough offsets
    ASSERT_RAISES(Invalid, ValidateFull(1, {}, ""));
    ASSERT_RAISES(Invalid, ValidateFull(1, {0}, ""));
    ASSERT_RAISES(Invalid, ValidateFull(2, {0, 4}, "data"));
    ASSERT_RAISES(Invalid, ValidateFull(1, {0, 4}, "data", 1));

    // Offset out of bounds
    ASSERT_RAISES(Invalid, ValidateFull(1, {0, 5}, "data"));
    // Negative offset
    ASSERT_RAISES(Invalid, ValidateFull(1, {-1, 0}, "data"));
    ASSERT_RAISES(Invalid, ValidateFull(1, {0, -1}, "data"));
    ASSERT_RAISES(Invalid, ValidateFull(1, {0, -1, -1}, "data", 1));
    // Offsets non-monotonic
    ASSERT_RAISES(Invalid, ValidateFull(2, {0, 5, 4}, "some data"));
  }

  void TestValidateData() {
    // Valid UTF8
    ASSERT_OK(ValidateFull(R"(["Voix", "ambiguë", "d’un", "cœur"])"));
    ASSERT_OK(ValidateFull(R"(["いろはにほへと", "ちりぬるを", "わかよたれそ"])"));
    ASSERT_OK(ValidateFull(R"(["😀", "😄"])"));
    ASSERT_OK(ValidateFull(1, {0, 4}, "\xf4\x8f\xbf\xbf"));  // \U0010ffff

    // Invalid UTF8
    auto ty = TypeTraits<T>::type_singleton();
    auto st1 = ValidateFull(3, {0, 4, 6, 9}, "abc \xff def");
    // Hypothetical \U00110000
    auto st2 = ValidateFull(1, {0, 4}, "\xf4\x90\x80\x80");
    // Single UTF8 character straddles two entries
    auto st3 = ValidateFull(2, {0, 1, 2}, "\xc3\xa9");
    if (T::is_utf8) {
      ASSERT_RAISES(Invalid, st1);
      ASSERT_RAISES(Invalid, st2);
      ASSERT_RAISES(Invalid, st3);
    } else {
      ASSERT_OK(st1);
      ASSERT_OK(st2);
      ASSERT_OK(st3);
    }
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

TYPED_TEST_SUITE(TestStringArray, BaseBinaryArrowTypes);

TYPED_TEST(TestStringArray, TestArrayBasics) { this->TestArrayBasics(); }

TYPED_TEST(TestStringArray, TestArrayIndexOperator) { this->TestArrayIndexOperator(); }

TYPED_TEST(TestStringArray, TestArrayCtors) { this->TestArrayCtors(); }

TYPED_TEST(TestStringArray, TestType) { this->TestType(); }

TYPED_TEST(TestStringArray, TestListFunctions) { this->TestListFunctions(); }

TYPED_TEST(TestStringArray, TestDestructor) { this->TestDestructor(); }

TYPED_TEST(TestStringArray, TestGetString) { this->TestGetString(); }

TYPED_TEST(TestStringArray, TestEmptyStringComparison) {
  this->TestEmptyStringComparison();
}

TYPED_TEST(TestStringArray, CompareNullByteSlots) { this->TestCompareNullByteSlots(); }

TYPED_TEST(TestStringArray, TestSliceGetString) { this->TestSliceGetString(); }

TYPED_TEST(TestStringArray, TestValidateOffsets) { this->TestValidateOffsets(); }

TYPED_TEST(TestStringArray, TestValidateData) { this->TestValidateData(); }

// Produce an Array of index/offset views from a std::vector of index/offset
// BinaryViewType::c_type
Result<std::shared_ptr<StringViewArray>> MakeBinaryViewArray(
    BufferVector data_buffers, const std::vector<BinaryViewType::c_type>& views,
    bool validate = true) {
  auto length = static_cast<int64_t>(views.size());
  auto arr = std::make_shared<StringViewArray>(
      utf8_view(), length, Buffer::FromVector(views), std::move(data_buffers));
  if (validate) {
    RETURN_NOT_OK(arr->ValidateFull());
  }
  return arr;
}

TEST(StringViewArray, Validate) {
  // Since this is a test of validation, we need to be able to construct invalid arrays.
  auto buffer_s = Buffer::FromString("supercalifragilistic(sp?)");
  auto buffer_y = Buffer::FromString("yyyyyyyyyyyyyyyyyyyyyyyyy");

  // empty array is valid
  EXPECT_THAT(MakeBinaryViewArray({}, {}), Ok());

  // empty array with some data buffers is valid
  EXPECT_THAT(MakeBinaryViewArray({buffer_s, buffer_y}, {}), Ok());

  // inline views need not have a corresponding buffer
  EXPECT_THAT(MakeBinaryViewArray({},
                                  {
                                      util::ToInlineBinaryView("hello"),
                                      util::ToInlineBinaryView("world"),
                                      util::ToInlineBinaryView("inline me"),
                                  }),
              Ok());

  // non-inline views are expected to reference only buffers managed by the array
  EXPECT_THAT(
      MakeBinaryViewArray(
          {buffer_s, buffer_y},
          {util::ToBinaryView("supe", static_cast<int32_t>(buffer_s->size()), 0, 0),
           util::ToBinaryView("yyyy", static_cast<int32_t>(buffer_y->size()), 1, 0)}),
      Ok());

  // views may not reference data buffers not present in the array
  EXPECT_THAT(
      MakeBinaryViewArray(
          {}, {util::ToBinaryView("supe", static_cast<int32_t>(buffer_s->size()), 0, 0)}),
      Raises(StatusCode::IndexError));
  // ... or ranges which overflow the referenced data buffer
  EXPECT_THAT(
      MakeBinaryViewArray(
          {buffer_s}, {util::ToBinaryView(
                          "supe", static_cast<int32_t>(buffer_s->size() + 50), 0, 0)}),
      Raises(StatusCode::IndexError));

  // Additionally, the prefixes of non-inline views must match the data buffer
  EXPECT_THAT(
      MakeBinaryViewArray(
          {buffer_s, buffer_y},
          {util::ToBinaryView("SUPE", static_cast<int32_t>(buffer_s->size()), 0, 0),
           util::ToBinaryView("yyyy", static_cast<int32_t>(buffer_y->size()), 1, 0)}),
      Raises(StatusCode::Invalid));

  // Invalid string views which are masked by a null bit do not cause validation to fail
  auto invalid_but_masked =
      MakeBinaryViewArray(
          {buffer_s},
          {util::ToBinaryView("SUPE", static_cast<int32_t>(buffer_s->size()), 0, 0),
           util::ToBinaryView("yyyy", 50, 40, 30)},
          /*validate=*/false)
          .ValueOrDie()
          ->data();
  invalid_but_masked->null_count = 2;
  invalid_but_masked->buffers[0] = *AllocateEmptyBitmap(2);
  EXPECT_THAT(internal::ValidateArrayFull(*invalid_but_masked), Ok());

  // overlapping views are allowed
  EXPECT_THAT(
      MakeBinaryViewArray(
          {buffer_s},
          {
              util::ToBinaryView("supe", static_cast<int32_t>(buffer_s->size()), 0, 0),
              util::ToBinaryView("uper", static_cast<int32_t>(buffer_s->size() - 1), 0,
                                 1),
              util::ToBinaryView("perc", static_cast<int32_t>(buffer_s->size() - 2), 0,
                                 2),
              util::ToBinaryView("erca", static_cast<int32_t>(buffer_s->size() - 3), 0,
                                 3),
          }),
      Ok());
}

template <typename T>
class TestUTF8Array : public ::testing::Test {
 public:
  using TypeClass = T;
  using ArrayType = typename TypeTraits<TypeClass>::ArrayType;

  std::shared_ptr<DataType> type() const {
    if constexpr (is_binary_view_like_type<TypeClass>::value) {
      return TypeClass::is_utf8 ? utf8_view() : binary_view();
    } else {
      return TypeTraits<TypeClass>::type_singleton();
    }
  }

  Status ValidateUTF8(const Array& arr) {
    return checked_cast<const ArrayType&>(arr).ValidateUTF8();
  }

  Status ValidateUTF8(std::vector<std::string> values) {
    std::shared_ptr<Array> arr;
    ArrayFromVector<T, std::string>(type(), values, &arr);
    return ValidateUTF8(*arr);
  }

  void TestValidateUTF8() {
    ASSERT_OK(
        ValidateUTF8(*ArrayFromJSON(type(), R"(["Voix", "ambiguë", "d’un", "cœur"])")));
    ASSERT_OK(ValidateUTF8({"\xf4\x8f\xbf\xbf"}));  // \U0010ffff

    ASSERT_RAISES(Invalid, ValidateUTF8({"\xf4"}));

    // More tests in TestValidateData() above
    // (ValidateFull() calls ValidateUTF8() internally)
  }
};

TYPED_TEST_SUITE(TestUTF8Array, StringOrStringViewArrowTypes);

TYPED_TEST(TestUTF8Array, TestValidateUTF8) { this->TestValidateUTF8(); }

// ----------------------------------------------------------------------
// String builder tests

template <typename T>
class TestStringBuilder : public ::testing::Test {
 public:
  using TypeClass = T;
  using offset_type = typename TypeClass::offset_type;
  using ArrayType = typename TypeTraits<TypeClass>::ArrayType;
  using BuilderType = typename TypeTraits<TypeClass>::BuilderType;

  void SetUp() { builder_.reset(new BuilderType(pool_)); }

  void Done() {
    std::shared_ptr<Array> out;
    FinishAndCheckPadding(builder_.get(), &out);

    result_ = std::dynamic_pointer_cast<ArrayType>(out);
    ASSERT_OK(result_->ValidateFull());
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

    ASSERT_OK(result_->ValidateFull());
    ASSERT_EQ(reps * N, result_->length());
    ASSERT_EQ(reps, result_->null_count());
    ASSERT_EQ(reps * total_length, result_->value_data()->size());

    CheckStringArray(*result_, strings, is_valid, reps);
  }

  void TestExtendCurrent() {
    std::vector<std::string> strings = {"", "bbbb", "aaaaa", "", "ccc"};
    std::vector<uint8_t> is_valid = {1, 1, 1, 0, 1};

    int N = static_cast<int>(strings.size());
    int reps = 10;

    for (int j = 0; j < reps; ++j) {
      for (int i = 0; i < N; ++i) {
        if (!is_valid[i]) {
          ASSERT_OK(builder_->AppendNull());
        } else if (strings[i].length() > 3) {
          ASSERT_OK(builder_->Append(strings[i].substr(0, 3)));
          ASSERT_OK(builder_->ExtendCurrent(strings[i].substr(3)));
        } else {
          ASSERT_OK(builder_->Append(strings[i]));
        }
      }
    }
    Done();

    ASSERT_EQ(reps * N, result_->length());
    ASSERT_EQ(reps, result_->null_count());
    ASSERT_EQ(reps * 12, result_->value_data()->size());

    CheckStringArray(*result_, strings, is_valid, reps);
  }

  void TestExtendCurrentUnsafe() {
    std::vector<std::string> strings = {"", "bbbb", "aaaaa", "", "ccc"};
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
        } else if (strings[i].length() > 3) {
          builder_->UnsafeAppend(strings[i].substr(0, 3));
          builder_->UnsafeExtendCurrent(strings[i].substr(3));
        } else {
          builder_->UnsafeAppend(strings[i]);
        }
      }
    }
    ASSERT_EQ(builder_->value_data_length(), total_length * reps);
    Done();

    ASSERT_EQ(reps * N, result_->length());
    ASSERT_EQ(reps, result_->null_count());
    ASSERT_EQ(reps * 12, result_->value_data()->size());

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
    int64_t expected_capacity = bit_util::RoundUpToMultipleOf64(capacity);

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
    expected_capacity = bit_util::RoundUpToMultipleOf64(length + extra_capacity);

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

  void TestOverflowCheck() {
    auto max_size = builder_->memory_limit();

    ASSERT_OK(builder_->ValidateOverflow(1));
    ASSERT_OK(builder_->ValidateOverflow(max_size));
    ASSERT_RAISES(CapacityError, builder_->ValidateOverflow(max_size + 1));

    ASSERT_OK(builder_->Append("bb"));
    ASSERT_OK(builder_->ValidateOverflow(max_size - 2));
    ASSERT_RAISES(CapacityError, builder_->ValidateOverflow(max_size - 1));

    ASSERT_OK(builder_->AppendNull());
    ASSERT_OK(builder_->ValidateOverflow(max_size - 2));
    ASSERT_RAISES(CapacityError, builder_->ValidateOverflow(max_size - 1));

    ASSERT_OK(builder_->Append("ccc"));
    ASSERT_OK(builder_->ValidateOverflow(max_size - 5));
    ASSERT_RAISES(CapacityError, builder_->ValidateOverflow(max_size - 4));
  }

  void TestZeroLength() {
    // All buffers are null
    Done();
    ASSERT_EQ(result_->length(), 0);
    ASSERT_EQ(result_->null_count(), 0);
  }

 protected:
  MemoryPool* pool_ = default_memory_pool();
  std::unique_ptr<BuilderType> builder_;
  std::shared_ptr<ArrayType> result_;
};

TYPED_TEST_SUITE(TestStringBuilder, BaseBinaryArrowTypes);

TYPED_TEST(TestStringBuilder, TestScalarAppend) { this->TestScalarAppend(); }

TYPED_TEST(TestStringBuilder, TestScalarAppendUnsafe) { this->TestScalarAppendUnsafe(); }

TYPED_TEST(TestStringBuilder, TestExtendCurrent) { this->TestExtendCurrent(); }

TYPED_TEST(TestStringBuilder, TestExtendCurrentUnsafe) {
  this->TestExtendCurrentUnsafe();
}

TYPED_TEST(TestStringBuilder, TestVectorAppend) { this->TestVectorAppend(); }

TYPED_TEST(TestStringBuilder, TestAppendCStringsWithValidBytes) {
  this->TestAppendCStringsWithValidBytes();
}

TYPED_TEST(TestStringBuilder, TestAppendCStringsWithoutValidBytes) {
  this->TestAppendCStringsWithoutValidBytes();
}

TYPED_TEST(TestStringBuilder, TestCapacityReserve) { this->TestCapacityReserve(); }

TYPED_TEST(TestStringBuilder, TestZeroLength) { this->TestZeroLength(); }

TYPED_TEST(TestStringBuilder, TestOverflowCheck) { this->TestOverflowCheck(); }

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

TEST_F(TestChunkedBinaryBuilder, Reserve) {
  // ARROW-6060
  const int32_t chunksize = 1000;
  Init(chunksize);
  ASSERT_OK(builder_->Reserve(chunksize / 2));
  auto bytes_after_first_reserve = default_memory_pool()->bytes_allocated();
  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(builder_->Reserve(chunksize / 2));
  }
  // no new memory will be allocated since capacity was sufficient for the loop's
  // Reserve() calls
  ASSERT_EQ(default_memory_pool()->bytes_allocated(), bytes_after_first_reserve);
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

// ----------------------------------------------------------------------
// ArraySpanVisitor<binary-like> tests

struct BinaryAppender {
  Status VisitNull() {
    data.emplace_back("(null)");
    return Status::OK();
  }

  Status VisitValue(std::string_view v) {
    data.push_back(v);
    return Status::OK();
  }

  std::vector<std::string_view> data;
};

template <typename T>
class TestBaseBinaryDataVisitor : public ::testing::Test {
 public:
  using TypeClass = T;

  void SetUp() override { type_ = TypeTraits<TypeClass>::type_singleton(); }

  void TestBasics() {
    auto array = ArrayFromJSON(
        type_,
        R"(["foo", null, "bar", "inline_me", "allocate_me_aaaaa", "allocate_me_bbbb"])");
    BinaryAppender appender;
    ArraySpanVisitor<TypeClass> visitor;
    ASSERT_OK(visitor.Visit(*array->data(), &appender));
    ASSERT_THAT(appender.data,
                ::testing::ElementsAreArray({"foo", "(null)", "bar", "inline_me",
                                             "allocate_me_aaaaa", "allocate_me_bbbb"}));
    ARROW_UNUSED(visitor);  // Workaround weird MSVC warning
  }

  void TestSliced() {
    auto array = ArrayFromJSON(type_, R"(["ab", null, "cd", "ef"])")->Slice(1, 2);
    BinaryAppender appender;
    ArraySpanVisitor<TypeClass> visitor;
    ASSERT_OK(visitor.Visit(*array->data(), &appender));
    ASSERT_THAT(appender.data, ::testing::ElementsAreArray({"(null)", "cd"}));
    ARROW_UNUSED(visitor);  // Workaround weird MSVC warning
  }

 protected:
  std::shared_ptr<DataType> type_;
};

TYPED_TEST_SUITE(TestBaseBinaryDataVisitor, BaseBinaryOrBinaryViewLikeArrowTypes);

TYPED_TEST(TestBaseBinaryDataVisitor, Basics) { this->TestBasics(); }

TYPED_TEST(TestBaseBinaryDataVisitor, Sliced) { this->TestSliced(); }

}  // namespace arrow
