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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_decimal.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/compare.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_compat.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_builders.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/macros.h"
#include "arrow/util/range.h"
#include "arrow/visitor_inline.h"

// This file is compiled together with array-*-test.cc into a single
// executable array-test.

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

class TestArray : public ::testing::Test {
 public:
  void SetUp() { pool_ = default_memory_pool(); }

 protected:
  MemoryPool* pool_;
};

TEST_F(TestArray, TestNullCount) {
  // These are placeholders
  auto data = std::make_shared<Buffer>(nullptr, 0);
  auto null_bitmap = std::make_shared<Buffer>(nullptr, 0);

  std::unique_ptr<Int32Array> arr(new Int32Array(100, data, null_bitmap, 10));
  ASSERT_EQ(10, arr->null_count());

  std::unique_ptr<Int32Array> arr_no_nulls(new Int32Array(100, data));
  ASSERT_EQ(0, arr_no_nulls->null_count());

  std::unique_ptr<Int32Array> arr_default_null_count(
      new Int32Array(100, data, null_bitmap));
  ASSERT_EQ(kUnknownNullCount, arr_default_null_count->data()->null_count);
}

TEST_F(TestArray, TestSlicePreservesAllNullCount) {
  // These are placeholders
  auto data = std::make_shared<Buffer>(nullptr, 0);
  auto null_bitmap = std::make_shared<Buffer>(nullptr, 0);

  Int32Array arr(/*length=*/100, data, null_bitmap,
                 /*null_count*/ 100);
  EXPECT_EQ(arr.Slice(1, 99)->data()->null_count, arr.Slice(1, 99)->length());
}

TEST_F(TestArray, TestLength) {
  // Placeholder buffer
  auto data = std::make_shared<Buffer>(nullptr, 0);

  std::unique_ptr<Int32Array> arr(new Int32Array(100, data));
  ASSERT_EQ(arr->length(), 100);
}

TEST_F(TestArray, TestNullToString) {
  // Invalid NULL buffer
  auto data = std::make_shared<Buffer>(nullptr, 400);

  std::unique_ptr<Int32Array> arr(new Int32Array(100, data));
  ASSERT_EQ(arr->ToString(), "<InvalidArray: Missing values buffer in non-empty array>");
}

TEST_F(TestArray, TestSliceSafe) {
  std::vector<int32_t> original_data{1, 2, 3, 4, 5, 6, 7};
  auto arr = std::make_shared<Int32Array>(7, Buffer::Wrap(original_data));

  auto check_data = [](const Array& arr, const std::vector<int32_t>& expected) {
    ASSERT_EQ(arr.length(), static_cast<int64_t>(expected.size()));
    const int32_t* data = arr.data()->GetValues<int32_t>(1);
    for (int64_t i = 0; i < arr.length(); ++i) {
      ASSERT_EQ(data[i], expected[i]);
    }
  };

  check_data(*arr, {1, 2, 3, 4, 5, 6, 7});

  ASSERT_OK_AND_ASSIGN(auto sliced, arr->SliceSafe(0, 0));
  check_data(*sliced, {});

  ASSERT_OK_AND_ASSIGN(sliced, arr->SliceSafe(0, 7));
  check_data(*sliced, original_data);

  ASSERT_OK_AND_ASSIGN(sliced, arr->SliceSafe(3, 4));
  check_data(*sliced, {4, 5, 6, 7});

  ASSERT_OK_AND_ASSIGN(sliced, arr->SliceSafe(0, 7));
  check_data(*sliced, {1, 2, 3, 4, 5, 6, 7});

  ASSERT_OK_AND_ASSIGN(sliced, arr->SliceSafe(7, 0));
  check_data(*sliced, {});

  ASSERT_RAISES(Invalid, arr->SliceSafe(8, 0));
  ASSERT_RAISES(Invalid, arr->SliceSafe(0, 8));
  ASSERT_RAISES(Invalid, arr->SliceSafe(-1, 0));
  ASSERT_RAISES(Invalid, arr->SliceSafe(0, -1));
  ASSERT_RAISES(Invalid, arr->SliceSafe(6, 2));
  ASSERT_RAISES(Invalid, arr->SliceSafe(6, std::numeric_limits<int64_t>::max() - 5));

  ASSERT_OK_AND_ASSIGN(sliced, arr->SliceSafe(0));
  check_data(*sliced, original_data);

  ASSERT_OK_AND_ASSIGN(sliced, arr->SliceSafe(3));
  check_data(*sliced, {4, 5, 6, 7});

  ASSERT_OK_AND_ASSIGN(sliced, arr->SliceSafe(7));
  check_data(*sliced, {});

  ASSERT_RAISES(Invalid, arr->SliceSafe(8));
  ASSERT_RAISES(Invalid, arr->SliceSafe(-1));
}

Status MakeArrayFromValidBytes(const std::vector<uint8_t>& v, MemoryPool* pool,
                               std::shared_ptr<Array>* out) {
  int64_t null_count = v.size() - std::accumulate(v.begin(), v.end(), 0);

  ARROW_ASSIGN_OR_RAISE(auto null_buf, internal::BytesToBits(v));

  TypedBufferBuilder<int32_t> value_builder(pool);
  for (size_t i = 0; i < v.size(); ++i) {
    RETURN_NOT_OK(value_builder.Append(0));
  }

  std::shared_ptr<Buffer> values;
  RETURN_NOT_OK(value_builder.Finish(&values));
  *out = std::make_shared<Int32Array>(v.size(), values, null_buf, null_count);
  return Status::OK();
}

TEST_F(TestArray, TestEquality) {
  std::shared_ptr<Array> array, equal_array, unequal_array;

  ASSERT_OK(MakeArrayFromValidBytes({1, 0, 1, 1, 0, 1, 0, 0}, pool_, &array));
  ASSERT_OK(MakeArrayFromValidBytes({1, 0, 1, 1, 0, 1, 0, 0}, pool_, &equal_array));
  ASSERT_OK(MakeArrayFromValidBytes({1, 1, 1, 1, 0, 1, 0, 0}, pool_, &unequal_array));

  EXPECT_TRUE(array->Equals(array));
  EXPECT_TRUE(array->Equals(equal_array));
  EXPECT_TRUE(equal_array->Equals(array));
  EXPECT_FALSE(equal_array->Equals(unequal_array));
  EXPECT_FALSE(unequal_array->Equals(equal_array));
  EXPECT_TRUE(array->RangeEquals(4, 8, 4, unequal_array));
  EXPECT_FALSE(array->RangeEquals(0, 4, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(0, 8, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(1, 2, 1, unequal_array));

  auto timestamp_ns_array = std::make_shared<NumericArray<TimestampType>>(
      timestamp(TimeUnit::NANO), array->length(), array->data()->buffers[1],
      array->data()->buffers[0], array->null_count());
  auto timestamp_us_array = std::make_shared<NumericArray<TimestampType>>(
      timestamp(TimeUnit::MICRO), array->length(), array->data()->buffers[1],
      array->data()->buffers[0], array->null_count());
  ASSERT_FALSE(array->Equals(timestamp_ns_array));
  // ARROW-2567: Ensure that not only the type id but also the type equality
  // itself is checked.
  ASSERT_FALSE(timestamp_us_array->Equals(timestamp_ns_array));
  ASSERT_TRUE(timestamp_us_array->RangeEquals(0, 1, 0, timestamp_us_array));
  ASSERT_FALSE(timestamp_us_array->RangeEquals(0, 1, 0, timestamp_ns_array));
}

TEST_F(TestArray, TestNullArrayEquality) {
  auto array_1 = std::make_shared<NullArray>(10);
  auto array_2 = std::make_shared<NullArray>(10);
  auto array_3 = std::make_shared<NullArray>(20);

  EXPECT_TRUE(array_1->Equals(array_1));
  EXPECT_TRUE(array_1->Equals(array_2));
  EXPECT_FALSE(array_1->Equals(array_3));
}

TEST_F(TestArray, SliceRecomputeNullCount) {
  std::vector<uint8_t> valid_bytes = {1, 0, 1, 1, 0, 1, 0, 0, 0};

  std::shared_ptr<Array> array;
  ASSERT_OK(MakeArrayFromValidBytes(valid_bytes, pool_, &array));

  ASSERT_EQ(5, array->null_count());

  auto slice = array->Slice(1, 4);
  ASSERT_EQ(2, slice->null_count());

  slice = array->Slice(4);
  ASSERT_EQ(4, slice->null_count());

  auto slice2 = slice->Slice(0);
  ASSERT_EQ(4, slice2->null_count());

  slice = array->Slice(0);
  ASSERT_EQ(5, slice->null_count());

  // No bitmap, compute 0
  const int kBufferSize = 64;
  ASSERT_OK_AND_ASSIGN(auto data, AllocateBuffer(kBufferSize, pool_));
  memset(data->mutable_data(), 0, kBufferSize);

  auto arr = std::make_shared<Int32Array>(16, std::move(data), nullptr, -1);
  ASSERT_EQ(0, arr->null_count());
}

TEST_F(TestArray, NullArraySliceNullCount) {
  auto null_arr = std::make_shared<NullArray>(10);
  auto null_arr_sliced = null_arr->Slice(3, 6);

  // The internal null count is 6, does not require recomputation
  ASSERT_EQ(6, null_arr_sliced->data()->null_count);

  ASSERT_EQ(6, null_arr_sliced->null_count());
}

TEST_F(TestArray, TestIsNullIsValid) {
  // clang-format off
  std::vector<uint8_t> null_bitmap = {1, 0, 1, 1, 0, 1, 0, 0,
                                      1, 0, 1, 1, 0, 1, 0, 0,
                                      1, 0, 1, 1, 0, 1, 0, 0,
                                      1, 0, 1, 1, 0, 1, 0, 0,
                                      1, 0, 0, 1};
  // clang-format on
  int64_t null_count = 0;
  for (uint8_t x : null_bitmap) {
    if (x == 0) {
      ++null_count;
    }
  }

  ASSERT_OK_AND_ASSIGN(auto null_buf, internal::BytesToBits(null_bitmap));

  std::unique_ptr<Array> arr;
  arr.reset(new Int32Array(null_bitmap.size(), nullptr, null_buf, null_count));

  ASSERT_EQ(null_count, arr->null_count());
  ASSERT_EQ(5, null_buf->size());

  ASSERT_TRUE(arr->null_bitmap()->Equals(*null_buf.get()));

  for (size_t i = 0; i < null_bitmap.size(); ++i) {
    EXPECT_EQ(null_bitmap[i] != 0, !arr->IsNull(i)) << i;
    EXPECT_EQ(null_bitmap[i] != 0, arr->IsValid(i)) << i;
  }
}

TEST_F(TestArray, TestIsNullIsValidNoNulls) {
  const int64_t size = 10;

  std::unique_ptr<Array> arr;
  arr.reset(new Int32Array(size, nullptr, nullptr, 0));

  for (size_t i = 0; i < size; ++i) {
    EXPECT_TRUE(arr->IsValid(i));
    EXPECT_FALSE(arr->IsNull(i));
  }
}

TEST_F(TestArray, BuildLargeInMemoryArray) {
#ifdef NDEBUG
  const int64_t length = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1;
#elif !defined(ARROW_VALGRIND)
  // use a smaller size since the insert function isn't optimized properly on debug and
  // the test takes a long time to complete
  const int64_t length = 2 << 24;
#else
  // use an even smaller size with valgrind
  const int64_t length = 2 << 20;
#endif

  BooleanBuilder builder;
  std::vector<bool> zeros(length);
  ASSERT_OK(builder.AppendValues(zeros));

  std::shared_ptr<Array> result;
  FinishAndCheckPadding(&builder, &result);

  ASSERT_EQ(length, result->length());
}

TEST_F(TestArray, TestMakeArrayOfNull) {
  std::shared_ptr<DataType> types[] = {
      // clang-format off
      null(),
      boolean(),
      int8(),
      uint16(),
      int32(),
      uint64(),
      float64(),
      binary(),
      large_binary(),
      fixed_size_binary(3),
      decimal(16, 4),
      utf8(),
      large_utf8(),
      list(utf8()),
      list(int64()),  // ARROW-9071
      large_list(large_utf8()),
      fixed_size_list(utf8(), 3),
      fixed_size_list(int64(), 4),
      dictionary(int32(), utf8()),
      struct_({field("a", utf8()), field("b", int32())}),
      // clang-format on
  };

  for (int64_t length : {0, 1, 16, 133}) {
    for (auto type : types) {
      ASSERT_OK_AND_ASSIGN(auto array, MakeArrayOfNull(type, length));
      ASSERT_OK(array->ValidateFull());
      ASSERT_EQ(array->length(), length);
      ASSERT_EQ(array->null_count(), length);
      for (int64_t i = 0; i < length; ++i) {
        ASSERT_TRUE(array->IsNull(i));
        ASSERT_FALSE(array->IsValid(i));
      }
    }
  }
}

TEST_F(TestArray, TestMakeArrayOfNullUnion) {
  // Unions need special checking -- the top level null count is 0 (per
  // ARROW-9222) so we check the first child to make sure is contains all nulls
  // and check that the type_ids all point to the first child
  const int64_t union_length = 10;
  auto s_union_ty = sparse_union({field("a", utf8()), field("b", int32())}, {0, 1});
  ASSERT_OK_AND_ASSIGN(auto s_union_nulls, MakeArrayOfNull(s_union_ty, union_length));
  ASSERT_EQ(s_union_nulls->null_count(), 0);
  {
    const auto& typed_union = checked_cast<const SparseUnionArray&>(*s_union_nulls);
    ASSERT_EQ(typed_union.field(0)->null_count(), union_length);

    // Check type codes are all 0
    for (int i = 0; i < union_length; ++i) {
      ASSERT_EQ(typed_union.raw_type_codes()[i], 0);
    }
  }

  auto d_union_ty = dense_union({field("a", utf8()), field("b", int32())}, {0, 1});
  ASSERT_OK_AND_ASSIGN(auto d_union_nulls, MakeArrayOfNull(d_union_ty, union_length));
  ASSERT_EQ(d_union_nulls->null_count(), 0);
  {
    const auto& typed_union = checked_cast<const DenseUnionArray&>(*d_union_nulls);

    // Child field has length 1 which is a null element
    ASSERT_EQ(typed_union.field(0)->length(), 1);
    ASSERT_EQ(typed_union.field(0)->null_count(), 1);

    // Check type codes are all 0 and the offsets point to the first element of
    // the first child
    for (int i = 0; i < union_length; ++i) {
      ASSERT_EQ(typed_union.raw_type_codes()[i], 0);
      ASSERT_EQ(typed_union.raw_value_offsets()[i], 0);
    }
  }
}

void AssertAppendScalar(MemoryPool* pool, const std::shared_ptr<Scalar>& scalar) {
  std::unique_ptr<arrow::ArrayBuilder> builder;
  auto null_scalar = MakeNullScalar(scalar->type);
  ASSERT_OK(MakeBuilder(pool, scalar->type, &builder));
  ASSERT_OK(builder->AppendScalar(*scalar));
  ASSERT_OK(builder->AppendScalar(*scalar));
  ASSERT_OK(builder->AppendScalar(*null_scalar));
  ASSERT_OK(builder->AppendScalars({scalar, null_scalar}));
  ASSERT_OK(builder->AppendScalar(*scalar, /*n_repeats=*/2));
  ASSERT_OK(builder->AppendScalar(*null_scalar, /*n_repeats=*/2));

  std::shared_ptr<Array> out;
  FinishAndCheckPadding(builder.get(), &out);
  ASSERT_OK(out->ValidateFull());
  AssertTypeEqual(scalar->type, out->type());
  ASSERT_EQ(out->length(), 9);

  const bool can_check_nulls = internal::HasValidityBitmap(out->type()->id());

  if (can_check_nulls) {
    ASSERT_EQ(out->null_count(), 4);
  }
  for (const auto index : {0, 1, 3, 5, 6}) {
    ASSERT_FALSE(out->IsNull(index));
    ASSERT_OK_AND_ASSIGN(auto scalar_i, out->GetScalar(index));
    AssertScalarsEqual(*scalar, *scalar_i, /*verbose=*/true);
  }
  for (const auto index : {2, 4, 7, 8}) {
    ASSERT_EQ(out->IsNull(index), can_check_nulls);
    ASSERT_OK_AND_ASSIGN(auto scalar_i, out->GetScalar(index));
    AssertScalarsEqual(*null_scalar, *scalar_i, /*verbose=*/true);
  }
}

static ScalarVector GetScalars() {
  auto hello = Buffer::FromString("hello");
  DayTimeIntervalType::DayMilliseconds daytime{1, 100};

  FieldVector union_fields{field("string", utf8()), field("number", int32()),
                           field("other_number", int32())};
  std::vector<int8_t> union_type_codes{5, 6, 42};

  const auto sparse_union_ty = ::arrow::sparse_union(union_fields, union_type_codes);
  const auto dense_union_ty = ::arrow::dense_union(union_fields, union_type_codes);

  return {
      std::make_shared<BooleanScalar>(false), std::make_shared<Int8Scalar>(3),
      std::make_shared<UInt16Scalar>(3), std::make_shared<Int32Scalar>(3),
      std::make_shared<UInt64Scalar>(3), std::make_shared<DoubleScalar>(3.0),
      std::make_shared<Date32Scalar>(10), std::make_shared<Date64Scalar>(11),
      std::make_shared<Time32Scalar>(1000, time32(TimeUnit::SECOND)),
      std::make_shared<Time64Scalar>(1111, time64(TimeUnit::MICRO)),
      std::make_shared<TimestampScalar>(1111, timestamp(TimeUnit::MILLI)),
      std::make_shared<MonthIntervalScalar>(1),
      std::make_shared<DayTimeIntervalScalar>(daytime),
      std::make_shared<DurationScalar>(60, duration(TimeUnit::SECOND)),
      std::make_shared<BinaryScalar>(hello), std::make_shared<LargeBinaryScalar>(hello),
      std::make_shared<FixedSizeBinaryScalar>(
          hello, fixed_size_binary(static_cast<int32_t>(hello->size()))),
      std::make_shared<Decimal128Scalar>(Decimal128(10), decimal(16, 4)),
      std::make_shared<Decimal256Scalar>(Decimal256(10), decimal(76, 38)),
      std::make_shared<StringScalar>(hello), std::make_shared<LargeStringScalar>(hello),
      std::make_shared<ListScalar>(ArrayFromJSON(int8(), "[1, 2, 3]")),
      std::make_shared<LargeListScalar>(ArrayFromJSON(int8(), "[1, 1, 2, 2, 3, 3]")),
      std::make_shared<FixedSizeListScalar>(ArrayFromJSON(int8(), "[1, 2, 3, 4]")),
      std::make_shared<StructScalar>(
          ScalarVector{
              std::make_shared<Int32Scalar>(2),
              std::make_shared<Int32Scalar>(6),
          },
          struct_({field("min", int32()), field("max", int32())})),
      // Same values, different union type codes
      std::make_shared<SparseUnionScalar>(std::make_shared<Int32Scalar>(100), 6,
                                          sparse_union_ty),
      std::make_shared<SparseUnionScalar>(std::make_shared<Int32Scalar>(100), 42,
                                          sparse_union_ty),
      std::make_shared<DenseUnionScalar>(std::make_shared<Int32Scalar>(101), 6,
                                         dense_union_ty),
      std::make_shared<DenseUnionScalar>(std::make_shared<Int32Scalar>(101), 42,
                                         dense_union_ty)};
}

TEST_F(TestArray, TestMakeArrayFromScalar) {
  ASSERT_OK_AND_ASSIGN(auto null_array, MakeArrayFromScalar(NullScalar(), 5));
  ASSERT_OK(null_array->ValidateFull());
  ASSERT_EQ(null_array->length(), 5);
  ASSERT_EQ(null_array->null_count(), 5);

  auto scalars = GetScalars();

  for (int64_t length : {16}) {
    for (auto scalar : scalars) {
      ASSERT_OK_AND_ASSIGN(auto array, MakeArrayFromScalar(*scalar, length));
      ASSERT_OK(array->ValidateFull());
      ASSERT_EQ(array->length(), length);
      ASSERT_EQ(array->null_count(), 0);

      // test case for ARROW-13321
      for (int64_t i : std::vector<int64_t>{0, length / 2, length - 1}) {
        ASSERT_OK_AND_ASSIGN(auto s, array->GetScalar(i));
        AssertScalarsEqual(*s, *scalar, /*verbose=*/true);
      }
    }
  }

  for (auto scalar : scalars) {
    AssertAppendScalar(pool_, scalar);
  }
}

TEST_F(TestArray, TestMakeArrayFromScalarSliced) {
  // Regression test for ARROW-13437
  auto scalars = GetScalars();

  for (auto scalar : scalars) {
    SCOPED_TRACE(scalar->type->ToString());
    ASSERT_OK_AND_ASSIGN(auto array, MakeArrayFromScalar(*scalar, 32));
    auto sliced = array->Slice(1, 4);
    ASSERT_EQ(sliced->length(), 4);
    ASSERT_EQ(sliced->null_count(), 0);
    ARROW_EXPECT_OK(sliced->ValidateFull());
  }
}

TEST_F(TestArray, TestMakeArrayFromDictionaryScalar) {
  auto dictionary = ArrayFromJSON(utf8(), R"(["foo", "bar", "baz"])");
  auto type = std::make_shared<DictionaryType>(int8(), utf8());
  ASSERT_OK_AND_ASSIGN(auto value, MakeScalar(int8(), 1));
  auto scalar = DictionaryScalar({value, dictionary}, type);

  ASSERT_OK_AND_ASSIGN(auto array, MakeArrayFromScalar(scalar, 4));
  ASSERT_OK(array->ValidateFull());
  ASSERT_EQ(array->length(), 4);
  ASSERT_EQ(array->null_count(), 0);

  for (int i = 0; i < 4; i++) {
    ASSERT_OK_AND_ASSIGN(auto item, array->GetScalar(i));
    ASSERT_TRUE(item->Equals(scalar));
  }
}

TEST_F(TestArray, TestMakeArrayFromMapScalar) {
  auto value =
      ArrayFromJSON(struct_({field("key", utf8(), false), field("value", int8())}),
                    R"([{"key": "a", "value": 1}, {"key": "b", "value": 2}])");
  auto scalar = MapScalar(value);

  ASSERT_OK_AND_ASSIGN(auto array, MakeArrayFromScalar(scalar, 11));
  ASSERT_OK(array->ValidateFull());
  ASSERT_EQ(array->length(), 11);
  ASSERT_EQ(array->null_count(), 0);

  for (int i = 0; i < 11; i++) {
    ASSERT_OK_AND_ASSIGN(auto item, array->GetScalar(i));
    ASSERT_TRUE(item->Equals(scalar));
  }

  AssertAppendScalar(pool_, std::make_shared<MapScalar>(scalar));
}

TEST_F(TestArray, ValidateBuffersPrimitive) {
  auto empty_buffer = std::make_shared<Buffer>("");
  auto null_buffer = Buffer::FromString("\xff");
  auto data_buffer = Buffer::FromString("123456789abcdef0");

  auto data = ArrayData::Make(int64(), 2, {null_buffer, data_buffer});
  auto array = MakeArray(data);
  ASSERT_OK(array->ValidateFull());
  data = ArrayData::Make(boolean(), 8, {null_buffer, data_buffer});
  array = MakeArray(data);
  ASSERT_OK(array->ValidateFull());

  // Null buffer too small
  data = ArrayData::Make(int64(), 2, {empty_buffer, data_buffer});
  array = MakeArray(data);
  ASSERT_RAISES(Invalid, array->Validate());
  data = ArrayData::Make(boolean(), 9, {null_buffer, data_buffer});
  array = MakeArray(data);
  ASSERT_RAISES(Invalid, array->Validate());

  // Data buffer too small
  data = ArrayData::Make(int64(), 3, {null_buffer, data_buffer});
  array = MakeArray(data);
  ASSERT_RAISES(Invalid, array->Validate());

  // Null buffer absent but null_count > 0.
  data = ArrayData::Make(int64(), 2, {nullptr, data_buffer}, 1);
  array = MakeArray(data);
  ASSERT_RAISES(Invalid, array->Validate());

  //
  // With offset > 0
  //
  data = ArrayData::Make(int64(), 1, {null_buffer, data_buffer}, kUnknownNullCount, 1);
  array = MakeArray(data);
  ASSERT_OK(array->ValidateFull());
  data = ArrayData::Make(boolean(), 6, {null_buffer, data_buffer}, kUnknownNullCount, 2);
  array = MakeArray(data);
  ASSERT_OK(array->ValidateFull());

  // Null buffer too small
  data = ArrayData::Make(boolean(), 7, {null_buffer, data_buffer}, kUnknownNullCount, 2);
  array = MakeArray(data);
  ASSERT_RAISES(Invalid, array->Validate());

  // Data buffer too small
  data = ArrayData::Make(int64(), 2, {null_buffer, data_buffer}, kUnknownNullCount, 1);
  array = MakeArray(data);
  ASSERT_RAISES(Invalid, array->Validate());
}

// ----------------------------------------------------------------------
// Null type tests

TEST(TestNullBuilder, Basics) {
  NullBuilder builder;
  std::shared_ptr<Array> array;

  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Append(nullptr));
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.AppendNulls(2));
  ASSERT_EQ(5, builder.null_count());
  ASSERT_OK(builder.Finish(&array));

  const auto& null_array = checked_cast<NullArray&>(*array);
  ASSERT_EQ(null_array.length(), 5);
  ASSERT_EQ(null_array.null_count(), 5);
}

// ----------------------------------------------------------------------
// Primitive type tests

TEST(TestPrimitiveArray, CtorNoValidityBitmap) {
  // ARROW-8863
  std::shared_ptr<Buffer> data = *AllocateBuffer(40);
  Int32Array arr(10, data);
  ASSERT_EQ(arr.data()->null_count, 0);
}

TEST_F(TestBuilder, TestReserve) {
  UInt8Builder builder(pool_);

  ASSERT_OK(builder.Resize(1000));
  ASSERT_EQ(1000, builder.capacity());

  // Reserve overallocates for small upsizes.
  ASSERT_OK(builder.Reserve(1030));
  ASSERT_GE(builder.capacity(), 2000);
}

TEST_F(TestBuilder, TestResizeDownsize) {
  UInt8Builder builder(pool_);

  ASSERT_OK(builder.Resize(1000));
  ASSERT_EQ(1000, builder.capacity());
  ASSERT_EQ(0, builder.length());
  ASSERT_OK(builder.AppendNulls(500));
  ASSERT_EQ(1000, builder.capacity());
  ASSERT_EQ(500, builder.length());

  // Can downsize below current capacity
  ASSERT_OK(builder.Resize(500));
  // ... but not below current populated length
  ASSERT_RAISES(Invalid, builder.Resize(499));
  ASSERT_GE(500, builder.capacity());
  ASSERT_EQ(500, builder.length());
}

template <typename Attrs>
class TestPrimitiveBuilder : public TestBuilder {
 public:
  typedef typename Attrs::ArrayType ArrayType;
  typedef typename Attrs::BuilderType BuilderType;
  typedef typename Attrs::T T;
  typedef typename Attrs::Type Type;

  virtual void SetUp() {
    TestBuilder::SetUp();

    type_ = Attrs::type();

    std::unique_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_.reset(checked_cast<BuilderType*>(tmp.release()));

    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_nn_.reset(checked_cast<BuilderType*>(tmp.release()));
  }

  void RandomData(int64_t N, double pct_null = 0.1) {
    Attrs::draw(N, &draws_);

    valid_bytes_.resize(static_cast<size_t>(N));
    random_null_bytes(N, pct_null, valid_bytes_.data());
  }

  void Check(const std::unique_ptr<BuilderType>& builder, bool nullable) {
    int64_t size = builder->length();
    auto ex_data = Buffer::Wrap(draws_.data(), size);

    std::shared_ptr<Buffer> ex_null_bitmap;
    int64_t ex_null_count = 0;

    if (nullable) {
      ASSERT_OK_AND_ASSIGN(ex_null_bitmap, internal::BytesToBits(valid_bytes_));
      ex_null_count = CountNulls(valid_bytes_);
    } else {
      ex_null_bitmap = nullptr;
    }

    auto expected =
        std::make_shared<ArrayType>(size, ex_data, ex_null_bitmap, ex_null_count);

    std::shared_ptr<Array> out;
    FinishAndCheckPadding(builder.get(), &out);

    std::shared_ptr<ArrayType> result = checked_pointer_cast<ArrayType>(out);

    // Builder is now reset
    ASSERT_EQ(0, builder->length());
    ASSERT_EQ(0, builder->capacity());
    ASSERT_EQ(0, builder->null_count());

    ASSERT_EQ(ex_null_count, result->null_count());
    ASSERT_TRUE(result->Equals(*expected));
  }

  void FlipValue(T* ptr) {
    auto byteptr = reinterpret_cast<uint8_t*>(ptr);
    *byteptr = static_cast<uint8_t>(~*byteptr);
  }

 protected:
  std::unique_ptr<BuilderType> builder_;
  std::unique_ptr<BuilderType> builder_nn_;

  std::vector<T> draws_;
  std::vector<uint8_t> valid_bytes_;
};

/// \brief uint8_t isn't a valid template parameter to uniform_int_distribution, so
/// we use SampleType to determine which kind of integer to use to sample.
template <typename T, typename = enable_if_t<std::is_integral<T>::value, T>>
struct UniformIntSampleType {
  using type = T;
};

template <>
struct UniformIntSampleType<uint8_t> {
  using type = uint16_t;
};

template <>
struct UniformIntSampleType<int8_t> {
  using type = int16_t;
};

#define PTYPE_DECL(CapType, c_type)     \
  typedef CapType##Array ArrayType;     \
  typedef CapType##Builder BuilderType; \
  typedef CapType##Type Type;           \
  typedef c_type T;                     \
                                        \
  static std::shared_ptr<DataType> type() { return std::make_shared<Type>(); }

#define PINT_DECL(CapType, c_type)                                                 \
  struct P##CapType {                                                              \
    PTYPE_DECL(CapType, c_type)                                                    \
    static void draw(int64_t N, std::vector<T>* draws) {                           \
      using sample_type = typename UniformIntSampleType<c_type>::type;             \
      const T lower = std::numeric_limits<T>::min();                               \
      const T upper = std::numeric_limits<T>::max();                               \
      randint(N, static_cast<sample_type>(lower), static_cast<sample_type>(upper), \
              draws);                                                              \
    }                                                                              \
  }

#define PFLOAT_DECL(CapType, c_type, LOWER, UPPER)       \
  struct P##CapType {                                    \
    PTYPE_DECL(CapType, c_type)                          \
    static void draw(int64_t N, std::vector<T>* draws) { \
      random_real(N, 0, LOWER, UPPER, draws);            \
    }                                                    \
  }

PINT_DECL(UInt8, uint8_t);
PINT_DECL(UInt16, uint16_t);
PINT_DECL(UInt32, uint32_t);
PINT_DECL(UInt64, uint64_t);

PINT_DECL(Int8, int8_t);
PINT_DECL(Int16, int16_t);
PINT_DECL(Int32, int32_t);
PINT_DECL(Int64, int64_t);

PFLOAT_DECL(Float, float, -1000.0f, 1000.0f);
PFLOAT_DECL(Double, double, -1000.0, 1000.0);

struct PBoolean {
  PTYPE_DECL(Boolean, uint8_t)
};

template <>
void TestPrimitiveBuilder<PBoolean>::RandomData(int64_t N, double pct_null) {
  draws_.resize(static_cast<size_t>(N));
  valid_bytes_.resize(static_cast<size_t>(N));

  random_null_bytes(N, 0.5, draws_.data());
  random_null_bytes(N, pct_null, valid_bytes_.data());
}

template <>
void TestPrimitiveBuilder<PBoolean>::FlipValue(T* ptr) {
  *ptr = !*ptr;
}

template <>
void TestPrimitiveBuilder<PBoolean>::Check(const std::unique_ptr<BooleanBuilder>& builder,
                                           bool nullable) {
  const int64_t size = builder->length();

  // Build expected result array
  std::shared_ptr<Buffer> ex_data;
  std::shared_ptr<Buffer> ex_null_bitmap;
  int64_t ex_null_count = 0;

  ASSERT_OK_AND_ASSIGN(ex_data, internal::BytesToBits(draws_));
  if (nullable) {
    ASSERT_OK_AND_ASSIGN(ex_null_bitmap, internal::BytesToBits(valid_bytes_));
    ex_null_count = CountNulls(valid_bytes_);
  } else {
    ex_null_bitmap = nullptr;
  }
  auto expected =
      std::make_shared<BooleanArray>(size, ex_data, ex_null_bitmap, ex_null_count);
  ASSERT_EQ(size, expected->length());

  // Finish builder and check result array
  std::shared_ptr<Array> out;
  FinishAndCheckPadding(builder.get(), &out);

  std::shared_ptr<BooleanArray> result = checked_pointer_cast<BooleanArray>(out);

  ASSERT_EQ(ex_null_count, result->null_count());
  ASSERT_EQ(size, result->length());

  for (int64_t i = 0; i < size; ++i) {
    if (nullable) {
      ASSERT_EQ(valid_bytes_[i] == 0, result->IsNull(i)) << i;
    } else {
      ASSERT_FALSE(result->IsNull(i));
    }
    if (!result->IsNull(i)) {
      bool actual = BitUtil::GetBit(result->values()->data(), i);
      ASSERT_EQ(draws_[i] != 0, actual) << i;
    }
  }
  AssertArraysEqual(*result, *expected);

  // buffers are correctly sized
  if (result->data()->buffers[0]) {
    ASSERT_EQ(result->data()->buffers[0]->size(), BitUtil::BytesForBits(size));
  } else {
    ASSERT_EQ(result->data()->null_count, 0);
  }
  ASSERT_EQ(result->data()->buffers[1]->size(), BitUtil::BytesForBits(size));

  // Builder is now reset
  ASSERT_EQ(0, builder->length());
  ASSERT_EQ(0, builder->capacity());
  ASSERT_EQ(0, builder->null_count());
}

TEST(TestBooleanArray, TrueCountFalseCount) {
  random::RandomArrayGenerator rng(/*seed=*/0);

  const int64_t length = 10000;
  auto arr = rng.Boolean(length, /*true_probability=*/0.5, /*null_probability=*/0.1);

  auto CheckArray = [&](const BooleanArray& values) {
    int64_t expected_false = 0;
    int64_t expected_true = 0;
    for (int64_t i = 0; i < values.length(); ++i) {
      if (values.IsValid(i)) {
        if (values.Value(i)) {
          ++expected_true;
        } else {
          ++expected_false;
        }
      }
    }
    ASSERT_EQ(values.true_count(), expected_true);
    ASSERT_EQ(values.false_count(), expected_false);
  };

  CheckArray(checked_cast<const BooleanArray&>(*arr));
  CheckArray(checked_cast<const BooleanArray&>(*arr->Slice(5)));
  CheckArray(checked_cast<const BooleanArray&>(*arr->Slice(0, 0)));
}

TEST(TestPrimitiveAdHoc, TestType) {
  Int8Builder i8(default_memory_pool());
  ASSERT_TRUE(i8.type()->Equals(int8()));

  DictionaryBuilder<Int8Type> d_i8(utf8());
  ASSERT_TRUE(d_i8.type()->Equals(dictionary(int8(), utf8())));

  Dictionary32Builder<Int8Type> d32_i8(utf8());
  ASSERT_TRUE(d32_i8.type()->Equals(dictionary(int32(), utf8())));
}

TEST(NumericBuilderAccessors, TestSettersGetters) {
  int64_t datum = 42;
  int64_t new_datum = 43;
  NumericBuilder<Int64Type> builder(int64(), default_memory_pool());

  builder.Reset();
  ASSERT_OK(builder.Append(datum));
  ASSERT_EQ(builder.GetValue(0), datum);

  // Now update the value.
  builder[0] = new_datum;

  ASSERT_EQ(builder.GetValue(0), new_datum);
  ASSERT_EQ(((const NumericBuilder<Int64Type>&)builder)[0], new_datum);
}

typedef ::testing::Types<PBoolean, PUInt8, PUInt16, PUInt32, PUInt64, PInt8, PInt16,
                         PInt32, PInt64, PFloat, PDouble>
    Primitives;

TYPED_TEST_SUITE(TestPrimitiveBuilder, Primitives);

TYPED_TEST(TestPrimitiveBuilder, TestInit) {
  ASSERT_OK(this->builder_->Reserve(1000));
  ASSERT_EQ(1000, this->builder_->capacity());

  // Small upsize => should overallocate
  ASSERT_OK(this->builder_->Reserve(1200));
  ASSERT_GE(2000, this->builder_->capacity());

  // Large upsize => should allocate exactly
  ASSERT_OK(this->builder_->Reserve(32768));
  ASSERT_EQ(32768, this->builder_->capacity());

  // unsure if this should go in all builder classes
  ASSERT_EQ(0, this->builder_->num_children());
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendNull) {
  int64_t size = 1000;
  for (int64_t i = 0; i < size; ++i) {
    ASSERT_OK(this->builder_->AppendNull());
    ASSERT_EQ(i + 1, this->builder_->null_count());
  }

  std::shared_ptr<Array> out;
  FinishAndCheckPadding(this->builder_.get(), &out);
  auto result = checked_pointer_cast<typename TypeParam::ArrayType>(out);

  for (int64_t i = 0; i < size; ++i) {
    ASSERT_TRUE(result->IsNull(i)) << i;
  }

  for (auto buffer : result->data()->buffers) {
    for (int64_t i = 0; i < buffer->capacity(); i++) {
      // Validates current implementation, algorithms shouldn't rely on this
      ASSERT_EQ(0, *(buffer->data() + i)) << i;
    }
  }
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendNulls) {
  const int64_t size = 10;
  ASSERT_OK(this->builder_->AppendNulls(size));
  ASSERT_EQ(size, this->builder_->null_count());

  std::shared_ptr<Array> result;
  FinishAndCheckPadding(this->builder_.get(), &result);

  for (int64_t i = 0; i < size; ++i) {
    ASSERT_FALSE(result->IsValid(i));
  }

  for (auto buffer : result->data()->buffers) {
    for (int64_t i = 0; i < buffer->capacity(); i++) {
      // Validates current implementation, algorithms shouldn't rely on this
      ASSERT_EQ(0, *(buffer->data() + i)) << i;
    }
  }
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendEmptyValue) {
  ASSERT_OK(this->builder_->AppendNull());
  ASSERT_OK(this->builder_->AppendEmptyValue());
  ASSERT_OK(this->builder_->AppendNulls(2));
  ASSERT_OK(this->builder_->AppendEmptyValues(2));

  std::shared_ptr<Array> out;
  FinishAndCheckPadding(this->builder_.get(), &out);
  ASSERT_OK(out->ValidateFull());

  auto result = checked_pointer_cast<typename TypeParam::ArrayType>(out);
  ASSERT_EQ(result->length(), 6);
  ASSERT_EQ(result->null_count(), 3);

  ASSERT_TRUE(result->IsNull(0));
  ASSERT_FALSE(result->IsNull(1));
  ASSERT_TRUE(result->IsNull(2));
  ASSERT_TRUE(result->IsNull(3));
  ASSERT_FALSE(result->IsNull(4));
  ASSERT_FALSE(result->IsNull(5));

  // implementation detail: the value slots are 0-initialized
  for (int64_t i = 0; i < result->length(); ++i) {
    ASSERT_EQ(result->Value(i), 0);
  }
}

TYPED_TEST(TestPrimitiveBuilder, TestArrayDtorDealloc) {
  DECL_T();

  int64_t size = 1000;

  std::vector<T>& draws = this->draws_;
  std::vector<uint8_t>& valid_bytes = this->valid_bytes_;

  int64_t memory_before = this->pool_->bytes_allocated();

  this->RandomData(size);
  ASSERT_OK(this->builder_->Reserve(size));

  int64_t i;
  for (i = 0; i < size; ++i) {
    if (valid_bytes[i] > 0) {
      ASSERT_OK(this->builder_->Append(draws[i]));
    } else {
      ASSERT_OK(this->builder_->AppendNull());
    }
  }

  do {
    std::shared_ptr<Array> result;
    FinishAndCheckPadding(this->builder_.get(), &result);
  } while (false);

  ASSERT_EQ(memory_before, this->pool_->bytes_allocated());
}

TYPED_TEST(TestPrimitiveBuilder, Equality) {
  DECL_T();

  const int64_t size = 1000;
  this->RandomData(size);
  std::vector<T>& draws = this->draws_;
  std::vector<uint8_t>& valid_bytes = this->valid_bytes_;
  std::shared_ptr<Array> array, equal_array, unequal_array;
  auto builder = this->builder_.get();
  ASSERT_OK(MakeArray(valid_bytes, draws, size, builder, &array));
  ASSERT_OK(MakeArray(valid_bytes, draws, size, builder, &equal_array));

  // Make the not equal array by negating the first valid element with itself.
  const auto first_valid = std::find_if(valid_bytes.begin(), valid_bytes.end(),
                                        [](uint8_t valid) { return valid > 0; });
  const int64_t first_valid_idx = std::distance(valid_bytes.begin(), first_valid);
  // This should be true with a very high probability, but might introduce flakiness
  ASSERT_LT(first_valid_idx, size - 1);
  this->FlipValue(&draws[first_valid_idx]);
  ASSERT_OK(MakeArray(valid_bytes, draws, size, builder, &unequal_array));

  // test normal equality
  EXPECT_TRUE(array->Equals(array));
  EXPECT_TRUE(array->Equals(equal_array));
  EXPECT_TRUE(equal_array->Equals(array));
  EXPECT_FALSE(equal_array->Equals(unequal_array));
  EXPECT_FALSE(unequal_array->Equals(equal_array));

  // Test range equality
  EXPECT_FALSE(array->RangeEquals(0, first_valid_idx + 1, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(first_valid_idx, size, first_valid_idx, unequal_array));
  EXPECT_TRUE(array->RangeEquals(0, first_valid_idx, 0, unequal_array));
  EXPECT_TRUE(
      array->RangeEquals(first_valid_idx + 1, size, first_valid_idx + 1, unequal_array));
}

TYPED_TEST(TestPrimitiveBuilder, SliceEquality) {
  DECL_T();

  const int64_t size = 1000;
  this->RandomData(size);
  std::vector<T>& draws = this->draws_;
  std::vector<uint8_t>& valid_bytes = this->valid_bytes_;
  auto builder = this->builder_.get();

  std::shared_ptr<Array> array;
  ASSERT_OK(MakeArray(valid_bytes, draws, size, builder, &array));

  std::shared_ptr<Array> slice, slice2;

  slice = array->Slice(5);
  slice2 = array->Slice(5);
  ASSERT_EQ(size - 5, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(5, array->length(), 0, slice));

  // Chained slices
  slice2 = array->Slice(2)->Slice(3);
  ASSERT_TRUE(slice->Equals(slice2));

  slice = array->Slice(5, 10);
  slice2 = array->Slice(5, 10);
  ASSERT_EQ(10, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(5, 15, 0, slice));
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendScalar) {
  DECL_T();

  const int64_t size = 10000;

  std::vector<T>& draws = this->draws_;
  std::vector<uint8_t>& valid_bytes = this->valid_bytes_;

  this->RandomData(size);

  ASSERT_OK(this->builder_->Reserve(1000));
  ASSERT_OK(this->builder_nn_->Reserve(1000));

  int64_t null_count = 0;
  // Append the first 1000
  for (size_t i = 0; i < 1000; ++i) {
    if (valid_bytes[i] > 0) {
      ASSERT_OK(this->builder_->Append(draws[i]));
    } else {
      ASSERT_OK(this->builder_->AppendNull());
      ++null_count;
    }
    ASSERT_OK(this->builder_nn_->Append(draws[i]));
  }

  ASSERT_EQ(null_count, this->builder_->null_count());

  ASSERT_EQ(1000, this->builder_->length());
  ASSERT_EQ(1000, this->builder_->capacity());

  ASSERT_EQ(1000, this->builder_nn_->length());
  ASSERT_EQ(1000, this->builder_nn_->capacity());

  ASSERT_OK(this->builder_->Reserve(size - 1000));
  ASSERT_OK(this->builder_nn_->Reserve(size - 1000));

  // Append the next 9000
  for (size_t i = 1000; i < size; ++i) {
    if (valid_bytes[i] > 0) {
      ASSERT_OK(this->builder_->Append(draws[i]));
    } else {
      ASSERT_OK(this->builder_->AppendNull());
    }
    ASSERT_OK(this->builder_nn_->Append(draws[i]));
  }

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_GE(size, this->builder_->capacity());

  ASSERT_EQ(size, this->builder_nn_->length());
  ASSERT_GE(size, this->builder_nn_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendValues) {
  DECL_T();

  int64_t size = 10000;
  this->RandomData(size);

  std::vector<T>& draws = this->draws_;
  std::vector<uint8_t>& valid_bytes = this->valid_bytes_;

  // first slug
  int64_t K = 1000;

  ASSERT_OK(this->builder_->AppendValues(draws.data(), K, valid_bytes.data()));
  ASSERT_OK(this->builder_nn_->AppendValues(draws.data(), K));

  ASSERT_EQ(1000, this->builder_->length());
  ASSERT_EQ(1000, this->builder_->capacity());

  ASSERT_EQ(1000, this->builder_nn_->length());
  ASSERT_EQ(1000, this->builder_nn_->capacity());

  // Append the next 9000
  ASSERT_OK(
      this->builder_->AppendValues(draws.data() + K, size - K, valid_bytes.data() + K));
  ASSERT_OK(this->builder_nn_->AppendValues(draws.data() + K, size - K));

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_GE(size, this->builder_->capacity());

  ASSERT_EQ(size, this->builder_nn_->length());
  ASSERT_GE(size, this->builder_nn_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestTypedFinish) {
  DECL_T();

  int64_t size = 1000;
  this->RandomData(size);

  std::vector<T>& draws = this->draws_;
  std::vector<uint8_t>& valid_bytes = this->valid_bytes_;

  ASSERT_OK(this->builder_->AppendValues(draws.data(), size, valid_bytes.data()));
  std::shared_ptr<Array> result_untyped;
  ASSERT_OK(this->builder_->Finish(&result_untyped));

  ASSERT_OK(this->builder_->AppendValues(draws.data(), size, valid_bytes.data()));
  std::shared_ptr<typename TestFixture::ArrayType> result;
  ASSERT_OK(this->builder_->Finish(&result));

  AssertArraysEqual(*result_untyped, *result);
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendValuesIter) {
  int64_t size = 10000;
  this->RandomData(size);

  ASSERT_OK(this->builder_->AppendValues(this->draws_.begin(), this->draws_.end(),
                                         this->valid_bytes_.begin()));
  ASSERT_OK(this->builder_nn_->AppendValues(this->draws_.begin(), this->draws_.end()));

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_GE(size, this->builder_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendValuesIterNullValid) {
  int64_t size = 10000;
  this->RandomData(size);

  ASSERT_OK(this->builder_nn_->AppendValues(this->draws_.begin(),
                                            this->draws_.begin() + size / 2,
                                            static_cast<uint8_t*>(nullptr)));

  ASSERT_GE(size / 2, this->builder_nn_->capacity());

  ASSERT_OK(this->builder_nn_->AppendValues(this->draws_.begin() + size / 2,
                                            this->draws_.end(),
                                            static_cast<uint64_t*>(nullptr)));

  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendValuesLazyIter) {
  DECL_T();

  int64_t size = 10000;
  this->RandomData(size);

  auto& draws = this->draws_;
  auto& valid_bytes = this->valid_bytes_;

  auto halve = [&draws](int64_t index) { return draws[index] / 2; };
  auto lazy_iter = internal::MakeLazyRange(halve, size);

  ASSERT_OK(this->builder_->AppendValues(lazy_iter.begin(), lazy_iter.end(),
                                         valid_bytes.begin()));

  std::vector<T> halved;
  transform(draws.begin(), draws.end(), back_inserter(halved),
            [](T in) { return in / 2; });

  std::shared_ptr<Array> result;
  FinishAndCheckPadding(this->builder_.get(), &result);

  std::shared_ptr<Array> expected;
  ASSERT_OK(
      this->builder_->AppendValues(halved.data(), halved.size(), valid_bytes.data()));
  FinishAndCheckPadding(this->builder_.get(), &expected);

  ASSERT_TRUE(expected->Equals(result));
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendValuesIterConverted) {
  DECL_T();
  // find type we can safely convert the tested values to and from
  using conversion_type =
      typename std::conditional<std::is_floating_point<T>::value, double,
                                typename std::conditional<std::is_unsigned<T>::value,
                                                          uint64_t, int64_t>::type>::type;

  int64_t size = 10000;
  this->RandomData(size);

  // append convertible values
  std::vector<conversion_type> draws_converted(this->draws_.begin(), this->draws_.end());
  std::vector<int32_t> valid_bytes_converted(this->valid_bytes_.begin(),
                                             this->valid_bytes_.end());

  auto cast_values = internal::MakeLazyRange(
      [&draws_converted](int64_t index) {
        return static_cast<T>(draws_converted[index]);
      },
      size);
  auto cast_valid = internal::MakeLazyRange(
      [&valid_bytes_converted](int64_t index) {
        return static_cast<bool>(valid_bytes_converted[index]);
      },
      size);

  ASSERT_OK(this->builder_->AppendValues(cast_values.begin(), cast_values.end(),
                                         cast_valid.begin()));
  ASSERT_OK(this->builder_nn_->AppendValues(cast_values.begin(), cast_values.end()));

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_GE(size, this->builder_->capacity());

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_GE(size, this->builder_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestZeroPadded) {
  DECL_T();

  int64_t size = 10000;
  this->RandomData(size);

  std::vector<T>& draws = this->draws_;
  std::vector<uint8_t>& valid_bytes = this->valid_bytes_;

  // first slug
  int64_t K = 1000;

  ASSERT_OK(this->builder_->AppendValues(draws.data(), K, valid_bytes.data()));

  std::shared_ptr<Array> out;
  FinishAndCheckPadding(this->builder_.get(), &out);
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendValuesStdBool) {
  // ARROW-1383
  DECL_T();

  int64_t size = 10000;
  this->RandomData(size);

  std::vector<T>& draws = this->draws_;

  std::vector<bool> is_valid;

  // first slug
  int64_t K = 1000;

  for (int64_t i = 0; i < K; ++i) {
    is_valid.push_back(this->valid_bytes_[i] != 0);
  }
  ASSERT_OK(this->builder_->AppendValues(draws.data(), K, is_valid));
  ASSERT_OK(this->builder_nn_->AppendValues(draws.data(), K));

  ASSERT_EQ(1000, this->builder_->length());
  ASSERT_EQ(1000, this->builder_->capacity());
  ASSERT_EQ(1000, this->builder_nn_->length());
  ASSERT_EQ(1000, this->builder_nn_->capacity());

  // Append the next 9000
  is_valid.clear();
  std::vector<T> partial_draws;
  for (int64_t i = K; i < size; ++i) {
    partial_draws.push_back(draws[i]);
    is_valid.push_back(this->valid_bytes_[i] != 0);
  }

  ASSERT_OK(this->builder_->AppendValues(partial_draws, is_valid));
  ASSERT_OK(this->builder_nn_->AppendValues(partial_draws));

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_GE(size, this->builder_->capacity());

  ASSERT_EQ(size, this->builder_nn_->length());
  ASSERT_GE(size, this->builder_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestAdvance) {
  int64_t n = 1000;
  ASSERT_OK(this->builder_->Reserve(n));

  ASSERT_OK(this->builder_->Advance(100));
  ASSERT_EQ(100, this->builder_->length());

  ASSERT_OK(this->builder_->Advance(900));

  int64_t too_many = this->builder_->capacity() - 1000 + 1;
  ASSERT_RAISES(Invalid, this->builder_->Advance(too_many));
}

TYPED_TEST(TestPrimitiveBuilder, TestResize) {
  int64_t cap = kMinBuilderCapacity * 2;

  ASSERT_OK(this->builder_->Reserve(cap));
  ASSERT_EQ(cap, this->builder_->capacity());
}

TYPED_TEST(TestPrimitiveBuilder, TestReserve) {
  ASSERT_OK(this->builder_->Reserve(10));
  ASSERT_EQ(0, this->builder_->length());
  ASSERT_EQ(kMinBuilderCapacity, this->builder_->capacity());

  ASSERT_OK(this->builder_->Reserve(100));
  ASSERT_EQ(0, this->builder_->length());
  ASSERT_GE(100, this->builder_->capacity());
  ASSERT_OK(this->builder_->Advance(100));
  ASSERT_EQ(100, this->builder_->length());
  ASSERT_GE(100, this->builder_->capacity());

  ASSERT_RAISES(Invalid, this->builder_->Resize(1));
}

TEST(TestBooleanBuilder, AppendNullsAdvanceBuilder) {
  BooleanBuilder builder;

  std::vector<uint8_t> values = {1, 0, 0, 1};
  std::vector<uint8_t> is_valid = {1, 1, 0, 1};

  std::shared_ptr<Array> arr;
  ASSERT_OK(builder.AppendValues(values.data(), 2));
  ASSERT_OK(builder.AppendNulls(1));
  ASSERT_OK(builder.AppendValues(values.data() + 3, 1));
  ASSERT_OK(builder.Finish(&arr));

  ASSERT_EQ(1, arr->null_count());

  const auto& barr = static_cast<const BooleanArray&>(*arr);
  ASSERT_TRUE(barr.Value(0));
  ASSERT_FALSE(barr.Value(1));
  ASSERT_TRUE(barr.IsNull(2));
  ASSERT_TRUE(barr.Value(3));
}

TEST(TestBooleanBuilder, TestStdBoolVectorAppend) {
  BooleanBuilder builder;
  BooleanBuilder builder_nn;

  std::vector<bool> values, is_valid;

  const int length = 10000;
  random_is_valid(length, 0.5, &values);
  random_is_valid(length, 0.1, &is_valid);

  const int chunksize = 1000;
  for (int chunk = 0; chunk < length / chunksize; ++chunk) {
    std::vector<bool> chunk_values, chunk_is_valid;
    for (int i = chunk * chunksize; i < (chunk + 1) * chunksize; ++i) {
      chunk_values.push_back(values[i]);
      chunk_is_valid.push_back(is_valid[i]);
    }
    ASSERT_OK(builder.AppendValues(chunk_values, chunk_is_valid));
    ASSERT_OK(builder_nn.AppendValues(chunk_values));
  }

  std::shared_ptr<Array> result, result_nn;
  ASSERT_OK(builder.Finish(&result));
  ASSERT_OK(builder_nn.Finish(&result_nn));

  const auto& arr = checked_cast<const BooleanArray&>(*result);
  const auto& arr_nn = checked_cast<const BooleanArray&>(*result_nn);
  for (int i = 0; i < length; ++i) {
    if (is_valid[i]) {
      ASSERT_FALSE(arr.IsNull(i));
      ASSERT_EQ(values[i], arr.Value(i));
    } else {
      ASSERT_TRUE(arr.IsNull(i));
    }
    ASSERT_EQ(values[i], arr_nn.Value(i));
  }
}

template <typename TYPE>
void CheckApproxEquals() {
  std::shared_ptr<Array> a, b;
  std::shared_ptr<DataType> type = TypeTraits<TYPE>::type_singleton();

  ArrayFromVector<TYPE>(type, {true, false}, {0.5, 1.0}, &a);
  ArrayFromVector<TYPE>(type, {true, false}, {0.5000001f, 1.0000001f}, &b);
  ASSERT_TRUE(a->ApproxEquals(b));
  ASSERT_TRUE(b->ApproxEquals(a));
  ASSERT_TRUE(a->ApproxEquals(b, EqualOptions().nans_equal(true)));
  ASSERT_TRUE(b->ApproxEquals(a, EqualOptions().nans_equal(true)));

  ArrayFromVector<TYPE>(type, {true, false}, {0.5001f, 1.000001f}, &b);
  ASSERT_FALSE(a->ApproxEquals(b));
  ASSERT_FALSE(b->ApproxEquals(a));
  ASSERT_FALSE(a->ApproxEquals(b, EqualOptions().nans_equal(true)));
  ASSERT_FALSE(b->ApproxEquals(a, EqualOptions().nans_equal(true)));
  ASSERT_TRUE(a->ApproxEquals(b, EqualOptions().atol(1e-3)));
  ASSERT_TRUE(b->ApproxEquals(a, EqualOptions().atol(1e-3)));
  ASSERT_TRUE(a->ApproxEquals(b, EqualOptions().atol(1e-3).nans_equal(true)));
  ASSERT_TRUE(b->ApproxEquals(a, EqualOptions().atol(1e-3).nans_equal(true)));

  ArrayFromVector<TYPE>(type, {true, false}, {0.5, 1.25}, &b);
  ASSERT_TRUE(a->ApproxEquals(b));
  ASSERT_TRUE(b->ApproxEquals(a));
  ASSERT_TRUE(a->ApproxEquals(b, EqualOptions().nans_equal(true)));
  ASSERT_TRUE(b->ApproxEquals(a, EqualOptions().nans_equal(true)));

  // Mismatching validity
  ArrayFromVector<TYPE>(type, {true, false}, {0.5, 1.0}, &a);
  ArrayFromVector<TYPE>(type, {false, false}, {0.5, 1.0}, &b);
  ASSERT_FALSE(a->ApproxEquals(b));
  ASSERT_FALSE(b->ApproxEquals(a));
  ASSERT_FALSE(a->ApproxEquals(b, EqualOptions().nans_equal(true)));
  ASSERT_FALSE(b->ApproxEquals(a, EqualOptions().nans_equal(true)));

  ArrayFromVector<TYPE>(type, {false, true}, {0.5, 1.0}, &b);
  ASSERT_FALSE(a->ApproxEquals(b));
  ASSERT_FALSE(b->ApproxEquals(a));
  ASSERT_FALSE(a->ApproxEquals(b, EqualOptions().nans_equal(true)));
  ASSERT_FALSE(b->ApproxEquals(a, EqualOptions().nans_equal(true)));
}

template <typename TYPE>
void CheckSliceApproxEquals() {
  using T = typename TYPE::c_type;

  const int64_t kSize = 50;
  std::vector<T> draws1;
  std::vector<T> draws2;

  const uint32_t kSeed = 0;
  random_real(kSize, kSeed, 0.0, 100.0, &draws1);
  random_real(kSize, kSeed + 1, 0.0, 100.0, &draws2);

  // Make the draws equal in the sliced segment, but unequal elsewhere (to
  // catch not using the slice offset)
  for (int64_t i = 10; i < 30; ++i) {
    draws2[i] = draws1[i];
  }

  std::vector<bool> is_valid;
  random_is_valid(kSize, 0.1, &is_valid);

  std::shared_ptr<Array> array1, array2;
  ArrayFromVector<TYPE, T>(is_valid, draws1, &array1);
  ArrayFromVector<TYPE, T>(is_valid, draws2, &array2);

  std::shared_ptr<Array> slice1 = array1->Slice(10, 20);
  std::shared_ptr<Array> slice2 = array2->Slice(10, 20);

  ASSERT_TRUE(slice1->ApproxEquals(slice2));
}

template <typename TYPE>
void CheckFloatingNanEquality() {
  std::shared_ptr<Array> a, b;
  std::shared_ptr<DataType> type = TypeTraits<TYPE>::type_singleton();

  const auto nan_value = static_cast<typename TYPE::c_type>(NAN);

  // NaN in a null entry
  ArrayFromVector<TYPE>(type, {true, false}, {0.5, nan_value}, &a);
  ArrayFromVector<TYPE>(type, {true, false}, {0.5, nan_value}, &b);
  ASSERT_TRUE(a->Equals(b));
  ASSERT_TRUE(b->Equals(a));
  ASSERT_TRUE(a->ApproxEquals(b));
  ASSERT_TRUE(b->ApproxEquals(a));
  ASSERT_TRUE(a->RangeEquals(b, 0, 2, 0));
  ASSERT_TRUE(b->RangeEquals(a, 0, 2, 0));
  ASSERT_TRUE(a->RangeEquals(b, 1, 2, 1));
  ASSERT_TRUE(b->RangeEquals(a, 1, 2, 1));

  // NaN in a valid entry
  ArrayFromVector<TYPE>(type, {false, true}, {0.5, nan_value}, &a);
  ArrayFromVector<TYPE>(type, {false, true}, {0.5, nan_value}, &b);
  ASSERT_FALSE(a->Equals(b));
  ASSERT_FALSE(b->Equals(a));
  ASSERT_TRUE(a->Equals(b, EqualOptions().nans_equal(true)));
  ASSERT_TRUE(b->Equals(a, EqualOptions().nans_equal(true)));
  ASSERT_FALSE(a->ApproxEquals(b));
  ASSERT_FALSE(b->ApproxEquals(a));
  ASSERT_TRUE(a->ApproxEquals(b, EqualOptions().atol(1e-5).nans_equal(true)));
  ASSERT_TRUE(b->ApproxEquals(a, EqualOptions().atol(1e-5).nans_equal(true)));
  // NaN in tested range
  ASSERT_FALSE(a->RangeEquals(b, 0, 2, 0));
  ASSERT_FALSE(b->RangeEquals(a, 0, 2, 0));
  ASSERT_FALSE(a->RangeEquals(b, 1, 2, 1));
  ASSERT_FALSE(b->RangeEquals(a, 1, 2, 1));
  // NaN not in tested range
  ASSERT_TRUE(a->RangeEquals(b, 0, 1, 0));
  ASSERT_TRUE(b->RangeEquals(a, 0, 1, 0));

  // NaN != non-NaN
  ArrayFromVector<TYPE>(type, {false, true}, {0.5, nan_value}, &a);
  ArrayFromVector<TYPE>(type, {false, true}, {0.5, 0.0}, &b);
  ASSERT_FALSE(a->Equals(b));
  ASSERT_FALSE(b->Equals(a));
  ASSERT_FALSE(a->Equals(b, EqualOptions().nans_equal(true)));
  ASSERT_FALSE(b->Equals(a, EqualOptions().nans_equal(true)));
  ASSERT_FALSE(a->ApproxEquals(b));
  ASSERT_FALSE(b->ApproxEquals(a));
  ASSERT_FALSE(a->ApproxEquals(b, EqualOptions().atol(1e-5).nans_equal(true)));
  ASSERT_FALSE(b->ApproxEquals(a, EqualOptions().atol(1e-5).nans_equal(true)));
  // NaN in tested range
  ASSERT_FALSE(a->RangeEquals(b, 0, 2, 0));
  ASSERT_FALSE(b->RangeEquals(a, 0, 2, 0));
  ASSERT_FALSE(a->RangeEquals(b, 1, 2, 1));
  ASSERT_FALSE(b->RangeEquals(a, 1, 2, 1));
  // NaN not in tested range
  ASSERT_TRUE(a->RangeEquals(b, 0, 1, 0));
  ASSERT_TRUE(b->RangeEquals(a, 0, 1, 0));
}

template <typename TYPE>
void CheckFloatingInfinityEquality() {
  std::shared_ptr<Array> a, b;
  std::shared_ptr<DataType> type = TypeTraits<TYPE>::type_singleton();

  const auto infinity = std::numeric_limits<typename TYPE::c_type>::infinity();

  for (auto nans_equal : {false, true}) {
    // Infinity in a null entry
    ArrayFromVector<TYPE>(type, {true, false}, {0.5, infinity}, &a);
    ArrayFromVector<TYPE>(type, {true, false}, {0.5, -infinity}, &b);
    ASSERT_TRUE(a->Equals(b));
    ASSERT_TRUE(b->Equals(a));
    ASSERT_TRUE(a->ApproxEquals(b, EqualOptions().atol(1e-5).nans_equal(nans_equal)));
    ASSERT_TRUE(b->ApproxEquals(a, EqualOptions().atol(1e-5).nans_equal(nans_equal)));
    ASSERT_TRUE(a->RangeEquals(b, 0, 2, 0));
    ASSERT_TRUE(b->RangeEquals(a, 0, 2, 0));
    ASSERT_TRUE(a->RangeEquals(b, 1, 2, 1));
    ASSERT_TRUE(b->RangeEquals(a, 1, 2, 1));

    // Infinity in a valid entry
    ArrayFromVector<TYPE>(type, {false, true}, {0.5, infinity}, &a);
    ArrayFromVector<TYPE>(type, {false, true}, {0.5, infinity}, &b);
    ASSERT_TRUE(a->Equals(b));
    ASSERT_TRUE(b->Equals(a));
    ASSERT_TRUE(a->ApproxEquals(b, EqualOptions().atol(1e-5).nans_equal(nans_equal)));
    ASSERT_TRUE(b->ApproxEquals(a, EqualOptions().atol(1e-5).nans_equal(nans_equal)));
    ASSERT_TRUE(a->ApproxEquals(b, EqualOptions().atol(1e-5).nans_equal(nans_equal)));
    ASSERT_TRUE(b->ApproxEquals(a, EqualOptions().atol(1e-5).nans_equal(nans_equal)));
    // Infinity in tested range
    ASSERT_TRUE(a->RangeEquals(b, 0, 2, 0));
    ASSERT_TRUE(b->RangeEquals(a, 0, 2, 0));
    ASSERT_TRUE(a->RangeEquals(b, 1, 2, 1));
    ASSERT_TRUE(b->RangeEquals(a, 1, 2, 1));
    // Infinity not in tested range
    ASSERT_TRUE(a->RangeEquals(b, 0, 1, 0));
    ASSERT_TRUE(b->RangeEquals(a, 0, 1, 0));

    // Infinity != non-infinity
    ArrayFromVector<TYPE>(type, {false, true}, {0.5, -infinity}, &a);
    ArrayFromVector<TYPE>(type, {false, true}, {0.5, 0.0}, &b);
    ASSERT_FALSE(a->Equals(b));
    ASSERT_FALSE(b->Equals(a));
    ASSERT_FALSE(a->ApproxEquals(b, EqualOptions().atol(1e-5).nans_equal(nans_equal)));
    ASSERT_FALSE(b->ApproxEquals(a));
    ASSERT_FALSE(a->ApproxEquals(b, EqualOptions().atol(1e-5).nans_equal(nans_equal)));
    ASSERT_FALSE(b->ApproxEquals(a, EqualOptions().atol(1e-5).nans_equal(nans_equal)));
    // Infinity != Negative infinity
    ArrayFromVector<TYPE>(type, {true, true}, {0.5, -infinity}, &a);
    ArrayFromVector<TYPE>(type, {true, true}, {0.5, infinity}, &b);
    ASSERT_FALSE(a->Equals(b));
    ASSERT_FALSE(b->Equals(a));
    ASSERT_FALSE(a->ApproxEquals(b));
    ASSERT_FALSE(b->ApproxEquals(a));
    ASSERT_FALSE(a->ApproxEquals(b, EqualOptions().atol(1e-5).nans_equal(nans_equal)));
    ASSERT_FALSE(b->ApproxEquals(a, EqualOptions().atol(1e-5).nans_equal(nans_equal)));
    // Infinity in tested range
    ASSERT_FALSE(a->RangeEquals(b, 0, 2, 0));
    ASSERT_FALSE(b->RangeEquals(a, 0, 2, 0));
    ASSERT_FALSE(a->RangeEquals(b, 1, 2, 1));
    ASSERT_FALSE(b->RangeEquals(a, 1, 2, 1));
    // Infinity not in tested range
    ASSERT_TRUE(a->RangeEquals(b, 0, 1, 0));
    ASSERT_TRUE(b->RangeEquals(a, 0, 1, 0));
  }
}

TEST(TestPrimitiveAdHoc, FloatingApproxEquals) {
  CheckApproxEquals<FloatType>();
  CheckApproxEquals<DoubleType>();
}

TEST(TestPrimitiveAdHoc, FloatingSliceApproxEquals) {
  CheckSliceApproxEquals<FloatType>();
  CheckSliceApproxEquals<DoubleType>();
}

TEST(TestPrimitiveAdHoc, FloatingNanEquality) {
  CheckFloatingNanEquality<FloatType>();
  CheckFloatingNanEquality<DoubleType>();
}

TEST(TestPrimitiveAdHoc, FloatingInfinityEquality) {
  CheckFloatingInfinityEquality<FloatType>();
  CheckFloatingInfinityEquality<DoubleType>();
}

// ----------------------------------------------------------------------
// FixedSizeBinary tests

class TestFWBinaryArray : public ::testing::Test {
 public:
  void SetUp() {}

  void InitBuilder(int byte_width) {
    auto type = fixed_size_binary(byte_width);
    builder_.reset(new FixedSizeBinaryBuilder(type, default_memory_pool()));
  }

 protected:
  std::unique_ptr<FixedSizeBinaryBuilder> builder_;
};

TEST_F(TestFWBinaryArray, Builder) {
  int32_t byte_width = 10;
  int64_t length = 4096;

  int64_t nbytes = length * byte_width;

  std::vector<uint8_t> data(nbytes);
  random_bytes(nbytes, 0, data.data());

  std::vector<uint8_t> is_valid(length);
  random_null_bytes(length, 0.1, is_valid.data());

  const uint8_t* raw_data = data.data();

  std::shared_ptr<Array> result;

  auto CheckResult = [&length, &is_valid, &raw_data, &byte_width](const Array& result) {
    // Verify output
    const auto& fw_result = checked_cast<const FixedSizeBinaryArray&>(result);

    ASSERT_EQ(length, result.length());

    for (int64_t i = 0; i < result.length(); ++i) {
      if (is_valid[i]) {
        ASSERT_EQ(0,
                  memcmp(raw_data + byte_width * i, fw_result.GetValue(i), byte_width));
      } else {
        ASSERT_TRUE(fw_result.IsNull(i));
      }
    }
  };

  // Build using iterative API
  InitBuilder(byte_width);
  for (int64_t i = 0; i < length; ++i) {
    if (is_valid[i]) {
      ASSERT_OK(builder_->Append(raw_data + byte_width * i));
    } else {
      ASSERT_OK(builder_->AppendNull());
    }
  }

  FinishAndCheckPadding(builder_.get(), &result);
  CheckResult(*result);

  // Build using batch API
  InitBuilder(byte_width);

  const uint8_t* raw_is_valid = is_valid.data();

  ASSERT_OK(builder_->AppendValues(raw_data, 50, raw_is_valid));
  ASSERT_OK(
      builder_->AppendValues(raw_data + 50 * byte_width, length - 50, raw_is_valid + 50));
  FinishAndCheckPadding(builder_.get(), &result);

  CheckResult(*result);

  // Build from std::string
  InitBuilder(byte_width);
  for (int64_t i = 0; i < length; ++i) {
    if (is_valid[i]) {
      ASSERT_OK(builder_->Append(std::string(
          reinterpret_cast<const char*>(raw_data + byte_width * i), byte_width)));
    } else {
      ASSERT_OK(builder_->AppendNull());
    }
  }

  ASSERT_OK(builder_->Finish(&result));
  CheckResult(*result);
}

TEST_F(TestFWBinaryArray, EqualsRangeEquals) {
  // Check that we don't compare data in null slots

  auto type = fixed_size_binary(4);
  FixedSizeBinaryBuilder builder1(type);
  FixedSizeBinaryBuilder builder2(type);

  ASSERT_OK(builder1.Append("foo1"));
  ASSERT_OK(builder1.AppendNull());

  ASSERT_OK(builder2.Append("foo1"));
  ASSERT_OK(builder2.Append("foo2"));

  std::shared_ptr<Array> array1, array2;
  ASSERT_OK(builder1.Finish(&array1));
  ASSERT_OK(builder2.Finish(&array2));

  const auto& a1 = checked_cast<const FixedSizeBinaryArray&>(*array1);
  const auto& a2 = checked_cast<const FixedSizeBinaryArray&>(*array2);

  FixedSizeBinaryArray equal1(type, 2, a1.values(), a1.null_bitmap(), 1);
  FixedSizeBinaryArray equal2(type, 2, a2.values(), a1.null_bitmap(), 1);

  ASSERT_TRUE(equal1.Equals(equal2));
  ASSERT_TRUE(equal1.RangeEquals(equal2, 0, 2, 0));
}

TEST_F(TestFWBinaryArray, ZeroSize) {
  auto type = fixed_size_binary(0);
  FixedSizeBinaryBuilder builder(type);

  ASSERT_OK(builder.Append(""));
  ASSERT_OK(builder.Append(std::string()));
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.AppendNull());

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));

  const auto& fw_array = checked_cast<const FixedSizeBinaryArray&>(*array);

  // data is never allocated
  ASSERT_EQ(fw_array.values()->size(), 0);
  ASSERT_EQ(0, fw_array.byte_width());

  ASSERT_EQ(5, array->length());
  ASSERT_EQ(3, array->null_count());
}

TEST_F(TestFWBinaryArray, ZeroPadding) {
  auto type = fixed_size_binary(4);
  FixedSizeBinaryBuilder builder(type);

  ASSERT_OK(builder.Append("foo1"));
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Append("foo2"));
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Append("foo3"));

  std::shared_ptr<Array> array;
  FinishAndCheckPadding(&builder, &array);
}

TEST_F(TestFWBinaryArray, Slice) {
  auto type = fixed_size_binary(4);
  FixedSizeBinaryBuilder builder(type);

  std::vector<std::string> strings = {"foo1", "foo2", "foo3", "foo4", "foo5"};
  std::vector<uint8_t> is_null = {0, 1, 0, 0, 0};

  for (int i = 0; i < 5; ++i) {
    if (is_null[i]) {
      ASSERT_OK(builder.AppendNull());
    } else {
      ASSERT_OK(builder.Append(strings[i]));
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

TEST_F(TestFWBinaryArray, BuilderNulls) {
  auto type = fixed_size_binary(4);
  FixedSizeBinaryBuilder builder(type);

  for (int x = 0; x < 100; x++) {
    ASSERT_OK(builder.AppendNull());
  }
  ASSERT_OK(builder.AppendNulls(500));

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));

  for (auto buffer : array->data()->buffers) {
    for (int64_t i = 0; i < buffer->capacity(); i++) {
      // Validates current implementation, algorithms shouldn't rely on this
      ASSERT_EQ(0, *(buffer->data() + i)) << i;
    }
  }
}

struct FWBinaryAppender {
  Status VisitNull() {
    data.emplace_back("(null)");
    return Status::OK();
  }

  Status VisitValue(util::string_view v) {
    data.push_back(v);
    return Status::OK();
  }

  std::vector<util::string_view> data;
};

TEST_F(TestFWBinaryArray, ArrayDataVisitor) {
  auto type = fixed_size_binary(3);

  auto array = ArrayFromJSON(type, R"(["abc", null, "def"])");
  FWBinaryAppender appender;
  ArrayDataVisitor<FixedSizeBinaryType> visitor;
  ASSERT_OK(visitor.Visit(*array->data(), &appender));
  ASSERT_THAT(appender.data, ::testing::ElementsAreArray({"abc", "(null)", "def"}));
  ARROW_UNUSED(visitor);  // Workaround weird MSVC warning
}

TEST_F(TestFWBinaryArray, ArrayDataVisitorSliced) {
  auto type = fixed_size_binary(3);

  auto array = ArrayFromJSON(type, R"(["abc", null, "def", "ghi"])")->Slice(1, 2);
  FWBinaryAppender appender;
  ArrayDataVisitor<FixedSizeBinaryType> visitor;
  ASSERT_OK(visitor.Visit(*array->data(), &appender));
  ASSERT_THAT(appender.data, ::testing::ElementsAreArray({"(null)", "def"}));
  ARROW_UNUSED(visitor);  // Workaround weird MSVC warning
}

// ----------------------------------------------------------------------
// AdaptiveInt tests

class TestAdaptiveIntBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();
    builder_ = std::make_shared<AdaptiveIntBuilder>(pool_);
  }

  void Done() { FinishAndCheckPadding(builder_.get(), &result_); }

  template <typename ExpectedType>
  void TestAppendValues() {
    using CType = typename TypeTraits<ExpectedType>::CType;
    auto type = TypeTraits<ExpectedType>::type_singleton();

    std::vector<int64_t> values(
        {0, std::numeric_limits<CType>::min(), std::numeric_limits<CType>::max()});
    std::vector<CType> expected_values(
        {0, std::numeric_limits<CType>::min(), std::numeric_limits<CType>::max()});
    ArrayFromVector<ExpectedType, CType>(expected_values, &expected_);

    SetUp();
    ASSERT_OK(builder_->AppendValues(values.data(), values.size()));
    AssertTypeEqual(*builder_->type(), *type);
    ASSERT_EQ(builder_->length(), static_cast<int64_t>(values.size()));
    Done();
    ASSERT_EQ(builder_->length(), 0);
    AssertArraysEqual(*expected_, *result_);

    // Reuse builder
    builder_->Reset();
    AssertTypeEqual(*builder_->type(), *int8());
    ASSERT_OK(builder_->AppendValues(values.data(), values.size()));
    AssertTypeEqual(*builder_->type(), *type);
    ASSERT_EQ(builder_->length(), static_cast<int64_t>(values.size()));
    Done();
    ASSERT_EQ(builder_->length(), 0);
    AssertArraysEqual(*expected_, *result_);
  }

 protected:
  std::shared_ptr<AdaptiveIntBuilder> builder_;

  std::shared_ptr<Array> expected_;
  std::shared_ptr<Array> result_;
};

TEST_F(TestAdaptiveIntBuilder, TestInt8) {
  ASSERT_EQ(builder_->type()->id(), Type::INT8);
  ASSERT_EQ(builder_->length(), 0);
  ASSERT_OK(builder_->Append(0));
  ASSERT_EQ(builder_->length(), 1);
  ASSERT_OK(builder_->Append(127));
  ASSERT_EQ(builder_->length(), 2);
  ASSERT_OK(builder_->Append(-128));
  ASSERT_EQ(builder_->length(), 3);

  Done();

  std::vector<int8_t> expected_values({0, 127, -128});
  ArrayFromVector<Int8Type, int8_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveIntBuilder, TestInt16) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_EQ(builder_->type()->id(), Type::INT8);
  ASSERT_OK(builder_->Append(128));
  ASSERT_EQ(builder_->type()->id(), Type::INT16);
  Done();

  std::vector<int16_t> expected_values({0, 128});
  ArrayFromVector<Int16Type, int16_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);

  SetUp();
  ASSERT_OK(builder_->Append(-129));
  expected_values = {-129};
  Done();

  ArrayFromVector<Int16Type, int16_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);

  SetUp();
  ASSERT_OK(builder_->Append(std::numeric_limits<int16_t>::max()));
  ASSERT_OK(builder_->Append(std::numeric_limits<int16_t>::min()));
  expected_values = {std::numeric_limits<int16_t>::max(),
                     std::numeric_limits<int16_t>::min()};
  Done();

  ArrayFromVector<Int16Type, int16_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveIntBuilder, TestInt16Nulls) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_EQ(builder_->type()->id(), Type::INT8);
  ASSERT_OK(builder_->Append(128));
  ASSERT_EQ(builder_->type()->id(), Type::INT16);
  ASSERT_OK(builder_->AppendNull());
  ASSERT_EQ(builder_->type()->id(), Type::INT16);
  ASSERT_EQ(1, builder_->null_count());
  Done();

  std::vector<int16_t> expected_values({0, 128, 0});
  ArrayFromVector<Int16Type, int16_t>({1, 1, 0}, expected_values, &expected_);
  ASSERT_ARRAYS_EQUAL(*expected_, *result_);
}

TEST_F(TestAdaptiveIntBuilder, TestInt32) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_EQ(builder_->type()->id(), Type::INT8);
  ASSERT_OK(
      builder_->Append(static_cast<int64_t>(std::numeric_limits<int16_t>::max()) + 1));
  ASSERT_EQ(builder_->type()->id(), Type::INT32);
  Done();

  std::vector<int32_t> expected_values(
      {0, static_cast<int32_t>(std::numeric_limits<int16_t>::max()) + 1});
  ArrayFromVector<Int32Type, int32_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);

  SetUp();
  ASSERT_OK(
      builder_->Append(static_cast<int64_t>(std::numeric_limits<int16_t>::min()) - 1));
  expected_values = {static_cast<int32_t>(std::numeric_limits<int16_t>::min()) - 1};
  Done();

  ArrayFromVector<Int32Type, int32_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);

  SetUp();
  ASSERT_OK(builder_->Append(std::numeric_limits<int32_t>::max()));
  ASSERT_OK(builder_->Append(std::numeric_limits<int32_t>::min()));
  expected_values = {std::numeric_limits<int32_t>::max(),
                     std::numeric_limits<int32_t>::min()};
  Done();

  ArrayFromVector<Int32Type, int32_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveIntBuilder, TestInt64) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_EQ(builder_->type()->id(), Type::INT8);
  ASSERT_OK(
      builder_->Append(static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1));
  ASSERT_EQ(builder_->type()->id(), Type::INT64);
  Done();

  std::vector<int64_t> expected_values(
      {0, static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1});
  ArrayFromVector<Int64Type, int64_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);

  SetUp();
  ASSERT_OK(
      builder_->Append(static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1));
  expected_values = {static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1};
  Done();

  ArrayFromVector<Int64Type, int64_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);

  SetUp();
  ASSERT_OK(builder_->Append(std::numeric_limits<int64_t>::max()));
  ASSERT_OK(builder_->Append(std::numeric_limits<int64_t>::min()));
  expected_values = {std::numeric_limits<int64_t>::max(),
                     std::numeric_limits<int64_t>::min()};
  Done();

  ArrayFromVector<Int64Type, int64_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveIntBuilder, TestManyAppends) {
  // More than the builder's internal scratchpad size
  const int32_t n_values = 99999;
  std::vector<int32_t> expected_values(n_values);

  for (int32_t i = 0; i < n_values; ++i) {
    int32_t val = (i & 1) ? i : -i;
    expected_values[i] = val;
    ASSERT_OK(builder_->Append(val));
    ASSERT_EQ(builder_->length(), i + 1);
  }
  ASSERT_EQ(builder_->type()->id(), Type::INT32);
  Done();

  ArrayFromVector<Int32Type, int32_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveIntBuilder, TestAppendValues) {
  this->template TestAppendValues<Int64Type>();
  this->template TestAppendValues<Int32Type>();
  this->template TestAppendValues<Int16Type>();
  this->template TestAppendValues<Int8Type>();
}

TEST_F(TestAdaptiveIntBuilder, TestAssertZeroPadded) {
  std::vector<int64_t> values(
      {0, static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1});
  ASSERT_OK(builder_->AppendValues(values.data(), values.size()));
  Done();
}

TEST_F(TestAdaptiveIntBuilder, TestAppendNull) {
  int64_t size = 1000;
  ASSERT_OK(builder_->Append(127));
  ASSERT_EQ(0, builder_->null_count());
  for (unsigned index = 1; index < size - 1; ++index) {
    ASSERT_OK(builder_->AppendNull());
    ASSERT_EQ(index, builder_->null_count());
  }
  ASSERT_OK(builder_->Append(-128));
  ASSERT_EQ(size - 2, builder_->null_count());

  Done();

  std::vector<bool> expected_valid(size, false);
  expected_valid[0] = true;
  expected_valid[size - 1] = true;
  std::vector<int8_t> expected_values(size);
  expected_values[0] = 127;
  expected_values[size - 1] = -128;
  std::shared_ptr<Array> expected;
  ArrayFromVector<Int8Type, int8_t>(expected_valid, expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveIntBuilder, TestAppendNulls) {
  constexpr int64_t size = 10;
  ASSERT_EQ(0, builder_->null_count());
  ASSERT_OK(builder_->AppendNulls(size));
  ASSERT_EQ(size, builder_->null_count());

  Done();

  for (unsigned index = 0; index < size; ++index) {
    ASSERT_FALSE(result_->IsValid(index));
  }
}

TEST_F(TestAdaptiveIntBuilder, TestAppendEmptyValue) {
  ASSERT_OK(builder_->AppendNulls(2));
  ASSERT_OK(builder_->AppendEmptyValue());
  ASSERT_OK(builder_->Append(42));
  ASSERT_OK(builder_->AppendEmptyValues(2));
  Done();

  ASSERT_OK(result_->ValidateFull());
  // NOTE: The fact that we get 0 is really an implementation detail
  AssertArraysEqual(*result_, *ArrayFromJSON(int8(), "[null, null, 0, 42, 0, 0]"));
}

TEST(TestAdaptiveIntBuilderWithStartIntSize, TestReset) {
  auto builder = std::make_shared<AdaptiveIntBuilder>(
      static_cast<uint8_t>(sizeof(int16_t)), default_memory_pool());
  AssertTypeEqual(*int16(), *builder->type());

  ASSERT_OK(
      builder->Append(static_cast<int64_t>(std::numeric_limits<int16_t>::max()) + 1));
  AssertTypeEqual(*int32(), *builder->type());

  builder->Reset();
  AssertTypeEqual(*int16(), *builder->type());
}

class TestAdaptiveUIntBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();
    builder_ = std::make_shared<AdaptiveUIntBuilder>(pool_);
  }

  void Done() { FinishAndCheckPadding(builder_.get(), &result_); }

  template <typename ExpectedType>
  void TestAppendValues() {
    using CType = typename TypeTraits<ExpectedType>::CType;
    auto type = TypeTraits<ExpectedType>::type_singleton();

    std::vector<uint64_t> values(
        {0, std::numeric_limits<CType>::min(), std::numeric_limits<CType>::max()});
    std::vector<CType> expected_values(
        {0, std::numeric_limits<CType>::min(), std::numeric_limits<CType>::max()});
    ArrayFromVector<ExpectedType, CType>(expected_values, &expected_);

    SetUp();
    ASSERT_OK(builder_->AppendValues(values.data(), values.size()));
    AssertTypeEqual(*builder_->type(), *type);
    ASSERT_EQ(builder_->length(), static_cast<int64_t>(values.size()));
    Done();
    ASSERT_EQ(builder_->length(), 0);
    AssertArraysEqual(*expected_, *result_);

    // Reuse builder
    builder_->Reset();
    AssertTypeEqual(*builder_->type(), *uint8());
    ASSERT_OK(builder_->AppendValues(values.data(), values.size()));
    AssertTypeEqual(*builder_->type(), *type);
    ASSERT_EQ(builder_->length(), static_cast<int64_t>(values.size()));
    Done();
    ASSERT_EQ(builder_->length(), 0);
    AssertArraysEqual(*expected_, *result_);
  }

 protected:
  std::shared_ptr<AdaptiveUIntBuilder> builder_;

  std::shared_ptr<Array> expected_;
  std::shared_ptr<Array> result_;
};

TEST_F(TestAdaptiveUIntBuilder, TestUInt8) {
  ASSERT_EQ(builder_->length(), 0);
  ASSERT_OK(builder_->Append(0));
  ASSERT_EQ(builder_->length(), 1);
  ASSERT_OK(builder_->Append(255));
  ASSERT_EQ(builder_->length(), 2);

  Done();

  std::vector<uint8_t> expected_values({0, 255});
  ArrayFromVector<UInt8Type, uint8_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));
}

TEST_F(TestAdaptiveUIntBuilder, TestUInt16) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_EQ(builder_->type()->id(), Type::UINT8);
  ASSERT_OK(builder_->Append(256));
  ASSERT_EQ(builder_->type()->id(), Type::UINT16);
  Done();

  std::vector<uint16_t> expected_values({0, 256});
  ArrayFromVector<UInt16Type, uint16_t>(expected_values, &expected_);
  ASSERT_ARRAYS_EQUAL(*expected_, *result_);

  SetUp();
  ASSERT_OK(builder_->Append(std::numeric_limits<uint16_t>::max()));
  expected_values = {std::numeric_limits<uint16_t>::max()};
  Done();

  ArrayFromVector<UInt16Type, uint16_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));
}

TEST_F(TestAdaptiveUIntBuilder, TestUInt16Nulls) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_EQ(builder_->type()->id(), Type::UINT8);
  ASSERT_OK(builder_->Append(256));
  ASSERT_EQ(builder_->type()->id(), Type::UINT16);
  ASSERT_OK(builder_->AppendNull());
  ASSERT_EQ(builder_->type()->id(), Type::UINT16);
  ASSERT_EQ(1, builder_->null_count());
  Done();

  std::vector<uint16_t> expected_values({0, 256, 0});
  ArrayFromVector<UInt16Type, uint16_t>({1, 1, 0}, expected_values, &expected_);
  ASSERT_ARRAYS_EQUAL(*expected_, *result_);
}

TEST_F(TestAdaptiveUIntBuilder, TestUInt32) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_EQ(builder_->type()->id(), Type::UINT8);
  ASSERT_OK(
      builder_->Append(static_cast<uint64_t>(std::numeric_limits<uint16_t>::max()) + 1));
  ASSERT_EQ(builder_->type()->id(), Type::UINT32);
  Done();

  std::vector<uint32_t> expected_values(
      {0, static_cast<uint32_t>(std::numeric_limits<uint16_t>::max()) + 1});
  ArrayFromVector<UInt32Type, uint32_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));

  SetUp();
  ASSERT_OK(builder_->Append(std::numeric_limits<uint32_t>::max()));
  expected_values = {std::numeric_limits<uint32_t>::max()};
  Done();

  ArrayFromVector<UInt32Type, uint32_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));
}

TEST_F(TestAdaptiveUIntBuilder, TestUInt64) {
  ASSERT_OK(builder_->Append(0));
  ASSERT_EQ(builder_->type()->id(), Type::UINT8);
  ASSERT_OK(
      builder_->Append(static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1));
  ASSERT_EQ(builder_->type()->id(), Type::UINT64);
  Done();

  std::vector<uint64_t> expected_values(
      {0, static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1});
  ArrayFromVector<UInt64Type, uint64_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));

  SetUp();
  ASSERT_OK(builder_->Append(std::numeric_limits<uint64_t>::max()));
  expected_values = {std::numeric_limits<uint64_t>::max()};
  Done();

  ArrayFromVector<UInt64Type, uint64_t>(expected_values, &expected_);
  ASSERT_TRUE(expected_->Equals(result_));
}

TEST_F(TestAdaptiveUIntBuilder, TestManyAppends) {
  // More than the builder's internal scratchpad size
  const int32_t n_values = 99999;
  std::vector<uint32_t> expected_values(n_values);

  for (int32_t i = 0; i < n_values; ++i) {
    auto val = static_cast<uint32_t>(i);
    expected_values[i] = val;
    ASSERT_OK(builder_->Append(val));
    ASSERT_EQ(builder_->length(), i + 1);
  }
  ASSERT_EQ(builder_->type()->id(), Type::UINT32);
  Done();

  ArrayFromVector<UInt32Type, uint32_t>(expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveUIntBuilder, TestAppendValues) {
  this->template TestAppendValues<UInt64Type>();
  this->template TestAppendValues<UInt32Type>();
  this->template TestAppendValues<UInt16Type>();
  this->template TestAppendValues<UInt8Type>();
}

TEST_F(TestAdaptiveUIntBuilder, TestAssertZeroPadded) {
  std::vector<uint64_t> values(
      {0, static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1});
  ASSERT_OK(builder_->AppendValues(values.data(), values.size()));
  Done();
}

TEST_F(TestAdaptiveUIntBuilder, TestAppendNull) {
  int64_t size = 1000;
  ASSERT_OK(builder_->Append(254));
  for (unsigned index = 1; index < size - 1; ++index) {
    ASSERT_OK(builder_->AppendNull());
    ASSERT_EQ(index, builder_->null_count());
  }
  ASSERT_OK(builder_->Append(255));

  Done();

  std::vector<bool> expected_valid(size, false);
  expected_valid[0] = true;
  expected_valid[size - 1] = true;
  std::vector<uint8_t> expected_values(size);
  expected_values[0] = 254;
  expected_values[size - 1] = 255;
  std::shared_ptr<Array> expected;
  ArrayFromVector<UInt8Type, uint8_t>(expected_valid, expected_values, &expected_);
  AssertArraysEqual(*expected_, *result_);
}

TEST_F(TestAdaptiveUIntBuilder, TestAppendNulls) {
  constexpr int64_t size = 10;
  ASSERT_OK(builder_->AppendNulls(size));
  ASSERT_EQ(size, builder_->null_count());

  Done();

  for (unsigned index = 0; index < size; ++index) {
    ASSERT_FALSE(result_->IsValid(index));
  }
}

TEST_F(TestAdaptiveUIntBuilder, TestAppendEmptyValue) {
  ASSERT_OK(builder_->AppendNulls(2));
  ASSERT_OK(builder_->AppendEmptyValue());
  ASSERT_OK(builder_->Append(42));
  ASSERT_OK(builder_->AppendEmptyValues(2));
  Done();

  ASSERT_OK(result_->ValidateFull());
  // NOTE: The fact that we get 0 is really an implementation detail
  AssertArraysEqual(*result_, *ArrayFromJSON(uint8(), "[null, null, 0, 42, 0, 0]"));
}

TEST(TestAdaptiveUIntBuilderWithStartIntSize, TestReset) {
  auto builder = std::make_shared<AdaptiveUIntBuilder>(
      static_cast<uint8_t>(sizeof(uint16_t)), default_memory_pool());
  AssertTypeEqual(uint16(), builder->type());

  ASSERT_OK(
      builder->Append(static_cast<uint64_t>(std::numeric_limits<uint16_t>::max()) + 1));
  AssertTypeEqual(uint32(), builder->type());

  builder->Reset();
  AssertTypeEqual(uint16(), builder->type());
}

// ----------------------------------------------------------------------
// Test Decimal arrays

template <typename TYPE>
class DecimalTest : public ::testing::TestWithParam<int> {
 public:
  using DecimalBuilder = typename TypeTraits<TYPE>::BuilderType;
  using DecimalValue = typename TypeTraits<TYPE>::ScalarType::ValueType;
  using DecimalArray = typename TypeTraits<TYPE>::ArrayType;
  using DecimalVector = std::vector<DecimalValue>;

  DecimalTest() {}

  template <size_t BYTE_WIDTH = 16>
  void MakeData(const DecimalVector& input, std::vector<uint8_t>* out) const {
    out->reserve(input.size() * BYTE_WIDTH);

    for (const auto& value : input) {
      auto bytes = value.ToBytes();
      out->insert(out->end(), bytes.cbegin(), bytes.cend());
    }
  }

  template <size_t BYTE_WIDTH = 16>
  void TestCreate(int32_t precision, const DecimalVector& draw,
                  const std::vector<uint8_t>& valid_bytes, int64_t offset) const {
    auto type = std::make_shared<TYPE>(precision, 4);
    auto builder = std::make_shared<DecimalBuilder>(type);

    size_t null_count = 0;

    const size_t size = draw.size();

    ASSERT_OK(builder->Reserve(size));

    for (size_t i = 0; i < size; ++i) {
      if (valid_bytes[i]) {
        ASSERT_OK(builder->Append(draw[i]));
      } else {
        ASSERT_OK(builder->AppendNull());
        ++null_count;
      }
    }

    std::shared_ptr<Array> out;
    FinishAndCheckPadding(builder.get(), &out);
    ASSERT_EQ(builder->length(), 0);

    std::vector<uint8_t> raw_bytes;

    raw_bytes.reserve(size * BYTE_WIDTH);
    MakeData<BYTE_WIDTH>(draw, &raw_bytes);

    auto expected_data = std::make_shared<Buffer>(raw_bytes.data(), BYTE_WIDTH);
    std::shared_ptr<Buffer> expected_null_bitmap;
    ASSERT_OK_AND_ASSIGN(expected_null_bitmap, internal::BytesToBits(valid_bytes));

    int64_t expected_null_count = CountNulls(valid_bytes);
    auto expected = std::make_shared<DecimalArray>(
        type, size, expected_data, expected_null_bitmap, expected_null_count);

    std::shared_ptr<Array> lhs = out->Slice(offset);
    std::shared_ptr<Array> rhs = expected->Slice(offset);
    ASSERT_ARRAYS_EQUAL(*rhs, *lhs);
  }
};

using Decimal128Test = DecimalTest<Decimal128Type>;

TEST_P(Decimal128Test, NoNulls) {
  int32_t precision = GetParam();
  std::vector<Decimal128> draw = {Decimal128(1), Decimal128(-2), Decimal128(2389),
                                  Decimal128(4), Decimal128(-12348)};
  std::vector<uint8_t> valid_bytes = {true, true, true, true, true};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

TEST_P(Decimal128Test, WithNulls) {
  int32_t precision = GetParam();
  std::vector<Decimal128> draw = {Decimal128(1), Decimal128(2),  Decimal128(-1),
                                  Decimal128(4), Decimal128(-1), Decimal128(1),
                                  Decimal128(2)};
  Decimal128 big;
  ASSERT_OK_AND_ASSIGN(big, Decimal128::FromString("230342903942.234234"));
  draw.push_back(big);

  Decimal128 big_negative;
  ASSERT_OK_AND_ASSIGN(big_negative, Decimal128::FromString("-23049302932.235234"));
  draw.push_back(big_negative);

  std::vector<uint8_t> valid_bytes = {true, true, false, true, false,
                                      true, true, true,  true};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

INSTANTIATE_TEST_SUITE_P(Decimal128Test, Decimal128Test, ::testing::Range(1, 38));

using Decimal256Test = DecimalTest<Decimal256Type>;

TEST_P(Decimal256Test, NoNulls) {
  int32_t precision = GetParam();
  std::vector<Decimal256> draw = {Decimal256(1), Decimal256(-2), Decimal256(2389),
                                  Decimal256(4), Decimal256(-12348)};
  std::vector<uint8_t> valid_bytes = {true, true, true, true, true};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

TEST_P(Decimal256Test, WithNulls) {
  int32_t precision = GetParam();
  std::vector<Decimal256> draw = {Decimal256(1), Decimal256(2),  Decimal256(-1),
                                  Decimal256(4), Decimal256(-1), Decimal256(1),
                                  Decimal256(2)};
  Decimal256 big;  // (pow(2, 255) - 1) / pow(10, 38)
  ASSERT_OK_AND_ASSIGN(big,
                       Decimal256::FromString("578960446186580977117854925043439539266."
                                              "34992332820282019728792003956564819967"));
  draw.push_back(big);

  Decimal256 big_negative;  // -pow(2, 255) / pow(10, 38)
  ASSERT_OK_AND_ASSIGN(big_negative,
                       Decimal256::FromString("-578960446186580977117854925043439539266."
                                              "34992332820282019728792003956564819968"));
  draw.push_back(big_negative);

  std::vector<uint8_t> valid_bytes = {true, true, false, true, false,
                                      true, true, true,  true};
  this->TestCreate(precision, draw, valid_bytes, 0);
  this->TestCreate(precision, draw, valid_bytes, 2);
}

INSTANTIATE_TEST_SUITE_P(Decimal256Test, Decimal256Test,
                         ::testing::Values(1, 2, 5, 10, 38, 39, 40, 75, 76));

// ----------------------------------------------------------------------
// Test rechunking

TEST(TestRechunkArraysConsistently, Trivial) {
  std::vector<ArrayVector> groups, rechunked;
  rechunked = internal::RechunkArraysConsistently(groups);
  ASSERT_EQ(rechunked.size(), 0);

  std::shared_ptr<Array> a1, a2, b1;
  ArrayFromVector<Int16Type, int16_t>({}, &a1);
  ArrayFromVector<Int16Type, int16_t>({}, &a2);
  ArrayFromVector<Int32Type, int32_t>({}, &b1);

  groups = {{a1, a2}, {}, {b1}};
  rechunked = internal::RechunkArraysConsistently(groups);
  ASSERT_EQ(rechunked.size(), 3);

  for (auto& arrvec : rechunked) {
    for (auto& arr : arrvec) {
      AssertZeroPadded(*arr);
      TestInitialized(*arr);
    }
  }
}

TEST(TestRechunkArraysConsistently, Plain) {
  std::shared_ptr<Array> expected;
  std::shared_ptr<Array> a1, a2, a3, b1, b2, b3, b4;
  ArrayFromVector<Int16Type, int16_t>({1, 2, 3}, &a1);
  ArrayFromVector<Int16Type, int16_t>({4, 5}, &a2);
  ArrayFromVector<Int16Type, int16_t>({6, 7, 8, 9}, &a3);

  ArrayFromVector<Int32Type, int32_t>({41, 42}, &b1);
  ArrayFromVector<Int32Type, int32_t>({43, 44, 45}, &b2);
  ArrayFromVector<Int32Type, int32_t>({46, 47}, &b3);
  ArrayFromVector<Int32Type, int32_t>({48, 49}, &b4);

  ArrayVector a{a1, a2, a3};
  ArrayVector b{b1, b2, b3, b4};

  std::vector<ArrayVector> groups{a, b}, rechunked;
  rechunked = internal::RechunkArraysConsistently(groups);
  ASSERT_EQ(rechunked.size(), 2);
  auto ra = rechunked[0];
  auto rb = rechunked[1];

  ASSERT_EQ(ra.size(), 5);
  ArrayFromVector<Int16Type, int16_t>({1, 2}, &expected);
  ASSERT_ARRAYS_EQUAL(*ra[0], *expected);
  ArrayFromVector<Int16Type, int16_t>({3}, &expected);
  ASSERT_ARRAYS_EQUAL(*ra[1], *expected);
  ArrayFromVector<Int16Type, int16_t>({4, 5}, &expected);
  ASSERT_ARRAYS_EQUAL(*ra[2], *expected);
  ArrayFromVector<Int16Type, int16_t>({6, 7}, &expected);
  ASSERT_ARRAYS_EQUAL(*ra[3], *expected);
  ArrayFromVector<Int16Type, int16_t>({8, 9}, &expected);
  ASSERT_ARRAYS_EQUAL(*ra[4], *expected);

  ASSERT_EQ(rb.size(), 5);
  ArrayFromVector<Int32Type, int32_t>({41, 42}, &expected);
  ASSERT_ARRAYS_EQUAL(*rb[0], *expected);
  ArrayFromVector<Int32Type, int32_t>({43}, &expected);
  ASSERT_ARRAYS_EQUAL(*rb[1], *expected);
  ArrayFromVector<Int32Type, int32_t>({44, 45}, &expected);
  ASSERT_ARRAYS_EQUAL(*rb[2], *expected);
  ArrayFromVector<Int32Type, int32_t>({46, 47}, &expected);
  ASSERT_ARRAYS_EQUAL(*rb[3], *expected);
  ArrayFromVector<Int32Type, int32_t>({48, 49}, &expected);
  ASSERT_ARRAYS_EQUAL(*rb[4], *expected);

  for (auto& arrvec : rechunked) {
    for (auto& arr : arrvec) {
      AssertZeroPadded(*arr);
      TestInitialized(*arr);
    }
  }
}

// ----------------------------------------------------------------------
// Test SwapEndianArrayData

/// \brief Indicate if fields are equals.
///
/// \param[in] target ArrayData to be converted and tested
/// \param[in] expected result ArrayData
void AssertArrayDataEqualsWithSwapEndian(const std::shared_ptr<ArrayData>& target,
                                         const std::shared_ptr<ArrayData>& expected) {
  auto swap_array = MakeArray(*::arrow::internal::SwapEndianArrayData(target));
  auto expected_array = MakeArray(expected);
  ASSERT_ARRAYS_EQUAL(*swap_array, *expected_array);
  ASSERT_OK(swap_array->ValidateFull());
}

TEST(TestSwapEndianArrayData, PrimitiveType) {
  auto null_buffer = Buffer::FromString("\xff");
  auto data_int_buffer = Buffer::FromString("01234567");

  auto data = ArrayData::Make(null(), 0, {nullptr}, 0);
  auto expected_data = data;
  AssertArrayDataEqualsWithSwapEndian(data, expected_data);

  data = ArrayData::Make(boolean(), 8, {null_buffer, data_int_buffer}, 0);
  expected_data = data;
  AssertArrayDataEqualsWithSwapEndian(data, expected_data);

  data = ArrayData::Make(int8(), 8, {null_buffer, data_int_buffer}, 0);
  expected_data = data;
  AssertArrayDataEqualsWithSwapEndian(data, expected_data);

  data = ArrayData::Make(uint16(), 4, {null_buffer, data_int_buffer}, 0);
  auto data_int16_buffer = Buffer::FromString("10325476");
  expected_data = ArrayData::Make(uint16(), 4, {null_buffer, data_int16_buffer}, 0);
  AssertArrayDataEqualsWithSwapEndian(data, expected_data);

  data = ArrayData::Make(int32(), 2, {null_buffer, data_int_buffer}, 0);
  auto data_int32_buffer = Buffer::FromString("32107654");
  expected_data = ArrayData::Make(int32(), 2, {null_buffer, data_int32_buffer}, 0);
  AssertArrayDataEqualsWithSwapEndian(data, expected_data);

  data = ArrayData::Make(uint64(), 1, {null_buffer, data_int_buffer}, 0);
  auto data_int64_buffer = Buffer::FromString("76543210");
  expected_data = ArrayData::Make(uint64(), 1, {null_buffer, data_int64_buffer}, 0);
  AssertArrayDataEqualsWithSwapEndian(data, expected_data);

  auto data_16byte_buffer = Buffer::FromString("0123456789abcdef");
  data = ArrayData::Make(decimal128(38, 10), 1, {null_buffer, data_16byte_buffer});
  auto data_decimal128_buffer = Buffer::FromString("fedcba9876543210");
  expected_data =
      ArrayData::Make(decimal128(38, 10), 1, {null_buffer, data_decimal128_buffer}, 0);
  AssertArrayDataEqualsWithSwapEndian(data, expected_data);

  auto data_32byte_buffer = Buffer::FromString("0123456789abcdef123456789ABCDEF0");
  data = ArrayData::Make(decimal256(76, 20), 1, {null_buffer, data_32byte_buffer});
  auto data_decimal256_buffer = Buffer::FromString("0FEDCBA987654321fedcba9876543210");
  expected_data =
      ArrayData::Make(decimal256(76, 20), 1, {null_buffer, data_decimal256_buffer}, 0);
  AssertArrayDataEqualsWithSwapEndian(data, expected_data);

  auto data_float_buffer = Buffer::FromString("01200560");
  data = ArrayData::Make(float32(), 2, {null_buffer, data_float_buffer}, 0);
  auto data_float32_buffer = Buffer::FromString("02100650");
  expected_data = ArrayData::Make(float32(), 2, {null_buffer, data_float32_buffer}, 0);
  AssertArrayDataEqualsWithSwapEndian(data, expected_data);

  data = ArrayData::Make(float64(), 1, {null_buffer, data_float_buffer});
  auto data_float64_buffer = Buffer::FromString("06500210");
  expected_data = ArrayData::Make(float64(), 1, {null_buffer, data_float64_buffer}, 0);
  AssertArrayDataEqualsWithSwapEndian(data, expected_data);

  // With offset > 0
  data =
      ArrayData::Make(int64(), 1, {null_buffer, data_int_buffer}, kUnknownNullCount, 1);
  ASSERT_RAISES(Invalid, ::arrow::internal::SwapEndianArrayData(data));
}

std::shared_ptr<ArrayData> ReplaceBuffers(const std::shared_ptr<ArrayData>& data,
                                          const int32_t buffer_index,
                                          const std::vector<uint8_t>& buffer_data) {
  const auto test_data = data->Copy();
  test_data->buffers[buffer_index] =
      std::make_shared<Buffer>(buffer_data.data(), buffer_data.size());
  return test_data;
}

std::shared_ptr<ArrayData> ReplaceBuffersInChild(const std::shared_ptr<ArrayData>& data,
                                                 const int32_t child_index,
                                                 const std::vector<uint8_t>& child_data) {
  const auto test_data = data->Copy();
  // assume updating only buffer[1] in child_data
  auto child_array_data = test_data->child_data[child_index]->Copy();
  child_array_data->buffers[1] =
      std::make_shared<Buffer>(child_data.data(), child_data.size());
  test_data->child_data[child_index] = child_array_data;
  return test_data;
}

std::shared_ptr<ArrayData> ReplaceBuffersInDictionary(
    const std::shared_ptr<ArrayData>& data, const int32_t buffer_index,
    const std::vector<uint8_t>& buffer_data) {
  const auto test_data = data->Copy();
  auto dict_array_data = test_data->dictionary->Copy();
  dict_array_data->buffers[buffer_index] =
      std::make_shared<Buffer>(buffer_data.data(), buffer_data.size());
  test_data->dictionary = dict_array_data;
  return test_data;
}

TEST(TestSwapEndianArrayData, BinaryType) {
  auto array = ArrayFromJSON(binary(), R"(["0123", null, "45"])");
  const std::vector<uint8_t> offset1 =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 6};
#else
      {0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0};
#endif
  auto expected_data = array->data();
  auto test_data = ReplaceBuffers(expected_data, 1, offset1);
  AssertArrayDataEqualsWithSwapEndian(test_data, expected_data);

  array = ArrayFromJSON(large_binary(), R"(["01234", null, "567"])");
  const std::vector<uint8_t> offset2 =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5,
       0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 8};
#else
      {0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0,
       5, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0};
#endif
  expected_data = array->data();
  test_data = ReplaceBuffers(expected_data, 1, offset2);
  AssertArrayDataEqualsWithSwapEndian(test_data, expected_data);

  array = ArrayFromJSON(fixed_size_binary(3), R"(["012", null, "345"])");
  expected_data = array->data();
  AssertArrayDataEqualsWithSwapEndian(expected_data, expected_data);
}

TEST(TestSwapEndianArrayData, StringType) {
  auto array = ArrayFromJSON(utf8(), R"(["ABCD", null, "EF"])");
  const std::vector<uint8_t> offset1 =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 6};
#else
      {0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0};
#endif
  auto expected_data = array->data();
  auto test_data = ReplaceBuffers(expected_data, 1, offset1);
  AssertArrayDataEqualsWithSwapEndian(test_data, expected_data);

  array = ArrayFromJSON(large_utf8(), R"(["ABCDE", null, "FGH"])");
  const std::vector<uint8_t> offset2 =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5,
       0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 8};
#else
      {0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0,
       5, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0};
#endif
  expected_data = array->data();
  test_data = ReplaceBuffers(expected_data, 1, offset2);
  AssertArrayDataEqualsWithSwapEndian(test_data, expected_data);
}

TEST(TestSwapEndianArrayData, ListType) {
  auto type1 = std::make_shared<ListType>(int32());
  auto array = ArrayFromJSON(type1, "[[0, 1, 2, 3], null, [4, 5]]");
  const std::vector<uint8_t> offset1 =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 6};
#else
      {0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0};
#endif
  const std::vector<uint8_t> data1 =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5};
#else
      {0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0};
#endif
  auto expected_data = array->data();
  auto test_data = ReplaceBuffers(expected_data, 1, offset1);
  test_data = ReplaceBuffersInChild(test_data, 0, data1);
  AssertArrayDataEqualsWithSwapEndian(test_data, expected_data);

  auto type2 = std::make_shared<LargeListType>(int64());
  array = ArrayFromJSON(type2, "[[0, 1, 2], null, [3]]");
  const std::vector<uint8_t> offset2 =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3,
       0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4};
#else
      {0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
       3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0};
#endif
  const std::vector<uint8_t> data2 =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
       0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3};
#else
      {0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
       2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0};
#endif
  expected_data = array->data();
  test_data = ReplaceBuffers(expected_data, 1, offset2);
  test_data = ReplaceBuffersInChild(test_data, 0, data2);
  AssertArrayDataEqualsWithSwapEndian(test_data, expected_data);

  auto type3 = std::make_shared<FixedSizeListType>(int32(), 2);
  array = ArrayFromJSON(type3, "[[0, 1], null, [2, 3]]");
  expected_data = array->data();
  const std::vector<uint8_t> data3 =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3};
#else
      {0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0};
#endif
  test_data = ReplaceBuffersInChild(expected_data, 0, data3);
  AssertArrayDataEqualsWithSwapEndian(test_data, expected_data);
}

TEST(TestSwapEndianArrayData, DictionaryType) {
  auto type = dictionary(int32(), int16());
  auto dict = ArrayFromJSON(int16(), "[4, 5, 6, 7]");
  DictionaryArray array(type, ArrayFromJSON(int32(), "[0, 2, 3]"), dict);
  auto expected_data = array.data();
  const std::vector<uint8_t> data1 =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3};
#else
      {0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0};
#endif
  const std::vector<uint8_t> data2 =
#if ARROW_LITTLE_ENDIAN
      {0, 4, 0, 5, 0, 6, 0, 7};
#else
      {4, 0, 5, 0, 6, 0, 7, 0};
#endif
  auto test_data = ReplaceBuffers(expected_data, 1, data1);
  test_data = ReplaceBuffersInDictionary(test_data, 1, data2);
  // dictionary must be explicitly swapped
  test_data->dictionary = *::arrow::internal::SwapEndianArrayData(test_data->dictionary);
  AssertArrayDataEqualsWithSwapEndian(test_data, expected_data);
}

TEST(TestSwapEndianArrayData, StructType) {
  auto array = ArrayFromJSON(struct_({field("a", int32()), field("b", utf8())}),
                             R"([{"a": 4, "b": null}, {"a": null, "b": "foo"}])");
  auto expected_data = array->data();
  const std::vector<uint8_t> data1 =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 4, 0, 0, 0, 0};
#else
      {4, 0, 0, 0, 0, 0, 0, 0};
#endif
  const std::vector<uint8_t> data2 =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3};
#else
      {0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0};
#endif
  auto test_data = ReplaceBuffersInChild(expected_data, 0, data1);
  test_data = ReplaceBuffersInChild(test_data, 1, data2);
  AssertArrayDataEqualsWithSwapEndian(test_data, expected_data);
}

TEST(TestSwapEndianArrayData, UnionType) {
  auto expected_i8 = ArrayFromJSON(int8(), "[127, null, null, null, null]");
  auto expected_str = ArrayFromJSON(utf8(), R"([null, "abcd", null, null, ""])");
  auto expected_i32 = ArrayFromJSON(int32(), "[null, null, 1, 2, null]");
  std::vector<uint8_t> expected_types_vector;
  expected_types_vector.push_back(Type::INT8);
  expected_types_vector.insert(expected_types_vector.end(), 2, Type::STRING);
  expected_types_vector.insert(expected_types_vector.end(), 2, Type::INT32);
  std::shared_ptr<Array> expected_types;
  ArrayFromVector<Int8Type, uint8_t>(expected_types_vector, &expected_types);
  auto arr1 = SparseUnionArray::Make(
      *expected_types, {expected_i8, expected_str, expected_i32}, {"i8", "str", "i32"},
      {Type::INT8, Type::STRING, Type::INT32});
  auto expected_data = (*arr1)->data();
  const std::vector<uint8_t> data1a =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4};
#else
      {0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0, 4, 0, 0, 0};
#endif
  const std::vector<uint8_t> data1b =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0};
#else
      {0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0};
#endif
  auto test_data = ReplaceBuffersInChild(expected_data, 1, data1a);
  test_data = ReplaceBuffersInChild(test_data, 2, data1b);
  AssertArrayDataEqualsWithSwapEndian(test_data, expected_data);

  expected_i8 = ArrayFromJSON(int8(), "[33, 10, -10]");
  expected_str = ArrayFromJSON(utf8(), R"(["abc", "", "def"])");
  expected_i32 = ArrayFromJSON(int32(), "[1, -259, 2]");
  auto expected_offsets = ArrayFromJSON(int32(), "[0, 0, 0, 1, 1, 1, 2, 2, 2]");
  auto arr2 = DenseUnionArray::Make(
      *expected_types, *expected_offsets, {expected_i8, expected_str, expected_i32},
      {"i8", "str", "i32"}, {Type::INT8, Type::STRING, Type::INT32});
  expected_data = (*arr2)->data();
  const std::vector<uint8_t> data2a =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
       0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 2};
#else
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0,
       0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0};
#endif
  const std::vector<uint8_t> data2b =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 6};
#else
      {0, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 6, 0, 0, 0};
#endif
  const std::vector<uint8_t> data2c =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 1, 255, 255, 254, 253, 0, 0, 0, 2};
#else
      {1, 0, 0, 0, 253, 254, 255, 255, 2, 0, 0, 0};
#endif
  test_data = ReplaceBuffers(expected_data, 2, data2a);
  test_data = ReplaceBuffersInChild(test_data, 1, data2b);
  test_data = ReplaceBuffersInChild(test_data, 2, data2c);
  AssertArrayDataEqualsWithSwapEndian(test_data, expected_data);
}

TEST(TestSwapEndianArrayData, ExtensionType) {
  auto array_int16 = ArrayFromJSON(int16(), "[0, 1, 2, 3]");
  auto ext_data = array_int16->data()->Copy();
  ext_data->type = std::make_shared<SmallintType>();
  auto array = MakeArray(ext_data);
  auto expected_data = array->data();
  const std::vector<uint8_t> data =
#if ARROW_LITTLE_ENDIAN
      {0, 0, 0, 1, 0, 2, 0, 3};
#else
      {0, 0, 1, 0, 2, 0, 3, 0};
#endif
  auto test_data = ReplaceBuffers(expected_data, 1, data);
  AssertArrayDataEqualsWithSwapEndian(test_data, expected_data);
}

}  // namespace arrow
