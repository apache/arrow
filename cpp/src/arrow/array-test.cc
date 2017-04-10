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
#include <numeric>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/ipc/test-common.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/test-common.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"

namespace arrow {

using std::string;
using std::vector;

class TestArray : public ::testing::Test {
 public:
  void SetUp() { pool_ = default_memory_pool(); }

 protected:
  MemoryPool* pool_;
};

TEST_F(TestArray, TestNullCount) {
  auto data = std::make_shared<PoolBuffer>(pool_);
  auto null_bitmap = std::make_shared<PoolBuffer>(pool_);

  std::unique_ptr<Int32Array> arr(new Int32Array(100, data, null_bitmap, 10));
  ASSERT_EQ(10, arr->null_count());

  std::unique_ptr<Int32Array> arr_no_nulls(new Int32Array(100, data));
  ASSERT_EQ(0, arr_no_nulls->null_count());
}

TEST_F(TestArray, TestLength) {
  auto data = std::make_shared<PoolBuffer>(pool_);
  std::unique_ptr<Int32Array> arr(new Int32Array(100, data));
  ASSERT_EQ(arr->length(), 100);
}

std::shared_ptr<Array> MakeArrayFromValidBytes(
    const vector<uint8_t>& v, MemoryPool* pool) {
  int64_t null_count = v.size() - std::accumulate(v.begin(), v.end(), 0);
  std::shared_ptr<Buffer> null_buf = test::bytes_to_null_buffer(v);

  BufferBuilder value_builder(pool);
  for (size_t i = 0; i < v.size(); ++i) {
    value_builder.Append<int32_t>(0);
  }

  std::shared_ptr<Array> arr(
      new Int32Array(v.size(), value_builder.Finish(), null_buf, null_count));
  return arr;
}

TEST_F(TestArray, TestEquality) {
  auto array = MakeArrayFromValidBytes({1, 0, 1, 1, 0, 1, 0, 0}, pool_);
  auto equal_array = MakeArrayFromValidBytes({1, 0, 1, 1, 0, 1, 0, 0}, pool_);
  auto unequal_array = MakeArrayFromValidBytes({1, 1, 1, 1, 0, 1, 0, 0}, pool_);

  EXPECT_TRUE(array->Equals(array));
  EXPECT_TRUE(array->Equals(equal_array));
  EXPECT_TRUE(equal_array->Equals(array));
  EXPECT_FALSE(equal_array->Equals(unequal_array));
  EXPECT_FALSE(unequal_array->Equals(equal_array));
  EXPECT_TRUE(array->RangeEquals(4, 8, 4, unequal_array));
  EXPECT_FALSE(array->RangeEquals(0, 4, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(0, 8, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(1, 2, 1, unequal_array));
}

TEST_F(TestArray, SliceRecomputeNullCount) {
  vector<uint8_t> valid_bytes = {1, 0, 1, 1, 0, 1, 0, 0, 0};

  auto array = MakeArrayFromValidBytes(valid_bytes, pool_);

  ASSERT_EQ(5, array->null_count());

  auto slice = array->Slice(1, 4);
  ASSERT_EQ(2, slice->null_count());

  slice = array->Slice(4);
  ASSERT_EQ(4, slice->null_count());

  slice = array->Slice(0);
  ASSERT_EQ(5, slice->null_count());

  // No bitmap, compute 0
  std::shared_ptr<MutableBuffer> data;
  const int kBufferSize = 64;
  ASSERT_OK(AllocateBuffer(pool_, kBufferSize, &data));
  memset(data->mutable_data(), 0, kBufferSize);

  auto arr = std::make_shared<Int32Array>(16, data, nullptr, -1);
  ASSERT_EQ(0, arr->null_count());
}

TEST_F(TestArray, TestIsNull) {
  // clang-format off
  vector<uint8_t> null_bitmap = {1, 0, 1, 1, 0, 1, 0, 0,
                                 1, 0, 1, 1, 0, 1, 0, 0,
                                 1, 0, 1, 1, 0, 1, 0, 0,
                                 1, 0, 1, 1, 0, 1, 0, 0,
                                 1, 0, 0, 1};
  // clang-format on
  int64_t null_count = 0;
  for (uint8_t x : null_bitmap) {
    if (x == 0) { ++null_count; }
  }

  std::shared_ptr<Buffer> null_buf = test::bytes_to_null_buffer(null_bitmap);
  std::unique_ptr<Array> arr;
  arr.reset(new Int32Array(null_bitmap.size(), nullptr, null_buf, null_count));

  ASSERT_EQ(null_count, arr->null_count());
  ASSERT_EQ(5, null_buf->size());

  ASSERT_TRUE(arr->null_bitmap()->Equals(*null_buf.get()));

  for (size_t i = 0; i < null_bitmap.size(); ++i) {
    EXPECT_EQ(null_bitmap[i] != 0, !arr->IsNull(i)) << i;
  }
}

TEST_F(TestArray, BuildLargeInMemoryArray) {
  const int64_t length = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1;

  BooleanBuilder builder(default_memory_pool());
  ASSERT_OK(builder.Reserve(length));
  ASSERT_OK(builder.Advance(length));

  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  ASSERT_EQ(length, result->length());
}

TEST_F(TestArray, TestCopy) {}

// ----------------------------------------------------------------------
// Primitive type tests

TEST_F(TestBuilder, TestReserve) {
  builder_->Init(10);
  ASSERT_EQ(2, builder_->null_bitmap()->size());

  builder_->Reserve(30);
  ASSERT_EQ(4, builder_->null_bitmap()->size());
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

    std::shared_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_ = std::dynamic_pointer_cast<BuilderType>(tmp);

    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_nn_ = std::dynamic_pointer_cast<BuilderType>(tmp);
  }

  void RandomData(int64_t N, double pct_null = 0.1) {
    Attrs::draw(N, &draws_);

    valid_bytes_.resize(static_cast<size_t>(N));
    test::random_null_bytes(N, pct_null, valid_bytes_.data());
  }

  void Check(const std::shared_ptr<BuilderType>& builder, bool nullable) {
    int64_t size = builder->length();

    auto ex_data = std::make_shared<Buffer>(
        reinterpret_cast<uint8_t*>(draws_.data()), size * sizeof(T));

    std::shared_ptr<Buffer> ex_null_bitmap;
    int64_t ex_null_count = 0;

    if (nullable) {
      ex_null_bitmap = test::bytes_to_null_buffer(valid_bytes_);
      ex_null_count = test::null_count(valid_bytes_);
    } else {
      ex_null_bitmap = nullptr;
    }

    auto expected =
        std::make_shared<ArrayType>(size, ex_data, ex_null_bitmap, ex_null_count);

    std::shared_ptr<Array> out;
    ASSERT_OK(builder->Finish(&out));

    std::shared_ptr<ArrayType> result = std::dynamic_pointer_cast<ArrayType>(out);

    // Builder is now reset
    ASSERT_EQ(0, builder->length());
    ASSERT_EQ(0, builder->capacity());
    ASSERT_EQ(0, builder->null_count());
    ASSERT_EQ(nullptr, builder->data());

    ASSERT_EQ(ex_null_count, result->null_count());
    ASSERT_TRUE(result->Equals(*expected));
  }

 protected:
  std::shared_ptr<DataType> type_;
  std::shared_ptr<BuilderType> builder_;
  std::shared_ptr<BuilderType> builder_nn_;

  vector<T> draws_;
  vector<uint8_t> valid_bytes_;
};

#define PTYPE_DECL(CapType, c_type)               \
  typedef CapType##Array ArrayType;               \
  typedef CapType##Builder BuilderType;           \
  typedef CapType##Type Type;                     \
  typedef c_type T;                               \
                                                  \
  static std::shared_ptr<DataType> type() {       \
    return std::shared_ptr<DataType>(new Type()); \
  }

#define PINT_DECL(CapType, c_type, LOWER, UPPER)    \
  struct P##CapType {                               \
    PTYPE_DECL(CapType, c_type);                    \
    static void draw(int64_t N, vector<T>* draws) { \
      test::randint<T>(N, LOWER, UPPER, draws);     \
    }                                               \
  }

#define PFLOAT_DECL(CapType, c_type, LOWER, UPPER)     \
  struct P##CapType {                                  \
    PTYPE_DECL(CapType, c_type);                       \
    static void draw(int64_t N, vector<T>* draws) {    \
      test::random_real<T>(N, 0, LOWER, UPPER, draws); \
    }                                                  \
  }

PINT_DECL(UInt8, uint8_t, 0, UINT8_MAX);
PINT_DECL(UInt16, uint16_t, 0, UINT16_MAX);
PINT_DECL(UInt32, uint32_t, 0, UINT32_MAX);
PINT_DECL(UInt64, uint64_t, 0, UINT64_MAX);

PINT_DECL(Int8, int8_t, INT8_MIN, INT8_MAX);
PINT_DECL(Int16, int16_t, INT16_MIN, INT16_MAX);
PINT_DECL(Int32, int32_t, INT32_MIN, INT32_MAX);
PINT_DECL(Int64, int64_t, INT64_MIN, INT64_MAX);

PFLOAT_DECL(Float, float, -1000, 1000);
PFLOAT_DECL(Double, double, -1000, 1000);

struct PBoolean {
  PTYPE_DECL(Boolean, uint8_t);
};

template <>
void TestPrimitiveBuilder<PBoolean>::RandomData(int64_t N, double pct_null) {
  draws_.resize(static_cast<size_t>(N));
  valid_bytes_.resize(static_cast<size_t>(N));

  test::random_null_bytes(N, 0.5, draws_.data());
  test::random_null_bytes(N, pct_null, valid_bytes_.data());
}

template <>
void TestPrimitiveBuilder<PBoolean>::Check(
    const std::shared_ptr<BooleanBuilder>& builder, bool nullable) {
  int64_t size = builder->length();

  auto ex_data = test::bytes_to_null_buffer(draws_);

  std::shared_ptr<Buffer> ex_null_bitmap;
  int64_t ex_null_count = 0;

  if (nullable) {
    ex_null_bitmap = test::bytes_to_null_buffer(valid_bytes_);
    ex_null_count = test::null_count(valid_bytes_);
  } else {
    ex_null_bitmap = nullptr;
  }

  auto expected =
      std::make_shared<BooleanArray>(size, ex_data, ex_null_bitmap, ex_null_count);

  std::shared_ptr<Array> out;
  ASSERT_OK(builder->Finish(&out));
  std::shared_ptr<BooleanArray> result = std::dynamic_pointer_cast<BooleanArray>(out);

  // Builder is now reset
  ASSERT_EQ(0, builder->length());
  ASSERT_EQ(0, builder->capacity());
  ASSERT_EQ(0, builder->null_count());
  ASSERT_EQ(nullptr, builder->data());

  ASSERT_EQ(ex_null_count, result->null_count());

  ASSERT_EQ(expected->length(), result->length());

  for (int64_t i = 0; i < result->length(); ++i) {
    if (nullable) { ASSERT_EQ(valid_bytes_[i] == 0, result->IsNull(i)) << i; }
    bool actual = BitUtil::GetBit(result->data()->data(), i);
    ASSERT_EQ(draws_[i] != 0, actual) << i;
  }
  ASSERT_TRUE(result->Equals(*expected));
}

typedef ::testing::Types<PBoolean, PUInt8, PUInt16, PUInt32, PUInt64, PInt8, PInt16,
    PInt32, PInt64, PFloat, PDouble>
    Primitives;

TYPED_TEST_CASE(TestPrimitiveBuilder, Primitives);

#define DECL_T() typedef typename TestFixture::T T;

#define DECL_TYPE() typedef typename TestFixture::Type Type;

#define DECL_ARRAYTYPE() typedef typename TestFixture::ArrayType ArrayType;

TYPED_TEST(TestPrimitiveBuilder, TestInit) {
  DECL_TYPE();

  int64_t n = 1000;
  ASSERT_OK(this->builder_->Reserve(n));
  ASSERT_EQ(BitUtil::NextPower2(n), this->builder_->capacity());
  ASSERT_EQ(BitUtil::NextPower2(TypeTraits<Type>::bytes_required(n)),
      this->builder_->data()->size());

  // unsure if this should go in all builder classes
  ASSERT_EQ(0, this->builder_->num_children());
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendNull) {
  int64_t size = 1000;
  for (int64_t i = 0; i < size; ++i) {
    ASSERT_OK(this->builder_->AppendNull());
  }

  std::shared_ptr<Array> result;
  ASSERT_OK(this->builder_->Finish(&result));

  for (int64_t i = 0; i < size; ++i) {
    ASSERT_TRUE(result->IsNull(i)) << i;
  }
}

TYPED_TEST(TestPrimitiveBuilder, TestArrayDtorDealloc) {
  DECL_T();

  int64_t size = 1000;

  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;

  int64_t memory_before = this->pool_->bytes_allocated();

  this->RandomData(size);

  this->builder_->Reserve(size);

  int64_t i;
  for (i = 0; i < size; ++i) {
    if (valid_bytes[i] > 0) {
      this->builder_->Append(draws[i]);
    } else {
      this->builder_->AppendNull();
    }
  }

  do {
    std::shared_ptr<Array> result;
    ASSERT_OK(this->builder_->Finish(&result));
  } while (false);

  ASSERT_EQ(memory_before, this->pool_->bytes_allocated());
}

TYPED_TEST(TestPrimitiveBuilder, Equality) {
  DECL_T();

  const int64_t size = 1000;
  this->RandomData(size);
  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;
  std::shared_ptr<Array> array, equal_array, unequal_array;
  auto builder = this->builder_.get();
  ASSERT_OK(MakeArray(valid_bytes, draws, size, builder, &array));
  ASSERT_OK(MakeArray(valid_bytes, draws, size, builder, &equal_array));

  // Make the not equal array by negating the first valid element with itself.
  const auto first_valid = std::find_if(
      valid_bytes.begin(), valid_bytes.end(), [](uint8_t valid) { return valid > 0; });
  const int64_t first_valid_idx = std::distance(valid_bytes.begin(), first_valid);
  // This should be true with a very high probability, but might introduce flakiness
  ASSERT_LT(first_valid_idx, size - 1);
  draws[first_valid_idx] =
      static_cast<T>(~*reinterpret_cast<int64_t*>(&draws[first_valid_idx]));
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
  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;
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

  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;

  this->RandomData(size);

  this->builder_->Reserve(1000);
  this->builder_nn_->Reserve(1000);

  int64_t null_count = 0;
  // Append the first 1000
  for (size_t i = 0; i < 1000; ++i) {
    if (valid_bytes[i] > 0) {
      this->builder_->Append(draws[i]);
    } else {
      this->builder_->AppendNull();
      ++null_count;
    }
    this->builder_nn_->Append(draws[i]);
  }

  ASSERT_EQ(null_count, this->builder_->null_count());

  ASSERT_EQ(1000, this->builder_->length());
  ASSERT_EQ(1024, this->builder_->capacity());

  ASSERT_EQ(1000, this->builder_nn_->length());
  ASSERT_EQ(1024, this->builder_nn_->capacity());

  this->builder_->Reserve(size - 1000);
  this->builder_nn_->Reserve(size - 1000);

  // Append the next 9000
  for (size_t i = 1000; i < size; ++i) {
    if (valid_bytes[i] > 0) {
      this->builder_->Append(draws[i]);
    } else {
      this->builder_->AppendNull();
    }
    this->builder_nn_->Append(draws[i]);
  }

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(BitUtil::NextPower2(size), this->builder_->capacity());

  ASSERT_EQ(size, this->builder_nn_->length());
  ASSERT_EQ(BitUtil::NextPower2(size), this->builder_nn_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendVector) {
  DECL_T();

  int64_t size = 10000;
  this->RandomData(size);

  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;

  // first slug
  int64_t K = 1000;

  ASSERT_OK(this->builder_->Append(draws.data(), K, valid_bytes.data()));
  ASSERT_OK(this->builder_nn_->Append(draws.data(), K));

  ASSERT_EQ(1000, this->builder_->length());
  ASSERT_EQ(1024, this->builder_->capacity());

  ASSERT_EQ(1000, this->builder_nn_->length());
  ASSERT_EQ(1024, this->builder_nn_->capacity());

  // Append the next 9000
  ASSERT_OK(this->builder_->Append(draws.data() + K, size - K, valid_bytes.data() + K));
  ASSERT_OK(this->builder_nn_->Append(draws.data() + K, size - K));

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(BitUtil::NextPower2(size), this->builder_->capacity());

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
  DECL_TYPE();

  int64_t cap = kMinBuilderCapacity * 2;

  ASSERT_OK(this->builder_->Reserve(cap));
  ASSERT_EQ(cap, this->builder_->capacity());

  ASSERT_EQ(TypeTraits<Type>::bytes_required(cap), this->builder_->data()->size());
  ASSERT_EQ(BitUtil::BytesForBits(cap), this->builder_->null_bitmap()->size());
}

TYPED_TEST(TestPrimitiveBuilder, TestReserve) {
  ASSERT_OK(this->builder_->Reserve(10));
  ASSERT_EQ(0, this->builder_->length());
  ASSERT_EQ(kMinBuilderCapacity, this->builder_->capacity());

  ASSERT_OK(this->builder_->Reserve(90));
  ASSERT_OK(this->builder_->Advance(100));
  ASSERT_OK(this->builder_->Reserve(kMinBuilderCapacity));

  ASSERT_EQ(BitUtil::NextPower2(kMinBuilderCapacity + 100), this->builder_->capacity());
}

template <typename TYPE>
void CheckSliceApproxEquals() {
  using T = typename TYPE::c_type;

  const int64_t kSize = 50;
  vector<T> draws1;
  vector<T> draws2;

  const uint32_t kSeed = 0;
  test::random_real<T>(kSize, kSeed, 0, 100, &draws1);
  test::random_real<T>(kSize, kSeed + 1, 0, 100, &draws2);

  // Make the draws equal in the sliced segment, but unequal elsewhere (to
  // catch not using the slice offset)
  for (int64_t i = 10; i < 30; ++i) {
    draws2[i] = draws1[i];
  }

  vector<bool> is_valid;
  test::random_is_valid(kSize, 0.1, &is_valid);

  std::shared_ptr<Array> array1, array2;
  ArrayFromVector<TYPE, T>(is_valid, draws1, &array1);
  ArrayFromVector<TYPE, T>(is_valid, draws2, &array2);

  std::shared_ptr<Array> slice1 = array1->Slice(10, 20);
  std::shared_ptr<Array> slice2 = array2->Slice(10, 20);

  ASSERT_TRUE(slice1->ApproxEquals(slice2));
}

TEST(TestPrimitiveAdHoc, FloatingSliceApproxEquals) {
  CheckSliceApproxEquals<FloatType>();
  CheckSliceApproxEquals<DoubleType>();
}

// ----------------------------------------------------------------------
// String / Binary tests

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
  vector<int32_t> offsets_;
  vector<char> chars_;
  vector<uint8_t> valid_bytes_;

  vector<string> expected_;

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
  std::shared_ptr<DataType> type = strings_->type();

  ASSERT_EQ(Type::STRING, type->id());
  ASSERT_EQ(Type::STRING, strings_->type_id());
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
  vector<string> strings = {"", "bb", "a", "", "ccc"};
  vector<uint8_t> is_null = {0, 0, 0, 1, 0};

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
  vector<int32_t> offsets_;
  vector<char> chars_;
  vector<uint8_t> valid_bytes_;

  vector<string> expected_;

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
  std::shared_ptr<DataType> type = strings_->type();

  ASSERT_EQ(Type::BINARY, type->id());
  ASSERT_EQ(Type::BINARY, strings_->type_id());
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

  string empty_string("");

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
  vector<string> strings = {"", "bb", "a", "", "ccc"};
  vector<uint8_t> is_null = {0, 0, 0, 1, 0};

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

  vector<string> strings = {"foo", "", "bar", "baz", "qux", ""};
  vector<uint8_t> is_null = {0, 1, 0, 1, 0, 0};

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
// FixedSizeBinary tests

class TestFWBinaryArray : public ::testing::Test {
 public:
  void SetUp() {}

  void InitBuilder(int byte_width) {
    auto type = fixed_size_binary(byte_width);
    builder_.reset(new FixedSizeBinaryBuilder(default_memory_pool(), type));
  }

 protected:
  std::unique_ptr<FixedSizeBinaryBuilder> builder_;
};

TEST_F(TestFWBinaryArray, Builder) {
  const int32_t byte_width = 10;
  int64_t length = 4096;

  int64_t nbytes = length * byte_width;

  vector<uint8_t> data(nbytes);
  test::random_bytes(nbytes, 0, data.data());

  vector<uint8_t> is_valid(length);
  test::random_null_bytes(length, 0.1, is_valid.data());

  const uint8_t* raw_data = data.data();

  std::shared_ptr<Array> result;

  auto CheckResult = [this, &length, &is_valid, &raw_data, &byte_width](
      const Array& result) {
    // Verify output
    const auto& fw_result = static_cast<const FixedSizeBinaryArray&>(result);

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
      builder_->Append(
          string(reinterpret_cast<const char*>(raw_data + byte_width * i), byte_width));
    } else {
      builder_->AppendNull();
    }
  }

  ASSERT_OK(builder_->Finish(&result));
  CheckResult(*result);
}

TEST_F(TestFWBinaryArray, EqualsRangeEquals) {
  // Check that we don't compare data in null slots

  auto type = fixed_size_binary(4);
  FixedSizeBinaryBuilder builder1(default_memory_pool(), type);
  FixedSizeBinaryBuilder builder2(default_memory_pool(), type);

  ASSERT_OK(builder1.Append("foo1"));
  ASSERT_OK(builder1.AppendNull());

  ASSERT_OK(builder2.Append("foo1"));
  ASSERT_OK(builder2.Append("foo2"));

  std::shared_ptr<Array> array1, array2;
  ASSERT_OK(builder1.Finish(&array1));
  ASSERT_OK(builder2.Finish(&array2));

  const auto& a1 = static_cast<const FixedSizeBinaryArray&>(*array1);
  const auto& a2 = static_cast<const FixedSizeBinaryArray&>(*array2);

  FixedSizeBinaryArray equal1(type, 2, a1.data(), a1.null_bitmap(), 1);
  FixedSizeBinaryArray equal2(type, 2, a2.data(), a1.null_bitmap(), 1);

  ASSERT_TRUE(equal1.Equals(equal2));
  ASSERT_TRUE(equal1.RangeEquals(equal2, 0, 2, 0));
}

TEST_F(TestFWBinaryArray, ZeroSize) {
  auto type = fixed_size_binary(0);
  FixedSizeBinaryBuilder builder(default_memory_pool(), type);

  ASSERT_OK(builder.Append(nullptr));
  ASSERT_OK(builder.Append(nullptr));
  ASSERT_OK(builder.Append(nullptr));
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.AppendNull());

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));

  const auto& fw_array = static_cast<const FixedSizeBinaryArray&>(*array);

  // data is never allocated
  ASSERT_TRUE(fw_array.data() == nullptr);
  ASSERT_EQ(0, fw_array.byte_width());

  ASSERT_EQ(6, array->length());
  ASSERT_EQ(3, array->null_count());
}

TEST_F(TestFWBinaryArray, Slice) {
  auto type = fixed_size_binary(4);
  FixedSizeBinaryBuilder builder(default_memory_pool(), type);

  vector<string> strings = {"foo1", "foo2", "foo3", "foo4", "foo5"};
  vector<uint8_t> is_null = {0, 1, 0, 0, 0};

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

// ----------------------------------------------------------------------
// List tests

class TestListBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();

    value_type_ = int32();
    type_ = list(value_type_);

    std::shared_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));
    builder_ = std::dynamic_pointer_cast<ListBuilder>(tmp);
  }

  void Done() {
    std::shared_ptr<Array> out;
    EXPECT_OK(builder_->Finish(&out));
    result_ = std::dynamic_pointer_cast<ListArray>(out);
  }

 protected:
  std::shared_ptr<DataType> value_type_;
  std::shared_ptr<DataType> type_;

  std::shared_ptr<ListBuilder> builder_;
  std::shared_ptr<ListArray> result_;
};

TEST_F(TestListBuilder, Equality) {
  Int32Builder* vb = static_cast<Int32Builder*>(builder_->value_builder().get());

  std::shared_ptr<Array> array, equal_array, unequal_array;
  vector<int32_t> equal_offsets = {0, 1, 2, 5, 6, 7, 8, 10};
  vector<int32_t> equal_values = {1, 2, 3, 4, 5, 2, 2, 2, 5, 6};
  vector<int32_t> unequal_offsets = {0, 1, 4, 7};
  vector<int32_t> unequal_values = {1, 2, 2, 2, 3, 4, 5};

  // setup two equal arrays
  ASSERT_OK(builder_->Append(equal_offsets.data(), equal_offsets.size()));
  ASSERT_OK(vb->Append(equal_values.data(), equal_values.size()));

  ASSERT_OK(builder_->Finish(&array));
  ASSERT_OK(builder_->Append(equal_offsets.data(), equal_offsets.size()));
  ASSERT_OK(vb->Append(equal_values.data(), equal_values.size()));

  ASSERT_OK(builder_->Finish(&equal_array));
  // now an unequal one
  ASSERT_OK(builder_->Append(unequal_offsets.data(), unequal_offsets.size()));
  ASSERT_OK(vb->Append(unequal_values.data(), unequal_values.size()));

  ASSERT_OK(builder_->Finish(&unequal_array));

  // Test array equality
  EXPECT_TRUE(array->Equals(array));
  EXPECT_TRUE(array->Equals(equal_array));
  EXPECT_TRUE(equal_array->Equals(array));
  EXPECT_FALSE(equal_array->Equals(unequal_array));
  EXPECT_FALSE(unequal_array->Equals(equal_array));

  // Test range equality
  EXPECT_TRUE(array->RangeEquals(0, 1, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(0, 2, 0, unequal_array));
  EXPECT_FALSE(array->RangeEquals(1, 2, 1, unequal_array));
  EXPECT_TRUE(array->RangeEquals(2, 3, 2, unequal_array));

  // Check with slices, ARROW-33
  std::shared_ptr<Array> slice, slice2;

  slice = array->Slice(2);
  slice2 = array->Slice(2);
  ASSERT_EQ(array->length() - 2, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(2, slice->length(), 0, slice));

  // Chained slices
  slice2 = array->Slice(1)->Slice(1);
  ASSERT_TRUE(slice->Equals(slice2));

  slice = array->Slice(1, 4);
  slice2 = array->Slice(1, 4);
  ASSERT_EQ(4, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(1, 5, 0, slice));
}

TEST_F(TestListBuilder, TestResize) {}

TEST_F(TestListBuilder, TestAppendNull) {
  ASSERT_OK(builder_->AppendNull());
  ASSERT_OK(builder_->AppendNull());

  Done();

  ASSERT_OK(result_->Validate());
  ASSERT_TRUE(result_->IsNull(0));
  ASSERT_TRUE(result_->IsNull(1));

  ASSERT_EQ(0, result_->raw_value_offsets()[0]);
  ASSERT_EQ(0, result_->value_offset(1));
  ASSERT_EQ(0, result_->value_offset(2));

  Int32Array* values = static_cast<Int32Array*>(result_->values().get());
  ASSERT_EQ(0, values->length());
}

void ValidateBasicListArray(const ListArray* result, const vector<int32_t>& values,
    const vector<uint8_t>& is_valid) {
  ASSERT_OK(result->Validate());
  ASSERT_EQ(1, result->null_count());
  ASSERT_EQ(0, result->values()->null_count());

  ASSERT_EQ(3, result->length());
  vector<int32_t> ex_offsets = {0, 3, 3, 7};
  for (size_t i = 0; i < ex_offsets.size(); ++i) {
    ASSERT_EQ(ex_offsets[i], result->value_offset(i));
  }

  for (int i = 0; i < result->length(); ++i) {
    ASSERT_EQ(is_valid[i] == 0, result->IsNull(i));
  }

  ASSERT_EQ(7, result->values()->length());
  Int32Array* varr = static_cast<Int32Array*>(result->values().get());

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(values[i], varr->Value(i));
  }
}

TEST_F(TestListBuilder, TestBasics) {
  vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6};
  vector<int> lengths = {3, 0, 4};
  vector<uint8_t> is_valid = {1, 0, 1};

  Int32Builder* vb = static_cast<Int32Builder*>(builder_->value_builder().get());

  ASSERT_OK(builder_->Reserve(lengths.size()));
  ASSERT_OK(vb->Reserve(values.size()));

  int pos = 0;
  for (size_t i = 0; i < lengths.size(); ++i) {
    ASSERT_OK(builder_->Append(is_valid[i] > 0));
    for (int j = 0; j < lengths[i]; ++j) {
      vb->Append(values[pos++]);
    }
  }

  Done();
  ValidateBasicListArray(result_.get(), values, is_valid);
}

TEST_F(TestListBuilder, BulkAppend) {
  vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6};
  vector<int> lengths = {3, 0, 4};
  vector<uint8_t> is_valid = {1, 0, 1};
  vector<int32_t> offsets = {0, 3, 3};

  Int32Builder* vb = static_cast<Int32Builder*>(builder_->value_builder().get());
  ASSERT_OK(vb->Reserve(values.size()));

  builder_->Append(offsets.data(), offsets.size(), is_valid.data());
  for (int32_t value : values) {
    vb->Append(value);
  }
  Done();
  ValidateBasicListArray(result_.get(), values, is_valid);
}

TEST_F(TestListBuilder, BulkAppendInvalid) {
  vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6};
  vector<int> lengths = {3, 0, 4};
  vector<uint8_t> is_null = {0, 1, 0};
  vector<uint8_t> is_valid = {1, 0, 1};
  vector<int32_t> offsets = {0, 2, 4};  // should be 0, 3, 3 given the is_null array

  Int32Builder* vb = static_cast<Int32Builder*>(builder_->value_builder().get());
  ASSERT_OK(vb->Reserve(values.size()));

  builder_->Append(offsets.data(), offsets.size(), is_valid.data());
  builder_->Append(offsets.data(), offsets.size(), is_valid.data());
  for (int32_t value : values) {
    vb->Append(value);
  }

  Done();
  ASSERT_RAISES(Invalid, result_->Validate());
}

TEST_F(TestListBuilder, TestZeroLength) {
  // All buffers are null
  Done();
  ASSERT_OK(result_->Validate());
}

// ----------------------------------------------------------------------
// DictionaryArray tests

TEST(TestDictionary, Basics) {
  vector<int32_t> values = {100, 1000, 10000, 100000};
  std::shared_ptr<Array> dict;
  ArrayFromVector<Int32Type, int32_t>(values, &dict);

  std::shared_ptr<DictionaryType> type1 =
      std::dynamic_pointer_cast<DictionaryType>(dictionary(int16(), dict));
  DictionaryType type2(int16(), dict);

  ASSERT_TRUE(int16()->Equals(type1->index_type()));
  ASSERT_TRUE(type1->dictionary()->Equals(dict));

  ASSERT_TRUE(int16()->Equals(type2.index_type()));
  ASSERT_TRUE(type2.dictionary()->Equals(dict));

  ASSERT_EQ("dictionary<values=int32, indices=int16>", type1->ToString());
}

TEST(TestDictionary, Equals) {
  vector<bool> is_valid = {true, true, false, true, true, true};

  std::shared_ptr<Array> dict;
  vector<string> dict_values = {"foo", "bar", "baz"};
  ArrayFromVector<StringType, string>(dict_values, &dict);
  std::shared_ptr<DataType> dict_type = dictionary(int16(), dict);

  std::shared_ptr<Array> dict2;
  vector<string> dict2_values = {"foo", "bar", "baz", "qux"};
  ArrayFromVector<StringType, string>(dict2_values, &dict2);
  std::shared_ptr<DataType> dict2_type = dictionary(int16(), dict2);

  std::shared_ptr<Array> indices;
  vector<int16_t> indices_values = {1, 2, -1, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(is_valid, indices_values, &indices);

  std::shared_ptr<Array> indices2;
  vector<int16_t> indices2_values = {1, 2, 0, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(is_valid, indices2_values, &indices2);

  std::shared_ptr<Array> indices3;
  vector<int16_t> indices3_values = {1, 1, 0, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(is_valid, indices3_values, &indices3);

  auto array = std::make_shared<DictionaryArray>(dict_type, indices);
  auto array2 = std::make_shared<DictionaryArray>(dict_type, indices2);
  auto array3 = std::make_shared<DictionaryArray>(dict2_type, indices);
  auto array4 = std::make_shared<DictionaryArray>(dict_type, indices3);

  ASSERT_TRUE(array->Equals(array));

  // Equal, because the unequal index is masked by null
  ASSERT_TRUE(array->Equals(array2));

  // Unequal dictionaries
  ASSERT_FALSE(array->Equals(array3));

  // Unequal indices
  ASSERT_FALSE(array->Equals(array4));

  // RangeEquals
  ASSERT_TRUE(array->RangeEquals(3, 6, 3, array4));
  ASSERT_FALSE(array->RangeEquals(1, 3, 1, array4));

  // ARROW-33 Test slices
  const int64_t size = array->length();

  std::shared_ptr<Array> slice, slice2;
  slice = array->Array::Slice(2);
  slice2 = array->Array::Slice(2);
  ASSERT_EQ(size - 2, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(2, array->length(), 0, slice));

  // Chained slices
  slice2 = array->Array::Slice(1)->Array::Slice(1);
  ASSERT_TRUE(slice->Equals(slice2));

  slice = array->Slice(1, 3);
  slice2 = array->Slice(1, 3);
  ASSERT_EQ(3, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(1, 4, 0, slice));
}

TEST(TestDictionary, Validate) {
  vector<bool> is_valid = {true, true, false, true, true, true};

  std::shared_ptr<Array> dict;
  vector<string> dict_values = {"foo", "bar", "baz"};
  ArrayFromVector<StringType, string>(dict_values, &dict);
  std::shared_ptr<DataType> dict_type = dictionary(int16(), dict);

  std::shared_ptr<Array> indices;
  vector<uint8_t> indices_values = {1, 2, 0, 0, 2, 0};
  ArrayFromVector<UInt8Type, uint8_t>(is_valid, indices_values, &indices);

  std::shared_ptr<Array> indices2;
  vector<float> indices2_values = {1., 2., 0., 0., 2., 0.};
  ArrayFromVector<FloatType, float>(is_valid, indices2_values, &indices2);

  std::shared_ptr<Array> indices3;
  vector<int64_t> indices3_values = {1, 2, 0, 0, 2, 0};
  ArrayFromVector<Int64Type, int64_t>(is_valid, indices3_values, &indices3);

  std::shared_ptr<Array> arr = std::make_shared<DictionaryArray>(dict_type, indices);
  std::shared_ptr<Array> arr2 = std::make_shared<DictionaryArray>(dict_type, indices2);
  std::shared_ptr<Array> arr3 = std::make_shared<DictionaryArray>(dict_type, indices3);

  // Only checking index type for now
  ASSERT_OK(arr->Validate());
  ASSERT_RAISES(Invalid, arr2->Validate());
  ASSERT_OK(arr3->Validate());
}

// ----------------------------------------------------------------------
// Struct tests

void ValidateBasicStructArray(const StructArray* result,
    const vector<uint8_t>& struct_is_valid, const vector<char>& list_values,
    const vector<uint8_t>& list_is_valid, const vector<int>& list_lengths,
    const vector<int>& list_offsets, const vector<int32_t>& int_values) {
  ASSERT_EQ(4, result->length());
  ASSERT_OK(result->Validate());

  auto list_char_arr = static_cast<ListArray*>(result->field(0).get());
  auto char_arr = static_cast<Int8Array*>(list_char_arr->values().get());
  auto int32_arr = static_cast<Int32Array*>(result->field(1).get());

  ASSERT_EQ(0, result->null_count());
  ASSERT_EQ(1, list_char_arr->null_count());
  ASSERT_EQ(0, int32_arr->null_count());

  // List<char>
  ASSERT_EQ(4, list_char_arr->length());
  ASSERT_EQ(10, list_char_arr->values()->length());
  for (size_t i = 0; i < list_offsets.size(); ++i) {
    ASSERT_EQ(list_offsets[i], list_char_arr->raw_value_offsets()[i]);
  }
  for (size_t i = 0; i < list_values.size(); ++i) {
    ASSERT_EQ(list_values[i], char_arr->Value(i));
  }

  // Int32
  ASSERT_EQ(4, int32_arr->length());
  for (size_t i = 0; i < int_values.size(); ++i) {
    ASSERT_EQ(int_values[i], int32_arr->Value(i));
  }
}

// ----------------------------------------------------------------------------------
// Struct test
class TestStructBuilder : public TestBuilder {
 public:
  void SetUp() {
    TestBuilder::SetUp();

    auto int32_type = int32();
    auto char_type = int8();
    auto list_type = list(char_type);

    vector<std::shared_ptr<DataType>> types = {list_type, int32_type};
    vector<FieldPtr> fields;
    fields.push_back(FieldPtr(new Field("list", list_type)));
    fields.push_back(FieldPtr(new Field("int", int32_type)));

    type_ = struct_(fields);
    value_fields_ = fields;

    std::shared_ptr<ArrayBuilder> tmp;
    ASSERT_OK(MakeBuilder(pool_, type_, &tmp));

    builder_ = std::dynamic_pointer_cast<StructBuilder>(tmp);
    ASSERT_EQ(2, static_cast<int>(builder_->field_builders().size()));
  }

  void Done() {
    std::shared_ptr<Array> out;
    ASSERT_OK(builder_->Finish(&out));
    result_ = std::dynamic_pointer_cast<StructArray>(out);
  }

 protected:
  vector<FieldPtr> value_fields_;
  std::shared_ptr<DataType> type_;

  std::shared_ptr<StructBuilder> builder_;
  std::shared_ptr<StructArray> result_;
};

TEST_F(TestStructBuilder, TestAppendNull) {
  ASSERT_OK(builder_->AppendNull());
  ASSERT_OK(builder_->AppendNull());
  ASSERT_EQ(2, static_cast<int>(builder_->field_builders().size()));

  ListBuilder* list_vb = static_cast<ListBuilder*>(builder_->field_builder(0).get());
  ASSERT_OK(list_vb->AppendNull());
  ASSERT_OK(list_vb->AppendNull());
  ASSERT_EQ(2, list_vb->length());

  Int32Builder* int_vb = static_cast<Int32Builder*>(builder_->field_builder(1).get());
  ASSERT_OK(int_vb->AppendNull());
  ASSERT_OK(int_vb->AppendNull());
  ASSERT_EQ(2, int_vb->length());

  Done();

  ASSERT_OK(result_->Validate());

  ASSERT_EQ(2, static_cast<int>(result_->fields().size()));
  ASSERT_EQ(2, result_->length());
  ASSERT_EQ(2, result_->field(0)->length());
  ASSERT_EQ(2, result_->field(1)->length());
  ASSERT_TRUE(result_->IsNull(0));
  ASSERT_TRUE(result_->IsNull(1));
  ASSERT_TRUE(result_->field(0)->IsNull(0));
  ASSERT_TRUE(result_->field(0)->IsNull(1));
  ASSERT_TRUE(result_->field(1)->IsNull(0));
  ASSERT_TRUE(result_->field(1)->IsNull(1));

  ASSERT_EQ(Type::LIST, result_->field(0)->type_id());
  ASSERT_EQ(Type::INT32, result_->field(1)->type_id());
}

TEST_F(TestStructBuilder, TestBasics) {
  vector<int32_t> int_values = {1, 2, 3, 4};
  vector<char> list_values = {'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'};
  vector<int> list_lengths = {3, 0, 3, 4};
  vector<int> list_offsets = {0, 3, 3, 6, 10};
  vector<uint8_t> list_is_valid = {1, 0, 1, 1};
  vector<uint8_t> struct_is_valid = {1, 1, 1, 1};

  ListBuilder* list_vb = static_cast<ListBuilder*>(builder_->field_builder(0).get());
  Int8Builder* char_vb = static_cast<Int8Builder*>(list_vb->value_builder().get());
  Int32Builder* int_vb = static_cast<Int32Builder*>(builder_->field_builder(1).get());
  ASSERT_EQ(2, static_cast<int>(builder_->field_builders().size()));

  EXPECT_OK(builder_->Resize(list_lengths.size()));
  EXPECT_OK(char_vb->Resize(list_values.size()));
  EXPECT_OK(int_vb->Resize(int_values.size()));

  int pos = 0;
  for (size_t i = 0; i < list_lengths.size(); ++i) {
    ASSERT_OK(list_vb->Append(list_is_valid[i] > 0));
    int_vb->UnsafeAppend(int_values[i]);
    for (int j = 0; j < list_lengths[i]; ++j) {
      char_vb->UnsafeAppend(list_values[pos++]);
    }
  }

  for (size_t i = 0; i < struct_is_valid.size(); ++i) {
    ASSERT_OK(builder_->Append(struct_is_valid[i] > 0));
  }

  Done();

  ValidateBasicStructArray(result_.get(), struct_is_valid, list_values, list_is_valid,
      list_lengths, list_offsets, int_values);
}

TEST_F(TestStructBuilder, BulkAppend) {
  vector<int32_t> int_values = {1, 2, 3, 4};
  vector<char> list_values = {'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'};
  vector<int> list_lengths = {3, 0, 3, 4};
  vector<int> list_offsets = {0, 3, 3, 6};
  vector<uint8_t> list_is_valid = {1, 0, 1, 1};
  vector<uint8_t> struct_is_valid = {1, 1, 1, 1};

  ListBuilder* list_vb = static_cast<ListBuilder*>(builder_->field_builder(0).get());
  Int8Builder* char_vb = static_cast<Int8Builder*>(list_vb->value_builder().get());
  Int32Builder* int_vb = static_cast<Int32Builder*>(builder_->field_builder(1).get());

  ASSERT_OK(builder_->Resize(list_lengths.size()));
  ASSERT_OK(char_vb->Resize(list_values.size()));
  ASSERT_OK(int_vb->Resize(int_values.size()));

  builder_->Append(struct_is_valid.size(), struct_is_valid.data());

  list_vb->Append(list_offsets.data(), list_offsets.size(), list_is_valid.data());
  for (int8_t value : list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : int_values) {
    int_vb->UnsafeAppend(value);
  }

  Done();
  ValidateBasicStructArray(result_.get(), struct_is_valid, list_values, list_is_valid,
      list_lengths, list_offsets, int_values);
}

TEST_F(TestStructBuilder, BulkAppendInvalid) {
  vector<int32_t> int_values = {1, 2, 3, 4};
  vector<char> list_values = {'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'};
  vector<int> list_lengths = {3, 0, 3, 4};
  vector<int> list_offsets = {0, 3, 3, 6};
  vector<uint8_t> list_is_valid = {1, 0, 1, 1};
  vector<uint8_t> struct_is_valid = {1, 0, 1, 1};  // should be 1, 1, 1, 1

  ListBuilder* list_vb = static_cast<ListBuilder*>(builder_->field_builder(0).get());
  Int8Builder* char_vb = static_cast<Int8Builder*>(list_vb->value_builder().get());
  Int32Builder* int_vb = static_cast<Int32Builder*>(builder_->field_builder(1).get());

  ASSERT_OK(builder_->Reserve(list_lengths.size()));
  ASSERT_OK(char_vb->Reserve(list_values.size()));
  ASSERT_OK(int_vb->Reserve(int_values.size()));

  builder_->Append(struct_is_valid.size(), struct_is_valid.data());

  list_vb->Append(list_offsets.data(), list_offsets.size(), list_is_valid.data());
  for (int8_t value : list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : int_values) {
    int_vb->UnsafeAppend(value);
  }

  Done();
  // Even null bitmap of the parent Struct is not valid, Validate() will ignore it.
  ASSERT_OK(result_->Validate());
}

TEST_F(TestStructBuilder, TestEquality) {
  std::shared_ptr<Array> array, equal_array;
  std::shared_ptr<Array> unequal_bitmap_array, unequal_offsets_array,
      unequal_values_array;

  vector<int32_t> int_values = {1, 2, 3, 4};
  vector<char> list_values = {'j', 'o', 'e', 'b', 'o', 'b', 'm', 'a', 'r', 'k'};
  vector<int> list_lengths = {3, 0, 3, 4};
  vector<int> list_offsets = {0, 3, 3, 6};
  vector<uint8_t> list_is_valid = {1, 0, 1, 1};
  vector<uint8_t> struct_is_valid = {1, 1, 1, 1};

  vector<int32_t> unequal_int_values = {4, 2, 3, 1};
  vector<char> unequal_list_values = {'j', 'o', 'e', 'b', 'o', 'b', 'l', 'u', 'c', 'y'};
  vector<int> unequal_list_offsets = {0, 3, 4, 6};
  vector<uint8_t> unequal_list_is_valid = {1, 1, 1, 1};
  vector<uint8_t> unequal_struct_is_valid = {1, 0, 0, 1};

  ListBuilder* list_vb = static_cast<ListBuilder*>(builder_->field_builder(0).get());
  Int8Builder* char_vb = static_cast<Int8Builder*>(list_vb->value_builder().get());
  Int32Builder* int_vb = static_cast<Int32Builder*>(builder_->field_builder(1).get());
  ASSERT_OK(builder_->Reserve(list_lengths.size()));
  ASSERT_OK(char_vb->Reserve(list_values.size()));
  ASSERT_OK(int_vb->Reserve(int_values.size()));

  // setup two equal arrays, one of which takes an unequal bitmap
  builder_->Append(struct_is_valid.size(), struct_is_valid.data());
  list_vb->Append(list_offsets.data(), list_offsets.size(), list_is_valid.data());
  for (int8_t value : list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : int_values) {
    int_vb->UnsafeAppend(value);
  }

  ASSERT_OK(builder_->Finish(&array));

  ASSERT_OK(builder_->Resize(list_lengths.size()));
  ASSERT_OK(char_vb->Resize(list_values.size()));
  ASSERT_OK(int_vb->Resize(int_values.size()));

  builder_->Append(struct_is_valid.size(), struct_is_valid.data());
  list_vb->Append(list_offsets.data(), list_offsets.size(), list_is_valid.data());
  for (int8_t value : list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : int_values) {
    int_vb->UnsafeAppend(value);
  }

  ASSERT_OK(builder_->Finish(&equal_array));

  ASSERT_OK(builder_->Resize(list_lengths.size()));
  ASSERT_OK(char_vb->Resize(list_values.size()));
  ASSERT_OK(int_vb->Resize(int_values.size()));

  // setup an unequal one with the unequal bitmap
  builder_->Append(unequal_struct_is_valid.size(), unequal_struct_is_valid.data());
  list_vb->Append(list_offsets.data(), list_offsets.size(), list_is_valid.data());
  for (int8_t value : list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : int_values) {
    int_vb->UnsafeAppend(value);
  }

  ASSERT_OK(builder_->Finish(&unequal_bitmap_array));

  ASSERT_OK(builder_->Resize(list_lengths.size()));
  ASSERT_OK(char_vb->Resize(list_values.size()));
  ASSERT_OK(int_vb->Resize(int_values.size()));

  // setup an unequal one with unequal offsets
  builder_->Append(struct_is_valid.size(), struct_is_valid.data());
  list_vb->Append(unequal_list_offsets.data(), unequal_list_offsets.size(),
      unequal_list_is_valid.data());
  for (int8_t value : list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : int_values) {
    int_vb->UnsafeAppend(value);
  }

  ASSERT_OK(builder_->Finish(&unequal_offsets_array));

  ASSERT_OK(builder_->Resize(list_lengths.size()));
  ASSERT_OK(char_vb->Resize(list_values.size()));
  ASSERT_OK(int_vb->Resize(int_values.size()));

  // setup anunequal one with unequal values
  builder_->Append(struct_is_valid.size(), struct_is_valid.data());
  list_vb->Append(list_offsets.data(), list_offsets.size(), list_is_valid.data());
  for (int8_t value : unequal_list_values) {
    char_vb->UnsafeAppend(value);
  }
  for (int32_t value : unequal_int_values) {
    int_vb->UnsafeAppend(value);
  }

  ASSERT_OK(builder_->Finish(&unequal_values_array));

  // Test array equality
  EXPECT_TRUE(array->Equals(array));
  EXPECT_TRUE(array->Equals(equal_array));
  EXPECT_TRUE(equal_array->Equals(array));
  EXPECT_FALSE(equal_array->Equals(unequal_bitmap_array));
  EXPECT_FALSE(unequal_bitmap_array->Equals(equal_array));
  EXPECT_FALSE(unequal_bitmap_array->Equals(unequal_values_array));
  EXPECT_FALSE(unequal_values_array->Equals(unequal_bitmap_array));
  EXPECT_FALSE(unequal_bitmap_array->Equals(unequal_offsets_array));
  EXPECT_FALSE(unequal_offsets_array->Equals(unequal_bitmap_array));

  // Test range equality
  EXPECT_TRUE(array->RangeEquals(0, 4, 0, equal_array));
  EXPECT_TRUE(array->RangeEquals(3, 4, 3, unequal_bitmap_array));
  EXPECT_TRUE(array->RangeEquals(0, 1, 0, unequal_offsets_array));
  EXPECT_FALSE(array->RangeEquals(0, 2, 0, unequal_offsets_array));
  EXPECT_FALSE(array->RangeEquals(1, 2, 1, unequal_offsets_array));
  EXPECT_FALSE(array->RangeEquals(0, 1, 0, unequal_values_array));
  EXPECT_TRUE(array->RangeEquals(1, 3, 1, unequal_values_array));
  EXPECT_FALSE(array->RangeEquals(3, 4, 3, unequal_values_array));

  // ARROW-33 Slice / equality
  std::shared_ptr<Array> slice, slice2;

  slice = array->Slice(2);
  slice2 = array->Slice(2);
  ASSERT_EQ(array->length() - 2, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(2, slice->length(), 0, slice));

  slice = array->Slice(1, 2);
  slice2 = array->Slice(1, 2);
  ASSERT_EQ(2, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(1, 3, 0, slice));
}

TEST_F(TestStructBuilder, TestZeroLength) {
  // All buffers are null
  Done();
  ASSERT_OK(result_->Validate());
}

// ----------------------------------------------------------------------
// Union tests

TEST(TestUnionArrayAdHoc, TestSliceEquals) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(ipc::MakeUnion(&batch));

  const int64_t size = batch->num_rows();

  auto CheckUnion = [&size](std::shared_ptr<Array> array) {
    std::shared_ptr<Array> slice, slice2;
    slice = array->Slice(2);
    slice2 = array->Slice(2);
    ASSERT_EQ(size - 2, slice->length());

    ASSERT_TRUE(slice->Equals(slice2));
    ASSERT_TRUE(array->RangeEquals(2, array->length(), 0, slice));

    // Chained slices
    slice2 = array->Slice(1)->Slice(1);
    ASSERT_TRUE(slice->Equals(slice2));

    slice = array->Slice(1, 5);
    slice2 = array->Slice(1, 5);
    ASSERT_EQ(5, slice->length());

    ASSERT_TRUE(slice->Equals(slice2));
    ASSERT_TRUE(array->RangeEquals(1, 6, 0, slice));
  };

  CheckUnion(batch->column(1));
  CheckUnion(batch->column(2));
}

}  // namespace arrow
