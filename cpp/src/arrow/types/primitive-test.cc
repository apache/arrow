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
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/builder.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/types/construct.h"
#include "arrow/types/primitive.h"
#include "arrow/types/test-common.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

using std::string;
using std::shared_ptr;
using std::unique_ptr;
using std::vector;

namespace arrow {

class Array;

#define PRIMITIVE_TEST(KLASS, ENUM, NAME) \
  TEST(TypesTest, TestPrimitive_##ENUM) { \
    KLASS tp;                             \
                                          \
    ASSERT_EQ(tp.type, Type::ENUM);       \
    ASSERT_EQ(tp.name(), string(NAME));   \
                                          \
    KLASS tp_copy = tp;                   \
    ASSERT_EQ(tp_copy.type, Type::ENUM);  \
  }

PRIMITIVE_TEST(Int8Type, INT8, "int8");
PRIMITIVE_TEST(Int16Type, INT16, "int16");
PRIMITIVE_TEST(Int32Type, INT32, "int32");
PRIMITIVE_TEST(Int64Type, INT64, "int64");
PRIMITIVE_TEST(UInt8Type, UINT8, "uint8");
PRIMITIVE_TEST(UInt16Type, UINT16, "uint16");
PRIMITIVE_TEST(UInt32Type, UINT32, "uint32");
PRIMITIVE_TEST(UInt64Type, UINT64, "uint64");

PRIMITIVE_TEST(FloatType, FLOAT, "float");
PRIMITIVE_TEST(DoubleType, DOUBLE, "double");

PRIMITIVE_TEST(BooleanType, BOOL, "bool");

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

  void RandomData(int N, double pct_null = 0.1) {
    Attrs::draw(N, &draws_);

    valid_bytes_.resize(N);
    test::random_null_bytes(N, pct_null, valid_bytes_.data());
  }

  void Check(const std::shared_ptr<BuilderType>& builder, bool nullable) {
    int size = builder->length();

    auto ex_data = std::make_shared<Buffer>(
        reinterpret_cast<uint8_t*>(draws_.data()), size * sizeof(T));

    std::shared_ptr<Buffer> ex_null_bitmap;
    int32_t ex_null_count = 0;

    if (nullable) {
      ex_null_bitmap = test::bytes_to_null_buffer(valid_bytes_);
      ex_null_count = test::null_count(valid_bytes_);
    } else {
      ex_null_bitmap = nullptr;
    }

    auto expected =
        std::make_shared<ArrayType>(size, ex_data, ex_null_count, ex_null_bitmap);
    std::shared_ptr<ArrayType> result =
        std::dynamic_pointer_cast<ArrayType>(builder->Finish());

    // Builder is now reset
    ASSERT_EQ(0, builder->length());
    ASSERT_EQ(0, builder->capacity());
    ASSERT_EQ(0, builder->null_count());
    ASSERT_EQ(nullptr, builder->data());

    ASSERT_EQ(ex_null_count, result->null_count());
    ASSERT_TRUE(result->EqualsExact(*expected.get()));
  }

 protected:
  std::shared_ptr<DataType> type_;
  shared_ptr<BuilderType> builder_;
  shared_ptr<BuilderType> builder_nn_;

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

#define PINT_DECL(CapType, c_type, LOWER, UPPER) \
  struct P##CapType {                            \
    PTYPE_DECL(CapType, c_type);                 \
    static void draw(int N, vector<T>* draws) {  \
      test::randint<T>(N, LOWER, UPPER, draws);  \
    }                                            \
  }

#define PFLOAT_DECL(CapType, c_type, LOWER, UPPER)     \
  struct P##CapType {                                  \
    PTYPE_DECL(CapType, c_type);                       \
    static void draw(int N, vector<T>* draws) {        \
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
void TestPrimitiveBuilder<PBoolean>::RandomData(int N, double pct_null) {
  draws_.resize(N);
  valid_bytes_.resize(N);

  test::random_null_bytes(N, 0.5, draws_.data());
  test::random_null_bytes(N, pct_null, valid_bytes_.data());
}

template <>
void TestPrimitiveBuilder<PBoolean>::Check(
    const std::shared_ptr<BooleanBuilder>& builder, bool nullable) {
  int size = builder->length();

  auto ex_data = test::bytes_to_null_buffer(draws_);

  std::shared_ptr<Buffer> ex_null_bitmap;
  int32_t ex_null_count = 0;

  if (nullable) {
    ex_null_bitmap = test::bytes_to_null_buffer(valid_bytes_);
    ex_null_count = test::null_count(valid_bytes_);
  } else {
    ex_null_bitmap = nullptr;
  }

  auto expected =
      std::make_shared<BooleanArray>(size, ex_data, ex_null_count, ex_null_bitmap);
  std::shared_ptr<BooleanArray> result =
      std::dynamic_pointer_cast<BooleanArray>(builder->Finish());

  // Builder is now reset
  ASSERT_EQ(0, builder->length());
  ASSERT_EQ(0, builder->capacity());
  ASSERT_EQ(0, builder->null_count());
  ASSERT_EQ(nullptr, builder->data());

  ASSERT_EQ(ex_null_count, result->null_count());

  ASSERT_EQ(expected->length(), result->length());

  for (int i = 0; i < result->length(); ++i) {
    if (nullable) { ASSERT_EQ(valid_bytes_[i] == 0, result->IsNull(i)) << i; }
    bool actual = util::get_bit(result->raw_data(), i);
    ASSERT_EQ(static_cast<bool>(draws_[i]), actual) << i;
  }
  ASSERT_TRUE(result->EqualsExact(*expected.get()));
}

typedef ::testing::Types<PBoolean, PUInt8, PUInt16, PUInt32, PUInt64, PInt8, PInt16,
    PInt32, PInt64, PFloat, PDouble> Primitives;

TYPED_TEST_CASE(TestPrimitiveBuilder, Primitives);

#define DECL_T() typedef typename TestFixture::T T;

#define DECL_TYPE() typedef typename TestFixture::Type Type;

#define DECL_ARRAYTYPE() typedef typename TestFixture::ArrayType ArrayType;

TYPED_TEST(TestPrimitiveBuilder, TestInit) {
  DECL_TYPE();

  int n = 1000;
  ASSERT_OK(this->builder_->Reserve(n));
  ASSERT_EQ(util::next_power2(n), this->builder_->capacity());
  ASSERT_EQ(util::next_power2(type_traits<Type>::bytes_required(n)),
      this->builder_->data()->size());

  // unsure if this should go in all builder classes
  ASSERT_EQ(0, this->builder_->num_children());
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendNull) {
  int size = 1000;
  for (int i = 0; i < size; ++i) {
    ASSERT_OK(this->builder_->AppendNull());
  }

  auto result = this->builder_->Finish();

  for (int i = 0; i < size; ++i) {
    ASSERT_TRUE(result->IsNull(i)) << i;
  }
}

TYPED_TEST(TestPrimitiveBuilder, TestArrayDtorDealloc) {
  DECL_T();

  int size = 1000;

  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;

  int64_t memory_before = this->pool_->bytes_allocated();

  this->RandomData(size);

  this->builder_->Reserve(size);

  int i;
  for (i = 0; i < size; ++i) {
    if (valid_bytes[i] > 0) {
      this->builder_->Append(draws[i]);
    } else {
      this->builder_->AppendNull();
    }
  }

  do {
    std::shared_ptr<Array> result = this->builder_->Finish();
  } while (false);

  ASSERT_EQ(memory_before, this->pool_->bytes_allocated());
}

template <class T, class Builder>
Status MakeArray(const vector<uint8_t>& valid_bytes, const vector<T>& draws, int size,
    Builder* builder, ArrayPtr* out) {
  // Append the first 1000
  for (int i = 0; i < size; ++i) {
    if (valid_bytes[i] > 0) {
      RETURN_NOT_OK(builder->Append(draws[i]));
    } else {
      RETURN_NOT_OK(builder->AppendNull());
    }
  }
  *out = builder->Finish();
  return Status::OK();
}

TYPED_TEST(TestPrimitiveBuilder, Equality) {
  DECL_T();

  const int size = 1000;
  this->RandomData(size);
  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;
  ArrayPtr array, equal_array, unequal_array;
  auto builder = this->builder_.get();
  ASSERT_OK(MakeArray(valid_bytes, draws, size, builder, &array));
  ASSERT_OK(MakeArray(valid_bytes, draws, size, builder, &equal_array));

  // Make the not equal array by negating the first valid element with itself.
  const auto first_valid = std::find_if(
      valid_bytes.begin(), valid_bytes.end(), [](uint8_t valid) { return valid > 0; });
  const int first_valid_idx = std::distance(valid_bytes.begin(), first_valid);
  // This should be true with a very high probability, but might introduce flakiness
  ASSERT_LT(first_valid_idx, size - 1);
  draws[first_valid_idx] = ~*reinterpret_cast<int64_t*>(&draws[first_valid_idx]);
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

TYPED_TEST(TestPrimitiveBuilder, TestAppendScalar) {
  DECL_T();

  const int size = 10000;

  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;

  this->RandomData(size);

  this->builder_->Reserve(1000);
  this->builder_nn_->Reserve(1000);

  int i;
  int null_count = 0;
  // Append the first 1000
  for (i = 0; i < 1000; ++i) {
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
  for (i = 1000; i < size; ++i) {
    if (valid_bytes[i] > 0) {
      this->builder_->Append(draws[i]);
    } else {
      this->builder_->AppendNull();
    }
    this->builder_nn_->Append(draws[i]);
  }

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(util::next_power2(size), this->builder_->capacity());

  ASSERT_EQ(size, this->builder_nn_->length());
  ASSERT_EQ(util::next_power2(size), this->builder_nn_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendVector) {
  DECL_T();

  int size = 10000;
  this->RandomData(size);

  vector<T>& draws = this->draws_;
  vector<uint8_t>& valid_bytes = this->valid_bytes_;

  // first slug
  int K = 1000;

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
  ASSERT_EQ(util::next_power2(size), this->builder_->capacity());

  this->Check(this->builder_, true);
  this->Check(this->builder_nn_, false);
}

TYPED_TEST(TestPrimitiveBuilder, TestAdvance) {
  int n = 1000;
  ASSERT_OK(this->builder_->Reserve(n));

  ASSERT_OK(this->builder_->Advance(100));
  ASSERT_EQ(100, this->builder_->length());

  ASSERT_OK(this->builder_->Advance(900));

  int too_many = this->builder_->capacity() - 1000 + 1;
  ASSERT_RAISES(Invalid, this->builder_->Advance(too_many));
}

TYPED_TEST(TestPrimitiveBuilder, TestResize) {
  DECL_TYPE();

  int cap = kMinBuilderCapacity * 2;

  ASSERT_OK(this->builder_->Reserve(cap));
  ASSERT_EQ(cap, this->builder_->capacity());

  ASSERT_EQ(type_traits<Type>::bytes_required(cap), this->builder_->data()->size());
  ASSERT_EQ(util::bytes_for_bits(cap), this->builder_->null_bitmap()->size());
}

TYPED_TEST(TestPrimitiveBuilder, TestReserve) {
  ASSERT_OK(this->builder_->Reserve(10));
  ASSERT_EQ(0, this->builder_->length());
  ASSERT_EQ(kMinBuilderCapacity, this->builder_->capacity());

  ASSERT_OK(this->builder_->Reserve(90));
  ASSERT_OK(this->builder_->Advance(100));
  ASSERT_OK(this->builder_->Reserve(kMinBuilderCapacity));

  ASSERT_EQ(util::next_power2(kMinBuilderCapacity + 100), this->builder_->capacity());
}

}  // namespace arrow
