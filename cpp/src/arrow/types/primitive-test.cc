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

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/types/boolean.h"
#include "arrow/types/construct.h"
#include "arrow/types/floating.h"
#include "arrow/types/integer.h"
#include "arrow/types/primitive.h"
#include "arrow/types/test-common.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace arrow {

TEST(TypesTest, TestBytesType) {
  BytesType t1(3);

  ASSERT_EQ(t1.type, LayoutEnum::BYTE);
  ASSERT_EQ(t1.size, 3);
}


#define PRIMITIVE_TEST(KLASS, ENUM, NAME)       \
  TEST(TypesTest, TestPrimitive_##ENUM) {       \
    KLASS tp;                                   \
    KLASS tp_nn(false);                         \
                                                \
    ASSERT_EQ(tp.type, TypeEnum::ENUM);         \
    ASSERT_EQ(tp.name(), string(NAME));         \
    ASSERT_TRUE(tp.nullable);                   \
    ASSERT_FALSE(tp_nn.nullable);               \
                                                \
    KLASS tp_copy = tp_nn;                      \
    ASSERT_FALSE(tp_copy.nullable);             \
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

TEST_F(TestBuilder, TestResize) {
  builder_->Init(10);
  ASSERT_EQ(2, builder_->nulls()->size());

  builder_->Resize(30);
  ASSERT_EQ(4, builder_->nulls()->size());
}

template <typename Attrs>
class TestPrimitiveBuilder : public TestBuilder {
 public:
  typedef typename Attrs::ArrayType ArrayType;
  typedef typename Attrs::BuilderType BuilderType;
  typedef typename Attrs::T T;

  void SetUp() {
    TestBuilder::SetUp();

    type_ = Attrs::type();
    type_nn_ = Attrs::type(false);

    ArrayBuilder* tmp;
    ASSERT_OK(make_builder(type_, &tmp));
    builder_.reset(static_cast<BuilderType*>(tmp));

    ASSERT_OK(make_builder(type_nn_, &tmp));
    builder_nn_.reset(static_cast<BuilderType*>(tmp));
  }

  void RandomData(int64_t N, double pct_null = 0.1) {
    Attrs::draw(N, &draws_);
    random_nulls(N, pct_null, &nulls_);
  }

  void CheckNullable() {
    ArrayType result;
    ArrayType expected;
    int64_t size = builder_->length();

    auto ex_data = std::make_shared<Buffer>(reinterpret_cast<uint8_t*>(draws_.data()),
        size * sizeof(T));

    auto ex_nulls = bytes_to_null_buffer(nulls_.data(), size);

    expected.Init(size, ex_data, ex_nulls);
    ASSERT_OK(builder_->Transfer(&result));

    // Builder is now reset
    ASSERT_EQ(0, builder_->length());
    ASSERT_EQ(0, builder_->capacity());
    ASSERT_EQ(nullptr, builder_->buffer());

    ASSERT_TRUE(result.Equals(expected));
  }

  void CheckNonNullable() {
    ArrayType result;
    ArrayType expected;
    int64_t size = builder_nn_->length();

    auto ex_data = std::make_shared<Buffer>(reinterpret_cast<uint8_t*>(draws_.data()),
        size * sizeof(T));

    expected.Init(size, ex_data);
    ASSERT_OK(builder_nn_->Transfer(&result));

    // Builder is now reset
    ASSERT_EQ(0, builder_nn_->length());
    ASSERT_EQ(0, builder_nn_->capacity());
    ASSERT_EQ(nullptr, builder_nn_->buffer());

    ASSERT_TRUE(result.Equals(expected));
  }

 protected:
  TypePtr type_;
  TypePtr type_nn_;
  unique_ptr<BuilderType> builder_;
  unique_ptr<BuilderType> builder_nn_;

  vector<T> draws_;
  vector<uint8_t> nulls_;
};

#define PTYPE_DECL(CapType, c_type)             \
  typedef CapType##Array ArrayType;             \
  typedef CapType##Builder BuilderType;         \
  typedef CapType##Type Type;                   \
  typedef c_type T;                             \
                                                \
  static TypePtr type(bool nullable = true) {   \
    return TypePtr(new Type(nullable));         \
  }

#define PINT_DECL(CapType, c_type, LOWER, UPPER)    \
  struct P##CapType {                               \
    PTYPE_DECL(CapType, c_type);                    \
    static void draw(int64_t N, vector<T>* draws) {  \
      randint<T>(N, LOWER, UPPER, draws);           \
    }                                               \
  }

PINT_DECL(UInt8, uint8_t, 0, UINT8_MAX);
PINT_DECL(UInt16, uint16_t, 0, UINT16_MAX);
PINT_DECL(UInt32, uint32_t, 0, UINT32_MAX);
PINT_DECL(UInt64, uint64_t, 0, UINT64_MAX);

PINT_DECL(Int8, int8_t, INT8_MIN, INT8_MAX);
PINT_DECL(Int16, int16_t, INT16_MIN, INT16_MAX);
PINT_DECL(Int32, int32_t, INT32_MIN, INT32_MAX);
PINT_DECL(Int64, int64_t, INT64_MIN, INT64_MAX);

typedef ::testing::Types<PUInt8, PUInt16, PUInt32, PUInt64,
                         PInt8, PInt16, PInt32, PInt64> Primitives;

TYPED_TEST_CASE(TestPrimitiveBuilder, Primitives);

#define DECL_T()                                \
  typedef typename TestFixture::T T;

#define DECL_ARRAYTYPE()                                \
  typedef typename TestFixture::ArrayType ArrayType;


TYPED_TEST(TestPrimitiveBuilder, TestInit) {
  DECL_T();

  int64_t n = 1000;
  ASSERT_OK(this->builder_->Init(n));
  ASSERT_EQ(n, this->builder_->capacity());
  ASSERT_EQ(n * sizeof(T), this->builder_->buffer()->size());

  // unsure if this should go in all builder classes
  ASSERT_EQ(0, this->builder_->num_children());
}

TYPED_TEST(TestPrimitiveBuilder, TestAppendNull) {
  int size = 10000;
  for (int i = 0; i < size; ++i) {
    ASSERT_OK(this->builder_->AppendNull());
  }

  Array* result;
  ASSERT_OK(this->builder_->ToArray(&result));
  unique_ptr<Array> holder(result);

  for (int i = 0; i < size; ++i) {
    ASSERT_TRUE(result->IsNull(i));
  }
}


TYPED_TEST(TestPrimitiveBuilder, TestAppendScalar) {
  DECL_T();

  int size = 10000;

  vector<T>& draws = this->draws_;
  vector<uint8_t>& nulls = this->nulls_;

  this->RandomData(size);

  int i;
  // Append the first 1000
  for (i = 0; i < 1000; ++i) {
    ASSERT_OK(this->builder_->Append(draws[i], nulls[i] > 0));
    ASSERT_OK(this->builder_nn_->Append(draws[i]));
  }

  ASSERT_EQ(1000, this->builder_->length());
  ASSERT_EQ(1024, this->builder_->capacity());

  ASSERT_EQ(1000, this->builder_nn_->length());
  ASSERT_EQ(1024, this->builder_nn_->capacity());

  // Append the next 9000
  for (i = 1000; i < size; ++i) {
    ASSERT_OK(this->builder_->Append(draws[i], nulls[i] > 0));
    ASSERT_OK(this->builder_nn_->Append(draws[i]));
  }

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(util::next_power2(size), this->builder_->capacity());

  ASSERT_EQ(size, this->builder_nn_->length());
  ASSERT_EQ(util::next_power2(size), this->builder_nn_->capacity());

  this->CheckNullable();
  this->CheckNonNullable();
}


TYPED_TEST(TestPrimitiveBuilder, TestAppendVector) {
  DECL_T();

  int size = 10000;
  this->RandomData(size);

  vector<T>& draws = this->draws_;
  vector<uint8_t>& nulls = this->nulls_;

  // first slug
  int K = 1000;

  ASSERT_OK(this->builder_->Append(draws.data(), K, nulls.data()));
  ASSERT_OK(this->builder_nn_->Append(draws.data(), K));

  ASSERT_EQ(1000, this->builder_->length());
  ASSERT_EQ(1024, this->builder_->capacity());

  ASSERT_EQ(1000, this->builder_nn_->length());
  ASSERT_EQ(1024, this->builder_nn_->capacity());

  // Append the next 9000
  ASSERT_OK(this->builder_->Append(draws.data() + K, size - K, nulls.data() + K));
  ASSERT_OK(this->builder_nn_->Append(draws.data() + K, size - K));

  ASSERT_EQ(size, this->builder_->length());
  ASSERT_EQ(util::next_power2(size), this->builder_->capacity());

  this->CheckNullable();
  this->CheckNonNullable();
}

TYPED_TEST(TestPrimitiveBuilder, TestAdvance) {
  int n = 1000;
  ASSERT_OK(this->builder_->Init(n));

  ASSERT_OK(this->builder_->Advance(100));
  ASSERT_EQ(100, this->builder_->length());

  ASSERT_OK(this->builder_->Advance(900));
  ASSERT_RAISES(Invalid, this->builder_->Advance(1));
}

TYPED_TEST(TestPrimitiveBuilder, TestResize) {
  DECL_T();

  int cap = MIN_BUILDER_CAPACITY * 2;

  ASSERT_OK(this->builder_->Resize(cap));
  ASSERT_EQ(cap, this->builder_->capacity());

  ASSERT_EQ(cap * sizeof(T), this->builder_->buffer()->size());
  ASSERT_EQ(util::ceil_byte(cap) / 8, this->builder_->nulls()->size());
}

TYPED_TEST(TestPrimitiveBuilder, TestReserve) {
  int n = 100;
  ASSERT_OK(this->builder_->Reserve(n));
  ASSERT_EQ(0, this->builder_->length());
  ASSERT_EQ(MIN_BUILDER_CAPACITY, this->builder_->capacity());

  ASSERT_OK(this->builder_->Advance(100));
  ASSERT_OK(this->builder_->Reserve(MIN_BUILDER_CAPACITY));

  ASSERT_EQ(util::next_power2(MIN_BUILDER_CAPACITY + 100),
      this->builder_->capacity());
}

} // namespace arrow
