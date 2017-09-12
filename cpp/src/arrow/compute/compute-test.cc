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
#include <sstream>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/compare.h"
#include "arrow/ipc/test-common.h"
#include "arrow/memory_pool.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/test-common.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"

#include "arrow/compute/cast.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"

using std::vector;

namespace arrow {
namespace compute {

void AssertArraysEqual(const Array& left, const Array& right) {
  bool are_equal = false;
  ASSERT_OK(ArrayEquals(left, right, &are_equal));

  if (!are_equal) {
    std::stringstream ss;

    ss << "Left: ";
    EXPECT_OK(PrettyPrint(left, 0, &ss));
    ss << "\nRight: ";
    EXPECT_OK(PrettyPrint(right, 0, &ss));
    FAIL() << ss.str();
  }
}

class ComputeFixture {
 public:
  ComputeFixture() : ctx_(default_memory_pool()) {}

 protected:
  FunctionContext ctx_;
};

// ----------------------------------------------------------------------
// Cast

static void AssertBufferSame(const Array& left, const Array& right, int buffer_index) {
  ASSERT_EQ(left.data()->buffers[buffer_index].get(),
            right.data()->buffers[buffer_index].get());
}

class TestCast : public ComputeFixture, public TestBase {
 public:
  void CheckPass(const Array& input, const Array& expected,
                 const std::shared_ptr<DataType>& out_type, const CastOptions& options) {
    std::shared_ptr<Array> result;
    ASSERT_OK(Cast(&ctx_, input, out_type, options, &result));
    AssertArraysEqual(expected, *result);
  }

  template <typename InType, typename I_TYPE>
  void CheckFails(const std::shared_ptr<DataType>& in_type,
                  const std::vector<I_TYPE>& in_values, const std::vector<bool>& is_valid,
                  const std::shared_ptr<DataType>& out_type, const CastOptions& options) {
    std::shared_ptr<Array> input, result;
    if (is_valid.size() > 0) {
      ArrayFromVector<InType, I_TYPE>(in_type, is_valid, in_values, &input);
    } else {
      ArrayFromVector<InType, I_TYPE>(in_type, in_values, &input);
    }
    ASSERT_RAISES(Invalid, Cast(&ctx_, *input, out_type, options, &result));
  }

  void CheckZeroCopy(const Array& input, const std::shared_ptr<DataType>& out_type) {
    std::shared_ptr<Array> result;
    ASSERT_OK(Cast(&ctx_, input, out_type, {}, &result));
    AssertBufferSame(input, *result, 0);
    AssertBufferSame(input, *result, 1);
  }

  template <typename InType, typename I_TYPE, typename OutType, typename O_TYPE>
  void CheckCase(const std::shared_ptr<DataType>& in_type,
                 const std::vector<I_TYPE>& in_values, const std::vector<bool>& is_valid,
                 const std::shared_ptr<DataType>& out_type,
                 const std::vector<O_TYPE>& out_values, const CastOptions& options) {
    std::shared_ptr<Array> input, expected;
    if (is_valid.size() > 0) {
      ArrayFromVector<InType, I_TYPE>(in_type, is_valid, in_values, &input);
      ArrayFromVector<OutType, O_TYPE>(out_type, is_valid, out_values, &expected);
    } else {
      ArrayFromVector<InType, I_TYPE>(in_type, in_values, &input);
      ArrayFromVector<OutType, O_TYPE>(out_type, out_values, &expected);
    }
    CheckPass(*input, *expected, out_type, options);
  }
};

TEST_F(TestCast, SameTypeZeroCopy) {
  vector<bool> is_valid = {true, false, true, true, true};
  vector<int32_t> v1 = {0, 1, 2, 3, 4};

  std::shared_ptr<Array> arr;
  ArrayFromVector<Int32Type, int32_t>(int32(), is_valid, v1, &arr);

  std::shared_ptr<Array> result;
  ASSERT_OK(Cast(&this->ctx_, *arr, int32(), {}, &result));

  AssertBufferSame(*arr, *result, 0);
  AssertBufferSame(*arr, *result, 1);
}

TEST_F(TestCast, ToBoolean) {
  CastOptions options;

  vector<bool> is_valid = {true, false, true, true, true};

  // int8, should suffice for other integers
  vector<int8_t> v1 = {0, 1, 127, -1, 0};
  vector<bool> e1 = {false, true, true, true, false};
  CheckCase<Int8Type, int8_t, BooleanType, bool>(int8(), v1, is_valid, boolean(), e1,
                                                 options);

  // floating point
  vector<double> v2 = {1.0, 0, 0, -1.0, 5.0};
  vector<bool> e2 = {true, false, false, true, true};
  CheckCase<DoubleType, double, BooleanType, bool>(float64(), v2, is_valid, boolean(), e2,
                                                   options);
}

TEST_F(TestCast, ToIntUpcast) {
  CastOptions options;
  options.allow_int_overflow = false;

  vector<bool> is_valid = {true, false, true, true, true};

  // int8 to int32
  vector<int8_t> v1 = {0, 1, 127, -1, 0};
  vector<int32_t> e1 = {0, 1, 127, -1, 0};
  CheckCase<Int8Type, int8_t, Int32Type, int32_t>(int8(), v1, is_valid, int32(), e1,
                                                  options);

  // bool to int8
  vector<bool> v2 = {false, true, false, true, true};
  vector<int8_t> e2 = {0, 1, 0, 1, 1};
  CheckCase<BooleanType, bool, Int8Type, int8_t>(boolean(), v2, is_valid, int8(), e2,
                                                 options);

  // uint8 to int16, no overflow/underrun
  vector<uint8_t> v3 = {0, 100, 200, 255, 0};
  vector<int16_t> e3 = {0, 100, 200, 255, 0};
  CheckCase<UInt8Type, uint8_t, Int16Type, int16_t>(uint8(), v3, is_valid, int16(), e3,
                                                    options);

  // floating point to integer
  vector<double> v4 = {1.5, 0, 0.5, -1.5, 5.5};
  vector<int32_t> e4 = {1, 0, 0, -1, 5};
  CheckCase<DoubleType, double, Int32Type, int32_t>(float64(), v4, is_valid, int32(), e4,
                                                    options);
}

TEST_F(TestCast, OverflowInNullSlot) {
  CastOptions options;
  options.allow_int_overflow = false;

  vector<bool> is_valid = {true, false, true, true, true};

  vector<int32_t> v11 = {0, 70000, 2000, 1000, 0};
  vector<int16_t> e11 = {0, 0, 2000, 1000, 0};

  std::shared_ptr<Array> expected;
  ArrayFromVector<Int16Type, int16_t>(int16(), is_valid, e11, &expected);

  auto buf = std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(v11.data()),
                                      static_cast<int64_t>(v11.size()));
  Int32Array tmp11(5, buf, expected->null_bitmap(), -1);

  CheckPass(tmp11, *expected, int16(), options);
}

TEST_F(TestCast, ToIntDowncastSafe) {
  CastOptions options;
  options.allow_int_overflow = false;

  vector<bool> is_valid = {true, false, true, true, true};

  // int16 to uint8, no overflow/underrun
  vector<int16_t> v5 = {0, 100, 200, 1, 2};
  vector<uint8_t> e5 = {0, 100, 200, 1, 2};
  CheckCase<Int16Type, int16_t, UInt8Type, uint8_t>(int16(), v5, is_valid, uint8(), e5,
                                                    options);

  // int16 to uint8, with overflow
  vector<int16_t> v6 = {0, 100, 256, 0, 0};
  CheckFails<Int16Type>(int16(), v6, is_valid, uint8(), options);

  // underflow
  vector<int16_t> v7 = {0, 100, -1, 0, 0};
  CheckFails<Int16Type>(int16(), v7, is_valid, uint8(), options);

  // int32 to int16, no overflow
  vector<int32_t> v8 = {0, 1000, 2000, 1, 2};
  vector<int16_t> e8 = {0, 1000, 2000, 1, 2};
  CheckCase<Int32Type, int32_t, Int16Type, int16_t>(int32(), v8, is_valid, int16(), e8,
                                                    options);

  // int32 to int16, overflow
  vector<int32_t> v9 = {0, 1000, 2000, 70000, 0};
  CheckFails<Int32Type>(int32(), v9, is_valid, int16(), options);

  // underflow
  vector<int32_t> v10 = {0, 1000, 2000, -70000, 0};
  CheckFails<Int32Type>(int32(), v9, is_valid, int16(), options);
}

TEST_F(TestCast, ToIntDowncastUnsafe) {
  CastOptions options;
  options.allow_int_overflow = true;

  vector<bool> is_valid = {true, false, true, true, true};

  // int16 to uint8, no overflow/underrun
  vector<int16_t> v5 = {0, 100, 200, 1, 2};
  vector<uint8_t> e5 = {0, 100, 200, 1, 2};
  CheckCase<Int16Type, int16_t, UInt8Type, uint8_t>(int16(), v5, is_valid, uint8(), e5,
                                                    options);

  // int16 to uint8, with overflow
  vector<int16_t> v6 = {0, 100, 256, 0, 0};
  vector<uint8_t> e6 = {0, 100, 0, 0, 0};
  CheckCase<Int16Type, int16_t, UInt8Type, uint8_t>(int16(), v6, is_valid, uint8(), e6,
                                                    options);

  // underflow
  vector<int16_t> v7 = {0, 100, -1, 0, 0};
  vector<uint8_t> e7 = {0, 100, 255, 0, 0};
  CheckCase<Int16Type, int16_t, UInt8Type, uint8_t>(int16(), v7, is_valid, uint8(), e7,
                                                    options);

  // int32 to int16, no overflow
  vector<int32_t> v8 = {0, 1000, 2000, 1, 2};
  vector<int16_t> e8 = {0, 1000, 2000, 1, 2};
  CheckCase<Int32Type, int32_t, Int16Type, int16_t>(int32(), v8, is_valid, int16(), e8,
                                                    options);

  // int32 to int16, overflow
  // TODO(wesm): do we want to allow this? we could set to null
  vector<int32_t> v9 = {0, 1000, 2000, 70000, 0};
  vector<int16_t> e9 = {0, 1000, 2000, 4464, 0};
  CheckCase<Int32Type, int32_t, Int16Type, int16_t>(int32(), v9, is_valid, int16(), e9,
                                                    options);

  // underflow
  // TODO(wesm): do we want to allow this? we could set overflow to null
  vector<int32_t> v10 = {0, 1000, 2000, -70000, 0};
  vector<int16_t> e10 = {0, 1000, 2000, -4464, 0};
  CheckCase<Int32Type, int32_t, Int16Type, int16_t>(int32(), v10, is_valid, int16(), e10,
                                                    options);
}

TEST_F(TestCast, ToDouble) {
  CastOptions options;
  vector<bool> is_valid = {true, false, true, true, true};

  // int16 to double
  vector<int16_t> v1 = {0, 100, 200, 1, 2};
  vector<double> e1 = {0, 100, 200, 1, 2};
  CheckCase<Int16Type, int16_t, DoubleType, double>(int16(), v1, is_valid, float64(), e1,
                                                    options);

  // float to double
  vector<float> v2 = {0, 100, 200, 1, 2};
  vector<double> e2 = {0, 100, 200, 1, 2};
  CheckCase<FloatType, float, DoubleType, double>(float32(), v2, is_valid, float64(), e2,
                                                  options);

  // bool to double
  vector<bool> v3 = {true, true, false, false, true};
  vector<double> e3 = {1, 1, 0, 0, 1};
  CheckCase<BooleanType, bool, DoubleType, double>(boolean(), v3, is_valid, float64(), e3,
                                                   options);
}

TEST_F(TestCast, UnsupportedTarget) {
  vector<bool> is_valid = {true, false, true, true, true};
  vector<int32_t> v1 = {0, 1, 2, 3, 4};

  std::shared_ptr<Array> arr;
  ArrayFromVector<Int32Type, int32_t>(int32(), is_valid, v1, &arr);

  std::shared_ptr<Array> result;
  ASSERT_RAISES(NotImplemented, Cast(&this->ctx_, *arr, utf8(), {}, &result));
}

TEST_F(TestCast, DateTimeZeroCopy) {
  vector<bool> is_valid = {true, false, true, true, true};

  std::shared_ptr<Array> arr;
  vector<int32_t> v1 = {0, 70000, 2000, 1000, 0};
  ArrayFromVector<Int32Type, int32_t>(int32(), is_valid, v1, &arr);

  CheckZeroCopy(*arr, time32(TimeUnit::SECOND));
  CheckZeroCopy(*arr, date32());

  vector<int64_t> v2 = {0, 70000, 2000, 1000, 0};
  ArrayFromVector<Int64Type, int64_t>(int64(), is_valid, v2, &arr);

  CheckZeroCopy(*arr, time64(TimeUnit::MICRO));
  CheckZeroCopy(*arr, date64());
  CheckZeroCopy(*arr, timestamp(TimeUnit::NANO));
}

TEST_F(TestCast, FromNull) {
  // Null casts to everything
  const int length = 10;

  NullArray arr(length);

  std::shared_ptr<Array> result;
  ASSERT_OK(Cast(&ctx_, arr, int32(), {}, &result));

  ASSERT_EQ(length, result->length());
  ASSERT_EQ(length, result->null_count());

  // OK to look at bitmaps
  AssertArraysEqual(*result, *result);
}

TEST_F(TestCast, PreallocatedMemory) {
  CastOptions options;
  options.allow_int_overflow = false;

  vector<bool> is_valid = {true, false, true, true, true};

  const int64_t length = 5;

  std::shared_ptr<Array> arr;
  vector<int32_t> v1 = {0, 70000, 2000, 1000, 0};
  vector<int64_t> e1 = {0, 70000, 2000, 1000, 0};
  ArrayFromVector<Int32Type, int32_t>(int32(), is_valid, v1, &arr);

  auto out_type = int64();

  std::unique_ptr<UnaryKernel> kernel;
  ASSERT_OK(GetCastFunction(*int32(), out_type, options, &kernel));

  auto out_data = std::make_shared<ArrayData>(out_type, length);

  std::shared_ptr<Buffer> out_values;
  ASSERT_OK(this->ctx_.Allocate(length * sizeof(int64_t), &out_values));

  out_data->buffers.push_back(nullptr);
  out_data->buffers.push_back(out_values);

  ASSERT_OK(kernel->Call(&this->ctx_, *arr, out_data.get()));

  // Buffer address unchanged
  ASSERT_EQ(out_values.get(), out_data->buffers[1].get());

  std::shared_ptr<Array> result, expected;
  ASSERT_OK(MakeArray(out_data, &result));
  ArrayFromVector<Int64Type, int64_t>(int64(), is_valid, e1, &expected);

  AssertArraysEqual(*expected, *result);
}

template <typename TestType>
class TestDictionaryCast : public TestCast {};

typedef ::testing::Types<UInt8Type, Int8Type, UInt16Type, Int16Type, Int32Type,
                         UInt32Type, UInt64Type, Int64Type, FloatType, DoubleType,
                         Date32Type, Date64Type, FixedSizeBinaryType, BinaryType>
    TestTypes;

TYPED_TEST_CASE(TestDictionaryCast, TestTypes);

TYPED_TEST(TestDictionaryCast, Basic) {
  CastOptions options;
  std::shared_ptr<Array> plain_array =
      TestBase::MakeRandomArray<typename TypeTraits<TypeParam>::ArrayType>(10, 2);

  std::shared_ptr<Array> dict_array;
  ASSERT_OK(EncodeArrayToDictionary(*plain_array, this->pool_, &dict_array));

  this->CheckPass(*dict_array, *plain_array, plain_array->type(), options);
}

}  // namespace compute
}  // namespace arrow
