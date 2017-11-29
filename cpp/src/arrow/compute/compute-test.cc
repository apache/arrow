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

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/cast.h"
#include "arrow/compute/kernels/hash.h"

using std::shared_ptr;
using std::vector;

namespace arrow {
namespace compute {

class ComputeFixture {
 public:
  ComputeFixture() : ctx_(default_memory_pool()) {}

 protected:
  FunctionContext ctx_;
};

template <typename Type, typename T>
shared_ptr<Array> _MakeArray(const shared_ptr<DataType>& type, const vector<T>& values,
                             const vector<bool>& is_valid) {
  shared_ptr<Array> result;
  if (is_valid.size() > 0) {
    ArrayFromVector<Type, T>(type, is_valid, values, &result);
  } else {
    ArrayFromVector<Type, T>(type, values, &result);
  }
  return result;
}

// ----------------------------------------------------------------------
// Cast

static void AssertBufferSame(const Array& left, const Array& right, int buffer_index) {
  ASSERT_EQ(left.data()->buffers[buffer_index].get(),
            right.data()->buffers[buffer_index].get());
}

class TestCast : public ComputeFixture, public TestBase {
 public:
  void CheckPass(const Array& input, const Array& expected,
                 const shared_ptr<DataType>& out_type, const CastOptions& options) {
    shared_ptr<Array> result;
    ASSERT_OK(Cast(&ctx_, input, out_type, options, &result));
    ASSERT_ARRAYS_EQUAL(expected, *result);
  }

  template <typename InType, typename I_TYPE>
  void CheckFails(const shared_ptr<DataType>& in_type, const vector<I_TYPE>& in_values,
                  const vector<bool>& is_valid, const shared_ptr<DataType>& out_type,
                  const CastOptions& options) {
    shared_ptr<Array> input, result;
    if (is_valid.size() > 0) {
      ArrayFromVector<InType, I_TYPE>(in_type, is_valid, in_values, &input);
    } else {
      ArrayFromVector<InType, I_TYPE>(in_type, in_values, &input);
    }
    ASSERT_RAISES(Invalid, Cast(&ctx_, *input, out_type, options, &result));
  }

  void CheckZeroCopy(const Array& input, const shared_ptr<DataType>& out_type) {
    shared_ptr<Array> result;
    ASSERT_OK(Cast(&ctx_, input, out_type, {}, &result));
    AssertBufferSame(input, *result, 0);
    AssertBufferSame(input, *result, 1);
  }

  template <typename InType, typename I_TYPE, typename OutType, typename O_TYPE>
  void CheckCase(const shared_ptr<DataType>& in_type, const vector<I_TYPE>& in_values,
                 const vector<bool>& is_valid, const shared_ptr<DataType>& out_type,
                 const vector<O_TYPE>& out_values, const CastOptions& options) {
    shared_ptr<Array> input, expected;
    if (is_valid.size() > 0) {
      ArrayFromVector<InType, I_TYPE>(in_type, is_valid, in_values, &input);
      ArrayFromVector<OutType, O_TYPE>(out_type, is_valid, out_values, &expected);
    } else {
      ArrayFromVector<InType, I_TYPE>(in_type, in_values, &input);
      ArrayFromVector<OutType, O_TYPE>(out_type, out_values, &expected);
    }
    CheckPass(*input, *expected, out_type, options);

    // Check a sliced variant
    if (input->length() > 1) {
      CheckPass(*input->Slice(1), *expected->Slice(1), out_type, options);
    }
  }
};

TEST_F(TestCast, SameTypeZeroCopy) {
  vector<bool> is_valid = {true, false, true, true, true};
  vector<int32_t> v1 = {0, 1, 2, 3, 4};

  shared_ptr<Array> arr;
  ArrayFromVector<Int32Type, int32_t>(int32(), is_valid, v1, &arr);

  shared_ptr<Array> result;
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

  shared_ptr<Array> expected;
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

TEST_F(TestCast, TimestampToTimestamp) {
  CastOptions options;

  auto CheckTimestampCast = [this](
      const CastOptions& options, TimeUnit::type from_unit, TimeUnit::type to_unit,
      const vector<int64_t>& from_values, const vector<int64_t>& to_values,
      const vector<bool>& is_valid) {
    CheckCase<TimestampType, int64_t, TimestampType, int64_t>(
        timestamp(from_unit), from_values, is_valid, timestamp(to_unit), to_values,
        options);
  };

  vector<bool> is_valid = {true, false, true, true, true};

  // Multiply promotions
  vector<int64_t> v1 = {0, 100, 200, 1, 2};
  vector<int64_t> e1 = {0, 100000, 200000, 1000, 2000};
  CheckTimestampCast(options, TimeUnit::SECOND, TimeUnit::MILLI, v1, e1, is_valid);

  vector<int64_t> v2 = {0, 100, 200, 1, 2};
  vector<int64_t> e2 = {0, 100000000L, 200000000L, 1000000, 2000000};
  CheckTimestampCast(options, TimeUnit::SECOND, TimeUnit::MICRO, v2, e2, is_valid);

  vector<int64_t> v3 = {0, 100, 200, 1, 2};
  vector<int64_t> e3 = {0, 100000000000L, 200000000000L, 1000000000L, 2000000000L};
  CheckTimestampCast(options, TimeUnit::SECOND, TimeUnit::NANO, v3, e3, is_valid);

  vector<int64_t> v4 = {0, 100, 200, 1, 2};
  vector<int64_t> e4 = {0, 100000, 200000, 1000, 2000};
  CheckTimestampCast(options, TimeUnit::MILLI, TimeUnit::MICRO, v4, e4, is_valid);

  vector<int64_t> v5 = {0, 100, 200, 1, 2};
  vector<int64_t> e5 = {0, 100000000L, 200000000L, 1000000, 2000000};
  CheckTimestampCast(options, TimeUnit::MILLI, TimeUnit::NANO, v5, e5, is_valid);

  vector<int64_t> v6 = {0, 100, 200, 1, 2};
  vector<int64_t> e6 = {0, 100000, 200000, 1000, 2000};
  CheckTimestampCast(options, TimeUnit::MICRO, TimeUnit::NANO, v6, e6, is_valid);

  // Zero copy
  vector<int64_t> v7 = {0, 70000, 2000, 1000, 0};
  shared_ptr<Array> arr;
  ArrayFromVector<TimestampType, int64_t>(timestamp(TimeUnit::SECOND), is_valid, v7,
                                          &arr);
  CheckZeroCopy(*arr, timestamp(TimeUnit::SECOND));

  // ARROW-1773, cast to integer
  CheckZeroCopy(*arr, int64());

  // Divide, truncate
  vector<int64_t> v8 = {0, 100123, 200456, 1123, 2456};
  vector<int64_t> e8 = {0, 100, 200, 1, 2};

  options.allow_time_truncate = true;
  CheckTimestampCast(options, TimeUnit::MILLI, TimeUnit::SECOND, v8, e8, is_valid);
  CheckTimestampCast(options, TimeUnit::MICRO, TimeUnit::MILLI, v8, e8, is_valid);
  CheckTimestampCast(options, TimeUnit::NANO, TimeUnit::MICRO, v8, e8, is_valid);

  vector<int64_t> v9 = {0, 100123000, 200456000, 1123000, 2456000};
  vector<int64_t> e9 = {0, 100, 200, 1, 2};
  CheckTimestampCast(options, TimeUnit::MICRO, TimeUnit::SECOND, v9, e9, is_valid);
  CheckTimestampCast(options, TimeUnit::NANO, TimeUnit::MILLI, v9, e9, is_valid);

  vector<int64_t> v10 = {0, 100123000000L, 200456000000L, 1123000000L, 2456000000};
  vector<int64_t> e10 = {0, 100, 200, 1, 2};
  CheckTimestampCast(options, TimeUnit::NANO, TimeUnit::SECOND, v10, e10, is_valid);

  // Disallow truncate, failures
  options.allow_time_truncate = false;
  CheckFails<TimestampType>(timestamp(TimeUnit::MILLI), v8, is_valid,
                            timestamp(TimeUnit::SECOND), options);
  CheckFails<TimestampType>(timestamp(TimeUnit::MICRO), v8, is_valid,
                            timestamp(TimeUnit::MILLI), options);
  CheckFails<TimestampType>(timestamp(TimeUnit::NANO), v8, is_valid,
                            timestamp(TimeUnit::MICRO), options);
  CheckFails<TimestampType>(timestamp(TimeUnit::MICRO), v9, is_valid,
                            timestamp(TimeUnit::SECOND), options);
  CheckFails<TimestampType>(timestamp(TimeUnit::NANO), v9, is_valid,
                            timestamp(TimeUnit::MILLI), options);
  CheckFails<TimestampType>(timestamp(TimeUnit::NANO), v10, is_valid,
                            timestamp(TimeUnit::SECOND), options);
}

TEST_F(TestCast, TimestampToDate32_Date64) {
  CastOptions options;

  vector<bool> is_valid = {true, true, false};

  // 2000-01-01, 2000-01-02, null
  vector<int64_t> v_nano = {946684800000000000, 946771200000000000, 0};
  vector<int64_t> v_micro = {946684800000000, 946771200000000, 0};
  vector<int64_t> v_milli = {946684800000, 946771200000, 0};
  vector<int64_t> v_second = {946684800, 946771200, 0};
  vector<int32_t> v_day = {10957, 10958, 0};

  // Simple conversions
  CheckCase<TimestampType, int64_t, Date64Type, int64_t>(
      timestamp(TimeUnit::NANO), v_nano, is_valid, date64(), v_milli, options);
  CheckCase<TimestampType, int64_t, Date64Type, int64_t>(
      timestamp(TimeUnit::MICRO), v_micro, is_valid, date64(), v_milli, options);
  CheckCase<TimestampType, int64_t, Date64Type, int64_t>(
      timestamp(TimeUnit::MILLI), v_milli, is_valid, date64(), v_milli, options);
  CheckCase<TimestampType, int64_t, Date64Type, int64_t>(
      timestamp(TimeUnit::SECOND), v_second, is_valid, date64(), v_milli, options);

  CheckCase<TimestampType, int64_t, Date32Type, int32_t>(
      timestamp(TimeUnit::NANO), v_nano, is_valid, date32(), v_day, options);
  CheckCase<TimestampType, int64_t, Date32Type, int32_t>(
      timestamp(TimeUnit::MICRO), v_micro, is_valid, date32(), v_day, options);
  CheckCase<TimestampType, int64_t, Date32Type, int32_t>(
      timestamp(TimeUnit::MILLI), v_milli, is_valid, date32(), v_day, options);
  CheckCase<TimestampType, int64_t, Date32Type, int32_t>(
      timestamp(TimeUnit::SECOND), v_second, is_valid, date32(), v_day, options);

  // Disallow truncate, failures
  vector<int64_t> v_nano_fail = {946684800000000001, 946771200000000001, 0};
  vector<int64_t> v_micro_fail = {946684800000001, 946771200000001, 0};
  vector<int64_t> v_milli_fail = {946684800001, 946771200001, 0};
  vector<int64_t> v_second_fail = {946684801, 946771201, 0};

  options.allow_time_truncate = false;
  CheckFails<TimestampType>(timestamp(TimeUnit::NANO), v_nano_fail, is_valid, date64(),
                            options);
  CheckFails<TimestampType>(timestamp(TimeUnit::MICRO), v_micro_fail, is_valid, date64(),
                            options);
  CheckFails<TimestampType>(timestamp(TimeUnit::MILLI), v_milli_fail, is_valid, date64(),
                            options);
  CheckFails<TimestampType>(timestamp(TimeUnit::SECOND), v_second_fail, is_valid,
                            date64(), options);

  CheckFails<TimestampType>(timestamp(TimeUnit::NANO), v_nano_fail, is_valid, date32(),
                            options);
  CheckFails<TimestampType>(timestamp(TimeUnit::MICRO), v_micro_fail, is_valid, date32(),
                            options);
  CheckFails<TimestampType>(timestamp(TimeUnit::MILLI), v_milli_fail, is_valid, date32(),
                            options);
  CheckFails<TimestampType>(timestamp(TimeUnit::SECOND), v_second_fail, is_valid,
                            date32(), options);

  // Make sure that nulls are excluded from the truncation checks
  vector<int64_t> v_second_nofail = {946684800, 946771200, 1};
  CheckCase<TimestampType, int64_t, Date64Type, int64_t>(
      timestamp(TimeUnit::SECOND), v_second_nofail, is_valid, date64(), v_milli, options);
  CheckCase<TimestampType, int64_t, Date32Type, int32_t>(
      timestamp(TimeUnit::SECOND), v_second_nofail, is_valid, date32(), v_day, options);
}

TEST_F(TestCast, TimeToCompatible) {
  CastOptions options;

  vector<bool> is_valid = {true, false, true, true, true};

  // Multiply promotions
  vector<int32_t> v1 = {0, 100, 200, 1, 2};
  vector<int32_t> e1 = {0, 100000, 200000, 1000, 2000};
  CheckCase<Time32Type, int32_t, Time32Type, int32_t>(
      time32(TimeUnit::SECOND), v1, is_valid, time32(TimeUnit::MILLI), e1, options);

  vector<int32_t> v2 = {0, 100, 200, 1, 2};
  vector<int64_t> e2 = {0, 100000000L, 200000000L, 1000000, 2000000};
  CheckCase<Time32Type, int32_t, Time64Type, int64_t>(
      time32(TimeUnit::SECOND), v2, is_valid, time64(TimeUnit::MICRO), e2, options);

  vector<int32_t> v3 = {0, 100, 200, 1, 2};
  vector<int64_t> e3 = {0, 100000000000L, 200000000000L, 1000000000L, 2000000000L};
  CheckCase<Time32Type, int32_t, Time64Type, int64_t>(
      time32(TimeUnit::SECOND), v3, is_valid, time64(TimeUnit::NANO), e3, options);

  vector<int32_t> v4 = {0, 100, 200, 1, 2};
  vector<int64_t> e4 = {0, 100000, 200000, 1000, 2000};
  CheckCase<Time32Type, int32_t, Time64Type, int64_t>(
      time32(TimeUnit::MILLI), v4, is_valid, time64(TimeUnit::MICRO), e4, options);

  vector<int32_t> v5 = {0, 100, 200, 1, 2};
  vector<int64_t> e5 = {0, 100000000L, 200000000L, 1000000, 2000000};
  CheckCase<Time32Type, int32_t, Time64Type, int64_t>(
      time32(TimeUnit::MILLI), v5, is_valid, time64(TimeUnit::NANO), e5, options);

  vector<int64_t> v6 = {0, 100, 200, 1, 2};
  vector<int64_t> e6 = {0, 100000, 200000, 1000, 2000};
  CheckCase<Time64Type, int64_t, Time64Type, int64_t>(
      time64(TimeUnit::MICRO), v6, is_valid, time64(TimeUnit::NANO), e6, options);

  // Zero copy
  vector<int64_t> v7 = {0, 70000, 2000, 1000, 0};
  shared_ptr<Array> arr;
  ArrayFromVector<Time64Type, int64_t>(time64(TimeUnit::MICRO), is_valid, v7, &arr);
  CheckZeroCopy(*arr, time64(TimeUnit::MICRO));

  // ARROW-1773: cast to int64
  CheckZeroCopy(*arr, int64());

  vector<int32_t> v7_2 = {0, 70000, 2000, 1000, 0};
  ArrayFromVector<Time32Type, int32_t>(time32(TimeUnit::SECOND), is_valid, v7_2, &arr);
  CheckZeroCopy(*arr, time32(TimeUnit::SECOND));

  // ARROW-1773: cast to int64
  CheckZeroCopy(*arr, int32());

  // Divide, truncate
  vector<int32_t> v8 = {0, 100123, 200456, 1123, 2456};
  vector<int32_t> e8 = {0, 100, 200, 1, 2};

  options.allow_time_truncate = true;
  CheckCase<Time32Type, int32_t, Time32Type, int32_t>(
      time32(TimeUnit::MILLI), v8, is_valid, time32(TimeUnit::SECOND), e8, options);
  CheckCase<Time64Type, int32_t, Time32Type, int32_t>(
      time64(TimeUnit::MICRO), v8, is_valid, time32(TimeUnit::MILLI), e8, options);
  CheckCase<Time64Type, int32_t, Time64Type, int32_t>(
      time64(TimeUnit::NANO), v8, is_valid, time64(TimeUnit::MICRO), e8, options);

  vector<int64_t> v9 = {0, 100123000, 200456000, 1123000, 2456000};
  vector<int32_t> e9 = {0, 100, 200, 1, 2};
  CheckCase<Time64Type, int64_t, Time32Type, int32_t>(
      time64(TimeUnit::MICRO), v9, is_valid, time32(TimeUnit::SECOND), e9, options);
  CheckCase<Time64Type, int64_t, Time32Type, int32_t>(
      time64(TimeUnit::NANO), v9, is_valid, time32(TimeUnit::MILLI), e9, options);

  vector<int64_t> v10 = {0, 100123000000L, 200456000000L, 1123000000L, 2456000000};
  vector<int32_t> e10 = {0, 100, 200, 1, 2};
  CheckCase<Time64Type, int64_t, Time32Type, int32_t>(
      time64(TimeUnit::NANO), v10, is_valid, time32(TimeUnit::SECOND), e10, options);

  // Disallow truncate, failures

  options.allow_time_truncate = false;
  CheckFails<Time32Type>(time32(TimeUnit::MILLI), v8, is_valid, time32(TimeUnit::SECOND),
                         options);
  CheckFails<Time64Type>(time64(TimeUnit::MICRO), v8, is_valid, time32(TimeUnit::MILLI),
                         options);
  CheckFails<Time64Type>(time64(TimeUnit::NANO), v8, is_valid, time64(TimeUnit::MICRO),
                         options);
  CheckFails<Time64Type>(time64(TimeUnit::MICRO), v9, is_valid, time32(TimeUnit::SECOND),
                         options);
  CheckFails<Time64Type>(time64(TimeUnit::NANO), v9, is_valid, time32(TimeUnit::MILLI),
                         options);
  CheckFails<Time64Type>(time64(TimeUnit::NANO), v10, is_valid, time32(TimeUnit::SECOND),
                         options);
}

TEST_F(TestCast, DateToCompatible) {
  CastOptions options;

  vector<bool> is_valid = {true, false, true, true, true};

  constexpr int64_t F = 86400000;

  // Multiply promotion
  vector<int32_t> v1 = {0, 100, 200, 1, 2};
  vector<int64_t> e1 = {0, 100 * F, 200 * F, F, 2 * F};
  CheckCase<Date32Type, int32_t, Date64Type, int64_t>(date32(), v1, is_valid, date64(),
                                                      e1, options);

  // Zero copy
  vector<int32_t> v2 = {0, 70000, 2000, 1000, 0};
  vector<int64_t> v3 = {0, 70000, 2000, 1000, 0};
  shared_ptr<Array> arr;
  ArrayFromVector<Date32Type, int32_t>(date32(), is_valid, v2, &arr);
  CheckZeroCopy(*arr, date32());

  // ARROW-1773: zero copy cast to integer
  CheckZeroCopy(*arr, int32());

  ArrayFromVector<Date64Type, int64_t>(date64(), is_valid, v3, &arr);
  CheckZeroCopy(*arr, date64());

  // ARROW-1773: zero copy cast to integer
  CheckZeroCopy(*arr, int64());

  // Divide, truncate
  vector<int64_t> v8 = {0, 100 * F + 123, 200 * F + 456, F + 123, 2 * F + 456};
  vector<int32_t> e8 = {0, 100, 200, 1, 2};

  options.allow_time_truncate = true;
  CheckCase<Date64Type, int64_t, Date32Type, int32_t>(date64(), v8, is_valid, date32(),
                                                      e8, options);

  // Disallow truncate, failures
  options.allow_time_truncate = false;
  CheckFails<Date64Type>(date64(), v8, is_valid, date32(), options);
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

TEST_F(TestCast, ChunkedArray) {
  vector<int16_t> values1 = {0, 1, 2};
  vector<int16_t> values2 = {3, 4, 5};

  auto type = int16();
  auto out_type = int64();

  auto a1 = _MakeArray<Int16Type, int16_t>(type, values1, {});
  auto a2 = _MakeArray<Int16Type, int16_t>(type, values2, {});

  ArrayVector arrays = {a1, a2};
  auto carr = std::make_shared<ChunkedArray>(arrays);

  CastOptions options;

  Datum out;
  ASSERT_OK(Cast(&this->ctx_, Datum(carr), out_type, options, &out));
  ASSERT_EQ(Datum::CHUNKED_ARRAY, out.kind());

  auto out_carr = out.chunked_array();

  vector<int64_t> ex_values1 = {0, 1, 2};
  vector<int64_t> ex_values2 = {3, 4, 5};
  auto a3 = _MakeArray<Int64Type, int64_t>(out_type, ex_values1, {});
  auto a4 = _MakeArray<Int64Type, int64_t>(out_type, ex_values2, {});

  ArrayVector ex_arrays = {a3, a4};
  auto ex_carr = std::make_shared<ChunkedArray>(ex_arrays);

  ASSERT_TRUE(out.chunked_array()->Equals(*ex_carr));
}

TEST_F(TestCast, UnsupportedTarget) {
  vector<bool> is_valid = {true, false, true, true, true};
  vector<int32_t> v1 = {0, 1, 2, 3, 4};

  shared_ptr<Array> arr;
  ArrayFromVector<Int32Type, int32_t>(int32(), is_valid, v1, &arr);

  shared_ptr<Array> result;
  ASSERT_RAISES(NotImplemented, Cast(&this->ctx_, *arr, utf8(), {}, &result));
}

TEST_F(TestCast, DateTimeZeroCopy) {
  vector<bool> is_valid = {true, false, true, true, true};

  vector<int32_t> v1 = {0, 70000, 2000, 1000, 0};
  shared_ptr<Array> arr;
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

  shared_ptr<Array> result;
  ASSERT_OK(Cast(&ctx_, arr, int32(), {}, &result));

  ASSERT_EQ(length, result->length());
  ASSERT_EQ(length, result->null_count());

  // OK to look at bitmaps
  ASSERT_ARRAYS_EQUAL(*result, *result);
}

TEST_F(TestCast, PreallocatedMemory) {
  CastOptions options;
  options.allow_int_overflow = false;

  vector<bool> is_valid = {true, false, true, true, true};

  const int64_t length = 5;

  shared_ptr<Array> arr;
  vector<int32_t> v1 = {0, 70000, 2000, 1000, 0};
  vector<int64_t> e1 = {0, 70000, 2000, 1000, 0};
  ArrayFromVector<Int32Type, int32_t>(int32(), is_valid, v1, &arr);

  auto out_type = int64();

  std::unique_ptr<UnaryKernel> kernel;
  ASSERT_OK(GetCastFunction(*int32(), out_type, options, &kernel));

  auto out_data = ArrayData::Make(out_type, length);

  shared_ptr<Buffer> out_values;
  ASSERT_OK(this->ctx_.Allocate(length * sizeof(int64_t), &out_values));

  out_data->buffers.push_back(nullptr);
  out_data->buffers.push_back(out_values);

  Datum out(out_data);
  ASSERT_OK(kernel->Call(&this->ctx_, Datum(arr), &out));

  // Buffer address unchanged
  ASSERT_EQ(out_values.get(), out_data->buffers[1].get());

  shared_ptr<Array> result = MakeArray(out_data);
  shared_ptr<Array> expected;
  ArrayFromVector<Int64Type, int64_t>(int64(), is_valid, e1, &expected);

  ASSERT_ARRAYS_EQUAL(*expected, *result);
}

template <typename InType, typename InT, typename OutType, typename OutT>
void CheckOffsetOutputCase(FunctionContext* ctx, const std::shared_ptr<DataType>& in_type,
                           const vector<InT>& in_values,
                           const std::shared_ptr<DataType>& out_type,
                           const vector<OutT>& out_values) {
  using OutTraits = TypeTraits<OutType>;

  CastOptions options;

  const int64_t length = static_cast<int64_t>(in_values.size());

  shared_ptr<Array> arr, expected;
  ArrayFromVector<InType, InT>(in_type, in_values, &arr);
  ArrayFromVector<OutType, OutT>(out_type, out_values, &expected);

  shared_ptr<Buffer> out_buffer;
  ASSERT_OK(ctx->Allocate(OutTraits::bytes_required(length), &out_buffer));

  std::unique_ptr<UnaryKernel> kernel;
  ASSERT_OK(GetCastFunction(*in_type, out_type, options, &kernel));

  const int64_t first_half = length / 2;

  auto out_data = ArrayData::Make(out_type, length, {nullptr, out_buffer});
  auto out_second_data = out_data->Copy();
  out_second_data->offset = first_half;

  Datum out_first(out_data);
  Datum out_second(out_second_data);

  // Cast each bit
  ASSERT_OK(kernel->Call(ctx, Datum(arr->Slice(0, first_half)), &out_first));
  ASSERT_OK(kernel->Call(ctx, Datum(arr->Slice(first_half)), &out_second));

  shared_ptr<Array> result = MakeArray(out_data);

  ASSERT_ARRAYS_EQUAL(*expected, *result);
}

TEST_F(TestCast, OffsetOutputBuffer) {
  // ARROW-1735
  vector<int32_t> v1 = {0, 10000, 2000, 1000, 0};
  vector<int64_t> e1 = {0, 10000, 2000, 1000, 0};

  auto in_type = int32();
  auto out_type = int64();
  CheckOffsetOutputCase<Int32Type, int32_t, Int64Type, int64_t>(&this->ctx_, in_type, v1,
                                                                out_type, e1);

  vector<bool> e2 = {false, true, true, true, false};

  out_type = boolean();
  CheckOffsetOutputCase<Int32Type, int32_t, BooleanType, bool>(&this->ctx_, in_type, v1,
                                                               boolean(), e2);

  vector<int16_t> e3 = {0, 10000, 2000, 1000, 0};
  CheckOffsetOutputCase<Int32Type, int32_t, Int16Type, int16_t>(&this->ctx_, in_type, v1,
                                                                int16(), e3);
}

template <typename TestType>
class TestDictionaryCast : public TestCast {};

typedef ::testing::Types<NullType, UInt8Type, Int8Type, UInt16Type, Int16Type, Int32Type,
                         UInt32Type, UInt64Type, Int64Type, FloatType, DoubleType,
                         Date32Type, Date64Type, FixedSizeBinaryType, BinaryType>
    TestTypes;

TYPED_TEST_CASE(TestDictionaryCast, TestTypes);

TYPED_TEST(TestDictionaryCast, Basic) {
  CastOptions options;
  shared_ptr<Array> plain_array =
      TestBase::MakeRandomArray<typename TypeTraits<TypeParam>::ArrayType>(10, 2);

  Datum out;
  ASSERT_OK(DictionaryEncode(&this->ctx_, Datum(plain_array->data()), &out));

  this->CheckPass(*MakeArray(out.array()), *plain_array, plain_array->type(), options);
}

/*TYPED_TEST(TestDictionaryCast, Reverse) {
  CastOptions options;
  shared_ptr<Array> plain_array =
      TestBase::MakeRandomArray<typename TypeTraits<TypeParam>::ArrayType>(10, 2);

  shared_ptr<Array> dict_array;
  ASSERT_OK(EncodeArrayToDictionary(*plain_array, this->pool_, &dict_array));

  this->CheckPass(*plain_array, *dict_array, dict_array->type(), options);
}*/

// ----------------------------------------------------------------------
// Dictionary tests

template <typename Type, typename T>
void CheckUnique(FunctionContext* ctx, const shared_ptr<DataType>& type,
                 const vector<T>& in_values, const vector<bool>& in_is_valid,
                 const vector<T>& out_values, const vector<bool>& out_is_valid) {
  shared_ptr<Array> input = _MakeArray<Type, T>(type, in_values, in_is_valid);
  shared_ptr<Array> expected = _MakeArray<Type, T>(type, out_values, out_is_valid);

  shared_ptr<Array> result;
  ASSERT_OK(Unique(ctx, Datum(input), &result));
  ASSERT_ARRAYS_EQUAL(*expected, *result);
}

template <typename Type, typename T>
void CheckDictEncode(FunctionContext* ctx, const shared_ptr<DataType>& type,
                     const vector<T>& in_values, const vector<bool>& in_is_valid,
                     const vector<T>& out_values, const vector<bool>& out_is_valid,
                     const vector<int32_t>& out_indices) {
  shared_ptr<Array> input = _MakeArray<Type, T>(type, in_values, in_is_valid);
  shared_ptr<Array> ex_dict = _MakeArray<Type, T>(type, out_values, out_is_valid);
  shared_ptr<Array> ex_indices =
      _MakeArray<Int32Type, int32_t>(int32(), out_indices, in_is_valid);

  DictionaryArray expected(dictionary(int32(), ex_dict), ex_indices);

  Datum datum_out;
  ASSERT_OK(DictionaryEncode(ctx, Datum(input), &datum_out));
  shared_ptr<Array> result = MakeArray(datum_out.array());

  ASSERT_ARRAYS_EQUAL(expected, *result);
}

class TestHashKernel : public ComputeFixture, public TestBase {};

template <typename Type>
class TestHashKernelPrimitive : public ComputeFixture, public TestBase {};

typedef ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type,
                         UInt32Type, Int64Type, UInt64Type, FloatType, DoubleType,
                         Date32Type, Date64Type>
    PrimitiveDictionaries;

TYPED_TEST_CASE(TestHashKernelPrimitive, PrimitiveDictionaries);

TYPED_TEST(TestHashKernelPrimitive, Unique) {
  using T = typename TypeParam::c_type;
  auto type = TypeTraits<TypeParam>::type_singleton();
  CheckUnique<TypeParam, T>(&this->ctx_, type, {2, 1, 2, 1}, {true, false, true, true},
                            {2, 1}, {});
}

TYPED_TEST(TestHashKernelPrimitive, DictEncode) {
  using T = typename TypeParam::c_type;
  auto type = TypeTraits<TypeParam>::type_singleton();
  CheckDictEncode<TypeParam, T>(&this->ctx_, type, {2, 1, 2, 1, 2, 3},
                                {true, false, true, true, true, true}, {2, 1, 3}, {},
                                {0, 0, 0, 1, 0, 2});
}

TYPED_TEST(TestHashKernelPrimitive, PrimitiveResizeTable) {
  using T = typename TypeParam::c_type;
  // Skip this test for (u)int8
  if (sizeof(Scalar) == 1) {
    return;
  }

  const int64_t kTotalValues = 1000000;
  const int64_t kRepeats = 5;

  vector<T> values;
  vector<T> uniques;
  vector<int32_t> indices;
  for (int64_t i = 0; i < kTotalValues * kRepeats; i++) {
    const auto val = static_cast<T>(i % kTotalValues);
    values.push_back(val);

    if (i < kTotalValues) {
      uniques.push_back(val);
    }
    indices.push_back(static_cast<int32_t>(i % kTotalValues));
  }

  auto type = TypeTraits<TypeParam>::type_singleton();
  CheckUnique<TypeParam, T>(&this->ctx_, type, values, {}, uniques, {});

  CheckDictEncode<TypeParam, T>(&this->ctx_, type, values, {}, uniques, {}, indices);
}

TEST_F(TestHashKernel, UniqueTimeTimestamp) {
  CheckUnique<Time32Type, int32_t>(&this->ctx_, time32(TimeUnit::SECOND), {2, 1, 2, 1},
                                   {true, false, true, true}, {2, 1}, {});

  CheckUnique<Time64Type, int64_t>(&this->ctx_, time64(TimeUnit::NANO), {2, 1, 2, 1},
                                   {true, false, true, true}, {2, 1}, {});

  CheckUnique<TimestampType, int64_t>(&this->ctx_, timestamp(TimeUnit::NANO),
                                      {2, 1, 2, 1}, {true, false, true, true}, {2, 1},
                                      {});
}

TEST_F(TestHashKernel, UniqueBoolean) {
  CheckUnique<BooleanType, bool>(&this->ctx_, boolean(), {true, true, false, true},
                                 {true, false, true, true}, {true, false}, {});

  CheckUnique<BooleanType, bool>(&this->ctx_, boolean(), {false, true, false, true},
                                 {true, false, true, true}, {false, true}, {});

  // No nulls
  CheckUnique<BooleanType, bool>(&this->ctx_, boolean(), {true, true, false, true}, {},
                                 {true, false}, {});

  CheckUnique<BooleanType, bool>(&this->ctx_, boolean(), {false, true, false, true}, {},
                                 {false, true}, {});
}

TEST_F(TestHashKernel, DictEncodeBoolean) {
  CheckDictEncode<BooleanType, bool>(
      &this->ctx_, boolean(), {true, true, false, true, false},
      {true, false, true, true, true}, {true, false}, {}, {0, 0, 1, 0, 1});

  CheckDictEncode<BooleanType, bool>(
      &this->ctx_, boolean(), {false, true, false, true, false},
      {true, false, true, true, true}, {false, true}, {}, {0, 0, 0, 1, 0});

  // No nulls
  CheckDictEncode<BooleanType, bool>(&this->ctx_, boolean(),
                                     {true, true, false, true, false}, {}, {true, false},
                                     {}, {0, 0, 1, 0, 1});

  CheckDictEncode<BooleanType, bool>(&this->ctx_, boolean(),
                                     {false, true, false, true, false}, {}, {false, true},
                                     {}, {0, 1, 0, 1, 0});
}

TEST_F(TestHashKernel, UniqueBinary) {
  CheckUnique<BinaryType, std::string>(&this->ctx_, binary(),
                                       {"test", "", "test2", "test"},
                                       {true, false, true, true}, {"test", "test2"}, {});

  CheckUnique<StringType, std::string>(&this->ctx_, utf8(), {"test", "", "test2", "test"},
                                       {true, false, true, true}, {"test", "test2"}, {});
}

TEST_F(TestHashKernel, DictEncodeBinary) {
  CheckDictEncode<BinaryType, std::string>(
      &this->ctx_, binary(), {"test", "", "test2", "test", "baz"},
      {true, false, true, true, true}, {"test", "test2", "baz"}, {}, {0, 0, 1, 0, 2});

  CheckDictEncode<StringType, std::string>(
      &this->ctx_, utf8(), {"test", "", "test2", "test", "baz"},
      {true, false, true, true, true}, {"test", "test2", "baz"}, {}, {0, 0, 1, 0, 2});
}

TEST_F(TestHashKernel, BinaryResizeTable) {
  const int64_t kTotalValues = 10000;
  const int64_t kRepeats = 10;

  vector<std::string> values;
  vector<std::string> uniques;
  vector<int32_t> indices;
  for (int64_t i = 0; i < kTotalValues * kRepeats; i++) {
    int64_t index = i % kTotalValues;
    std::stringstream ss;
    ss << "test" << index;
    std::string val = ss.str();

    values.push_back(val);

    if (i < kTotalValues) {
      uniques.push_back(val);
    }
    indices.push_back(static_cast<int32_t>(i % kTotalValues));
  }

  CheckUnique<BinaryType, std::string>(&this->ctx_, binary(), values, {}, uniques, {});
  CheckDictEncode<BinaryType, std::string>(&this->ctx_, binary(), values, {}, uniques, {},
                                           indices);

  CheckUnique<StringType, std::string>(&this->ctx_, utf8(), values, {}, uniques, {});
  CheckDictEncode<StringType, std::string>(&this->ctx_, utf8(), values, {}, uniques, {},
                                           indices);
}

TEST_F(TestHashKernel, UniqueFixedSizeBinary) {
  CheckUnique<FixedSizeBinaryType, std::string>(
      &this->ctx_, fixed_size_binary(5), {"aaaaa", "", "bbbbb", "aaaaa"},
      {true, false, true, true}, {"aaaaa", "bbbbb"}, {});
}

TEST_F(TestHashKernel, DictEncodeFixedSizeBinary) {
  CheckDictEncode<FixedSizeBinaryType, std::string>(
      &this->ctx_, fixed_size_binary(5), {"bbbbb", "", "bbbbb", "aaaaa", "ccccc"},
      {true, false, true, true, true}, {"bbbbb", "aaaaa", "ccccc"}, {}, {0, 0, 0, 1, 2});
}

TEST_F(TestHashKernel, FixedSizeBinaryResizeTable) {
  const int64_t kTotalValues = 10000;
  const int64_t kRepeats = 10;

  vector<std::string> values;
  vector<std::string> uniques;
  vector<int32_t> indices;
  for (int64_t i = 0; i < kTotalValues * kRepeats; i++) {
    int64_t index = i % kTotalValues;
    std::stringstream ss;
    ss << "test" << static_cast<char>(index / 128) << static_cast<char>(index % 128);
    std::string val = ss.str();

    values.push_back(val);

    if (i < kTotalValues) {
      uniques.push_back(val);
    }
    indices.push_back(static_cast<int32_t>(i % kTotalValues));
  }

  auto type = fixed_size_binary(6);
  CheckUnique<FixedSizeBinaryType, std::string>(&this->ctx_, type, values, {}, uniques,
                                                {});
  CheckDictEncode<FixedSizeBinaryType, std::string>(&this->ctx_, type, values, {},
                                                    uniques, {}, indices);
}

TEST_F(TestHashKernel, UniqueDecimal) {
  vector<Decimal128> values{12, 12, 11, 12};
  vector<Decimal128> expected{12, 11};

  CheckUnique<Decimal128Type, Decimal128>(&this->ctx_, decimal(2, 0), values,
                                          {true, false, true, true}, expected, {});
}

TEST_F(TestHashKernel, DictEncodeDecimal) {
  vector<Decimal128> values{12, 12, 11, 12, 13};
  vector<Decimal128> expected{12, 11, 13};

  CheckDictEncode<Decimal128Type, Decimal128>(&this->ctx_, decimal(2, 0), values,
                                              {true, false, true, true, true}, expected,
                                              {}, {0, 0, 1, 0, 2});
}

TEST_F(TestHashKernel, ChunkedArrayInvoke) {
  vector<std::string> values1 = {"foo", "bar", "foo"};
  vector<std::string> values2 = {"bar", "baz", "quuux", "foo"};

  auto type = utf8();
  auto a1 = _MakeArray<StringType, std::string>(type, values1, {});
  auto a2 = _MakeArray<StringType, std::string>(type, values2, {});

  vector<std::string> dict_values = {"foo", "bar", "baz", "quuux"};
  auto ex_dict = _MakeArray<StringType, std::string>(type, dict_values, {});

  ArrayVector arrays = {a1, a2};
  auto carr = std::make_shared<ChunkedArray>(arrays);

  // Unique
  shared_ptr<Array> result;
  ASSERT_OK(Unique(&this->ctx_, Datum(carr), &result));
  ASSERT_ARRAYS_EQUAL(*ex_dict, *result);

  // Dictionary encode
  auto dict_type = dictionary(int32(), ex_dict);

  auto i1 = _MakeArray<Int32Type, int32_t>(int32(), {0, 1, 0}, {});
  auto i2 = _MakeArray<Int32Type, int32_t>(int32(), {1, 2, 3, 0}, {});

  ArrayVector dict_arrays = {std::make_shared<DictionaryArray>(dict_type, i1),
                             std::make_shared<DictionaryArray>(dict_type, i2)};
  auto dict_carr = std::make_shared<ChunkedArray>(dict_arrays);

  Datum encoded_out;
  ASSERT_OK(DictionaryEncode(&this->ctx_, Datum(carr), &encoded_out));
  ASSERT_EQ(Datum::CHUNKED_ARRAY, encoded_out.kind());

  ASSERT_TRUE(encoded_out.chunked_array()->Equals(*dict_carr));
}

}  // namespace compute
}  // namespace arrow
