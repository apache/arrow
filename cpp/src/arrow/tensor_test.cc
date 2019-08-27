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

// Unit tests for DataType (and subclasses), Field, and Schema

#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/tensor.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow {

void AssertCountNonZero(const Tensor& t, int64_t expected) {
  int64_t count = -1;
  ASSERT_OK(t.CountNonZero(&count));
  ASSERT_EQ(count, expected);
}

TEST(TestTensor, ZeroDim) {
  const int64_t values = 1;
  std::vector<int64_t> shape = {};

  using T = int64_t;

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(AllocateBuffer(values * sizeof(T), &buffer));

  Tensor t0(int64(), buffer, shape);

  ASSERT_EQ(1, t0.size());
}

TEST(TestTensor, BasicCtors) {
  const int64_t values = 24;
  std::vector<int64_t> shape = {4, 6};
  std::vector<int64_t> strides = {48, 8};
  std::vector<std::string> dim_names = {"foo", "bar"};

  using T = int64_t;

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(AllocateBuffer(values * sizeof(T), &buffer));

  Tensor t1(int64(), buffer, shape);
  Tensor t2(int64(), buffer, shape, strides);
  Tensor t3(int64(), buffer, shape, strides, dim_names);

  ASSERT_EQ(24, t1.size());
  ASSERT_TRUE(t1.is_mutable());

  ASSERT_EQ(strides, t1.strides());
  ASSERT_EQ(strides, t2.strides());

  ASSERT_EQ(std::vector<std::string>({"foo", "bar"}), t3.dim_names());
  ASSERT_EQ("foo", t3.dim_name(0));
  ASSERT_EQ("bar", t3.dim_name(1));

  ASSERT_EQ(std::vector<std::string>({}), t1.dim_names());
  ASSERT_EQ("", t1.dim_name(0));
  ASSERT_EQ("", t1.dim_name(1));
}

TEST(TestTensor, IsContiguous) {
  const int64_t values = 24;
  std::vector<int64_t> shape = {4, 6};
  std::vector<int64_t> strides = {48, 8};

  using T = int64_t;

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(AllocateBuffer(values * sizeof(T), &buffer));

  std::vector<int64_t> c_strides = {48, 8};
  std::vector<int64_t> f_strides = {8, 32};
  std::vector<int64_t> noncontig_strides = {8, 8};
  Tensor t1(int64(), buffer, shape, c_strides);
  Tensor t2(int64(), buffer, shape, f_strides);
  Tensor t3(int64(), buffer, shape, noncontig_strides);

  ASSERT_TRUE(t1.is_contiguous());
  ASSERT_TRUE(t2.is_contiguous());
  ASSERT_FALSE(t3.is_contiguous());
}

TEST(TestTensor, ZeroSizedTensor) {
  std::vector<int64_t> shape = {0};

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(AllocateBuffer(0, &buffer));

  Tensor t(int64(), buffer, shape);
  ASSERT_EQ(t.strides().size(), 1);
}

TEST(TestTensor, CountNonZeroForZeroSizedTensor) {
  std::vector<int64_t> shape = {0};

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(AllocateBuffer(0, &buffer));

  Tensor t(int64(), buffer, shape);
  AssertCountNonZero(t, 0);
}

TEST(TestTensor, CountNonZeroForContiguousTensor) {
  std::vector<int64_t> shape = {4, 6};
  std::vector<int64_t> values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                 0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);

  std::vector<int64_t> c_strides = {48, 8};
  std::vector<int64_t> f_strides = {8, 32};
  Tensor t1(int64(), buffer, shape, c_strides);
  Tensor t2(int64(), buffer, shape, f_strides);

  ASSERT_TRUE(t1.is_contiguous());
  ASSERT_TRUE(t2.is_contiguous());
  AssertCountNonZero(t1, 12);
  AssertCountNonZero(t2, 12);
}

TEST(TestTensor, CountNonZeroForNonContiguousTensor) {
  std::vector<int64_t> shape = {4, 4};
  std::vector<int64_t> values = {
      1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,  7, 0,  8, 0,
      0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16, 0, 15, 0, 16,
  };
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);

  std::vector<int64_t> noncontig_strides = {64, 16};
  Tensor t(int64(), buffer, shape, noncontig_strides);

  ASSERT_FALSE(t.is_contiguous());
  AssertCountNonZero(t, 8);
}

TEST(TestTensor, ElementAccessInt32) {
  std::vector<int64_t> shape = {2, 3};
  std::vector<int32_t> values = {1, 2, 3, 4, 5, 6};
  std::vector<int64_t> c_strides = {sizeof(int32_t) * 3, sizeof(int32_t)};
  std::vector<int64_t> f_strides = {sizeof(int32_t), sizeof(int32_t) * 2};
  Tensor tc(int64(), Buffer::Wrap(values), shape, c_strides);
  Tensor tf(int64(), Buffer::Wrap(values), shape, f_strides);

  EXPECT_EQ(1, tc.Value<Int32Type>({0, 0}));
  EXPECT_EQ(2, tc.Value<Int32Type>({0, 1}));
  EXPECT_EQ(4, tc.Value<Int32Type>({1, 0}));

  EXPECT_EQ(1, tf.Value<Int32Type>({0, 0}));
  EXPECT_EQ(3, tf.Value<Int32Type>({0, 1}));
  EXPECT_EQ(2, tf.Value<Int32Type>({1, 0}));

  // Tensor::Value<T>() doesn't prohibit element access if the type T is different from
  // the value type of the tensor
  EXPECT_NO_THROW({
    int32_t x = 3;
    EXPECT_EQ(*reinterpret_cast<int8_t*>(&x), tc.Value<Int8Type>({0, 2}));

    int64_t y;
    reinterpret_cast<int32_t*>(&y)[0] = 4;
    reinterpret_cast<int32_t*>(&y)[1] = 5;
    EXPECT_EQ(y, tc.Value<Int64Type>({1, 0}));
  });
}

TEST(TestTensor, EqualsInt64) {
  std::vector<int64_t> shape = {4, 4};

  std::vector<int64_t> c_values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  std::vector<int64_t> c_strides = {32, 8};
  Tensor tc1(int64(), Buffer::Wrap(c_values), shape, c_strides);

  std::vector<int64_t> c_values_2 = c_values;
  Tensor tc2(int64(), Buffer::Wrap(c_values_2), shape, c_strides);

  std::vector<int64_t> f_values = {1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15, 4, 8, 12, 16};
  Tensor tc3(int64(), Buffer::Wrap(f_values), shape, c_strides);

  Tensor tc4(int64(), Buffer::Wrap(c_values), {8, 2}, {16, 8});

  std::vector<int64_t> f_strides = {8, 32};
  Tensor tf1(int64(), Buffer::Wrap(f_values), shape, f_strides);

  std::vector<int64_t> f_values_2 = f_values;
  Tensor tf2(int64(), Buffer::Wrap(f_values_2), shape, f_strides);

  Tensor tf3(int64(), Buffer::Wrap(c_values), shape, f_strides);

  std::vector<int64_t> nc_values = {1, 0, 5, 0, 9,  0, 13, 0, 2, 0, 6, 0, 10, 0, 14, 0,
                                    3, 0, 7, 0, 11, 0, 15, 0, 4, 0, 8, 0, 12, 0, 16, 0};
  std::vector<int64_t> nc_strides = {16, 64};
  Tensor tnc(int64(), Buffer::Wrap(nc_values), shape, nc_strides);

  ASSERT_TRUE(tc1.is_contiguous());
  ASSERT_TRUE(tc1.is_row_major());

  ASSERT_TRUE(tf1.is_contiguous());
  ASSERT_TRUE(tf1.is_column_major());

  ASSERT_FALSE(tnc.is_contiguous());

  // same object
  EXPECT_TRUE(tc1.Equals(tc1));
  EXPECT_TRUE(tf1.Equals(tf1));
  EXPECT_TRUE(tnc.Equals(tnc));

  // different memory
  EXPECT_TRUE(tc1.Equals(tc2));
  EXPECT_TRUE(tf1.Equals(tf2));
  EXPECT_FALSE(tc1.Equals(tc3));

  // different shapes but same data
  EXPECT_FALSE(tc1.Equals(tc4));

  // row-major and column-major
  EXPECT_TRUE(tc1.Equals(tf1));
  EXPECT_FALSE(tc3.Equals(tf1));

  // row-major and non-contiguous
  EXPECT_TRUE(tc1.Equals(tnc));
  EXPECT_FALSE(tc3.Equals(tnc));

  // column-major and non-contiguous
  EXPECT_TRUE(tf1.Equals(tnc));
  EXPECT_FALSE(tf3.Equals(tnc));

  // zero-size tensor
  std::shared_ptr<Buffer> empty_buffer1, empty_buffer2;
  ASSERT_OK(AllocateBuffer(0, &empty_buffer1));
  ASSERT_OK(AllocateBuffer(0, &empty_buffer2));
  Tensor empty1(int64(), empty_buffer1, {0});
  Tensor empty2(int64(), empty_buffer2, {0});
  EXPECT_FALSE(empty1.Equals(tc1));
  EXPECT_TRUE(empty1.Equals(empty2));
}

template <typename DataType>
class TestFloatTensor : public ::testing::Test {};

TYPED_TEST_CASE_P(TestFloatTensor);

TYPED_TEST_P(TestFloatTensor, Equals) {
  using DataType = TypeParam;
  using c_data_type = typename DataType::c_type;
  const int unit_size = sizeof(c_data_type);

  std::vector<int64_t> shape = {4, 4};

  std::vector<c_data_type> c_values = {1, 2,  3,  4,  5,  6,  7,  8,
                                       9, 10, 11, 12, 13, 14, 15, 16};
  std::vector<int64_t> c_strides = {unit_size * shape[1], unit_size};
  Tensor tc1(TypeTraits<DataType>::type_singleton(), Buffer::Wrap(c_values), shape,
             c_strides);

  std::vector<c_data_type> c_values_2 = c_values;
  Tensor tc2(TypeTraits<DataType>::type_singleton(), Buffer::Wrap(c_values_2), shape,
             c_strides);

  std::vector<c_data_type> f_values = {1, 5, 9,  13, 2, 6, 10, 14,
                                       3, 7, 11, 15, 4, 8, 12, 16};
  Tensor tc3(TypeTraits<DataType>::type_singleton(), Buffer::Wrap(f_values), shape,
             c_strides);

  std::vector<int64_t> f_strides = {unit_size, unit_size * shape[0]};
  Tensor tf1(TypeTraits<DataType>::type_singleton(), Buffer::Wrap(f_values), shape,
             f_strides);

  std::vector<c_data_type> f_values_2 = f_values;
  Tensor tf2(TypeTraits<DataType>::type_singleton(), Buffer::Wrap(f_values_2), shape,
             f_strides);

  Tensor tf3(TypeTraits<DataType>::type_singleton(), Buffer::Wrap(c_values), shape,
             f_strides);

  std::vector<c_data_type> nc_values = {1,  0,  5, 0,  9, 0, 13, 0, 2,  0,  6,
                                        0,  10, 0, 14, 0, 3, 0,  7, 0,  11, 0,
                                        15, 0,  4, 0,  8, 0, 12, 0, 16, 0};
  std::vector<int64_t> nc_strides = {unit_size * 2, unit_size * 2 * shape[0]};
  Tensor tnc(TypeTraits<DataType>::type_singleton(), Buffer::Wrap(nc_values), shape,
             nc_strides);

  ASSERT_TRUE(tc1.is_contiguous());
  ASSERT_TRUE(tc1.is_row_major());

  ASSERT_TRUE(tf1.is_contiguous());
  ASSERT_TRUE(tf1.is_column_major());

  ASSERT_FALSE(tnc.is_contiguous());

  // same object
  EXPECT_TRUE(tc1.Equals(tc1));
  EXPECT_TRUE(tf1.Equals(tf1));
  EXPECT_TRUE(tnc.Equals(tnc));

  // different memory
  EXPECT_TRUE(tc1.Equals(tc2));
  EXPECT_TRUE(tf1.Equals(tf2));
  EXPECT_FALSE(tc1.Equals(tc3));

  // row-major and column-major
  EXPECT_TRUE(tc1.Equals(tf1));
  EXPECT_FALSE(tc3.Equals(tf1));

  // row-major and non-contiguous
  EXPECT_TRUE(tc1.Equals(tnc));
  EXPECT_FALSE(tc3.Equals(tnc));

  // column-major and non-contiguous
  EXPECT_TRUE(tf1.Equals(tnc));
  EXPECT_FALSE(tf3.Equals(tnc));

  // tensors with NaNs
  const c_data_type nan_value = static_cast<c_data_type>(NAN);
  c_values[0] = nan_value;
  EXPECT_TRUE(std::isnan(tc1.Value<DataType>({0, 0})));
  EXPECT_FALSE(tc1.Equals(tc1));                                  // same object
  EXPECT_TRUE(tc1.Equals(tc1, EqualOptions().nans_equal(true)));  // same object
  EXPECT_FALSE(std::isnan(tc2.Value<DataType>({0, 0})));
  EXPECT_FALSE(tc1.Equals(tc2));                                   // different memory
  EXPECT_FALSE(tc1.Equals(tc2, EqualOptions().nans_equal(true)));  // different memory

  c_values_2[0] = nan_value;
  EXPECT_TRUE(std::isnan(tc2.Value<DataType>({0, 0})));
  EXPECT_FALSE(tc1.Equals(tc2));                                  // different memory
  EXPECT_TRUE(tc1.Equals(tc2, EqualOptions().nans_equal(true)));  // different memory
}

REGISTER_TYPED_TEST_CASE_P(TestFloatTensor, Equals);

INSTANTIATE_TYPED_TEST_CASE_P(Float32, TestFloatTensor, FloatType);
INSTANTIATE_TYPED_TEST_CASE_P(Float64, TestFloatTensor, DoubleType);

TEST(TestNumericTensor, ElementAccessWithRowMajorStrides) {
  std::vector<int64_t> shape = {3, 4};

  std::vector<int64_t> values_i64 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
  std::shared_ptr<Buffer> buffer_i64(Buffer::Wrap(values_i64));
  NumericTensor<Int64Type> t_i64(buffer_i64, shape);

  ASSERT_TRUE(t_i64.is_row_major());
  ASSERT_FALSE(t_i64.is_column_major());
  ASSERT_TRUE(t_i64.is_contiguous());
  ASSERT_EQ(1, t_i64.Value({0, 0}));
  ASSERT_EQ(5, t_i64.Value({1, 0}));
  ASSERT_EQ(6, t_i64.Value({1, 1}));
  ASSERT_EQ(11, t_i64.Value({2, 2}));

  std::vector<float> values_f32 = {1.1f, 2.1f, 3.1f, 4.1f,  5.1f,  6.1f,
                                   7.1f, 8.1f, 9.1f, 10.1f, 11.1f, 12.1f};
  std::shared_ptr<Buffer> buffer_f32(Buffer::Wrap(values_f32));
  NumericTensor<FloatType> t_f32(buffer_f32, shape);

  ASSERT_TRUE(t_f32.is_row_major());
  ASSERT_FALSE(t_f32.is_column_major());
  ASSERT_TRUE(t_f32.is_contiguous());
  ASSERT_EQ(1.1f, t_f32.Value({0, 0}));
  ASSERT_EQ(5.1f, t_f32.Value({1, 0}));
  ASSERT_EQ(6.1f, t_f32.Value({1, 1}));
  ASSERT_EQ(11.1f, t_f32.Value({2, 2}));
}

TEST(TestNumericTensor, ElementAccessWithColumnMajorStrides) {
  std::vector<int64_t> shape = {3, 4};

  const int64_t i64_size = sizeof(int64_t);
  std::vector<int64_t> values_i64 = {1, 5, 9, 2, 6, 10, 3, 7, 11, 4, 8, 12};
  std::vector<int64_t> strides_i64 = {i64_size, i64_size * 3};
  std::shared_ptr<Buffer> buffer_i64(Buffer::Wrap(values_i64));
  NumericTensor<Int64Type> t_i64(buffer_i64, shape, strides_i64);

  ASSERT_TRUE(t_i64.is_column_major());
  ASSERT_FALSE(t_i64.is_row_major());
  ASSERT_TRUE(t_i64.is_contiguous());
  ASSERT_EQ(1, t_i64.Value({0, 0}));
  ASSERT_EQ(2, t_i64.Value({0, 1}));
  ASSERT_EQ(4, t_i64.Value({0, 3}));
  ASSERT_EQ(5, t_i64.Value({1, 0}));
  ASSERT_EQ(6, t_i64.Value({1, 1}));
  ASSERT_EQ(11, t_i64.Value({2, 2}));

  const int64_t f32_size = sizeof(float);
  std::vector<float> values_f32 = {1.1f, 5.1f, 9.1f,  2.1f, 6.1f, 10.1f,
                                   3.1f, 7.1f, 11.1f, 4.1f, 8.1f, 12.1f};
  std::vector<int64_t> strides_f32 = {f32_size, f32_size * 3};
  std::shared_ptr<Buffer> buffer_f32(Buffer::Wrap(values_f32));
  NumericTensor<FloatType> t_f32(buffer_f32, shape, strides_f32);

  ASSERT_TRUE(t_f32.is_column_major());
  ASSERT_FALSE(t_f32.is_row_major());
  ASSERT_TRUE(t_f32.is_contiguous());
  ASSERT_EQ(1.1f, t_f32.Value({0, 0}));
  ASSERT_EQ(2.1f, t_f32.Value({0, 1}));
  ASSERT_EQ(4.1f, t_f32.Value({0, 3}));
  ASSERT_EQ(5.1f, t_f32.Value({1, 0}));
  ASSERT_EQ(6.1f, t_f32.Value({1, 1}));
  ASSERT_EQ(11.1f, t_f32.Value({2, 2}));
}

TEST(TestNumericTensor, ElementAccessWithNonContiguousStrides) {
  std::vector<int64_t> shape = {3, 4};

  const int64_t i64_size = sizeof(int64_t);
  std::vector<int64_t> values_i64 = {1, 2, 3, 4, 0,  0,  5,  6, 7,
                                     8, 0, 0, 9, 10, 11, 12, 0, 0};
  std::vector<int64_t> strides_i64 = {i64_size * 6, i64_size};
  std::shared_ptr<Buffer> buffer_i64(Buffer::Wrap(values_i64));
  NumericTensor<Int64Type> t_i64(buffer_i64, shape, strides_i64);

  ASSERT_FALSE(t_i64.is_contiguous());
  ASSERT_FALSE(t_i64.is_row_major());
  ASSERT_FALSE(t_i64.is_column_major());
  ASSERT_EQ(1, t_i64.Value({0, 0}));
  ASSERT_EQ(2, t_i64.Value({0, 1}));
  ASSERT_EQ(4, t_i64.Value({0, 3}));
  ASSERT_EQ(5, t_i64.Value({1, 0}));
  ASSERT_EQ(6, t_i64.Value({1, 1}));
  ASSERT_EQ(11, t_i64.Value({2, 2}));

  const int64_t f32_size = sizeof(float);
  std::vector<float> values_f32 = {1.1f, 2.1f,  3.1f,  4.1f,  0.0f, 0.0f,
                                   5.1f, 6.1f,  7.1f,  8.1f,  0.0f, 0.0f,
                                   9.1f, 10.1f, 11.1f, 12.1f, 0.0f, 0.0f};
  std::vector<int64_t> strides_f32 = {f32_size * 6, f32_size};
  std::shared_ptr<Buffer> buffer_f32(Buffer::Wrap(values_f32));
  NumericTensor<FloatType> t_f32(buffer_f32, shape, strides_f32);

  ASSERT_FALSE(t_f32.is_contiguous());
  ASSERT_FALSE(t_f32.is_row_major());
  ASSERT_FALSE(t_f32.is_column_major());
  ASSERT_EQ(1.1f, t_f32.Value({0, 0}));
  ASSERT_EQ(2.1f, t_f32.Value({0, 1}));
  ASSERT_EQ(4.1f, t_f32.Value({0, 3}));
  ASSERT_EQ(5.1f, t_f32.Value({1, 0}));
  ASSERT_EQ(6.1f, t_f32.Value({1, 1}));
  ASSERT_EQ(11.1f, t_f32.Value({2, 2}));
}

}  // namespace arrow
