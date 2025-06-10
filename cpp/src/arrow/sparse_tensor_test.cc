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
#include <numeric>
#include <string>
#include <vector>

#include <iostream>

#include <gtest/gtest.h>

#include "arrow/sparse_tensor.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/sort_internal.h"

namespace arrow {

static inline void CheckSparseIndexFormatType(SparseTensorFormat::type expected,
                                              const SparseTensor& sparse_tensor) {
  ASSERT_EQ(expected, sparse_tensor.format_id());
  ASSERT_EQ(expected, sparse_tensor.sparse_index()->format_id());
}

static inline void AssertCOOIndex(const std::shared_ptr<Tensor>& sidx, const int64_t nth,
                                  const std::vector<int64_t>& expected_values) {
  int64_t n = static_cast<int64_t>(expected_values.size());
  for (int64_t i = 0; i < n; ++i) {
    ASSERT_EQ(expected_values[i], sidx->Value<Int64Type>({nth, i}));
  }
}

//-----------------------------------------------------------------------------
// SparseCOOIndex

TEST(TestSparseCOOIndex, MakeRowMajorCanonical) {
  std::vector<int32_t> values = {0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 1, 3, 0, 2, 0, 0, 2, 2,
                                 1, 0, 1, 1, 0, 3, 1, 1, 0, 1, 1, 2, 1, 2, 1, 1, 2, 3};
  auto data = Buffer::Wrap(values);
  std::vector<int64_t> shape = {12, 3};
  std::vector<int64_t> strides = {3 * sizeof(int32_t), sizeof(int32_t)};  // Row-major

  // OK
  std::shared_ptr<SparseCOOIndex> si;
  ASSERT_OK_AND_ASSIGN(si, SparseCOOIndex::Make(int32(), shape, strides, data));
  ASSERT_EQ(shape, si->indices()->shape());
  ASSERT_EQ(strides, si->indices()->strides());
  ASSERT_EQ(data->data(), si->indices()->raw_data());
  ASSERT_TRUE(si->is_canonical());

  // Non-integer type
  auto res = SparseCOOIndex::Make(float32(), shape, strides, data);
  ASSERT_RAISES(TypeError, res);

  // Non-matrix indices
  res = SparseCOOIndex::Make(int32(), {4, 3, 4}, strides, data);
  ASSERT_RAISES(Invalid, res);

  // Non-contiguous indices
  res = SparseCOOIndex::Make(int32(), {6, 3}, {6 * sizeof(int32_t), 2 * sizeof(int32_t)},
                             data);
  ASSERT_RAISES(Invalid, res);

  // Make from sparse tensor properties
  // (shape is arbitrary 3-dim, non-zero length = 12)
  ASSERT_OK_AND_ASSIGN(si, SparseCOOIndex::Make(int32(), {99, 99, 99}, 12, data));
  ASSERT_EQ(shape, si->indices()->shape());
  ASSERT_EQ(strides, si->indices()->strides());
  ASSERT_EQ(data->data(), si->indices()->raw_data());
}

TEST(TestSparseCOOIndex, MakeRowMajorNonCanonical) {
  std::vector<int32_t> values = {0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 1, 3, 0, 2, 0, 1, 0, 1,
                                 0, 2, 2, 1, 0, 3, 1, 1, 0, 1, 1, 2, 1, 2, 1, 1, 2, 3};
  auto data = Buffer::Wrap(values);
  std::vector<int64_t> shape = {12, 3};
  std::vector<int64_t> strides = {3 * sizeof(int32_t), sizeof(int32_t)};  // Row-major

  // OK
  std::shared_ptr<SparseCOOIndex> si;
  ASSERT_OK_AND_ASSIGN(si, SparseCOOIndex::Make(int32(), shape, strides, data));
  ASSERT_EQ(shape, si->indices()->shape());
  ASSERT_EQ(strides, si->indices()->strides());
  ASSERT_EQ(data->data(), si->indices()->raw_data());
  ASSERT_FALSE(si->is_canonical());
}

TEST(TestSparseCOOIndex, MakeColumnMajorCanonical) {
  std::vector<int32_t> values = {0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 2, 2,
                                 0, 0, 1, 1, 2, 2, 0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3};
  auto data = Buffer::Wrap(values);
  std::vector<int64_t> shape = {12, 3};
  std::vector<int64_t> strides = {sizeof(int32_t), 12 * sizeof(int32_t)};  // Column-major

  // OK
  std::shared_ptr<SparseCOOIndex> si;
  ASSERT_OK_AND_ASSIGN(si, SparseCOOIndex::Make(int32(), shape, strides, data));
  ASSERT_EQ(shape, si->indices()->shape());
  ASSERT_EQ(strides, si->indices()->strides());
  ASSERT_EQ(data->data(), si->indices()->raw_data());
  ASSERT_TRUE(si->is_canonical());
}

TEST(TestSparseCOOIndex, MakeColumnMajorNonCanonical) {
  std::vector<int32_t> values = {0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1, 1, 2, 0,
                                 2, 0, 1, 1, 2, 2, 0, 2, 1, 3, 0, 1, 2, 3, 0, 2, 1, 3};
  auto data = Buffer::Wrap(values);
  std::vector<int64_t> shape = {12, 3};
  std::vector<int64_t> strides = {sizeof(int32_t), 12 * sizeof(int32_t)};  // Column-major

  // OK
  std::shared_ptr<SparseCOOIndex> si;
  ASSERT_OK_AND_ASSIGN(si, SparseCOOIndex::Make(int32(), shape, strides, data));
  ASSERT_EQ(shape, si->indices()->shape());
  ASSERT_EQ(strides, si->indices()->strides());
  ASSERT_EQ(data->data(), si->indices()->raw_data());
  ASSERT_FALSE(si->is_canonical());
}

TEST(TestSparseCOOIndex, MakeEmptyIndex) {
  std::vector<int32_t> values = {};
  auto data = Buffer::Wrap(values);
  std::vector<int64_t> shape = {0, 3};
  std::vector<int64_t> strides = {sizeof(int32_t), sizeof(int32_t)};  // Empty strides

  // OK
  std::shared_ptr<SparseCOOIndex> si;
  ASSERT_OK_AND_ASSIGN(si, SparseCOOIndex::Make(int32(), shape, strides, data));
  ASSERT_EQ(shape, si->indices()->shape());
  ASSERT_EQ(strides, si->indices()->strides());
  ASSERT_EQ(data->data(), si->indices()->raw_data());
  ASSERT_TRUE(si->is_canonical());
}

TEST(TestSparseCSRIndex, Make) {
  std::vector<int32_t> indptr_values = {0, 2, 4, 6, 8, 10, 12};
  std::vector<int32_t> indices_values = {0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3};
  auto indptr_data = Buffer::Wrap(indptr_values);
  auto indices_data = Buffer::Wrap(indices_values);
  std::vector<int64_t> indptr_shape = {7};
  std::vector<int64_t> indices_shape = {12};

  // OK
  std::shared_ptr<SparseCSRIndex> si;
  ASSERT_OK_AND_ASSIGN(si, SparseCSRIndex::Make(int32(), indptr_shape, indices_shape,
                                                indptr_data, indices_data));
  ASSERT_EQ(indptr_shape, si->indptr()->shape());
  ASSERT_EQ(indptr_data->data(), si->indptr()->raw_data());
  ASSERT_EQ(indices_shape, si->indices()->shape());
  ASSERT_EQ(indices_data->data(), si->indices()->raw_data());
  ASSERT_EQ(std::string("SparseCSRIndex"), si->ToString());

  // Non-integer type
  auto res = SparseCSRIndex::Make(float32(), indptr_shape, indices_shape, indptr_data,
                                  indices_data);
  ASSERT_RAISES(TypeError, res);

  // Non-vector indptr shape
  ASSERT_RAISES(Invalid, SparseCSRIndex::Make(int32(), {1, 2}, indices_shape, indptr_data,
                                              indices_data));

  // Non-vector indices shape
  ASSERT_RAISES(Invalid, SparseCSRIndex::Make(int32(), indptr_shape, {1, 2}, indptr_data,
                                              indices_data));
}

TEST(TestSparseCSCIndex, Make) {
  std::vector<int32_t> indptr_values = {0, 2, 4, 6, 8, 10, 12};
  std::vector<int32_t> indices_values = {0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3};
  auto indptr_data = Buffer::Wrap(indptr_values);
  auto indices_data = Buffer::Wrap(indices_values);
  std::vector<int64_t> indptr_shape = {7};
  std::vector<int64_t> indices_shape = {12};

  // OK
  std::shared_ptr<SparseCSCIndex> si;
  ASSERT_OK_AND_ASSIGN(si, SparseCSCIndex::Make(int32(), indptr_shape, indices_shape,
                                                indptr_data, indices_data));
  ASSERT_EQ(indptr_shape, si->indptr()->shape());
  ASSERT_EQ(indptr_data->data(), si->indptr()->raw_data());
  ASSERT_EQ(indices_shape, si->indices()->shape());
  ASSERT_EQ(indices_data->data(), si->indices()->raw_data());
  ASSERT_EQ(std::string("SparseCSCIndex"), si->ToString());

  // Non-integer type
  ASSERT_RAISES(TypeError, SparseCSCIndex::Make(float32(), indptr_shape, indices_shape,
                                                indptr_data, indices_data));

  // Non-vector indptr shape
  ASSERT_RAISES(Invalid, SparseCSCIndex::Make(int32(), {1, 2}, indices_shape, indptr_data,
                                              indices_data));

  // Non-vector indices shape
  ASSERT_RAISES(Invalid, SparseCSCIndex::Make(int32(), indptr_shape, {1, 2}, indptr_data,
                                              indices_data));
}

template <typename ValueType>
class TestSparseTensorBase : public ::testing::Test {
 protected:
  std::vector<int64_t> shape_;
  std::vector<std::string> dim_names_;
};

//-----------------------------------------------------------------------------
// SparseCOOTensor

template <typename IndexValueType, typename ValueType = Int64Type>
class TestSparseCOOTensorBase : public TestSparseTensorBase<ValueType> {
 public:
  using c_value_type = typename ValueType::c_type;

  void SetUp() {
    shape_ = {2, 3, 4};
    dim_names_ = {"foo", "bar", "baz"};

    // Dense representation:
    // [
    //   [
    //     1 0 2 0
    //     0 3 0 4
    //     5 0 6 0
    //   ],
    //   [
    //      0 11  0 12
    //     13  0 14  0
    //      0 15  0 16
    //   ]
    // ]
    dense_values_ = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                     0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
    auto dense_data = Buffer::Wrap(dense_values_);
    NumericTensor<ValueType> dense_tensor(dense_data, shape_, {}, dim_names_);
    ASSERT_OK_AND_ASSIGN(sparse_tensor_from_dense_,
                         SparseCOOTensor::Make(
                             dense_tensor, TypeTraits<IndexValueType>::type_singleton()));
  }

 protected:
  using TestSparseTensorBase<ValueType>::shape_;
  using TestSparseTensorBase<ValueType>::dim_names_;
  std::vector<c_value_type> dense_values_;
  std::shared_ptr<SparseCOOTensor> sparse_tensor_from_dense_;
};

class TestSparseCOOTensor : public TestSparseCOOTensorBase<Int64Type> {};

TEST_F(TestSparseCOOTensor, CreationEmptyTensor) {
  SparseCOOTensor st1(int64(), this->shape_);
  SparseCOOTensor st2(int64(), this->shape_, this->dim_names_);

  ASSERT_EQ(0, st1.non_zero_length());
  ASSERT_EQ(0, st2.non_zero_length());

  ASSERT_EQ(24, st1.size());
  ASSERT_EQ(24, st2.size());

  ASSERT_EQ(std::vector<std::string>({"foo", "bar", "baz"}), st2.dim_names());
  ASSERT_EQ("foo", st2.dim_name(0));
  ASSERT_EQ("bar", st2.dim_name(1));
  ASSERT_EQ("baz", st2.dim_name(2));

  ASSERT_EQ(std::vector<std::string>({}), st1.dim_names());
  ASSERT_EQ("", st1.dim_name(0));
  ASSERT_EQ("", st1.dim_name(1));
  ASSERT_EQ("", st1.dim_name(2));
}

TEST_F(TestSparseCOOTensor, CreationFromZeroTensor) {
  const auto dense_size =
      std::accumulate(this->shape_.begin(), this->shape_.end(), int64_t(1),
                      [](int64_t a, int64_t x) { return a * x; });
  std::vector<int64_t> dense_values(dense_size, 0);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> t_zero,
                       Tensor::Make(int64(), Buffer::Wrap(dense_values), this->shape_));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<SparseCOOTensor> st_zero,
                       SparseCOOTensor::Make(*t_zero, int64()));

  ASSERT_EQ(0, st_zero->non_zero_length());
  ASSERT_EQ(dense_size, st_zero->size());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> t, st_zero->ToTensor());
  ASSERT_TRUE(t->Equals(*t_zero));
}

TEST_F(TestSparseCOOTensor, CreationFromNumericTensor) {
  auto st = this->sparse_tensor_from_dense_;
  CheckSparseIndexFormatType(SparseTensorFormat::COO, *st);

  ASSERT_EQ(12, st->non_zero_length());
  ASSERT_TRUE(st->is_mutable());

  auto* raw_data = reinterpret_cast<const int64_t*>(st->raw_data());
  AssertNumericDataEqual(raw_data, {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16});

  auto si = internal::checked_pointer_cast<SparseCOOIndex>(st->sparse_index());
  ASSERT_EQ(std::string("SparseCOOIndex"), si->ToString());
  ASSERT_TRUE(si->is_canonical());

  std::shared_ptr<Tensor> sidx = si->indices();
  ASSERT_EQ(std::vector<int64_t>({12, 3}), sidx->shape());
  ASSERT_TRUE(sidx->is_row_major());

  AssertCOOIndex(sidx, 0, {0, 0, 0});
  AssertCOOIndex(sidx, 1, {0, 0, 2});
  AssertCOOIndex(sidx, 2, {0, 1, 1});
  AssertCOOIndex(sidx, 10, {1, 2, 1});
  AssertCOOIndex(sidx, 11, {1, 2, 3});
}

TEST_F(TestSparseCOOTensor, CreationFromNumericTensor1D) {
  auto dense_data = Buffer::Wrap(this->dense_values_);
  std::vector<int64_t> dense_shape({static_cast<int64_t>(this->dense_values_.size())});
  NumericTensor<Int64Type> dense_vector(dense_data, dense_shape);

  std::shared_ptr<SparseCOOTensor> st;
  ASSERT_OK_AND_ASSIGN(st, SparseCOOTensor::Make(dense_vector));

  ASSERT_EQ(12, st->non_zero_length());
  ASSERT_TRUE(st->is_mutable());

  auto* raw_data = reinterpret_cast<const int64_t*>(st->raw_data());
  AssertNumericDataEqual(raw_data, {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16});

  auto si = internal::checked_pointer_cast<SparseCOOIndex>(st->sparse_index());
  ASSERT_TRUE(si->is_canonical());

  auto sidx = si->indices();
  ASSERT_EQ(std::vector<int64_t>({12, 1}), sidx->shape());

  AssertCOOIndex(sidx, 0, {0});
  AssertCOOIndex(sidx, 1, {2});
  AssertCOOIndex(sidx, 2, {5});
  AssertCOOIndex(sidx, 10, {21});
  AssertCOOIndex(sidx, 11, {23});
}

TEST_F(TestSparseCOOTensor, CreationFromTensor) {
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(this->dense_values_);
  Tensor tensor(int64(), buffer, this->shape_, {}, this->dim_names_);

  std::shared_ptr<SparseCOOTensor> st;
  ASSERT_OK_AND_ASSIGN(st, SparseCOOTensor::Make(tensor));

  ASSERT_EQ(12, st->non_zero_length());
  ASSERT_TRUE(st->is_mutable());

  ASSERT_EQ(std::vector<std::string>({"foo", "bar", "baz"}), st->dim_names());
  ASSERT_EQ("foo", st->dim_name(0));
  ASSERT_EQ("bar", st->dim_name(1));
  ASSERT_EQ("baz", st->dim_name(2));

  ASSERT_TRUE(st->Equals(*this->sparse_tensor_from_dense_));

  auto si = internal::checked_pointer_cast<SparseCOOIndex>(st->sparse_index());
  ASSERT_TRUE(si->is_canonical());
}

TEST_F(TestSparseCOOTensor, CreationFromNonContiguousTensor) {
  std::vector<int64_t> values = {1,  0, 0, 0, 2,  0, 0, 0, 0, 0, 3,  0, 0, 0, 4,  0,
                                 5,  0, 0, 0, 6,  0, 0, 0, 0, 0, 11, 0, 0, 0, 12, 0,
                                 13, 0, 0, 0, 14, 0, 0, 0, 0, 0, 15, 0, 0, 0, 16, 0};
  std::vector<int64_t> strides = {192, 64, 16};
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);
  Tensor tensor(int64(), buffer, this->shape_, strides);

  std::shared_ptr<SparseCOOTensor> st;
  ASSERT_OK_AND_ASSIGN(st, SparseCOOTensor::Make(tensor));

  ASSERT_EQ(12, st->non_zero_length());
  ASSERT_TRUE(st->is_mutable());

  ASSERT_TRUE(st->Equals(*this->sparse_tensor_from_dense_));

  auto si = internal::checked_pointer_cast<SparseCOOIndex>(st->sparse_index());
  ASSERT_TRUE(si->is_canonical());
}

TEST_F(TestSparseCOOTensor, TestToTensor) {
  std::vector<int64_t> values = {1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
                                 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4};
  std::vector<int64_t> shape({4, 3, 2, 1});
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);
  Tensor tensor(int64(), buffer, shape, {}, this->dim_names_);

  std::shared_ptr<SparseCOOTensor> sparse_tensor;
  ASSERT_OK_AND_ASSIGN(sparse_tensor, SparseCOOTensor::Make(tensor));

  ASSERT_EQ(5, sparse_tensor->non_zero_length());
  ASSERT_TRUE(sparse_tensor->is_mutable());
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> dense_tensor, sparse_tensor->ToTensor());
  ASSERT_TRUE(tensor.Equals(*dense_tensor));
}

template <typename ValueType>
class TestSparseCOOTensorEquality : public TestSparseTensorBase<ValueType> {
 public:
  void SetUp() {
    shape_ = {2, 3, 4};
    values1_ = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
    values2_ = {1, 0, 2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                0, 0, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
    auto buffer1 = Buffer::Wrap(values1_);
    auto buffer2 = Buffer::Wrap(values2_);
    DCHECK_OK(NumericTensor<ValueType>::Make(buffer1, this->shape_).Value(&tensor1_));
    DCHECK_OK(NumericTensor<ValueType>::Make(buffer2, this->shape_).Value(&tensor2_));
  }

 protected:
  using TestSparseTensorBase<ValueType>::shape_;
  std::vector<typename ValueType::c_type> values1_;
  std::vector<typename ValueType::c_type> values2_;
  std::shared_ptr<NumericTensor<ValueType>> tensor1_;
  std::shared_ptr<NumericTensor<ValueType>> tensor2_;
};

template <typename ValueType>
class TestIntegerSparseCOOTensorEquality : public TestSparseCOOTensorEquality<ValueType> {
};

TYPED_TEST_SUITE_P(TestIntegerSparseCOOTensorEquality);

TYPED_TEST_P(TestIntegerSparseCOOTensorEquality, TestEquality) {
  using ValueType = TypeParam;
  static_assert(is_integer_type<ValueType>::value, "Integer type is required");

  std::shared_ptr<SparseCOOTensor> st1, st2, st3;
  ASSERT_OK_AND_ASSIGN(st1, SparseCOOTensor::Make(*this->tensor1_));
  ASSERT_OK_AND_ASSIGN(st2, SparseCOOTensor::Make(*this->tensor2_));
  ASSERT_OK_AND_ASSIGN(st3, SparseCOOTensor::Make(*this->tensor1_));

  ASSERT_TRUE(st1->Equals(*st1));
  ASSERT_FALSE(st1->Equals(*st2));
  ASSERT_TRUE(st1->Equals(*st3));
}

REGISTER_TYPED_TEST_SUITE_P(TestIntegerSparseCOOTensorEquality, TestEquality);

INSTANTIATE_TYPED_TEST_SUITE_P(TestInt8, TestIntegerSparseCOOTensorEquality, Int8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt8, TestIntegerSparseCOOTensorEquality, UInt8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt16, TestIntegerSparseCOOTensorEquality, Int16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt16, TestIntegerSparseCOOTensorEquality,
                               UInt16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt32, TestIntegerSparseCOOTensorEquality, Int32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt32, TestIntegerSparseCOOTensorEquality,
                               UInt32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt64, TestIntegerSparseCOOTensorEquality, Int64Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt64, TestIntegerSparseCOOTensorEquality,
                               UInt64Type);

template <typename ValueType>
class TestFloatingSparseCOOTensorEquality
    : public TestSparseCOOTensorEquality<ValueType> {};

TYPED_TEST_SUITE_P(TestFloatingSparseCOOTensorEquality);

TYPED_TEST_P(TestFloatingSparseCOOTensorEquality, TestEquality) {
  using ValueType = TypeParam;
  using c_value_type = typename ValueType::c_type;
  static_assert(is_floating_type<ValueType>::value, "Float type is required");

  std::shared_ptr<SparseCOOTensor> st1, st2, st3;
  ASSERT_OK_AND_ASSIGN(st1, SparseCOOTensor::Make(*this->tensor1_));
  ASSERT_OK_AND_ASSIGN(st2, SparseCOOTensor::Make(*this->tensor2_));
  ASSERT_OK_AND_ASSIGN(st3, SparseCOOTensor::Make(*this->tensor1_));

  ASSERT_TRUE(st1->Equals(*st1));
  ASSERT_FALSE(st1->Equals(*st2));
  ASSERT_TRUE(st1->Equals(*st3));

  // sparse tensors with NaNs
  const c_value_type nan_value = static_cast<c_value_type>(NAN);
  this->values2_[13] = nan_value;
  EXPECT_TRUE(std::isnan(this->tensor2_->Value({1, 0, 1})));

  std::shared_ptr<SparseCOOTensor> st4;
  ASSERT_OK_AND_ASSIGN(st4, SparseCOOTensor::Make(*this->tensor2_));
  EXPECT_FALSE(st4->Equals(*st4));                                  // same object
  EXPECT_TRUE(st4->Equals(*st4, EqualOptions().nans_equal(true)));  // same object

  std::vector<c_value_type> values5 = this->values2_;
  std::shared_ptr<SparseCOOTensor> st5;
  std::shared_ptr<Buffer> buffer5 = Buffer::Wrap(values5);
  NumericTensor<ValueType> tensor5(buffer5, this->shape_);
  ASSERT_OK_AND_ASSIGN(st5, SparseCOOTensor::Make(tensor5));
  EXPECT_FALSE(st4->Equals(*st5));                                  // different memory
  EXPECT_TRUE(st4->Equals(*st5, EqualOptions().nans_equal(true)));  // different memory
}

REGISTER_TYPED_TEST_SUITE_P(TestFloatingSparseCOOTensorEquality, TestEquality);

INSTANTIATE_TYPED_TEST_SUITE_P(TestFloat, TestFloatingSparseCOOTensorEquality, FloatType);
INSTANTIATE_TYPED_TEST_SUITE_P(TestDouble, TestFloatingSparseCOOTensorEquality,
                               DoubleType);

template <typename IndexValueType>
class TestSparseCOOTensorForIndexValueType
    : public TestSparseCOOTensorBase<IndexValueType> {
 public:
  using c_index_value_type = typename IndexValueType::c_type;

  void SetUp() override {
    TestSparseCOOTensorBase<IndexValueType>::SetUp();

    // Sparse representation:
    // idx[0] = [0 0 0 0 0 0  1  1  1  1  1  1]
    // idx[1] = [0 0 1 1 2 2  0  0  1  1  2  2]
    // idx[2] = [0 2 1 3 0 2  1  3  0  2  1  3]
    // data   = [1 2 3 4 5 6 11 12 13 14 15 16]

    coords_values_row_major_ = {0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 1, 3, 0, 2, 0, 0, 2, 2,
                                1, 0, 1, 1, 0, 3, 1, 1, 0, 1, 1, 2, 1, 2, 1, 1, 2, 3};

    coords_values_col_major_ = {0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 2, 2,
                                0, 0, 1, 1, 2, 2, 0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3};
  }

  std::shared_ptr<DataType> index_data_type() const {
    return TypeTraits<IndexValueType>::type_singleton();
  }

 protected:
  std::vector<c_index_value_type> coords_values_row_major_;
  std::vector<c_index_value_type> coords_values_col_major_;

  Result<std::shared_ptr<SparseCOOIndex>> MakeSparseCOOIndex(
      const std::vector<int64_t>& shape, const std::vector<int64_t>& strides,
      const std::vector<c_index_value_type>& values) const {
    return SparseCOOIndex::Make(index_data_type(), shape, strides, Buffer::Wrap(values));
  }

  template <typename CValueType>
  Result<std::shared_ptr<SparseCOOTensor>> MakeSparseTensor(
      const std::shared_ptr<SparseCOOIndex>& si,
      std::vector<CValueType>& sparse_values) const {
    auto data = Buffer::Wrap(sparse_values);
    return SparseCOOTensor::Make(si, CTypeTraits<CValueType>::type_singleton(), data,
                                 this->shape_, this->dim_names_);
  }
};

TYPED_TEST_SUITE_P(TestSparseCOOTensorForIndexValueType);

TYPED_TEST_P(TestSparseCOOTensorForIndexValueType, Make) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  constexpr int sizeof_index_value = sizeof(c_index_value_type);
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<SparseCOOIndex> si,
      this->MakeSparseCOOIndex({12, 3}, {sizeof_index_value * 3, sizeof_index_value},
                               this->coords_values_row_major_));

  std::vector<int64_t> sparse_values = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16};
  auto sparse_data = Buffer::Wrap(sparse_values);

  std::shared_ptr<SparseCOOTensor> st;

  // OK
  ASSERT_OK_AND_ASSIGN(st, SparseCOOTensor::Make(si, int64(), sparse_data, this->shape_,
                                                 this->dim_names_));
  ASSERT_EQ(int64(), st->type());
  ASSERT_EQ(this->shape_, st->shape());
  ASSERT_EQ(this->dim_names_, st->dim_names());
  ASSERT_EQ(sparse_data->data(), st->raw_data());
  ASSERT_TRUE(
      internal::checked_pointer_cast<SparseCOOIndex>(st->sparse_index())->Equals(*si));

  // OK with an empty dim_names
  ASSERT_OK_AND_ASSIGN(st,
                       SparseCOOTensor::Make(si, int64(), sparse_data, this->shape_, {}));
  ASSERT_EQ(int64(), st->type());
  ASSERT_EQ(this->shape_, st->shape());
  ASSERT_EQ(std::vector<std::string>{}, st->dim_names());
  ASSERT_EQ(sparse_data->data(), st->raw_data());
  ASSERT_TRUE(
      internal::checked_pointer_cast<SparseCOOIndex>(st->sparse_index())->Equals(*si));

  // invalid data type
  auto res = SparseCOOTensor::Make(si, binary(), sparse_data, this->shape_, {});
  ASSERT_RAISES(Invalid, res);

  // negative items in shape
  res = SparseCOOTensor::Make(si, int64(), sparse_data, {2, -3, 4}, {});
  ASSERT_RAISES(Invalid, res);

  // sparse index and ndim (shape length) are inconsistent
  res = SparseCOOTensor::Make(si, int64(), sparse_data, {6, 4}, {});
  ASSERT_RAISES(Invalid, res);

  // shape and dim_names are inconsistent
  res = SparseCOOTensor::Make(si, int64(), sparse_data, this->shape_,
                              std::vector<std::string>{"foo"});
  ASSERT_RAISES(Invalid, res);
}

TYPED_TEST_P(TestSparseCOOTensorForIndexValueType, CreationWithRowMajorIndex) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  constexpr int sizeof_index_value = sizeof(c_index_value_type);
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<SparseCOOIndex> si,
      this->MakeSparseCOOIndex({12, 3}, {sizeof_index_value * 3, sizeof_index_value},
                               this->coords_values_row_major_));

  std::vector<int64_t> sparse_values = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16};
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<SparseCOOTensor> st,
                       this->MakeSparseTensor(si, sparse_values));

  ASSERT_EQ(std::vector<std::string>({"foo", "bar", "baz"}), st->dim_names());
  ASSERT_EQ("foo", st->dim_name(0));
  ASSERT_EQ("bar", st->dim_name(1));
  ASSERT_EQ("baz", st->dim_name(2));

  ASSERT_TRUE(st->Equals(*this->sparse_tensor_from_dense_));
}

TYPED_TEST_P(TestSparseCOOTensorForIndexValueType, CreationWithColumnMajorIndex) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  constexpr int sizeof_index_value = sizeof(c_index_value_type);
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<SparseCOOIndex> si,
      this->MakeSparseCOOIndex({12, 3}, {sizeof_index_value, sizeof_index_value * 12},
                               this->coords_values_col_major_));

  std::vector<int64_t> sparse_values = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16};
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<SparseCOOTensor> st,
                       this->MakeSparseTensor(si, sparse_values));

  ASSERT_EQ(std::vector<std::string>({"foo", "bar", "baz"}), st->dim_names());
  ASSERT_EQ("foo", st->dim_name(0));
  ASSERT_EQ("bar", st->dim_name(1));
  ASSERT_EQ("baz", st->dim_name(2));

  ASSERT_TRUE(st->Equals(*this->sparse_tensor_from_dense_));
}

TYPED_TEST_P(TestSparseCOOTensorForIndexValueType,
             EqualityBetweenRowAndColumnMajorIndices) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  // Row-major COO index
  const std::vector<int64_t> coords_shape = {12, 3};
  constexpr int sizeof_index_value = sizeof(c_index_value_type);
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<SparseCOOIndex> si_row_major,
      this->MakeSparseCOOIndex(coords_shape, {sizeof_index_value * 3, sizeof_index_value},
                               this->coords_values_row_major_));

  // Column-major COO index
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<SparseCOOIndex> si_col_major,
                       this->MakeSparseCOOIndex(
                           coords_shape, {sizeof_index_value, sizeof_index_value * 12},
                           this->coords_values_col_major_));

  std::vector<int64_t> sparse_values_1 = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16};
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<SparseCOOTensor> st1,
                       this->MakeSparseTensor(si_row_major, sparse_values_1));

  std::vector<int64_t> sparse_values_2 = sparse_values_1;
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<SparseCOOTensor> st2,
                       this->MakeSparseTensor(si_row_major, sparse_values_2));

  ASSERT_TRUE(st2->Equals(*st1));
}

REGISTER_TYPED_TEST_SUITE_P(TestSparseCOOTensorForIndexValueType, Make,
                            CreationWithRowMajorIndex, CreationWithColumnMajorIndex,
                            EqualityBetweenRowAndColumnMajorIndices);

INSTANTIATE_TYPED_TEST_SUITE_P(TestInt8, TestSparseCOOTensorForIndexValueType, Int8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt8, TestSparseCOOTensorForIndexValueType,
                               UInt8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt16, TestSparseCOOTensorForIndexValueType,
                               Int16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt16, TestSparseCOOTensorForIndexValueType,
                               UInt16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt32, TestSparseCOOTensorForIndexValueType,
                               Int32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt32, TestSparseCOOTensorForIndexValueType,
                               UInt32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt64, TestSparseCOOTensorForIndexValueType,
                               Int64Type);

TEST(TestSparseCOOTensorForUInt64Index, Make) {
  std::vector<int64_t> dense_values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                       0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  Tensor dense_tensor(uint64(), Buffer::Wrap(dense_values), {2, 3, 4});
  ASSERT_RAISES(Invalid, SparseCOOTensor::Make(dense_tensor, uint64()));
}

template <typename IndexValueType>
class TestSparseCSRMatrixBase : public TestSparseTensorBase<Int64Type> {
 public:
  void SetUp() {
    shape_ = {6, 4};
    dim_names_ = {"foo", "bar"};

    // Dense representation:
    // [
    //    1  0  2  0
    //    0  3  0  4
    //    5  0  6  0
    //    0 11  0 12
    //   13  0 14  0
    //    0 15  0 16
    // ]
    dense_values_ = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                     0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
    auto dense_data = Buffer::Wrap(dense_values_);
    NumericTensor<Int64Type> dense_tensor(dense_data, shape_, {}, dim_names_);
    ASSERT_OK_AND_ASSIGN(sparse_tensor_from_dense_,
                         SparseCSRMatrix::Make(
                             dense_tensor, TypeTraits<IndexValueType>::type_singleton()));
  }

 protected:
  std::vector<int64_t> dense_values_;
  std::shared_ptr<SparseCSRMatrix> sparse_tensor_from_dense_;
};

class TestSparseCSRMatrix : public TestSparseCSRMatrixBase<Int64Type> {};

TEST_F(TestSparseCSRMatrix, CreationFromZeroTensor) {
  const auto dense_size =
      std::accumulate(this->shape_.begin(), this->shape_.end(), int64_t(1),
                      [](int64_t a, int64_t x) { return a * x; });
  std::vector<int64_t> dense_values(dense_size, 0);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> t_zero,
                       Tensor::Make(int64(), Buffer::Wrap(dense_values), this->shape_));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<SparseCSRMatrix> st_zero,
                       SparseCSRMatrix::Make(*t_zero, int64()));

  ASSERT_EQ(0, st_zero->non_zero_length());
  ASSERT_EQ(dense_size, st_zero->size());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> t, st_zero->ToTensor());
  ASSERT_TRUE(t->Equals(*t_zero));
}

TEST_F(TestSparseCSRMatrix, CreationFromNumericTensor2D) {
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(this->dense_values_);
  NumericTensor<Int64Type> tensor(buffer, this->shape_);

  std::shared_ptr<SparseCSRMatrix> st1;
  ASSERT_OK_AND_ASSIGN(st1, SparseCSRMatrix::Make(tensor));

  auto st2 = this->sparse_tensor_from_dense_;

  CheckSparseIndexFormatType(SparseTensorFormat::CSR, *st1);

  ASSERT_EQ(12, st1->non_zero_length());
  ASSERT_TRUE(st1->is_mutable());

  ASSERT_EQ(std::vector<std::string>({"foo", "bar"}), st2->dim_names());
  ASSERT_EQ("foo", st2->dim_name(0));
  ASSERT_EQ("bar", st2->dim_name(1));

  ASSERT_EQ(std::vector<std::string>({}), st1->dim_names());
  ASSERT_EQ("", st1->dim_name(0));
  ASSERT_EQ("", st1->dim_name(1));
  ASSERT_EQ("", st1->dim_name(2));

  const int64_t* raw_data = reinterpret_cast<const int64_t*>(st1->raw_data());
  AssertNumericDataEqual(raw_data, {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16});

  auto si = internal::checked_pointer_cast<SparseCSRIndex>(st1->sparse_index());
  ASSERT_EQ(std::string("SparseCSRIndex"), si->ToString());
  ASSERT_EQ(1, si->indptr()->ndim());
  ASSERT_EQ(1, si->indices()->ndim());

  const int64_t* indptr_begin =
      reinterpret_cast<const int64_t*>(si->indptr()->raw_data());
  std::vector<int64_t> indptr_values(indptr_begin,
                                     indptr_begin + si->indptr()->shape()[0]);

  ASSERT_EQ(7, indptr_values.size());
  ASSERT_EQ(std::vector<int64_t>({0, 2, 4, 6, 8, 10, 12}), indptr_values);

  const int64_t* indices_begin =
      reinterpret_cast<const int64_t*>(si->indices()->raw_data());
  std::vector<int64_t> indices_values(indices_begin,
                                      indices_begin + si->indices()->shape()[0]);

  ASSERT_EQ(12, indices_values.size());
  ASSERT_EQ(std::vector<int64_t>({0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3}), indices_values);
}

TEST_F(TestSparseCSRMatrix, CreationFromNonContiguousTensor) {
  std::vector<int64_t> values = {1,  0, 0, 0, 2,  0, 0, 0, 0, 0, 3,  0, 0, 0, 4,  0,
                                 5,  0, 0, 0, 6,  0, 0, 0, 0, 0, 11, 0, 0, 0, 12, 0,
                                 13, 0, 0, 0, 14, 0, 0, 0, 0, 0, 15, 0, 0, 0, 16, 0};
  std::vector<int64_t> strides = {64, 16};
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);
  Tensor tensor(int64(), buffer, this->shape_, strides);

  std::shared_ptr<SparseCSRMatrix> st;
  ASSERT_OK_AND_ASSIGN(st, SparseCSRMatrix::Make(tensor));

  ASSERT_EQ(12, st->non_zero_length());
  ASSERT_TRUE(st->is_mutable());

  const int64_t* raw_data = reinterpret_cast<const int64_t*>(st->raw_data());
  AssertNumericDataEqual(raw_data, {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16});

  auto si = internal::checked_pointer_cast<SparseCSRIndex>(st->sparse_index());
  ASSERT_EQ(1, si->indptr()->ndim());
  ASSERT_EQ(1, si->indices()->ndim());

  const int64_t* indptr_begin =
      reinterpret_cast<const int64_t*>(si->indptr()->raw_data());
  std::vector<int64_t> indptr_values(indptr_begin,
                                     indptr_begin + si->indptr()->shape()[0]);

  ASSERT_EQ(7, indptr_values.size());
  ASSERT_EQ(std::vector<int64_t>({0, 2, 4, 6, 8, 10, 12}), indptr_values);

  const int64_t* indices_begin =
      reinterpret_cast<const int64_t*>(si->indices()->raw_data());
  std::vector<int64_t> indices_values(indices_begin,
                                      indices_begin + si->indices()->shape()[0]);

  ASSERT_EQ(12, indices_values.size());
  ASSERT_EQ(std::vector<int64_t>({0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3}), indices_values);

  ASSERT_TRUE(st->Equals(*this->sparse_tensor_from_dense_));
}

TEST_F(TestSparseCSRMatrix, TestToTensor) {
  std::vector<int64_t> values = {1, 0, 0, 0, 0, 0, 2, 1, 0, 0, 0, 1,
                                 0, 2, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1};
  std::vector<int64_t> shape({6, 4});
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);
  Tensor tensor(int64(), buffer, shape, {}, this->dim_names_);

  std::shared_ptr<SparseCSRMatrix> sparse_tensor;
  ASSERT_OK_AND_ASSIGN(sparse_tensor, SparseCSRMatrix::Make(tensor));

  ASSERT_EQ(7, sparse_tensor->non_zero_length());
  ASSERT_TRUE(sparse_tensor->is_mutable());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> dense_tensor, sparse_tensor->ToTensor());
  ASSERT_TRUE(tensor.Equals(*dense_tensor));
}

template <typename ValueType>
class TestSparseCSRMatrixEquality : public TestSparseTensorBase<ValueType> {
 public:
  void SetUp() {
    shape_ = {6, 4};
    values1_ = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
    values2_ = {9, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
    auto buffer1 = Buffer::Wrap(values1_);
    auto buffer2 = Buffer::Wrap(values2_);
    DCHECK_OK(NumericTensor<ValueType>::Make(buffer1, this->shape_).Value(&tensor1_));
    DCHECK_OK(NumericTensor<ValueType>::Make(buffer2, this->shape_).Value(&tensor2_));
  }

 protected:
  using TestSparseTensorBase<ValueType>::shape_;
  std::vector<typename ValueType::c_type> values1_;
  std::vector<typename ValueType::c_type> values2_;
  std::shared_ptr<NumericTensor<ValueType>> tensor1_;
  std::shared_ptr<NumericTensor<ValueType>> tensor2_;
};

template <typename ValueType>
class TestIntegerSparseCSRMatrixEquality : public TestSparseCSRMatrixEquality<ValueType> {
};

TYPED_TEST_SUITE_P(TestIntegerSparseCSRMatrixEquality);

TYPED_TEST_P(TestIntegerSparseCSRMatrixEquality, TestEquality) {
  using ValueType = TypeParam;
  static_assert(is_integer_type<ValueType>::value, "Integer type is required");

  std::shared_ptr<SparseCSRMatrix> st1, st2, st3;
  ASSERT_OK_AND_ASSIGN(st1, SparseCSRMatrix::Make(*this->tensor1_));
  ASSERT_OK_AND_ASSIGN(st2, SparseCSRMatrix::Make(*this->tensor2_));
  ASSERT_OK_AND_ASSIGN(st3, SparseCSRMatrix::Make(*this->tensor1_));

  ASSERT_TRUE(st1->Equals(*st1));
  ASSERT_FALSE(st1->Equals(*st2));
  ASSERT_TRUE(st1->Equals(*st3));
}

REGISTER_TYPED_TEST_SUITE_P(TestIntegerSparseCSRMatrixEquality, TestEquality);

INSTANTIATE_TYPED_TEST_SUITE_P(TestInt8, TestIntegerSparseCSRMatrixEquality, Int8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt8, TestIntegerSparseCSRMatrixEquality, UInt8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt16, TestIntegerSparseCSRMatrixEquality, Int16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt16, TestIntegerSparseCSRMatrixEquality,
                               UInt16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt32, TestIntegerSparseCSRMatrixEquality, Int32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt32, TestIntegerSparseCSRMatrixEquality,
                               UInt32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt64, TestIntegerSparseCSRMatrixEquality, Int64Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt64, TestIntegerSparseCSRMatrixEquality,
                               UInt64Type);

template <typename ValueType>
class TestFloatingSparseCSRMatrixEquality
    : public TestSparseCSRMatrixEquality<ValueType> {};

TYPED_TEST_SUITE_P(TestFloatingSparseCSRMatrixEquality);

TYPED_TEST_P(TestFloatingSparseCSRMatrixEquality, TestEquality) {
  using ValueType = TypeParam;
  using c_value_type = typename ValueType::c_type;
  static_assert(is_floating_type<ValueType>::value, "Float type is required");

  std::shared_ptr<SparseCSRMatrix> st1, st2, st3;
  ASSERT_OK_AND_ASSIGN(st1, SparseCSRMatrix::Make(*this->tensor1_));
  ASSERT_OK_AND_ASSIGN(st2, SparseCSRMatrix::Make(*this->tensor2_));
  ASSERT_OK_AND_ASSIGN(st3, SparseCSRMatrix::Make(*this->tensor1_));

  ASSERT_TRUE(st1->Equals(*st1));
  ASSERT_FALSE(st1->Equals(*st2));
  ASSERT_TRUE(st1->Equals(*st3));

  // sparse tensors with NaNs
  const c_value_type nan_value = static_cast<c_value_type>(NAN);
  this->values2_[13] = nan_value;
  EXPECT_TRUE(std::isnan(this->tensor2_->Value({3, 1})));

  std::shared_ptr<SparseCSRMatrix> st4;
  ASSERT_OK_AND_ASSIGN(st4, SparseCSRMatrix::Make(*this->tensor2_));
  EXPECT_FALSE(st4->Equals(*st4));                                  // same object
  EXPECT_TRUE(st4->Equals(*st4, EqualOptions().nans_equal(true)));  // same object

  std::vector<c_value_type> values5 = this->values2_;
  std::shared_ptr<SparseCSRMatrix> st5;
  std::shared_ptr<Buffer> buffer5 = Buffer::Wrap(values5);
  NumericTensor<ValueType> tensor5(buffer5, this->shape_);
  ASSERT_OK_AND_ASSIGN(st5, SparseCSRMatrix::Make(tensor5));
  EXPECT_FALSE(st4->Equals(*st5));                                  // different memory
  EXPECT_TRUE(st4->Equals(*st5, EqualOptions().nans_equal(true)));  // different memory
}

REGISTER_TYPED_TEST_SUITE_P(TestFloatingSparseCSRMatrixEquality, TestEquality);

INSTANTIATE_TYPED_TEST_SUITE_P(TestFloat, TestFloatingSparseCSRMatrixEquality, FloatType);
INSTANTIATE_TYPED_TEST_SUITE_P(TestDouble, TestFloatingSparseCSRMatrixEquality,
                               DoubleType);

template <typename IndexValueType>
class TestSparseCSRMatrixForIndexValueType
    : public TestSparseCSRMatrixBase<IndexValueType> {};

TYPED_TEST_SUITE_P(TestSparseCSRMatrixForIndexValueType);

TYPED_TEST_P(TestSparseCSRMatrixForIndexValueType, Make) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  // Sparse representation:
  std::vector<c_index_value_type> indptr_values = {0, 2, 4, 6, 8, 10, 12};
  std::vector<c_index_value_type> indices_values = {0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3};

  std::vector<int64_t> indptr_shape = {7};
  std::vector<int64_t> indices_shape = {12};

  std::shared_ptr<SparseCSRIndex> si;
  ASSERT_OK_AND_ASSIGN(
      si, SparseCSRIndex::Make(TypeTraits<IndexValueType>::type_singleton(), indptr_shape,
                               indices_shape, Buffer::Wrap(indptr_values),
                               Buffer::Wrap(indices_values)));

  std::vector<int64_t> sparse_values = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16};
  auto sparse_data = Buffer::Wrap(sparse_values);

  std::shared_ptr<SparseCSRMatrix> sm;

  // OK
  ASSERT_OK(
      SparseCSRMatrix::Make(si, int64(), sparse_data, this->shape_, this->dim_names_));

  // OK with an empty dim_names
  ASSERT_OK(SparseCSRMatrix::Make(si, int64(), sparse_data, this->shape_, {}));

  // invalid data type
  ASSERT_RAISES(Invalid,
                SparseCSRMatrix::Make(si, binary(), sparse_data, this->shape_, {}));

  // empty shape
  ASSERT_RAISES(Invalid, SparseCSRMatrix::Make(si, int64(), sparse_data, {}, {}));

  // 1D shape
  ASSERT_RAISES(Invalid, SparseCSRMatrix::Make(si, int64(), sparse_data, {24}, {}));

  // negative items in shape
  ASSERT_RAISES(Invalid, SparseCSRMatrix::Make(si, int64(), sparse_data, {6, -4}, {}));

  // sparse index and ndim (shape length) are inconsistent
  ASSERT_RAISES(Invalid, SparseCSRMatrix::Make(si, int64(), sparse_data, {4, 6}, {}));

  // shape and dim_names are inconsistent
  ASSERT_RAISES(Invalid, SparseCSRMatrix::Make(si, int64(), sparse_data, this->shape_,
                                               std::vector<std::string>{"foo"}));
}

REGISTER_TYPED_TEST_SUITE_P(TestSparseCSRMatrixForIndexValueType, Make);

INSTANTIATE_TYPED_TEST_SUITE_P(TestInt8, TestSparseCSRMatrixForIndexValueType, Int8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt8, TestSparseCSRMatrixForIndexValueType,
                               UInt8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt16, TestSparseCSRMatrixForIndexValueType,
                               Int16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt16, TestSparseCSRMatrixForIndexValueType,
                               UInt16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt32, TestSparseCSRMatrixForIndexValueType,
                               Int32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt32, TestSparseCSRMatrixForIndexValueType,
                               UInt32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt64, TestSparseCSRMatrixForIndexValueType,
                               Int64Type);

TEST(TestSparseCSRMatrixForUInt64Index, Make) {
  std::vector<int64_t> dense_values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                       0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  Tensor dense_tensor(uint64(), Buffer::Wrap(dense_values), {6, 4});
  ASSERT_RAISES(Invalid, SparseCSRMatrix::Make(dense_tensor, uint64()));
}

template <typename IndexValueType>
class TestSparseCSCMatrixBase : public TestSparseTensorBase<Int64Type> {
 public:
  void SetUp() {
    shape_ = {6, 4};
    dim_names_ = {"foo", "bar"};

    // Dense representation:
    // [
    //    1  0  2  0
    //    0  3  0  4
    //    5  0  6  0
    //    0 11  0 12
    //   13  0 14  0
    //    0 15  0 16
    // ]
    dense_values_ = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                     0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
    auto dense_data = Buffer::Wrap(dense_values_);
    NumericTensor<Int64Type> dense_tensor(dense_data, shape_, {}, dim_names_);
    ASSERT_OK_AND_ASSIGN(sparse_tensor_from_dense_,
                         SparseCSCMatrix::Make(
                             dense_tensor, TypeTraits<IndexValueType>::type_singleton()));
  }

 protected:
  std::vector<int64_t> dense_values_;
  std::shared_ptr<SparseCSCMatrix> sparse_tensor_from_dense_;
};

class TestSparseCSCMatrix : public TestSparseCSCMatrixBase<Int64Type> {};

TEST_F(TestSparseCSCMatrix, CreationFromZeroTensor) {
  const auto dense_size =
      std::accumulate(this->shape_.begin(), this->shape_.end(), int64_t(1),
                      [](int64_t a, int64_t x) { return a * x; });
  std::vector<int64_t> dense_values(dense_size, 0);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> t_zero,
                       Tensor::Make(int64(), Buffer::Wrap(dense_values), this->shape_));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<SparseCSCMatrix> st_zero,
                       SparseCSCMatrix::Make(*t_zero, int64()));

  ASSERT_EQ(0, st_zero->non_zero_length());
  ASSERT_EQ(dense_size, st_zero->size());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> t, st_zero->ToTensor());
  ASSERT_TRUE(t->Equals(*t_zero));
}

TEST_F(TestSparseCSCMatrix, CreationFromNumericTensor2D) {
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(this->dense_values_);
  NumericTensor<Int64Type> tensor(buffer, this->shape_);

  std::shared_ptr<SparseCSCMatrix> st1;
  ASSERT_OK_AND_ASSIGN(st1, SparseCSCMatrix::Make(tensor));

  auto st2 = this->sparse_tensor_from_dense_;

  CheckSparseIndexFormatType(SparseTensorFormat::CSC, *st1);

  ASSERT_EQ(12, st1->non_zero_length());
  ASSERT_TRUE(st1->is_mutable());

  ASSERT_EQ(std::vector<std::string>({"foo", "bar"}), st2->dim_names());
  ASSERT_EQ("foo", st2->dim_name(0));
  ASSERT_EQ("bar", st2->dim_name(1));

  ASSERT_EQ(std::vector<std::string>({}), st1->dim_names());
  ASSERT_EQ("", st1->dim_name(0));
  ASSERT_EQ("", st1->dim_name(1));
  ASSERT_EQ("", st1->dim_name(2));

  const int64_t* raw_data = reinterpret_cast<const int64_t*>(st1->raw_data());
  AssertNumericDataEqual(raw_data, {1, 5, 13, 3, 11, 15, 2, 6, 14, 4, 12, 16});

  auto si = internal::checked_pointer_cast<SparseCSCIndex>(st1->sparse_index());
  ASSERT_EQ(std::string("SparseCSCIndex"), si->ToString());
  ASSERT_EQ(1, si->indptr()->ndim());
  ASSERT_EQ(1, si->indices()->ndim());

  const int64_t* indptr_begin =
      reinterpret_cast<const int64_t*>(si->indptr()->raw_data());
  std::vector<int64_t> indptr_values(indptr_begin,
                                     indptr_begin + si->indptr()->shape()[0]);

  ASSERT_EQ(5, indptr_values.size());
  ASSERT_EQ(std::vector<int64_t>({0, 3, 6, 9, 12}), indptr_values);

  const int64_t* indices_begin =
      reinterpret_cast<const int64_t*>(si->indices()->raw_data());
  std::vector<int64_t> indices_values(indices_begin,
                                      indices_begin + si->indices()->shape()[0]);

  ASSERT_EQ(12, indices_values.size());
  ASSERT_EQ(std::vector<int64_t>({0, 2, 4, 1, 3, 5, 0, 2, 4, 1, 3, 5}), indices_values);
}

TEST_F(TestSparseCSCMatrix, CreationFromNonContiguousTensor) {
  std::vector<int64_t> values = {1,  0, 0, 0, 2,  0, 0, 0, 0, 0, 3,  0, 0, 0, 4,  0,
                                 5,  0, 0, 0, 6,  0, 0, 0, 0, 0, 11, 0, 0, 0, 12, 0,
                                 13, 0, 0, 0, 14, 0, 0, 0, 0, 0, 15, 0, 0, 0, 16, 0};
  std::vector<int64_t> strides = {64, 16};
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);
  Tensor tensor(int64(), buffer, this->shape_, strides);

  std::shared_ptr<SparseCSCMatrix> st;
  ASSERT_OK_AND_ASSIGN(st, SparseCSCMatrix::Make(tensor));

  ASSERT_EQ(12, st->non_zero_length());
  ASSERT_TRUE(st->is_mutable());

  const int64_t* raw_data = reinterpret_cast<const int64_t*>(st->raw_data());
  AssertNumericDataEqual(raw_data, {1, 5, 13, 3, 11, 15, 2, 6, 14, 4, 12, 16});

  auto si = internal::checked_pointer_cast<SparseCSCIndex>(st->sparse_index());
  ASSERT_EQ(1, si->indptr()->ndim());
  ASSERT_EQ(1, si->indices()->ndim());

  const int64_t* indptr_begin =
      reinterpret_cast<const int64_t*>(si->indptr()->raw_data());
  std::vector<int64_t> indptr_values(indptr_begin,
                                     indptr_begin + si->indptr()->shape()[0]);

  ASSERT_EQ(5, indptr_values.size());
  ASSERT_EQ(std::vector<int64_t>({0, 3, 6, 9, 12}), indptr_values);

  const int64_t* indices_begin =
      reinterpret_cast<const int64_t*>(si->indices()->raw_data());
  std::vector<int64_t> indices_values(indices_begin,
                                      indices_begin + si->indices()->shape()[0]);

  ASSERT_EQ(12, indices_values.size());
  ASSERT_EQ(std::vector<int64_t>({0, 2, 4, 1, 3, 5, 0, 2, 4, 1, 3, 5}), indices_values);

  ASSERT_TRUE(st->Equals(*this->sparse_tensor_from_dense_));
}

TEST_F(TestSparseCSCMatrix, TestToTensor) {
  std::vector<int64_t> values = {1, 0, 0, 0, 0, 0, 2, 1, 0, 0, 0, 1,
                                 0, 2, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1};
  std::vector<int64_t> shape({6, 4});
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);
  Tensor tensor(int64(), buffer, shape, {}, this->dim_names_);

  std::shared_ptr<SparseCSCMatrix> sparse_tensor;
  ASSERT_OK_AND_ASSIGN(sparse_tensor, SparseCSCMatrix::Make(tensor));

  ASSERT_EQ(7, sparse_tensor->non_zero_length());
  ASSERT_TRUE(sparse_tensor->is_mutable());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> dense_tensor, sparse_tensor->ToTensor());
  ASSERT_TRUE(tensor.Equals(*dense_tensor));
}

template <typename ValueType>
class TestSparseCSCMatrixEquality : public TestSparseTensorBase<ValueType> {
 public:
  void SetUp() {
    shape_ = {6, 4};
    values1_ = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
    values2_ = {9, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
    auto buffer1 = Buffer::Wrap(values1_);
    auto buffer2 = Buffer::Wrap(values2_);
    DCHECK_OK(NumericTensor<ValueType>::Make(buffer1, shape_).Value(&tensor1_));
    DCHECK_OK(NumericTensor<ValueType>::Make(buffer2, shape_).Value(&tensor2_));
  }

 protected:
  using TestSparseTensorBase<ValueType>::shape_;
  std::vector<typename ValueType::c_type> values1_;
  std::vector<typename ValueType::c_type> values2_;
  std::shared_ptr<NumericTensor<ValueType>> tensor1_;
  std::shared_ptr<NumericTensor<ValueType>> tensor2_;
};

template <typename ValueType>
class TestIntegerSparseCSCMatrixEquality : public TestSparseCSCMatrixEquality<ValueType> {
};

TYPED_TEST_SUITE_P(TestIntegerSparseCSCMatrixEquality);

TYPED_TEST_P(TestIntegerSparseCSCMatrixEquality, TestEquality) {
  using ValueType = TypeParam;
  static_assert(is_integer_type<ValueType>::value, "Integer type is required");

  std::shared_ptr<SparseCSCMatrix> st1, st2, st3;
  ASSERT_OK_AND_ASSIGN(st1, SparseCSCMatrix::Make(*this->tensor1_));
  ASSERT_OK_AND_ASSIGN(st2, SparseCSCMatrix::Make(*this->tensor2_));
  ASSERT_OK_AND_ASSIGN(st3, SparseCSCMatrix::Make(*this->tensor1_));

  ASSERT_TRUE(st1->Equals(*st1));
  ASSERT_FALSE(st1->Equals(*st2));
  ASSERT_TRUE(st1->Equals(*st3));
}

REGISTER_TYPED_TEST_SUITE_P(TestIntegerSparseCSCMatrixEquality, TestEquality);

INSTANTIATE_TYPED_TEST_SUITE_P(TestInt8, TestIntegerSparseCSCMatrixEquality, Int8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt8, TestIntegerSparseCSCMatrixEquality, UInt8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt16, TestIntegerSparseCSCMatrixEquality, Int16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt16, TestIntegerSparseCSCMatrixEquality,
                               UInt16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt32, TestIntegerSparseCSCMatrixEquality, Int32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt32, TestIntegerSparseCSCMatrixEquality,
                               UInt32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt64, TestIntegerSparseCSCMatrixEquality, Int64Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt64, TestIntegerSparseCSCMatrixEquality,
                               UInt64Type);

template <typename ValueType>
class TestFloatingSparseCSCMatrixEquality
    : public TestSparseCSCMatrixEquality<ValueType> {};

TYPED_TEST_SUITE_P(TestFloatingSparseCSCMatrixEquality);

TYPED_TEST_P(TestFloatingSparseCSCMatrixEquality, TestEquality) {
  using ValueType = TypeParam;
  using c_value_type = typename ValueType::c_type;
  static_assert(is_floating_type<ValueType>::value, "Float type is required");

  std::shared_ptr<SparseCSCMatrix> st1, st2, st3;
  ASSERT_OK_AND_ASSIGN(st1, SparseCSCMatrix::Make(*this->tensor1_));
  ASSERT_OK_AND_ASSIGN(st2, SparseCSCMatrix::Make(*this->tensor2_));
  ASSERT_OK_AND_ASSIGN(st3, SparseCSCMatrix::Make(*this->tensor1_));

  ASSERT_TRUE(st1->Equals(*st1));
  ASSERT_FALSE(st1->Equals(*st2));
  ASSERT_TRUE(st1->Equals(*st3));

  // sparse tensors with NaNs
  const c_value_type nan_value = static_cast<c_value_type>(NAN);
  this->values2_[13] = nan_value;
  EXPECT_TRUE(std::isnan(this->tensor2_->Value({3, 1})));

  std::shared_ptr<SparseCSCMatrix> st4;
  ASSERT_OK_AND_ASSIGN(st4, SparseCSCMatrix::Make(*this->tensor2_));
  EXPECT_FALSE(st4->Equals(*st4));                                  // same object
  EXPECT_TRUE(st4->Equals(*st4, EqualOptions().nans_equal(true)));  // same object

  std::vector<c_value_type> values5 = this->values2_;
  std::shared_ptr<SparseCSCMatrix> st5;
  std::shared_ptr<Buffer> buffer5 = Buffer::Wrap(values5);
  NumericTensor<ValueType> tensor5(buffer5, this->shape_);
  ASSERT_OK_AND_ASSIGN(st5, SparseCSCMatrix::Make(tensor5));
  EXPECT_FALSE(st4->Equals(*st5));                                  // different memory
  EXPECT_TRUE(st4->Equals(*st5, EqualOptions().nans_equal(true)));  // different memory
}

REGISTER_TYPED_TEST_SUITE_P(TestFloatingSparseCSCMatrixEquality, TestEquality);

INSTANTIATE_TYPED_TEST_SUITE_P(TestFloat, TestFloatingSparseCSCMatrixEquality, FloatType);
INSTANTIATE_TYPED_TEST_SUITE_P(TestDouble, TestFloatingSparseCSCMatrixEquality,
                               DoubleType);

template <typename ValueType>
class TestSparseCSFTensorEquality : public TestSparseTensorBase<ValueType> {
 public:
  void SetUp() {
    shape_ = {2, 3, 4, 5};

    values1_[0][0][0][1] = 1;
    values1_[0][0][0][2] = 2;
    values1_[0][1][0][0] = 3;
    values1_[0][1][0][2] = 4;
    values1_[0][1][1][0] = 5;
    values1_[1][1][1][0] = 6;
    values1_[1][1][1][1] = 7;
    values1_[1][1][1][2] = 8;

    length_ = sizeof(values1_);

    values2_[0][0][0][1] = 1;
    values2_[0][0][0][2] = 2;
    values2_[0][1][0][0] = 3;
    values2_[0][1][0][2] = 9;
    values2_[0][1][1][0] = 5;
    values2_[1][1][1][0] = 6;
    values2_[1][1][1][1] = 7;
    values2_[1][1][1][2] = 8;

    auto buffer1 = Buffer::Wrap(values1_, length_);
    auto buffer2 = Buffer::Wrap(values2_, length_);

    DCHECK_OK(NumericTensor<ValueType>::Make(buffer1, shape_).Value(&tensor1_));
    DCHECK_OK(NumericTensor<ValueType>::Make(buffer2, shape_).Value(&tensor2_));
  }

 protected:
  using TestSparseTensorBase<ValueType>::shape_;
  typename ValueType::c_type values1_[2][3][4][5] = {};
  typename ValueType::c_type values2_[2][3][4][5] = {};
  int64_t length_;
  std::shared_ptr<NumericTensor<ValueType>> tensor1_;
  std::shared_ptr<NumericTensor<ValueType>> tensor2_;
};

template <typename ValueType>
class TestIntegerSparseCSFTensorEquality : public TestSparseCSFTensorEquality<ValueType> {
};

TYPED_TEST_SUITE_P(TestIntegerSparseCSFTensorEquality);

TYPED_TEST_P(TestIntegerSparseCSFTensorEquality, TestEquality) {
  using ValueType = TypeParam;
  static_assert(is_integer_type<ValueType>::value, "Integer type is required");

  std::shared_ptr<SparseCSFTensor> st1, st2, st3;
  ASSERT_OK_AND_ASSIGN(st1, SparseCSFTensor::Make(*this->tensor1_));
  ASSERT_OK_AND_ASSIGN(st2, SparseCSFTensor::Make(*this->tensor2_));
  ASSERT_OK_AND_ASSIGN(st3, SparseCSFTensor::Make(*this->tensor1_));

  ASSERT_TRUE(st1->Equals(*st1));
  ASSERT_FALSE(st1->Equals(*st2));
  ASSERT_TRUE(st1->Equals(*st3));
}

REGISTER_TYPED_TEST_SUITE_P(TestIntegerSparseCSFTensorEquality, TestEquality);

INSTANTIATE_TYPED_TEST_SUITE_P(TestInt8, TestIntegerSparseCSFTensorEquality, Int8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt8, TestIntegerSparseCSFTensorEquality, UInt8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt16, TestIntegerSparseCSFTensorEquality, Int16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt16, TestIntegerSparseCSFTensorEquality,
                               UInt16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt32, TestIntegerSparseCSFTensorEquality, Int32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt32, TestIntegerSparseCSFTensorEquality,
                               UInt32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt64, TestIntegerSparseCSFTensorEquality, Int64Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt64, TestIntegerSparseCSFTensorEquality,
                               UInt64Type);

template <typename ValueType>
class TestFloatingSparseCSFTensorEquality
    : public TestSparseCSFTensorEquality<ValueType> {};

TYPED_TEST_SUITE_P(TestFloatingSparseCSFTensorEquality);

TYPED_TEST_P(TestFloatingSparseCSFTensorEquality, TestEquality) {
  using ValueType = TypeParam;
  using c_value_type = typename ValueType::c_type;
  static_assert(is_floating_type<ValueType>::value, "Floating type is required");

  std::shared_ptr<SparseCSFTensor> st1, st2, st3;
  ASSERT_OK_AND_ASSIGN(st1, SparseCSFTensor::Make(*this->tensor1_));
  ASSERT_OK_AND_ASSIGN(st2, SparseCSFTensor::Make(*this->tensor2_));
  ASSERT_OK_AND_ASSIGN(st3, SparseCSFTensor::Make(*this->tensor1_));

  ASSERT_TRUE(st1->Equals(*st1));
  ASSERT_FALSE(st1->Equals(*st2));
  ASSERT_TRUE(st1->Equals(*st3));

  // sparse tensors with NaNs
  const c_value_type nan_value = static_cast<c_value_type>(NAN);
  this->values2_[1][1][1][1] = nan_value;
  EXPECT_TRUE(std::isnan(this->tensor2_->Value({1, 1, 1, 1})));

  std::shared_ptr<SparseCSFTensor> st4;
  ASSERT_OK_AND_ASSIGN(st4, SparseCSFTensor::Make(*this->tensor2_));
  EXPECT_FALSE(st4->Equals(*st4));                                  // same object
  EXPECT_TRUE(st4->Equals(*st4, EqualOptions().nans_equal(true)));  // same object

  c_value_type values5[2][3][4][5] = {};
  std::copy_n(&this->values2_[0][0][0][0], this->length_ / sizeof(c_value_type),
              &values5[0][0][0][0]);
  std::shared_ptr<SparseCSFTensor> st5;
  std::shared_ptr<Buffer> buffer5 = Buffer::Wrap(values5, sizeof(values5));
  NumericTensor<ValueType> tensor5(buffer5, this->shape_);
  ASSERT_OK_AND_ASSIGN(st5, SparseCSFTensor::Make(tensor5));
  EXPECT_FALSE(st4->Equals(*st5));                                  // different memory
  EXPECT_TRUE(st4->Equals(*st5, EqualOptions().nans_equal(true)));  // different memory
}

REGISTER_TYPED_TEST_SUITE_P(TestFloatingSparseCSFTensorEquality, TestEquality);

INSTANTIATE_TYPED_TEST_SUITE_P(TestFloat, TestFloatingSparseCSFTensorEquality, FloatType);
INSTANTIATE_TYPED_TEST_SUITE_P(TestDouble, TestFloatingSparseCSFTensorEquality,
                               DoubleType);

template <typename IndexValueType>
class TestSparseCSFTensorBase : public TestSparseTensorBase<Int16Type> {
 public:
  void SetUp() {
    dim_names_ = {"a", "b", "c", "d"};
    shape_ = {2, 3, 4, 5};

    dense_values_[0][0][0][1] = 1;
    dense_values_[0][0][0][2] = 2;
    dense_values_[0][1][0][0] = 3;
    dense_values_[0][1][0][2] = 4;
    dense_values_[0][1][1][0] = 5;
    dense_values_[1][1][1][0] = 6;
    dense_values_[1][1][1][1] = 7;
    dense_values_[1][1][1][2] = 8;

    auto dense_buffer = Buffer::Wrap(dense_values_, sizeof(dense_values_));
    Tensor dense_tensor_(int16(), dense_buffer, shape_, {}, dim_names_);
    ASSERT_OK_AND_ASSIGN(
        sparse_tensor_from_dense_,
        SparseCSFTensor::Make(dense_tensor_,
                              TypeTraits<IndexValueType>::type_singleton()));
  }

 protected:
  std::vector<int64_t> shape_;
  std::vector<std::string> dim_names_;
  int16_t dense_values_[2][3][4][5] = {};
  std::shared_ptr<SparseCSFTensor> sparse_tensor_from_dense_;
};

class TestSparseCSFTensor : public TestSparseCSFTensorBase<Int64Type> {};

TEST_F(TestSparseCSFTensor, CreationFromZeroTensor) {
  const auto dense_size =
      std::accumulate(this->shape_.begin(), this->shape_.end(), int64_t(1),
                      [](int64_t a, int64_t x) { return a * x; });
  std::vector<int64_t> dense_values(dense_size, 0);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> t_zero,
                       Tensor::Make(int64(), Buffer::Wrap(dense_values), this->shape_));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<SparseCSFTensor> st_zero,
                       SparseCSFTensor::Make(*t_zero, int64()));

  ASSERT_EQ(0, st_zero->non_zero_length());
  ASSERT_EQ(dense_size, st_zero->size());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> t, st_zero->ToTensor());
  ASSERT_TRUE(t->Equals(*t_zero));
}

template <typename IndexValueType>
class TestSparseCSFTensorForIndexValueType
    : public TestSparseCSFTensorBase<IndexValueType> {
 protected:
  std::shared_ptr<SparseCSFIndex> MakeSparseCSFIndex(
      const std::vector<int64_t>& axis_order,
      const std::vector<std::vector<typename IndexValueType::c_type>>& indptr_values,
      const std::vector<std::vector<typename IndexValueType::c_type>>& indices_values)
      const {
    int64_t ndim = axis_order.size();
    std::vector<std::shared_ptr<Tensor>> indptr(ndim - 1);
    std::vector<std::shared_ptr<Tensor>> indices(ndim);

    for (int64_t i = 0; i < ndim - 1; ++i) {
      indptr[i] = std::make_shared<Tensor>(
          TypeTraits<IndexValueType>::type_singleton(), Buffer::Wrap(indptr_values[i]),
          std::vector<int64_t>({static_cast<int64_t>(indptr_values[i].size())}));
    }
    for (int64_t i = 0; i < ndim; ++i) {
      indices[i] = std::make_shared<Tensor>(
          TypeTraits<IndexValueType>::type_singleton(), Buffer::Wrap(indices_values[i]),
          std::vector<int64_t>({static_cast<int64_t>(indices_values[i].size())}));
    }
    return std::make_shared<SparseCSFIndex>(indptr, indices, axis_order);
  }

  template <typename CValueType>
  std::shared_ptr<SparseCSFTensor> MakeSparseTensor(
      const std::shared_ptr<SparseCSFIndex>& si, std::vector<CValueType>& sparse_values,
      const std::vector<int64_t>& shape,
      const std::vector<std::string>& dim_names) const {
    auto data_buffer = Buffer::Wrap(sparse_values);
    return std::make_shared<SparseCSFTensor>(
        si, CTypeTraits<CValueType>::type_singleton(), data_buffer, shape, dim_names);
  }
};

TYPED_TEST_SUITE_P(TestSparseCSFTensorForIndexValueType);

TYPED_TEST_P(TestSparseCSFTensorForIndexValueType, TestCreateSparseTensor) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  std::vector<int64_t> shape = {2, 3, 4, 5};
  std::vector<std::string> dim_names = {"a", "b", "c", "d"};
  std::vector<int64_t> axis_order = {0, 1, 2, 3};
  std::vector<int16_t> sparse_values = {1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<std::vector<c_index_value_type>> indptr_values = {
      {0, 2, 3}, {0, 1, 3, 4}, {0, 2, 4, 5, 8}};
  std::vector<std::vector<c_index_value_type>> indices_values = {
      {0, 1}, {0, 1, 1}, {0, 0, 1, 1}, {1, 2, 0, 2, 0, 0, 1, 2}};

  auto si = this->MakeSparseCSFIndex(axis_order, indptr_values, indices_values);
  auto st = this->MakeSparseTensor(si, sparse_values, shape, dim_names);

  ASSERT_TRUE(st->Equals(*this->sparse_tensor_from_dense_));
}

TYPED_TEST_P(TestSparseCSFTensorForIndexValueType, TestTensorToSparseTensor) {
  std::vector<std::string> dim_names = {"a", "b", "c", "d"};
  ASSERT_EQ(8, this->sparse_tensor_from_dense_->non_zero_length());
  ASSERT_TRUE(this->sparse_tensor_from_dense_->is_mutable());
  ASSERT_EQ(dim_names, this->sparse_tensor_from_dense_->dim_names());
}

TYPED_TEST_P(TestSparseCSFTensorForIndexValueType, TestSparseTensorToTensor) {
  std::vector<int64_t> shape = {2, 3, 4, 5};
  auto dense_buffer = Buffer::Wrap(this->dense_values_, sizeof(this->dense_values_));
  Tensor dense_tensor(int16(), dense_buffer, shape, {}, this->dim_names_);

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> dt,
                       this->sparse_tensor_from_dense_->ToTensor());
  ASSERT_TRUE(dense_tensor.Equals(*dt));
  ASSERT_EQ(dense_tensor.dim_names(), dt->dim_names());
}

TYPED_TEST_P(TestSparseCSFTensorForIndexValueType, TestRoundTrip) {
  using IndexValueType = TypeParam;

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> dt,
                       this->sparse_tensor_from_dense_->ToTensor());
  std::shared_ptr<SparseCSFTensor> st;
  ASSERT_OK_AND_ASSIGN(
      st, SparseCSFTensor::Make(*dt, TypeTraits<IndexValueType>::type_singleton()));

  ASSERT_TRUE(st->Equals(*this->sparse_tensor_from_dense_));
}

TYPED_TEST_P(TestSparseCSFTensorForIndexValueType, TestAlternativeAxisOrder) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  std::vector<int16_t> dense_values = {1, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 0,
                                       0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 5};
  std::vector<int64_t> shape = {4, 6};
  std::vector<std::string> dim_names = {"a", "b"};
  std::shared_ptr<Buffer> dense_buffer = Buffer::Wrap(dense_values);
  Tensor tensor(int16(), dense_buffer, shape, {}, dim_names);

  // Axis order 1
  std::vector<int64_t> axis_order_1 = {0, 1};
  std::vector<int16_t> sparse_values_1 = {1, 3, 2, 4, 5};
  std::vector<std::vector<c_index_value_type>> indptr_values_1 = {{0, 2, 3, 5}};
  std::vector<std::vector<c_index_value_type>> indices_values_1 = {{0, 1, 3},
                                                                   {0, 3, 1, 3, 5}};
  auto si_1 = this->MakeSparseCSFIndex(axis_order_1, indptr_values_1, indices_values_1);
  auto st_1 = this->MakeSparseTensor(si_1, sparse_values_1, shape, dim_names);

  // Axis order 2
  std::vector<int64_t> axis_order_2 = {1, 0};
  std::vector<int16_t> sparse_values_2 = {1, 2, 3, 4, 5};
  std::vector<std::vector<c_index_value_type>> indptr_values_2 = {{0, 1, 2, 4, 5}};
  std::vector<std::vector<c_index_value_type>> indices_values_2 = {{0, 1, 3, 5},
                                                                   {0, 1, 0, 3, 3}};
  auto si_2 = this->MakeSparseCSFIndex(axis_order_2, indptr_values_2, indices_values_2);
  auto st_2 = this->MakeSparseTensor(si_2, sparse_values_2, shape, dim_names);

  std::shared_ptr<Tensor> dt_1, dt_2;
  ASSERT_OK_AND_ASSIGN(dt_1, st_1->ToTensor());
  ASSERT_OK_AND_ASSIGN(dt_2, st_2->ToTensor());

  ASSERT_FALSE(st_1->Equals(*st_2));
  ASSERT_TRUE(dt_1->Equals(*dt_2));
  ASSERT_TRUE(dt_1->Equals(tensor));
}

TYPED_TEST_P(TestSparseCSFTensorForIndexValueType, TestNonAscendingShape) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  std::vector<int64_t> shape = {5, 2, 3, 4};
  int16_t dense_values[5][2][3][4] = {};  // zero-initialized
  dense_values[0][0][0][1] = 1;
  dense_values[0][0][0][2] = 2;
  dense_values[0][1][0][0] = 3;
  dense_values[0][1][0][2] = 4;
  dense_values[0][1][1][0] = 5;
  dense_values[1][1][1][0] = 6;
  dense_values[1][1][1][1] = 7;
  dense_values[1][1][1][2] = 8;
  auto dense_buffer = Buffer::Wrap(dense_values, sizeof(dense_values));
  Tensor dense_tensor(int16(), dense_buffer, shape, {}, this->dim_names_);

  std::shared_ptr<SparseCSFTensor> sparse_tensor;
  ASSERT_OK_AND_ASSIGN(
      sparse_tensor,
      SparseCSFTensor::Make(dense_tensor, TypeTraits<IndexValueType>::type_singleton()));

  std::vector<std::vector<c_index_value_type>> indptr_values = {
      {0, 1, 3}, {0, 2, 4, 7}, {0, 1, 2, 3, 4, 6, 7, 8}};
  std::vector<std::vector<c_index_value_type>> indices_values = {
      {0, 1}, {0, 0, 1}, {1, 2, 0, 2, 0, 1, 2}, {0, 0, 0, 0, 0, 1, 1, 1}};
  std::vector<int64_t> axis_order = {1, 2, 3, 0};
  std::vector<int16_t> sparse_values = {1, 2, 3, 4, 5, 6, 7, 8};
  auto si = this->MakeSparseCSFIndex(axis_order, indptr_values, indices_values);
  auto st = this->MakeSparseTensor(si, sparse_values, shape, this->dim_names_);

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Tensor> dt, st->ToTensor());
  ASSERT_TRUE(dt->Equals(dense_tensor));
  ASSERT_TRUE(st->Equals(*sparse_tensor));
}

REGISTER_TYPED_TEST_SUITE_P(TestSparseCSFTensorForIndexValueType, TestCreateSparseTensor,
                            TestTensorToSparseTensor, TestSparseTensorToTensor,
                            TestAlternativeAxisOrder, TestNonAscendingShape,
                            TestRoundTrip);

INSTANTIATE_TYPED_TEST_SUITE_P(TestInt8, TestSparseCSFTensorForIndexValueType, Int8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt8, TestSparseCSFTensorForIndexValueType,
                               UInt8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt16, TestSparseCSFTensorForIndexValueType,
                               Int16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt16, TestSparseCSFTensorForIndexValueType,
                               UInt16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt32, TestSparseCSFTensorForIndexValueType,
                               Int32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt32, TestSparseCSFTensorForIndexValueType,
                               UInt32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt64, TestSparseCSFTensorForIndexValueType,
                               Int64Type);

TEST(TestSparseCSFMatrixForUInt64Index, Make) {
  int16_t dense_values[2][3][4][5] = {};
  dense_values[0][0][0][1] = 1;
  dense_values[0][0][0][2] = 2;
  dense_values[0][1][0][0] = 3;
  dense_values[0][1][0][2] = 4;
  dense_values[0][1][1][0] = 5;
  dense_values[1][1][1][0] = 6;
  dense_values[1][1][1][1] = 7;
  dense_values[1][1][1][2] = 8;

  Tensor dense_tensor(uint64(), Buffer::Wrap(dense_values, sizeof(dense_values)),
                      {2, 3, 4, 5});
  ASSERT_RAISES(Invalid, SparseCSFTensor::Make(dense_tensor, uint64()));
}

}  // namespace arrow
