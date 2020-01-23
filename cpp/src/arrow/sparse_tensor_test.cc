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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <iostream>

#include <gtest/gtest.h>

#include "arrow/sparse_tensor.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

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

TEST(TestSparseCOOIndex, Make) {
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

template <typename IndexValueType>
class TestSparseCOOTensorBase : public ::testing::Test {
 public:
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
    std::vector<int64_t> dense_values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                         0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
    auto dense_data = Buffer::Wrap(dense_values);
    NumericTensor<Int64Type> dense_tensor(dense_data, shape_, {}, dim_names_);
    ASSERT_OK_AND_ASSIGN(sparse_tensor_from_dense_,
                         SparseCOOTensor::Make(
                             dense_tensor, TypeTraits<IndexValueType>::type_singleton()));
  }

 protected:
  std::vector<int64_t> shape_;
  std::vector<std::string> dim_names_;
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

TEST_F(TestSparseCOOTensor, CreationFromNumericTensor) {
  auto st = this->sparse_tensor_from_dense_;
  CheckSparseIndexFormatType(SparseTensorFormat::COO, *st);

  ASSERT_EQ(12, st->non_zero_length());
  ASSERT_TRUE(st->is_mutable());

  auto* raw_data = reinterpret_cast<const int64_t*>(st->raw_data());
  AssertNumericDataEqual(raw_data, {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16});

  auto si = internal::checked_pointer_cast<SparseCOOIndex>(st->sparse_index());
  ASSERT_EQ(std::string("SparseCOOIndex"), si->ToString());

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
  std::vector<int64_t> dense_values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                       0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  auto dense_data = Buffer::Wrap(dense_values);
  std::vector<int64_t> dense_shape({static_cast<int64_t>(dense_values.size())});
  NumericTensor<Int64Type> dense_vector(dense_data, dense_shape);

  std::shared_ptr<SparseCOOTensor> st;
  ASSERT_OK_AND_ASSIGN(st, SparseCOOTensor::Make(dense_vector));

  ASSERT_EQ(12, st->non_zero_length());
  ASSERT_TRUE(st->is_mutable());

  auto* raw_data = reinterpret_cast<const int64_t*>(st->raw_data());
  AssertNumericDataEqual(raw_data, {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16});

  auto si = internal::checked_pointer_cast<SparseCOOIndex>(st->sparse_index());
  auto sidx = si->indices();
  ASSERT_EQ(std::vector<int64_t>({12, 1}), sidx->shape());

  AssertCOOIndex(sidx, 0, {0});
  AssertCOOIndex(sidx, 1, {2});
  AssertCOOIndex(sidx, 2, {5});
  AssertCOOIndex(sidx, 10, {21});
  AssertCOOIndex(sidx, 11, {23});
}

TEST_F(TestSparseCOOTensor, CreationFromTensor) {
  std::vector<int64_t> values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                 0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);
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
}

TEST_F(TestSparseCOOTensor, TensorEquality) {
  std::vector<int64_t> values1 = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                  0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  std::vector<int64_t> values2 = {0, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                  0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  std::shared_ptr<Buffer> buffer1 = Buffer::Wrap(values1);
  std::shared_ptr<Buffer> buffer2 = Buffer::Wrap(values2);
  NumericTensor<Int64Type> tensor1(buffer1, this->shape_);
  NumericTensor<Int64Type> tensor2(buffer2, this->shape_);

  std::shared_ptr<SparseCOOTensor> st1;
  ASSERT_OK_AND_ASSIGN(st1, SparseCOOTensor::Make(tensor1));

  std::shared_ptr<SparseCOOTensor> st2;
  ASSERT_OK_AND_ASSIGN(st2, SparseCOOTensor::Make(tensor2));

  ASSERT_TRUE(st1->Equals(*this->sparse_tensor_from_dense_));
  ASSERT_FALSE(st1->Equals(*st2));
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
  std::shared_ptr<Tensor> dense_tensor;
  ASSERT_OK(sparse_tensor->ToTensor(&dense_tensor));
  ASSERT_TRUE(tensor.Equals(*dense_tensor));
}

template <typename IndexValueType>
class TestSparseCOOTensorForIndexValueType
    : public TestSparseCOOTensorBase<IndexValueType> {
 protected:
  std::shared_ptr<SparseCOOIndex> MakeSparseCOOIndex(
      const std::vector<int64_t>& coords_shape,
      const std::vector<int64_t>& coords_strides,
      std::vector<typename IndexValueType::c_type>& coords_values) const {
    auto coords_data = Buffer::Wrap(coords_values);
    auto coords = std::make_shared<NumericTensor<IndexValueType>>(
        coords_data, coords_shape, coords_strides);
    return std::make_shared<SparseCOOIndex>(coords);
  }

  template <typename CValueType>
  std::shared_ptr<SparseCOOTensor> MakeSparseTensor(
      const std::shared_ptr<SparseCOOIndex>& si,
      std::vector<CValueType>& sparse_values) const {
    auto data = Buffer::Wrap(sparse_values);
    return std::make_shared<SparseCOOTensor>(si,
                                             CTypeTraits<CValueType>::type_singleton(),
                                             data, this->shape_, this->dim_names_);
  }
};

TYPED_TEST_CASE_P(TestSparseCOOTensorForIndexValueType);

TYPED_TEST_P(TestSparseCOOTensorForIndexValueType, Make) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  // Sparse representation:
  // idx[0] = [0 0 0 0 0 0  1  1  1  1  1  1]
  // idx[1] = [0 0 1 1 2 2  0  0  1  1  2  2]
  // idx[2] = [0 2 1 3 0 2  1  3  0  2  1  3]
  // data   = [1 2 3 4 5 6 11 12 13 14 15 16]
  std::vector<c_index_value_type> coords_values = {0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 1, 3,
                                                   0, 2, 0, 0, 2, 2, 1, 0, 1, 1, 0, 3,
                                                   1, 1, 0, 1, 1, 2, 1, 2, 1, 1, 2, 3};
  constexpr int sizeof_index_value = sizeof(c_index_value_type);
  auto si = this->MakeSparseCOOIndex(
      {12, 3}, {sizeof_index_value * 3, sizeof_index_value}, coords_values);

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

  // Sparse representation:
  // idx[0] = [0 0 0 0 0 0  1  1  1  1  1  1]
  // idx[1] = [0 0 1 1 2 2  0  0  1  1  2  2]
  // idx[2] = [0 2 1 3 0 2  1  3  0  2  1  3]
  // data   = [1 2 3 4 5 6 11 12 13 14 15 16]
  std::vector<c_index_value_type> coords_values = {0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 1, 3,
                                                   0, 2, 0, 0, 2, 2, 1, 0, 1, 1, 0, 3,
                                                   1, 1, 0, 1, 1, 2, 1, 2, 1, 1, 2, 3};
  constexpr int sizeof_index_value = sizeof(c_index_value_type);
  auto si = this->MakeSparseCOOIndex(
      {12, 3}, {sizeof_index_value * 3, sizeof_index_value}, coords_values);

  std::vector<int64_t> sparse_values = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16};
  auto st = this->MakeSparseTensor(si, sparse_values);

  ASSERT_EQ(std::vector<std::string>({"foo", "bar", "baz"}), st->dim_names());
  ASSERT_EQ("foo", st->dim_name(0));
  ASSERT_EQ("bar", st->dim_name(1));
  ASSERT_EQ("baz", st->dim_name(2));

  ASSERT_TRUE(st->Equals(*this->sparse_tensor_from_dense_));
}

TYPED_TEST_P(TestSparseCOOTensorForIndexValueType, CreationWithColumnMajorIndex) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  // Sparse representation:
  // idx[0] = [0 0 0 0 0 0  1  1  1  1  1  1]
  // idx[1] = [0 0 1 1 2 2  0  0  1  1  2  2]
  // idx[2] = [0 2 1 3 0 2  1  3  0  2  1  3]
  // data   = [1 2 3 4 5 6 11 12 13 14 15 16]
  std::vector<c_index_value_type> coords_values = {0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1,
                                                   0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2,
                                                   0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3};
  constexpr int sizeof_index_value = sizeof(c_index_value_type);
  auto si = this->MakeSparseCOOIndex(
      {12, 3}, {sizeof_index_value, sizeof_index_value * 12}, coords_values);

  std::vector<int64_t> sparse_values = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16};
  auto st = this->MakeSparseTensor(si, sparse_values);

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

  // Sparse representation:
  // idx[0] = [0 0 0 0 0 0  1  1  1  1  1  1]
  // idx[1] = [0 0 1 1 2 2  0  0  1  1  2  2]
  // idx[2] = [0 2 1 3 0 2  1  3  0  2  1  3]
  // data   = [1 2 3 4 5 6 11 12 13 14 15 16]

  // Row-major COO index
  const std::vector<int64_t> coords_shape = {12, 3};
  constexpr int sizeof_index_value = sizeof(c_index_value_type);
  std::vector<c_index_value_type> coords_values_row_major = {
      0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 1, 3, 0, 2, 0, 0, 2, 2,
      1, 0, 1, 1, 0, 3, 1, 1, 0, 1, 1, 2, 1, 2, 1, 1, 2, 3};
  auto si_row_major =
      this->MakeSparseCOOIndex(coords_shape, {sizeof_index_value * 3, sizeof_index_value},
                               coords_values_row_major);

  // Column-major COO index
  std::vector<c_index_value_type> coords_values_col_major = {
      0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 2, 2,
      0, 0, 1, 1, 2, 2, 0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3};
  auto si_col_major = this->MakeSparseCOOIndex(
      coords_shape, {sizeof_index_value, sizeof_index_value * 12},
      coords_values_col_major);

  std::vector<int64_t> sparse_values_1 = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16};
  auto st1 = this->MakeSparseTensor(si_row_major, sparse_values_1);

  std::vector<int64_t> sparse_values_2 = sparse_values_1;
  auto st2 = this->MakeSparseTensor(si_row_major, sparse_values_2);

  ASSERT_TRUE(st2->Equals(*st1));
}

REGISTER_TYPED_TEST_CASE_P(TestSparseCOOTensorForIndexValueType, Make,
                           CreationWithRowMajorIndex, CreationWithColumnMajorIndex,
                           EqualityBetweenRowAndColumnMajorIndices);

INSTANTIATE_TYPED_TEST_CASE_P(TestInt8, TestSparseCOOTensorForIndexValueType, Int8Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt8, TestSparseCOOTensorForIndexValueType, UInt8Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestInt16, TestSparseCOOTensorForIndexValueType, Int16Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt16, TestSparseCOOTensorForIndexValueType,
                              UInt16Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestInt32, TestSparseCOOTensorForIndexValueType, Int32Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt32, TestSparseCOOTensorForIndexValueType,
                              UInt32Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestInt64, TestSparseCOOTensorForIndexValueType, Int64Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt64, TestSparseCOOTensorForIndexValueType,
                              UInt64Type);

template <typename IndexValueType>
class TestSparseCSRMatrixBase : public ::testing::Test {
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
    std::vector<int64_t> dense_values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                         0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
    auto dense_data = Buffer::Wrap(dense_values);
    NumericTensor<Int64Type> dense_tensor(dense_data, shape_, {}, dim_names_);
    ASSERT_OK_AND_ASSIGN(sparse_tensor_from_dense_,
                         SparseCSRMatrix::Make(
                             dense_tensor, TypeTraits<IndexValueType>::type_singleton()));
  }

 protected:
  std::vector<int64_t> shape_;
  std::vector<std::string> dim_names_;
  std::shared_ptr<SparseCSRMatrix> sparse_tensor_from_dense_;
};

class TestSparseCSRMatrix : public TestSparseCSRMatrixBase<Int64Type> {};

TEST_F(TestSparseCSRMatrix, CreationFromNumericTensor2D) {
  std::vector<int64_t> values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                 0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);
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

TEST_F(TestSparseCSRMatrix, TensorEquality) {
  std::vector<int64_t> values1 = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                  0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  std::vector<int64_t> values2 = {9, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                  0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  std::shared_ptr<Buffer> buffer1 = Buffer::Wrap(values1);
  std::shared_ptr<Buffer> buffer2 = Buffer::Wrap(values2);
  NumericTensor<Int64Type> tensor1(buffer1, this->shape_);
  NumericTensor<Int64Type> tensor2(buffer2, this->shape_);

  std::shared_ptr<SparseCSRMatrix> st1, st2;
  ASSERT_OK_AND_ASSIGN(st1, SparseCSRMatrix::Make(tensor1));
  ASSERT_OK_AND_ASSIGN(st2, SparseCSRMatrix::Make(tensor2));

  ASSERT_TRUE(st1->Equals(*this->sparse_tensor_from_dense_));
  ASSERT_FALSE(st1->Equals(*st2));
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

  std::shared_ptr<Tensor> dense_tensor;
  ASSERT_OK(sparse_tensor->ToTensor(&dense_tensor));
  ASSERT_TRUE(tensor.Equals(*dense_tensor));
}

template <typename IndexValueType>
class TestSparseCSRMatrixForIndexValueType
    : public TestSparseCSRMatrixBase<IndexValueType> {};

TYPED_TEST_CASE_P(TestSparseCSRMatrixForIndexValueType);

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

REGISTER_TYPED_TEST_CASE_P(TestSparseCSRMatrixForIndexValueType, Make);

INSTANTIATE_TYPED_TEST_CASE_P(TestInt8, TestSparseCSRMatrixForIndexValueType, Int8Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt8, TestSparseCSRMatrixForIndexValueType, UInt8Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestInt16, TestSparseCSRMatrixForIndexValueType, Int16Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt16, TestSparseCSRMatrixForIndexValueType,
                              UInt16Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestInt32, TestSparseCSRMatrixForIndexValueType, Int32Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt32, TestSparseCSRMatrixForIndexValueType,
                              UInt32Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestInt64, TestSparseCSRMatrixForIndexValueType, Int64Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt64, TestSparseCSRMatrixForIndexValueType,
                              UInt64Type);

template <typename IndexValueType>
class TestSparseCSCMatrixBase : public ::testing::Test {
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
    std::vector<int64_t> dense_values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                         0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
    auto dense_data = Buffer::Wrap(dense_values);
    NumericTensor<Int64Type> dense_tensor(dense_data, shape_, {}, dim_names_);
    ASSERT_OK_AND_ASSIGN(sparse_tensor_from_dense_,
                         SparseCSCMatrix::Make(
                             dense_tensor, TypeTraits<IndexValueType>::type_singleton()));
  }

 protected:
  std::vector<int64_t> shape_;
  std::vector<std::string> dim_names_;
  std::shared_ptr<SparseCSCMatrix> sparse_tensor_from_dense_;
};

class TestSparseCSCMatrix : public TestSparseCSCMatrixBase<Int64Type> {};

TEST_F(TestSparseCSCMatrix, CreationFromNumericTensor2D) {
  std::vector<int64_t> values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                 0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);
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

TEST_F(TestSparseCSCMatrix, TensorEquality) {
  std::vector<int64_t> values1 = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                  0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  std::vector<int64_t> values2 = {9, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                  0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  std::shared_ptr<Buffer> buffer1 = Buffer::Wrap(values1);
  std::shared_ptr<Buffer> buffer2 = Buffer::Wrap(values2);
  NumericTensor<Int64Type> tensor1(buffer1, this->shape_);
  NumericTensor<Int64Type> tensor2(buffer2, this->shape_);

  std::shared_ptr<SparseCSCMatrix> st1, st2;
  ASSERT_OK_AND_ASSIGN(st1, SparseCSCMatrix::Make(tensor1));
  ASSERT_OK_AND_ASSIGN(st2, SparseCSCMatrix::Make(tensor2));

  ASSERT_TRUE(st1->Equals(*this->sparse_tensor_from_dense_));
  ASSERT_FALSE(st1->Equals(*st2));
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

  std::shared_ptr<Tensor> dense_tensor;
  ASSERT_OK(sparse_tensor->ToTensor(&dense_tensor));
  ASSERT_TRUE(tensor.Equals(*dense_tensor));
}

}  // namespace arrow
