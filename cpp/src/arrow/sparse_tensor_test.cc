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
    sparse_tensor_from_dense_ = std::make_shared<SparseTensorCOO>(
        dense_tensor, TypeTraits<IndexValueType>::type_singleton());
  }

 protected:
  std::vector<int64_t> shape_;
  std::vector<std::string> dim_names_;
  std::shared_ptr<SparseTensorCOO> sparse_tensor_from_dense_;
};

class TestSparseCOOTensor : public TestSparseCOOTensorBase<Int64Type> {};

TEST_F(TestSparseCOOTensor, CreationEmptyTensor) {
  SparseTensorImpl<SparseCOOIndex> st1(int64(), this->shape_);
  SparseTensorImpl<SparseCOOIndex> st2(int64(), this->shape_, this->dim_names_);

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
  auto& st = *this->sparse_tensor_from_dense_;
  CheckSparseIndexFormatType(SparseTensorFormat::COO, st);

  ASSERT_EQ(12, st.non_zero_length());
  ASSERT_TRUE(st.is_mutable());

  auto* raw_data = reinterpret_cast<const int64_t*>(st.raw_data());
  AssertNumericDataEqual(raw_data, {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16});

  const auto& si = internal::checked_cast<const SparseCOOIndex&>(*st.sparse_index());
  ASSERT_EQ(std::string("SparseCOOIndex"), si.ToString());

  std::shared_ptr<Tensor> sidx = si.indices();
  ASSERT_EQ(std::vector<int64_t>({12, 3}), sidx->shape());
  ASSERT_TRUE(sidx->is_column_major());

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
  SparseTensorImpl<SparseCOOIndex> st(dense_vector);

  ASSERT_EQ(12, st.non_zero_length());
  ASSERT_TRUE(st.is_mutable());

  auto* raw_data = reinterpret_cast<const int64_t*>(st.raw_data());
  AssertNumericDataEqual(raw_data, {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16});

  const auto& si = internal::checked_cast<const SparseCOOIndex&>(*st.sparse_index());
  auto sidx = si.indices();
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
  SparseTensorImpl<SparseCOOIndex> st(tensor);

  ASSERT_EQ(12, st.non_zero_length());
  ASSERT_TRUE(st.is_mutable());

  ASSERT_EQ(std::vector<std::string>({"foo", "bar", "baz"}), st.dim_names());
  ASSERT_EQ("foo", st.dim_name(0));
  ASSERT_EQ("bar", st.dim_name(1));
  ASSERT_EQ("baz", st.dim_name(2));

  ASSERT_TRUE(st.Equals(*this->sparse_tensor_from_dense_));
}

TEST_F(TestSparseCOOTensor, CreationFromNonContiguousTensor) {
  std::vector<int64_t> values = {1,  0, 0, 0, 2,  0, 0, 0, 0, 0, 3,  0, 0, 0, 4,  0,
                                 5,  0, 0, 0, 6,  0, 0, 0, 0, 0, 11, 0, 0, 0, 12, 0,
                                 13, 0, 0, 0, 14, 0, 0, 0, 0, 0, 15, 0, 0, 0, 16, 0};
  std::vector<int64_t> strides = {192, 64, 16};
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);
  Tensor tensor(int64(), buffer, this->shape_, strides);
  SparseTensorImpl<SparseCOOIndex> st(tensor);

  ASSERT_EQ(12, st.non_zero_length());
  ASSERT_TRUE(st.is_mutable());

  ASSERT_TRUE(st.Equals(*this->sparse_tensor_from_dense_));
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
  SparseTensorImpl<SparseCOOIndex> st1(tensor1);
  SparseTensorImpl<SparseCOOIndex> st2(tensor2);

  ASSERT_TRUE(st1.Equals(*this->sparse_tensor_from_dense_));
  ASSERT_FALSE(st1.Equals(st2));
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
  std::shared_ptr<SparseTensorCOO> MakeSparseTensor(
      const std::shared_ptr<SparseCOOIndex>& si,
      std::vector<CValueType>& sparse_values) const {
    auto data = Buffer::Wrap(sparse_values);
    return std::make_shared<SparseTensorCOO>(si,
                                             CTypeTraits<CValueType>::type_singleton(),
                                             data, this->shape_, this->dim_names_);
  }
};

TYPED_TEST_CASE_P(TestSparseCOOTensorForIndexValueType);

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
  const int sizeof_index_value = sizeof(c_index_value_type);
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
  const int sizeof_index_value = sizeof(c_index_value_type);
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
  const int sizeof_index_value = sizeof(c_index_value_type);
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

REGISTER_TYPED_TEST_CASE_P(TestSparseCOOTensorForIndexValueType,
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
    sparse_tensor_from_dense_ = std::make_shared<SparseTensorCSR>(
        dense_tensor, TypeTraits<IndexValueType>::type_singleton());
  }

 protected:
  std::vector<int64_t> shape_;
  std::vector<std::string> dim_names_;
  std::shared_ptr<SparseTensorCSR> sparse_tensor_from_dense_;
};

class TestSparseCSRMatrix : public TestSparseCSRMatrixBase<Int64Type> {};

TEST_F(TestSparseCSRMatrix, CreationFromNumericTensor2D) {
  std::vector<int64_t> values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                 0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};
  std::shared_ptr<Buffer> buffer = Buffer::Wrap(values);
  NumericTensor<Int64Type> tensor(buffer, this->shape_);

  SparseTensorImpl<SparseCSRIndex> st1(tensor);
  SparseTensorImpl<SparseCSRIndex>& st2 = *this->sparse_tensor_from_dense_;

  CheckSparseIndexFormatType(SparseTensorFormat::CSR, st1);

  ASSERT_EQ(12, st1.non_zero_length());
  ASSERT_TRUE(st1.is_mutable());

  ASSERT_EQ(std::vector<std::string>({"foo", "bar"}), st2.dim_names());
  ASSERT_EQ("foo", st2.dim_name(0));
  ASSERT_EQ("bar", st2.dim_name(1));

  ASSERT_EQ(std::vector<std::string>({}), st1.dim_names());
  ASSERT_EQ("", st1.dim_name(0));
  ASSERT_EQ("", st1.dim_name(1));
  ASSERT_EQ("", st1.dim_name(2));

  const int64_t* raw_data = reinterpret_cast<const int64_t*>(st1.raw_data());
  AssertNumericDataEqual(raw_data, {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16});

  const auto& si = internal::checked_cast<const SparseCSRIndex&>(*st1.sparse_index());
  ASSERT_EQ(std::string("SparseCSRIndex"), si.ToString());
  ASSERT_EQ(1, si.indptr()->ndim());
  ASSERT_EQ(1, si.indices()->ndim());

  const int64_t* indptr_begin = reinterpret_cast<const int64_t*>(si.indptr()->raw_data());
  std::vector<int64_t> indptr_values(indptr_begin,
                                     indptr_begin + si.indptr()->shape()[0]);

  ASSERT_EQ(7, indptr_values.size());
  ASSERT_EQ(std::vector<int64_t>({0, 2, 4, 6, 8, 10, 12}), indptr_values);

  const int64_t* indices_begin =
      reinterpret_cast<const int64_t*>(si.indices()->raw_data());
  std::vector<int64_t> indices_values(indices_begin,
                                      indices_begin + si.indices()->shape()[0]);

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
  SparseTensorImpl<SparseCSRIndex> st(tensor);

  ASSERT_EQ(12, st.non_zero_length());
  ASSERT_TRUE(st.is_mutable());

  const int64_t* raw_data = reinterpret_cast<const int64_t*>(st.raw_data());
  AssertNumericDataEqual(raw_data, {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16});

  const auto& si = internal::checked_cast<const SparseCSRIndex&>(*st.sparse_index());
  ASSERT_EQ(1, si.indptr()->ndim());
  ASSERT_EQ(1, si.indices()->ndim());

  const int64_t* indptr_begin = reinterpret_cast<const int64_t*>(si.indptr()->raw_data());
  std::vector<int64_t> indptr_values(indptr_begin,
                                     indptr_begin + si.indptr()->shape()[0]);

  ASSERT_EQ(7, indptr_values.size());
  ASSERT_EQ(std::vector<int64_t>({0, 2, 4, 6, 8, 10, 12}), indptr_values);

  const int64_t* indices_begin =
      reinterpret_cast<const int64_t*>(si.indices()->raw_data());
  std::vector<int64_t> indices_values(indices_begin,
                                      indices_begin + si.indices()->shape()[0]);

  ASSERT_EQ(12, indices_values.size());
  ASSERT_EQ(std::vector<int64_t>({0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3}), indices_values);

  ASSERT_TRUE(st.Equals(*this->sparse_tensor_from_dense_));
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
  SparseTensorImpl<SparseCSRIndex> st1(tensor1);
  SparseTensorImpl<SparseCSRIndex> st2(tensor2);

  ASSERT_TRUE(st1.Equals(*this->sparse_tensor_from_dense_));
  ASSERT_FALSE(st1.Equals(st2));
}

}  // namespace arrow
