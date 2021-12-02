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
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unordered_set>

#include <gtest/gtest.h>

#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/io/test_common.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/test_common.h"
#include "arrow/ipc/writer.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/io_util.h"

namespace arrow {

using internal::checked_cast;
using internal::GetByteWidth;
using internal::TemporaryDir;

namespace ipc {
namespace test {

class BaseTensorTest : public ::testing::Test, public io::MemoryMapFixture {
 public:
  void SetUp() { ASSERT_OK_AND_ASSIGN(temp_dir_, TemporaryDir::Make("ipc-test-")); }

  void TearDown() { io::MemoryMapFixture::TearDown(); }

 protected:
  std::shared_ptr<io::MemoryMappedFile> mmap_;
  std::unique_ptr<TemporaryDir> temp_dir_;
};

class TestTensorRoundTrip : public BaseTensorTest {
 public:
  void CheckTensorRoundTrip(const Tensor& tensor) {
    int32_t metadata_length;
    int64_t body_length;
    const int elem_size = GetByteWidth(*tensor.type());

    ASSERT_OK(mmap_->Seek(0));

    ASSERT_OK(WriteTensor(tensor, mmap_.get(), &metadata_length, &body_length));

    const int64_t expected_body_length = elem_size * tensor.size();
    ASSERT_EQ(expected_body_length, body_length);

    ASSERT_OK(mmap_->Seek(0));

    std::shared_ptr<Tensor> result;
    ASSERT_OK_AND_ASSIGN(result, ReadTensor(mmap_.get()));

    ASSERT_EQ(result->data()->size(), expected_body_length);
    ASSERT_TRUE(tensor.Equals(*result));
  }

 protected:
  std::shared_ptr<io::MemoryMappedFile> mmap_;
  std::unique_ptr<TemporaryDir> temp_dir_;
};

TEST_F(TestTensorRoundTrip, BasicRoundtrip) {
  std::string path = "test-write-tensor";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(mmap_, io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  std::vector<int64_t> shape = {4, 6};
  std::vector<int64_t> strides = {48, 8};
  std::vector<std::string> dim_names = {"foo", "bar"};
  int64_t size = 24;

  std::vector<int64_t> values;
  randint(size, 0, 100, &values);

  auto data = Buffer::Wrap(values);

  Tensor t0(int64(), data, shape, strides, dim_names);
  Tensor t_no_dims(int64(), data, {}, {}, {});
  Tensor t_zero_length_dim(int64(), data, {0}, {8}, {"foo"});

  CheckTensorRoundTrip(t0);
  CheckTensorRoundTrip(t_no_dims);
  CheckTensorRoundTrip(t_zero_length_dim);

  int64_t serialized_size;
  ASSERT_OK(GetTensorSize(t0, &serialized_size));
  ASSERT_TRUE(serialized_size > static_cast<int64_t>(size * sizeof(int64_t)));

  // ARROW-2840: Check that padding/alignment minded
  std::vector<int64_t> shape_2 = {1, 1};
  std::vector<int64_t> strides_2 = {8, 8};
  Tensor t0_not_multiple_64(int64(), data, shape_2, strides_2, dim_names);
  CheckTensorRoundTrip(t0_not_multiple_64);
}

TEST_F(TestTensorRoundTrip, NonContiguous) {
  std::string path = "test-write-tensor-strided";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(mmap_, io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  std::vector<int64_t> values;
  randint(24, 0, 100, &values);

  auto data = Buffer::Wrap(values);
  Tensor tensor(int64(), data, {4, 3}, {48, 16});

  CheckTensorRoundTrip(tensor);
}

template <typename IndexValueType>
class TestSparseTensorRoundTrip : public BaseTensorTest {
 public:
  void CheckSparseCOOTensorRoundTrip(const SparseCOOTensor& sparse_tensor) {
    const int elem_size = GetByteWidth(*sparse_tensor.type());
    const int index_elem_size = sizeof(typename IndexValueType::c_type);

    int32_t metadata_length;
    int64_t body_length;

    ASSERT_OK(mmap_->Seek(0));

    ASSERT_OK(
        WriteSparseTensor(sparse_tensor, mmap_.get(), &metadata_length, &body_length));

    const auto& sparse_index =
        checked_cast<const SparseCOOIndex&>(*sparse_tensor.sparse_index());
    const int64_t indices_length =
        bit_util::RoundUpToMultipleOf8(index_elem_size * sparse_index.indices()->size());
    const int64_t data_length =
        bit_util::RoundUpToMultipleOf8(elem_size * sparse_tensor.non_zero_length());
    const int64_t expected_body_length = indices_length + data_length;
    ASSERT_EQ(expected_body_length, body_length);

    ASSERT_OK(mmap_->Seek(0));

    std::shared_ptr<SparseTensor> result;
    ASSERT_OK_AND_ASSIGN(result, ReadSparseTensor(mmap_.get()));
    ASSERT_EQ(SparseTensorFormat::COO, result->format_id());

    const auto& resulted_sparse_index =
        checked_cast<const SparseCOOIndex&>(*result->sparse_index());
    ASSERT_EQ(resulted_sparse_index.indices()->data()->size(), indices_length);
    ASSERT_EQ(resulted_sparse_index.is_canonical(), sparse_index.is_canonical());
    ASSERT_EQ(result->data()->size(), data_length);
    ASSERT_TRUE(result->Equals(sparse_tensor));
  }

  template <typename SparseIndexType>
  void CheckSparseCSXMatrixRoundTrip(
      const SparseTensorImpl<SparseIndexType>& sparse_tensor) {
    static_assert(std::is_same<SparseIndexType, SparseCSRIndex>::value ||
                      std::is_same<SparseIndexType, SparseCSCIndex>::value,
                  "SparseIndexType must be either SparseCSRIndex or SparseCSCIndex");

    const int elem_size = GetByteWidth(*sparse_tensor.type());
    const int index_elem_size = sizeof(typename IndexValueType::c_type);

    int32_t metadata_length;
    int64_t body_length;

    ASSERT_OK(mmap_->Seek(0));

    ASSERT_OK(
        WriteSparseTensor(sparse_tensor, mmap_.get(), &metadata_length, &body_length));

    const auto& sparse_index =
        checked_cast<const SparseIndexType&>(*sparse_tensor.sparse_index());
    const int64_t indptr_length =
        bit_util::RoundUpToMultipleOf8(index_elem_size * sparse_index.indptr()->size());
    const int64_t indices_length =
        bit_util::RoundUpToMultipleOf8(index_elem_size * sparse_index.indices()->size());
    const int64_t data_length =
        bit_util::RoundUpToMultipleOf8(elem_size * sparse_tensor.non_zero_length());
    const int64_t expected_body_length = indptr_length + indices_length + data_length;
    ASSERT_EQ(expected_body_length, body_length);

    ASSERT_OK(mmap_->Seek(0));

    std::shared_ptr<SparseTensor> result;
    ASSERT_OK_AND_ASSIGN(result, ReadSparseTensor(mmap_.get()));

    constexpr auto expected_format_id =
        std::is_same<SparseIndexType, SparseCSRIndex>::value ? SparseTensorFormat::CSR
                                                             : SparseTensorFormat::CSC;
    ASSERT_EQ(expected_format_id, result->format_id());

    const auto& resulted_sparse_index =
        checked_cast<const SparseIndexType&>(*result->sparse_index());
    ASSERT_EQ(resulted_sparse_index.indptr()->data()->size(), indptr_length);
    ASSERT_EQ(resulted_sparse_index.indices()->data()->size(), indices_length);
    ASSERT_EQ(result->data()->size(), data_length);
    ASSERT_TRUE(result->Equals(sparse_tensor));
  }

  void CheckSparseCSFTensorRoundTrip(const SparseCSFTensor& sparse_tensor) {
    const int elem_size = GetByteWidth(*sparse_tensor.type());
    const int index_elem_size = sizeof(typename IndexValueType::c_type);

    int32_t metadata_length;
    int64_t body_length;

    ASSERT_OK(mmap_->Seek(0));

    ASSERT_OK(
        WriteSparseTensor(sparse_tensor, mmap_.get(), &metadata_length, &body_length));

    const auto& sparse_index =
        checked_cast<const SparseCSFIndex&>(*sparse_tensor.sparse_index());

    const int64_t ndim = sparse_index.axis_order().size();
    int64_t indptr_length = 0;
    int64_t indices_length = 0;

    for (int64_t i = 0; i < ndim - 1; ++i) {
      indptr_length += bit_util::RoundUpToMultipleOf8(index_elem_size *
                                                      sparse_index.indptr()[i]->size());
    }
    for (int64_t i = 0; i < ndim; ++i) {
      indices_length += bit_util::RoundUpToMultipleOf8(index_elem_size *
                                                       sparse_index.indices()[i]->size());
    }
    const int64_t data_length =
        bit_util::RoundUpToMultipleOf8(elem_size * sparse_tensor.non_zero_length());
    const int64_t expected_body_length = indptr_length + indices_length + data_length;
    ASSERT_EQ(expected_body_length, body_length);

    ASSERT_OK(mmap_->Seek(0));

    std::shared_ptr<SparseTensor> result;
    ASSERT_OK_AND_ASSIGN(result, ReadSparseTensor(mmap_.get()));
    ASSERT_EQ(SparseTensorFormat::CSF, result->format_id());

    const auto& resulted_sparse_index =
        checked_cast<const SparseCSFIndex&>(*result->sparse_index());

    int64_t out_indptr_length = 0;
    int64_t out_indices_length = 0;
    for (int i = 0; i < ndim - 1; ++i) {
      out_indptr_length += bit_util::RoundUpToMultipleOf8(
          index_elem_size * resulted_sparse_index.indptr()[i]->size());
    }
    for (int i = 0; i < ndim; ++i) {
      out_indices_length += bit_util::RoundUpToMultipleOf8(
          index_elem_size * resulted_sparse_index.indices()[i]->size());
    }

    ASSERT_EQ(out_indptr_length, indptr_length);
    ASSERT_EQ(out_indices_length, indices_length);
    ASSERT_EQ(result->data()->size(), data_length);
    ASSERT_TRUE(resulted_sparse_index.Equals(sparse_index));
    ASSERT_TRUE(result->Equals(sparse_tensor));
  }

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

  template <typename ValueType>
  Result<std::shared_ptr<SparseCOOTensor>> MakeSparseCOOTensor(
      const std::shared_ptr<SparseCOOIndex>& si, std::vector<ValueType>& sparse_values,
      const std::vector<int64_t>& shape,
      const std::vector<std::string>& dim_names = {}) const {
    auto data = Buffer::Wrap(sparse_values);
    return SparseCOOTensor::Make(si, CTypeTraits<ValueType>::type_singleton(), data,
                                 shape, dim_names);
  }
};

TYPED_TEST_SUITE_P(TestSparseTensorRoundTrip);

TYPED_TEST_P(TestSparseTensorRoundTrip, WithSparseCOOIndexRowMajor) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  std::string path = "test-write-sparse-coo-tensor";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(this->mmap_,
                       io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

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
  //
  // Sparse representation:
  // idx[0] = [0 0 0 0 0 0  1  1  1  1  1  1]
  // idx[1] = [0 0 1 1 2 2  0  0  1  1  2  2]
  // idx[2] = [0 2 1 3 0 2  1  3  0  2  1  3]
  // data   = [1 2 3 4 5 6 11 12 13 14 15 16]

  // canonical
  std::vector<c_index_value_type> coords_values = {0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 1, 3,
                                                   0, 2, 0, 0, 2, 2, 1, 0, 1, 1, 0, 3,
                                                   1, 1, 0, 1, 1, 2, 1, 2, 1, 1, 2, 3};
  const int sizeof_index_value = sizeof(c_index_value_type);
  std::shared_ptr<SparseCOOIndex> si;
  ASSERT_OK_AND_ASSIGN(
      si, SparseCOOIndex::Make(TypeTraits<IndexValueType>::type_singleton(), {12, 3},
                               {sizeof_index_value * 3, sizeof_index_value},
                               Buffer::Wrap(coords_values)));
  ASSERT_TRUE(si->is_canonical());

  std::vector<int64_t> shape = {2, 3, 4};
  std::vector<std::string> dim_names = {"foo", "bar", "baz"};
  std::vector<int64_t> values = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16};
  std::shared_ptr<SparseCOOTensor> st;
  ASSERT_OK_AND_ASSIGN(st, this->MakeSparseCOOTensor(si, values, shape, dim_names));

  this->CheckSparseCOOTensorRoundTrip(*st);

  // non-canonical
  ASSERT_OK_AND_ASSIGN(
      si, SparseCOOIndex::Make(TypeTraits<IndexValueType>::type_singleton(), {12, 3},
                               {sizeof_index_value * 3, sizeof_index_value},
                               Buffer::Wrap(coords_values), false));
  ASSERT_FALSE(si->is_canonical());
  ASSERT_OK_AND_ASSIGN(st, this->MakeSparseCOOTensor(si, values, shape, dim_names));

  this->CheckSparseCOOTensorRoundTrip(*st);
}

TYPED_TEST_P(TestSparseTensorRoundTrip, WithSparseCOOIndexColumnMajor) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  std::string path = "test-write-sparse-coo-tensor";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(this->mmap_,
                       io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

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
  //
  // Sparse representation:
  // idx[0] = [0 0 0 0 0 0  1  1  1  1  1  1]
  // idx[1] = [0 0 1 1 2 2  0  0  1  1  2  2]
  // idx[2] = [0 2 1 3 0 2  1  3  0  2  1  3]
  // data   = [1 2 3 4 5 6 11 12 13 14 15 16]

  // canonical
  std::vector<c_index_value_type> coords_values = {0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1,
                                                   0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2,
                                                   0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3};
  const int sizeof_index_value = sizeof(c_index_value_type);
  std::shared_ptr<SparseCOOIndex> si;
  ASSERT_OK_AND_ASSIGN(
      si, SparseCOOIndex::Make(TypeTraits<IndexValueType>::type_singleton(), {12, 3},
                               {sizeof_index_value, sizeof_index_value * 12},
                               Buffer::Wrap(coords_values)));
  ASSERT_TRUE(si->is_canonical());

  std::vector<int64_t> shape = {2, 3, 4};
  std::vector<std::string> dim_names = {"foo", "bar", "baz"};
  std::vector<int64_t> values = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16};

  std::shared_ptr<SparseCOOTensor> st;
  ASSERT_OK_AND_ASSIGN(st, this->MakeSparseCOOTensor(si, values, shape, dim_names));

  this->CheckSparseCOOTensorRoundTrip(*st);

  // non-canonical
  ASSERT_OK_AND_ASSIGN(
      si, SparseCOOIndex::Make(TypeTraits<IndexValueType>::type_singleton(), {12, 3},
                               {sizeof_index_value, sizeof_index_value * 12},
                               Buffer::Wrap(coords_values), false));
  ASSERT_FALSE(si->is_canonical());
  ASSERT_OK_AND_ASSIGN(st, this->MakeSparseCOOTensor(si, values, shape, dim_names));

  this->CheckSparseCOOTensorRoundTrip(*st);
}

TYPED_TEST_P(TestSparseTensorRoundTrip, WithSparseCSRIndex) {
  using IndexValueType = TypeParam;

  std::string path = "test-write-sparse-csr-matrix";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(this->mmap_,
                       io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  std::vector<int64_t> shape = {4, 6};
  std::vector<std::string> dim_names = {"foo", "bar", "baz"};
  std::vector<int64_t> values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                 0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};

  auto data = Buffer::Wrap(values);
  NumericTensor<Int64Type> t(data, shape, {}, dim_names);
  std::shared_ptr<SparseCSRMatrix> st;
  ASSERT_OK_AND_ASSIGN(
      st, SparseCSRMatrix::Make(t, TypeTraits<IndexValueType>::type_singleton()));

  this->CheckSparseCSXMatrixRoundTrip(*st);
}

TYPED_TEST_P(TestSparseTensorRoundTrip, WithSparseCSCIndex) {
  using IndexValueType = TypeParam;

  std::string path = "test-write-sparse-csc-matrix";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(this->mmap_,
                       io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  std::vector<int64_t> shape = {4, 6};
  std::vector<std::string> dim_names = {"foo", "bar", "baz"};
  std::vector<int64_t> values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                 0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};

  auto data = Buffer::Wrap(values);
  NumericTensor<Int64Type> t(data, shape, {}, dim_names);
  std::shared_ptr<SparseCSCMatrix> st;
  ASSERT_OK_AND_ASSIGN(
      st, SparseCSCMatrix::Make(t, TypeTraits<IndexValueType>::type_singleton()));

  this->CheckSparseCSXMatrixRoundTrip(*st);
}

TYPED_TEST_P(TestSparseTensorRoundTrip, WithSparseCSFIndex) {
  using IndexValueType = TypeParam;

  std::string path = "test-write-sparse-csf-tensor";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(this->mmap_,
                       io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  std::vector<int64_t> shape = {4, 6};
  std::vector<std::string> dim_names = {"foo", "bar", "baz"};
  std::vector<int64_t> values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                 0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};

  auto data = Buffer::Wrap(values);
  NumericTensor<Int64Type> t(data, shape, {}, dim_names);
  std::shared_ptr<SparseCSFTensor> st;
  ASSERT_OK_AND_ASSIGN(
      st, SparseCSFTensor::Make(t, TypeTraits<IndexValueType>::type_singleton()));

  this->CheckSparseCSFTensorRoundTrip(*st);
}
REGISTER_TYPED_TEST_SUITE_P(TestSparseTensorRoundTrip, WithSparseCOOIndexRowMajor,
                            WithSparseCOOIndexColumnMajor, WithSparseCSRIndex,
                            WithSparseCSCIndex, WithSparseCSFIndex);

INSTANTIATE_TYPED_TEST_SUITE_P(TestInt8, TestSparseTensorRoundTrip, Int8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt8, TestSparseTensorRoundTrip, UInt8Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt16, TestSparseTensorRoundTrip, Int16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt16, TestSparseTensorRoundTrip, UInt16Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt32, TestSparseTensorRoundTrip, Int32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestUInt32, TestSparseTensorRoundTrip, UInt32Type);
INSTANTIATE_TYPED_TEST_SUITE_P(TestInt64, TestSparseTensorRoundTrip, Int64Type);

}  // namespace test
}  // namespace ipc
}  // namespace arrow
