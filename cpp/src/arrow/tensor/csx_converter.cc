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

#include "arrow/tensor/converter.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class MemoryPool;

namespace internal {
namespace {

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCSRIndex

class SparseCSXMatrixConverter : private SparseTensorConverterMixin {
  using SparseTensorConverterMixin::AssignIndex;
  using SparseTensorConverterMixin::IsNonZero;

 public:
  SparseCSXMatrixConverter(SparseMatrixCompressedAxis axis, const Tensor& tensor,
                           const std::shared_ptr<DataType>& index_value_type,
                           MemoryPool* pool)
      : axis_(axis), tensor_(tensor), index_value_type_(index_value_type), pool_(pool) {}

  Status Convert() {
    RETURN_NOT_OK(::arrow::internal::CheckSparseIndexMaximumValue(index_value_type_,
                                                                  tensor_.shape()));

    const int index_elsize = GetByteWidth(*index_value_type_);
    const int value_elsize = GetByteWidth(*tensor_.type());

    const int64_t ndim = tensor_.ndim();
    if (ndim > 2) {
      return Status::Invalid("Invalid tensor dimension");
    }

    const int major_axis = static_cast<int>(axis_);
    const int64_t n_major = tensor_.shape()[major_axis];
    const int64_t n_minor = tensor_.shape()[1 - major_axis];
    ARROW_ASSIGN_OR_RAISE(int64_t nonzero_count, tensor_.CountNonZero());

    std::shared_ptr<Buffer> indptr_buffer;
    std::shared_ptr<Buffer> indices_buffer;

    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(value_elsize * nonzero_count, pool_));
    auto* values = values_buffer->mutable_data();

    const auto* tensor_data = tensor_.raw_data();

    if (ndim <= 1) {
      return Status::NotImplemented("TODO for ndim <= 1");
    } else {
      ARROW_ASSIGN_OR_RAISE(indptr_buffer,
                            AllocateBuffer(index_elsize * (n_major + 1), pool_));
      auto* indptr = indptr_buffer->mutable_data();

      ARROW_ASSIGN_OR_RAISE(indices_buffer,
                            AllocateBuffer(index_elsize * nonzero_count, pool_));
      auto* indices = indices_buffer->mutable_data();

      std::vector<int64_t> coords(2);
      int64_t k = 0;
      std::fill_n(indptr, index_elsize, 0);
      indptr += index_elsize;
      for (int64_t i = 0; i < n_major; ++i) {
        for (int64_t j = 0; j < n_minor; ++j) {
          if (axis_ == SparseMatrixCompressedAxis::ROW) {
            coords = {i, j};
          } else {
            coords = {j, i};
          }
          const int64_t offset = tensor_.CalculateValueOffset(coords);
          if (std::any_of(tensor_data + offset, tensor_data + offset + value_elsize,
                          IsNonZero)) {
            std::copy_n(tensor_data + offset, value_elsize, values);
            values += value_elsize;

            AssignIndex(indices, j, index_elsize);
            indices += index_elsize;

            k++;
          }
        }
        AssignIndex(indptr, k, index_elsize);
        indptr += index_elsize;
      }
    }

    std::vector<int64_t> indptr_shape({n_major + 1});
    std::shared_ptr<Tensor> indptr_tensor =
        std::make_shared<Tensor>(index_value_type_, indptr_buffer, indptr_shape);

    std::vector<int64_t> indices_shape({nonzero_count});
    std::shared_ptr<Tensor> indices_tensor =
        std::make_shared<Tensor>(index_value_type_, indices_buffer, indices_shape);

    if (axis_ == SparseMatrixCompressedAxis::ROW) {
      sparse_index = std::make_shared<SparseCSRIndex>(indptr_tensor, indices_tensor);
    } else {
      sparse_index = std::make_shared<SparseCSCIndex>(indptr_tensor, indices_tensor);
    }
    data = std::move(values_buffer);

    return Status::OK();
  }

  std::shared_ptr<SparseIndex> sparse_index;
  std::shared_ptr<Buffer> data;

 private:
  SparseMatrixCompressedAxis axis_;
  const Tensor& tensor_;
  const std::shared_ptr<DataType>& index_value_type_;
  MemoryPool* pool_;
};

}  // namespace

Status MakeSparseCSXMatrixFromTensor(SparseMatrixCompressedAxis axis,
                                     const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data) {
  SparseCSXMatrixConverter converter(axis, tensor, index_value_type, pool);
  RETURN_NOT_OK(converter.Convert());

  *out_sparse_index = converter.sparse_index;
  *out_data = converter.data;
  return Status::OK();
}

Result<std::shared_ptr<Tensor>> MakeTensorFromSparseCSXMatrix(
    SparseMatrixCompressedAxis axis, MemoryPool* pool,
    const std::shared_ptr<Tensor>& indptr, const std::shared_ptr<Tensor>& indices,
    const int64_t non_zero_length, const std::shared_ptr<DataType>& value_type,
    const std::vector<int64_t>& shape, const int64_t tensor_size, const uint8_t* raw_data,
    const std::vector<std::string>& dim_names) {
  const auto* indptr_data = indptr->raw_data();
  const auto* indices_data = indices->raw_data();

  const int indptr_elsize = GetByteWidth(*indptr->type());
  const int indices_elsize = GetByteWidth(*indices->type());

  const auto& fw_value_type = checked_cast<const FixedWidthType&>(*value_type);
  const int value_elsize = GetByteWidth(fw_value_type);
  ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                        AllocateBuffer(value_elsize * tensor_size, pool));
  auto values = values_buffer->mutable_data();
  std::fill_n(values, value_elsize * tensor_size, 0);

  std::vector<int64_t> strides;
  RETURN_NOT_OK(ComputeRowMajorStrides(fw_value_type, shape, &strides));

  const auto nc = shape[1];

  int64_t offset = 0;
  for (int64_t i = 0; i < indptr->size() - 1; ++i) {
    const auto start =
        SparseTensorConverterMixin::GetIndexValue(indptr_data, indptr_elsize);
    const auto stop = SparseTensorConverterMixin::GetIndexValue(
        indptr_data + indptr_elsize, indptr_elsize);

    for (int64_t j = start; j < stop; ++j) {
      const auto index = SparseTensorConverterMixin::GetIndexValue(
          indices_data + j * indices_elsize, indices_elsize);
      switch (axis) {
        case SparseMatrixCompressedAxis::ROW:
          offset = (index + i * nc) * value_elsize;
          break;
        case SparseMatrixCompressedAxis::COLUMN:
          offset = (i + index * nc) * value_elsize;
          break;
      }

      std::copy_n(raw_data, value_elsize, values + offset);
      raw_data += value_elsize;
    }

    indptr_data += indptr_elsize;
  }

  return std::make_shared<Tensor>(value_type, std::move(values_buffer), shape, strides,
                                  dim_names);
}

Result<std::shared_ptr<Tensor>> MakeTensorFromSparseCSRMatrix(
    MemoryPool* pool, const SparseCSRMatrix* sparse_tensor) {
  const auto& sparse_index =
      internal::checked_cast<const SparseCSRIndex&>(*sparse_tensor->sparse_index());
  const auto& indptr = sparse_index.indptr();
  const auto& indices = sparse_index.indices();
  const auto non_zero_length = sparse_tensor->non_zero_length();
  return MakeTensorFromSparseCSXMatrix(
      SparseMatrixCompressedAxis::ROW, pool, indptr, indices, non_zero_length,
      sparse_tensor->type(), sparse_tensor->shape(), sparse_tensor->size(),
      sparse_tensor->raw_data(), sparse_tensor->dim_names());
}

Result<std::shared_ptr<Tensor>> MakeTensorFromSparseCSCMatrix(
    MemoryPool* pool, const SparseCSCMatrix* sparse_tensor) {
  const auto& sparse_index =
      internal::checked_cast<const SparseCSCIndex&>(*sparse_tensor->sparse_index());
  const auto& indptr = sparse_index.indptr();
  const auto& indices = sparse_index.indices();
  const auto non_zero_length = sparse_tensor->non_zero_length();
  return MakeTensorFromSparseCSXMatrix(
      SparseMatrixCompressedAxis::COLUMN, pool, indptr, indices, non_zero_length,
      sparse_tensor->type(), sparse_tensor->shape(), sparse_tensor->size(),
      sparse_tensor->raw_data(), sparse_tensor->dim_names());
}

}  // namespace internal
}  // namespace arrow
