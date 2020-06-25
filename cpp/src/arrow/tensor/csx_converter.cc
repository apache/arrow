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
    RETURN_NOT_OK(CheckSparseIndexMaximumValue(index_value_type_, tensor_.shape()));

    const int index_elsize =
        checked_cast<const IntegerType&>(*index_value_type_).bit_width() / CHAR_BIT;
    const int value_elsize =
        checked_cast<const FixedWidthType&>(*tensor_.type()).bit_width() / CHAR_BIT;

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

}  // namespace internal
}  // namespace arrow
