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

inline void IncrementIndex(std::vector<int64_t>& coord,
                           const std::vector<int64_t>& shape) {
  const int64_t ndim = shape.size();
  ++coord[ndim - 1];
  if (coord[ndim - 1] == shape[ndim - 1]) {
    int64_t d = ndim - 1;
    while (d > 0 && coord[d] == shape[d]) {
      coord[d] = 0;
      ++coord[d - 1];
      --d;
    }
  }
}

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCOOIndex

class SparseCOOTensorConverter : private SparseTensorConverterMixin {
  using SparseTensorConverterMixin::AssignIndex;
  using SparseTensorConverterMixin::IsNonZero;

 public:
  SparseCOOTensorConverter(const Tensor& tensor,
                           const std::shared_ptr<DataType>& index_value_type,
                           MemoryPool* pool)
      : tensor_(tensor), index_value_type_(index_value_type), pool_(pool) {}

  Status Convert() {
    RETURN_NOT_OK(::arrow::internal::CheckSparseIndexMaximumValue(index_value_type_,
                                                                  tensor_.shape()));

    const int index_elsize = GetByteWidth(*index_value_type_);
    const int value_elsize = GetByteWidth(*tensor_.type());

    const int64_t ndim = tensor_.ndim();
    ARROW_ASSIGN_OR_RAISE(int64_t nonzero_count, tensor_.CountNonZero());

    ARROW_ASSIGN_OR_RAISE(auto indices_buffer,
                          AllocateBuffer(index_elsize * ndim * nonzero_count, pool_));
    uint8_t* indices = indices_buffer->mutable_data();

    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(value_elsize * nonzero_count, pool_));
    uint8_t* values = values_buffer->mutable_data();

    const uint8_t* tensor_data = tensor_.raw_data();
    if (ndim <= 1) {
      const int64_t count = ndim == 0 ? 1 : tensor_.shape()[0];
      for (int64_t i = 0; i < count; ++i) {
        if (std::any_of(tensor_data, tensor_data + value_elsize, IsNonZero)) {
          AssignIndex(indices, i, index_elsize);
          std::copy_n(tensor_data, value_elsize, values);

          indices += index_elsize;
          values += value_elsize;
        }
        tensor_data += value_elsize;
      }
    } else {
      const std::vector<int64_t>& shape = tensor_.shape();
      std::vector<int64_t> coord(ndim, 0);  // The current logical coordinates

      for (int64_t n = tensor_.size(); n > 0; n--) {
        int64_t offset = tensor_.CalculateValueOffset(coord);
        if (std::any_of(tensor_data + offset, tensor_data + offset + value_elsize,
                        IsNonZero)) {
          std::copy_n(tensor_data + offset, value_elsize, values);
          values += value_elsize;

          // Write indices in row-major order.
          for (int64_t i = 0; i < ndim; ++i) {
            AssignIndex(indices, coord[i], index_elsize);
            indices += index_elsize;
          }
        }

        IncrementIndex(coord, shape);
      }
    }

    // make results
    const std::vector<int64_t> indices_shape = {nonzero_count, ndim};
    const std::vector<int64_t> indices_strides = {index_elsize * ndim, index_elsize};
    sparse_index = std::make_shared<SparseCOOIndex>(std::make_shared<Tensor>(
        index_value_type_, std::move(indices_buffer), indices_shape, indices_strides));
    data = std::move(values_buffer);

    return Status::OK();
  }

  std::shared_ptr<SparseCOOIndex> sparse_index;
  std::shared_ptr<Buffer> data;

 private:
  const Tensor& tensor_;
  const std::shared_ptr<DataType>& index_value_type_;
  MemoryPool* pool_;
};

}  // namespace

void SparseTensorConverterMixin::AssignIndex(uint8_t* indices, int64_t val,
                                             const int elsize) {
  switch (elsize) {
    case 1:
      *indices = static_cast<uint8_t>(val);
      break;
    case 2:
      *reinterpret_cast<uint16_t*>(indices) = static_cast<uint16_t>(val);
      break;
    case 4:
      *reinterpret_cast<uint32_t*>(indices) = static_cast<uint32_t>(val);
      break;
    case 8:
      *reinterpret_cast<int64_t*>(indices) = val;
      break;
    default:
      break;
  }
}

int64_t SparseTensorConverterMixin::GetIndexValue(const uint8_t* value_ptr,
                                                  const int elsize) {
  switch (elsize) {
    case 1:
      return *value_ptr;

    case 2:
      return *reinterpret_cast<const uint16_t*>(value_ptr);

    case 4:
      return *reinterpret_cast<const uint32_t*>(value_ptr);

    case 8:
      return *reinterpret_cast<const int64_t*>(value_ptr);

    default:
      return 0;
  }
}

Status MakeSparseCOOTensorFromTensor(const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data) {
  SparseCOOTensorConverter converter(tensor, index_value_type, pool);
  RETURN_NOT_OK(converter.Convert());

  *out_sparse_index = checked_pointer_cast<SparseIndex>(converter.sparse_index);
  *out_data = converter.data;
  return Status::OK();
}

Result<std::shared_ptr<Tensor>> MakeTensorFromSparseCOOTensor(
    MemoryPool* pool, const SparseCOOTensor* sparse_tensor) {
  const auto& sparse_index =
      checked_cast<const SparseCOOIndex&>(*sparse_tensor->sparse_index());
  const auto& coords = sparse_index.indices();
  const auto* coords_data = coords->raw_data();

  const int index_elsize = GetByteWidth(*coords->type());

  const auto& value_type = checked_cast<const FixedWidthType&>(*sparse_tensor->type());
  const int value_elsize = GetByteWidth(value_type);
  ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                        AllocateBuffer(value_elsize * sparse_tensor->size(), pool));
  auto values = values_buffer->mutable_data();
  std::fill_n(values, value_elsize * sparse_tensor->size(), 0);

  std::vector<int64_t> strides;
  ComputeRowMajorStrides(value_type, sparse_tensor->shape(), &strides);

  const auto* raw_data = sparse_tensor->raw_data();
  const int ndim = sparse_tensor->ndim();

  for (int64_t i = 0; i < sparse_tensor->non_zero_length(); ++i) {
    int64_t offset = 0;

    for (int j = 0; j < ndim; ++j) {
      auto index = static_cast<int64_t>(
          SparseTensorConverterMixin::GetIndexValue(coords_data, index_elsize));
      offset += index * strides[j];
      coords_data += index_elsize;
    }

    std::copy_n(raw_data, value_elsize, values + offset);
    raw_data += value_elsize;
  }

  return std::make_shared<Tensor>(sparse_tensor->type(), std::move(values_buffer),
                                  sparse_tensor->shape(), strides,
                                  sparse_tensor->dim_names());
}

}  // namespace internal
}  // namespace arrow
