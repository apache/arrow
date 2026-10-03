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

#include "arrow/tensor/converter_internal.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/macros.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

class MemoryPool;

namespace internal {

namespace {

void IncrementRowMajorIndex(std::vector<int64_t>& coord,
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

Status ConvertRowMajorTensor(const Tensor& tensor, const DataType& index_type,
                             Buffer& indices_buffer, Buffer& value_buffer) {
  const auto ndim = tensor.ndim();
  const auto& shape = tensor.shape();
  auto index_elsize = index_type.byte_width();
  auto indices = indices_buffer.mutable_data();
  std::vector<int64_t> coord(ndim, 0);

  auto visitor = [&](const auto& value_type) {
    using ValueType = std::decay_t<decltype(value_type)>;
    if constexpr (is_number_type<ValueType>::value) {
      using ValueCType = typename ValueType::c_type;

      auto values = value_buffer.mutable_data_as<ValueCType>();
      const auto* tensor_data = tensor.data()->data_as<ValueCType>();

      for (int64_t n = tensor.size(); n > 0; --n) {
        auto x = *tensor_data;
        if (is_not_zero<ValueType>(x)) {
          std::for_each(coord.begin(), coord.end(), [&](int64_t coord_value) {
            SparseTensorConverterMixin::AssignIndex(indices, coord_value, index_elsize);
            indices += index_elsize;
          });
          *values++ = x;
        }

        IncrementRowMajorIndex(coord, shape);
        ++tensor_data;
      }
    }
    return Status::OK();
  };

  RETURN_NOT_OK(VisitType(*tensor.type(), visitor));
  return Status::OK();
}

Status ConvertStridedOrColumnMajorTensor(const Tensor& tensor, const DataType& index_type,
                                         Buffer& indices_buffer, Buffer& values_buffer) {
  const auto& shape = tensor.shape();
  const auto ndim = tensor.ndim();
  std::vector<int64_t> coord(ndim, 0);
  auto indices = indices_buffer.mutable_data();
  auto index_elsize = index_type.byte_width();

  auto visitor = [&](const auto& value_type) {
    using ValueType = std::decay_t<decltype(value_type)>;
    if constexpr (is_number_type<ValueType>::value) {
      using ValueCType = typename ValueType::c_type;

      auto values = values_buffer.mutable_data_as<ValueCType>();

      for (int64_t n = tensor.size(); n > 0; --n) {
        ValueCType x = tensor.Value<ValueType>(coord);
        if (is_not_zero<ValueType>(x)) {
          *values++ = x;
          std::for_each(coord.begin(), coord.end(), [&](int64_t coord_value) {
            SparseTensorConverterMixin::AssignIndex(indices, coord_value, index_elsize);
            indices += index_elsize;
          });
        }

        IncrementRowMajorIndex(coord, shape);
      }
    }
    return Status::OK();
  };

  RETURN_NOT_OK(VisitType(*tensor.type(), visitor));
  return Status::OK();
}

Status ConvertVectorTensor(const Tensor& tensor, const DataType& index_type,
                           Buffer& indices_buffer, Buffer& values_buffer) {
  auto ndim = tensor.ndim();
  const int64_t count = ndim == 0 ? 1 : tensor.shape()[0];
  auto indices = indices_buffer.mutable_data();
  auto index_elsize = index_type.byte_width();

  auto visitor = [&](const auto& value_type) {
    using ValueType = std::decay_t<decltype(value_type)>;
    if constexpr (is_number_type<ValueType>::value) {
      using ValueCType = typename ValueType::c_type;

      auto values = values_buffer.mutable_data_as<ValueCType>();

      const auto* tensor_data = tensor.data()->data_as<ValueCType>();
      for (int64_t i = 0; i < count; ++i) {
        if (is_not_zero<ValueType>(*tensor_data)) {
          SparseTensorConverterMixin::AssignIndex(indices, i, index_elsize);
          indices += index_elsize;
          *values++ = *tensor_data;
        }
        ++tensor_data;
      }
    }
    return Status::OK();
  };

  RETURN_NOT_OK(VisitType(*tensor.type(), visitor));
  return Status::OK();
}

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCOOIndex

class SparseCOOTensorConverter {
 public:
  SparseCOOTensorConverter(const Tensor& tensor,
                           const std::shared_ptr<DataType>& index_value_type,
                           MemoryPool* pool)
      : tensor_(tensor), index_value_type_(index_value_type), pool_(pool) {}

  Status Convert() {
    RETURN_NOT_OK(::arrow::internal::CheckSparseIndexMaximumValue(index_value_type_,
                                                                  tensor_.shape()));

    const int index_elsize = index_value_type_->byte_width();
    const int value_elsize = tensor_.type()->byte_width();

    const int64_t ndim = tensor_.ndim();
    ARROW_ASSIGN_OR_RAISE(int64_t nonzero_count, tensor_.CountNonZero());

    ARROW_ASSIGN_OR_RAISE(auto indices_buffer,
                          AllocateBuffer(index_elsize * ndim * nonzero_count, pool_));

    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(value_elsize * nonzero_count, pool_));

    if (ndim <= 1) {
      ARROW_RETURN_NOT_OK(ConvertVectorTensor(tensor_, *index_value_type_,
                                              *indices_buffer, *values_buffer));
    } else if (tensor_.is_row_major()) {
      ARROW_RETURN_NOT_OK(ConvertRowMajorTensor(tensor_, *index_value_type_,
                                                *indices_buffer, *values_buffer));
    } else if (tensor_.is_column_major()) {
      ARROW_RETURN_NOT_OK(ConvertStridedOrColumnMajorTensor(
          tensor_, *index_value_type_, *indices_buffer, *values_buffer));
    } else {
      ARROW_RETURN_NOT_OK(ConvertStridedOrColumnMajorTensor(
          tensor_, *index_value_type_, *indices_buffer, *values_buffer));
    }

    // make results
    const std::vector<int64_t> indices_shape = {nonzero_count, ndim};
    std::vector<int64_t> indices_strides;
    RETURN_NOT_OK(internal::ComputeRowMajorStrides(
        checked_cast<const FixedWidthType&>(*index_value_type_), indices_shape,
        &indices_strides));
    auto coords = std::make_shared<Tensor>(index_value_type_, std::move(indices_buffer),
                                           indices_shape, indices_strides);
    ARROW_ASSIGN_OR_RAISE(sparse_index, SparseCOOIndex::Make(coords, true));
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
  ARROW_RETURN_NOT_OK(converter.Convert());
  *out_sparse_index = checked_pointer_cast<SparseIndex>(converter.sparse_index);
  *out_data = converter.data;
  return Status::OK();
}

// TODO(GH-47580): Enable column-major index tensor
Result<std::shared_ptr<Tensor>> MakeTensorFromSparseCOOTensor(
    MemoryPool* pool, const SparseCOOTensor* sparse_tensor) {
  const auto& sparse_index =
      checked_cast<const SparseCOOIndex&>(*sparse_tensor->sparse_index());
  const auto& coords = sparse_index.indices();
  const auto* coords_data = coords->raw_data();

  const int index_elsize = coords->type()->byte_width();

  const auto& value_type = checked_cast<const FixedWidthType&>(*sparse_tensor->type());
  const int value_elsize = value_type.byte_width();
  ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                        AllocateBuffer(value_elsize * sparse_tensor->size(), pool));
  auto values = values_buffer->mutable_data();
  std::fill_n(values, value_elsize * sparse_tensor->size(), 0);

  std::vector<int64_t> strides;
  RETURN_NOT_OK(ComputeRowMajorStrides(value_type, sparse_tensor->shape(), &strides));

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
