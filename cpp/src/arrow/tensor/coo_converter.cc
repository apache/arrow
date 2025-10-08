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
#include <cmath>
#include <cstdint>
#include <memory>
#include <numeric>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/macros.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

class MemoryPool;

namespace internal {

namespace {

template <typename ValueType, typename IndexType>
Status ValidateSparseCooTensorCreation(const SparseCOOIndex& sparse_coo_index,
                                       const Buffer& sparse_coo_values_buffer,
                                       const Tensor& tensor) {
  using IndexCType = typename IndexType::c_type;
  using ValueCType = typename ValueType::c_type;

  const auto& indices = sparse_coo_index.indices();
  const auto* indices_data = sparse_coo_index.indices()->data()->data_as<IndexCType>();
  const auto* sparse_coo_values = sparse_coo_values_buffer.data_as<ValueCType>();

  ARROW_ASSIGN_OR_RAISE(auto non_zero_count, tensor.CountNonZero());

  if (indices->shape()[0] != non_zero_count) {
    return Status::Invalid("Mismatch between non-zero count in sparse tensor (",
                           indices->shape()[0], ") and dense tensor (", non_zero_count,
                           ")");
  } else if (indices->shape()[1] != static_cast<int64_t>(tensor.shape().size())) {
    return Status::Invalid("Mismatch between coordinate dimension in sparse tensor (",
                           indices->shape()[1], ") and tensor shape (",
                           tensor.shape().size(), ")");
  }

  auto coord_size = indices->shape()[1];
  std::vector<int64_t> coord(coord_size);
  for (int64_t i = 0; i < indices->shape()[0]; i++) {
    if (!is_not_zero<ValueType>(sparse_coo_values[i])) {
      return Status::Invalid("Sparse tensor values must be non-zero");
    }

    for (int64_t j = 0; j < coord_size; j++) {
      coord[j] = static_cast<int64_t>(indices_data[i * coord_size + j]);
    }

    if (sparse_coo_values[i] != tensor.Value<ValueType>(coord)) {
      if constexpr (is_floating_type<ValueType>::value) {
        if (!std::isnan(tensor.Value<ValueType>(coord)) ||
            !std::isnan(sparse_coo_values[i])) {
          return Status::Invalid(
              "Inconsistent values between sparse tensor and dense tensor");
        }
      } else {
        return Status::Invalid(
            "Inconsistent values between sparse tensor and dense tensor");
      }
    }
  }

  return Status::OK();
}

template <typename IndexCType>
inline void IncrementRowMajorIndex(std::vector<IndexCType>& coord,
                                   const std::vector<int64_t>& shape) {
  const int64_t ndim = shape.size();
  ++coord[ndim - 1];
  if (static_cast<int64_t>(coord[ndim - 1]) == shape[ndim - 1]) {
    int64_t d = ndim - 1;
    while (d > 0 && static_cast<int64_t>(coord[d]) == shape[d]) {
      coord[d] = 0;
      ++coord[d - 1];
      --d;
    }
  }
}

template <typename IndexType, typename ValueType>
void ConvertRowMajorTensor(const Tensor& tensor, typename IndexType::c_type* indices,
                           typename ValueType::c_type* values) {
  using ValueCType = typename ValueType::c_type;
  using IndexCType = typename IndexType::c_type;

  const auto ndim = tensor.ndim();
  const auto& shape = tensor.shape();
  const auto* tensor_data = tensor.data()->data_as<ValueCType>();

  std::vector<IndexCType> coord(ndim, 0);
  for (int64_t n = tensor.size(); n > 0; --n) {
    auto x = *tensor_data;
    if (is_not_zero<ValueType>(x)) {
      std::copy(coord.begin(), coord.end(), indices);
      *values++ = x;
      indices += ndim;
    }

    IncrementRowMajorIndex(coord, shape);
    ++tensor_data;
  }
}

// TODO(GH-47580): Correct column-major tensor conversion
template <typename IndexType, typename ValueType>
void ConvertColumnMajorTensor(const Tensor& tensor,
                              typename IndexType::c_type* out_indices,
                              typename ValueType::c_type* out_values,
                              const int64_t size) {
  using ValueCtype = typename ValueType::c_type;
  using IndexCType = typename IndexType::c_type;

  const auto ndim = tensor.ndim();
  std::vector<IndexCType> indices(ndim * size);
  std::vector<ValueCtype> values(size);
  ConvertRowMajorTensor<IndexType, ValueType>(tensor, indices.data(), values.data());

  // transpose indices
  for (int64_t i = 0; i < size; ++i) {
    for (int j = 0; j < ndim / 2; ++j) {
      std::swap(indices[i * ndim + j], indices[i * ndim + ndim - j - 1]);
    }
  }

  // sort indices
  std::vector<int64_t> order(size);
  std::iota(order.begin(), order.end(), 0);
  std::sort(order.begin(), order.end(), [&](const int64_t xi, const int64_t yi) {
    const int64_t x_offset = xi * ndim;
    const int64_t y_offset = yi * ndim;
    for (int j = 0; j < ndim; ++j) {
      const auto x = indices[x_offset + j];
      const auto y = indices[y_offset + j];
      if (x < y) return true;
      if (x > y) return false;
    }
    return false;
  });

  // transfer result
  const auto* indices_data = indices.data();
  for (int64_t i = 0; i < size; ++i) {
    out_values[i] = values[i];

    std::copy_n(indices_data, ndim, out_indices);
    indices_data += ndim;
    out_indices += ndim;
  }
}

template <typename IndexType, typename ValueType>
void ConvertStridedTensor(const Tensor& tensor, typename IndexType::c_type* indices,
                          typename ValueType::c_type* values) {
  using ValueCType = typename ValueType::c_type;
  using IndexCType = typename IndexType::c_type;

  const auto& shape = tensor.shape();
  const auto ndim = tensor.ndim();
  std::vector<int64_t> coord(ndim, 0);

  ValueCType x;
  int64_t i;
  for (int64_t n = tensor.size(); n > 0; --n) {
    x = tensor.Value<ValueType>(coord);
    if (is_not_zero<ValueType>(x)) {
      *values++ = x;
      for (i = 0; i < ndim; ++i) {
        *indices++ = static_cast<IndexCType>(coord[i]);
      }
    }

    IncrementRowMajorIndex(coord, shape);
  }
}

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCOOIndex

class SparseCOOTensorConverter {
 public:
  SparseCOOTensorConverter(const Tensor& tensor,
                           const std::shared_ptr<DataType>& index_value_type,
                           MemoryPool* pool)
      : tensor_(tensor), index_value_type_(index_value_type), pool_(pool) {}

  template <typename ValueType, typename IndexType>
  Status Convert(const ValueType&, const IndexType&) {
    using ValueCType = typename ValueType::c_type;
    using IndexCType = typename IndexType::c_type;

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

    auto* values = values_buffer->mutable_data_as<ValueCType>();
    const auto* tensor_data = tensor_.data()->data_as<ValueCType>();
    auto* indices = indices_buffer->mutable_data_as<IndexCType>();
    if (ndim <= 1) {
      const int64_t count = ndim == 0 ? 1 : tensor_.shape()[0];
      for (int64_t i = 0; i < count; ++i) {
        if (is_not_zero<ValueType>(*tensor_data)) {
          *indices++ = static_cast<IndexCType>(i);
          *values++ = *tensor_data;
        }
        ++tensor_data;
      }
    } else if (tensor_.is_row_major()) {
      ConvertRowMajorTensor<IndexType, ValueType>(tensor_, indices, values);
    } else if (tensor_.is_column_major()) {
      ConvertColumnMajorTensor<IndexType, ValueType>(tensor_, indices, values,
                                                     nonzero_count);
    } else {
      ConvertStridedTensor<IndexType, ValueType>(tensor_, indices, values);
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
    DCHECK_OK((ValidateSparseCooTensorCreation<ValueType, IndexType>(*sparse_index, *data,
                                                                     tensor_)));
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
  ConverterVisitor visitor{converter};
  ARROW_RETURN_NOT_OK(VisitValueAndIndexType(*tensor.type(), *index_value_type, visitor));
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
