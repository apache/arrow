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
#include "arrow/tensor/util.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class MemoryPool;

namespace internal {

namespace {

inline Status RaiseIndexValueTypeLimitError(const int dim) {
  return Status::TypeError(
      "The ", dim, internal::ordinal_suffix(dim),
      " index value type is too small to represent this dimension's index");
}

template <typename DataType>
Status CheckIndexValueTypeLimit(const int dim, const int64_t size) {
  using c_data_type = typename DataType::c_type;

  // NOTE: This function is not applied to UInt64Type, so the following static_cast is
  // safe
  const auto max = static_cast<int64_t>(std::numeric_limits<c_data_type>::max());
  if (max < size) {
    return RaiseIndexValueTypeLimitError(dim);
  }

  return Status::OK();
}

template <>
Status CheckIndexValueTypeLimit<UInt64Type>(const int dim, const int64_t size) {
  const auto max = std::numeric_limits<uint64_t>::max();
  // NOTE: we can assume size > 0, so the following static_cast is safe
  if (max < static_cast<uint64_t>(size)) {
    return RaiseIndexValueTypeLimitError(dim);
  }

  return Status::OK();
}

#define CALL_CHECK_INDEX_VALUE_TYPE_LIMIT(TYPE_CLASS) \
  case TYPE_CLASS##Type::type_id:                     \
    return CheckIndexValueTypeLimit<TYPE_CLASS##Type>(dim, size);

Status CheckIndexValueTypeLimit(const int dim, const int64_t size,
                                const std::shared_ptr<DataType>& index_value_type) {
  switch (index_value_type->id()) {
    ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(CALL_CHECK_INDEX_VALUE_TYPE_LIMIT);
    // LCOV_EXCL_START: The following invalid causes program failure.
    default:
      return Status::TypeError("Unsupported SparseTensor index value type");
      // LCOV_EXCL_STOP
  }

  return Status::OK();
}

#undef CALL_CHECK_INDEX_VALUE_TYPE_LIMIT

Status CheckIndexValueTypes(
    const Tensor& tensor,
    const std::vector<std::shared_ptr<DataType>>& index_value_types) {
  const auto ndim = tensor.ndim();
  if (index_value_types.size() != static_cast<size_t>(ndim)) {
    return Status::Invalid(
        "The number of index value types is inconsistent to tensor.ndim()");
  }
  const auto& shape = tensor.shape();
  for (int i = 0; i < ndim; ++i) {
    RETURN_NOT_OK(CheckIndexValueTypeLimit(i, shape[i], index_value_types[i]));
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCOOIndex

template <typename ValueType>
class SparseSplitCOOTensorConverter {
 public:
  using c_value_type = typename ValueType::c_type;

  SparseSplitCOOTensorConverter(
      const Tensor& tensor,
      const std::vector<std::shared_ptr<DataType>>& index_value_types, MemoryPool* pool)
      : tensor_(tensor), index_value_types_(index_value_types), pool_(pool) {}

  template <typename IndexValueType>
  Status TransferNonZeros1D(c_value_type* non_zero_values,
                            std::shared_ptr<Buffer> non_zero_indices_buffer) {
    constexpr c_value_type zero = c_value_type(0);
    const auto* values = reinterpret_cast<const c_value_type*>(tensor_.raw_data());

    using c_index_value_type = typename IndexValueType::c_type;
    auto* non_zero_indices =
        reinterpret_cast<c_index_value_type*>(non_zero_indices_buffer->mutable_data());

    const auto size = tensor_.size();
    for (int64_t i = 0; i < size; ++i, ++values) {
      if (*values != zero) {
        *non_zero_values++ = *values;
        *non_zero_indices++ =
            static_cast<c_index_value_type>(i);  // This cast should be safe
      }
    }

    return Status::OK();
  }

  Status Convert1D() {
    DCHECK_LE(tensor_.ndim(), 1);

    constexpr c_value_type zero = c_value_type(0);
    const auto& fw_index_value_type =
        checked_cast<const FixedWidthType&>(*index_value_types_[0]);
    const size_t indices_elsize = fw_index_value_type.bit_width() / CHAR_BIT;
    const auto size = tensor_.size();

    ARROW_ASSIGN_OR_RAISE(int64_t non_zero_count, tensor_.CountNonZero());

    const auto* values = reinterpret_cast<const c_value_type*>(tensor_.raw_data());

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> values_buffer,
                          AllocateBuffer(sizeof(c_value_type) * non_zero_count, pool_));
    auto* out_values = reinterpret_cast<c_value_type*>(values_buffer->mutable_data());

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> indices_buffer,
                          AllocateBuffer(indices_elsize * non_zero_count, pool_));

    // NOTE: Using unsigned type for out_indices is safe because we already checked
    // index value limits beforehand.
    switch (indices_elsize) {
      case 1: {  // Int8, UInt8
        auto* out_indices = indices_buffer->mutable_data();
        for (int64_t i = 0; i < size; ++i, ++values) {
          if (*values != zero) {
            *out_values++ = *values;
            *out_indices++ = static_cast<uint8_t>(i);
          }
        }
        break;
      }
      case 2: {  // Int16, UInt16
        auto* out_indices = reinterpret_cast<uint16_t*>(indices_buffer->mutable_data());
        for (int64_t i = 0; i < size; ++i, ++values) {
          if (*values != zero) {
            *out_values++ = *values;
            *out_indices++ = static_cast<uint16_t>(i);
          }
        }
        break;
      }
      case 4: {  // Int32, UInt32
        auto* out_indices = reinterpret_cast<uint32_t*>(indices_buffer->mutable_data());
        for (int64_t i = 0; i < size; ++i, ++values) {
          if (*values != zero) {
            *out_values++ = *values;
            *out_indices++ = static_cast<uint32_t>(i);
          }
        }
        break;
      }
      case 8: {  // Int64, UInt64
        auto* out_indices = reinterpret_cast<uint64_t*>(indices_buffer->mutable_data());
        for (int64_t i = 0; i < size; ++i, ++values) {
          if (*values != zero) {
            *out_values++ = *values;
            *out_indices++ = static_cast<uint64_t>(i);
          }
        }
        break;
      }
      default:
        return Status::Invalid("Invalid element size of index value type: ",
                               indices_elsize);
    }

    ARROW_ASSIGN_OR_RAISE(
        sparse_index,
        SparseSplitCOOIndex::Make(index_value_types_, non_zero_count, {indices_buffer}));
    data = std::move(values_buffer);

    return Status::OK();
  }

  Status ConvertND() {
    DCHECK_GE(tensor_.ndim(), 2);

    const auto ndim = tensor_.ndim();

    // Collect non-zero indices
    std::vector<std::vector<int64_t>> non_zero_indices;
    ARROW_ASSIGN_OR_RAISE(non_zero_indices, tensor_.NonZeroIndices());

    const auto non_zero_count = non_zero_indices.size();

    // Prepare buffers for storing non-zero values
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> values_buffer,
                          AllocateBuffer(sizeof(c_value_type) * non_zero_count, pool_));
    auto* out_values = reinterpret_cast<c_value_type*>(values_buffer->mutable_data());

    // Prepare buffers for storing indices of non-zero values
    std::vector<std::shared_ptr<Buffer>> indices_buffers;
    for (int i = 0; i < ndim; ++i) {
      const auto& fw_index_value_type =
          checked_cast<const FixedWidthType&>(*index_value_types_[i]);
      const size_t indices_elsize = fw_index_value_type.bit_width() / CHAR_BIT;
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> indices_buffer,
                            AllocateBuffer(indices_elsize * non_zero_count, pool_));
      indices_buffers.push_back(indices_buffer);
    }

    // Transfer non-zero values
    for (const auto& non_zero_index : non_zero_indices) {
      *out_values++ = tensor_.Value<ValueType>(non_zero_index);
    }

    // Transfer indices of non-zero values
    for (int i = 0; i < ndim; ++i) {
      const auto& fw_index_value_type =
          checked_cast<const FixedWidthType&>(*index_value_types_[i]);
      const size_t indices_elsize = fw_index_value_type.bit_width() / CHAR_BIT;

      // NOTE: Using unsigned type for out_indices is safe because we already checked
      // index value limits beforehand.
      switch (indices_elsize) {
        case 1: {  // Int8, UInt8
          auto* out_indices = indices_buffers[i]->mutable_data();
          std::transform(non_zero_indices.begin(), non_zero_indices.end(), out_indices,
                         [i](const std::vector<int64_t>& index) {
                           return static_cast<uint8_t>(index[i]);
                         });
          break;
        }
        case 2: {  // Int16, UInt16
          auto* out_indices =
              reinterpret_cast<uint16_t*>(indices_buffers[i]->mutable_data());
          std::transform(non_zero_indices.begin(), non_zero_indices.end(), out_indices,
                         [i](const std::vector<int64_t>& index) {
                           return static_cast<uint16_t>(index[i]);
                         });
          break;
        }
        case 4: {  // Int32, UInt32
          auto* out_indices =
              reinterpret_cast<uint32_t*>(indices_buffers[i]->mutable_data());
          std::transform(non_zero_indices.begin(), non_zero_indices.end(), out_indices,
                         [i](const std::vector<int64_t>& index) {
                           return static_cast<uint32_t>(index[i]);
                         });
          break;
        }
        case 8: {  // Int64, UInt64
          auto* out_indices =
              reinterpret_cast<uint64_t*>(indices_buffers[i]->mutable_data());
          std::transform(non_zero_indices.begin(), non_zero_indices.end(), out_indices,
                         [i](const std::vector<int64_t>& index) {
                           return static_cast<uint64_t>(index[i]);
                         });
          break;
        }
        default:
          return Status::Invalid("Invalid element size of index value type: ",
                                 indices_elsize);
      }
    }

    ARROW_ASSIGN_OR_RAISE(
        sparse_index,
        SparseSplitCOOIndex::Make(index_value_types_, non_zero_count, indices_buffers));
    data = std::move(values_buffer);

    return Status::OK();
  }

  Status Convert() {
    // This check assures all the item in the index_value_types_ are integer types.
    RETURN_NOT_OK(CheckIndexValueTypes(tensor_, index_value_types_));

    if (tensor_.ndim() <= 1) {
      return Convert1D();
    } else {
      return ConvertND();
    }
  }

  std::shared_ptr<SparseSplitCOOIndex> sparse_index;
  std::shared_ptr<Buffer> data;

 private:
  const Tensor& tensor_;
  const std::vector<std::shared_ptr<DataType>>& index_value_types_;
  MemoryPool* pool_;
};

template <typename TYPE>
Status MakeSparseSplitCOOTensorFromTensor(
    const Tensor& tensor, const std::vector<std::shared_ptr<DataType>>& index_value_types,
    MemoryPool* pool, std::shared_ptr<SparseIndex>* out_sparse_index,
    std::shared_ptr<Buffer>* out_data) {
  SparseSplitCOOTensorConverter<TYPE> converter(tensor, index_value_types, pool);
  RETURN_NOT_OK(converter.Convert());

  *out_sparse_index = checked_pointer_cast<SparseIndex>(converter.sparse_index);
  *out_data = converter.data;
  return Status::OK();
}

}  // namespace

#define MAKE_SPARSE_TENSOR_FROM_TENSOR(TYPE_CLASS)               \
  case TYPE_CLASS##Type::type_id:                                \
    return MakeSparseSplitCOOTensorFromTensor<TYPE_CLASS##Type>( \
        tensor, index_value_types, pool, out_sparse_index, out_data);

Status MakeSparseSplitCOOTensorFromTensor(
    const Tensor& tensor, const std::vector<std::shared_ptr<DataType>>& index_value_types,
    MemoryPool* pool, std::shared_ptr<SparseIndex>* out_sparse_index,
    std::shared_ptr<Buffer>* out_data) {
  switch (tensor.type()->id()) {
    ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(MAKE_SPARSE_TENSOR_FROM_TENSOR);
      // LCOV_EXCL_START: ignore program failure
    default:
      return Status::TypeError("Unsupported Tensor value type");
      // LCOV_EXCL_STOP
  }
}

#undef MAKE_SPARSE_TENSOR_FROM_TENSOR

Status MakeSparseSplitCOOTensorFromTensor(
    const Tensor& tensor, const std::shared_ptr<DataType>& index_value_type,
    MemoryPool* pool, std::shared_ptr<SparseIndex>* out_sparse_index,
    std::shared_ptr<Buffer>* out_data) {
  std::vector<std::shared_ptr<DataType>> index_value_types(tensor.ndim(),
                                                           index_value_type);
  return MakeSparseSplitCOOTensorFromTensor(tensor, index_value_types, pool,
                                            out_sparse_index, out_data);
}

Result<std::shared_ptr<Tensor>> MakeTensorFromSparseSplitCOOTensor(
    MemoryPool* pool, const SparseSplitCOOTensor* sparse_tensor) {
  const auto& value_type = checked_cast<const FixedWidthType&>(*sparse_tensor->type());
  const int value_elsize = GetByteWidth(value_type);
  ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                        AllocateBuffer(value_elsize * sparse_tensor->size(), pool));
  auto values = values_buffer->mutable_data();
  std::fill_n(values, value_elsize * sparse_tensor->size(), 0);

  // Generate row-major index-strides (not offset-strides)
  const auto ndim = sparse_tensor->ndim();
  std::vector<int64_t> strides(ndim, 1);
  for (auto i = ndim - 1; i > 0; --i) {
    strides[i - 1] = strides[i] * sparse_tensor->shape()[i];
  }

  const auto* raw_data = sparse_tensor->raw_data();
  const auto& sparse_index =
      internal::checked_cast<const SparseSplitCOOIndex&>(*sparse_tensor->sparse_index());

  const auto& indices = sparse_index.indices();

  std::vector<int64_t> coord;
  const auto n = sparse_tensor->non_zero_length();
  for (int64_t i = 0; i < n; ++i) {
    int64_t offset = 0;

    for (int64_t j = 0; j < ndim; ++j) {
      const auto& indices_j = indices[j];
      const auto index_data = indices_j->raw_data();
      const auto s = strides[j];

      switch (indices_j->type_id()) {
        case Type::INT8:
        case Type::UINT8:
          offset += s * index_data[i];
          break;
        case Type::INT16:
        case Type::UINT16:
          offset += s * reinterpret_cast<const uint16_t*>(index_data)[i];
          break;
        case Type::INT32:
        case Type::UINT32:
          offset += s * reinterpret_cast<const uint32_t*>(index_data)[i];
          break;
        case Type::INT64:
          offset += s * reinterpret_cast<const int64_t*>(index_data)[i];
          break;
        default:
          ARROW_CHECK(false) << "unreachable";
          break;
      }
    }

    std::copy_n(raw_data, value_elsize, values + offset);
    raw_data += value_elsize;
  }

  return Tensor::Make(sparse_tensor->type(), std::move(values_buffer),
                      sparse_tensor->shape(), {}, sparse_tensor->dim_names());
}

}  // namespace internal
}  // namespace arrow
