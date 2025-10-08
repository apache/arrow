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

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

class MemoryPool;

namespace internal {

namespace {

template <typename SparseIndexType, typename ValueType, typename IndexType>
Status ValidateSparseCSXTensorCreation(const SparseIndexType& sparse_csx_index,
                                       const Buffer& values_buffer,
                                       const Tensor& tensor) {
  using ValueCType = typename ValueType::c_type;
  using IndexCType = typename IndexType::c_type;
  auto axis = sparse_csx_index.kCompressedAxis;

  auto& indptr = sparse_csx_index.indptr();
  auto& indices = sparse_csx_index.indices();
  auto indptr_data = indptr->data()->template data_as<IndexCType>();
  auto indices_data = indices->data()->template data_as<IndexCType>();
  auto sparse_csx_values = values_buffer.data_as<ValueCType>();

  ARROW_ASSIGN_OR_RAISE(auto non_zero_count, tensor.CountNonZero());
  if (indices->shape()[0] != non_zero_count) {
    return Status::Invalid("Mismatch between non-zero count in sparse tensor (",
                           indices->shape()[0], ") and dense tensor (", non_zero_count,
                           ")");
  }

  for (int64_t i = 0; i < indptr->size() - 1; ++i) {
    const auto start = static_cast<int64_t>(indptr_data[i]);
    const auto stop = static_cast<int64_t>(indptr_data[i + 1]);
    std::vector<int64_t> coord(2);
    for (int64_t j = start; j < stop; ++j) {
      if (!is_not_zero<ValueType>(sparse_csx_values[j])) {
        return Status::Invalid("Sparse tensor values must be non-zero");
      }

      switch (axis) {
        case SparseMatrixCompressedAxis::ROW:
          coord[0] = i;
          coord[1] = static_cast<int64_t>(indices_data[j]);
          break;
        case SparseMatrixCompressedAxis::COLUMN:
          coord[0] = static_cast<int64_t>(indices_data[j]);
          coord[1] = i;
          break;
      }
      if (sparse_csx_values[j] != tensor.Value<ValueType>(coord)) {
        if constexpr (is_floating_type<ValueType>::value) {
          if (!std::isnan(sparse_csx_values[j]) ||
              !std::isnan(tensor.Value<ValueType>(coord))) {
            return Status::Invalid(
                "Inconsistent values between sparse tensor and dense tensor");
          }
        } else {
          return Status::Invalid(
              "Inconsistent values between sparse tensor and dense tensor");
        }
      }
    }
  }
  return Status::OK();
}

template <typename ValueType, typename IndexType>
Status ValidateSparseCSXTensorCreation(const SparseIndex& sparse_index,
                                       const Buffer& values_buffer,
                                       const Tensor& tensor) {
  if (sparse_index.format_id() == SparseTensorFormat::CSC) {
    auto sparse_csc_index = checked_cast<const SparseCSCIndex&>(sparse_index);
    return ValidateSparseCSXTensorCreation<SparseCSCIndex, ValueType, IndexType>(
        sparse_csc_index, values_buffer, tensor);
  } else {
    auto sparse_csr_index = checked_cast<const SparseCSRIndex&>(sparse_index);
    return ValidateSparseCSXTensorCreation<SparseCSRIndex, ValueType, IndexType>(
        sparse_csr_index, values_buffer, tensor);
  }
}

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCSRIndex

class SparseCSXMatrixConverter {
 public:
  SparseCSXMatrixConverter(SparseMatrixCompressedAxis axis, const Tensor& tensor,
                           const std::shared_ptr<DataType>& index_value_type,
                           MemoryPool* pool)
      : axis_(axis), tensor_(tensor), index_value_type_(index_value_type), pool_(pool) {}

  template <typename ValueType, typename IndexType>
  Status Convert(const ValueType&, const IndexType&) {
    RETURN_NOT_OK(::arrow::internal::CheckSparseIndexMaximumValue(index_value_type_,
                                                                  tensor_.shape()));
    using ValueCType = typename ValueType::c_type;
    using IndexCType = typename IndexType::c_type;

    const int index_elsize = index_value_type_->byte_width();
    const int value_elsize = tensor_.type()->byte_width();

    const int64_t ndim = tensor_.ndim();
    if (ndim > 2) {
      return Status::Invalid("Invalid tensor dimension");
    } else if (ndim <= 1) {
      return Status::NotImplemented("TODO for ndim <= 1");
    }

    const int major_axis = static_cast<int>(axis_);
    const int64_t n_major = tensor_.shape()[major_axis];
    const int64_t n_minor = tensor_.shape()[1 - major_axis];
    ARROW_ASSIGN_OR_RAISE(int64_t nonzero_count, tensor_.CountNonZero());

    std::shared_ptr<Buffer> indptr_buffer;
    std::shared_ptr<Buffer> indices_buffer;

    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(value_elsize * nonzero_count, pool_));
    ARROW_ASSIGN_OR_RAISE(indptr_buffer,
                          AllocateBuffer(index_elsize * (n_major + 1), pool_));
    ARROW_ASSIGN_OR_RAISE(indices_buffer,
                          AllocateBuffer(index_elsize * nonzero_count, pool_));

    auto* indptr = indptr_buffer->mutable_data_as<IndexCType>();
    auto* values = values_buffer->mutable_data_as<ValueCType>();
    auto* indices = indices_buffer->mutable_data_as<IndexCType>();

    std::vector<int64_t> coords(2);
    int64_t k = 0;
    indptr[0] = 0;
    ++indptr;
    for (int64_t i = 0; i < n_major; ++i) {
      for (int64_t j = 0; j < n_minor; ++j) {
        if (axis_ == SparseMatrixCompressedAxis::ROW) {
          coords = {i, j};
        } else {
          coords = {j, i};
        }
        auto value = tensor_.Value<ValueType>(coords);
        if (is_not_zero<ValueType>(value)) {
          *values++ = value;
          *indices++ = static_cast<IndexCType>(j);
          k++;
        }
      }
      *indptr++ = static_cast<IndexCType>(k);
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
    DCHECK_OK((ValidateSparseCSXTensorCreation<ValueType, IndexType>(*sparse_index, *data,
                                                                     tensor_)));
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
  ConverterVisitor visitor(converter);
  ARROW_RETURN_NOT_OK(VisitValueAndIndexType(*tensor.type(), *index_value_type, visitor));
  *out_sparse_index = converter.sparse_index;
  *out_data = converter.data;
  return Status::OK();
}

namespace {

Result<std::shared_ptr<Tensor>> MakeTensorFromSparseCSXMatrix(
    SparseMatrixCompressedAxis axis, MemoryPool* pool,
    const std::shared_ptr<Tensor>& indptr, const std::shared_ptr<Tensor>& indices,
    const int64_t non_zero_length, const std::shared_ptr<DataType>& value_type,
    const std::vector<int64_t>& shape, const int64_t tensor_size, const uint8_t* raw_data,
    const std::vector<std::string>& dim_names) {
  const auto* indptr_data = indptr->raw_data();
  const auto* indices_data = indices->raw_data();

  const int indptr_elsize = indptr->type()->byte_width();
  const int indices_elsize = indices->type()->byte_width();

  const auto& fw_value_type = checked_cast<const FixedWidthType&>(*value_type);
  const int value_elsize = fw_value_type.byte_width();
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

}  // namespace

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
