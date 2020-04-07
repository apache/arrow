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

#include "arrow/sparse_tensor.h"
#include "arrow/tensor/converter.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <numeric>

#include "arrow/compare.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class MemoryPool;

// ----------------------------------------------------------------------
// SparseIndex

Status SparseIndex::ValidateShape(const std::vector<int64_t>& shape) const {
  if (!std::all_of(shape.begin(), shape.end(), [](int64_t x) { return x >= 0; })) {
    return Status::Invalid("Shape elements must be positive");
  }

  return Status::OK();
}

namespace internal {

Status MakeSparseTensorFromTensor(const Tensor& tensor,
                                  SparseTensorFormat::type sparse_format_id,
                                  const std::shared_ptr<DataType>& index_value_type,
                                  MemoryPool* pool,
                                  std::shared_ptr<SparseIndex>* out_sparse_index,
                                  std::shared_ptr<Buffer>* out_data) {
  switch (sparse_format_id) {
    case SparseTensorFormat::COO:
      return MakeSparseCOOTensorFromTensor(tensor, index_value_type, pool,
                                           out_sparse_index, out_data);
    case SparseTensorFormat::CSR:
      return MakeSparseCSRMatrixFromTensor(tensor, index_value_type, pool,
                                           out_sparse_index, out_data);
    case SparseTensorFormat::CSC:
      return MakeSparseCSCMatrixFromTensor(tensor, index_value_type, pool,
                                           out_sparse_index, out_data);
    case SparseTensorFormat::CSF:
      return MakeSparseCSFTensorFromTensor(tensor, index_value_type, pool,
                                           out_sparse_index, out_data);

    // LCOV_EXCL_START: ignore program failure
    default:
      return Status::Invalid("Invalid sparse tensor format");
      // LCOV_EXCL_STOP
  }
}

namespace {

template <typename TYPE, typename IndexValueType>
void ExpandSparseCSFTensorValues(int64_t dimension, int64_t dense_offset,
                                 int64_t first_ptr, int64_t last_ptr,
                                 const SparseCSFIndex& sparse_index, const TYPE* raw_data,
                                 const std::vector<int64_t>& strides,
                                 const std::vector<int64_t>& axis_order, TYPE* out) {
  int64_t ndim = axis_order.size();

  for (int64_t i = first_ptr; i < last_ptr; ++i) {
    int64_t tmp_dense_offset =
        dense_offset + sparse_index.indices()[dimension]->Value<IndexValueType>({i}) *
                           strides[axis_order[dimension]];

    if (dimension < ndim - 1) {
      ExpandSparseCSFTensorValues<TYPE, IndexValueType>(
          dimension + 1, tmp_dense_offset,
          sparse_index.indptr()[dimension]->Value<IndexValueType>({i}),
          sparse_index.indptr()[dimension]->Value<IndexValueType>({i + 1}), sparse_index,
          raw_data, strides, axis_order, out);
    } else {
      out[tmp_dense_offset] = raw_data[i];
    }
  }
}

}  // namespace

template <typename TYPE, typename IndexValueType>
Status MakeTensorFromSparseTensor(MemoryPool* pool, const SparseTensor* sparse_tensor,
                                  std::shared_ptr<Tensor>* out) {
  using c_index_value_type = typename IndexValueType::c_type;
  using NumericTensorType = NumericTensor<TYPE>;
  using value_type = typename NumericTensorType::value_type;

  ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                        AllocateBuffer(sizeof(value_type) * sparse_tensor->size(), pool));
  auto values = reinterpret_cast<value_type*>(values_buffer->mutable_data());

  std::fill_n(values, sparse_tensor->size(), static_cast<value_type>(0));

  std::vector<int64_t> strides(sparse_tensor->ndim(), 1);
  for (int i = sparse_tensor->ndim() - 1; i > 0; --i) {
    strides[i - 1] *= strides[i] * sparse_tensor->shape()[i];
  }
  std::vector<int64_t> empty_strides;

  const auto raw_data = reinterpret_cast<const value_type*>(sparse_tensor->raw_data());

  switch (sparse_tensor->format_id()) {
    case SparseTensorFormat::COO: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCOOIndex&>(*sparse_tensor->sparse_index());
      const std::shared_ptr<const Tensor> coords = sparse_index.indices();

      for (int64_t i = 0; i < sparse_tensor->non_zero_length(); ++i) {
        std::vector<c_index_value_type> coord(sparse_tensor->ndim());
        int64_t offset = 0;
        for (int64_t j = 0; j < static_cast<int>(coord.size()); ++j) {
          coord[j] = coords->Value<IndexValueType>({i, j});
          offset += coord[j] * strides[j];
        }
        values[offset] = raw_data[i];
      }
      *out = std::make_shared<Tensor>(sparse_tensor->type(), std::move(values_buffer),
                                      sparse_tensor->shape(), empty_strides,
                                      sparse_tensor->dim_names());
      return Status::OK();
    }

    case SparseTensorFormat::CSR: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCSRIndex&>(*sparse_tensor->sparse_index());
      const std::shared_ptr<const Tensor> indptr = sparse_index.indptr();
      const std::shared_ptr<const Tensor> indices = sparse_index.indices();

      int64_t offset;
      for (int64_t i = 0; i < indptr->size() - 1; ++i) {
        const int64_t start = indptr->Value<IndexValueType>({i});
        const int64_t stop = indptr->Value<IndexValueType>({i + 1});
        for (int64_t j = start; j < stop; ++j) {
          offset = indices->Value<IndexValueType>({j}) + i * sparse_tensor->shape()[1];
          values[offset] = raw_data[j];
        }
      }
      *out = std::make_shared<Tensor>(sparse_tensor->type(), std::move(values_buffer),
                                      sparse_tensor->shape(), empty_strides,
                                      sparse_tensor->dim_names());
      return Status::OK();
    }

    case SparseTensorFormat::CSC: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCSCIndex&>(*sparse_tensor->sparse_index());
      const std::shared_ptr<const Tensor> indptr = sparse_index.indptr();
      const std::shared_ptr<const Tensor> indices = sparse_index.indices();

      int64_t offset;
      for (int64_t j = 0; j < indptr->size() - 1; ++j) {
        const int64_t start = indptr->Value<IndexValueType>({j});
        const int64_t stop = indptr->Value<IndexValueType>({j + 1});
        for (int64_t i = start; i < stop; ++i) {
          offset = j + indices->Value<IndexValueType>({i}) * sparse_tensor->shape()[1];
          values[offset] = raw_data[i];
        }
      }
      *out = std::make_shared<Tensor>(sparse_tensor->type(), std::move(values_buffer),
                                      sparse_tensor->shape(), empty_strides,
                                      sparse_tensor->dim_names());
      return Status::OK();
    }

    case SparseTensorFormat::CSF: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCSFIndex&>(*sparse_tensor->sparse_index());

      ExpandSparseCSFTensorValues<value_type, IndexValueType>(
          0, 0, 0, sparse_index.indptr()[0]->size() - 1, sparse_index, raw_data, strides,
          sparse_index.axis_order(), values);
      *out = std::make_shared<Tensor>(sparse_tensor->type(), std::move(values_buffer),
                                      sparse_tensor->shape(), empty_strides,
                                      sparse_tensor->dim_names());
      return Status::OK();
    }
  }
  return Status::NotImplemented("Unsupported SparseIndex format type");
}

#define MAKE_TENSOR_FROM_SPARSE_TENSOR_INDEX_TYPE(IndexValueType)                      \
  case IndexValueType##Type::type_id:                                                  \
    return MakeTensorFromSparseTensor<TYPE, IndexValueType##Type>(pool, sparse_tensor, \
                                                                  out);                \
    break;

template <typename TYPE>
Status MakeTensorFromSparseTensor(MemoryPool* pool, const SparseTensor* sparse_tensor,
                                  std::shared_ptr<Tensor>* out) {
  std::shared_ptr<DataType> type;
  switch (sparse_tensor->format_id()) {
    case SparseTensorFormat::COO: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCOOIndex&>(*sparse_tensor->sparse_index());
      const std::shared_ptr<const Tensor> indices = sparse_index.indices();
      type = indices->type();
      break;
    }
    case SparseTensorFormat::CSR: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCSRIndex&>(*sparse_tensor->sparse_index());
      const std::shared_ptr<const Tensor> indices = sparse_index.indices();
      type = indices->type();
      break;
    }
    case SparseTensorFormat::CSC: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCSCIndex&>(*sparse_tensor->sparse_index());
      const std::shared_ptr<const Tensor> indices = sparse_index.indices();
      type = indices->type();
      break;
    }
    case SparseTensorFormat::CSF: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCSFIndex&>(*sparse_tensor->sparse_index());
      const std::vector<std::shared_ptr<Tensor>> indices = sparse_index.indices();
      type = indices[0]->type();
      break;
    }
      // LCOV_EXCL_START: ignore program failure
    default:
      ARROW_LOG(FATAL) << "Unsupported SparseIndex format";
      break;
      // LCOV_EXCL_STOP
  }

  switch (type->id()) {
    ARROW_GENERATE_FOR_ALL_INTEGER_TYPES(MAKE_TENSOR_FROM_SPARSE_TENSOR_INDEX_TYPE);
      // LCOV_EXCL_START: ignore program failure
    default:
      ARROW_LOG(FATAL) << "Unsupported SparseIndex value type";
      return Status::NotImplemented("Unsupported SparseIndex value type");
      // LCOV_EXCL_STOP
  }
}
#undef MAKE_TENSOR_FROM_SPARSE_TENSOR_INDEX_TYPE

#define MAKE_TENSOR_FROM_SPARSE_TENSOR_VALUE_TYPE(TYPE) \
  case TYPE##Type::type_id:                             \
    return MakeTensorFromSparseTensor<TYPE##Type>(pool, sparse_tensor, out);

Status MakeTensorFromSparseTensor(MemoryPool* pool, const SparseTensor* sparse_tensor,
                                  std::shared_ptr<Tensor>* out) {
  switch (sparse_tensor->type()->id()) {
    ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(MAKE_TENSOR_FROM_SPARSE_TENSOR_VALUE_TYPE);
    // LCOV_EXCL_START: ignore program failure
    default:
      ARROW_LOG(FATAL) << "Unsupported SparseTensor value type";
      return Status::NotImplemented("Unsupported SparseTensor data value type");
      // LCOV_EXCL_STOP
  }
}
#undef MAKE_TENSOR_FROM_SPARSE_TENSOR_VALUE_TYPE

}  // namespace internal

// ----------------------------------------------------------------------
// SparseCOOIndex

namespace {

inline Status CheckSparseCOOIndexValidity(const std::shared_ptr<DataType>& type,
                                          const std::vector<int64_t>& shape,
                                          const std::vector<int64_t>& strides) {
  if (!is_integer(type->id())) {
    return Status::TypeError("Type of SparseCOOIndex indices must be integer");
  }
  if (shape.size() != 2) {
    return Status::Invalid("SparseCOOIndex indices must be a matrix");
  }
  if (!internal::IsTensorStridesContiguous(type, shape, strides)) {
    return Status::Invalid("SparseCOOIndex indices must be contiguous");
  }
  return Status::OK();
}

}  // namespace

Result<std::shared_ptr<SparseCOOIndex>> SparseCOOIndex::Make(
    const std::shared_ptr<DataType>& indices_type,
    const std::vector<int64_t>& indices_shape,
    const std::vector<int64_t>& indices_strides, std::shared_ptr<Buffer> indices_data) {
  RETURN_NOT_OK(
      CheckSparseCOOIndexValidity(indices_type, indices_shape, indices_strides));
  return std::make_shared<SparseCOOIndex>(std::make_shared<Tensor>(
      indices_type, indices_data, indices_shape, indices_strides));
}

Result<std::shared_ptr<SparseCOOIndex>> SparseCOOIndex::Make(
    const std::shared_ptr<DataType>& indices_type, const std::vector<int64_t>& shape,
    int64_t non_zero_length, std::shared_ptr<Buffer> indices_data) {
  auto ndim = static_cast<int64_t>(shape.size());
  if (!is_integer(indices_type->id())) {
    return Status::TypeError("Type of SparseCOOIndex indices must be integer");
  }
  const int64_t elsize =
      internal::checked_cast<const IntegerType&>(*indices_type).bit_width() / 8;
  std::vector<int64_t> indices_shape({non_zero_length, ndim});
  std::vector<int64_t> indices_strides({elsize * ndim, elsize});
  return Make(indices_type, indices_shape, indices_strides, indices_data);
}

// Constructor with a contiguous NumericTensor
SparseCOOIndex::SparseCOOIndex(const std::shared_ptr<Tensor>& coords)
    : SparseIndexBase(coords->shape()[0]), coords_(coords) {
  ARROW_CHECK_OK(
      CheckSparseCOOIndexValidity(coords_->type(), coords_->shape(), coords_->strides()));
}

std::string SparseCOOIndex::ToString() const { return std::string("SparseCOOIndex"); }

// ----------------------------------------------------------------------
// SparseCSXIndex

namespace internal {

Status ValidateSparseCSXIndex(const std::shared_ptr<DataType>& indptr_type,
                              const std::shared_ptr<DataType>& indices_type,
                              const std::vector<int64_t>& indptr_shape,
                              const std::vector<int64_t>& indices_shape,
                              char const* type_name) {
  if (!is_integer(indptr_type->id())) {
    return Status::TypeError("Type of ", type_name, " indptr must be integer");
  }
  if (indptr_shape.size() != 1) {
    return Status::Invalid(type_name, " indptr must be a vector");
  }
  if (!is_integer(indices_type->id())) {
    return Status::Invalid("Type of ", type_name, " indices must be integer");
  }
  if (indices_shape.size() != 1) {
    return Status::Invalid(type_name, " indices must be a vector");
  }
  return Status::OK();
}

void CheckSparseCSXIndexValidity(const std::shared_ptr<DataType>& indptr_type,
                                 const std::shared_ptr<DataType>& indices_type,
                                 const std::vector<int64_t>& indptr_shape,
                                 const std::vector<int64_t>& indices_shape,
                                 char const* type_name) {
  ARROW_CHECK_OK(ValidateSparseCSXIndex(indptr_type, indices_type, indptr_shape,
                                        indices_shape, type_name));
}

}  // namespace internal

// ----------------------------------------------------------------------
// SparseCSFIndex

namespace {

inline Status CheckSparseCSFIndexValidity(const std::shared_ptr<DataType>& indptr_type,
                                          const std::shared_ptr<DataType>& indices_type,
                                          const int64_t num_indptrs,
                                          const int64_t num_indices,
                                          const std::vector<int64_t>& indptr_shape,
                                          const std::vector<int64_t>& indices_shape,
                                          const int64_t axis_order_size) {
  if (!is_integer(indptr_type->id())) {
    return Status::TypeError("Type of SparseCSFIndex indptr must be integer");
  }
  if (!is_integer(indices_type->id())) {
    return Status::TypeError("Type of SparseCSFIndex indices must be integer");
  }
  if (num_indptrs + 1 != num_indices) {
    return Status::Invalid(
        "Length of indices must be equal to length of indptrs + 1 for SparseCSFIndex.");
  }
  if (axis_order_size != num_indices) {
    return Status::Invalid(
        "Length of indices must be equal to number of dimensions for SparseCSFIndex.");
  }
  return Status::OK();
}

}  // namespace

Result<std::shared_ptr<SparseCSFIndex>> SparseCSFIndex::Make(
    const std::shared_ptr<DataType>& indptr_type,
    const std::shared_ptr<DataType>& indices_type,
    const std::vector<int64_t>& indices_shapes, const std::vector<int64_t>& axis_order,
    const std::vector<std::shared_ptr<Buffer>>& indptr_data,
    const std::vector<std::shared_ptr<Buffer>>& indices_data) {
  int64_t ndim = axis_order.size();
  std::vector<std::shared_ptr<Tensor>> indptr(ndim - 1);
  std::vector<std::shared_ptr<Tensor>> indices(ndim);

  for (int64_t i = 0; i < ndim - 1; ++i)
    indptr[i] = std::make_shared<Tensor>(indptr_type, indptr_data[i],
                                         std::vector<int64_t>({indices_shapes[i] + 1}));
  for (int64_t i = 0; i < ndim; ++i)
    indices[i] = std::make_shared<Tensor>(indices_type, indices_data[i],
                                          std::vector<int64_t>({indices_shapes[i]}));

  RETURN_NOT_OK(CheckSparseCSFIndexValidity(indptr_type, indices_type, indptr.size(),
                                            indices.size(), indptr.back()->shape(),
                                            indices.back()->shape(), axis_order.size()));

  return std::make_shared<SparseCSFIndex>(indptr, indices, axis_order);
}

// Constructor with two index vectors
SparseCSFIndex::SparseCSFIndex(const std::vector<std::shared_ptr<Tensor>>& indptr,
                               const std::vector<std::shared_ptr<Tensor>>& indices,
                               const std::vector<int64_t>& axis_order)
    : SparseIndexBase(indices.back()->size()),
      indptr_(indptr),
      indices_(indices),
      axis_order_(axis_order) {
  ARROW_CHECK_OK(CheckSparseCSFIndexValidity(
      indptr_.front()->type(), indices_.front()->type(), indptr_.size(), indices_.size(),
      indptr_.back()->shape(), indices_.back()->shape(), axis_order_.size()));
}

std::string SparseCSFIndex::ToString() const { return std::string("SparseCSFIndex"); }

bool SparseCSFIndex::Equals(const SparseCSFIndex& other) const {
  for (int64_t i = 0; i < static_cast<int64_t>(indices().size()); ++i) {
    if (!indices()[i]->Equals(*other.indices()[i])) return false;
  }
  for (int64_t i = 0; i < static_cast<int64_t>(indptr().size()); ++i) {
    if (!indptr()[i]->Equals(*other.indptr()[i])) return false;
  }
  return axis_order() == other.axis_order();
}

// ----------------------------------------------------------------------
// SparseTensor

// Constructor with all attributes
SparseTensor::SparseTensor(const std::shared_ptr<DataType>& type,
                           const std::shared_ptr<Buffer>& data,
                           const std::vector<int64_t>& shape,
                           const std::shared_ptr<SparseIndex>& sparse_index,
                           const std::vector<std::string>& dim_names)
    : type_(type),
      data_(data),
      shape_(shape),
      sparse_index_(sparse_index),
      dim_names_(dim_names) {
  ARROW_CHECK(is_tensor_supported(type->id()));
}

const std::string& SparseTensor::dim_name(int i) const {
  static const std::string kEmpty = "";
  if (dim_names_.size() == 0) {
    return kEmpty;
  } else {
    ARROW_CHECK_LT(i, static_cast<int>(dim_names_.size()));
    return dim_names_[i];
  }
}

int64_t SparseTensor::size() const {
  return std::accumulate(shape_.begin(), shape_.end(), 1LL, std::multiplies<int64_t>());
}

bool SparseTensor::Equals(const SparseTensor& other, const EqualOptions& opts) const {
  return SparseTensorEquals(*this, other, opts);
}

Status SparseTensor::ToTensor(MemoryPool* pool, std::shared_ptr<Tensor>* out) const {
  return internal::MakeTensorFromSparseTensor(pool, this, out);
}

}  // namespace arrow
