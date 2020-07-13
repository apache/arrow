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
namespace {

template <typename IndexValueType>
Status CheckSparseIndexMaximumValue(const std::vector<int64_t>& shape) {
  using c_index_value_type = typename IndexValueType::c_type;
  constexpr int64_t type_max =
      static_cast<int64_t>(std::numeric_limits<c_index_value_type>::max());
  auto greater_than_type_max = [&](int64_t x) { return x > type_max; };
  if (std::any_of(shape.begin(), shape.end(), greater_than_type_max)) {
    return Status::Invalid("The bit width of the index value type is too small");
  }
  return Status::OK();
}

template <>
Status CheckSparseIndexMaximumValue<Int64Type>(const std::vector<int64_t>& shape) {
  return Status::OK();
}

template <>
Status CheckSparseIndexMaximumValue<UInt64Type>(const std::vector<int64_t>& shape) {
  return Status::Invalid("UInt64Type cannot be used as IndexValueType of SparseIndex");
}

}  // namespace

#define CALL_CHECK_MAXIMUM_VALUE(TYPE_CLASS) \
  case TYPE_CLASS##Type::type_id:            \
    return CheckSparseIndexMaximumValue<TYPE_CLASS##Type>(shape);

Status CheckSparseIndexMaximumValue(const std::shared_ptr<DataType>& index_value_type,
                                    const std::vector<int64_t>& shape) {
  switch (index_value_type->id()) {
    ARROW_GENERATE_FOR_ALL_INTEGER_TYPES(CALL_CHECK_MAXIMUM_VALUE);
    default:
      return Status::TypeError("Unsupported SparseTensor index value type");
  }
}

#undef CALL_CHECK_MAXIMUM_VALUE

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
      return MakeSparseCSXMatrixFromTensor(SparseMatrixCompressedAxis::ROW, tensor,
                                           index_value_type, pool, out_sparse_index,
                                           out_data);
    case SparseTensorFormat::CSC:
      return MakeSparseCSXMatrixFromTensor(SparseMatrixCompressedAxis::COLUMN, tensor,
                                           index_value_type, pool, out_sparse_index,
                                           out_data);
    case SparseTensorFormat::CSF:
      return MakeSparseCSFTensorFromTensor(tensor, index_value_type, pool,
                                           out_sparse_index, out_data);

    // LCOV_EXCL_START: ignore program failure
    default:
      return Status::Invalid("Invalid sparse tensor format");
      // LCOV_EXCL_STOP
  }
}

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

  RETURN_NOT_OK(internal::CheckSparseIndexMaximumValue(type, shape));

  if (!internal::IsTensorStridesContiguous(type, shape, strides)) {
    return Status::Invalid("SparseCOOIndex indices must be contiguous");
  }
  return Status::OK();
}

void GetCOOIndexTensorRow(const std::shared_ptr<Tensor>& coords, const int64_t row,
                          std::vector<int64_t>* out_index) {
  const auto& fw_index_value_type =
      internal::checked_cast<const FixedWidthType&>(*coords->type());
  const size_t indices_elsize = fw_index_value_type.bit_width() / CHAR_BIT;

  const auto& shape = coords->shape();
  const int64_t non_zero_length = shape[0];
  DCHECK(0 <= row && row < non_zero_length);

  const int64_t ndim = shape[1];
  out_index->resize(ndim);

  switch (indices_elsize) {
    case 1:  // Int8, UInt8
      for (int64_t i = 0; i < ndim; ++i) {
        (*out_index)[i] = static_cast<int64_t>(coords->Value<UInt8Type>({row, i}));
      }
      break;
    case 2:  // Int16, UInt16
      for (int64_t i = 0; i < ndim; ++i) {
        (*out_index)[i] = static_cast<int64_t>(coords->Value<UInt16Type>({row, i}));
      }
      break;
    case 4:  // Int32, UInt32
      for (int64_t i = 0; i < ndim; ++i) {
        (*out_index)[i] = static_cast<int64_t>(coords->Value<UInt32Type>({row, i}));
      }
      break;
    case 8:  // Int64
      for (int64_t i = 0; i < ndim; ++i) {
        (*out_index)[i] = coords->Value<Int64Type>({row, i});
      }
      break;
    default:
      DCHECK(false) << "Must not reach here";
      break;
  }
}

bool DetectSparseCOOIndexCanonicality(const std::shared_ptr<Tensor>& coords) {
  DCHECK_EQ(coords->ndim(), 2);

  const auto& shape = coords->shape();
  const int64_t non_zero_length = shape[0];
  if (non_zero_length <= 1) return true;

  const int64_t ndim = shape[1];
  std::vector<int64_t> last_index, index;
  GetCOOIndexTensorRow(coords, 0, &last_index);
  for (int64_t i = 1; i < non_zero_length; ++i) {
    GetCOOIndexTensorRow(coords, i, &index);
    int64_t j = 0;
    while (j < ndim) {
      if (last_index[j] > index[j]) {
        // last_index > index, so we can detect non-canonical here
        return false;
      }
      if (last_index[j] < index[j]) {
        // last_index < index, so we can skip the remaining dimensions
        break;
      }
      ++j;
    }
    if (j == ndim) {
      // last_index == index, so we can detect non-canonical here
      return false;
    }
    swap(last_index, index);
  }

  return true;
}

}  // namespace

Result<std::shared_ptr<SparseCOOIndex>> SparseCOOIndex::Make(
    const std::shared_ptr<Tensor>& coords, bool is_canonical) {
  RETURN_NOT_OK(
      CheckSparseCOOIndexValidity(coords->type(), coords->shape(), coords->strides()));
  return std::make_shared<SparseCOOIndex>(coords, is_canonical);
}

Result<std::shared_ptr<SparseCOOIndex>> SparseCOOIndex::Make(
    const std::shared_ptr<Tensor>& coords) {
  RETURN_NOT_OK(
      CheckSparseCOOIndexValidity(coords->type(), coords->shape(), coords->strides()));
  auto is_canonical = DetectSparseCOOIndexCanonicality(coords);
  return std::make_shared<SparseCOOIndex>(coords, is_canonical);
}

Result<std::shared_ptr<SparseCOOIndex>> SparseCOOIndex::Make(
    const std::shared_ptr<DataType>& indices_type,
    const std::vector<int64_t>& indices_shape,
    const std::vector<int64_t>& indices_strides, std::shared_ptr<Buffer> indices_data,
    bool is_canonical) {
  RETURN_NOT_OK(
      CheckSparseCOOIndexValidity(indices_type, indices_shape, indices_strides));
  return std::make_shared<SparseCOOIndex>(
      std::make_shared<Tensor>(indices_type, indices_data, indices_shape,
                               indices_strides),
      is_canonical);
}

Result<std::shared_ptr<SparseCOOIndex>> SparseCOOIndex::Make(
    const std::shared_ptr<DataType>& indices_type,
    const std::vector<int64_t>& indices_shape,
    const std::vector<int64_t>& indices_strides, std::shared_ptr<Buffer> indices_data) {
  RETURN_NOT_OK(
      CheckSparseCOOIndexValidity(indices_type, indices_shape, indices_strides));
  auto coords = std::make_shared<Tensor>(indices_type, indices_data, indices_shape,
                                         indices_strides);
  auto is_canonical = DetectSparseCOOIndexCanonicality(coords);
  return std::make_shared<SparseCOOIndex>(coords, is_canonical);
}

Result<std::shared_ptr<SparseCOOIndex>> SparseCOOIndex::Make(
    const std::shared_ptr<DataType>& indices_type, const std::vector<int64_t>& shape,
    int64_t non_zero_length, std::shared_ptr<Buffer> indices_data, bool is_canonical) {
  auto ndim = static_cast<int64_t>(shape.size());
  if (!is_integer(indices_type->id())) {
    return Status::TypeError("Type of SparseCOOIndex indices must be integer");
  }
  const int64_t elsize =
      internal::checked_cast<const IntegerType&>(*indices_type).bit_width() / 8;
  std::vector<int64_t> indices_shape({non_zero_length, ndim});
  std::vector<int64_t> indices_strides({elsize * ndim, elsize});
  return Make(indices_type, indices_shape, indices_strides, indices_data, is_canonical);
}

Result<std::shared_ptr<SparseCOOIndex>> SparseCOOIndex::Make(
    const std::shared_ptr<DataType>& indices_type, const std::vector<int64_t>& shape,
    int64_t non_zero_length, std::shared_ptr<Buffer> indices_data) {
  auto ndim = static_cast<int64_t>(shape.size());
  if (!is_integer(indices_type->id())) {
    return Status::TypeError("Type of SparseCOOIndex indices must be integer");
  }
  const int64_t elsize = internal::GetByteWidth(*indices_type);
  std::vector<int64_t> indices_shape({non_zero_length, ndim});
  std::vector<int64_t> indices_strides({elsize * ndim, elsize});
  return Make(indices_type, indices_shape, indices_strides, indices_data);
}

// Constructor with a contiguous NumericTensor
SparseCOOIndex::SparseCOOIndex(const std::shared_ptr<Tensor>& coords, bool is_canonical)
    : SparseIndexBase(), coords_(coords), is_canonical_(is_canonical) {
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

  RETURN_NOT_OK(internal::CheckSparseIndexMaximumValue(indptr_type, indptr_shape));
  RETURN_NOT_OK(internal::CheckSparseIndexMaximumValue(indices_type, indices_shape));

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
                                            indices.size(), axis_order.size()));

  for (auto tensor : indptr) {
    RETURN_NOT_OK(internal::CheckSparseIndexMaximumValue(indptr_type, tensor->shape()));
  }

  for (auto tensor : indices) {
    RETURN_NOT_OK(internal::CheckSparseIndexMaximumValue(indices_type, tensor->shape()));
  }

  return std::make_shared<SparseCSFIndex>(indptr, indices, axis_order);
}

// Constructor with two index vectors
SparseCSFIndex::SparseCSFIndex(const std::vector<std::shared_ptr<Tensor>>& indptr,
                               const std::vector<std::shared_ptr<Tensor>>& indices,
                               const std::vector<int64_t>& axis_order)
    : SparseIndexBase(), indptr_(indptr), indices_(indices), axis_order_(axis_order) {
  ARROW_CHECK_OK(CheckSparseCSFIndexValidity(indptr_.front()->type(),
                                             indices_.front()->type(), indptr_.size(),
                                             indices_.size(), axis_order_.size()));
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

Result<std::shared_ptr<Tensor>> SparseTensor::ToTensor(MemoryPool* pool) const {
  switch (format_id()) {
    case SparseTensorFormat::COO:
      return MakeTensorFromSparseCOOTensor(
          pool, internal::checked_cast<const SparseCOOTensor*>(this));
      break;

    case SparseTensorFormat::CSR:
      return MakeTensorFromSparseCSRMatrix(
          pool, internal::checked_cast<const SparseCSRMatrix*>(this));
      break;

    case SparseTensorFormat::CSC:
      return MakeTensorFromSparseCSCMatrix(
          pool, internal::checked_cast<const SparseCSCMatrix*>(this));
      break;

    case SparseTensorFormat::CSF:
      return MakeTensorFromSparseCSFTensor(
          pool, internal::checked_cast<const SparseCSFTensor*>(this));

    default:
      return Status::NotImplemented("Unsupported SparseIndex format type");
  }
}

}  // namespace arrow
