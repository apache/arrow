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

#include "arrow/tensor.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/unreachable.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::checked_cast;

namespace internal {

Status ComputeRowMajorStrides(const FixedWidthType& type,
                              const std::vector<int64_t>& shape,
                              std::vector<int64_t>* strides) {
  const int byte_width = type.byte_width();
  const size_t ndim = shape.size();

  int64_t remaining = 0;
  if (!shape.empty() && shape.front() > 0) {
    remaining = byte_width;
    for (size_t i = 1; i < ndim; ++i) {
      if (internal::MultiplyWithOverflow(remaining, shape[i], &remaining)) {
        return Status::Invalid(
            "Row-major strides computed from shape would not fit in 64-bit integer");
      }
    }
  }

  if (remaining == 0) {
    strides->assign(shape.size(), byte_width);
    return Status::OK();
  }

  strides->push_back(remaining);
  for (size_t i = 1; i < ndim; ++i) {
    remaining /= shape[i];
    strides->push_back(remaining);
  }

  return Status::OK();
}

Status ComputeColumnMajorStrides(const FixedWidthType& type,
                                 const std::vector<int64_t>& shape,
                                 std::vector<int64_t>* strides) {
  const int byte_width = type.byte_width();
  const size_t ndim = shape.size();

  int64_t total = 0;
  if (!shape.empty() && shape.back() > 0) {
    total = byte_width;
    for (size_t i = 0; i < ndim - 1; ++i) {
      if (internal::MultiplyWithOverflow(total, shape[i], &total)) {
        return Status::Invalid(
            "Column-major strides computed from shape would not fit in 64-bit "
            "integer");
      }
    }
  }

  if (total == 0) {
    strides->assign(shape.size(), byte_width);
    return Status::OK();
  }

  total = byte_width;
  for (size_t i = 0; i < ndim - 1; ++i) {
    strides->push_back(total);
    total *= shape[i];
  }
  strides->push_back(total);

  return Status::OK();
}

}  // namespace internal

namespace {

inline bool IsTensorStridesRowMajor(const std::shared_ptr<DataType>& type,
                                    const std::vector<int64_t>& shape,
                                    const std::vector<int64_t>& strides) {
  std::vector<int64_t> c_strides;
  const auto& fw_type = checked_cast<const FixedWidthType&>(*type);
  if (internal::ComputeRowMajorStrides(fw_type, shape, &c_strides).ok()) {
    return strides == c_strides;
  } else {
    return false;
  }
}

inline bool IsTensorStridesColumnMajor(const std::shared_ptr<DataType>& type,
                                       const std::vector<int64_t>& shape,
                                       const std::vector<int64_t>& strides) {
  std::vector<int64_t> f_strides;
  const auto& fw_type = checked_cast<const FixedWidthType&>(*type);
  if (internal::ComputeColumnMajorStrides(fw_type, shape, &f_strides).ok()) {
    return strides == f_strides;
  } else {
    return false;
  }
}

inline Status CheckTensorValidity(const std::shared_ptr<DataType>& type,
                                  const std::shared_ptr<Buffer>& data,
                                  const std::vector<int64_t>& shape) {
  if (!type) {
    return Status::Invalid("Null type is supplied");
  }
  if (!is_tensor_supported(type->id())) {
    return Status::Invalid(type->ToString(), " is not valid data type for a tensor");
  }
  if (!data) {
    return Status::Invalid("Null data is supplied");
  }
  if (!std::all_of(shape.begin(), shape.end(), [](int64_t x) { return x >= 0; })) {
    return Status::Invalid("Shape elements must be positive");
  }
  return Status::OK();
}

Status CheckTensorStridesValidity(const std::shared_ptr<Buffer>& data,
                                  const std::vector<int64_t>& shape,
                                  const std::vector<int64_t>& strides,
                                  const std::shared_ptr<DataType>& type) {
  if (strides.size() != shape.size()) {
    return Status::Invalid("strides must have the same length as shape");
  }
  if (data->size() == 0 && std::find(shape.begin(), shape.end(), 0) != shape.end()) {
    return Status::OK();
  }

  // Check the largest offset can be computed without overflow
  const size_t ndim = shape.size();
  int64_t largest_offset = 0;
  for (size_t i = 0; i < ndim; ++i) {
    if (shape[i] == 0) continue;
    if (strides[i] < 0) {
      // TODO(mrkn): Support negative strides for sharing views
      return Status::Invalid("negative strides not supported");
    }

    int64_t dim_offset;
    if (!internal::MultiplyWithOverflow(shape[i] - 1, strides[i], &dim_offset)) {
      if (!internal::AddWithOverflow(largest_offset, dim_offset, &largest_offset)) {
        continue;
      }
    }

    return Status::Invalid(
        "offsets computed from shape and strides would not fit in 64-bit integer");
  }

  const int byte_width = type->byte_width();
  if (largest_offset > data->size() - byte_width) {
    return Status::Invalid("strides must not involve buffer over run");
  }
  return Status::OK();
}

}  // namespace

namespace internal {

bool IsTensorStridesContiguous(const std::shared_ptr<DataType>& type,
                               const std::vector<int64_t>& shape,
                               const std::vector<int64_t>& strides) {
  return IsTensorStridesRowMajor(type, shape, strides) ||
         IsTensorStridesColumnMajor(type, shape, strides);
}

Status ValidateTensorParameters(const std::shared_ptr<DataType>& type,
                                const std::shared_ptr<Buffer>& data,
                                const std::vector<int64_t>& shape,
                                const std::vector<int64_t>& strides,
                                const std::vector<std::string>& dim_names) {
  RETURN_NOT_OK(CheckTensorValidity(type, data, shape));
  if (!strides.empty()) {
    RETURN_NOT_OK(CheckTensorStridesValidity(data, shape, strides, type));
  } else {
    std::vector<int64_t> tmp_strides;
    RETURN_NOT_OK(ComputeRowMajorStrides(checked_cast<const FixedWidthType&>(*type),
                                         shape, &tmp_strides));
  }
  if (dim_names.size() > shape.size()) {
    return Status::Invalid("too many dim_names are supplied");
  }
  return Status::OK();
}

template <typename Out>
struct ConvertColumnsToTensorVisitor {
  Out*& out_values;
  const ArrayData& in_data;

  template <typename T>
  Status Visit(const T&) {
    if constexpr (is_numeric(T::type_id)) {
      using In = typename T::c_type;
      auto in_values = ArraySpan(in_data).GetSpan<In>(1, in_data.length);

      if (in_data.null_count == 0) {
        if constexpr (std::is_same_v<In, Out>) {
          memcpy(out_values, in_values.data(), in_values.size_bytes());
          out_values += in_values.size();
        } else {
          for (In in_value : in_values) {
            *out_values++ = static_cast<Out>(in_value);
          }
        }
      } else {
        for (int64_t i = 0; i < in_data.length; ++i) {
          *out_values++ =
              in_data.IsNull(i) ? static_cast<Out>(NAN) : static_cast<Out>(in_values[i]);
        }
      }
      return Status::OK();
    }
    Unreachable();
  }
};

template <typename Out>
struct ConvertColumnsToTensorRowMajorVisitor {
  Out*& out_values;
  const ArrayData& in_data;
  int num_cols;
  int col_idx;

  template <typename T>
  Status Visit(const T&) {
    if constexpr (is_numeric(T::type_id)) {
      using In = typename T::c_type;
      auto in_values = ArraySpan(in_data).GetSpan<In>(1, in_data.length);

      if (in_data.null_count == 0) {
        for (int64_t i = 0; i < in_data.length; ++i) {
          out_values[i * num_cols + col_idx] = static_cast<Out>(in_values[i]);
        }
      } else {
        for (int64_t i = 0; i < in_data.length; ++i) {
          out_values[i * num_cols + col_idx] =
              in_data.IsNull(i) ? static_cast<Out>(NAN) : static_cast<Out>(in_values[i]);
        }
      }
      return Status::OK();
    }
    Unreachable();
  }
};

template <typename DataType>
inline void ConvertColumnsToTensor(const RecordBatch& batch, uint8_t* out,
                                   bool row_major) {
  using CType = typename arrow::TypeTraits<DataType>::CType;
  auto* out_values = reinterpret_cast<CType*>(out);

  int i = 0;
  for (const auto& column : batch.columns()) {
    if (row_major) {
      ConvertColumnsToTensorRowMajorVisitor<CType> visitor{out_values, *column->data(),
                                                           batch.num_columns(), i++};
      DCHECK_OK(VisitTypeInline(*column->type(), &visitor));
    } else {
      ConvertColumnsToTensorVisitor<CType> visitor{out_values, *column->data()};
      DCHECK_OK(VisitTypeInline(*column->type(), &visitor));
    }
  }
}

Status RecordBatchToTensor(const RecordBatch& batch, bool null_to_nan, bool row_major,
                           MemoryPool* pool, std::shared_ptr<Tensor>* tensor) {
  if (batch.num_columns() == 0) {
    return Status::TypeError(
        "Conversion to Tensor for RecordBatches without columns/schema is not "
        "supported.");
  }
  // Check for no validity bitmap of each field
  // if null_to_nan conversion is set to false
  for (int i = 0; i < batch.num_columns(); ++i) {
    if (batch.column(i)->null_count() > 0 && !null_to_nan) {
      return Status::TypeError(
          "Can only convert a RecordBatch with no nulls. Set null_to_nan to true to "
          "convert nulls to NaN");
    }
  }

  // Check for supported data types and merge fields
  // to get the resulting uniform data type
  if (!is_integer(batch.column(0)->type()->id()) &&
      !is_floating(batch.column(0)->type()->id())) {
    return Status::TypeError("DataType is not supported: ",
                             batch.column(0)->type()->ToString());
  }
  std::shared_ptr<Field> result_field = batch.schema()->field(0);
  std::shared_ptr<DataType> result_type = result_field->type();

  Field::MergeOptions options;
  options.promote_integer_to_float = true;
  options.promote_integer_sign = true;
  options.promote_numeric_width = true;

  if (batch.num_columns() > 1) {
    for (int i = 1; i < batch.num_columns(); ++i) {
      if (!is_numeric(batch.column(i)->type()->id())) {
        return Status::TypeError("DataType is not supported: ",
                                 batch.column(i)->type()->ToString());
      }

      // Casting of float16 is not supported, throw an error in this case
      if ((batch.column(i)->type()->id() == Type::HALF_FLOAT ||
           result_field->type()->id() == Type::HALF_FLOAT) &&
          batch.column(i)->type()->id() != result_field->type()->id()) {
        return Status::NotImplemented("Casting from or to halffloat is not supported.");
      }

      ARROW_ASSIGN_OR_RAISE(
          result_field,
          result_field->MergeWith(
              batch.schema()->field(i)->WithName(result_field->name()), options));
    }
    result_type = result_field->type();
  }

  // Check if result_type is signed or unsigned integer and null_to_nan is set to true
  // Then all columns should be promoted to float type
  if (is_integer(result_type->id()) && null_to_nan) {
    ARROW_ASSIGN_OR_RAISE(
        result_field,
        result_field->MergeWith(field(result_field->name(), float32()), options));
    result_type = result_field->type();
  }

  // Allocate memory
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<Buffer> result,
      AllocateBuffer(result_type->bit_width() * batch.num_columns() * batch.num_rows(),
                     pool));
  // Copy data
  switch (result_type->id()) {
    case Type::UINT8:
      ConvertColumnsToTensor<UInt8Type>(batch, result->mutable_data(), row_major);
      break;
    case Type::UINT16:
    case Type::HALF_FLOAT:
      ConvertColumnsToTensor<UInt16Type>(batch, result->mutable_data(), row_major);
      break;
    case Type::UINT32:
      ConvertColumnsToTensor<UInt32Type>(batch, result->mutable_data(), row_major);
      break;
    case Type::UINT64:
      ConvertColumnsToTensor<UInt64Type>(batch, result->mutable_data(), row_major);
      break;
    case Type::INT8:
      ConvertColumnsToTensor<Int8Type>(batch, result->mutable_data(), row_major);
      break;
    case Type::INT16:
      ConvertColumnsToTensor<Int16Type>(batch, result->mutable_data(), row_major);
      break;
    case Type::INT32:
      ConvertColumnsToTensor<Int32Type>(batch, result->mutable_data(), row_major);
      break;
    case Type::INT64:
      ConvertColumnsToTensor<Int64Type>(batch, result->mutable_data(), row_major);
      break;
    case Type::FLOAT:
      ConvertColumnsToTensor<FloatType>(batch, result->mutable_data(), row_major);
      break;
    case Type::DOUBLE:
      ConvertColumnsToTensor<DoubleType>(batch, result->mutable_data(), row_major);
      break;
    default:
      return Status::TypeError("DataType is not supported: ", result_type->ToString());
  }

  // Construct Tensor object
  const auto& fixed_width_type =
      internal::checked_cast<const FixedWidthType&>(*result_type);
  std::vector<int64_t> shape = {batch.num_rows(), batch.num_columns()};
  std::vector<int64_t> strides;

  if (row_major) {
    ARROW_RETURN_NOT_OK(
        internal::ComputeRowMajorStrides(fixed_width_type, shape, &strides));
  } else {
    ARROW_RETURN_NOT_OK(
        internal::ComputeColumnMajorStrides(fixed_width_type, shape, &strides));
  }
  ARROW_ASSIGN_OR_RAISE(*tensor,
                        Tensor::Make(result_type, std::move(result), shape, strides));
  return Status::OK();
}

}  // namespace internal

/// Constructor with strides and dimension names
Tensor::Tensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
               const std::vector<int64_t>& shape, const std::vector<int64_t>& strides,
               const std::vector<std::string>& dim_names)
    : type_(type), data_(data), shape_(shape), strides_(strides), dim_names_(dim_names) {
  ARROW_CHECK(is_tensor_supported(type->id()));
  if (shape.size() > 0 && strides.size() == 0) {
    ARROW_CHECK_OK(internal::ComputeRowMajorStrides(
        checked_cast<const FixedWidthType&>(*type_), shape, &strides_));
  }
}

Tensor::Tensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
               const std::vector<int64_t>& shape, const std::vector<int64_t>& strides)
    : Tensor(type, data, shape, strides, {}) {}

Tensor::Tensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
               const std::vector<int64_t>& shape)
    : Tensor(type, data, shape, {}, {}) {}

const std::string& Tensor::dim_name(int i) const {
  static const std::string kEmpty = "";
  if (dim_names_.size() == 0) {
    return kEmpty;
  } else {
    ARROW_CHECK_LT(i, static_cast<int>(dim_names_.size()));
    return dim_names_[i];
  }
}

int64_t Tensor::size() const {
  return std::accumulate(shape_.begin(), shape_.end(), 1LL, std::multiplies<int64_t>());
}

bool Tensor::is_contiguous() const {
  return internal::IsTensorStridesContiguous(type_, shape_, strides_);
}

bool Tensor::is_row_major() const {
  return IsTensorStridesRowMajor(type_, shape_, strides_);
}

bool Tensor::is_column_major() const {
  return IsTensorStridesColumnMajor(type_, shape_, strides_);
}

Type::type Tensor::type_id() const { return type_->id(); }

bool Tensor::Equals(const Tensor& other, const EqualOptions& opts) const {
  return TensorEquals(*this, other, opts);
}

namespace {

template <typename TYPE>
int64_t StridedTensorCountNonZero(int dim_index, int64_t offset, const Tensor& tensor) {
  using c_type = typename TYPE::c_type;
  const c_type zero = c_type(0);
  int64_t nnz = 0;
  if (dim_index == tensor.ndim() - 1) {
    for (int64_t i = 0; i < tensor.shape()[dim_index]; ++i) {
      const auto* ptr = tensor.raw_data() + offset + i * tensor.strides()[dim_index];
      auto& elem = *reinterpret_cast<const c_type*>(ptr);
      if (elem != zero) ++nnz;
    }
    return nnz;
  }
  for (int64_t i = 0; i < tensor.shape()[dim_index]; ++i) {
    nnz += StridedTensorCountNonZero<TYPE>(dim_index + 1, offset, tensor);
    offset += tensor.strides()[dim_index];
  }
  return nnz;
}

template <typename TYPE>
int64_t ContiguousTensorCountNonZero(const Tensor& tensor) {
  using c_type = typename TYPE::c_type;
  auto* data = reinterpret_cast<const c_type*>(tensor.raw_data());
  return std::count_if(data, data + tensor.size(),
                       [](const c_type& x) { return x != 0; });
}

template <typename TYPE>
inline int64_t TensorCountNonZero(const Tensor& tensor) {
  if (tensor.is_contiguous()) {
    return ContiguousTensorCountNonZero<TYPE>(tensor);
  } else {
    return StridedTensorCountNonZero<TYPE>(0, 0, tensor);
  }
}

struct NonZeroCounter {
  explicit NonZeroCounter(const Tensor& tensor) : tensor_(tensor) {}

  template <typename TYPE>
  enable_if_number<TYPE, Status> Visit(const TYPE& type) {
    result = TensorCountNonZero<TYPE>(tensor_);
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    ARROW_CHECK(!is_tensor_supported(type.id()));
    return Status::NotImplemented("Tensor of ", type.ToString(), " is not implemented");
  }

  const Tensor& tensor_;
  int64_t result = 0;
};

}  // namespace

Result<int64_t> Tensor::CountNonZero() const {
  NonZeroCounter counter(*this);
  RETURN_NOT_OK(VisitTypeInline(*type(), &counter));
  return counter.result;
}

}  // namespace arrow
